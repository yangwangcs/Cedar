// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/workload_driver.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <variant>

#include "cedar/benchmark/arrival_scheduler.h"
#include "cedar/blob/blob_store.h"
#include "cedar/db/cedar_database.h"

namespace cedar {
namespace {

using Clock = std::chrono::steady_clock;

uint64_t ElapsedNs(Clock::time_point origin) {
  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      Clock::now() - origin).count();
  return elapsed <= 0 ? 0 : static_cast<uint64_t>(elapsed);
}

uint64_t SaturatingAdd(uint64_t left, uint64_t right) {
  return right > std::numeric_limits<uint64_t>::max() - left
      ? std::numeric_limits<uint64_t>::max()
      : left + right;
}

uint64_t MaintenanceServiceNs(const CedarDatabase* database) {
  uint64_t total = 0;
  for (const HistogramMetricPoint& point :
       database->metrics().HistogramSnapshot()) {
    if (point.definition.name != "cedar_maintenance_latency_ns") continue;
    for (const auto& labeled : point.values) {
      total = SaturatingAdd(total, labeled.second.sum());
    }
  }
  return total;
}

uint64_t SampleServiceNs(const BenchmarkWorkloadResult& result) {
  uint64_t total = 0;
  for (const BenchmarkOperationSample& sample : result.samples) {
    if (sample.completed_ns < sample.started_ns) continue;
    total = SaturatingAdd(total, sample.completed_ns - sample.started_ns);
  }
  return total;
}

void AttachExecutionSamples(
    const std::shared_ptr<TcypherExecutionStats>& stats,
    BenchmarkWorkloadResult* result) {
  if (!stats || result == nullptr) return;
  result->logical_result_bytes = stats->logical_result_bytes;
  result->candidate_intervals = stats->candidate_intervals;
  result->output_intervals = stats->output_intervals;
  result->blob_refs_seen = stats->blob_refs_seen;
  result->blob_payload_reads = stats->blob_payload_reads;
  if (result->candidate_intervals != 0) {
    result->derived_metrics.interval_survival = BenchmarkRatio{
        result->output_intervals, result->candidate_intervals};
  }
  if (result->blob_refs_seen != 0) {
    result->derived_metrics.blob_materialization = BenchmarkRatio{
        result->blob_payload_reads, result->blob_refs_seen};
  }
}

std::optional<Value> ResolveDatasetValue(
    const CedarTgDataset& dataset, const LogicalKey& key, uint64_t valid_time,
    uint64_t snapshot_seq) {
  const TemporalEvent* selected = nullptr;
  for (const TemporalEvent& event : dataset.events) {
    if (event.logical_key() != key || event.valid_from() > valid_time ||
        event.commit_seq() > snapshot_seq) {
      continue;
    }
    if (selected == nullptr || event.valid_from() > selected->valid_from() ||
        (event.valid_from() == selected->valid_from() &&
         event.commit_seq() > selected->commit_seq())) {
      selected = &event;
    }
  }
  if (selected == nullptr || selected->is_delete()) return std::nullopt;
  return selected->value();
}

std::string ResultChecksum(const CedarTgDataset& dataset,
                           BenchmarkWorkloadFamily family,
                           uint64_t work_units) {
  return BlobHashHex(Blake3Hash(
      dataset.dataset_hash + ":" + BenchmarkWorkloadFamilyName(family) +
      ":" + std::to_string(work_units)));
}

std::string BlobProjectionValue(const CedarTgDataset& dataset) {
  const std::string prefix =
      "bench-blob-" + dataset.dataset_hash.substr(0, 16) + "-";
  return prefix + std::string(8192 - prefix.size(), 'b');
}

std::string EscapeTcypherString(const std::string& input) {
  std::string output;
  output.reserve(input.size());
  for (const char character : input) {
    switch (character) {
      case '\\': output.append("\\\\"); break;
      case '\'': output.append("\\\'"); break;
      case '\n': output.append("\\n"); break;
      case '\r': output.append("\\r"); break;
      case '\t': output.append("\\t"); break;
      default: output.push_back(character); break;
    }
  }
  return output;
}

StatusOr<uint64_t> ExecuteCountQuery(
    CedarDatabase* database, const std::string& query,
    const TcypherQueryOptions& options = {}) {
  auto opened = database->ExecuteTcypher(query, options);
  if (!opened.ok()) return opened.status();
  std::unique_ptr<QueryResultStream> stream =
      std::move(opened).ConsumeValueOrDie();
  ResultBatch batch;
  const Status next = stream->Next(&batch);
  if (!next.ok()) return next;
  if (batch.batch().row_count() != 1 || batch.batch().column_count() != 1) {
    return Status::Corruption("benchmark workload", "count query shape is invalid");
  }
  const std::optional<Value> value = batch.batch().ValueAt(0, 0);
  if (!value.has_value() || value->type() != PhysicalType::kInt64) {
    return Status::Corruption("benchmark workload", "count query result is not Int64");
  }
  const int64_t count = std::get<int64_t>(value->data());
  if (count < 0) {
    return Status::Corruption("benchmark workload", "count query returned a negative value");
  }
  const Status exhausted = stream->Next(&batch);
  if (!exhausted.IsNotFound()) {
    return exhausted.ok()
        ? Status::Corruption("benchmark workload", "count query returned extra batches")
        : exhausted;
  }
  const Status terminal = stream->terminal_status();
  if (!terminal.ok()) return terminal;
  return static_cast<uint64_t>(count);
}

StatusOr<uint64_t> ExecuteRowCountQuery(
    CedarDatabase* database, const std::string& query,
    const TcypherQueryOptions& options = {}) {
  auto opened = database->ExecuteTcypher(query, options);
  if (!opened.ok()) return opened.status();
  std::unique_ptr<QueryResultStream> stream =
      std::move(opened).ConsumeValueOrDie();
  uint64_t rows = 0;
  for (;;) {
    ResultBatch batch;
    const Status next = stream->Next(&batch);
    if (next.IsNotFound()) break;
    if (!next.ok()) return next;
    rows += batch.batch().row_count();
  }
  const Status terminal = stream->terminal_status();
  if (!terminal.ok()) return terminal;
  return rows;
}

template <typename Operation>
BenchmarkWorkloadResult RunClosedLoopQuery(
    const CedarTgDataset& dataset, BenchmarkWorkloadFamily family,
    uint64_t expected_work_units, Operation operation) {
  BenchmarkWorkloadResult result;
  result.measurement_mode = "closed_loop";
  BenchmarkOperationSample sample;
  const Clock::time_point origin = Clock::now();
  sample.started_ns = ElapsedNs(origin);
  const StatusOr<uint64_t> actual = operation();
  sample.completed_ns = ElapsedNs(origin);
  if (!actual.ok()) {
    result.terminal_status = actual.status();
    sample.terminal_status = actual.status().ToString();
  } else if (actual.ValueOrDie() != expected_work_units) {
    result.terminal_status = Status::Corruption(
        "benchmark workload", "verified result count differs from Cedar-TG");
    sample.terminal_status = result.terminal_status.ToString();
  } else {
    result.logical_work_units = actual.ValueOrDie();
    result.result_checksum = ResultChecksum(dataset, family, result.logical_work_units);
    result.verified = true;
  }
  result.elapsed_ns = sample.completed_ns;
  result.samples.push_back(std::move(sample));
  return result;
}

BenchmarkWorkloadResult RunHtapBalanced(
    CedarDatabase* database, const CedarTgDataset& dataset,
    const BenchmarkWorkloadConfig& config) {
  BenchmarkWorkloadResult result;
  result.measurement_mode = "mixed";
  const uint64_t vertex_count = dataset.config.vertex_count;
  result.samples.resize(static_cast<size_t>(2 * vertex_count + 3));
  const Clock::time_point origin = Clock::now();
  std::mutex start_mutex;
  std::condition_variable start_ready;
  bool start = false;
  uint32_t waiting = 0;
  const auto wait_for_start = [&] {
    std::unique_lock<std::mutex> lock(start_mutex);
    ++waiting;
    start_ready.notify_all();
    start_ready.wait(lock, [&] { return start; });
  };
  const auto execute = [&](size_t sample_index,
                           const std::function<Status()>& operation) {
    BenchmarkOperationSample& sample = result.samples[sample_index];
    sample.requested_arrival_ns = 0;
    sample.admitted_ns = 0;
    sample.started_ns = ElapsedNs(origin);
    const Status status = operation();
    sample.completed_ns = ElapsedNs(origin);
    sample.terminal_status = status.ok() ? "PASS" : status.ToString();
  };

  std::thread writer([&] {
    wait_for_start();
    for (uint64_t vertex_id = 1; vertex_id <= vertex_count; ++vertex_id) {
      execute(static_cast<size_t>(vertex_id - 1), [&] {
        return database->Put(
            LogicalKey::VertexProperty(vertex_id, 1),
            dataset.config.valid_time_span + 100 + vertex_id,
            config.vertex_property_schema_epoch,
            Value::String(BenchmarkHtapIngestionValue(dataset, vertex_id)));
      });
    }
  });
  std::thread point_reader([&] {
    wait_for_start();
    for (uint64_t vertex_id = 1; vertex_id <= vertex_count; ++vertex_id) {
      execute(static_cast<size_t>(vertex_count + vertex_id - 1), [&] {
        const auto value = database->Get(
            LogicalKey::VertexExistence(vertex_id), 0);
        if (!value.ok()) return value.status();
        return value.ValueOrDie().has_value()
            ? Status::OK()
            : Status::Corruption("benchmark HTAP", "vertex is absent");
      });
    }
  });
  std::thread analytical([&] {
    wait_for_start();
    execute(static_cast<size_t>(2 * vertex_count), [&] {
      TcypherQueryOptions options;
      options.workload_class = TcypherWorkloadClass::kAnalytical;
      const auto count = ExecuteCountQuery(
          database,
          "FOR VALID_TIME AS OF " +
              std::to_string(dataset.config.valid_time_span) +
              " MATCH (n) RETURN COUNT(n);",
          options);
      if (!count.ok()) return count.status();
      return count.ValueOrDie() == vertex_count
          ? Status::OK()
          : Status::Corruption("benchmark HTAP", "analytical count differs");
    });
  });
  std::thread maintenance([&] {
    wait_for_start();
    execute(static_cast<size_t>(2 * vertex_count + 1),
            [&] { return database->Flush(); });
    execute(static_cast<size_t>(2 * vertex_count + 2),
            [&] { return database->Compact(); });
  });
  {
    std::unique_lock<std::mutex> lock(start_mutex);
    start_ready.wait(lock, [&] { return waiting == 4; });
    start = true;
  }
  start_ready.notify_all();
  writer.join();
  point_reader.join();
  analytical.join();
  maintenance.join();
  result.elapsed_ns = ElapsedNs(origin);
  result.logical_work_units = static_cast<uint64_t>(std::count_if(
      result.samples.begin(), result.samples.end(),
      [](const BenchmarkOperationSample& sample) {
        return sample.terminal_status == "PASS";
      }));
  if (result.logical_work_units != result.samples.size()) {
    result.terminal_status = Status::Corruption(
        "benchmark HTAP", "one or more concurrent operations failed");
    return result;
  }
  const auto visible = database->visible_seq();
  if (!visible.ok()) {
    result.terminal_status = visible.status();
    return result;
  }
  const uint64_t snapshot_seq = visible.ValueOrDie();
  for (uint64_t vertex_id = 1; vertex_id <= vertex_count; ++vertex_id) {
    const auto value = database->Get(
        LogicalKey::VertexProperty(vertex_id, 1),
        std::numeric_limits<uint64_t>::max(), snapshot_seq);
    if (!value.ok() || value.ValueOrDie() != std::optional<Value>(
            Value::String(BenchmarkHtapIngestionValue(dataset, vertex_id)))) {
      result.terminal_status = value.ok()
          ? Status::Corruption("benchmark HTAP", "writer verification failed")
          : value.status();
      return result;
    }
  }
  result.verified = true;
  result.result_checksum =
      ResultChecksum(dataset, config.family, result.logical_work_units);
  return result;
}

}  // namespace

const char* BenchmarkWorkloadFamilyName(BenchmarkWorkloadFamily family) {
  switch (family) {
    case BenchmarkWorkloadFamily::kPointRead: return "point-read";
    case BenchmarkWorkloadFamily::kBitemporalPointRead: return "bitemporal-point-read";
    case BenchmarkWorkloadFamily::kAnalyticalVertexCount: return "analytical-vertex-count";
    case BenchmarkWorkloadFamily::kValidTimeRange: return "valid-time-range";
    case BenchmarkWorkloadFamily::kGraphOneHop: return "graph-one-hop";
    case BenchmarkWorkloadFamily::kBlobProjection: return "blob-projection";
    case BenchmarkWorkloadFamily::kDurableIngestion: return "durable-ingestion";
    case BenchmarkWorkloadFamily::kIndexEquality: return "index-equality";
    case BenchmarkWorkloadFamily::kMaintenanceCycle: return "maintenance-cycle";
    case BenchmarkWorkloadFamily::kHtapBalanced: return "htap-balanced";
    case BenchmarkWorkloadFamily::kRecovery: return "recovery";
  }
  return "unknown";
}

std::string BenchmarkDurableIngestionValue(
    const CedarTgDataset& dataset, uint64_t vertex_id) {
  return "bench-ingest-" + dataset.dataset_hash.substr(0, 16) + "-" +
      std::to_string(vertex_id);
}

std::string BenchmarkHtapIngestionValue(
    const CedarTgDataset& dataset, uint64_t vertex_id) {
  return "bench-htap-" + dataset.dataset_hash.substr(0, 16) + "-" +
      std::to_string(vertex_id);
}

std::string BenchmarkRecoveryValue(
    const CedarTgDataset& dataset, uint64_t vertex_id) {
  return "bench-recovery-" + dataset.dataset_hash.substr(0, 16) + "-" +
      std::to_string(vertex_id);
}

StatusOr<BenchmarkWorkloadFamily> ParseBenchmarkWorkloadFamily(
    const std::string& name) {
  for (const BenchmarkWorkloadFamily family : {
           BenchmarkWorkloadFamily::kPointRead,
           BenchmarkWorkloadFamily::kBitemporalPointRead,
           BenchmarkWorkloadFamily::kAnalyticalVertexCount,
           BenchmarkWorkloadFamily::kValidTimeRange,
           BenchmarkWorkloadFamily::kGraphOneHop,
           BenchmarkWorkloadFamily::kBlobProjection,
           BenchmarkWorkloadFamily::kDurableIngestion,
           BenchmarkWorkloadFamily::kIndexEquality,
           BenchmarkWorkloadFamily::kMaintenanceCycle,
           BenchmarkWorkloadFamily::kHtapBalanced,
           BenchmarkWorkloadFamily::kRecovery}) {
    if (name == BenchmarkWorkloadFamilyName(family)) return family;
  }
  return Status::InvalidArgument("benchmark workload", "unknown workload family");
}

Status PrepareBenchmarkWorkload(
    CedarDatabase* database, const CedarTgDataset& dataset,
    const BenchmarkWorkloadConfig& config) {
  if (database == nullptr || dataset.config.vertex_count == 0) {
    return Status::InvalidArgument("benchmark workload",
                                   "open database and Cedar-TG dataset are required");
  }
  if (config.family == BenchmarkWorkloadFamily::kHtapBalanced) {
    if (config.vertex_property_schema_epoch == 0) {
      return Status::InvalidArgument(
          "benchmark workload", "HTAP workload requires property schema epoch");
    }
    return database->Flush();
  }
  if (config.family == BenchmarkWorkloadFamily::kBlobProjection) {
    ColumnSchema registered;
    const Status registered_status = database->RegisterColumn(
        ColumnSchema{EntityType::Vertex, 2, 0, "blob_payload",
                     PhysicalType::kString, 8,
                     EncodingPolicy::kAdaptive, CompressionPolicy::kLz4},
        &registered);
    if (!registered_status.ok()) return registered_status;
    const Status put = database->Put(
        LogicalKey::VertexProperty(1, 2),
        dataset.config.valid_time_span,
        registered.schema_epoch,
        Value::String(BlobProjectionValue(dataset)));
    return put.ok() ? database->Flush() : put;
  }
  if (config.family == BenchmarkWorkloadFamily::kPointRead ||
      config.family == BenchmarkWorkloadFamily::kBitemporalPointRead ||
      config.family == BenchmarkWorkloadFamily::kAnalyticalVertexCount ||
      config.family == BenchmarkWorkloadFamily::kValidTimeRange ||
      config.family == BenchmarkWorkloadFamily::kGraphOneHop) {
    return database->Flush();
  }
  if (config.family != BenchmarkWorkloadFamily::kIndexEquality) {
    return Status::OK();
  }
  if (config.vertex_property_schema_epoch == 0) {
    return Status::InvalidArgument(
        "benchmark workload", "index workload requires property schema epoch");
  }
  Status status = database->Flush();
  if (!status.ok()) return status;
  IndexDefinition definition;
  definition.entity_type = EntityType::Vertex;
  definition.column_id = 1;
  definition.schema_epoch = config.vertex_property_schema_epoch;
  definition.capabilities = kIndexEquality | kIndexOrderedRange | kIndexPrefix;
  definition.canonical_encoding_id = kIndexCanonicalEncoding;
  uint64_t index_id = 0;
  status = database->RegisterIndex(definition, &index_id);
  if (!status.ok()) return status;
  status = database->SetIndexState(index_id, IndexState::kBuilding);
  if (!status.ok()) return status;
  return database->SetIndexState(index_id, IndexState::kActive);
}

Status WarmBenchmarkWorkingSet(CedarDatabase* database,
                               const CedarTgDataset& dataset) {
  if (database == nullptr || dataset.events.empty()) {
    return Status::InvalidArgument(
        "benchmark workload", "open database and non-empty dataset are required");
  }
  const auto visible = database->visible_seq();
  if (!visible.ok()) return visible.status();
  const uint64_t snapshot_seq = visible.ValueOrDie();
  for (const TemporalEvent& event : dataset.events) {
    const auto value = database->Get(
        event.logical_key(), std::numeric_limits<uint64_t>::max(),
        snapshot_seq);
    if (!value.ok()) return value.status();
  }
  return Status::OK();
}

StatusOr<BenchmarkWorkloadResult> RunBenchmarkWorkloadImpl(
    CedarDatabase* database, const CedarTgDataset& dataset,
    const BenchmarkWorkloadConfig& config) {
  if (database == nullptr || dataset.config.vertex_count == 0 ||
      dataset.dataset_hash.empty()) {
    return Status::InvalidArgument("benchmark workload",
                                   "open database and Cedar-TG dataset are required");
  }
  if (config.family == BenchmarkWorkloadFamily::kAnalyticalVertexCount) {
    const std::string query = "FOR VALID_TIME AS OF " +
        std::to_string(dataset.config.valid_time_span) +
        " MATCH (n) RETURN COUNT(n);";
    auto stats = std::make_shared<TcypherExecutionStats>();
    TcypherQueryOptions options;
    options.execution_stats = stats;
    BenchmarkWorkloadResult result = RunClosedLoopQuery(
        dataset, config.family, dataset.config.vertex_count,
        [&] { return ExecuteCountQuery(database, query, options); });
    AttachExecutionSamples(stats, &result);
    return result;
  }
  if (config.family == BenchmarkWorkloadFamily::kValidTimeRange) {
    const std::string query = "FOR VALID_TIME BETWEEN 0 AND " +
        std::to_string(dataset.config.valid_time_span) +
        " MATCH (n) RETURN n;";
    auto stats = std::make_shared<TcypherExecutionStats>();
    TcypherQueryOptions options;
    options.execution_stats = stats;
    BenchmarkWorkloadResult result = RunClosedLoopQuery(
        dataset, config.family, dataset.config.vertex_count,
        [&] { return ExecuteRowCountQuery(database, query, options); });
    AttachExecutionSamples(stats, &result);
    return result;
  }
  if (config.family == BenchmarkWorkloadFamily::kGraphOneHop) {
    const std::string query = "FOR VALID_TIME AS OF " +
        std::to_string(dataset.config.valid_time_span) +
        " MATCH (a)-[r]->(b) RETURN r;";
    auto stats = std::make_shared<TcypherExecutionStats>();
    TcypherQueryOptions options;
    options.execution_stats = stats;
    BenchmarkWorkloadResult result = RunClosedLoopQuery(
        dataset, config.family, dataset.config.edge_count,
        [&] { return ExecuteRowCountQuery(database, query, options); });
    AttachExecutionSamples(stats, &result);
    return result;
  }
  if (config.family == BenchmarkWorkloadFamily::kBlobProjection) {
    const std::string query = "FOR VALID_TIME AS OF " +
        std::to_string(dataset.config.valid_time_span) +
        " MATCH (n {id: 1}) RETURN n.blob_payload;";
    auto stats = std::make_shared<TcypherExecutionStats>();
    TcypherQueryOptions options;
    options.statement_start_valid_time = dataset.config.valid_time_span;
    options.execution_stats = stats;
    BenchmarkWorkloadResult result = RunClosedLoopQuery(
        dataset, config.family, 1,
        [&] { return ExecuteRowCountQuery(database, query, options); });
    AttachExecutionSamples(stats, &result);
    return result;
  }
  if (config.family == BenchmarkWorkloadFamily::kIndexEquality) {
    const auto visible = database->visible_seq();
    if (!visible.ok()) return visible.status();
    const uint64_t snapshot_seq = visible.ValueOrDie();
    const std::optional<Value> target = ResolveDatasetValue(
        dataset, LogicalKey::VertexProperty(1, 1),
        dataset.config.valid_time_span, snapshot_seq);
    if (!target.has_value() || target->type() != PhysicalType::kString) {
      return Status::Corruption(
          "benchmark workload", "Cedar-TG index target is not a string");
    }
    const std::string& literal = std::get<std::string>(target->data());
    uint64_t expected_rows = 0;
    for (uint64_t vertex_id = 1;
         vertex_id <= dataset.config.vertex_count; ++vertex_id) {
      const auto value = database->Get(
          LogicalKey::VertexProperty(vertex_id, 1),
          dataset.config.valid_time_span, snapshot_seq);
      if (!value.ok()) return value.status();
      if (value.ValueOrDie() == target) ++expected_rows;
    }
    const std::string query = "FOR VALID_TIME AS OF " +
        std::to_string(dataset.config.valid_time_span) +
        " MATCH (n) WHERE n.value = '" + EscapeTcypherString(literal) +
        "' RETURN n;";
    auto stats = std::make_shared<TcypherExecutionStats>();
    TcypherQueryOptions options;
    options.statement_start_valid_time = dataset.config.valid_time_span;
    options.execution_stats = stats;
    BenchmarkWorkloadResult result = RunClosedLoopQuery(
        dataset, config.family, expected_rows,
        [&] { return ExecuteRowCountQuery(database, query, options); });
    if (result.verified) {
      result.derived_metrics.index_survival = BenchmarkRatio{
          result.logical_work_units, stats->index_candidate_entity_count};
    }
    AttachExecutionSamples(stats, &result);
    return result;
  }
  if (config.family == BenchmarkWorkloadFamily::kMaintenanceCycle) {
    BenchmarkWorkloadResult result;
    result.measurement_mode = "closed_loop";
    const Clock::time_point origin = Clock::now();
    const std::vector<std::function<Status()>> operations = {
        [&] { return database->Flush(); },
        [&] { return database->Compact(); },
        [&] { return database->RotateBlobSegments(); },
        [&] { return database->CollectBlobGarbage(); },
        [&] { return database->Checkpoint(); },
    };
    for (const auto& operation : operations) {
      BenchmarkOperationSample sample;
      sample.requested_arrival_ns = ElapsedNs(origin);
      sample.admitted_ns = sample.requested_arrival_ns;
      sample.started_ns = sample.requested_arrival_ns;
      const Status status = operation();
      sample.completed_ns = ElapsedNs(origin);
      sample.terminal_status = status.ok() ? "PASS" : status.ToString();
      if (status.ok()) {
        ++result.logical_work_units;
      } else if (result.terminal_status.ok()) {
        result.terminal_status = status;
      }
      result.samples.push_back(std::move(sample));
    }
    result.elapsed_ns = ElapsedNs(origin);
    result.verified = result.terminal_status.ok() &&
        result.logical_work_units == operations.size();
    if (result.verified) {
      result.result_checksum =
          ResultChecksum(dataset, config.family, result.logical_work_units);
    }
    return result;
  }
  if (config.family == BenchmarkWorkloadFamily::kRecovery) {
    BenchmarkWorkloadResult result;
    result.measurement_mode = "closed_loop";
    const Clock::time_point origin = Clock::now();
    BenchmarkOperationSample write;
    write.started_ns = ElapsedNs(origin);
    const Status write_status = config.vertex_property_schema_epoch == 0
        ? Status::InvalidArgument("benchmark workload",
                                  "recovery requires property schema epoch")
        : database->Put(
              LogicalKey::VertexProperty(dataset.config.vertex_count, 1),
              dataset.config.valid_time_span + 100,
              config.vertex_property_schema_epoch,
              Value::String(BenchmarkRecoveryValue(
                  dataset, dataset.config.vertex_count)));
    write.completed_ns = ElapsedNs(origin);
    write.terminal_status = write_status.ok() ? "PASS" : write_status.ToString();
    result.samples.push_back(std::move(write));
    if (!write_status.ok()) {
      result.terminal_status = write_status;
      result.elapsed_ns = ElapsedNs(origin);
      return result;
    }
    BenchmarkOperationSample checkpoint;
    checkpoint.started_ns = ElapsedNs(origin);
    const Status checkpoint_status = database->Checkpoint();
    checkpoint.completed_ns = ElapsedNs(origin);
    checkpoint.terminal_status = checkpoint_status.ok()
        ? "PASS" : checkpoint_status.ToString();
    result.samples.push_back(std::move(checkpoint));
    result.elapsed_ns = ElapsedNs(origin);
    result.terminal_status = checkpoint_status;
    if (checkpoint_status.ok()) {
      result.logical_work_units = 2;
      result.verified = true;
      result.result_checksum = ResultChecksum(
          dataset, config.family, result.logical_work_units);
    }
    return result;
  }
  if (config.family == BenchmarkWorkloadFamily::kHtapBalanced) {
    return RunHtapBalanced(database, dataset, config);
  }

  const auto visible = database->visible_seq();
  if (!visible.ok()) return visible.status();
  const uint64_t snapshot_seq = visible.ValueOrDie();
  OpenLoopScheduleConfig schedule;
  schedule.operation_count = dataset.config.vertex_count;
  schedule.arrival_interval_ns = config.arrival_interval_ns;
  schedule.worker_count = config.worker_count;
  schedule.queue_capacity = std::max<uint32_t>(
      1, std::min<uint32_t>(config.queue_capacity,
                            static_cast<uint32_t>(std::min<uint64_t>(
                                dataset.config.vertex_count,
                                std::numeric_limits<uint32_t>::max()))));
  const auto scheduled = RunOpenLoopSchedule(
      schedule, [&](uint64_t operation_id) {
        const uint64_t vertex_id = operation_id + 1;
        if (config.family == BenchmarkWorkloadFamily::kPointRead) {
          const auto actual = database->Get(
              LogicalKey::VertexExistence(vertex_id), 0, snapshot_seq);
          if (!actual.ok()) return actual.status();
          return actual.ValueOrDie().has_value()
              ? Status::OK()
              : Status::Corruption("benchmark workload", "vertex is absent");
        }
        if (config.family == BenchmarkWorkloadFamily::kDurableIngestion) {
          if (config.vertex_property_schema_epoch == 0) {
            return Status::InvalidArgument(
                "benchmark workload", "ingestion requires property schema epoch");
          }
          return database->Put(
              LogicalKey::VertexProperty(vertex_id, 1),
              dataset.config.valid_time_span + 1 + operation_id % 3,
              config.vertex_property_schema_epoch,
              Value::String(BenchmarkDurableIngestionValue(dataset, vertex_id)));
        }
        const uint64_t operation_snapshot = snapshot_seq == 0
            ? 0 : (operation_id % snapshot_seq) + 1;
        const uint64_t valid_time = dataset.config.valid_time_span == 0
            ? 0 : operation_id % dataset.config.valid_time_span;
        const LogicalKey key = LogicalKey::VertexProperty(vertex_id, 1);
        const std::optional<Value> expected = ResolveDatasetValue(
            dataset, key, valid_time, operation_snapshot);
        const auto actual = database->Get(key, valid_time, operation_snapshot);
        if (!actual.ok()) return actual.status();
        return actual.ValueOrDie() == expected
            ? Status::OK()
            : Status::Corruption("benchmark workload",
                                 "bitemporal read differs from Cedar-TG oracle");
      });
  if (!scheduled.ok()) return scheduled.status();
  BenchmarkWorkloadResult result;
  result.measurement_mode = "open_loop";
  result.samples = scheduled.ValueOrDie().samples;
  result.elapsed_ns = scheduled.ValueOrDie().elapsed_ns;
  result.terminal_status = scheduled.ValueOrDie().terminal_status;
  result.logical_work_units = static_cast<uint64_t>(std::count_if(
      result.samples.begin(), result.samples.end(),
      [](const BenchmarkOperationSample& sample) {
        return sample.terminal_status == "PASS";
      }));
  result.verified = result.terminal_status.ok() &&
      result.logical_work_units == dataset.config.vertex_count;
  if (result.verified &&
      config.family == BenchmarkWorkloadFamily::kDurableIngestion) {
    const auto visible_after = database->visible_seq();
    if (!visible_after.ok()) return visible_after.status();
    const uint64_t verification_snapshot = visible_after.ValueOrDie();
    for (uint64_t vertex_id = 1;
         vertex_id <= dataset.config.vertex_count; ++vertex_id) {
      const auto value = database->Get(
          LogicalKey::VertexProperty(vertex_id, 1),
          std::numeric_limits<uint64_t>::max(), verification_snapshot);
      if (!value.ok() || value.ValueOrDie() != std::optional<Value>(
              Value::String(BenchmarkDurableIngestionValue(dataset, vertex_id)))) {
        result.verified = false;
        result.terminal_status = value.ok()
            ? Status::Corruption("benchmark workload",
                                 "durable ingestion verification failed")
            : value.status();
        break;
      }
    }
  }
  if (result.verified) {
    result.result_checksum =
        ResultChecksum(dataset, config.family, result.logical_work_units);
  }
  return result;
}

StatusOr<BenchmarkWorkloadResult> RunBenchmarkWorkload(
    CedarDatabase* database, const CedarTgDataset& dataset,
    const BenchmarkWorkloadConfig& config) {
  CacheStats before;
  StorageRuntimeStats storage_before;
  BenchmarkStorageStats benchmark_before;
  TransactionMeasurementSnapshot transaction_measurements_before;
  uint64_t maintenance_before = 0;
  if (database != nullptr) {
    const auto cache_snapshot = database->cache_stats();
    if (!cache_snapshot.ok()) return cache_snapshot.status();
    before = cache_snapshot.ValueOrDie();
    const auto storage_snapshot = database->storage_stats();
    if (!storage_snapshot.ok()) return storage_snapshot.status();
    storage_before = storage_snapshot.ValueOrDie();
    const auto snapshot = database->benchmark_storage_stats();
    if (!snapshot.ok()) return snapshot.status();
    benchmark_before = snapshot.ValueOrDie();
    const auto transaction_snapshot = database->transaction_measurements();
    if (!transaction_snapshot.ok()) return transaction_snapshot.status();
    transaction_measurements_before = transaction_snapshot.ValueOrDie();
    maintenance_before = MaintenanceServiceNs(database);
  }
  auto result = RunBenchmarkWorkloadImpl(database, dataset, config);
  if (!result.ok() || database == nullptr) return result;
  const auto cache_snapshot = database->cache_stats();
  if (!cache_snapshot.ok()) return cache_snapshot.status();
  const CacheStats after = cache_snapshot.ValueOrDie();
  const auto storage_snapshot = database->storage_stats();
  if (!storage_snapshot.ok()) return storage_snapshot.status();
  const StorageRuntimeStats storage_after = storage_snapshot.ValueOrDie();
  const auto benchmark_snapshot = database->benchmark_storage_stats(true);
  if (!benchmark_snapshot.ok()) return benchmark_snapshot.status();
  const BenchmarkStorageStats benchmark_after = benchmark_snapshot.ValueOrDie();
  const auto transaction_snapshot = database->transaction_measurements();
  if (!transaction_snapshot.ok()) return transaction_snapshot.status();
  const auto transaction_window = BuildTransactionMeasurementWindow(
      transaction_measurements_before, transaction_snapshot.ValueOrDie());
  if (!transaction_window.ok()) return transaction_window.status();
  result.ValueOrDie().transaction_measurements = transaction_window.ValueOrDie();
  if (result.ValueOrDie().logical_result_bytes != 0 &&
      storage_after.sst_physical_bytes_read >=
          storage_before.sst_physical_bytes_read &&
      storage_after.blob_payload_bytes_read >=
          storage_before.blob_payload_bytes_read) {
    const uint64_t sst_read = storage_after.sst_physical_bytes_read -
        storage_before.sst_physical_bytes_read;
    const uint64_t blob_read = storage_after.blob_payload_bytes_read -
        storage_before.blob_payload_bytes_read;
    result.ValueOrDie().derived_metrics.read_amplification = BenchmarkRatio{
        SaturatingAdd(sst_read, blob_read),
        result.ValueOrDie().logical_result_bytes};
    result.ValueOrDie().physical_read_bytes = SaturatingAdd(sst_read, blob_read);
    result.ValueOrDie().physical_read_bytes_available = true;
  }
  const uint64_t physical_before =
      benchmark_before.physical_durable_bytes_written();
  const uint64_t physical_after =
      benchmark_after.physical_durable_bytes_written();
  if (physical_after >= physical_before &&
      benchmark_after.logical_committed_bytes >=
          benchmark_before.logical_committed_bytes) {
    result.ValueOrDie().physical_write_bytes = physical_after - physical_before;
    result.ValueOrDie().physical_write_bytes_available = true;
    const uint64_t logical_committed =
        benchmark_after.logical_committed_bytes -
        benchmark_before.logical_committed_bytes;
    if (logical_committed != 0) {
      result.ValueOrDie().derived_metrics.write_amplification = BenchmarkRatio{
          physical_after - physical_before, logical_committed};
    }
  }
  const auto delta = [](uint64_t before_value, uint64_t after_value) {
    return after_value >= before_value ? after_value - before_value
                                       : after_value;
  };
  result.ValueOrDie().durable_write_bytes = BenchmarkDurableWriteBytes{
      delta(benchmark_before.wal_bytes_written,
            benchmark_after.wal_bytes_written),
      delta(benchmark_before.decision_log_bytes_written,
            benchmark_after.decision_log_bytes_written),
      delta(benchmark_before.sst_flush_bytes_written,
            benchmark_after.sst_flush_bytes_written),
      delta(benchmark_before.compaction_bytes_written,
            benchmark_after.compaction_bytes_written),
      delta(benchmark_before.blob_bytes_written,
            benchmark_after.blob_bytes_written),
      delta(benchmark_before.manifest_bytes_written,
            benchmark_after.manifest_bytes_written)};
  if (benchmark_after.logical_live_bytes != 0) {
    result.ValueOrDie().derived_metrics.space_amplification = BenchmarkRatio{
        benchmark_after.live_physical_bytes,
        benchmark_after.logical_live_bytes};
  }
  if (after.page_insert_requests >= before.page_insert_requests &&
      after.page_admissions >= before.page_admissions) {
    const uint64_t requests =
        after.page_insert_requests - before.page_insert_requests;
    if (requests != 0) {
      result.ValueOrDie().derived_metrics.cache_admission = BenchmarkRatio{
          after.page_admissions - before.page_admissions, requests};
    }
  }
  const uint64_t maintenance_after = MaintenanceServiceNs(database);
  const uint64_t total_service = SampleServiceNs(result.ValueOrDie());
  if (maintenance_after >= maintenance_before && total_service != 0) {
    const uint64_t maintenance_service =
        maintenance_after - maintenance_before;
    if (maintenance_service != 0 && maintenance_service <= total_service) {
      result.ValueOrDie().derived_metrics.maintenance_share = BenchmarkRatio{
          maintenance_service, total_service};
    }
  }
  return result;
}

}  // namespace cedar
