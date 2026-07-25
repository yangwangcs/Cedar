// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/workload_driver.h"

#include <algorithm>
#include <array>
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

class BenchmarkSchedulerCampaignAccess {
 public:
  static WorkExecutionService* ExecutionService(CedarDatabase* database) {
    return database == nullptr ? nullptr
                               : database->work_execution_service_.get();
  }
};

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
  result.samples.resize(static_cast<size_t>(2 * vertex_count + 4));
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
  execute(static_cast<size_t>(2 * vertex_count + 3), [&] {
    auto first = database->CreateTcypherSession();
    if (!first.ok()) return first.status();
    auto second = database->CreateTcypherSession();
    if (!second.ok()) return second.status();
    Status status = first.ValueOrDie()->Begin(TcypherSessionMode::kStrict);
    if (!status.ok()) return status;
    status = second.ValueOrDie()->Begin(TcypherSessionMode::kStrict);
    if (!status.ok()) return status;
    const uint64_t conflict_time = dataset.config.valid_time_span + 10'000;
    const LogicalKey first_key = LogicalKey::VertexProperty(1, 1);
    const LogicalKey second_key = LogicalKey::VertexProperty(
        vertex_count > 1 ? 2 : 1, 1);
    status = first.ValueOrDie()->RecordRead(
        TransactionCoordinator::StrictReadPoint(second_key, conflict_time));
    if (!status.ok()) return status;
    status = second.ValueOrDie()->RecordRead(
        TransactionCoordinator::StrictReadPoint(first_key, conflict_time));
    if (!status.ok()) return status;
    status = first.ValueOrDie()->Stage({PendingEvent::Put(
        first_key, conflict_time, config.vertex_property_schema_epoch,
        Value::String("bench-htap-strict-first"))});
    if (!status.ok()) return status;
    status = second.ValueOrDie()->Stage({PendingEvent::Put(
        second_key, conflict_time, config.vertex_property_schema_epoch,
        Value::String("bench-htap-strict-second"))});
    if (!status.ok()) return status;
    uint64_t first_commit_seq = 0;
    status = first.ValueOrDie()->Commit(&first_commit_seq);
    if (!status.ok()) return status;
    uint64_t second_commit_seq = 0;
    const Status rejected = second.ValueOrDie()->Commit(&second_commit_seq);
    if (!rejected.IsConflict()) {
      return rejected.ok()
          ? Status::Corruption(
                "benchmark HTAP", "strict conflict was not rejected")
          : rejected;
    }
    return first_commit_seq != 0 && second_commit_seq == 0
        ? Status::OK()
        : Status::Corruption(
              "benchmark HTAP", "strict conflict commit sequence mismatch");
  });
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
    const Value expected = vertex_id == 1
        ? Value::String("bench-htap-strict-first")
        : Value::String(BenchmarkHtapIngestionValue(dataset, vertex_id));
    const auto value = database->Get(
        LogicalKey::VertexProperty(vertex_id, 1),
        std::numeric_limits<uint64_t>::max(), snapshot_seq);
    if (!value.ok() || value.ValueOrDie() != std::optional<Value>(expected)) {
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

enum class RequiredAccessPath : uint8_t {
  kBase,
  kIndex,
  kHybrid,
  kIntersection,
};

CandidateSource CandidateSourceFor(RequiredAccessPath path) {
  switch (path) {
    case RequiredAccessPath::kBase: return CandidateSource::kBase;
    case RequiredAccessPath::kIndex: return CandidateSource::kIndex;
    case RequiredAccessPath::kHybrid: return CandidateSource::kHybrid;
    case RequiredAccessPath::kIntersection:
      return CandidateSource::kIntersection;
  }
  return CandidateSource::kBase;
}

Status VerifyExecutedAccessPath(
    const TcypherExecutionStats& stats, RequiredAccessPath required) {
  const CandidateSource expected = CandidateSourceFor(required);
  if (!stats.has_selected_access_path ||
      stats.selected_access_path != expected ||
      !stats.has_executed_access_path ||
      stats.executed_access_path != expected) {
    return Status::Corruption(
        "benchmark index path matrix",
        "required=" + std::to_string(static_cast<uint8_t>(expected)) +
            " selected_present=" +
            std::to_string(stats.has_selected_access_path) +
            " selected=" + std::to_string(static_cast<uint8_t>(
                stats.selected_access_path)) +
            " executed_present=" +
            std::to_string(stats.has_executed_access_path) +
            " executed=" + std::to_string(static_cast<uint8_t>(
                stats.executed_access_path)) +
            " candidates=" +
            std::to_string(stats.index_candidate_entity_count));
  }
  return Status::OK();
}

Status VerifyExecutedGraphOrder(
    const TcypherExecutionStats& stats, GraphOrder required) {
  if (!stats.has_selected_graph_order ||
      stats.selected_graph_order != required ||
      !stats.has_executed_graph_order ||
      stats.executed_graph_order != required) {
    return Status::Corruption(
        "benchmark index path matrix",
        "required=" + std::to_string(static_cast<uint8_t>(required)) +
            " selected_present=" +
            std::to_string(stats.has_selected_graph_order) +
            " selected=" + std::to_string(static_cast<uint8_t>(
                stats.selected_graph_order)) +
            " executed_present=" +
            std::to_string(stats.has_executed_graph_order) +
            " executed=" + std::to_string(static_cast<uint8_t>(
                stats.executed_graph_order)));
  }
  return Status::OK();
}

BenchmarkWorkloadResult RunIndexPathMatrix(
    CedarDatabase* database, const CedarTgDataset& dataset) {
  BenchmarkWorkloadResult result;
  result.measurement_mode = "closed_loop";
  const Clock::time_point origin = Clock::now();
  uint64_t indexed_output_rows = 0;
  uint64_t indexed_candidate_rows = 0;
  const auto run_query = [&](
      const std::string& query, uint64_t expected_rows,
      std::optional<RequiredAccessPath> access_path,
      std::optional<GraphOrder> graph_order,
      TcypherSession* session = nullptr) {
    BenchmarkOperationSample sample;
    sample.requested_arrival_ns = ElapsedNs(origin);
    sample.admitted_ns = sample.requested_arrival_ns;
    sample.started_ns = sample.requested_arrival_ns;
    auto stats = std::make_shared<TcypherExecutionStats>();
    TcypherQueryOptions options;
    options.statement_start_valid_time = 0;
    options.execution_stats = stats;
    Status status = Status::OK();
    StatusOr<uint64_t> rows = session == nullptr
        ? ExecuteRowCountQuery(database, query, options)
        : [&]() -> StatusOr<uint64_t> {
            auto opened = database->ExecuteTcypher(*session, query, options);
            if (!opened.ok()) return opened.status();
            std::unique_ptr<QueryResultStream> stream =
                std::move(opened).ConsumeValueOrDie();
            uint64_t row_count = 0;
            for (;;) {
              ResultBatch batch;
              const Status next = stream->Next(&batch);
              if (next.IsNotFound()) break;
              if (!next.ok()) return next;
              row_count = SaturatingAdd(
                  row_count, batch.batch().row_count());
            }
            const Status terminal = stream->terminal_status();
            return terminal.ok() ? StatusOr<uint64_t>(row_count)
                                 : StatusOr<uint64_t>(terminal);
          }();
    if (!rows.ok()) {
      status = rows.status();
    } else if (rows.ValueOrDie() != expected_rows) {
      status = Status::Corruption(
          "benchmark index path matrix",
          "query result count expected=" + std::to_string(expected_rows) +
              " actual=" + std::to_string(rows.ValueOrDie()));
    } else if (access_path.has_value()) {
      status = VerifyExecutedAccessPath(*stats, *access_path);
    }
    if (status.ok() && graph_order.has_value()) {
      status = VerifyExecutedGraphOrder(*stats, *graph_order);
    }
    sample.completed_ns = ElapsedNs(origin);
    sample.terminal_status = status.ok() ? "PASS" : status.ToString();
    result.samples.push_back(std::move(sample));
    if (!status.ok()) {
      if (result.terminal_status.ok()) result.terminal_status = status;
      return;
    }
    ++result.logical_work_units;
    if (access_path.has_value() &&
        *access_path != RequiredAccessPath::kBase) {
      indexed_output_rows = SaturatingAdd(indexed_output_rows, expected_rows);
    }
    indexed_candidate_rows = SaturatingAdd(
        indexed_candidate_rows, stats->index_candidate_entity_count);
  };

  const uint64_t matrix_vertex_count =
      std::max<uint64_t>(dataset.config.vertex_count, 256);
  run_query("MATCH (n) WHERE n.indexed_name = 'Other' RETURN n;",
            matrix_vertex_count - 1,
            RequiredAccessPath::kBase, std::nullopt);
  run_query("MATCH (n) WHERE n.indexed_name = 'target' RETURN n;", 1,
            RequiredAccessPath::kIndex, std::nullopt);
  auto session = database->CreateTcypherSession();
  if (!session.ok()) {
    result.terminal_status = session.status();
  } else {
    Status status = session.ValueOrDie()->Begin(TcypherSessionMode::kSnapshot);
    if (status.ok()) {
      auto staged = database->ExecuteTcypher(
          *session.ValueOrDie(),
          "MATCH (n {id: 2}) SET n.hybrid_name = 'target' VALID FROM 0;");
      status = staged.ok() ? Status::OK() : staged.status();
    }
    if (status.ok()) {
      run_query("MATCH (n) WHERE n.hybrid_name = 'target' RETURN n;", 2,
                RequiredAccessPath::kHybrid, std::nullopt,
                session.ValueOrDie().get());
    } else if (result.terminal_status.ok()) {
      result.terminal_status = status;
    }
    const Status rollback = session.ValueOrDie()->Rollback();
    if (!rollback.ok() && result.terminal_status.ok()) {
      result.terminal_status = rollback;
    }
  }
  run_query(
      "MATCH (n) WHERE n.left_key = 'Ada' AND "
      "n.right_key = 'Paris' RETURN n;",
      2, RequiredAccessPath::kIntersection, std::nullopt);
  run_query(
      "MATCH (a) MATCH (b) WHERE a.graph_name = 'target' AND "
      "a.graph_city = b.graph_code RETURN a, b;",
      1, std::nullopt, GraphOrder::kIndexFirst);
  run_query(
      "MATCH (a) MATCH (b) MATCH (c) WHERE "
      "a.graph_city = b.graph_code AND "
      "b.graph_city = c.graph_code RETURN a, b, c;",
      1, std::nullopt, GraphOrder::kAdjacencyFirst);

  result.elapsed_ns = ElapsedNs(origin);
  result.verified = result.terminal_status.ok() &&
      result.logical_work_units == 6 && indexed_candidate_rows != 0;
  if (result.verified) {
    result.derived_metrics.index_survival =
        BenchmarkRatio{indexed_output_rows, indexed_candidate_rows};
    result.result_checksum =
        ResultChecksum(dataset, BenchmarkWorkloadFamily::kIndexPathMatrix,
                       result.logical_work_units);
  } else if (result.terminal_status.ok()) {
    result.terminal_status = Status::Corruption(
        "benchmark index path matrix", "path matrix verification incomplete");
  }
  return result;
}

size_t BenchmarkWorkClassIndex(WorkClass work_class) {
  switch (work_class) {
    case WorkClass::kCommitCritical: return 0;
    case WorkClass::kRecovery: return 1;
    case WorkClass::kShutdown: return 2;
    case WorkClass::kForegroundWrite: return 3;
    case WorkClass::kPointRead: return 4;
    case WorkClass::kInteractiveQuery: return 5;
    case WorkClass::kFlush: return 6;
    case WorkClass::kCompactionUrgent: return 7;
    case WorkClass::kAnalyticalQuery: return 8;
    case WorkClass::kCompactionNormal: return 9;
    case WorkClass::kIndexBuild: return 10;
    case WorkClass::kStatsMerge: return 11;
    case WorkClass::kBlobGc: return 12;
  }
  return 12;
}

BenchmarkWorkloadResult RunSchedulerSaturation(
    CedarDatabase* database, const CedarTgDataset& dataset) {
  BenchmarkWorkloadResult result;
  result.measurement_mode = "closed_loop";
  const Clock::time_point origin = Clock::now();
  const auto record = [&](const Status& status) {
    BenchmarkOperationSample sample;
    sample.requested_arrival_ns = ElapsedNs(origin);
    sample.admitted_ns = sample.requested_arrival_ns;
    sample.started_ns = sample.requested_arrival_ns;
    sample.completed_ns = ElapsedNs(origin);
    sample.terminal_status = status.ok() ? "PASS" : status.ToString();
    result.samples.push_back(std::move(sample));
    if (status.ok()) {
      ++result.logical_work_units;
    } else if (result.terminal_status.ok()) {
      result.terminal_status = status;
    }
  };

  WorkScheduler fairness(1'000);
  uint64_t id = 1;
  for (size_t cycle = 0; cycle < 10; ++cycle) {
    for (size_t count = 0; count < 4; ++count) {
      fairness.EnqueueExecutable(WorkClass::kForegroundWrite,
                                 ExecutableTaskId{id++});
    }
    for (size_t count = 0; count < 2; ++count) {
      fairness.EnqueueExecutable(WorkClass::kFlush, ExecutableTaskId{id++});
      fairness.EnqueueExecutable(WorkClass::kAnalyticalQuery,
                                 ExecutableTaskId{id++});
    }
    fairness.EnqueueExecutable(WorkClass::kCompactionNormal,
                               ExecutableTaskId{id++});
  }
  Status fairness_status = Status::OK();
  for (size_t window = 0; window < 10 && fairness_status.ok(); ++window) {
    std::array<uint64_t, 4> counts{};
    for (size_t dispatch = 0; dispatch < 9; ++dispatch) {
      const auto work = fairness.NextExecutableWork();
      if (!work.has_value()) {
        fairness_status = Status::Corruption(
            "benchmark scheduler", "fairness queue drained early");
        break;
      }
      switch (work->work_class) {
        case WorkClass::kForegroundWrite: ++counts[0]; break;
        case WorkClass::kFlush: ++counts[1]; break;
        case WorkClass::kAnalyticalQuery: ++counts[2]; break;
        case WorkClass::kCompactionNormal: ++counts[3]; break;
        default:
          fairness_status = Status::Corruption(
              "benchmark scheduler", "unexpected fairness lane class");
          break;
      }
    }
    if (fairness_status.ok() &&
        counts != std::array<uint64_t, 4>{4, 2, 2, 1}) {
      fairness_status = Status::Corruption(
          "benchmark scheduler", "fairness window differs from 4:2:2:1");
    }
  }
  record(fairness_status);

  WorkScheduler deadlines;
  deadlines.EnqueueExecutable(WorkClass::kInteractiveQuery,
                              ExecutableTaskId{1}, 30);
  deadlines.EnqueueExecutable(WorkClass::kInteractiveQuery,
                              ExecutableTaskId{2}, 10);
  deadlines.EnqueueExecutable(WorkClass::kInteractiveQuery,
                              ExecutableTaskId{3}, 20);
  const std::array<uint64_t, 3> expected_deadlines = {2, 3, 1};
  Status deadline_order = Status::OK();
  for (const uint64_t expected : expected_deadlines) {
    const auto work = deadlines.NextExecutableWork();
    if (!work.has_value() || work->id.value != expected) {
      deadline_order = Status::Corruption(
          "benchmark scheduler", "interactive EDF order differs");
      break;
    }
  }
  record(deadline_order);

  WorkExecutionService* service =
      BenchmarkSchedulerCampaignAccess::ExecutionService(database);
  if (service == nullptr) {
    const Status missing = Status::InvalidArgument(
        "benchmark scheduler", "database execution service is unavailable");
    record(missing);
    record(missing);
    record(missing);
    result.elapsed_ns = ElapsedNs(origin);
    return result;
  }
  std::mutex blocker_mutex;
  std::condition_variable blocker_changed;
  uint32_t blockers_started = 0;
  bool release_blockers = false;
  std::vector<WorkTaskHandle> blocker_handles;
  for (size_t blocker = 0; blocker < 4; ++blocker) {
    auto submitted = service->Submit(
        WorkClass::kAnalyticalQuery, [&] {
          std::unique_lock<std::mutex> lock(blocker_mutex);
          ++blockers_started;
          blocker_changed.notify_all();
          blocker_changed.wait(lock, [&] { return release_blockers; });
          return Status::OK();
        });
    if (!submitted.ok()) {
      record(submitted.status());
      result.elapsed_ns = ElapsedNs(origin);
      return result;
    }
    blocker_handles.push_back(submitted.ValueOrDie());
  }
  {
    std::unique_lock<std::mutex> lock(blocker_mutex);
    if (!blocker_changed.wait_for(
            lock, std::chrono::seconds(5),
            [&] { return blockers_started == 4; })) {
      release_blockers = true;
      blocker_changed.notify_all();
      const Status timed_out = Status::ResourceExhausted(
          "benchmark scheduler", "workers did not saturate");
      record(timed_out);
      result.elapsed_ns = ElapsedNs(origin);
      return result;
    }
  }
  std::vector<WorkTaskHandle> saturated_handles;
  const auto submit_many = [&](WorkClass work_class, size_t count) {
    for (size_t index = 0; index < count; ++index) {
      auto submitted = service->Submit(work_class, [] { return Status::OK(); });
      if (!submitted.ok()) return submitted.status();
      saturated_handles.push_back(submitted.ValueOrDie());
    }
    return Status::OK();
  };
  Status saturation_status = submit_many(WorkClass::kForegroundWrite, 40);
  if (saturation_status.ok()) {
    saturation_status = submit_many(WorkClass::kFlush, 20);
  }
  if (saturation_status.ok()) {
    saturation_status = submit_many(WorkClass::kAnalyticalQuery, 20);
  }
  if (saturation_status.ok()) {
    saturation_status = submit_many(WorkClass::kCompactionNormal, 10);
  }
  StatusOr<WorkTaskHandle> deadline_task = service->Submit(
      WorkClass::kInteractiveQuery, [] { return Status::OK(); }, 1);
  if (!deadline_task.ok() && saturation_status.ok()) {
    saturation_status = deadline_task.status();
  }
  {
    std::lock_guard<std::mutex> lock(blocker_mutex);
    release_blockers = true;
  }
  blocker_changed.notify_all();
  for (const WorkTaskHandle& handle : blocker_handles) {
    const Status waited = handle.Wait();
    if (!waited.ok() && saturation_status.ok()) saturation_status = waited;
  }
  for (const WorkTaskHandle& handle : saturated_handles) {
    const Status waited = handle.Wait();
    if (!waited.ok() && saturation_status.ok()) saturation_status = waited;
  }
  if (deadline_task.ok()) {
    const Status waited = deadline_task.ValueOrDie().Wait();
    if (!waited.ok() && saturation_status.ok()) saturation_status = waited;
  }
  record(saturation_status);

  const WorkExecutionStats after_saturation = service->stats();
  record(after_saturation.deadline_misses[
             BenchmarkWorkClassIndex(WorkClass::kInteractiveQuery)] > 0
      ? Status::OK()
      : Status::Corruption(
            "benchmark scheduler", "interactive deadline miss was absent"));

  const std::array<WorkClass, 13> work_classes = {
      WorkClass::kCommitCritical, WorkClass::kRecovery, WorkClass::kShutdown,
      WorkClass::kForegroundWrite, WorkClass::kPointRead,
      WorkClass::kInteractiveQuery, WorkClass::kFlush,
      WorkClass::kCompactionUrgent, WorkClass::kAnalyticalQuery,
      WorkClass::kCompactionNormal, WorkClass::kIndexBuild,
      WorkClass::kStatsMerge, WorkClass::kBlobGc};
  Status grants = Status::OK();
  for (const WorkClass work_class : work_classes) {
    WorkTaskRequest request;
    request.work_class = work_class;
    request.resources = ResourceProfile{1, 1, 1, 1, 1, 1, 1, 1, 1};
    request.commit_critical = work_class == WorkClass::kCommitCritical;
    auto submitted = service->Submit(request, [] { return Status::OK(); });
    if (!submitted.ok()) {
      grants = submitted.status();
      break;
    }
    grants = submitted.ValueOrDie().Wait();
    if (!grants.ok()) break;
  }
  record(grants);

  result.elapsed_ns = ElapsedNs(origin);
  result.verified = result.terminal_status.ok() &&
      result.logical_work_units == 5 && result.samples.size() == 5;
  if (result.verified) {
    result.result_checksum = ResultChecksum(
        dataset, BenchmarkWorkloadFamily::kSchedulerSaturation,
        result.logical_work_units);
  }
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
    case BenchmarkWorkloadFamily::kIndexPathMatrix: return "index-path-matrix";
    case BenchmarkWorkloadFamily::kSchedulerSaturation: return "scheduler-saturation";
    case BenchmarkWorkloadFamily::kMaintenanceCycle: return "maintenance-cycle";
    case BenchmarkWorkloadFamily::kHtapBalanced: return "htap-balanced";
    case BenchmarkWorkloadFamily::kRecovery: return "recovery";
  }
  return "unknown";
}

std::vector<MetricActivityRequirement> ProductionMetricActivityRequirements(
    BenchmarkWorkloadFamily family) {
  if (family == BenchmarkWorkloadFamily::kHtapBalanced) {
    return {
        {"cedar_txn_conflict_total", "strict", 1},
        {"cedar_txn_visible_prefix_stall_ns", "snapshot:succeeded", 1},
        {"cedar_scheduler_queue_delay_ns", "analytical_query", 1},
        {"cedar_scheduler_service_ns", "analytical_query", 1},
        {"cedar_scheduler_grant_cpu_slots_total", "analytical_query", 1},
    };
  }
  if (family == BenchmarkWorkloadFamily::kIndexEquality) {
    return {
        {"cedar_index_candidate_rows_total", "all", 1},
        {"cedar_scheduler_service_ns", "interactive_query", 1},
    };
  }
  if (family == BenchmarkWorkloadFamily::kIndexPathMatrix) {
    return {
        {"cedar_index_candidate_rows_total", "all", 1},
        {"cedar_scheduler_service_ns", "interactive_query", 1},
        {"cedar_scheduler_grant_cpu_slots_total", "interactive_query", 1},
    };
  }
  if (family == BenchmarkWorkloadFamily::kSchedulerSaturation) {
    std::vector<MetricActivityRequirement> requirements;
    for (const std::string& label : {
             std::string("commit_critical"), std::string("recovery"),
             std::string("shutdown"), std::string("foreground_write"),
             std::string("point_read"), std::string("interactive_query"),
             std::string("flush"), std::string("compaction_urgent"),
             std::string("analytical_query"),
             std::string("compaction_normal"), std::string("index_build"),
             std::string("stats_merge"), std::string("blob_gc")}) {
      requirements.push_back(
          {"cedar_scheduler_queue_delay_ns", label, 1});
      requirements.push_back(
          {"cedar_scheduler_service_ns", label, 1});
      requirements.push_back(
          {"cedar_scheduler_grant_cpu_slots_total", label, 1});
    }
    requirements.push_back(
        {"cedar_scheduler_deadline_misses_total", "interactive_query", 1});
    return requirements;
  }
  if (family == BenchmarkWorkloadFamily::kMaintenanceCycle) {
    return {
        {"cedar_maintenance_completed_total", "flush", 1},
        {"cedar_maintenance_completed_total", "compaction", 1},
        {"cedar_maintenance_completed_total", "blob_rotation", 1},
        {"cedar_maintenance_completed_total", "blob_gc", 1},
        {"cedar_maintenance_completed_total", "checkpoint", 1},
        {"cedar_compaction_input_bytes_total", "all", 1},
        {"cedar_compaction_output_bytes_total", "all", 1},
        {"cedar_compaction_buffer_peak_bytes", "all", 1},
        {"cedar_compaction_buffer_peak_events", "all", 1},
        {"cedar_cache_resident_peak_bytes", "all", 1},
    };
  }
  return {};
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
           BenchmarkWorkloadFamily::kIndexPathMatrix,
           BenchmarkWorkloadFamily::kSchedulerSaturation,
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
  if (config.family == BenchmarkWorkloadFamily::kMaintenanceCycle) {
    if (config.vertex_property_schema_epoch == 0) {
      return Status::InvalidArgument(
          "benchmark workload",
          "maintenance workload requires property schema epoch");
    }
    Status status = database->Flush();
    if (!status.ok()) return status;
    return database->Put(
        LogicalKey::VertexProperty(1, 1),
        dataset.config.valid_time_span + 1,
        config.vertex_property_schema_epoch,
        Value::String("bench-maintenance-" +
                      dataset.dataset_hash.substr(0, 16)));
  }
  if (config.family == BenchmarkWorkloadFamily::kIndexPathMatrix) {
    if (config.vertex_property_schema_epoch == 0) {
      return Status::InvalidArgument(
          "benchmark workload",
          "index path matrix requires vertex existence schema epoch");
    }
    struct MatrixColumn {
      uint16_t id;
      const char* name;
      bool indexed;
      ColumnSchema registered;
    };
    std::array<MatrixColumn, 7> columns = {{
        {7, "indexed_name", true, {}},
        {8, "left_key", true, {}},
        {9, "right_key", true, {}},
        {10, "hybrid_name", true, {}},
        {11, "graph_name", true, {}},
        {12, "graph_city", false, {}},
        {13, "graph_code", false, {}},
    }};
    Status status = Status::OK();
    for (MatrixColumn& column : columns) {
      status = database->RegisterColumn(
          ColumnSchema{EntityType::Vertex, column.id, 0, column.name,
                       PhysicalType::kString, 4096,
                       EncodingPolicy::kAdaptive,
                       CompressionPolicy::kLz4},
          &column.registered);
      if (!status.ok()) return status;
    }
    const uint64_t matrix_vertex_count =
        std::max<uint64_t>(dataset.config.vertex_count, 256);
    for (uint64_t id = 1; id <= matrix_vertex_count; ++id) {
      if (id > dataset.config.vertex_count) {
        status = database->Put(
            LogicalKey::VertexExistence(id), 0,
            config.vertex_property_schema_epoch, Value::Binary(""));
        if (!status.ok()) return status;
      }
      const std::array<std::string, 7> values = {
          id == 1 ? "target" : "Other",
          id <= 8 ? "Ada" : "OtherLeft",
          id >= 7 && id <= 14 ? "Paris" : "OtherRight",
          id == 1 ? "target" : "other",
          id == 1 ? "target" : "other",
          id == 1 ? "join-1"
                  : (id == 2 ? "join-2" : "city-" + std::to_string(id)),
          id == 2 ? "join-1"
                  : (id == 3 ? "join-2" : "code-" + std::to_string(id)),
      };
      for (size_t column_index = 0;
           column_index < columns.size(); ++column_index) {
        status = database->Put(
            LogicalKey::VertexProperty(id, columns[column_index].id), 0,
            columns[column_index].registered.schema_epoch,
            Value::String(values[column_index]));
        if (!status.ok()) return status;
      }
    }
    status = database->Flush();
    if (!status.ok()) return status;
    for (const MatrixColumn& column : columns) {
      if (!column.indexed) continue;
      IndexDefinition definition;
      definition.entity_type = EntityType::Vertex;
      definition.column_id = column.registered.column_id;
      definition.schema_epoch = column.registered.schema_epoch;
      definition.capabilities = kIndexEquality;
      definition.canonical_encoding_id = kIndexCanonicalEncoding;
      uint64_t index_id = 0;
      status = database->RegisterIndex(definition, &index_id);
      if (!status.ok()) return status;
      status = database->SetIndexState(index_id, IndexState::kBuilding);
      if (!status.ok()) return status;
      status = database->SetIndexState(index_id, IndexState::kActive);
      if (!status.ok()) return status;
    }
    return Status::OK();
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
      if (stats->index_candidate_entity_count == 0 ||
          !stats->has_executed_access_path ||
          stats->executed_access_path == CandidateSource::kBase) {
        result.verified = false;
        result.terminal_status = Status::Corruption(
            "benchmark workload",
            "index workload did not execute a nonzero indexed candidate path");
        result.result_checksum.clear();
      }
      result.derived_metrics.index_survival = BenchmarkRatio{
          result.logical_work_units, stats->index_candidate_entity_count};
    }
    AttachExecutionSamples(stats, &result);
    return result;
  }
  if (config.family == BenchmarkWorkloadFamily::kIndexPathMatrix) {
    return RunIndexPathMatrix(database, dataset);
  }
  if (config.family == BenchmarkWorkloadFamily::kSchedulerSaturation) {
    return RunSchedulerSaturation(database, dataset);
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
