#include "benchmarks/cedar_query_bench_workload.h"

#include <algorithm>
#include <atomic>
#include <array>
#include <chrono>
#include <filesystem>
#include <limits>
#include <mutex>
#include <sstream>
#include <thread>
#include <tuple>
#include <vector>

#include "cedar/database.h"
#include "cedar/query/query.h"
#include "cedar/query/result.h"
#include "cedar/storage_files.h"
#include "query/runtime/graph_frontier.h"
#include "query/runtime/journey.h"

namespace cedar::benchmark {
namespace {
using Clock = std::chrono::steady_clock;
constexpr uint64_t kChecksumSalt = 0x9e3779b97f4a7c15ULL;

struct BenchmarkGraph {
  VertexRef source{PartId{1}, VertexId{1000001}};
  VertexRef middle{PartId{1}, VertexId{1000002}};
  VertexRef target{PartId{1}, VertexId{1000003}};
  VertexRef corrected{PartId{1}, VertexId{1000004}};
  EdgeRef first{PartId{1}, EdgeId{2000001}};
  EdgeRef second{PartId{1}, EdgeId{2000002}};
  EdgeRef direct{PartId{1}, EdgeId{2000003}};
  EdgeRef incoming{PartId{1}, EdgeId{2000004}};
  static constexpr PropertyId kDuration{7};
};

uint64_t EventChecksum(const FactEvent& event) {
  uint64_t value = event.ref.entity_id() * kChecksumSalt;
  value ^= static_cast<uint64_t>(event.ref.family()) << 56;
  value ^= static_cast<uint64_t>(event.ref.property_id().value) << 40;
  value ^= event.valid_from.value * 0xbf58476d1ce4e5b9ULL;
  if (event.edge_identity) {
    value ^= event.edge_identity->source_vertex_id.value;
    value ^= event.edge_identity->target_vertex_id.value << 1;
    value ^= event.edge_identity->edge_type << 17;
  }
  if (event.value) value ^= static_cast<uint64_t>(event.value->type()) << 32;
  return value;
}

StatusOr<std::pair<uint64_t, uint64_t>> ScanFactChecksum(
    const Snapshot& snapshot) {
  uint64_t count = 0;
  uint64_t checksum = 0;
  const std::array<std::pair<FactFamily, PropertyId>, 5> families = {{
      {FactFamily::kVertexState, PropertyId{}},
      {FactFamily::kEdgeIdentity, PropertyId{}},
      {FactFamily::kEdgeState, PropertyId{}},
      {FactFamily::kVertexProperty, BenchmarkGraph::kDuration},
      {FactFamily::kEdgeProperty, BenchmarkGraph::kDuration},
  }};
  for (const PartId part : {PartId{1}, PartId{2}}) {
    for (const auto [family, property] : families) {
      FactScanSpec spec{part, family, property, ValidTime{1},
                        std::numeric_limits<uint32_t>::max()};
      const Status status = snapshot.StateScan(spec, [&](const FactEventBatch& batch) {
        for (const auto& event : batch.events) {
          ++count;
          checksum ^= EventChecksum(event);
        }
        return Status::OK();
      });
      if (!status.ok() && family != FactFamily::kVertexProperty &&
          family != FactFamily::kEdgeProperty) return status;
    }
  }
  return std::make_pair(count, checksum);
}

uint64_t Percentile(std::vector<uint64_t> values, uint32_t p) {
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  const size_t rank = (values.size() * p + 99) / 100;
  return values[std::min(values.size() - 1, rank - 1)];
}

Status CommitFacts(Database* db, uint64_t first, uint64_t count,
                   uint64_t* committed) {
  auto txn = db->BeginTransaction();
  if (!txn.ok()) return txn.status();
  for (uint64_t i = 0; i < count; ++i) {
    const Status s = txn.ValueOrDie()->Assert(
        EntityFact::Vertex({PartId{1}, VertexId{first + i}}), ValidTime{1});
    if (!s.ok()) return s;
  }
  auto result = txn.ValueOrDie()->Commit();
  if (!result.ok()) return result.status();
  if (result.ValueOrDie().outcome != CommitOutcome::kCommitted) {
    return result.ValueOrDie().status;
  }
  *committed += count;
  return Status::OK();
}

Status SeedGraph(Database* db, const BenchmarkGraph& graph) {
  auto txn = db->BeginTransaction();
  if (!txn.ok()) return txn.status();
  for (const VertexRef vertex : {graph.source, graph.middle, graph.target}) {
    if (Status status = txn.ValueOrDie()->Assert(EntityFact::Vertex(vertex), ValidTime{0});
        !status.ok()) return status;
    if (Status status = txn.ValueOrDie()->Retract(EntityFact::Vertex(vertex), ValidTime{100});
        !status.ok()) return status;
  }
  if (Status status = txn.ValueOrDie()->Assert(EntityFact::Vertex(graph.corrected), ValidTime{0});
      !status.ok()) return status;
  if (Status status = txn.ValueOrDie()->Retract(EntityFact::Vertex(graph.corrected), ValidTime{20});
      !status.ok()) return status;
  if (Status status = txn.ValueOrDie()->Assert(EntityFact::Vertex(graph.corrected), ValidTime{30});
      !status.ok()) return status;
  if (Status status = txn.ValueOrDie()->Retract(EntityFact::Vertex(graph.corrected), ValidTime{100});
      !status.ok()) return status;
  const std::array<std::tuple<EdgeRef, VertexRef, VertexRef, int64_t>, 4> edges = {{
      {graph.first, graph.source, graph.middle, 1},
      {graph.second, graph.middle, graph.target, 2},
      {graph.direct, graph.source, graph.target, 5},
      {graph.incoming, graph.target, graph.source, 3},
  }};
  for (const auto& [edge, source, target, duration] : edges) {
    if (Status status = txn.ValueOrDie()->Assert(
            EdgeIdentity{edge, source, target, 1}, ValidTime{0});
        !status.ok()) return status;
    if (Status status = txn.ValueOrDie()->Retract(EntityFact::Edge(edge), ValidTime{100});
        !status.ok()) return status;
    if (Status status = txn.ValueOrDie()->Set(
            PropertyFact::Edge(edge, BenchmarkGraph::kDuration), ValidTime{0},
            Value::Int64(duration));
        !status.ok()) return status;
  }
  auto committed = txn.ValueOrDie()->Commit();
  if (!committed.ok()) return committed.status();
  if (committed.ValueOrDie().outcome != CommitOutcome::kCommitted)
    return committed.ValueOrDie().status;
  return Status::OK();
}

Status ExecuteOperation(Database* database, Snapshot& snapshot,
                        QueryBenchmarkOperation op,
                        uint32_t max_hops, uint64_t limit, uint64_t* rows,
                        uint64_t* first_result_us = nullptr) {
  if (!QueryBenchmarkOperationSupported(op)) {
    return Status::NotSupported("query benchmark",
                                QueryBenchmarkOperationName(op));
  }
  const auto started = Clock::now();
  auto finish = [&](uint64_t count) {
    *rows += std::min(count, limit);
    if (first_result_us != nullptr && count != 0) {
      *first_result_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - started).count());
    }
    return Status::OK();
  };
  FactScanSpec spec{PartId{1}, FactFamily::kVertexState, PropertyId{},
                    ValidTime{1}, 256};
  uint64_t seen = 0;
  const auto visitor = [&](const FactEventBatch& batch) {
    seen += batch.events.size();
    return Status::OK();
  };
  const FactEventBatchVisitor bounded_visitor = visitor;
  Status status = Status::OK();
  switch (op) {
    case QueryBenchmarkOperation::kStateAt:
      status = snapshot.StateScan(spec, bounded_visitor);
      break;
    case QueryBenchmarkOperation::kPropertyFilter: {
      auto edge = Slot<EdgeRef>::Named("edge_filter");
      auto duration = OptionalSlot<int64_t>::Named("duration_filter");
      auto source = Query::Edges(edge, At{ValidTime{1}});
      if (!source.ok()) return source.status();
      auto bound = source.ValueOrDie().BindEdgeProperty(
          edge, BenchmarkGraph::kDuration, duration);
      if (!bound.ok()) return bound.status();
      auto filtered = bound.ValueOrDie().Where(
          IsPresent(duration) && GreaterThan(ValueOf(duration), Literal<int64_t>(1)));
      if (!filtered.ok()) return filtered.status();
      auto selected = filtered.ValueOrDie().Select({Project(edge), Project(duration)});
      if (!selected.ok()) return selected.status();
      auto prepared = database->PrepareQuery(selected.ValueOrDie());
      if (!prepared.ok()) return prepared.status();
      auto cursor = prepared.ValueOrDie().Execute(
          std::move(snapshot), Bindings{}, QueryOptions{});
      if (!cursor.ok()) return cursor.status();
      uint64_t count = 0;
      while (true) {
        auto batch = cursor.ValueOrDie().Next();
        if (!batch.ok()) return batch.status();
        if (!batch.ValueOrDie().has_value()) break;
        count += batch.ValueOrDie()->row_count();
      }
      return finish(count);
    }
    case QueryBenchmarkOperation::kEvents:
    case QueryBenchmarkOperation::kChanges:
    case QueryBenchmarkOperation::kHistory:
      status = snapshot.EventScan(spec, bounded_visitor);
      break;
    case QueryBenchmarkOperation::kExpandOut:
    case QueryBenchmarkOperation::kExpandIn:
    case QueryBenchmarkOperation::kExpandBoth:
    case QueryBenchmarkOperation::kKHop:
    case QueryBenchmarkOperation::kCoexistingShortestPath:
    case QueryBenchmarkOperation::kEarliestArrival:
    case QueryBenchmarkOperation::kLatestDeparture:
    case QueryBenchmarkOperation::kFastestDuration: {
      const BenchmarkGraph graph;
      const ExpandDirection direction =
          op == QueryBenchmarkOperation::kExpandIn ? ExpandDirection::kIn :
          op == QueryBenchmarkOperation::kExpandBoth ? ExpandDirection::kBoth :
                                                        ExpandDirection::kOut;
      const internal::GraphExpansionRequest request{
          {graph.source}, ValidTimeInterval{ValidTime{0}, ValidTime{100}},
          direction, 1};
      if (op == QueryBenchmarkOperation::kExpandOut ||
          op == QueryBenchmarkOperation::kExpandIn ||
          op == QueryBenchmarkOperation::kExpandBoth) {
        auto expanded = internal::ExpandTemporal(snapshot, request);
        if (!expanded.ok()) return expanded.status();
        return finish(expanded.ValueOrDie().size());
      }
      if (op == QueryBenchmarkOperation::kKHop) {
        internal::GraphFrontierOptions options;
        options.max_hops = max_hops;
        auto expanded = internal::KHopExpand(snapshot, request, options);
        if (!expanded.ok()) return expanded.status();
        return finish(expanded.ValueOrDie().labels.size());
      }
      if (op == QueryBenchmarkOperation::kCoexistingShortestPath) {
        auto path = internal::CoexistingShortestPath(snapshot, request,
                                                     graph.target);
        if (!path.ok()) return path.status();
        return finish(path.ValueOrDie().paths.size());
      }
      internal::JourneyRequest journey_request{
          graph.source, graph.target,
          ValidTimeInterval{ValidTime{0}, ValidTime{100}},
          op == QueryBenchmarkOperation::kEarliestArrival
              ? internal::JourneyObjective::kEarliestArrival
              : op == QueryBenchmarkOperation::kLatestDeparture
                    ? internal::JourneyObjective::kLatestDeparture
                    : internal::JourneyObjective::kFastestDuration,
          BenchmarkGraph::kDuration,
          [&snapshot](EdgeRef edge, ValidTime time)
              -> StatusOr<std::optional<ValidDuration>> {
            auto value = snapshot.Get(PropertyFact::Edge(
                edge, BenchmarkGraph::kDuration), time);
            if (!value.ok()) return value.status();
            if (!value.ValueOrDie().has_value())
              return std::optional<ValidDuration>{};
            const Value& duration = *value.ValueOrDie();
            if (duration.type() != PhysicalType::kInt64)
              return Status::SchemaMismatch("query benchmark", "duration type");
            return std::optional<ValidDuration>{ValidDuration{
                static_cast<uint64_t>(std::get<int64_t>(duration.data()))}};
          },
          std::nullopt, max_hops, ExpandDirection::kOut, 1};
      auto journey = internal::FindJourney(snapshot, journey_request);
      if (!journey.ok()) return journey.status();
      return finish(1);
    }
    default:
      return Status::NotSupported("query benchmark",
                                  QueryBenchmarkOperationName(op));
  }
  if (!status.ok()) return status;
  return finish(seen);
}

void AddFileBytes(const std::vector<StorageFileInfo>& files,
                  QueryBenchmarkResult* result) {
  for (const auto& file : files) {
    result->total_bytes += file.size_bytes;
    if (file.role == StorageFileRole::kAuthoritativeFacts) {
      result->authoritative_bytes += file.size_bytes;
    } else if (file.query_file.has_value() &&
        file.query_file->authority == StorageFileAuthority::kAuthoritative) {
      result->authoritative_bytes += file.size_bytes;
    } else if (file.query_file.has_value() &&
               file.query_file->authority == StorageFileAuthority::kTemporary) {
      result->scratch_bytes += file.size_bytes;
    } else if (file.table_format == StorageTableFormat::kCedarAdjacency) {
      result->adjacency_bytes += file.size_bytes;
      result->derived_bytes += file.size_bytes;
    } else if (file.table_format == StorageTableFormat::kCedarProperty) {
      result->property_bytes += file.size_bytes;
      result->derived_bytes += file.size_bytes;
    } else if (file.table_format == StorageTableFormat::kCedarStatistics) {
      result->statistics_bytes += file.size_bytes;
      result->derived_bytes += file.size_bytes;
    } else if (file.query_file.has_value() &&
               file.query_file->authority == StorageFileAuthority::kDerived) {
      result->derived_bytes += file.size_bytes;
    } else {
      result->engine_internal_bytes += file.size_bytes;
      if (file.relative_filename.find("MANIFEST") != std::string::npos ||
          file.relative_filename == "CURRENT" ||
          file.relative_filename.find(".log") != std::string::npos) {
        result->wal_manifest_bytes += file.size_bytes;
      }
    }
  }
}
}  // namespace

StatusOr<QueryBenchmarkResult> RunQueryBenchmark(
    const QueryBenchmarkOptions& options) {
  std::filesystem::remove_all(options.path);
  DatabaseOptions db_options;
  db_options.path = options.path;
  db_options.storage_profile = StorageProfile::kProductionAppend;
  db_options.production.memory_budget_bytes = 1ULL << 30;
  db_options.production.kernel_mode = true;
  db_options.query_runtime.query_memory_bytes = 32ULL << 20;
  db_options.query_runtime.projection_cache_bytes = 32ULL << 20;
  db_options.query_runtime.query_delta_bytes = 32ULL << 20;
  auto opened = Database::Open(db_options);
  if (!opened.ok()) return opened.status();
  auto database = std::move(opened).ConsumeValueOrDie();
  QueryBenchmarkResult result;
  result.raw_sample_path = options.path;
#ifdef NDEBUG
  result.build_type = "release";
#else
  result.build_type = "debug";
#endif
  result.seed = options.seed;
  result.dataset_checksum = 0;
  if (!QueryBenchmarkOperationSupported(options.operation)) {
    result.operation_supported = false;
    result.terminal_status = std::string("unsupported operation: ") +
                             QueryBenchmarkOperationName(options.operation);
    result.gate_classification = "unsupported";
    result.hard_gate_pass = false;
    return result;
  }
  if (options.projection != ProjectionState::kCanonicalOnly) {
    result.projection_state_supported = false;
    result.terminal_status =
        "projection benchmark setup unavailable: canonical-only paused is the only measured state";
    result.gate_classification = "unsupported";
    result.hard_gate_pass = false;
    return result;
  }
  const auto run_start = Clock::now();
  result.projection_active = options.projection_work == ProjectionWork::kActive;
  std::optional<QueryMaintenanceHandle> maintenance;
  if (auto registered = database->RegisterProperty(PropertyDefinition{
          BenchmarkGraph::kDuration, 0, "duration", PropertyEntityKind::kEdge,
          PhysicalType::kInt64, 4096});
      !registered.ok()) {
    result.terminal_status = registered.status().ToString();
    result.gate_classification = "incomplete";
    return result;
  }
  const BenchmarkGraph graph;
  if (Status status = SeedGraph(database.get(), graph); !status.ok()) {
    result.terminal_status = status.ToString();
    result.gate_classification = "incomplete";
    return result;
  }

  std::atomic<uint64_t> next_id{1}, transactions{0}, facts{0};
  std::atomic<uint64_t> dataset_checksum{0};
  std::atomic<bool> failed{false};
  std::mutex failure_mutex, sample_mutex;
  std::string failure;
  std::vector<uint64_t> write_samples;
  StatusOr<std::pair<uint64_t, uint64_t>> seeded_checksum(
      Status::InvalidArgument("benchmark", "seed checksum not attempted"));
  {
    auto seeded_snapshot = database->BeginSnapshot();
    if (!seeded_snapshot.ok()) return seeded_snapshot.status();
    seeded_checksum = ScanFactChecksum(seeded_snapshot.ValueOrDie());
  }
  if (!seeded_checksum.ok()) return seeded_checksum.status();
  transactions.store(1);
  facts.store(seeded_checksum.ValueOrDie().first);
  dataset_checksum.store(seeded_checksum.ValueOrDie().second);
  if (result.projection_active) {
    auto refreshed = database->RefreshQueryStatistics();
    if (!refreshed.ok()) {
      if (!refreshed.status().IsNotFound()) {
        result.terminal_status = refreshed.status().ToString();
        result.gate_classification = "incomplete";
        return result;
      }
      // A fresh canonical-only database has no derived generation to refresh.
      // Still execute Cedar's observable maintenance sampler so active runs
      // remain measurable without pretending that a projection was built.
      const auto runtime = database->SampleQueryMetrics();
      (void)runtime;
      result.maintenance_status = "canonical-only-no-generation";
      result.maintenance_observed = true;
    } else {
      maintenance = std::move(refreshed).ConsumeValueOrDie();
      result.maintenance_status = "refresh-submitted";
      result.maintenance_observed = true;
    }
  }
  const auto seed_done = Clock::now();
  const uint64_t seed_facts = std::max<uint64_t>(options.degree * 4,
                                                  options.facts_per_txn);
  uint64_t seed_transaction_count = transactions.load();
  while (next_id.load() <= seed_facts) {
    const uint64_t first = next_id.fetch_add(options.facts_per_txn);
    uint64_t committed = 0;
    const Status s = CommitFacts(database.get(), first, options.facts_per_txn,
                                 &committed);
    if (!s.ok()) return s;
    transactions.fetch_add(1);
    facts.fetch_add(committed);
    for (uint64_t id = first; id < first + committed; ++id)
      dataset_checksum.fetch_xor(id * kChecksumSalt);
  }
  seed_transaction_count = transactions.load();
  const auto write_start = Clock::now();
  const auto deadline = write_start +
                        std::chrono::seconds(options.duration_seconds);
  const auto writer = [&] {
    while (Clock::now() < deadline) {
      const uint64_t first = next_id.fetch_add(options.facts_per_txn);
      const auto start = Clock::now();
      uint64_t committed = 0;
      const Status s = CommitFacts(database.get(), first, options.facts_per_txn,
                                   &committed);
      if (!s.ok()) {
        failed.store(true);
        std::lock_guard<std::mutex> lock(failure_mutex);
        if (failure.empty()) failure = s.ToString();
        return;
      }
      transactions.fetch_add(1);
      facts.fetch_add(committed);
      for (uint64_t id = first; id < first + committed; ++id)
        dataset_checksum.fetch_xor(id * kChecksumSalt);
      const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
          Clock::now() - start).count();
      std::lock_guard<std::mutex> lock(sample_mutex);
      write_samples.push_back(static_cast<uint64_t>(us));
    }
  };
  std::vector<std::jthread> writers;
  writers.reserve(options.writers);
  for (uint32_t i = 0; i < options.writers; ++i) writers.emplace_back(writer);
  writers.clear();
  const auto write_done = Clock::now();
  if (failed.load()) result.terminal_status = failure;

  if (options.cache == QueryCacheState::kWarm) {
    auto warm_snapshot = database->BeginSnapshot();
    if (!warm_snapshot.ok()) return warm_snapshot.status();
    uint64_t warm_rows = 0;
    const Status warm_status = ExecuteOperation(
        database.get(), warm_snapshot.ValueOrDie(), options.operation, options.max_hops,
        options.result_limit,
        &warm_rows);
    if (!warm_status.ok()) return warm_status;
    result.cache_conditioned = true;
  }
  const auto query_start = Clock::now();
  const auto query_deadline = query_start +
                              std::chrono::seconds(options.duration_seconds);
  std::vector<uint64_t> query_samples;
  std::vector<uint64_t> first_result_samples;
  std::mutex query_mutex;
  std::atomic<uint64_t> rows{0};
  std::vector<std::jthread> readers;
  readers.reserve(options.readers);
  for (uint32_t i = 0; i < options.readers; ++i) {
    readers.emplace_back([&] {
    bool executed = false;
    do {
      auto snapshot = database->BeginSnapshot();
      if (!snapshot.ok()) {
        std::lock_guard<std::mutex> lock(query_mutex);
        result.terminal_status = snapshot.status().ToString();
        return;
      }
      const auto start = Clock::now();
      uint64_t local_rows = 0;
      uint64_t first_result_us = 0;
      const Status s = ExecuteOperation(database.get(), snapshot.ValueOrDie(), options.operation,
                                        options.max_hops, options.result_limit,
                                        &local_rows, &first_result_us);
      if (!s.ok()) {
        std::lock_guard<std::mutex> lock(query_mutex);
        result.terminal_status = s.ToString();
        return;
      }
      executed = true;
      rows.fetch_add(local_rows);
      const uint64_t elapsed = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start)
              .count());
      std::lock_guard<std::mutex> lock(query_mutex);
      query_samples.push_back(elapsed);
      first_result_samples.push_back(first_result_us);
    } while (!executed || Clock::now() < query_deadline);
  });
  }
  readers.clear();
  const auto query_done = Clock::now();
  result.elapsed_seconds = std::chrono::duration<double>(query_done - run_start).count();
  result.write_elapsed_seconds = std::chrono::duration<double>(write_done - write_start).count();
  result.query_elapsed_seconds = std::chrono::duration<double>(query_done - query_start).count();
  result.dataset_checksum = dataset_checksum.load();
  result.transactions = transactions.load();
  result.facts = facts.load();
  result.measured_transactions = result.transactions >= seed_transaction_count
                                     ? result.transactions - seed_transaction_count
                                     : 0;
  result.measured_facts = result.facts >= seeded_checksum.ValueOrDie().first
                              ? result.facts - seeded_checksum.ValueOrDie().first
                              : 0;
  result.dataset_checksum = dataset_checksum.load();
  result.rows = rows.load();
  result.query_samples = query_samples.size();
  result.query_p50_us = Percentile(query_samples, 50);
  result.query_p95_us = Percentile(query_samples, 95);
  result.query_p99_us = Percentile(query_samples, 99);
  result.first_result_p50_us = Percentile(first_result_samples, 50);
  result.write_p50_us = Percentile(write_samples, 50);
  result.write_p95_us = Percentile(write_samples, 95);
  result.write_p99_us = Percentile(write_samples, 99);
  const CommitPipelineMetrics pipeline = database->GetCommitPipelineMetrics();
  result.wal_sync_p99_us =
      pipeline.latency.wal_sync.ApproximatePercentile(99);
  result.end_to_end_p99_us =
      pipeline.latency.end_to_end.ApproximatePercentile(99);
  result.query_qps = result.query_elapsed_seconds > 0
                        ? static_cast<double>(result.rows) / result.query_elapsed_seconds
                        : 0.0;
  result.metrics_complete = pipeline.latency.wal_sync.count > 0 &&
                            pipeline.latency.end_to_end.count > 0 &&
                            !query_samples.empty() && result.rows > 0;

  if (options.verify_reopen && result.terminal_status == "OK") {
    const Status closed = database->Close();
    if (!closed.ok()) result.terminal_status = closed.ToString();
    database.reset();
    if (result.terminal_status == "OK") {
      auto reopened = Database::Open(db_options);
      if (!reopened.ok()) {
        result.terminal_status = reopened.status().ToString();
      } else {
        auto snapshot = reopened.ValueOrDie()->BeginSnapshot();
        auto scanned = snapshot.ok()
                           ? ScanFactChecksum(snapshot.ValueOrDie())
                           : StatusOr<std::pair<uint64_t, uint64_t>>(snapshot.status());
        result.reopen_verified = scanned.ok() &&
                                 scanned.ValueOrDie().first == result.facts &&
                                 scanned.ValueOrDie().second == result.dataset_checksum;
        if (!result.reopen_verified) result.terminal_status = "reopen verification failed";
        reopened.ValueOrDie()->Close().IgnoreError();
      }
    }
  }
  auto files = InspectStorageFiles({options.path, StorageProfile::kProductionAppend,
                                    db_options.production});
  if (!files.ok()) {
    result.storage_inspection_status = files.status().ToString();
    result.terminal_status = result.storage_inspection_status;
  } else {
    AddFileBytes(files.ValueOrDie(), &result);
  }
  result.mib_per_second = result.elapsed_seconds > 0
                              ? static_cast<double>(result.total_bytes) /
                                    (1024.0 * 1024.0 * result.elapsed_seconds)
                              : 0.0;
  result.group_fill_p50 = Percentile(write_samples, 50);
  result.space_amplification = result.authoritative_bytes == 0
                                   ? 0.0
                                   : static_cast<double>(result.total_bytes) /
                                         result.authoritative_bytes;
  result.write_amplification = result.authoritative_bytes == 0
                                   ? 0.0
                                   : static_cast<double>(result.authoritative_bytes +
                                                         result.derived_bytes) /
                                         result.authoritative_bytes;
  result.hard_gate_pass = result.metrics_complete &&
                          files.ok() &&
                          result.operation_supported &&
                          result.projection_state_supported &&
                          (!result.projection_active || result.maintenance_observed) &&
                          result.terminal_status == "OK" &&
                          (!options.verify_reopen || result.reopen_verified) &&
                          (result.authoritative_bytes == 0 ||
                           result.derived_bytes <= result.authoritative_bytes * 3 / 2);
  result.gate_classification = result.hard_gate_pass ? "pass" : "incomplete";
  return result;
}

std::string QueryBenchmarkCsvHeader() {
  return "operation,projection_state,degree,selectivity_percent,readers,cache_state,writers,facts_per_txn,seed,dataset_checksum,transactions,facts,measured_transactions,measured_facts,rows,elapsed_seconds,write_elapsed_seconds,query_elapsed_seconds,transactions_per_second,facts_per_second,query_qps,mib_per_second,group_fill_p50,query_samples,query_p50_us,query_p95_us,query_p99_us,first_result_p50_us,write_p50_us,write_p95_us,write_p99_us,wal_sync_p99_us,end_to_end_p99_us,authoritative_bytes,adjacency_bytes,property_bytes,statistics_bytes,derived_bytes,scratch_bytes,engine_internal_bytes,wal_manifest_bytes,total_bytes,write_amplification,space_amplification,projection_lag,projection_work,maintenance_status,maintenance_observed,cache_conditioned,operation_supported,projection_state_supported,metrics_complete,build_type,sanitizer,host,plan_fingerprint,raw_sample_path,storage_inspection_status,terminal_status,reopen_verified,gate_classification,hard_gate_pass";
}

std::string QueryBenchmarkCsvRow(const QueryBenchmarkOptions& o,
                                 const QueryBenchmarkResult& r) {
  std::ostringstream x;
  const double t = r.write_elapsed_seconds ? r.measured_transactions / r.write_elapsed_seconds : 0;
  const double f = r.write_elapsed_seconds ? r.measured_facts / r.write_elapsed_seconds : 0;
  x << QueryBenchmarkOperationName(o.operation) << ',' << ProjectionStateName(o.projection)
    << ',' << o.degree << ',' << o.selectivity_percent << ',' << o.readers << ','
    << (o.cache == QueryCacheState::kCold ? "cold" : "warm") << ',' << o.writers << ','
    << o.facts_per_txn << ',' << o.seed << ',' << r.dataset_checksum << ','
    << r.transactions << ',' << r.facts << ',' << r.measured_transactions << ','
    << r.measured_facts << ',' << r.rows << ','
    << r.elapsed_seconds << ',' << r.write_elapsed_seconds << ',' << r.query_elapsed_seconds << ','
    << t << ',' << f << ',' << r.query_qps << ',' << r.mib_per_second << ','
    << r.group_fill_p50 << ',' << r.query_samples << ','
    << r.query_p50_us << ',' << r.query_p95_us << ',' << r.query_p99_us << ','
    << r.first_result_p50_us << ','
    << r.write_p50_us << ',' << r.write_p95_us << ',' << r.write_p99_us << ','
    << r.wal_sync_p99_us << ',' << r.end_to_end_p99_us << ',' << r.authoritative_bytes << ','
    << r.adjacency_bytes << ',' << r.property_bytes << ',' << r.statistics_bytes << ','
    << r.derived_bytes << ',' << r.scratch_bytes << ',' << r.engine_internal_bytes << ','
    << r.wal_manifest_bytes << ',' << r.total_bytes << ','
    << r.write_amplification << ',' << r.space_amplification << ','
    << r.projection_lag << ',' << (r.projection_active ? "active" : "paused") << ','
    << r.maintenance_status << ',' << (r.maintenance_observed ? "true" : "false") << ','
    << (r.cache_conditioned ? "true" : "false") << ','
    << (r.operation_supported ? "true" : "false") << ','
    << (r.projection_state_supported ? "true" : "false") << ','
    << (r.metrics_complete ? "true" : "false") << ','
    << r.build_type << ',' << r.sanitizer << ',' << r.host << ','
    << r.plan_fingerprint << ',' << r.raw_sample_path << ','
    << r.storage_inspection_status << ','
    << r.terminal_status << ',' << (r.reopen_verified ? "true" : "false") << ','
    << r.gate_classification << ',' << (r.hard_gate_pass ? "true" : "false");
  return x.str();
}

std::string QueryBenchmarkJson(const QueryBenchmarkOptions& o,
                               const QueryBenchmarkResult& r) {
  std::ostringstream x;
  x << "{\"operation\":\"" << QueryBenchmarkOperationName(o.operation)
    << "\",\"projection_state\":\"" << ProjectionStateName(o.projection)
    << "\",\"writers\":" << o.writers << ",\"facts_per_txn\":"
    << o.facts_per_txn << ",\"transactions\":" << r.transactions
    << ",\"facts\":" << r.facts << ",\"rows\":" << r.rows
    << ",\"measured_transactions\":" << r.measured_transactions
    << ",\"measured_facts\":" << r.measured_facts
    << ",\"query_p50_us\":" << r.query_p50_us << ",\"query_p95_us\":"
    << r.query_p95_us << ",\"query_p99_us\":" << r.query_p99_us
    << ",\"first_result_p50_us\":" << r.first_result_p50_us
    << ",\"query_qps\":" << r.query_qps
    << ",\"authoritative_bytes\":" << r.authoritative_bytes
    << ",\"derived_bytes\":" << r.derived_bytes
    << ",\"adjacency_bytes\":" << r.adjacency_bytes
    << ",\"property_bytes\":" << r.property_bytes
    << ",\"statistics_bytes\":" << r.statistics_bytes
    << ",\"scratch_bytes\":" << r.scratch_bytes
    << ",\"engine_internal_bytes\":" << r.engine_internal_bytes
    << ",\"wal_manifest_bytes\":" << r.wal_manifest_bytes
    << ",\"mib_per_second\":" << r.mib_per_second
    << ",\"group_fill_p50\":" << r.group_fill_p50
    << ",\"write_amplification\":" << r.write_amplification
    << ",\"space_amplification\":" << r.space_amplification
    << ",\"build_type\":\"" << r.build_type
    << "\",\"sanitizer\":\"" << r.sanitizer
    << "\",\"host\":\"" << r.host
    << "\",\"plan_fingerprint\":\"" << r.plan_fingerprint
    << "\",\"raw_sample_path\":\"" << r.raw_sample_path
    << "\",\"seed\":" << o.seed << ",\"dataset_checksum\":"
    << r.dataset_checksum << ",\"cache_conditioned\":"
    << (r.cache_conditioned ? "true" : "false")
    << ",\"operation_supported\":" << (r.operation_supported ? "true" : "false")
    << ",\"projection_state_supported\":"
    << (r.projection_state_supported ? "true" : "false")
    << ",\"metrics_complete\":" << (r.metrics_complete ? "true" : "false")
    << ",\"maintenance_status\":\"" << r.maintenance_status
    << "\",\"maintenance_observed\":"
    << (r.maintenance_observed ? "true" : "false")
    << ",\"storage_inspection_status\":\"" << r.storage_inspection_status << "\""
    << ",\"terminal_status\":\"" << r.terminal_status
    << "\",\"reopen_verified\":" << (r.reopen_verified ? "true" : "false")
    << ",\"gate_classification\":\"" << r.gate_classification
    << "\",\"hard_gate_pass\":" << (r.hard_gate_pass ? "true" : "false") << '}';
  return x.str();
}
}  // namespace cedar::benchmark
