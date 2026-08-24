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
#include <unordered_set>
#include <vector>

#include "cedar/database.h"
#include "cedar/core/crc32c.h"
#include "cedar/query/query.h"
#include "cedar/query/result.h"
#include "cedar/storage_files.h"
#include "query/runtime/graph_frontier.h"
#include "query/runtime/journey.h"
#include "query/runtime/property_binding.h"
#include "query/runtime/relational.h"
#include "query/runtime/temporal_source.h"
#include "query/projection/projection_format.h"
#include "query/projection/projection_store.h"

namespace cedar::benchmark {
namespace {
using Clock = std::chrono::steady_clock;
constexpr uint64_t kChecksumSalt = 0x9e3779b97f4a7c15ULL;

struct BenchmarkGraph {
  // Canonical typed temporal sources operate on Cedar's local partition 0;
  // keep the deterministic graph in that partition so graph and property
  // operators exercise the same authoritative source.
  VertexRef source{PartId{0}, VertexId{1000001}};
  VertexRef middle{PartId{0}, VertexId{1000002}};
  VertexRef target{PartId{0}, VertexId{1000003}};
  VertexRef corrected{PartId{0}, VertexId{1000004}};
  EdgeRef first{PartId{0}, EdgeId{2000001}};
  EdgeRef second{PartId{0}, EdgeId{2000002}};
  EdgeRef direct{PartId{0}, EdgeId{2000003}};
  EdgeRef incoming{PartId{0}, EdgeId{2000004}};
  static constexpr PropertyId kDuration{7};
  static constexpr PropertyId kScore{8};
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

uint64_t VertexRowsChecksum(const std::vector<VertexRef>& rows) {
  uint64_t checksum = 0;
  for (const VertexRef& vertex : rows) {
    checksum ^= (vertex.vertex_id.value * kChecksumSalt) ^
                (static_cast<uint64_t>(vertex.part_id.value) << 48);
  }
  return checksum;
}

StatusOr<std::pair<uint64_t, uint64_t>> ScanFactChecksum(
    const Snapshot& snapshot) {
  uint64_t count = 0;
  uint64_t checksum = 0;
  const std::array<std::pair<FactFamily, PropertyId>, 6> families = {{
      {FactFamily::kVertexState, PropertyId{}},
      {FactFamily::kEdgeIdentity, PropertyId{}},
      {FactFamily::kEdgeState, PropertyId{}},
      {FactFamily::kVertexProperty, BenchmarkGraph::kDuration},
      {FactFamily::kEdgeProperty, BenchmarkGraph::kDuration},
      {FactFamily::kVertexProperty, BenchmarkGraph::kScore},
  }};
  for (const PartId part : {PartId{0}, PartId{1}, PartId{2}}) {
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

CommitGroupFillMetrics GroupFillDelta(const CommitGroupFillMetrics& before,
                                      const CommitGroupFillMetrics& after) {
  CommitGroupFillMetrics delta;
  delta.groups = after.groups >= before.groups ? after.groups - before.groups : 0;
  delta.total_transactions =
      after.total_transactions >= before.total_transactions
          ? after.total_transactions - before.total_transactions
          : 0;
  for (size_t index = 0; index < delta.buckets.size(); ++index) {
    delta.buckets[index] = after.buckets[index] >= before.buckets[index]
                               ? after.buckets[index] - before.buckets[index]
                               : 0;
  }
  // The cumulative max cannot be subtracted.  Bound it by the timed
  // transaction count so seed/setup groups cannot become the timed maximum.
  delta.max_transactions = std::min(after.max_transactions,
                                    delta.total_transactions);
  return delta;
}

uint64_t NondecreasingDelta(uint64_t after, uint64_t before) {
  return after >= before ? after - before : 0;
}

std::string CsvEscape(const std::string& value) {
  std::string escaped;
  escaped.reserve(value.size() + 2);
  escaped.push_back('"');
  for (const char c : value) {
    if (c == '"') escaped.push_back('"');
    escaped.push_back(c);
  }
  escaped.push_back('"');
  return escaped;
}

std::string JsonEscape(const std::string& value) {
  std::string escaped;
  escaped.reserve(value.size());
  for (const unsigned char c : value) {
    switch (c) {
      case '"': escaped += "\\\""; break;
      case '\\': escaped += "\\\\"; break;
      case '\b': escaped += "\\b"; break;
      case '\f': escaped += "\\f"; break;
      case '\n': escaped += "\\n"; break;
      case '\r': escaped += "\\r"; break;
      case '\t': escaped += "\\t"; break;
      default:
        if (c < 0x20) {
          constexpr char kHex[] = "0123456789abcdef";
          escaped += "\\u00";
          escaped.push_back(kHex[c >> 4]);
          escaped.push_back(kHex[c & 0x0f]);
        } else {
          escaped.push_back(static_cast<char>(c));
        }
    }
  }
  return escaped;
}

Status CommitFacts(Database* db, uint64_t first, uint64_t count,
                   uint64_t* committed, uint64_t commit_deadline_us) {
  auto txn = db->BeginTransaction(
      TransactionOptions{.commit_deadline_us = commit_deadline_us});
  if (!txn.ok()) return txn.status();
  for (uint64_t i = 0; i < count; ++i) {
    const Status s = txn.ValueOrDie()->Assert(
        EntityFact::Vertex({PartId{0}, VertexId{first + i}}), ValidTime{1});
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

Status SeedGraph(Database* db, const BenchmarkGraph& graph,
                 uint64_t commit_deadline_us) {
  auto txn = db->BeginTransaction(
      TransactionOptions{.commit_deadline_us = commit_deadline_us});
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

Status SeedBenchmarkScore(Database* db, const BenchmarkGraph& graph,
                          uint64_t commit_deadline_us) {
  auto txn = db->BeginTransaction(
      TransactionOptions{.commit_deadline_us = commit_deadline_us});
  if (!txn.ok()) return txn.status();
  if (Status status = txn.ValueOrDie()->Set(
          PropertyFact::Vertex(graph.source, BenchmarkGraph::kScore),
          ValidTime{0}, Value::Int64(10));
      !status.ok()) return status;
  auto committed = txn.ValueOrDie()->Commit();
  if (!committed.ok()) return committed.status();
  return committed.ValueOrDie().outcome == CommitOutcome::kCommitted
             ? Status::OK()
             : committed.ValueOrDie().status;
}

Status BuildBenchmarkProjection(const std::string& database_path,
                                const BenchmarkGraph& graph,
                                CommitSeq base_seq,
                                uint64_t seed_facts,
                                ProjectionState state) {
  internal::ProjectionStoreOptions options;
  options.path = database_path + "/projections";
  options.database_identity = database_path;
  options.visible_seq = base_seq;
  options.oldest_readable_seq = CommitSeq{1};
  auto opened = internal::QueryProjectionStore::Open(std::move(options));
  if (!opened.ok()) return opened.status();

  constexpr uint64_t kGeneration = 1;
  internal::ProjectionChain chain;
  chain.header.kind = internal::ProjectionKind::kState;
  chain.header.generation_id = kGeneration;
  chain.header.base_seq = base_seq;
  chain.header.part_id = graph.source.part_id;
  const bool partial = state == ProjectionState::kPartialCoverage;
  chain.header.entity_min = partial ? graph.source.vertex_id.value : 0;
  chain.header.entity_max_exclusive =
      partial ? graph.source.vertex_id.value + 1 : UINT64_MAX;
  chain.header.valid_from_min = ValidTime{0};
  chain.header.valid_to_max = std::nullopt;
  if (partial) {
    chain.intervals.push_back({ValidTimeInterval{ValidTime{0}, ValidTime{100}},
                               Value::Int64(1), graph.source.vertex_id.value});
  } else {
    for (const VertexRef vertex : {graph.source, graph.middle, graph.target}) {
      chain.intervals.push_back({ValidTimeInterval{ValidTime{0}, ValidTime{100}},
                                 Value::Int64(1), vertex.vertex_id.value});
    }
    chain.intervals.push_back({ValidTimeInterval{ValidTime{0}, ValidTime{20}},
                               Value::Int64(1), graph.corrected.vertex_id.value});
    chain.intervals.push_back({ValidTimeInterval{ValidTime{30}, ValidTime{100}},
                               Value::Int64(1), graph.corrected.vertex_id.value});
    for (uint64_t id = 1; id <= seed_facts; ++id) {
      chain.intervals.push_back({ValidTimeInterval{ValidTime{1}, std::nullopt},
                                 Value::Int64(1), id});
    }
  }
  std::sort(chain.intervals.begin(), chain.intervals.end(),
            [](const internal::ProjectionInterval& left,
               const internal::ProjectionInterval& right) {
              if (left.entity_id != right.entity_id) {
                return left.entity_id < right.entity_id;
              }
              return left.effective.from.value < right.effective.from.value;
            });
  auto encoded = internal::EncodeProjectionPage(chain,
                                                 internal::CompressionCodec::kNone);
  if (!encoded.ok()) return encoded.status();

  internal::SegmentDescriptor descriptor;
  descriptor.segment_id = "cedar-benchmark-state";
  descriptor.filename = "cedar-benchmark-state.csegment";
  descriptor.header = chain.header;
  descriptor.file_bytes = encoded.ValueOrDie().size();
  descriptor.checksum = crc32c::Value(encoded.ValueOrDie().data(),
                                      encoded.ValueOrDie().size());
  internal::CoverageRegion region;
  region.kind = internal::ProjectionKind::kState;
  region.part_id = chain.header.part_id;
  region.schema_epoch = chain.header.schema_epoch;
  region.entity_min = chain.header.entity_min;
  region.entity_max_exclusive = chain.header.entity_max_exclusive;
  region.valid_time = ValidTimeInterval{ValidTime{0}, std::nullopt};
  region.segments.push_back(descriptor);
  internal::ProjectionBuild build;
  build.manifest.database_identity = database_path;
  build.manifest.generation_id = kGeneration;
  build.manifest.base_seq = base_seq;
  build.manifest.regions.push_back(std::move(region));
  build.segments.push_back({descriptor, encoded.ConsumeValueOrDie()});
  return opened.ValueOrDie()->Build(build);
}

Status ApplyProjectionDelta(Database* database, uint64_t first,
                            uint64_t count, uint64_t commit_deadline_us,
                            uint64_t* transactions, uint64_t* facts) {
  for (uint64_t i = 0; i < count; ++i) {
    auto transaction = database->BeginTransaction(
        TransactionOptions{.commit_deadline_us = commit_deadline_us});
    if (!transaction.ok()) return transaction.status();
    // Use a real retract/assert pair per entity.  At valid time 1 the
    // retract removes the base interval, while the valid-time-2 assert makes
    // the same entity visible again only after the queried point.  This keeps
    // the long-delta fixture observable instead of repeatedly asserting one
    // already-present fact at the same time.
    const VertexRef vertex{PartId{0}, VertexId{first + i / 2}};
    const ValidTime valid_time{
        static_cast<uint64_t>((i % 2 == 0) ? 1 : 2)};
    const Status mutation =
        (i % 2 == 0)
            ? transaction.ValueOrDie()->Retract(EntityFact::Vertex(vertex),
                                                valid_time)
            : transaction.ValueOrDie()->Assert(EntityFact::Vertex(vertex),
                                               valid_time);
    if (!mutation.ok()) return mutation;
    auto committed = transaction.ValueOrDie()->Commit();
    if (!committed.ok()) return committed.status();
    if (committed.ValueOrDie().outcome != CommitOutcome::kCommitted) {
      return committed.ValueOrDie().status;
    }
    ++*transactions;
    ++*facts;
  }
  return Status::OK();
}

Status ExecutePublicOperation(Database* database, Snapshot snapshot,
                              QueryBenchmarkOperation op, uint32_t max_hops,
                              uint64_t limit, uint32_t readers,
                              uint64_t* rows,
                              uint64_t* first_result_us,
                              std::vector<VertexRef>* typed_vertex_rows = nullptr) {
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("vertex");
  Slot<EdgeRef> edge = Slot<EdgeRef>::Named("edge");
  Slot<VertexRef> destination = Slot<VertexRef>::Named("destination");
  Slot<PathValue> path = Slot<PathValue>::Named("path");
  Slot<JourneyValue> journey = Slot<JourneyValue>::Named("journey");
  StatusOr<Query> built(Status::InvalidArgument("query benchmark", "operation not public"));
  const TemporalScope state_scope = At{ValidTime{1}};
  const TemporalScope interval_scope = Events{ValidTimeInterval{ValidTime{0}, ValidTime{100}}};
  const ExpandSpec expand{vertex, edge, destination, ExpandDirection::kOut, 1};
  auto graph_vertices = [&]() -> StatusOr<Query> {
    auto base = Query::Vertices(vertex, state_scope);
    if (!base.ok()) return base.status();
    OptionalSlot<int64_t> score = OptionalSlot<int64_t>::Named("score");
    auto with_score = base.ValueOrDie().BindVertexProperty(
        vertex, BenchmarkGraph::kScore, score);
    if (!with_score.ok()) return with_score.status();
    return with_score.ValueOrDie().Where(IsPresent(score));
  };
  switch (op) {
    case QueryBenchmarkOperation::kStateAt:
      built = Query::Vertices(vertex, state_scope);
      break;
    case QueryBenchmarkOperation::kHistory:
      built = Query::Vertices(vertex, History{ValidTimeInterval{ValidTime{0}, ValidTime{100}}});
      break;
    case QueryBenchmarkOperation::kEvents:
      built = Query::Vertices(vertex, interval_scope);
      break;
    case QueryBenchmarkOperation::kChanges:
      built = Query::Vertices(vertex, Changes{ValidTimeInterval{ValidTime{0}, ValidTime{100}}});
      break;
    case QueryBenchmarkOperation::kExpandOut:
    case QueryBenchmarkOperation::kExpandIn:
    case QueryBenchmarkOperation::kExpandBoth: {
      auto base = graph_vertices();
      if (!base.ok()) return base.status();
      ExpandSpec spec = expand;
      spec.direction = op == QueryBenchmarkOperation::kExpandIn
                           ? ExpandDirection::kIn
                           : op == QueryBenchmarkOperation::kExpandBoth
                                 ? ExpandDirection::kBoth
                                 : ExpandDirection::kOut;
      built = base.ValueOrDie().Expand(spec);
      break;
    }
    case QueryBenchmarkOperation::kKHop: {
      auto base = graph_vertices();
      if (!base.ok()) return base.status();
      built = base.ValueOrDie().KHopExpand(expand, max_hops);
      break;
    }
    case QueryBenchmarkOperation::kCoexistingShortestPath: {
      auto base = graph_vertices();
      if (!base.ok()) return base.status();
      built = base.ValueOrDie().CoexistingShortestPath(expand, max_hops, path);
      break;
    }
    case QueryBenchmarkOperation::kEarliestArrival:
    case QueryBenchmarkOperation::kLatestDeparture:
    case QueryBenchmarkOperation::kFastestDuration: {
      auto base = Query::Vertices(vertex, state_scope);
      if (!base.ok()) return base.status();
      // The setup transaction marks the small graph fixture with score. Keep
      // the journey benchmark anchored to those graph vertices; otherwise a
      // write phase that appends thousands of unrelated vertices asks the
      // public journey operator to search from every one of them.
      OptionalSlot<int64_t> score = OptionalSlot<int64_t>::Named("score");
      auto with_score = base.ValueOrDie().BindVertexProperty(
          vertex, BenchmarkGraph::kScore, score);
      if (!with_score.ok()) return with_score.status();
      auto journey_vertices = with_score.ValueOrDie().Where(IsPresent(score));
      if (!journey_vertices.ok()) return journey_vertices.status();
      built = op == QueryBenchmarkOperation::kEarliestArrival
                  ? journey_vertices.ValueOrDie().EarliestArrival(
                        expand, max_hops, BenchmarkGraph::kDuration, journey)
                  : op == QueryBenchmarkOperation::kLatestDeparture
                        ? journey_vertices.ValueOrDie().LatestDeparture(
                              expand, max_hops, BenchmarkGraph::kDuration, journey)
                        : journey_vertices.ValueOrDie().FastestDuration(
                              expand, max_hops, BenchmarkGraph::kDuration, journey);
      break;
    }
    case QueryBenchmarkOperation::kPropertyFilter: {
      auto base = Query::Vertices(vertex, state_scope);
      if (!base.ok()) return base.status();
      OptionalSlot<int64_t> score = OptionalSlot<int64_t>::Named("score");
      auto bound = base.ValueOrDie().BindVertexProperty(
          vertex, BenchmarkGraph::kScore, score);
      if (!bound.ok()) return bound.status();
      built = bound.ValueOrDie().Where(
          IsPresent(score) &&
          GreaterThan(ValueOf(score), Literal<int64_t>(0)));
      break;
    }
    default:
      return Status::NotSupported("query benchmark", "operator has no public API");
  }
  if (!built.ok()) return built.status();
  StatusOr<Query> projected = built.ValueOrDie().Select([&] {
    switch (op) {
      case QueryBenchmarkOperation::kStateAt:
      case QueryBenchmarkOperation::kHistory:
      case QueryBenchmarkOperation::kEvents:
      case QueryBenchmarkOperation::kChanges:
        return std::vector<Projection>{Project(vertex)};
      case QueryBenchmarkOperation::kExpandOut:
      case QueryBenchmarkOperation::kExpandIn:
      case QueryBenchmarkOperation::kExpandBoth:
      case QueryBenchmarkOperation::kKHop:
        return std::vector<Projection>{Project(destination)};
      case QueryBenchmarkOperation::kCoexistingShortestPath:
        return std::vector<Projection>{Project(path)};
      case QueryBenchmarkOperation::kEarliestArrival:
      case QueryBenchmarkOperation::kLatestDeparture:
      case QueryBenchmarkOperation::kFastestDuration:
        return std::vector<Projection>{Project(journey)};
      case QueryBenchmarkOperation::kPropertyFilter:
        return std::vector<Projection>{Project(vertex)};
      default:
        return std::vector<Projection>{};
    }
  }());
  if (!projected.ok()) return projected.status();
  auto prepared = database->PrepareQuery(projected.ValueOrDie());
  if (!prepared.ok()) return prepared.status();
  QueryOptions options;
  options.mode = QueryExecutionMode::kInteractive;
  // The runtime materializes whole batches. Keep the benchmark's result cap
  // as a consumer-side limit while leaving enough budget for one batch; a
  // small cap must not turn a valid public query into ResourceExhausted.
  // Reserve for one bounded materialized batch. The consumer stops at
  // result_limit, but the canonical source may deliver a large batch before
  // that stop; the calibration lane runs with one reader and a Cedar-owned
  // 512 MiB pool.
  const uint64_t execution_budget = std::max<uint64_t>(limit, 1'000'000);
  constexpr uint64_t kBenchmarkQueryPoolMemoryBytes = 512ULL << 20;
  constexpr uint64_t kMinimumInteractiveQueryMemoryBytes = 8ULL << 20;
  // The benchmark's reader matrix shares a fixed Cedar query pool. Divide
  // that pool across the configured readers so 8/32 concurrent readers are
  // admitted without inflating the storage profile's global allocation.
  options.budget.memory_bytes = std::max<uint64_t>(
      kMinimumInteractiveQueryMemoryBytes,
      kBenchmarkQueryPoolMemoryBytes / std::max<uint32_t>(1, readers));
  options.budget.output_rows = execution_budget;
  options.budget.decoded_rows = execution_budget;
  auto cursor = prepared.ValueOrDie().Execute(std::move(snapshot), Bindings{}, options);
  if (!cursor.ok()) return cursor.status();
  uint64_t count = 0;
  const auto started = Clock::now();
  for (;;) {
    auto batch = cursor.ValueOrDie().Next();
    if (!batch.ok()) return batch.status();
    if (!batch.ValueOrDie().has_value()) break;
    const QueryBatch& query_batch = *batch.ValueOrDie();
    if (typed_vertex_rows != nullptr && op == QueryBenchmarkOperation::kStateAt) {
      for (size_t row = 0; row < query_batch.row_count(); ++row) {
        typed_vertex_rows->push_back(query_batch.Get<VertexRef>(vertex, row));
      }
    }
    count += query_batch.row_count();
    if (count >= limit) break;
  }
  *rows += std::min(count, limit);
  if (first_result_us != nullptr && count != 0) {
    *first_result_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - started).count());
  }
  return cursor.ValueOrDie().Close();
}

Status ExecuteOperation(Database* database, Snapshot snapshot,
                        QueryBenchmarkOperation op,
                        uint32_t max_hops, uint64_t limit, uint32_t readers,
                        uint64_t* rows,
                        uint64_t* first_result_us = nullptr,
                        std::vector<VertexRef>* typed_vertex_rows = nullptr) {
  if (!QueryBenchmarkOperationSupported(op)) {
    return Status::NotSupported("query benchmark",
                                QueryBenchmarkOperationName(op));
  }
  if (op != QueryBenchmarkOperation::kTemporalAggregate &&
      op != QueryBenchmarkOperation::kIntervalJoin &&
      op != QueryBenchmarkOperation::kEarliestArrival &&
      op != QueryBenchmarkOperation::kLatestDeparture &&
      op != QueryBenchmarkOperation::kFastestDuration) {
    return ExecutePublicOperation(database, std::move(snapshot), op, max_hops,
                                  limit, readers, rows, first_result_us,
                                  typed_vertex_rows);
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
      // Exercise the same typed canonical source/binder used by the query
      // runtime.  Keeping this path internal avoids treating a zero-row public
      // logical plan as a successful benchmark result while preserving typed
      // temporal/property semantics (including corrected valid-time chains).
      const ValidTimeInterval entity_range{ValidTime{0}, ValidTime{100}};
      auto entities = internal::TemporalSource::ReadHistory(
          snapshot, FactFamily::kVertexState, PropertyId{}, entity_range);
      if (!entities.ok()) return entities.status();
      const PropertyDefinition score_definition{
          BenchmarkGraph::kScore, 1, "score", PropertyEntityKind::kVertex,
          PhysicalType::kInt64, 4096};
      auto bound = internal::PropertyBinder::BindAt(
          snapshot, entities.ValueOrDie(), ValidTime{1}, score_definition);
      if (!bound.ok()) return bound.status();
      uint64_t count = 0;
      for (const auto& row : bound.ValueOrDie()) {
        if (row.value.has_value()) ++count;
      }
      return finish(count);
    }
    case QueryBenchmarkOperation::kTemporalAggregate: {
      std::vector<internal::RelationalRow> input_rows;
      for (int64_t entity = 1; entity <= 4; ++entity) {
        internal::RelationalRow row;
        row.cells.push_back(
            internal::RelationalCell::Present(QueryType::kInt64, 1));
        row.cells.push_back(
            internal::RelationalCell::Present(QueryType::kInt64, entity));
        row.effective = ValidTimeInterval{
            ValidTime{static_cast<uint64_t>(entity * 10)},
            std::optional<ValidTime>{
                ValidTime{static_cast<uint64_t>(entity * 10 + 10)}}};
        input_rows.push_back(std::move(row));
      }
      internal::QueryReservation reservation(32ULL << 20);
      internal::FragmentBudget fragments(1'000'000);
      auto aggregate = internal::TemporalAggregate(
          internal::TemporalAggregateInput{
              internal::BatchStream(std::move(input_rows)), {0}},
          &fragments, &reservation, limit);
      if (!aggregate.ok()) return aggregate.status();
      return finish(aggregate.ValueOrDie().rows.size());
    }
    case QueryBenchmarkOperation::kIntervalJoin: {
      std::vector<internal::RelationalRow> left, right;
      for (const auto& edge : {BenchmarkGraph{}.first, BenchmarkGraph{}.second,
                               BenchmarkGraph{}.direct}) {
        const int64_t key = static_cast<int64_t>(edge.edge_id.value);
        left.push_back({{internal::RelationalCell::Present(QueryType::kInt64, key)},
                        ValidTimeInterval{ValidTime{0}, ValidTime{100}}});
        right.push_back({{internal::RelationalCell::Present(QueryType::kInt64, key)},
                         ValidTimeInterval{ValidTime{50}, ValidTime{100}}});
      }
      internal::QueryReservation reservation(32ULL << 20);
      internal::FragmentBudget fragments(1'000'000);
      auto joined = internal::IntervalMergeJoin(
          internal::TemporalJoinInput{
              internal::BatchStream(std::move(left)),
              internal::BatchStream(std::move(right)), 0, 0,
              internal::JoinKind::kInner},
          &fragments, &reservation, limit);
      if (!joined.ok()) return joined.status();
      return finish(joined.ValueOrDie().rows.size());
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

Status AddWalManifestBytes(const std::string& root,
                           const std::vector<StorageFileInfo>& inspected,
                           QueryBenchmarkResult* result) {
  std::unordered_set<std::string> known;
  for (const auto& file : inspected) known.insert(file.relative_filename);
  std::error_code ec;
  std::filesystem::recursive_directory_iterator it(root, ec), end;
  if (ec) return Status::IOError("benchmark storage", ec.message());
  for (;
       it != end; it.increment(ec)) {
    if (ec) return Status::IOError("benchmark storage", ec.message());
    if (!it->is_regular_file(ec)) {
      if (ec) return Status::IOError("benchmark storage", ec.message());
      continue;
    }
    const auto name = it->path().filename().string();
    const bool wal = name == "CURRENT" || name == "LOG" ||
                     name.rfind("MANIFEST-", 0) == 0 ||
                     it->path().extension() == ".log";
    if (!wal) continue;
    const auto relative = std::filesystem::relative(it->path(), root, ec).string();
    if (ec) return Status::IOError("benchmark storage", ec.message());
    if (known.count(relative) != 0) continue;
    const uint64_t bytes = it->file_size(ec);
    if (ec) return Status::IOError("benchmark storage", ec.message());
    result->total_bytes += bytes;
    result->engine_internal_bytes += bytes;
    result->wal_manifest_bytes += bytes;
  }
  return Status::OK();
}

Status InspectAndAccountStorage(const std::string& path,
                                const ProductionStorageOptions& production,
                                QueryBenchmarkResult* result) {
  auto files = InspectStorageFiles(
      {path, StorageProfile::kProductionAppend, production});
  if (!files.ok()) return files.status();
  AddFileBytes(files.ValueOrDie(), result);
  return AddWalManifestBytes(path, files.ValueOrDie(), result);
}

void ComputeSpaceMetrics(QueryBenchmarkResult* result) {
  result->space_amplification = result->authoritative_bytes == 0
                                    ? 0.0
                                    : static_cast<double>(result->derived_bytes) /
                                          result->authoritative_bytes;
  result->total_space_amplification = result->authoritative_bytes == 0
                                          ? 0.0
                                          : static_cast<double>(result->total_bytes) /
                                                result->authoritative_bytes;
  result->write_amplification = result->authoritative_bytes == 0
                                    ? 0.0
                                    : static_cast<double>(result->authoritative_bytes +
                                                          result->derived_bytes) /
                                          result->authoritative_bytes;
}
}  // namespace

Status SeedQueryBenchmarkSetupForTesting(Database* database,
                                         uint64_t commit_deadline_us,
                                         std::function<void()> after_graph_commit_for_testing) {
  const BenchmarkGraph graph;
  if (Status status = SeedGraph(database, graph, commit_deadline_us);
      !status.ok()) {
    return status;
  }
  if (after_graph_commit_for_testing) after_graph_commit_for_testing();
  return SeedBenchmarkScore(database, graph, commit_deadline_us);
}

StatusOr<QueryBenchmarkResult> RunQueryBenchmark(
    const QueryBenchmarkOptions& options) {
  if (!options.verify_existing) {
    std::filesystem::remove_all(options.path);
  } else {
    std::error_code ec;
    if (!std::filesystem::exists(std::filesystem::path(options.path) / "CURRENT", ec)) {
      if (ec) return Status::IOError("benchmark", ec.message());
      return Status::NotFound("benchmark", "verify-existing path is not an existing database");
    }
  }
  DatabaseOptions db_options;
  db_options.path = options.path;
  db_options.storage_profile = StorageProfile::kProductionAppend;
  db_options.production.memory_budget_bytes = 1ULL << 30;
  db_options.production.kernel_mode = true;
  // Reserve enough of the fixed production budget for the benchmark's
  // 32-reader query matrix without allowing the default block cache to crowd
  // out Cedar's explicit query pool.
  db_options.production.block_cache_bytes = 128ULL << 20;
  db_options.group_commit_max_queue_requests = options.group_queue_requests;
  db_options.group_commit_max_queue_bytes = options.group_queue_bytes;
  // The benchmark intentionally exercises the 1/8/32-reader matrix. Expand
  // only the worker admission limit; query memory remains a fixed Cedar-owned
  // pool and each interactive query receives its proportional budget above.
  db_options.query_runtime.query_memory_bytes = 512ULL << 20;
  db_options.query_runtime.query_workers = std::max<uint32_t>(4, options.readers);
  db_options.query_runtime.projection_cache_bytes = 32ULL << 20;
  db_options.query_runtime.query_delta_bytes = 32ULL << 20;
  auto opened = Database::Open(db_options);
  if (!opened.ok()) return opened.status();
  auto database = std::move(opened).ConsumeValueOrDie();
  QueryBenchmarkResult result;
  result.raw_sample_path = options.path;
  result.query_api_surface =
      (options.operation == QueryBenchmarkOperation::kTemporalAggregate ||
       options.operation == QueryBenchmarkOperation::kIntervalJoin ||
       options.operation == QueryBenchmarkOperation::kEarliestArrival ||
       options.operation == QueryBenchmarkOperation::kLatestDeparture ||
       options.operation == QueryBenchmarkOperation::kFastestDuration)
          ? "internal-operator"
          : "public";
#ifdef NDEBUG
  result.build_type = "release";
#else
  result.build_type = "debug";
#endif
  result.seed = options.seed;
  result.dataset_checksum = 0;
  if (options.verify_existing) {
    StatusOr<std::pair<uint64_t, uint64_t>> initial(
        Status::InvalidArgument("benchmark", "initial checksum not attempted"));
    {
      auto snapshot = database->BeginSnapshot();
      if (!snapshot.ok()) return snapshot.status();
      initial = ScanFactChecksum(snapshot.ValueOrDie());
    }
    if (!initial.ok()) return initial.status();
    result.facts = initial.ValueOrDie().first;
    result.dataset_checksum = initial.ValueOrDie().second;
    const bool initial_match = result.facts == options.expected_facts &&
                               result.dataset_checksum == options.expected_checksum;

    result.terminal_status = initial_match ? "OK" : "existing verification failed";
    Status closed = database->Close();
    database.reset();
    if (!closed.ok()) {
      result.terminal_status = closed.ToString();
    } else {
      auto reopened = Database::Open(db_options);
      if (!reopened.ok()) {
        result.terminal_status = reopened.status().ToString();
      } else {
        database = std::move(reopened).ConsumeValueOrDie();
        StatusOr<std::pair<uint64_t, uint64_t>> rescanned(
            Status::InvalidArgument("benchmark", "reopen checksum not attempted"));
        {
          auto reopened_snapshot = database->BeginSnapshot();
          if (!reopened_snapshot.ok()) {
            result.terminal_status = reopened_snapshot.status().ToString();
          } else {
            rescanned = ScanFactChecksum(reopened_snapshot.ValueOrDie());
          }
        }
        if (rescanned.ok()) {
          const bool reopen_match = rescanned.ok() &&
                                    rescanned.ValueOrDie().first == options.expected_facts &&
                                    rescanned.ValueOrDie().second == options.expected_checksum;
          result.reopen_verified = initial_match && reopen_match;
          if (!result.reopen_verified) result.terminal_status = "reopen verification failed";
        } else {
          result.terminal_status = rescanned.status().ToString();
        }
        const Status reopened_closed = database->Close();
        if (!reopened_closed.ok()) {
          result.reopen_verified = false;
          result.terminal_status = reopened_closed.ToString();
        }
        database.reset();
      }
    }
    result.metrics_complete = true;
    const Status inspected = InspectAndAccountStorage(options.path,
                                                       db_options.production, &result);
    if (!inspected.ok()) {
      result.storage_inspection_status = inspected.ToString();
      result.terminal_status = inspected.ToString();
    }
    ComputeSpaceMetrics(&result);
    result.hard_gate_pass = result.terminal_status == "OK" &&
                            result.reopen_verified && inspected.ok() &&
                            (result.authoritative_bytes == 0 ||
                             result.derived_bytes <= result.authoritative_bytes * 3 / 2);
    result.gate_classification = result.hard_gate_pass ? "pass" : "incomplete";
    return result;
  }
  if (!QueryBenchmarkOperationSupported(options.operation)) {
    result.operation_supported = false;
    result.terminal_status = std::string("unsupported operation: ") +
                             QueryBenchmarkOperationName(options.operation);
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
  if (auto registered = database->RegisterProperty(PropertyDefinition{
          BenchmarkGraph::kScore, 0, "score", PropertyEntityKind::kVertex,
          PhysicalType::kInt64, 4096});
      !registered.ok()) {
    result.terminal_status = registered.status().ToString();
    result.gate_classification = "incomplete";
    return result;
  }
  if (Status status = SeedQueryBenchmarkSetupForTesting(
          database.get(), options.commit_deadline_us);
      !status.ok()) {
    result.terminal_status = status.ToString();
    result.gate_classification = "incomplete";
    return result;
  }
  const BenchmarkGraph graph;

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
  const auto seed_done = Clock::now();
  const uint64_t seed_facts = std::max<uint64_t>(options.degree * 4,
                                                  options.facts_per_txn);
  uint64_t seed_transaction_count = transactions.load();
  while (next_id.load() <= seed_facts) {
    const uint64_t first = next_id.fetch_add(options.facts_per_txn);
    uint64_t committed = 0;
    const Status s = CommitFacts(database.get(), first, options.facts_per_txn,
                                 &committed, options.commit_deadline_us);
    if (!s.ok()) return s;
    transactions.fetch_add(1);
    facts.fetch_add(committed);
    for (uint64_t id = first; id < first + committed; ++id)
      dataset_checksum.fetch_xor(id * kChecksumSalt);
  }
  const uint64_t projection_seed_facts = next_id.load() - 1;
  // Keep timed appends disjoint from the persisted projection base. The
  // benchmark graph and append facts share one Cedar partition so a complete
  // projection fixture can be read without a cross-partition fallback.
  next_id.store(projection_seed_facts + 1);
  seed_transaction_count = transactions.load();
  uint64_t projection_base_checksum = 0;
  const bool projection_requested =
      options.projection != ProjectionState::kCanonicalOnly;
  if (projection_requested || result.projection_active) {
    CommitSeq base_seq;
    {
      auto seeded_snapshot = database->BeginSnapshot();
      if (!seeded_snapshot.ok()) return seeded_snapshot.status();
      base_seq = seeded_snapshot.ValueOrDie().commit_seq();
    }
    if (Status closed = database->Close(); !closed.ok()) return closed;
    database.reset();
    if (Status built = BuildBenchmarkProjection(options.path, graph, base_seq,
                                                projection_seed_facts,
                                                options.projection);
        !built.ok()) {
      result.terminal_status = built.ToString();
      result.gate_classification = "incomplete";
      return result;
    }
    auto reopened = Database::Open(db_options);
    if (!reopened.ok()) return reopened.status();
    database = std::move(reopened).ConsumeValueOrDie();
    auto refreshed = database->RefreshQueryStatistics();
    if (!refreshed.ok()) {
      result.terminal_status = refreshed.status().ToString();
      result.gate_classification = "incomplete";
      return result;
    }
    maintenance = std::move(refreshed).ConsumeValueOrDie();
    const Status awaited = maintenance->Await();
    if (!awaited.ok()) {
      result.terminal_status = awaited.ToString();
      result.gate_classification = "incomplete";
      return result;
    }
    result.maintenance_status = "refresh-complete";
    result.maintenance_observed = true;

    std::vector<VertexRef> projection_base_rows;
    if (options.projection == ProjectionState::kShortDelta ||
        options.projection == ProjectionState::kLongDelta) {
      auto base_snapshot = database->BeginSnapshot();
      if (!base_snapshot.ok()) return base_snapshot.status();
      uint64_t base_count = 0;
      Status base_query = ExecuteOperation(
          database.get(), std::move(base_snapshot).ConsumeValueOrDie(),
          QueryBenchmarkOperation::kStateAt, options.max_hops,
          std::numeric_limits<uint64_t>::max(), 1, &base_count, nullptr,
          &projection_base_rows);
      if (!base_query.ok()) {
        result.terminal_status = base_query.ToString();
        result.gate_classification = "incomplete";
        return result;
      }
      if (base_count != projection_base_rows.size()) {
        result.terminal_status =
            "projection base typed row accounting mismatch";
        result.gate_classification = "incomplete";
        return result;
      }
      projection_base_checksum = VertexRowsChecksum(projection_base_rows);
    }

    // Keep the projection base immutable, then create a real contiguous
    // QueryDelta for the delta fixtures. The mutations are included in the
    // setup watermark below, so measured write throughput excludes fixture
    // construction while query correctness still observes the tail.
    if (options.projection == ProjectionState::kShortDelta ||
        options.projection == ProjectionState::kLongDelta) {
      const uint64_t delta_facts =
          options.projection == ProjectionState::kShortDelta ? 1 : 128;
      uint64_t delta_transactions = 0;
      uint64_t delta_committed_facts = 0;
      if (Status status = ApplyProjectionDelta(
              database.get(), 1, delta_facts,
              options.commit_deadline_us, &delta_transactions,
              &delta_committed_facts);
          !status.ok()) {
        result.terminal_status = status.ToString();
        result.gate_classification = "incomplete";
        return result;
      }
      transactions.fetch_add(delta_transactions);
      facts.fetch_add(delta_committed_facts);
      // Reopen after constructing the tail so Database::Open performs the
      // authoritative sequence-range repair before the differential check.
      // This makes the fixture deterministic instead of racing the async
      // QueryDelta index worker.
      if (Status closed = database->Close(); !closed.ok()) return closed;
      database.reset();
      auto delta_reopened = Database::Open(db_options);
      if (!delta_reopened.ok()) return delta_reopened.status();
      database = std::move(delta_reopened).ConsumeValueOrDie();
    }
    {
      auto setup_snapshot = database->BeginSnapshot();
      if (!setup_snapshot.ok()) return setup_snapshot.status();
      seeded_checksum = ScanFactChecksum(setup_snapshot.ValueOrDie());
      if (!seeded_checksum.ok()) return seeded_checksum.status();
      seed_transaction_count = transactions.load();
    }
  }
  if (projection_requested) {
    // Validate the persisted fixture against canonical truth before timed
    // samples. This catches a projection/QueryDelta path that merely passes
    // lifecycle gates while dropping a corrected interval or retract tail.
    auto fixture_snapshot = database->BeginSnapshot();
    if (!fixture_snapshot.ok()) return fixture_snapshot.status();
    auto canonical_rows = internal::TemporalSource::ReadAt(
        fixture_snapshot.ValueOrDie(), FactFamily::kVertexState, PropertyId{},
        ValidTime{1});
    if (!canonical_rows.ok()) return canonical_rows.status();
    uint64_t projected_rows = 0;
    std::vector<VertexRef> projected_vertex_rows;
    Status fixture_query = ExecuteOperation(
        database.get(), std::move(fixture_snapshot).ConsumeValueOrDie(),
        QueryBenchmarkOperation::kStateAt, options.max_hops,
        std::numeric_limits<uint64_t>::max(), 1, &projected_rows, nullptr,
        &projected_vertex_rows);
    if (!fixture_query.ok()) {
      result.terminal_status = fixture_query.ToString();
      result.gate_classification = "incomplete";
      return result;
    }
    std::vector<VertexRef> canonical_vertex_rows;
    canonical_vertex_rows.reserve(canonical_rows.ValueOrDie().size());
    for (const auto& row : canonical_rows.ValueOrDie()) {
      canonical_vertex_rows.push_back(
          VertexRef{row.ref.part_id(), VertexId{row.ref.entity_id()}});
    }
    const auto sort_vertices = [](std::vector<VertexRef>* rows) {
      std::sort(rows->begin(), rows->end(), [](const VertexRef& left,
                                               const VertexRef& right) {
        return std::tie(left.part_id.value, left.vertex_id.value) <
               std::tie(right.part_id.value, right.vertex_id.value);
      });
    };
    sort_vertices(&canonical_vertex_rows);
    sort_vertices(&projected_vertex_rows);
    if (projected_rows != canonical_vertex_rows.size() ||
        projected_vertex_rows != canonical_vertex_rows) {
      result.terminal_status =
          "projection fixture differs from canonical expected=" +
          std::to_string(canonical_vertex_rows.size()) +
          " observed=" + std::to_string(projected_rows);
      result.gate_classification = "incomplete";
      return result;
    }
    if ((options.projection == ProjectionState::kShortDelta ||
         options.projection == ProjectionState::kLongDelta) &&
        VertexRowsChecksum(projected_vertex_rows) == projection_base_checksum) {
      result.terminal_status =
          "projection delta did not change typed state-at result";
      result.gate_classification = "incomplete";
      return result;
    }
  }
  const CommitPipelineMetrics pipeline_before_write =
      database->GetCommitPipelineMetrics();
  const auto write_start = Clock::now();
  const auto deadline = write_start +
                        std::chrono::seconds(options.duration_seconds);
  const auto writer = [&] {
    while (Clock::now() < deadline) {
      const uint64_t first = next_id.fetch_add(options.facts_per_txn);
      const auto start = Clock::now();
      uint64_t committed = 0;
      const Status s = CommitFacts(database.get(), first, options.facts_per_txn,
                                   &committed, options.commit_deadline_us);
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
  const CommitPipelineMetrics pipeline_after_write =
      database->GetCommitPipelineMetrics();
  if (failed.load()) result.terminal_status = failure;

  if (options.cache == QueryCacheState::kWarm) {
    auto warm_snapshot = database->BeginSnapshot();
    if (!warm_snapshot.ok()) return warm_snapshot.status();
    uint64_t warm_rows = 0;
    const Status warm_status = ExecuteOperation(
        database.get(), std::move(warm_snapshot).ConsumeValueOrDie(), options.operation, options.max_hops,
        options.result_limit, options.readers,
        &warm_rows);
    if (!warm_status.ok()) return warm_status;
    result.cache_conditioned = true;
  }
  const auto query_start = Clock::now();
  const auto runtime_metrics_before = database->SampleRuntimeMetrics();
  const auto query_deadline = query_start +
                              std::chrono::seconds(options.duration_seconds);
  std::vector<uint64_t> query_samples;
  std::vector<uint64_t> first_result_samples;
  std::mutex query_mutex;
  std::atomic<uint64_t> rows{0};
  std::atomic<uint64_t> query_operations{0};
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
      const Status s = ExecuteOperation(database.get(), std::move(snapshot).ConsumeValueOrDie(), options.operation,
                                        options.max_hops, options.result_limit, options.readers,
                                        &local_rows, &first_result_us);
      if (!s.ok()) {
        std::lock_guard<std::mutex> lock(query_mutex);
        result.terminal_status = s.ToString();
        return;
      }
      executed = true;
      query_operations.fetch_add(1);
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
  const auto runtime_metrics_after = database->SampleRuntimeMetrics();
  result.elapsed_seconds = std::chrono::duration<double>(query_done - run_start).count();
  result.write_elapsed_seconds = std::chrono::duration<double>(write_done - write_start).count();
  result.query_elapsed_seconds = std::chrono::duration<double>(query_done - query_start).count();
  if (runtime_metrics_before.ok() && runtime_metrics_after.ok()) {
    const RuntimeMetrics& before = runtime_metrics_before.ValueOrDie();
    const RuntimeMetrics& after = runtime_metrics_after.ValueOrDie();
    const bool counters_monotonic =
        after.canonical_read_physical_bytes >=
            before.canonical_read_physical_bytes &&
        after.projected_scan_physical_bytes_read >=
            before.projected_scan_physical_bytes_read;
    if (counters_monotonic) {
      result.query_physical_bytes = NondecreasingDelta(
          after.canonical_read_physical_bytes,
          before.canonical_read_physical_bytes);
      result.query_physical_bytes =
          result.query_physical_bytes >
                  std::numeric_limits<uint64_t>::max() -
                      NondecreasingDelta(after.projected_scan_physical_bytes_read,
                                         before.projected_scan_physical_bytes_read)
              ? std::numeric_limits<uint64_t>::max()
              : result.query_physical_bytes +
                    NondecreasingDelta(after.projected_scan_physical_bytes_read,
                                       before.projected_scan_physical_bytes_read);
      // A successful sample with a zero delta is a valid cache-resident query.
      result.query_bytes_complete = true;
    }
  }
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
  result.query_operations = query_operations.load();
  result.query_samples = query_samples.size();
  result.query_p50_us = Percentile(query_samples, 50);
  result.query_p95_us = Percentile(query_samples, 95);
  result.query_p99_us = Percentile(query_samples, 99);
  result.first_result_p50_us = Percentile(first_result_samples, 50);
  result.write_p50_us = Percentile(write_samples, 50);
  result.write_p95_us = Percentile(write_samples, 95);
  result.write_p99_us = Percentile(write_samples, 99);
  const CommitPipelineMetrics pipeline = database->GetCommitPipelineMetrics();
  const CommitGroupFillMetrics timed_group_fill =
      GroupFillDelta(pipeline_before_write.group_fill,
                     pipeline_after_write.group_fill);
  result.wal_sync_p99_us =
      pipeline.latency.wal_sync.ApproximatePercentile(99);
  result.end_to_end_p99_us =
      pipeline.latency.end_to_end.ApproximatePercentile(99);
  result.query_qps = result.query_elapsed_seconds > 0
                        ? static_cast<double>(result.query_operations) /
                              result.query_elapsed_seconds
                        : 0.0;
  result.rows_per_second = result.query_elapsed_seconds > 0
                               ? static_cast<double>(result.rows) /
                                     result.query_elapsed_seconds
                               : 0.0;
  result.group_fill_p50 = timed_group_fill.ApproximatePercentile(50);
  result.metrics_complete = pipeline.latency.wal_sync.count > 0 &&
                            pipeline.latency.end_to_end.count > 0 &&
                            !query_samples.empty() && result.rows > 0;

  // The incremental writer checksum is an execution aid; publish the
  // canonical scan answer so artifacts can be reopened and verified later.
  {
    auto final_snapshot = database->BeginSnapshot();
    if (!final_snapshot.ok()) {
      result.terminal_status = final_snapshot.status().ToString();
    } else {
      const auto final_scan = ScanFactChecksum(final_snapshot.ValueOrDie());
      if (!final_scan.ok()) {
        result.terminal_status = final_scan.status().ToString();
      } else {
        result.facts = final_scan.ValueOrDie().first;
        result.dataset_checksum = final_scan.ValueOrDie().second;
      }
    }
  }

  if (options.verify_reopen && result.terminal_status == "OK") {
    const Status closed = database->Close();
    if (!closed.ok()) result.terminal_status = closed.ToString();
    database.reset();
    if (result.terminal_status == "OK") {
      auto reopened = Database::Open(db_options);
      if (!reopened.ok()) {
        result.terminal_status = reopened.status().ToString();
      } else {
        StatusOr<std::pair<uint64_t, uint64_t>> scanned(
            Status::InvalidArgument("benchmark", "reopen checksum not attempted"));
        {
          auto snapshot = reopened.ValueOrDie()->BeginSnapshot();
          scanned = snapshot.ok()
                        ? ScanFactChecksum(snapshot.ValueOrDie())
                        : StatusOr<std::pair<uint64_t, uint64_t>>(snapshot.status());
        }
        result.reopen_verified = scanned.ok() &&
                                 scanned.ValueOrDie().first == result.facts &&
                                 scanned.ValueOrDie().second == result.dataset_checksum;
        if (!result.reopen_verified) result.terminal_status = "reopen verification failed";
        const Status reopened_closed = reopened.ValueOrDie()->Close();
        if (!reopened_closed.ok()) {
          result.reopen_verified = false;
          result.terminal_status = reopened_closed.ToString();
        }
      }
    }
  }
  const Status inspected = InspectAndAccountStorage(options.path,
                                                     db_options.production, &result);
  if (!inspected.ok()) {
    const Status& status = inspected;
    result.storage_inspection_status = status.ToString();
    result.terminal_status = result.storage_inspection_status;
  }
  result.mib_per_second = result.elapsed_seconds > 0
                              ? static_cast<double>(result.total_bytes) /
                                    (1024.0 * 1024.0 * result.elapsed_seconds)
                              : 0.0;
  result.write_mib_per_second = 0.0;
  result.query_mib_per_second = result.query_bytes_complete &&
                                        result.query_elapsed_seconds > 0
                                    ? static_cast<double>(result.query_physical_bytes) /
                                          (1024.0 * 1024.0 * result.query_elapsed_seconds)
                                    : 0.0;
  ComputeSpaceMetrics(&result);
  result.hard_gate_pass = result.metrics_complete &&
                          inspected.ok() &&
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
  return "operation,projection_state,degree,selectivity_percent,readers,cache_state,writers,facts_per_txn,commit_deadline_us,group_queue_requests,group_queue_bytes,seed,verify_existing,expected_facts,expected_checksum,dataset_checksum,transactions,facts,measured_transactions,measured_facts,rows,elapsed_seconds,write_elapsed_seconds,query_elapsed_seconds,transactions_per_second,facts_per_second,query_qps,rows_per_second,mib_per_second,write_mib_per_second,query_mib_per_second,query_physical_bytes,query_bytes_complete,group_fill_p50,query_samples,query_p50_us,query_p95_us,query_p99_us,first_result_p50_us,write_p50_us,write_p95_us,write_p99_us,wal_sync_p99_us,end_to_end_p99_us,authoritative_bytes,adjacency_bytes,property_bytes,statistics_bytes,derived_bytes,scratch_bytes,engine_internal_bytes,wal_manifest_bytes,total_bytes,write_amplification,space_amplification,total_space_amplification,projection_lag,projection_work,maintenance_status,maintenance_observed,cache_conditioned,query_api_surface,operation_supported,projection_state_supported,metrics_complete,build_type,sanitizer,host,plan_fingerprint,raw_sample_path,storage_inspection_status,terminal_status,reopen_verified,gate_classification,hard_gate_pass";
}

std::string QueryBenchmarkCsvRow(const QueryBenchmarkOptions& o,
                                 const QueryBenchmarkResult& r) {
  std::ostringstream x;
  const double t = r.write_elapsed_seconds ? r.measured_transactions / r.write_elapsed_seconds : 0;
  const double f = r.write_elapsed_seconds ? r.measured_facts / r.write_elapsed_seconds : 0;
  x << CsvEscape(QueryBenchmarkOperationName(o.operation)) << ','
    << CsvEscape(ProjectionStateName(o.projection))
    << ',' << o.degree << ',' << o.selectivity_percent << ',' << o.readers << ','
    << CsvEscape(o.cache == QueryCacheState::kCold ? "cold" : "warm") << ',' << o.writers << ','
    << o.facts_per_txn << ',' << o.commit_deadline_us << ','
    << o.group_queue_requests << ',' << o.group_queue_bytes << ',' << o.seed << ','
    << (o.verify_existing ? "true" : "false") << ',' << o.expected_facts << ','
    << o.expected_checksum << ',' << r.dataset_checksum << ','
    << r.transactions << ',' << r.facts << ',' << r.measured_transactions << ','
    << r.measured_facts << ',' << r.rows << ','
    << r.elapsed_seconds << ',' << r.write_elapsed_seconds << ',' << r.query_elapsed_seconds << ','
    << t << ',' << f << ',' << r.query_qps << ',' << r.rows_per_second << ',' << r.mib_per_second << ','
    << r.write_mib_per_second << ',' << r.query_mib_per_second << ','
    << r.query_physical_bytes << ','
    << (r.query_bytes_complete ? "true" : "false") << ','
    << r.group_fill_p50 << ',' << r.query_samples << ','
    << r.query_p50_us << ',' << r.query_p95_us << ',' << r.query_p99_us << ','
    << r.first_result_p50_us << ','
    << r.write_p50_us << ',' << r.write_p95_us << ',' << r.write_p99_us << ','
    << r.wal_sync_p99_us << ',' << r.end_to_end_p99_us << ',' << r.authoritative_bytes << ','
    << r.adjacency_bytes << ',' << r.property_bytes << ',' << r.statistics_bytes << ','
    << r.derived_bytes << ',' << r.scratch_bytes << ',' << r.engine_internal_bytes << ','
    << r.wal_manifest_bytes << ',' << r.total_bytes << ','
    << r.write_amplification << ',' << r.space_amplification << ','
    << r.total_space_amplification << ','
    << r.projection_lag << ',' << CsvEscape(r.projection_active ? "active" : "paused") << ','
    << CsvEscape(r.maintenance_status) << ',' << (r.maintenance_observed ? "true" : "false") << ','
    << (r.cache_conditioned ? "true" : "false") << ','
    << CsvEscape(r.query_api_surface) << ','
    << (r.operation_supported ? "true" : "false") << ','
    << (r.projection_state_supported ? "true" : "false") << ','
    << (r.metrics_complete ? "true" : "false") << ','
    << CsvEscape(r.build_type) << ',' << CsvEscape(r.sanitizer) << ','
    << CsvEscape(r.host) << ',' << CsvEscape(r.plan_fingerprint) << ','
    << CsvEscape(r.raw_sample_path) << ','
    << CsvEscape(r.storage_inspection_status) << ','
    << CsvEscape(r.terminal_status) << ',' << (r.reopen_verified ? "true" : "false") << ','
    << CsvEscape(r.gate_classification) << ',' << (r.hard_gate_pass ? "true" : "false");
  return x.str();
}

std::string QueryBenchmarkJson(const QueryBenchmarkOptions& o,
                               const QueryBenchmarkResult& r) {
  std::ostringstream x;
  x << "{\"operation\":\"" << JsonEscape(QueryBenchmarkOperationName(o.operation))
    << "\",\"projection_state\":\"" << JsonEscape(ProjectionStateName(o.projection))
    << "\",\"writers\":" << o.writers << ",\"facts_per_txn\":"
    << o.facts_per_txn << ",\"commit_deadline_us\":" << o.commit_deadline_us
    << ",\"group_queue_requests\":" << o.group_queue_requests
    << ",\"group_queue_bytes\":" << o.group_queue_bytes
    << ",\"verify_existing\":"
    << (o.verify_existing ? "true" : "false") << ",\"expected_facts\":"
    << o.expected_facts << ",\"expected_checksum\":" << o.expected_checksum
    << ",\"transactions\":" << r.transactions
    << ",\"facts\":" << r.facts << ",\"rows\":" << r.rows
    << ",\"query_operations\":" << r.query_operations
    << ",\"measured_transactions\":" << r.measured_transactions
    << ",\"measured_facts\":" << r.measured_facts
    << ",\"query_p50_us\":" << r.query_p50_us << ",\"query_p95_us\":"
    << r.query_p95_us << ",\"query_p99_us\":" << r.query_p99_us
    << ",\"first_result_p50_us\":" << r.first_result_p50_us
    << ",\"query_qps\":" << r.query_qps
    << ",\"rows_per_second\":" << r.rows_per_second
    << ",\"authoritative_bytes\":" << r.authoritative_bytes
    << ",\"derived_bytes\":" << r.derived_bytes
    << ",\"adjacency_bytes\":" << r.adjacency_bytes
    << ",\"property_bytes\":" << r.property_bytes
    << ",\"statistics_bytes\":" << r.statistics_bytes
    << ",\"scratch_bytes\":" << r.scratch_bytes
    << ",\"engine_internal_bytes\":" << r.engine_internal_bytes
    << ",\"wal_manifest_bytes\":" << r.wal_manifest_bytes
    << ",\"mib_per_second\":" << r.mib_per_second
    << ",\"write_mib_per_second\":" << r.write_mib_per_second
    << ",\"query_mib_per_second\":" << r.query_mib_per_second
    << ",\"query_physical_bytes\":" << r.query_physical_bytes
    << ",\"query_bytes_complete\":" << (r.query_bytes_complete ? "true" : "false")
    << ",\"group_fill_p50\":" << r.group_fill_p50
    << ",\"write_amplification\":" << r.write_amplification
    << ",\"space_amplification\":" << r.space_amplification
    << ",\"total_space_amplification\":" << r.total_space_amplification
    << ",\"build_type\":\"" << JsonEscape(r.build_type)
    << "\",\"sanitizer\":\"" << JsonEscape(r.sanitizer)
    << "\",\"host\":\"" << JsonEscape(r.host)
    << "\",\"plan_fingerprint\":\"" << JsonEscape(r.plan_fingerprint)
    << "\",\"raw_sample_path\":\"" << JsonEscape(r.raw_sample_path)
    << "\",\"seed\":" << o.seed << ",\"dataset_checksum\":"
    << r.dataset_checksum << ",\"cache_conditioned\":"
    << (r.cache_conditioned ? "true" : "false")
    << ",\"query_api_surface\":\"" << JsonEscape(r.query_api_surface) << "\""
    << ",\"operation_supported\":" << (r.operation_supported ? "true" : "false")
    << ",\"projection_state_supported\":"
    << (r.projection_state_supported ? "true" : "false")
    << ",\"metrics_complete\":" << (r.metrics_complete ? "true" : "false")
    << ",\"maintenance_status\":\"" << JsonEscape(r.maintenance_status)
    << "\",\"maintenance_observed\":"
    << (r.maintenance_observed ? "true" : "false")
    << ",\"storage_inspection_status\":\"" << JsonEscape(r.storage_inspection_status) << "\""
    << ",\"terminal_status\":\"" << JsonEscape(r.terminal_status)
    << "\",\"reopen_verified\":" << (r.reopen_verified ? "true" : "false")
    << ",\"gate_classification\":\"" << JsonEscape(r.gate_classification)
    << "\",\"hard_gate_pass\":" << (r.hard_gate_pass ? "true" : "false") << '}';
  return x.str();
}
}  // namespace cedar::benchmark
