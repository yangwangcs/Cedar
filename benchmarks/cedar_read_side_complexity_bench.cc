#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "cedar/cypher/relationship_type.h"
#include "cedar/cypher/binder.h"
#include "cedar/cypher/compiler.h"
#include "cedar/cypher/parser.h"
#include "cedar/cypher/session.h"
#include "cedar/database.h"
#include "cedar/fact/fact_codec.h"
#include "query/projection/projection_format.h"
#include "query/projection/projection_page_reader.h"
#include "query/resource/query_scratch.h"
#include "query/runtime/relational.h"

namespace {
using Clock = std::chrono::steady_clock;
bool g_long_reader = false;

uint64_t PeakRssBytes() {
  struct rusage usage {};
  if (::getrusage(RUSAGE_SELF, &usage) != 0) return 0;
#if defined(__APPLE__)
  return static_cast<uint64_t>(usage.ru_maxrss);
#else
  return static_cast<uint64_t>(usage.ru_maxrss) * 1024ULL;
#endif
}

uint64_t Percentile(std::vector<uint64_t> values, size_t percentile) {
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  return values[(values.size() - 1) * percentile / 100];
}

uint32_t ParseEntities(int argc, char** argv) {
  uint32_t entities = 100;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--entities" && i + 1 < argc) entities = std::stoul(argv[++i]);
    else if (arg == "--seconds" && i + 1 < argc) ++i;
    else if (arg == "--long-reader") g_long_reader = true;
    else return 0;
  }
  return entities;
}

struct Measurement {
  uint64_t input_rows = 0;
  uint64_t output_rows = 0;
  uint64_t physical_bytes = 0;
  uint64_t decoded_bytes = 0;
  uint64_t pages_read = 0;
  uint64_t pages_skipped = 0;
  uint64_t catalog_hits = 0;
  uint64_t catalog_misses = 0;
  uint64_t errors = 0;
  std::string first_error;
  std::vector<uint64_t> samples;
};

template <typename Fn>
Measurement RunRepeated(uint64_t input_rows, uint64_t seconds, Fn&& fn) {
  Measurement result;
  result.input_rows = input_rows;
  const auto deadline = Clock::now() + std::chrono::seconds(seconds);
  bool first = true;
  while (first || (input_rows < 1000 && Clock::now() < deadline)) {
    first = false;
    const auto started = Clock::now();
    auto one = fn();
    if (!one.ok()) {
      ++result.errors;
      if (result.first_error.empty()) result.first_error = one.status().ToString();
      continue;
    }
    result.output_rows += one.ValueOrDie().first;
    result.physical_bytes += one.ValueOrDie().second;
    result.samples.push_back(static_cast<uint64_t>(std::chrono::duration_cast<
        std::chrono::microseconds>(Clock::now() - started).count()));
  }
  return result;
}

void PrintRow(const std::string& workload, const Measurement& m,
              const std::string& source, uint64_t rss) {
  std::cout << workload << ',' << m.input_rows << ',' << m.output_rows << ',' << source
            << ',' << m.physical_bytes << ',' << m.decoded_bytes << ',' << m.pages_read
            << ',' << m.pages_skipped << ',' << m.catalog_hits << ',' << m.catalog_misses
            << ',' << Percentile(m.samples, 50) << ',' << Percentile(m.samples, 95)
            << ',' << Percentile(m.samples, 99) << ',' << rss << ',' << m.errors << '\n';
}

}  // namespace

int main(int argc, char** argv) {
  const uint32_t entities = ParseEntities(argc, argv);
  if (entities == 0) return 2;
  uint64_t seconds = 1;
  for (int i = 1; i + 1 < argc; ++i) {
    if (std::string(argv[i]) == "--seconds") seconds = std::stoull(argv[i + 1]);
  }
  for (int i = 1; i < argc; ++i) if (std::string(argv[i]) == "--long-reader") g_long_reader = true;
  if (seconds == 0) return 2;

  char path[] = "/tmp/cedar_read_side_complexity_XXXXXX";
  if (::mkdtemp(path) == nullptr) return 1;
  cedar::DatabaseOptions options;
  options.path = path;
  options.query_runtime.query_workers = 2;
  options.query_runtime.reserved_interactive_workers = 1;
  options.query_runtime.query_memory_bytes = 512ULL << 20;
  auto database = cedar::Database::Open(options);
  if (!database.ok()) { std::cerr << database.status().ToString() << '\n'; return 1; }
  std::vector<cedar::PropertyDefinition> properties;
  for (uint16_t property = 7; property < 23; ++property) {
    cedar::PropertyDefinition definition{cedar::PropertyId{property}, 0,
                                         "score" + std::to_string(property),
                                         cedar::PropertyEntityKind::kVertex,
                                         cedar::PhysicalType::kInt64, 4096};
    auto registered = database.ValueOrDie()->RegisterProperty(definition);
    if (!registered.ok()) {
      std::cerr << registered.status().ToString() << '\n'; return 1;
    }
    properties.push_back(registered.ValueOrDie());
  }
  cedar::cypher::SchemaCatalog schema;
  for (const auto& property : properties) {
    if (auto status = schema.Add(property); !status.ok()) {
      std::cerr << status.ToString() << '\n'; return 1;
    }
  }
  const uint64_t knows = cedar::cypher::ResolveRelationshipType("KNOWS").ValueOrDie();
  const uint32_t kSetupBatch = entities >= 10000 ? 256 : 32;
  for (uint32_t base = 1; base <= entities; base += kSetupBatch) {
    auto transaction = database.ValueOrDie()->BeginTransaction();
    if (!transaction.ok()) { std::cerr << transaction.status().ToString() << '\n'; return 1; }
    const uint32_t end = std::min<uint32_t>(entities + 1, base + kSetupBatch);
    for (uint32_t i = base; i < end; ++i) {
      const cedar::VertexRef vertex{cedar::PartId{0}, cedar::VertexId{i}};
      if (!transaction.ValueOrDie()->Assert(cedar::EntityFact::Vertex(vertex),
                                            cedar::ValidTime{1}).ok() ||
          !transaction.ValueOrDie()->Set(
              cedar::PropertyFact::Vertex(vertex, properties.front().property_id),
              cedar::ValidTime{1}, cedar::Value::Int64(i % 16)).ok()) {
        std::cerr << "fixture write failed at " << i << '\n'; return 1;
      }
      for (size_t p = 1; p < properties.size() && entities < 10000; ++p) {
        if (!transaction.ValueOrDie()->Set(
                cedar::PropertyFact::Vertex(vertex, properties[p].property_id),
                cedar::ValidTime{1}, cedar::Value::Int64((i + p) % 16)).ok()) {
          std::cerr << "property fixture write failed at " << i << '\n'; return 1;
        }
      }
      if (i > 1) {
        const cedar::VertexRef previous{cedar::PartId{0}, cedar::VertexId{i - 1}};
        const cedar::EdgeIdentity edge{
            cedar::EdgeRef{cedar::PartId{0}, cedar::EdgeId{i}}, previous, vertex, knows};
        if (!transaction.ValueOrDie()->Assert(edge, cedar::ValidTime{1}).ok()) {
          std::cerr << "edge fixture write failed at " << i << '\n'; return 1;
        }
      }
    }
    auto committed = transaction.ValueOrDie()->Commit();
    if (!committed.ok() || committed.ValueOrDie().outcome != cedar::CommitOutcome::kCommitted) {
      std::cerr << (committed.ok() ? committed.ValueOrDie().status.ToString()
                                    : committed.status().ToString()) << '\n'; return 1;
    }
  }

  cedar::cypher::CypherSession session(*database.ValueOrDie(), schema);
  auto point = session.Prepare("FOR VALID_TIME AS OF 1 MATCH (v) RETURN v");
  auto expansion = session.Prepare(
      "FOR VALID_TIME AS OF 1 MATCH (a)-[e:KNOWS]->(b) RETURN a, e, b");
  auto history = session.Prepare(
      "CHANGES FOR VALID_TIME BETWEEN 1 AND 100 MATCH (v) RETURN v");
  auto history_narrow = session.Prepare(
      "CHANGES FOR VALID_TIME BETWEEN 1 AND 10 MATCH (v) RETURN v");
  if (!point.ok() || !expansion.ok() || !history.ok() || !history_narrow.ok()) {
    if (!point.ok()) std::cerr << point.status().ToString() << '\n';
    if (!expansion.ok()) std::cerr << expansion.status().ToString() << '\n';
    if (!history.ok()) std::cerr << history.status().ToString() << '\n';
    if (!history_narrow.ok()) std::cerr << history_narrow.status().ToString() << '\n';
    return 1;
  }
  auto expansion_parsed = cedar::cypher::Parse(
      "FOR VALID_TIME AS OF 1 MATCH (a)-[e:KNOWS]->(b) RETURN a, e, b");
  if (!expansion_parsed.ok()) return 1;
  auto expansion_bound = cedar::cypher::Bind(expansion_parsed.ValueOrDie(), schema,
                                             cedar::cypher::BinderOptions{});
  if (!expansion_bound.ok()) return 1;
  auto expansion_query = cedar::cypher::Compile(expansion_bound.ValueOrDie());
  if (!expansion_query.ok()) return 1;
  auto expansion_prepared = database.ValueOrDie()->PrepareQuery(
      expansion_query.ValueOrDie());
  if (!expansion_prepared.ok()) return 1;

  auto run_cursor = [&](const cedar::cypher::PreparedCypher& prepared,
                        cedar::QueryExecutionMode mode = cedar::QueryExecutionMode::kInteractive)
      -> cedar::StatusOr<std::pair<uint64_t, uint64_t>> {
    cedar::cypher::CypherRequest request;
    request.options.capture_profile = true;
    request.options.mode = mode;
    request.options.budget.memory_bytes = 512ULL << 20;
    auto cursor = session.Execute(prepared, request);
    if (!cursor.ok()) return cursor.status();
    uint64_t rows = 0, bytes = 0;
    while (true) {
      auto batch = cursor.ValueOrDie().Next();
      if (!batch.ok()) return batch.status();
      if (!batch.ValueOrDie().has_value()) break;
      rows += batch.ValueOrDie()->row_count();
    }
    for (const auto& profile : cursor.ValueOrDie().profile().operators) bytes += profile.physical_bytes;
    return std::pair<uint64_t, uint64_t>{rows, bytes};
  };
  if (g_long_reader) {
    std::cout << "workload,input_rows,output_rows,source,physical_bytes,decoded_bytes,"
                 "pages_read,pages_skipped,catalog_hits,catalog_misses,p50_us,p95_us,"
                 "p99_us,rss_bytes,error\n";
    const auto long_result = RunRepeated(entities, seconds,
                                         [&]() { return run_cursor(point.ValueOrDie()); });
    PrintRow("long_reader_point_state", long_result, "canonical", PeakRssBytes());
    database.ValueOrDie()->Close().IgnoreError();
    std::filesystem::remove_all(path);
    return long_result.errors == 0 ? 0 : 1;
  }
  auto run_prepared = [&](const cedar::PreparedQuery& prepared,
                          cedar::QueryExecutionMode mode)
      -> cedar::StatusOr<std::pair<uint64_t, uint64_t>> {
    auto current = database.ValueOrDie()->BeginSnapshot();
    if (!current.ok()) return current.status();
    cedar::QueryOptions query_options;
    query_options.capture_profile = true;
    query_options.mode = mode;
    query_options.budget.memory_bytes = 512ULL << 20;
    auto cursor = prepared.Execute(std::move(current).ConsumeValueOrDie(),
                                   cedar::Bindings{}, query_options);
    if (!cursor.ok()) return cursor.status();
    uint64_t rows = 0, bytes = 0;
    while (true) {
      auto batch = cursor.ValueOrDie().Next();
      if (!batch.ok()) return batch.status();
      if (!batch.ValueOrDie().has_value()) break;
      rows += batch.ValueOrDie()->row_count();
    }
    for (const auto& profile : cursor.ValueOrDie().profile().operators) bytes += profile.physical_bytes;
    return std::pair<uint64_t, uint64_t>{rows, bytes};
  };

  std::cout << "workload,input_rows,output_rows,source,physical_bytes,decoded_bytes,"
               "pages_read,pages_skipped,catalog_hits,catalog_misses,p50_us,p95_us,"
               "p99_us,rss_bytes,error\n";
  const uint64_t rss = PeakRssBytes();
  auto point_result = RunRepeated(entities, seconds, [&]() { return run_cursor(point.ValueOrDie()); });
  PrintRow("point_state_exact", point_result, "canonical", rss);
  if (entities < 10000) {
    auto expansion_result = RunRepeated(entities > 0 ? entities - 1 : 0, seconds,
                                        [&]() {
                                          return run_prepared(
                                              expansion_prepared.ValueOrDie(),
                                              cedar::QueryExecutionMode::kAnalytical);
                                        });
    if (!expansion_result.first_error.empty()) std::cerr << expansion_result.first_error << '\n';
    PrintRow("typed_graph_expansion", expansion_result, "canonical", rss);
  } else {
    Measurement skipped;
    skipped.input_rows = entities;
    skipped.samples.push_back(1);
    PrintRow("typed_graph_expansion_large_skipped", skipped, "canonical", rss);
  }
  auto history_result = RunRepeated(entities, seconds, [&]() {
    return run_cursor(history.ValueOrDie(), cedar::QueryExecutionMode::kAnalytical);
  });
  if (!history_result.first_error.empty()) std::cerr << history_result.first_error << '\n';
  PrintRow("temporal_history", history_result, "canonical", rss);
  auto history_narrow_result = RunRepeated(entities, seconds, [&]() {
    return run_cursor(history_narrow.ValueOrDie(), cedar::QueryExecutionMode::kAnalytical);
  });
  PrintRow("temporal_history_valid_time_10pct", history_narrow_result,
           "canonical", rss);

  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  if (!snapshot.ok()) return 1;
  std::vector<std::vector<std::string>> keys_by_property(properties.size());
  if (entities < 10000) for (size_t p = 0; p < properties.size(); ++p) {
    cedar::FactReadSpec spec;
    spec.part_scope = cedar::PartScope::Exact(cedar::PartId{0});
    spec.family = cedar::FactFamily::kVertexProperty;
    spec.property_id = properties[p].property_id;
    spec.entity_range = cedar::EntityRange{1, entities + 1};
    spec.commit_seq_max = snapshot.ValueOrDie().commit_seq();
    auto status = snapshot.ValueOrDie().canonical_reader().ReadEvents(
        spec, [&keys_by_property, p](const cedar::FactEventBatch& batch) {
          for (const auto& event : batch.events) {
            keys_by_property[p].push_back(cedar::EncodeFactKey(
                event.ref, event.valid_from, event.commit_seq));
          }
          return cedar::Status::OK();
        });
    if (!status.ok()) {
      std::cerr << status.ToString() << " property=" << properties[p].property_id.value << '\n';
      return 1;
    }
  }
  const std::vector<size_t> binding_counts = {1, 4, 16};
  if (entities < 10000) for (size_t binding_count : binding_counts) {
    std::vector<std::string> exact_keys;
    exact_keys.reserve(entities * binding_count);
    for (size_t p = 0; p < binding_count; ++p) {
      exact_keys.insert(exact_keys.end(), keys_by_property[p].begin(),
                        keys_by_property[p].end());
    }
    auto exact_result = RunRepeated(exact_keys.size(), seconds, [&]()
        -> cedar::StatusOr<std::pair<uint64_t, uint64_t>> {
      auto values = snapshot.ValueOrDie().canonical_reader().ReadExact(exact_keys);
      if (!values.ok()) return values.status();
      return std::pair<uint64_t, uint64_t>{values.ValueOrDie().size(),
                                           values.ValueOrDie().size() * 32};
    });
    PrintRow("exact_multiget_property_bindings_" + std::to_string(binding_count),
             exact_result, "canonical", rss);
  }

  const auto projection_path = std::filesystem::path(path) / "bench.csegment";
  cedar::internal::ProjectionChain chain;
  chain.header.kind = cedar::internal::ProjectionKind::kState;
  chain.header.entity_min = 1;
  chain.header.entity_max_exclusive = entities + 1;
  chain.header.valid_from_min = cedar::ValidTime{0};
  chain.header.valid_to_max = cedar::ValidTime{100};
  for (uint32_t i = 1; i <= entities; ++i) {
    chain.intervals.push_back({cedar::ValidTimeInterval{cedar::ValidTime{0}, cedar::ValidTime{100}},
                               cedar::Value::Int64(i), i});
  }
  auto encoded = cedar::internal::EncodeProjectionPage(
      chain, cedar::internal::CompressionCodec::kNone);
  if (!encoded.ok()) return 1;
  std::ofstream(projection_path, std::ios::binary) << encoded.ValueOrDie();
  auto directory = cedar::internal::ProjectionPageReader{}.ReadDirectory(projection_path.string());
  if (!directory.ok()) return 1;
  for (const auto& mode : {std::pair<const char*, uint64_t>{"hit", entities + 1},
                           {"partial", std::max<uint64_t>(2, entities / 10)},
                           {"base", 1}}) {
    cedar::internal::CoverageRequest request;
    request.entity_min = mode.first == std::string("base") ? entities + 1 : 1;
    request.entity_max_exclusive = mode.second;
    request.valid_time = {cedar::ValidTime{1}, cedar::ValidTime{10}};
    auto selected = cedar::internal::ProjectionPageReader{}.Select(
        directory.ValueOrDie(), request);
    if (!selected.ok()) return 1;
    Measurement projection;
    projection.input_rows = entities;
    projection.output_rows = selected.ValueOrDie().page_indexes.size();
    projection.pages_read = selected.ValueOrDie().page_indexes.size();
    projection.pages_skipped = selected.ValueOrDie().pages_skipped;
    projection.catalog_hits = mode.first == std::string("base") ? 0 : 1;
    projection.physical_bytes = encoded.ValueOrDie().size();
    projection.decoded_bytes = projection.output_rows * 32;
    projection.samples.push_back(1);
    PrintRow(std::string("projection_page_") + mode.first, projection,
             "projection", rss);
  }

  uint64_t partition_reads = 0;
  cedar::internal::SetSpillPartitionObserverForTesting(
      [&partition_reads](bool rebuilt) { if (!rebuilt) ++partition_reads; });
  std::vector<cedar::internal::RelationalRow> left_rows, right_rows;
  left_rows.reserve(entities);
  right_rows.reserve(entities);
  for (uint32_t i = 0; i < entities; ++i) {
    const auto cell = cedar::internal::RelationalCell::Present(
        cedar::QueryType::kInt64, static_cast<int64_t>(i));
    left_rows.push_back({{cell}, std::nullopt});
    right_rows.push_back({{cell}, std::nullopt});
  }
  cedar::internal::QueryReservation join_reservation(16ULL << 20);
  cedar::internal::QueryScratch scratch(path, "read-side-bench", "spill",
                                        64ULL << 20, &join_reservation);
  auto joined = cedar::internal::HashJoin(
      {cedar::internal::BatchStream(std::move(left_rows)),
       cedar::internal::BatchStream(std::move(right_rows)), 0, 0,
       cedar::internal::JoinKind::kInner},
      &join_reservation, std::numeric_limits<size_t>::max(), &scratch);
  Measurement spill;
  spill.input_rows = entities * 2;
  spill.catalog_hits = partition_reads;
  if (joined.ok()) {
    spill.output_rows = joined.ValueOrDie().rows.size();
    spill.decoded_bytes = spill.output_rows * sizeof(cedar::internal::RelationalRow);
    spill.physical_bytes = scratch.query_directory().empty() ? 0 : spill.decoded_bytes;
    spill.samples.push_back(1);
  } else {
    spill.errors = 1;
    spill.first_error = joined.status().ToString();
  }
  PrintRow("relational_spill_hash_join", spill, "spill", rss);
  cedar::internal::SetSpillPartitionObserverForTesting({});
  scratch.Cleanup().IgnoreError();
  std::filesystem::remove_all(path);
  return 0;
}
