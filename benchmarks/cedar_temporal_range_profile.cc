#include <sys/resource.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <unistd.h>

#include "cedar/cypher/session.h"
#include "cedar/cypher/relationship_type.h"
#include "cedar/database.h"

namespace {
using Clock = std::chrono::steady_clock;

struct Options {
  uint32_t entities = 32;
  uint32_t edges = 32;
  uint32_t properties = 1;
  uint32_t commits = 3;
  uint32_t sessions = 1;
  uint32_t duration_seconds = 1;
  uint64_t projection_generation = 0;
  uint64_t query_budget = 64ULL << 20;
  std::string build_id = "unknown";
  std::string output;
};

bool Parse(int argc, char** argv, Options* options) {
  for (int i = 1; i < argc; ++i) {
    if (i + 1 >= argc) return false;
    const std::string arg = argv[i];
    if (arg == "--entities") options->entities = std::stoul(argv[++i]);
    else if (arg == "--edges") options->edges = std::stoul(argv[++i]);
    else if (arg == "--properties") options->properties = std::stoul(argv[++i]);
    else if (arg == "--commits") options->commits = std::stoul(argv[++i]);
    else if (arg == "--sessions") options->sessions = std::stoul(argv[++i]);
    else if (arg == "--duration-seconds") options->duration_seconds = std::stoul(argv[++i]);
    else if (arg == "--projection-generation") options->projection_generation = std::stoull(argv[++i]);
    else if (arg == "--query-budget") options->query_budget = std::stoull(argv[++i]);
    else if (arg == "--build-id") options->build_id = argv[++i];
    else if (arg == "--output") options->output = argv[++i];
    else return false;
  }
  return options->entities != 0 && options->edges != 0 && options->properties != 0 &&
         options->commits != 0 && options->sessions != 0 && options->sessions <= 64 &&
         options->duration_seconds != 0 && !options->output.empty();
}

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

struct Measurements {
  uint64_t operations = 0;
  uint64_t rows = 0;
  uint64_t physical_bytes = 0;
  uint64_t decoded_bytes = 0;
  uint64_t pages_read = 0;
  uint64_t pages_skipped = 0;
  uint64_t errors = 0;
  std::vector<uint64_t> latency_us;
  std::string source = "canonical";
};

void AddProfile(const cedar::QueryProfile& profile, Measurements* result) {
  for (const auto& op : profile.operators) {
    result->physical_bytes += op.physical_bytes;
    result->decoded_bytes += op.decoded_bytes;
    result->pages_read += op.pages;
    result->pages_skipped += op.pages_skipped;
  }
}

template <typename Execute>
Measurements RunCase(Execute execute, uint32_t duration_seconds) {
  Measurements result;
  const auto deadline = Clock::now() + std::chrono::seconds(duration_seconds);
  while (Clock::now() < deadline || result.operations == 0) {
    const auto started = Clock::now();
    auto cursor = execute();
    if (!cursor.ok()) {
      ++result.errors;
      if (result.operations != 0) continue;
      break;
    }
    uint64_t rows = 0;
    while (true) {
      auto batch = cursor.ValueOrDie().Next();
      if (!batch.ok()) {
        ++result.errors;
        break;
      }
      if (!batch.ValueOrDie().has_value()) break;
      rows += batch.ValueOrDie()->row_count();
    }
    AddProfile(cursor.ValueOrDie().profile(), &result);
    result.rows += rows;
    ++result.operations;
    result.latency_us.push_back(static_cast<uint64_t>(std::chrono::duration_cast<
        std::chrono::microseconds>(Clock::now() - started).count()));
  }
  return result;
}

void WriteRow(std::ofstream* out, const Options& options, const std::string& name,
              const Measurements& result) {
  const double seconds = static_cast<double>(options.duration_seconds);
  *out << options.build_id << ',' << name << ',' << result.source << ','
       << result.operations << ',' << result.rows << ','
       << result.operations / seconds << ',' << result.rows / seconds << ','
       << result.physical_bytes << ',' << result.decoded_bytes << ','
       << result.pages_read << ',' << result.pages_skipped << ','
       << Percentile(result.latency_us, 50) << ','
       << Percentile(result.latency_us, 95) << ','
       << Percentile(result.latency_us, 99) << ',' << PeakRssBytes() << ','
       << result.errors << '\n';
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!Parse(argc, argv, &options)) {
    std::cerr << "usage: cedar_temporal_range_profile --output FILE [options]\n";
    return 2;
  }
  char pattern[] = "/tmp/cedar_temporal_range_profile_XXXXXX";
  if (::mkdtemp(pattern) == nullptr) return 1;
  cedar::DatabaseOptions database_options;
  database_options.path = pattern;
  database_options.query_runtime.query_memory_bytes = options.query_budget;
  database_options.query_runtime.query_workers = std::max(1U, options.sessions);
  database_options.query_runtime.reserved_interactive_workers =
      std::min(8U, options.sessions);
  auto database = cedar::Database::Open(database_options);
  if (!database.ok()) return 1;
  auto relationship_knows = cedar::cypher::ResolveRelationshipType("KNOWS");
  auto relationship_likes = cedar::cypher::ResolveRelationshipType("LIKES");
  if (!relationship_knows.ok() || !relationship_likes.ok()) return 1;
  auto property = database.ValueOrDie()->RegisterProperty(cedar::PropertyDefinition{
      cedar::PropertyId{7}, 0, "score", cedar::PropertyEntityKind::kVertex,
      cedar::PhysicalType::kInt64, 4096});
  if (!property.ok()) return 1;

  std::vector<cedar::VertexRef> vertices;
  vertices.reserve(options.entities);
  auto first = database.ValueOrDie()->BeginTransaction();
  if (!first.ok()) return 1;
  for (uint32_t i = 0; i < options.entities; ++i) {
    auto id = database.ValueOrDie()->AllocateVertexId();
    if (!id.ok()) return 1;
    vertices.push_back({cedar::PartId{0}, id.ValueOrDie()});
    if (!first.ValueOrDie()->Assert(cedar::EntityFact::Vertex(vertices.back()),
                                    cedar::ValidTime{1}).ok()) return 1;
    if (i < options.properties &&
        !first.ValueOrDie()->Set(
            cedar::PropertyFact::Vertex(vertices.back(), cedar::PropertyId{7}),
            cedar::ValidTime{1}, cedar::Value::Int64(static_cast<int64_t>(i)))
             .ok()) return 1;
  }
  for (uint32_t i = 0; i < options.edges; ++i) {
    auto edge_id = database.ValueOrDie()->AllocateEdgeId();
    if (!edge_id.ok()) return 1;
    const auto& source = vertices[i % vertices.size()];
    const auto& target = vertices[(i + 1) % vertices.size()];
    const uint64_t type = i % 2 == 0 ? relationship_knows.ValueOrDie()
                                     : relationship_likes.ValueOrDie();
    if (!first.ValueOrDie()->Assert(
            cedar::EdgeIdentity{{cedar::PartId{0}, edge_id.ValueOrDie()}, source,
                                target, type},
            cedar::ValidTime{1})
             .ok()) return 1;
  }
  if (!first.ValueOrDie()->Commit().ok()) return 1;
  for (uint32_t commit = 1; commit < options.commits; ++commit) {
    auto next = database.ValueOrDie()->BeginTransaction();
    if (!next.ok()) return 1;
    if (!next.ValueOrDie()
             ->Set(cedar::PropertyFact::Vertex(vertices.front(), cedar::PropertyId{7}),
                   cedar::ValidTime{1 + commit},
                   cedar::Value::Int64(static_cast<int64_t>(commit)))
             .ok() ||
        !next.ValueOrDie()->Commit().ok()) return 1;
  }

  std::ofstream out(options.output);
  if (!out) return 1;
  out << "build_id,case,source,operations,rows,ops_per_second,rows_per_second,"
          "physical_bytes,decoded_bytes,pages_read,pages_skipped,p50_us,p95_us,"
          "p99_us,peak_rss_bytes,errors\n";

  cedar::cypher::CypherSession session(*database.ValueOrDie(),
                                       cedar::cypher::SchemaCatalog{});
  const auto prepare = [&](const std::string& text)
      -> cedar::StatusOr<cedar::cypher::PreparedCypher> {
    return session.Prepare(text);
  };
  auto point = prepare("FOR VALID_TIME AS OF 1 MATCH (v) RETURN v");
  auto as_of = prepare("FOR VALID_TIME AS OF 1 FOR SYSTEM_TIME AS OF 1 MATCH (v) RETURN v");
  auto range = prepare("FOR VALID_TIME AS OF 1 FOR SYSTEM_TIME BETWEEN 1 AND 3 MATCH (v) RETURN v");
  auto path = prepare("FOR VALID_TIME AS OF 1 MATCH (a)-[e:KNOWS]->(b)-[f:LIKES]->(c) RETURN a, e, b, f, c");
  if (!point.ok() || !as_of.ok() || !range.ok() || !path.ok()) return 1;
  const auto execute = [&](const cedar::cypher::PreparedCypher& prepared) {
    cedar::cypher::CypherRequest request;
    request.options.capture_profile = true;
    return session.Execute(prepared, request);
  };
  bool ok = true;
  for (const auto& item : std::vector<std::pair<std::string, cedar::cypher::PreparedCypher*>>{
           {"point_state", &point.ValueOrDie()}, {"system_time_as_of", &as_of.ValueOrDie()},
           {"system_time_between", &range.ValueOrDie()}, {"two_segment_path", &path.ValueOrDie()}}) {
    Measurements result = RunCase([&] { return execute(*item.second); }, options.duration_seconds);
    auto explain = session.Explain(*item.second);
    if (explain.ok()) result.source = explain.ValueOrDie().source;
    WriteRow(&out, options, item.first, result);
    ok = ok && result.errors == 0;
  }

  auto overlay_txn = database.ValueOrDie()->BeginTransaction();
  if (!overlay_txn.ok() || !overlay_txn.ValueOrDie()
                              ->Assert(cedar::EntityFact::Vertex(
                                           {cedar::PartId{0}, cedar::VertexId{999999}}),
                                       cedar::ValidTime{1})
                              .ok()) return 1;
  Measurements overlay = RunCase(
      [&] {
        cedar::cypher::CypherRequest request;
        request.options.capture_profile = true;
        return session.Execute(point.ValueOrDie(), *overlay_txn.ValueOrDie(),
                               request);
      },
      options.duration_seconds);
  overlay.source = "canonical_overlay";
  WriteRow(&out, options, "transaction_read_your_writes", overlay);
  ok = ok && overlay.errors == 0;
  if (!overlay_txn.ValueOrDie()->Rollback().ok()) ok = false;

  out.close();
  std::ofstream manifest(options.output + ".manifest");
  manifest << "build_id=" << options.build_id << "\nentities=" << options.entities
           << "\nedges=" << options.edges << "\nproperties=" << options.properties
           << "\ncommits=" << options.commits << "\nsessions=" << options.sessions
           << "\nduration_seconds=" << options.duration_seconds
           << "\nprojection_generation=" << options.projection_generation
           << "\nquery_budget=" << options.query_budget << "\n";
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(pattern);
  return ok ? 0 : 1;
}
