#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cedar/cypher/relationship_type.h"
#include "cedar/cypher/session.h"
#include "cedar/database.h"

namespace {
using Clock = std::chrono::steady_clock;

struct Options {
  std::string workload = "typed";
  uint64_t seconds = 1;
  uint32_t sessions = 1;
  uint32_t total_edges = 1000;
  uint32_t selected_edges = 100;
};

bool Parse(int argc, char** argv, Options* options) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (i + 1 >= argc) return false;
    if (arg == "--workload") options->workload = argv[++i];
    else if (arg == "--seconds") options->seconds = std::stoull(argv[++i]);
    else if (arg == "--sessions") options->sessions = std::stoul(argv[++i]);
    else if (arg == "--total-edges") options->total_edges = std::stoul(argv[++i]);
    else if (arg == "--selected-edges") options->selected_edges = std::stoul(argv[++i]);
    else return false;
  }
  return (options->workload == "typed" || options->workload == "point") &&
         options->seconds > 0 && options->sessions > 0 && options->total_edges > 0 &&
         options->selected_edges <= options->total_edges;
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

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!Parse(argc, argv, &options)) {
    std::cerr << "usage: cedar_tcypher_optimization_bench --workload typed|point "
                 "--seconds N --sessions N --total-edges N --selected-edges N\n";
    return 2;
  }
  char pattern[] = "/tmp/cedar_tcypher_optimization_XXXXXX";
  if (::mkdtemp(pattern) == nullptr) return 1;
  cedar::DatabaseOptions database_options;
  database_options.path = pattern;
  database_options.query_runtime.query_workers = std::max(4U, options.sessions);
  database_options.query_runtime.reserved_interactive_workers =
      std::min(8U, database_options.query_runtime.query_workers);
  database_options.query_runtime.query_memory_bytes = 512ULL << 20;
  auto database = cedar::Database::Open(database_options);
  if (!database.ok()) return 1;
  auto tx = database.ValueOrDie()->BeginTransaction();
  if (!tx.ok()) return 1;
  auto source = database.ValueOrDie()->AllocateVertexId();
  if (!source.ok() || !tx.ValueOrDie()->Assert(
          cedar::EntityFact::Vertex({cedar::PartId{0}, source.ValueOrDie()}),
          cedar::ValidTime{1}).ok()) return 1;
  const auto selected_hash = cedar::cypher::ResolveRelationshipType("KNOWS");
  const auto other_hash = cedar::cypher::ResolveRelationshipType("LIKES");
  if (!selected_hash.ok() || !other_hash.ok()) return 1;
  for (uint32_t i = 0; i < options.total_edges; ++i) {
    auto target = database.ValueOrDie()->AllocateVertexId();
    auto edge = database.ValueOrDie()->AllocateEdgeId();
    if (!target.ok() || !edge.ok() ||
        !tx.ValueOrDie()->Assert(cedar::EntityFact::Vertex(
                                    {cedar::PartId{0}, target.ValueOrDie()}),
                                cedar::ValidTime{1}).ok() ||
        !tx.ValueOrDie()->Assert(
            cedar::EdgeIdentity{{cedar::PartId{0}, edge.ValueOrDie()},
                                 {cedar::PartId{0}, source.ValueOrDie()},
                                 {cedar::PartId{0}, target.ValueOrDie()},
                                 i < options.selected_edges ? selected_hash.ValueOrDie()
                                                             : other_hash.ValueOrDie()},
            cedar::ValidTime{1}).ok()) return 1;
  }
  auto committed = tx.ValueOrDie()->Commit();
  if (!committed.ok()) { std::cerr << committed.status().ToString() << '\n'; return 1; }
  if (committed.ValueOrDie().outcome != cedar::CommitOutcome::kCommitted) {
    std::cerr << committed.ValueOrDie().status.ToString() << '\n';
    return 1;
  }
  std::atomic<uint64_t> operations{0};
  std::atomic<uint64_t> rows{0};
  std::atomic<uint64_t> errors{0};
  std::atomic<uint64_t> physical_bytes{0};
  std::atomic<uint64_t> decoded_bytes{0};
  std::atomic<uint64_t> pages{0};
  std::atomic<uint64_t> projection_hits{0};
  std::atomic<uint64_t> projection_fallbacks{0};
  std::atomic<uint64_t> canonical_fallbacks{0};
  std::mutex samples_mutex;
  std::mutex source_mutex;
  std::string explain_source = "unknown";
  std::vector<uint64_t> samples;
  const auto deadline = Clock::now() + std::chrono::seconds(options.seconds);
  std::vector<std::thread> workers;
  workers.reserve(options.sessions);
  for (uint32_t worker = 0; worker < options.sessions; ++worker) {
    workers.emplace_back([&, worker] {
      cedar::cypher::CypherSession session(*database.ValueOrDie(),
                                           cedar::cypher::SchemaCatalog{});
      const std::string query = options.workload == "typed"
                                    ? "FOR VALID_TIME AS OF 1 MATCH (a)-[e:KNOWS]->(b) RETURN a, e, b"
                                    : "FOR VALID_TIME AS OF 1 MATCH (v) RETURN v";
      auto prepared = session.Prepare(query);
      if (!prepared.ok()) { ++errors; return; }
      auto explained = session.Explain(prepared.ValueOrDie());
      if (!explained.ok()) { ++errors; return; }
      {
        std::lock_guard<std::mutex> lock(source_mutex);
        if (explain_source == "unknown") {
          explain_source = explained.ValueOrDie().source;
        } else if (explain_source != explained.ValueOrDie().source) {
          explain_source = "mixed";
        }
      }
      while (Clock::now() < deadline) {
        const auto started = Clock::now();
        cedar::cypher::CypherRequest request;
        request.options.capture_profile = true;
        auto cursor = session.Execute(prepared.ValueOrDie(), request);
        if (!cursor.ok()) { ++errors; continue; }
        uint64_t local_rows = 0;
        while (true) {
          auto batch = cursor.ValueOrDie().Next();
          if (!batch.ok()) { ++errors; break; }
          if (!batch.ValueOrDie().has_value()) break;
          local_rows += batch.ValueOrDie()->row_count();
        }
        const auto profile = cursor.ValueOrDie().profile();
        for (const auto& operator_profile : profile.operators) {
          physical_bytes.fetch_add(operator_profile.physical_bytes,
                                   std::memory_order_relaxed);
          decoded_bytes.fetch_add(operator_profile.decoded_bytes,
                                  std::memory_order_relaxed);
          pages.fetch_add(operator_profile.pages, std::memory_order_relaxed);
        }
        const auto metrics = database.ValueOrDie()->SampleQueryMetrics();
        projection_hits.store(
            metrics.projection[static_cast<size_t>(cedar::QueryMetricProjection::kHit)],
            std::memory_order_relaxed);
        projection_fallbacks.store(
            metrics.projection[static_cast<size_t>(cedar::QueryMetricProjection::kFallback)],
            std::memory_order_relaxed);
        canonical_fallbacks.store(
            metrics.fallback[static_cast<size_t>(cedar::QueryMetricFallback::kCanonical)],
            std::memory_order_relaxed);
        rows.fetch_add(local_rows, std::memory_order_relaxed);
        operations.fetch_add(1, std::memory_order_relaxed);
        const auto micros = static_cast<uint64_t>(std::chrono::duration_cast<
            std::chrono::microseconds>(Clock::now() - started).count());
        std::lock_guard<std::mutex> lock(samples_mutex);
        samples.push_back(micros);
      }
    });
  }
  for (auto& worker : workers) worker.join();
  std::cout << "workload,seconds,sessions,total_edges,selected_edges,operations,rows,"
               "ops_per_second,rows_per_second,physical_bytes,decoded_bytes,pages,"
               "explain_source,projection_hits,projection_fallbacks,canonical_fallbacks,"
               "p50_us,p95_us,p99_us,peak_rss_bytes,errors\n";
  const double seconds = static_cast<double>(options.seconds);
  std::cout << options.workload << ',' << options.seconds << ',' << options.sessions << ','
            << options.total_edges << ',' << options.selected_edges << ','
            << operations << ',' << rows << ',' << operations / seconds << ','
            << rows / seconds << ',' << physical_bytes << ',' << decoded_bytes << ','
            << pages << ',' << explain_source << ',' << projection_hits << ','
            << projection_fallbacks << ','
            << canonical_fallbacks << ',' << Percentile(samples, 50) << ','
            << Percentile(samples, 95) << ',' << Percentile(samples, 99) << ','
            << PeakRssBytes() << ',' << errors << '\n';
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(pattern);
  return errors == 0 ? 0 : 1;
}
