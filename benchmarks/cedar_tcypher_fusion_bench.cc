#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "cedar/cypher.h"
#include "cedar/cypher/session.h"
#include "cedar/database.h"
#include "cedar/transaction.h"

namespace {
using Clock = std::chrono::steady_clock;

uint64_t Percentile(std::vector<uint64_t> values, size_t percentile) {
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  const size_t index = (values.size() - 1) * percentile / 100;
  return values[index];
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

struct Options {
  std::string path;
  uint64_t seconds = 5;
};

bool Parse(int argc, char** argv, Options* options) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--db" && i + 1 < argc) options->path = argv[++i];
    else if (arg == "--seconds" && i + 1 < argc) options->seconds = std::stoull(argv[++i]);
    else return false;
  }
  return options->seconds > 0;
}
}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!Parse(argc, argv, &options)) {
    std::cerr << "usage: cedar_tcypher_fusion_bench --db PATH [--seconds N]\n";
    return 2;
  }
  bool remove_path = false;
  if (options.path.empty()) {
    char pattern[] = "/tmp/cedar_tcypher_bench_XXXXXX";
    if (mkdtemp(pattern) == nullptr) return 1;
    options.path = pattern;
    remove_path = true;
  }
  auto database = cedar::Database::Open(cedar::DatabaseOptions{.path = options.path});
  if (!database.ok()) {
    std::cerr << database.status().ToString() << '\n';
    return 1;
  }
  auto tx = database.ValueOrDie()->BeginTransaction();
  if (!tx.ok()) return 1;
  auto vertex = database.ValueOrDie()->AllocateVertexId();
  if (!vertex.ok() || !tx.ValueOrDie()->Assert(
          cedar::EntityFact::Vertex({cedar::PartId{0}, vertex.ValueOrDie()}),
          cedar::ValidTime{1}).ok() || !tx.ValueOrDie()->Commit().ok()) return 1;

  cedar::cypher::CypherSession session(*database.ValueOrDie(),
                                       cedar::cypher::SchemaCatalog{});
  auto prepared = session.Prepare("FOR VALID_TIME AS OF 1 MATCH (v) RETURN v");
  if (!prepared.ok()) {
    std::cerr << prepared.status().ToString() << '\n';
    return 1;
  }
  std::vector<uint64_t> samples;
  uint64_t rows = 0;
  uint64_t errors = 0;
  const auto deadline = Clock::now() + std::chrono::seconds(options.seconds);
  while (Clock::now() < deadline) {
    const auto started = Clock::now();
    auto cursor = session.Execute(prepared.ValueOrDie(), cedar::cypher::CypherRequest{});
    if (!cursor.ok()) { ++errors; continue; }
    while (true) {
      auto batch = cursor.ValueOrDie().Next();
      if (!batch.ok()) { ++errors; break; }
      if (!batch.ValueOrDie().has_value()) break;
      rows += batch.ValueOrDie()->row_count();
    }
    samples.push_back(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        Clock::now() - started).count()));
  }
  const uint64_t operations = samples.size();
  const double seconds = static_cast<double>(options.seconds);
  std::cout << "workload,seconds,operations,rows,ops_per_second,rows_per_second,"
                "p50_us,p95_us,p99_us,peak_rss_bytes,errors\n";
  std::cout << "match_vertex," << options.seconds << ',' << operations << ',' << rows << ','
            << operations / seconds << ',' << rows / seconds << ','
            << Percentile(samples, 50) << ',' << Percentile(samples, 95) << ','
            << Percentile(samples, 99) << ',' << PeakRssBytes() << ',' << errors << '\n';
  database.ValueOrDie()->Close().IgnoreError();
  if (remove_path) std::filesystem::remove_all(options.path);
  return errors == 0 ? 0 : 1;
}
