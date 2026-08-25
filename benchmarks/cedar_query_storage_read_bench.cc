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

#include "cedar/cypher/session.h"
#include "cedar/database.h"

namespace {
using Clock = std::chrono::steady_clock;

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
  uint64_t seconds = 1;
  uint32_t entities = 1000;
  uint32_t sessions = 1;
  bool exact_part = false;
  uint32_t part_id = 0;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--seconds" && i + 1 < argc) seconds = std::stoull(argv[++i]);
    else if (arg == "--entities" && i + 1 < argc) entities = std::stoul(argv[++i]);
    else if (arg == "--sessions" && i + 1 < argc) sessions = std::stoul(argv[++i]);
    else if (arg == "--part-scope" && i + 1 < argc) {
      const std::string scope = argv[++i];
      if (scope == "exact") exact_part = true;
      else if (scope == "all" || scope == "wildcard") exact_part = false;
      else return 2;
    }
    else if (arg == "--part-id" && i + 1 < argc) part_id = std::stoul(argv[++i]);
    else if (i == 1) seconds = std::stoull(arg);
    else if (i == 2) entities = std::stoul(arg);
    else return 2;
  }
  if (seconds == 0 || entities == 0 || sessions == 0 || sessions > 64) return 2;

  char path[] = "/tmp/cedar_query_storage_read_XXXXXX";
  if (::mkdtemp(path) == nullptr) return 1;
  cedar::DatabaseOptions options;
  options.path = path;
  options.query_runtime.query_workers = std::max(1U, sessions);
  options.query_runtime.reserved_interactive_workers = std::min(8U, sessions);
  auto database = cedar::Database::Open(options);
  if (!database.ok()) return 1;
  auto transaction = database.ValueOrDie()->BeginTransaction();
  if (!transaction.ok()) return 1;
  for (uint32_t i = 0; i < entities; ++i) {
    auto id = database.ValueOrDie()->AllocateVertexId();
    if (!id.ok() || !transaction.ValueOrDie()->Assert(
                         cedar::EntityFact::Vertex({cedar::PartId{0}, id.ValueOrDie()}),
                         cedar::ValidTime{1}).ok()) {
      return 1;
    }
  }
  auto commit = transaction.ValueOrDie()->Commit();
  if (!commit.ok() || commit.ValueOrDie().outcome != cedar::CommitOutcome::kCommitted)
    return 1;

  std::atomic<uint64_t> operations{0}, rows{0}, physical_bytes{0};
  std::atomic<uint64_t> decoded_bytes{0}, pages_read{0}, errors{0};
  std::atomic<uint64_t> pages_skipped{0};
  std::mutex samples_mutex;
  std::mutex error_mutex;
  std::string first_error;
  std::vector<uint64_t> samples;
  const auto deadline = Clock::now() + std::chrono::seconds(seconds);
  std::vector<std::thread> workers;
  workers.reserve(sessions);
  for (uint32_t worker = 0; worker < sessions; ++worker) {
    workers.emplace_back([&, worker] {
      (void)worker;
      cedar::cypher::BinderOptions binder_options;
      binder_options.part_id = cedar::PartId{part_id};
      binder_options.require_explicit_part_id = exact_part;
      cedar::cypher::CypherSession session(
          *database.ValueOrDie(), cedar::cypher::SchemaCatalog{},
          std::move(binder_options));
      auto prepared = session.Prepare(
          "FOR VALID_TIME AS OF 1 MATCH (v) RETURN v");
      if (!prepared.ok()) {
        ++errors;
        std::lock_guard<std::mutex> lock(error_mutex);
        if (first_error.empty()) first_error = prepared.status().ToString();
        return;
      }
      while (Clock::now() < deadline) {
        const auto started = Clock::now();
        cedar::cypher::CypherRequest request;
        request.options.capture_profile = true;
        if (exact_part) request.part_id = cedar::PartId{part_id};
        auto cursor = session.Execute(prepared.ValueOrDie(), request);
        if (!cursor.ok()) {
          ++errors;
          std::lock_guard<std::mutex> lock(error_mutex);
          if (first_error.empty()) first_error = cursor.status().ToString();
          continue;
        }
        uint64_t local_rows = 0;
        while (true) {
          auto batch = cursor.ValueOrDie().Next();
          if (!batch.ok()) {
            ++errors;
            std::lock_guard<std::mutex> lock(error_mutex);
            if (first_error.empty()) first_error = batch.status().ToString();
            break;
          }
          if (!batch.ValueOrDie().has_value()) break;
          local_rows += batch.ValueOrDie()->row_count();
        }
        for (const auto& profile : cursor.ValueOrDie().profile().operators) {
          physical_bytes.fetch_add(profile.physical_bytes);
          decoded_bytes.fetch_add(profile.decoded_bytes);
          pages_read.fetch_add(profile.pages);
          pages_skipped.fetch_add(profile.pages_skipped);
        }
        rows.fetch_add(local_rows);
        operations.fetch_add(1);
        const auto micros = static_cast<uint64_t>(std::chrono::duration_cast<
            std::chrono::microseconds>(Clock::now() - started).count());
        std::lock_guard<std::mutex> lock(samples_mutex);
        samples.push_back(micros);
      }
    });
  }
  for (auto& worker : workers) worker.join();
  std::cout << "workload,parts,entities,events,source,physical_bytes,decoded_bytes,"
               "pages_read,pages_skipped,rows,p50_us,p95_us,p99_us,rss_bytes,error\n";
  std::cout << "vertex_state,1," << entities << ',' << entities << ",canonical,"
            << physical_bytes.load() << ',' << decoded_bytes.load() << ','
            << pages_read.load() << ',' << pages_skipped.load() << ',' << rows.load() << ',' << Percentile(samples, 50) << ','
            << Percentile(samples, 95) << ',' << Percentile(samples, 99) << ','
            << PeakRssBytes() << ',' << errors.load() << '\n';
  if (!first_error.empty()) std::cerr << "error=" << first_error << '\n';
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
  return errors.load() == 0 ? 0 : 1;
}
