// Reopen validation measurement harness for F9 Task 5. This is a benchmark,
// not a correctness test: it builds a multi-hundred-thousand-fact database and
// measures FactStore::Open through the test-only reopen counters. It is
// intentionally not registered with ctest (the actual benchmark binaries in
// benchmarks/ follow the same convention), so the default suite stays small.
//
// Environment:
//   TXNS          committed transactions to create (default 10000)
//   FACTS_PER_TX  vertex facts per transaction (default 64 -> 640k facts)
//   THREADS       build threads (default 4)
//   DEEP          0|1 reopen fact cross-check policy (default 1)
//   RECENT_SEQS   bounded-window size used when DEEP=0 (default 4096)
//   CACHE_BYTES   validation cache budget (default: FactStoreOptions default).
//                 Small values (e.g. 4096) force the S3 identity-history skip.
//   DB_PATH       reuse/build this path instead of a fresh temporary directory
//   REBUILD       1 forces DB_PATH to be recreated before the measurement
//
// Run one policy per invocation, e.g.:
//   DB_PATH=/tmp/reopen640k DEEP=1 build/tests/reopen_validation_benchmark
//   DB_PATH=/tmp/reopen640k DEEP=0 build/tests/reopen_validation_benchmark
// The first run writes the fixture; later runs reuse it, so the deep/bounded
// comparison measures reopen only.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "storage/facts/fact_store.h"

namespace {

using cedar::EntityFact;
using cedar::FactOperation;
using cedar::FactStore;
using cedar::FactStoreOptions;
using cedar::PartId;
using cedar::PendingFactMutation;
using cedar::ReopenValidationPhaseCounters;
using cedar::Status;
using cedar::StoreCommitBatch;
using cedar::TxnId;
using cedar::ValidTime;
using cedar::VertexId;
using cedar::VertexRef;

uint64_t EnvU64(const char* name, uint64_t fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') return fallback;
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(value, &end, 10);
  if (end == value || *end != '\0') {
    std::fprintf(stderr, "invalid %s=%s\n", name, value);
    std::exit(2);
  }
  return static_cast<uint64_t>(parsed);
}

StoreCommitBatch VertexFactsBatch(TxnId txn_id, uint64_t first_vertex,
                                  uint64_t fact_count, uint64_t valid_from) {
  StoreCommitBatch batch{txn_id, valid_from, {}, {}, {}, {}, false};
  batch.mutations.reserve(fact_count);
  for (uint64_t index = 0; index < fact_count; ++index) {
    batch.mutations.push_back(PendingFactMutation{
        EntityFact::Vertex(VertexRef{PartId{0}, first_vertex + index}).ref(),
        ValidTime{valid_from}, FactOperation::kPut, 0, std::nullopt});
  }
  return batch;
}

double MillisSince(std::chrono::steady_clock::time_point started) {
  return std::chrono::duration<double, std::milli>(
             std::chrono::steady_clock::now() - started)
      .count();
}

bool BuildDatabase(const std::string& path, uint64_t transactions,
                   uint64_t facts_per_transaction, uint64_t threads) {
  FactStoreOptions options;
  options.path = path;
  // The measured artifact is reopen; do not pay full-device flushes for the
  // bulk fixture. Reopen reads the same durable files either way.
  options.wal_sync_mode = cedar::WalSyncMode::kFilesystemFlush;
  FactStore store(options);
  const Status opened = store.Open();
  if (!opened.ok()) {
    std::printf("build_open_failed=%s\n", opened.ToString().c_str());
    return false;
  }
  // Persist the allocator record; otherwise reopen takes the cold recovery path
  // that scans every sequence record before the measured passes even start.
  const auto lease = store.AllocateTransactionId();
  if (!lease.ok()) {
    std::printf("build_allocate_failed=%s\n", lease.status().ToString().c_str());
    return false;
  }

  std::atomic<uint64_t> next{1};
  std::atomic<uint64_t> failed{0};
  std::atomic<bool> reported{false};
  const auto started = std::chrono::steady_clock::now();
  const auto work = [&] {
    for (;;) {
      const uint64_t txn = next.fetch_add(1, std::memory_order_relaxed);
      if (txn > transactions) return;
      const auto committed = store.Commit(VertexFactsBatch(
          TxnId{txn}, (txn - 1) * facts_per_transaction + 1,
          facts_per_transaction, 1'000'000 + txn));
      if (!committed.ok()) {
        failed.fetch_add(1, std::memory_order_relaxed);
        if (!reported.exchange(true, std::memory_order_relaxed)) {
          std::printf("build_commit_failed=%s\n",
                      committed.status().ToString().c_str());
        }
      }
    }
  };
  std::vector<std::thread> pool;
  pool.reserve(threads);
  for (uint64_t index = 0; index < threads; ++index) {
    pool.emplace_back(work);
  }
  for (std::thread& thread : pool) thread.join();
  const double build_ms = MillisSince(started);
  const Status closed = store.Close();
  std::printf(
      "build path=%s txns=%" PRIu64 " facts_per_tx=%" PRIu64 " facts=%" PRIu64
      " threads=%" PRIu64 " failed=%" PRIu64 " close_ok=%d build_ms=%.1f\n",
      path.c_str(), transactions, facts_per_transaction,
      transactions * facts_per_transaction, threads, failed.load(),
      closed.ok() ? 1 : 0, build_ms);
  return failed.load() == 0 && closed.ok();
}

bool MeasureReopen(const std::string& path, bool deep, uint64_t recent_sequences,
                   uint64_t cache_bytes) {
  FactStoreOptions options;
  options.path = path;
  options.deep_recovery_validation = deep;
  options.recovery_validation_recent_sequences = recent_sequences;
  if (cache_bytes != 0) options.validation_cache_bytes = cache_bytes;
  std::vector<ReopenValidationPhaseCounters> phases;
  options.reopen_validation_counters_for_testing =
      [&phases](const ReopenValidationPhaseCounters& counters) {
        phases.push_back(counters);
      };

  FactStore store(options);
  const auto started = std::chrono::steady_clock::now();
  const Status opened = store.Open();
  const double reopen_ms = MillisSince(started);
  if (!opened.ok()) {
    std::printf("reopen_failed=%s\n", opened.ToString().c_str());
    return false;
  }
  for (const ReopenValidationPhaseCounters& phase : phases) {
    std::printf(
        "phase=%.*s records=%" PRIu64 " fact_gets=%" PRIu64
        " meta_gets=%" PRIu64 " elapsed_us=%" PRIu64 "\n",
        static_cast<int>(phase.phase.size()), phase.phase.data(), phase.records,
        phase.fact_gets, phase.meta_gets, phase.elapsed_us);
  }
  std::printf(
      "reopen deep=%d recent_seqs=%" PRIu64 " phases=%zu reopen_ms=%.1f\n",
      deep ? 1 : 0, recent_sequences, phases.size(), reopen_ms);
  const auto metrics = store.SampleValidationCacheMetrics();
  if (metrics.ok()) {
    std::printf(
        "identity_history disabled=%d resident_bytes=%" PRIu64 "\n",
        metrics.ValueOrDie().identity_history_disabled ? 1 : 0,
        metrics.ValueOrDie().identity_history_resident_bytes);
  }
  const Status closed = store.Close();
  if (!closed.ok()) {
    std::printf("close_failed=%s\n", closed.ToString().c_str());
    return false;
  }
  std::fflush(stdout);
  return true;
}

}  // namespace

int main() {
  const uint64_t transactions = EnvU64("TXNS", 10'000);
  const uint64_t facts_per_transaction = EnvU64("FACTS_PER_TX", 64);
  const uint64_t threads = std::max<uint64_t>(1, EnvU64("THREADS", 4));
  const bool deep = EnvU64("DEEP", 1) != 0;
  const uint64_t recent_sequences = std::max<uint64_t>(1, EnvU64("RECENT_SEQS", 4096));
  const uint64_t cache_bytes = EnvU64("CACHE_BYTES", 0);
  const bool rebuild = EnvU64("REBUILD", 0) != 0;

  std::string path;
  bool temporary = false;
  const char* configured_path = std::getenv("DB_PATH");
  if (configured_path != nullptr && *configured_path != '\0') {
    path = configured_path;
  } else {
    char pattern[] = "/tmp/cedar_reopen_validation_benchmark_XXXXXX";
    if (mkdtemp(pattern) == nullptr) {
      std::perror("mkdtemp");
      return 1;
    }
    path = pattern;
    temporary = true;
  }

  const bool database_exists =
      !rebuild && std::filesystem::exists(path + "/CURRENT");
  if (database_exists) {
    std::printf("build skipped path=%s (REBUILD=1 recreates it)\n",
                path.c_str());
  } else {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
    std::filesystem::create_directories(path, ignored);
    if (!BuildDatabase(path, transactions, facts_per_transaction, threads)) {
      return 1;
    }
  }

  const bool measured =
      MeasureReopen(path, deep, recent_sequences, cache_bytes);
  if (temporary) {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
  return measured ? 0 : 1;
}
