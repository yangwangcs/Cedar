#include "benchmarks/cedar_query_bench_workload.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <limits>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>

#include "cedar/database.h"
#include "cedar/storage_files.h"

namespace cedar::benchmark {
namespace {
using Clock = std::chrono::steady_clock;
constexpr uint64_t kChecksumSalt = 0x9e3779b97f4a7c15ULL;

StatusOr<std::pair<uint64_t, uint64_t>> ScanFactChecksum(
    const Snapshot& snapshot) {
  FactScanSpec spec{PartId{1}, FactFamily::kVertexState, PropertyId{},
                    ValidTime{1}, std::numeric_limits<uint32_t>::max()};
  uint64_t count = 0;
  uint64_t checksum = 0;
  const Status status = snapshot.StateScan(spec, [&](const FactEventBatch& batch) {
    for (const auto& event : batch.events) {
      ++count;
      checksum ^= event.ref.entity_id() * kChecksumSalt;
    }
    return Status::OK();
  });
  if (!status.ok()) return status;
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

Status ExecuteOperation(const Snapshot& snapshot, QueryBenchmarkOperation op,
                        uint64_t limit, uint64_t* rows) {
  if (!QueryBenchmarkOperationSupported(op)) {
    return Status::NotSupported("query benchmark",
                                QueryBenchmarkOperationName(op));
  }
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
    case QueryBenchmarkOperation::kEvents:
    case QueryBenchmarkOperation::kChanges:
    case QueryBenchmarkOperation::kHistory:
    case QueryBenchmarkOperation::kIntervalJoin:
      status = snapshot.EventScan(spec, bounded_visitor);
      break;
    default:
      // The public API has no generic operation dispatcher. Every typed matrix
      // entry still performs a bounded canonical Cedar scan; graph/journey
      // entries use the same state source and are reported as such.
      status = snapshot.StateScan(spec, bounded_visitor);
      break;
  }
  if (!status.ok()) return status;
  *rows += std::min(seen, limit);
  return Status::OK();
}

void AddFileBytes(const std::vector<StorageFileInfo>& files,
                  QueryBenchmarkResult* result) {
  for (const auto& file : files) {
    result->total_bytes += file.size_bytes;
    if (!file.query_file.has_value() ||
        file.query_file->authority == StorageFileAuthority::kAuthoritative) {
      result->authoritative_bytes += file.size_bytes;
    } else if (file.query_file->authority == StorageFileAuthority::kTemporary) {
      result->scratch_bytes += file.size_bytes;
    } else {
      result->derived_bytes += file.size_bytes;
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
  if (options.projection != ProjectionState::kCanonicalOnly ||
      options.projection_work == ProjectionWork::kActive) {
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
  if (result.projection_active) {
    auto refreshed = database->RefreshQueryStatistics();
    if (!refreshed.ok()) {
      result.terminal_status = refreshed.status().ToString();
      result.gate_classification = "incomplete";
      result.hard_gate_pass = false;
      return result;
    }
    maintenance = std::move(refreshed).ConsumeValueOrDie();
  }

  std::atomic<uint64_t> next_id{1}, transactions{0}, facts{0};
  std::atomic<uint64_t> dataset_checksum{0};
  std::atomic<bool> failed{false};
  std::mutex failure_mutex, sample_mutex;
  std::string failure;
  std::vector<uint64_t> write_samples;
  const uint64_t seed_facts = std::max<uint64_t>(options.degree * 4,
                                                  options.facts_per_txn);
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
  const auto deadline = Clock::now() +
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
  if (failed.load()) result.terminal_status = failure;

  if (options.cache == QueryCacheState::kWarm) {
    auto warm_snapshot = database->BeginSnapshot();
    if (!warm_snapshot.ok()) return warm_snapshot.status();
    uint64_t warm_rows = 0;
    const Status warm_status = ExecuteOperation(
        warm_snapshot.ValueOrDie(), options.operation, options.result_limit,
        &warm_rows);
    if (!warm_status.ok()) return warm_status;
    result.cache_conditioned = true;
  }
  std::vector<uint64_t> query_samples;
  std::mutex query_mutex;
  std::atomic<uint64_t> rows{0};
  std::vector<std::jthread> readers;
  readers.reserve(options.readers);
  for (uint32_t i = 0; i < options.readers; ++i) {
    readers.emplace_back([&] {
    auto snapshot = database->BeginSnapshot();
    if (!snapshot.ok()) {
      std::lock_guard<std::mutex> lock(query_mutex);
      result.terminal_status = snapshot.status().ToString();
      return;
    }
    const auto start = Clock::now();
    uint64_t local_rows = 0;
    const Status s = ExecuteOperation(snapshot.ValueOrDie(), options.operation,
                                      options.result_limit, &local_rows);
    if (!s.ok()) {
      std::lock_guard<std::mutex> lock(query_mutex);
      result.terminal_status = s.ToString();
      return;
    }
    rows.fetch_add(local_rows);
    const uint64_t elapsed = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start)
            .count());
    std::lock_guard<std::mutex> lock(query_mutex);
    query_samples.push_back(elapsed);
  });
  }
  readers.clear();
  result.elapsed_seconds = std::chrono::duration<double>(Clock::now() - run_start).count();
  result.dataset_checksum = dataset_checksum.load();
  result.transactions = transactions.load();
  result.facts = facts.load();
  result.dataset_checksum = dataset_checksum.load();
  result.rows = rows.load();
  result.query_samples = query_samples.size();
  result.query_p50_us = Percentile(query_samples, 50);
  result.query_p95_us = Percentile(query_samples, 95);
  result.query_p99_us = Percentile(query_samples, 99);
  result.write_p50_us = Percentile(write_samples, 50);
  result.write_p95_us = Percentile(write_samples, 95);
  result.write_p99_us = Percentile(write_samples, 99);
  const CommitPipelineMetrics pipeline = database->GetCommitPipelineMetrics();
  result.wal_sync_p99_us =
      pipeline.latency.wal_sync.ApproximatePercentile(99);
  result.end_to_end_p99_us =
      pipeline.latency.end_to_end.ApproximatePercentile(99);
  result.metrics_complete = pipeline.latency.wal_sync.count > 0 &&
                            pipeline.latency.end_to_end.count > 0 &&
                            !query_samples.empty();

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
  if (files.ok()) AddFileBytes(files.ValueOrDie(), &result);
  result.mib_per_second = result.elapsed_seconds > 0
                              ? static_cast<double>(result.total_bytes) /
                                    (1024.0 * 1024.0 * result.elapsed_seconds)
                              : 0.0;
  result.group_fill_p50 = result.transactions == 0
                              ? 0
                              : result.facts / result.transactions;
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
                          result.operation_supported &&
                          result.projection_state_supported &&
                          result.terminal_status == "OK" &&
                          (!options.verify_reopen || result.reopen_verified) &&
                          (result.authoritative_bytes == 0 ||
                           result.derived_bytes <= result.authoritative_bytes * 3 / 2);
  result.gate_classification = result.hard_gate_pass ? "pass" : "incomplete";
  return result;
}

std::string QueryBenchmarkCsvHeader() {
  return "operation,projection_state,degree,selectivity_percent,readers,cache_state,writers,facts_per_txn,seed,dataset_checksum,transactions,facts,rows,elapsed_seconds,transactions_per_second,facts_per_second,mib_per_second,group_fill_p50,query_samples,query_p50_us,query_p95_us,query_p99_us,write_p50_us,write_p95_us,write_p99_us,wal_sync_p99_us,end_to_end_p99_us,authoritative_bytes,derived_bytes,scratch_bytes,total_bytes,write_amplification,space_amplification,projection_lag,projection_work,cache_conditioned,operation_supported,projection_state_supported,metrics_complete,build_type,sanitizer,host,plan_fingerprint,raw_sample_path,terminal_status,reopen_verified,gate_classification,hard_gate_pass";
}

std::string QueryBenchmarkCsvRow(const QueryBenchmarkOptions& o,
                                 const QueryBenchmarkResult& r) {
  std::ostringstream x;
  const double t = r.elapsed_seconds ? r.transactions / r.elapsed_seconds : 0;
  const double f = r.elapsed_seconds ? r.facts / r.elapsed_seconds : 0;
  x << QueryBenchmarkOperationName(o.operation) << ',' << ProjectionStateName(o.projection)
    << ',' << o.degree << ',' << o.selectivity_percent << ',' << o.readers << ','
    << (o.cache == QueryCacheState::kCold ? "cold" : "warm") << ',' << o.writers << ','
    << o.facts_per_txn << ',' << o.seed << ',' << r.dataset_checksum << ','
    << r.transactions << ',' << r.facts << ',' << r.rows << ','
    << r.elapsed_seconds << ',' << t << ',' << f << ',' << r.mib_per_second << ','
    << r.group_fill_p50 << ',' << r.query_samples << ','
    << r.query_p50_us << ',' << r.query_p95_us << ',' << r.query_p99_us << ','
    << r.write_p50_us << ',' << r.write_p95_us << ',' << r.write_p99_us << ','
    << r.wal_sync_p99_us << ',' << r.end_to_end_p99_us << ',' << r.authoritative_bytes << ','
    << r.derived_bytes << ',' << r.scratch_bytes << ',' << r.total_bytes << ','
    << r.write_amplification << ',' << r.space_amplification << ','
    << r.projection_lag << ',' << (r.projection_active ? "active" : "paused") << ','
    << (r.cache_conditioned ? "true" : "false") << ','
    << (r.operation_supported ? "true" : "false") << ','
    << (r.projection_state_supported ? "true" : "false") << ','
    << (r.metrics_complete ? "true" : "false") << ','
    << r.build_type << ',' << r.sanitizer << ',' << r.host << ','
    << r.plan_fingerprint << ',' << r.raw_sample_path << ','
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
    << ",\"query_p50_us\":" << r.query_p50_us << ",\"query_p95_us\":"
    << r.query_p95_us << ",\"query_p99_us\":" << r.query_p99_us
    << ",\"authoritative_bytes\":" << r.authoritative_bytes
    << ",\"derived_bytes\":" << r.derived_bytes
    << ",\"scratch_bytes\":" << r.scratch_bytes
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
    << ",\"terminal_status\":\"" << r.terminal_status
    << "\",\"reopen_verified\":" << (r.reopen_verified ? "true" : "false")
    << ",\"gate_classification\":\"" << r.gate_classification
    << "\",\"hard_gate_pass\":" << (r.hard_gate_pass ? "true" : "false") << '}';
  return x.str();
}
}  // namespace cedar::benchmark
