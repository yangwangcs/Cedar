#ifndef CEDAR_BENCHMARKS_CEDAR_QUERY_BENCH_WORKLOAD_H_
#define CEDAR_BENCHMARKS_CEDAR_QUERY_BENCH_WORKLOAD_H_
#include <cstdint>
#include <string>
#include "benchmarks/cedar_query_bench_options.h"
namespace cedar {
class Database;
}
namespace cedar::benchmark {
struct QueryBenchmarkResult {
  uint64_t transactions = 0;
  uint64_t facts = 0;
  uint64_t measured_transactions = 0;
  uint64_t measured_facts = 0;
  uint64_t rows = 0;
  uint64_t query_operations = 0;
  uint64_t query_samples = 0;
  uint64_t query_p50_us = 0;
  uint64_t query_p95_us = 0;
  uint64_t query_p99_us = 0;
  uint64_t first_result_p50_us = 0;
  double query_qps = 0;
  double rows_per_second = 0;
  uint64_t write_p50_us = 0;
  uint64_t write_p95_us = 0;
  uint64_t write_p99_us = 0;
  uint64_t wal_sync_p99_us = 0;
  uint64_t end_to_end_p99_us = 0;
  uint64_t authoritative_bytes = 0;
  uint64_t derived_bytes = 0;
  uint64_t scratch_bytes = 0;
  uint64_t total_bytes = 0;
  uint64_t adjacency_bytes = 0;
  uint64_t property_bytes = 0;
  uint64_t statistics_bytes = 0;
  uint64_t engine_internal_bytes = 0;
  uint64_t wal_manifest_bytes = 0;
  double mib_per_second = 0;
  double write_mib_per_second = 0;
  double query_mib_per_second = 0;
  bool query_bytes_complete = false;
  uint64_t query_physical_bytes = 0;
  double write_amplification = 0;
  double space_amplification = 0;
  double total_space_amplification = 0;
  uint64_t group_fill_p50 = 0;
  uint64_t dataset_checksum = 0;
  uint64_t seed = 0;
  double elapsed_seconds = 0;
  double write_elapsed_seconds = 0;
  double query_elapsed_seconds = 0;
  double projection_lag = 0;
  bool projection_active = false;
  bool reopen_verified = false;
  bool hard_gate_pass = false;
  bool metrics_complete = false;
  bool cache_conditioned = false;
  bool maintenance_observed = false;
  bool operation_supported = true;
  bool projection_state_supported = true;
  std::string gate_classification = "incomplete";
  std::string terminal_status = "OK";
  std::string build_type = "unknown";
  std::string sanitizer = "none";
  std::string host = "unknown";
  std::string plan_fingerprint = "cedar-typed-runtime-v2";
  std::string raw_sample_path;
  std::string storage_inspection_status = "OK";
  std::string maintenance_status = "paused";
};
StatusOr<QueryBenchmarkResult> RunQueryBenchmark(const QueryBenchmarkOptions& options);
// Runs the graph and score setup used by RunQueryBenchmark. This benchmark
// helper keeps the setup transaction call sites directly testable without
// changing Cedar's public database API or defaults.
Status SeedQueryBenchmarkSetupForTesting(Database* database,
                                         uint64_t commit_deadline_us);
std::string QueryBenchmarkCsvHeader();
std::string QueryBenchmarkCsvRow(const QueryBenchmarkOptions&, const QueryBenchmarkResult&);
std::string QueryBenchmarkJson(const QueryBenchmarkOptions&, const QueryBenchmarkResult&);
}
#endif
