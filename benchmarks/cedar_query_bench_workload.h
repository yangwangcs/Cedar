#ifndef CEDAR_BENCHMARKS_CEDAR_QUERY_BENCH_WORKLOAD_H_
#define CEDAR_BENCHMARKS_CEDAR_QUERY_BENCH_WORKLOAD_H_
#include <cstdint>
#include <string>
#include "benchmarks/cedar_query_bench_options.h"
namespace cedar::benchmark {
struct QueryBenchmarkResult {
  uint64_t transactions = 0;
  uint64_t facts = 0;
  uint64_t rows = 0;
  uint64_t query_samples = 0;
  uint64_t query_p50_us = 0;
  uint64_t query_p95_us = 0;
  uint64_t query_p99_us = 0;
  uint64_t write_p50_us = 0;
  uint64_t write_p95_us = 0;
  uint64_t write_p99_us = 0;
  uint64_t wal_sync_p99_us = 0;
  uint64_t end_to_end_p99_us = 0;
  uint64_t authoritative_bytes = 0;
  uint64_t derived_bytes = 0;
  uint64_t scratch_bytes = 0;
  uint64_t total_bytes = 0;
  double elapsed_seconds = 0;
  double projection_lag = 0;
  bool projection_active = false;
  bool reopen_verified = false;
  bool hard_gate_pass = false;
  std::string gate_classification = "incomplete";
  std::string terminal_status = "OK";
};
StatusOr<QueryBenchmarkResult> RunQueryBenchmark(const QueryBenchmarkOptions& options);
std::string QueryBenchmarkCsvHeader();
std::string QueryBenchmarkCsvRow(const QueryBenchmarkOptions&, const QueryBenchmarkResult&);
std::string QueryBenchmarkJson(const QueryBenchmarkOptions&, const QueryBenchmarkResult&);
}
#endif
