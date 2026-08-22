#ifndef CEDAR_BENCHMARKS_CEDAR_QUERY_BENCH_WORKLOAD_H_
#define CEDAR_BENCHMARKS_CEDAR_QUERY_BENCH_WORKLOAD_H_
#include <cstdint>
#include <string>
#include "benchmarks/cedar_query_bench_options.h"
namespace cedar::benchmark {
struct QueryBenchmarkResult { uint64_t transactions=0, facts=0, rows=0; double elapsed_seconds=0; bool reopen_verified=false; std::string terminal_status="OK"; };
StatusOr<QueryBenchmarkResult> RunQueryBenchmark(const QueryBenchmarkOptions& options);
std::string QueryBenchmarkCsvHeader();
std::string QueryBenchmarkCsvRow(const QueryBenchmarkOptions&, const QueryBenchmarkResult&);
std::string QueryBenchmarkJson(const QueryBenchmarkOptions&, const QueryBenchmarkResult&);
}
#endif
