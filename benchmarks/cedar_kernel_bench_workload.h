#ifndef CEDAR_BENCHMARKS_CEDAR_KERNEL_BENCH_WORKLOAD_H_
#define CEDAR_BENCHMARKS_CEDAR_KERNEL_BENCH_WORKLOAD_H_

#include <cstdint>

#include "cedar/core/status.h"
#include "benchmarks/cedar_kernel_bench_options.h"

namespace cedar {
class Database;
struct DatabaseOptions;

namespace benchmark {

struct BoundedWriterResult {
  Status status;
  uint64_t attempted = 0;
  uint64_t committed = 0;
  uint64_t failures = 0;
};

BoundedWriterResult RunBoundedWriters(Database* database,
                                      uint32_t clients,
                                      uint64_t duration_seconds);
DatabaseOptions MakeBenchmarkDatabaseOptions(const KernelBenchmarkOptions& options);

}  // namespace benchmark
}  // namespace cedar

#endif  // CEDAR_BENCHMARKS_CEDAR_KERNEL_BENCH_WORKLOAD_H_
