#ifndef CEDAR_BENCHMARKS_CEDAR_QUERY_BENCH_OPTIONS_H_
#define CEDAR_BENCHMARKS_CEDAR_QUERY_BENCH_OPTIONS_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/status.h"

namespace cedar::benchmark {

enum class QueryBenchmarkOperation : uint8_t {
  kStateAt, kHistory, kEvents, kChanges, kExpandOut, kExpandIn,
  kExpandBoth, kPropertyFilter, kTemporalAggregate, kIntervalJoin, kKHop,
  kCoexistingShortestPath, kEarliestArrival, kLatestDeparture,
  kFastestDuration,
};
enum class ProjectionState : uint8_t {
  kCanonicalOnly, kBase, kShortDelta, kLongDelta, kPartialCoverage,
};
enum class QueryCacheState : uint8_t { kCold, kWarm };
enum class ProjectionWork : uint8_t { kPaused, kActive };

struct QueryBenchmarkOptions {
  std::string path;
  QueryBenchmarkOperation operation = QueryBenchmarkOperation::kStateAt;
  ProjectionState projection = ProjectionState::kCanonicalOnly;
  uint32_t degree = 10;
  double selectivity_percent = 1.0;
  uint32_t readers = 1;
  QueryCacheState cache = QueryCacheState::kCold;
  uint32_t max_hops = 4;
  uint64_t result_limit = 1000;
  bool capture_profile = false;
  uint64_t seed = 1;
  uint64_t duration_seconds = 10;
  uint64_t facts_per_txn = 16;
  bool verify_reopen = true;
  uint32_t writers = 1;
  ProjectionWork projection_work = ProjectionWork::kPaused;
};

StatusOr<QueryBenchmarkOptions> ParseQueryBenchmarkOptions(
    const std::vector<std::string>& arguments);
const char* QueryBenchmarkOperationName(QueryBenchmarkOperation operation);
const char* ProjectionStateName(ProjectionState state);
bool QueryBenchmarkOperationSupported(QueryBenchmarkOperation operation);

}  // namespace cedar::benchmark

#endif
