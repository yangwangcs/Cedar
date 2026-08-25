#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "cedar/query/result.h"
#include "query/runtime/relational.h"

namespace cedar::internal {
namespace {

RelationalCell Key(uint64_t value) {
  return RelationalCell::Present(QueryType::kInt64,
                                 static_cast<int64_t>(value));
}

RelationalRow Row(uint64_t key, int64_t value) {
  return {{Key(key), RelationalCell::Present(QueryType::kInt64, value)},
          std::nullopt};
}

RelationalRow IntervalRow(uint64_t key, uint64_t from, uint64_t to) {
  return {{Key(key), Key(key)},
          ValidTimeInterval{ValidTime{from}, ValidTime{to}}};
}

TEST(QueryComplexityRegression, DistinctGroupsScaleWithRowsNotGroupCount) {
  constexpr size_t kRows = 4096;
  BatchStream stream;
  stream.rows.reserve(kRows);
  for (size_t i = kRows; i-- > 0;) {
    stream.rows.push_back(Row(i, static_cast<int64_t>(i)));
  }
  AggregateInput input{std::move(stream), {0},
                       {{AggregateKind::kCount, 1}}};
  QueryReservation reservation(64ULL << 20);
  auto result = AggregateRows(input, &reservation);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.ValueOrDie().rows.size(), kRows);
}

TEST(QueryComplexityRegression, IntervalJoinBucketsByTypedKey) {
  constexpr size_t kRows = 2048;
  BatchStream left;
  BatchStream right;
  left.rows.reserve(kRows);
  right.rows.reserve(kRows);
  for (size_t i = kRows; i-- > 0;) {
    left.rows.push_back(IntervalRow(i, 0, 10));
    right.rows.push_back(IntervalRow(i, 2, 8));
  }
  TemporalJoinInput input{std::move(left), std::move(right), 0, 0,
                          JoinKind::kInner};
  FragmentBudget budget(kRows + 1);
  QueryReservation reservation(64ULL << 20);
  auto result = IntervalMergeJoin(input, &budget, &reservation);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().rows.size(), kRows);
  EXPECT_EQ(result.ValueOrDie().rows.front().effective->from.value, 2U);
  EXPECT_EQ(result.ValueOrDie().rows.front().effective->to->value, 8U);
}

TEST(QueryComplexityRegression, ReadCountersRemainFixedCardinality) {
  QueryExecutionState state;
  state.RecordCatalogLookup(true);
  state.RecordCatalogLookup(false);
  state.RecordChainSortFallback(7);
  state.RecordPageDirectory(true, 3);
  state.RecordPageDirectory(false, 2);
  state.RecordSpillPartitionRead(true);
  state.RecordSpillPartitionRead(false);
  state.RecordLimitEarlyStop();
  state.RecordMaterializationBytes(128);

  const auto counters = state.profile().complexity;
  EXPECT_EQ(counters.catalog_hits, 1U);
  EXPECT_EQ(counters.catalog_misses, 1U);
  EXPECT_EQ(counters.chain_sort_fallbacks, 1U);
  EXPECT_EQ(counters.chain_events_decoded, 7U);
  EXPECT_EQ(counters.page_directory_hits, 1U);
  EXPECT_EQ(counters.page_pruned, 5U);
  EXPECT_EQ(counters.spill_partition_reads, 2U);
  EXPECT_EQ(counters.spill_partition_rebuilds, 1U);
  EXPECT_EQ(counters.limit_early_stops, 1U);
  EXPECT_EQ(counters.materialization_bytes, 128U);
}

}  // namespace
}  // namespace cedar::internal
