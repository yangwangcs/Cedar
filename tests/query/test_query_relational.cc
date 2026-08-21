#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "cedar/database.h"
#include "query/runtime/relational.h"
#include "query/runtime/query_runtime.h"
#include "query/runtime/vector_kernels.h"

namespace cedar::internal {
namespace {

static_assert(!std::is_copy_constructible_v<BatchStream>);
static_assert(std::is_move_constructible_v<BatchStream>);

QueryColumn Int32Column(std::vector<int32_t> values,
                        std::vector<uint8_t> present = {}) {
  return {SlotId{1}, QueryType::kInt32, std::move(values),
          std::move(present)};
}

QueryColumn Int64Column(std::vector<int64_t> values,
                        std::vector<uint8_t> present = {}) {
  return {SlotId{1}, QueryType::kInt64, std::move(values),
          std::move(present)};
}

QueryColumn BoolColumn(std::vector<uint8_t> values,
                       std::vector<uint8_t> present = {}) {
  return {SlotId{1}, QueryType::kBool, std::move(values), std::move(present)};
}

RelationalCell Int64(int64_t value) {
  return RelationalCell::Present(QueryType::kInt64, value);
}

RelationalCell Float64(double value) {
  return RelationalCell::Present(QueryType::kFloat64, value);
}

RelationalCell MissingInt64() {
  return RelationalCell::Missing(QueryType::kInt64);
}

RelationalRow Row(int64_t key, int64_t payload) {
  return {{Int64(key), Int64(payload)}, std::nullopt};
}

RelationalRow FloatRow(double key, int64_t payload) {
  return {{Float64(key), Int64(payload)}, std::nullopt};
}

RelationalRow TemporalRow(int64_t key, uint64_t from, uint64_t to,
                          int64_t payload = 0) {
  return {{Int64(key), Int64(payload)},
          ValidTimeInterval{ValidTime{from}, ValidTime{to}}};
}

TEST(VectorKernelsTest, ComparisonAgainstMissingIsFalse) {
  QueryColumn left = Int64Column({7, 0, 9}, {1, 0, 1});
  QueryColumn right = Int64Column({7, 7, 7}, {1, 1, 1});
  EXPECT_EQ(EvaluateEqual(left, right),
            (BoolVector{true, false, false}));
  EXPECT_EQ(EvaluateNotEqual(left, right),
            (BoolVector{false, false, true}));
}

TEST(VectorKernelsTest, RejectsImplicitNumericConversion) {
  auto compared = EvaluateComparison(Int32Column({7}), Int64Column({7}),
                                     ComparisonKind::kEqual);
  ASSERT_FALSE(compared.ok());
  EXPECT_TRUE(compared.status().IsInvalidArgument());
}

TEST(VectorKernelsTest, ExplicitNarrowingCastReportsOverflow) {
  auto cast = CastColumn(
      Int64Column({std::numeric_limits<int64_t>::max()}), QueryType::kInt32);
  ASSERT_FALSE(cast.ok());
  EXPECT_TRUE(cast.status().IsNumericOverflow());
}

TEST(VectorKernelsTest, FiltersAndProjectsOnlySelectedPresentRows) {
  QueryColumn predicate = BoolColumn({1, 0, 1, 1}, {1, 1, 0, 1});
  const SelectionVector input{1, 2, 3};
  auto selected = FilterSelection(predicate, input);
  ASSERT_TRUE(selected.ok()) << selected.status().ToString();
  EXPECT_EQ(selected.ValueOrDie(), (SelectionVector{3}));

  auto projected = ProjectColumn(Int64Column({10, 20, 30, 40}),
                                 selected.ValueOrDie());
  ASSERT_TRUE(projected.ok()) << projected.status().ToString();
  EXPECT_EQ(std::get<std::vector<int64_t>>(projected.ValueOrDie().values),
            (std::vector<int64_t>{40}));
}

TEST(VectorKernelsTest, ProjectionCapacityDependsOnlyOnSelectedRows) {
  constexpr size_t kRejectedRows = 64;
  QueryColumn input{SlotId{1}, QueryType::kString,
                    std::vector<std::string>(kRejectedRows + 1,
                                             std::string(512, 'x')),
                    {}};
  const SelectionVector selected{kRejectedRows};

  auto projected = ProjectColumn(input, selected);

  ASSERT_TRUE(projected.ok()) << projected.status().ToString();
  const auto& values =
      std::get<std::vector<std::string>>(projected.ValueOrDie().values);
  ASSERT_EQ(values.size(), 1U);
  EXPECT_LT(values.capacity(), kRejectedRows);
}

TEST(VectorKernelsTest, DictionaryStringsCompareIdsOnlyWhenDictionaryIsShared) {
  auto first_dictionary =
      std::make_shared<const std::vector<std::string>>(
          std::vector<std::string>{"alpha", "beta"});
  auto second_dictionary =
      std::make_shared<const std::vector<std::string>>(
          std::vector<std::string>{"beta", "alpha"});
  DictionaryStringColumn left{first_dictionary, {0, 1}, {1, 1}};
  DictionaryStringColumn shared{first_dictionary, {0, 0}, {1, 1}};
  DictionaryStringColumn distinct{second_dictionary, {1, 1}, {1, 1}};
  EXPECT_EQ(EvaluateEqual(left, shared), (BoolVector{true, false}));
  EXPECT_EQ(EvaluateEqual(left, distinct), (BoolVector{true, false}));
}

TEST(RelationalTest, UnionDistinctStableSortAndLimitAreExact) {
  QueryReservation reservation(1 << 20);
  auto rows = UnionAll(BatchStream{{Row(2, 20), Row(1, 10)}},
                       BatchStream{{Row(2, 20), Row(1, 11)}}, &reservation);
  ASSERT_TRUE(rows.ok()) << rows.status().ToString();
  auto distinct = Distinct(rows.ValueOrDie(), &reservation);
  ASSERT_TRUE(distinct.ok()) << distinct.status().ToString();
  EXPECT_EQ(distinct.ValueOrDie().rows.size(), 3U);

  QueryReservation sort_reservation(1 << 20);
  auto sorted = Sort(rows.ValueOrDie(), {{0, SortDirection::kAscending}},
                     &sort_reservation);
  ASSERT_TRUE(sorted.ok()) << sorted.status().ToString();
  EXPECT_EQ(sorted.ValueOrDie().rows,
            (std::vector<RelationalRow>{Row(1, 10), Row(1, 11), Row(2, 20),
                                        Row(2, 20)}));
  auto limited = Limit(sorted.ValueOrDie(), 1, 2, &reservation);
  ASSERT_TRUE(limited.ok()) << limited.status().ToString();
  EXPECT_EQ(limited.ValueOrDie().rows,
            (std::vector<RelationalRow>{Row(1, 11), Row(2, 20)}));
  EXPECT_FALSE(rows.ValueOrDie().order_specified);
}

TEST(RelationalTest, ImplementsInnerSemiAndAntiEqualityJoins) {
  JoinInput input{{{Row(1, 10), Row(2, 20)}},
                  {{Row(2, 200), Row(3, 300)}}, 0, 0, JoinKind::kInner};
  QueryReservation reservation(1 << 20);
  auto inner = IndexNestedLoopJoin(input, &reservation);
  ASSERT_TRUE(inner.ok()) << inner.status().ToString();
  ASSERT_EQ(inner.ValueOrDie().rows.size(), 1U);
  EXPECT_EQ(inner.ValueOrDie().rows.front().cells,
            (std::vector<RelationalCell>{Int64(2), Int64(20), Int64(2),
                                         Int64(200)}));

  input.kind = JoinKind::kSemi;
  EXPECT_EQ(IndexNestedLoopJoin(input, &reservation).ValueOrDie().rows,
            (std::vector<RelationalRow>{Row(2, 20)}));
  input.kind = JoinKind::kAnti;
  EXPECT_EQ(IndexNestedLoopJoin(input, &reservation).ValueOrDie().rows,
            (std::vector<RelationalRow>{Row(1, 10)}));
}

TEST(RelationalTest, MissingJoinKeysNeverCompareEqual) {
  RelationalRow missing{{MissingInt64(), Int64(10)}, std::nullopt};
  JoinInput input{{{missing}}, {{missing}}, 0, 0, JoinKind::kInner};
  QueryReservation reservation(1 << 20);
  EXPECT_TRUE(
      IndexNestedLoopJoin(input, &reservation).ValueOrDie().rows.empty());
}

TEST(RelationalTest, HashJoinReturnsNeedsSpillWithoutExceedingReservation) {
  JoinInput input{{{Row(1, 10)}}, {{Row(1, 20)}}, 0, 0,
                  JoinKind::kInner};
  QueryReservation reservation(1);
  auto result = HashJoin(input, &reservation);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(IsNeedsSpill(result.status()));
  EXPECT_LE(reservation.used_bytes(), reservation.limit_bytes());
}

TEST(RelationalTest, HashAndSortJoinsReserveTheirPublishedOutput) {
  JoinInput input{{{Row(1, 10)}}, {{Row(1, 20)}}, 0, 0,
                  JoinKind::kInner};
  const size_t minimum_output_bytes =
      sizeof(RelationalRow) + 4 * sizeof(RelationalCell);

  QueryReservation hash_reservation(4096);
  auto hashed = HashJoin(input, &hash_reservation);
  ASSERT_TRUE(hashed.ok()) << hashed.status().ToString();
  EXPECT_GE(hash_reservation.peak_bytes(), minimum_output_bytes);
  EXPECT_LE(hash_reservation.peak_bytes(), hash_reservation.limit_bytes());
  EXPECT_GT(hash_reservation.used_bytes(), 0U);

  QueryReservation sort_reservation(4096);
  auto sorted = SortMergeJoin(input, &sort_reservation);
  ASSERT_TRUE(sorted.ok()) << sorted.status().ToString();
  EXPECT_GE(sort_reservation.peak_bytes(), minimum_output_bytes);
  EXPECT_LE(sort_reservation.peak_bytes(), sort_reservation.limit_bytes());
  EXPECT_GT(sort_reservation.used_bytes(), 0U);

  QueryReservation hash_spill(1);
  auto hash_overflow = HashJoin(input, &hash_spill);
  ASSERT_FALSE(hash_overflow.ok());
  EXPECT_TRUE(IsNeedsSpill(hash_overflow.status()));
  EXPECT_EQ(hash_spill.used_bytes(), 0U);

  QueryReservation sort_spill(1);
  auto sort_overflow = SortMergeJoin(input, &sort_spill);
  ASSERT_FALSE(sort_overflow.ok());
  EXPECT_TRUE(IsNeedsSpill(sort_overflow.status()));
  EXPECT_EQ(sort_spill.used_bytes(), 0U);
}

TEST(RelationalTest, PublishedOutputRetainsReservationUntilStreamDestruction) {
  JoinInput input{{{Row(1, 10)}}, {{Row(1, 20)}}, 0, 0,
                  JoinKind::kInner};

  QueryReservation hash_reservation(4096);
  {
    auto hashed = HashJoin(input, &hash_reservation);
    ASSERT_TRUE(hashed.ok()) << hashed.status().ToString();
    EXPECT_GT(hash_reservation.used_bytes(), 0U);
  }
  EXPECT_EQ(hash_reservation.used_bytes(), 0U);

  QueryReservation sort_reservation(4096);
  {
    auto sorted = SortMergeJoin(input, &sort_reservation);
    ASSERT_TRUE(sorted.ok()) << sorted.status().ToString();
    EXPECT_GT(sort_reservation.used_bytes(), 0U);
  }
  EXPECT_EQ(sort_reservation.used_bytes(), 0U);
}

TEST(RelationalTest, HashAndSortOperatorsRequireReservationAndDiagnoseSpill) {
  JoinInput input{{{Row(1, 10)}}, {{Row(1, 20)}}, 0, 0,
                  JoinKind::kInner};
  auto hash = HashJoin(input, nullptr);
  ASSERT_FALSE(hash.ok());
  EXPECT_TRUE(IsNeedsSpill(hash.status()));
  EXPECT_NE(hash.status().ToString().find("memory_bytes"), std::string::npos);

  auto sort_merge = SortMergeJoin(input, nullptr);
  ASSERT_FALSE(sort_merge.ok());
  EXPECT_TRUE(IsNeedsSpill(sort_merge.status()));

  auto sort = Sort(BatchStream{{Row(2, 20), Row(1, 10)}},
                   {{0, SortDirection::kAscending}}, nullptr);
  ASSERT_FALSE(sort.ok());
  EXPECT_TRUE(IsNeedsSpill(sort.status()));
}

TEST(RelationalTest, HashAndSortMergeProduceAllAndOnlyMatchingRows) {
  JoinInput input{{{Row(3, 30), Row(1, 10), Row(1, 11), Row(2, 20)}},
                  {{Row(1, 100), Row(2, 200), Row(1, 101)}}, 0, 0,
                  JoinKind::kInner};
  QueryReservation reservation(1 << 20);
  auto hashed = HashJoin(input, &reservation);
  ASSERT_TRUE(hashed.ok()) << hashed.status().ToString();
  EXPECT_EQ(hashed.ValueOrDie().rows.size(), 5U);

  auto merged = SortMergeJoin(input, &reservation);
  ASSERT_TRUE(merged.ok()) << merged.status().ToString();
  EXPECT_EQ(merged.ValueOrDie().rows.size(), 5U);
  EXPECT_TRUE(merged.ValueOrDie().order_specified);
}

TEST(RelationalTest, FloatJoinKeysUseConsistentEqualityHashingAndOrdering) {
  JoinInput signed_zero{{{FloatRow(-0.0, 10)}}, {{FloatRow(+0.0, 20)}},
                        0, 0, JoinKind::kInner};
  QueryReservation hash_reservation(4096);
  auto hashed = HashJoin(signed_zero, &hash_reservation);
  ASSERT_TRUE(hashed.ok()) << hashed.status().ToString();
  EXPECT_EQ(hashed.ValueOrDie().rows.size(), 1U);

  const double nan = std::numeric_limits<double>::quiet_NaN();
  JoinInput nan_and_number{{{FloatRow(nan, 10), FloatRow(1.0, 11)}},
                           {{FloatRow(1.0, 20)}}, 0, 0, JoinKind::kAnti};
  QueryReservation sort_reservation(1 << 20);
  auto merged = SortMergeJoin(nan_and_number, &sort_reservation);
  ASSERT_TRUE(merged.ok()) << merged.status().ToString();
  ASSERT_EQ(merged.ValueOrDie().rows.size(), 1U);
  EXPECT_TRUE(std::isnan(
      std::get<double>(merged.ValueOrDie().rows.front().cells.front().value)));
}

TEST(RelationalTest, ChoosesAlgorithmsAtExactPlannerBoundaries) {
  EXPECT_EQ(ChooseJoinAlgorithm(4095, false, false),
            JoinAlgorithm::kIndexNestedLoop);
  EXPECT_EQ(ChooseJoinAlgorithm(4096, false, false), JoinAlgorithm::kHash);
  EXPECT_EQ(ChooseJoinAlgorithm(4096, true, false), JoinAlgorithm::kSortMerge);
  EXPECT_EQ(ChooseJoinAlgorithm(4096, false, true),
            JoinAlgorithm::kIntervalMerge);
}

TEST(RelationalTest, IntervalJoinEmitsOnlyClippedIntersections) {
  TemporalJoinInput input{
      {{TemporalRow(1, 0, 10), TemporalRow(1, 15, 30)}},
      {{TemporalRow(1, 5, 20)}}, 0, 0, JoinKind::kInner};
  FragmentBudget budget(10);
  QueryReservation reservation(1 << 20);
  auto result = IntervalMergeJoin(input, &budget, &reservation);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().rows.size(), 2U);
  EXPECT_EQ(result.ValueOrDie().rows[0].effective,
            (ValidTimeInterval{ValidTime{5}, ValidTime{10}}));
  EXPECT_EQ(result.ValueOrDie().rows[1].effective,
            (ValidTimeInterval{ValidTime{15}, ValidTime{20}}));
}

TEST(RelationalTest, IntervalMergeEstablishesKeyAndEffectiveTimeOrder) {
  TemporalJoinInput input{
      {{TemporalRow(2, 20, 30), TemporalRow(1, 10, 20),
        TemporalRow(1, 0, 10)}},
      {{TemporalRow(2, 0, 40), TemporalRow(1, 0, 40)}}, 0, 0,
      JoinKind::kInner};
  FragmentBudget budget(8);
  QueryReservation reservation(1 << 20);

  auto result = IntervalMergeJoin(input, &budget, &reservation);

  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_TRUE(result.ValueOrDie().order_specified);
  ASSERT_EQ(result.ValueOrDie().rows.size(), 2U);
  EXPECT_EQ(result.ValueOrDie().rows[0].cells.front(), Int64(1));
  EXPECT_EQ(result.ValueOrDie().rows[0].effective,
            (ValidTimeInterval{ValidTime{0}, ValidTime{20}}));
  EXPECT_EQ(result.ValueOrDie().rows[1].cells.front(), Int64(2));
  EXPECT_EQ(result.ValueOrDie().rows[1].effective,
            (ValidTimeInterval{ValidTime{20}, ValidTime{30}}));
}

TEST(RelationalTest, IntervalMergeGloballyOrdersInterleavedOverlaps) {
  TemporalJoinInput input{
      {{TemporalRow(1, 10, 80, 1), TemporalRow(1, 55, 70, 2)}},
      {{TemporalRow(1, 10, 60, 10), TemporalRow(1, 70, 80, 20)}}, 0, 0,
      JoinKind::kInner};
  FragmentBudget budget(8);
  QueryReservation reservation(1 << 20);

  auto result = IntervalMergeJoin(input, &budget, &reservation);

  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_TRUE(result.ValueOrDie().order_specified);
  ASSERT_EQ(result.ValueOrDie().rows.size(), 3U);
  EXPECT_EQ(result.ValueOrDie().rows[0].effective,
            (ValidTimeInterval{ValidTime{10}, ValidTime{60}}));
  EXPECT_EQ(result.ValueOrDie().rows[1].effective,
            (ValidTimeInterval{ValidTime{55}, ValidTime{60}}));
  EXPECT_EQ(result.ValueOrDie().rows[2].effective,
            (ValidTimeInterval{ValidTime{70}, ValidTime{80}}));
}

TEST(RelationalTest, TemporalOperatorsRejectZeroFragmentsBeforeOutputReserve) {
  TemporalJoinInput join_input{{{TemporalRow(1, 0, 10)}},
                               {{TemporalRow(1, 0, 10)}}, 0, 0,
                               JoinKind::kInner};
  FragmentBudget no_fragments(0);
  QueryReservation join_reservation(1 << 20);
  auto join = IntervalMergeJoin(join_input, &no_fragments, &join_reservation);
  ASSERT_FALSE(join.ok());
  EXPECT_TRUE(join.status().IsResourceExhausted());
  EXPECT_EQ(join_reservation.peak_bytes(), 0U);

  TemporalAggregateInput aggregate_input{
      BatchStream{{TemporalRow(1, 0, 10)}}, {0}};
  FragmentBudget aggregate_no_fragments(0);
  QueryReservation aggregate_reservation(1 << 20);
  auto aggregate = TemporalAggregate(aggregate_input, &aggregate_no_fragments,
                                     &aggregate_reservation);
  ASSERT_FALSE(aggregate.ok());
  EXPECT_TRUE(aggregate.status().IsResourceExhausted());
  EXPECT_EQ(aggregate_reservation.peak_bytes(), 0U);
}

TEST(RelationalTest, TemporalAntiJoinDerivesMissingFragments) {
  TemporalJoinInput input{{{TemporalRow(1, 0, 20)}},
                          {{TemporalRow(1, 5, 10), TemporalRow(1, 15, 20)}},
                          0, 0, JoinKind::kAnti};
  FragmentBudget budget(10);
  QueryReservation reservation(1 << 20);
  auto result = IntervalMergeJoin(input, &budget, &reservation);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().rows.size(), 2U);
  EXPECT_EQ(result.ValueOrDie().rows[0].effective,
            (ValidTimeInterval{ValidTime{0}, ValidTime{5}}));
  EXPECT_EQ(result.ValueOrDie().rows[1].effective,
            (ValidTimeInterval{ValidTime{10}, ValidTime{15}}));
}

TEST(RelationalTest, CoalescedTemporalOutputConsumesOneFragment) {
  TemporalJoinInput input{
      {{TemporalRow(1, 0, 5), TemporalRow(1, 5, 10)}},
      {{TemporalRow(1, 0, 10)}}, 0, 0, JoinKind::kInner};
  FragmentBudget budget(1);
  QueryReservation reservation(1 << 20);
  auto result = IntervalMergeJoin(input, &budget, &reservation);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().rows.size(), 1U);
  EXPECT_EQ(result.ValueOrDie().rows.front().effective,
            (ValidTimeInterval{ValidTime{0}, ValidTime{10}}));
  EXPECT_EQ(budget.used_fragments(), 1U);
}

TEST(RelationalTest, TemporalSemiJoinUnionsOverlappingCoverage) {
  TemporalJoinInput input{{{TemporalRow(1, 0, 10)}},
                          {{TemporalRow(1, 0, 7), TemporalRow(1, 5, 10)}},
                          0, 0, JoinKind::kSemi};
  FragmentBudget budget(1);
  QueryReservation reservation(1 << 20);
  auto result = IntervalMergeJoin(input, &budget, &reservation);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().rows.size(), 1U);
  EXPECT_EQ(result.ValueOrDie().rows.front().effective,
            (ValidTimeInterval{ValidTime{0}, ValidTime{10}}));
  EXPECT_EQ(budget.used_fragments(), 1U);
}

TEST(RelationalTest, TemporalAggregateProcessesExitBeforeEntry) {
  TemporalAggregateInput input{
      {{TemporalRow(1, 0, 5), TemporalRow(1, 5, 10)}}, {0}};
  FragmentBudget budget(10);
  QueryReservation reservation(1 << 20);
  auto result = TemporalAggregate(input, &budget, &reservation);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().rows.size(), 1U);
  EXPECT_EQ(result.ValueOrDie().rows.front().effective,
            (ValidTimeInterval{ValidTime{0}, ValidTime{10}}));
  EXPECT_EQ(result.ValueOrDie().rows.front().cells,
            (std::vector<RelationalCell>{Int64(1), Int64(1)}));
}

TEST(RelationalTest, FragmentBudgetFailsBeforePublishingExtraOutput) {
  TemporalJoinInput input{{{TemporalRow(1, 0, 10), TemporalRow(1, 15, 30)}},
                          {{TemporalRow(1, 5, 20)}}, 0, 0,
                          JoinKind::kInner};
  FragmentBudget budget(1);
  QueryReservation reservation(1 << 20);
  auto result = IntervalMergeJoin(input, &budget, &reservation);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsResourceExhausted());
  EXPECT_EQ(budget.used_fragments(), 1U);
}

TEST(RelationalTest, RowAggregateCountsAndSumsStrictlyTypedValues) {
  AggregateInput input{{{Row(1, 10), Row(1, 20), Row(2, 7)}},
                       {0}, {{AggregateKind::kCount, 1},
                             {AggregateKind::kSum, 1}}};
  QueryReservation reservation(1 << 20);
  auto result = AggregateRows(input, &reservation);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.ValueOrDie().rows,
            (std::vector<RelationalRow>{
                {{Int64(1), Int64(2), Int64(30)}, std::nullopt},
                {{Int64(2), Int64(1), Int64(7)}, std::nullopt}}));
}

TEST(QueryRuntimeRelationalTest,
     PullBoundaryDispatchesJoinsAndAggregatesThroughLogicalNodes) {
  const auto equality_input = [](size_t estimated_rows, bool sorted = false) {
    RuntimeRelationalInput input;
    input.left = BatchStream{{Row(1, 10)}};
    input.right = BatchStream{{Row(1, 20)}};
    input.left_key = 0;
    input.right_key = 0;
    input.estimated_rows = estimated_rows;
    input.sorted_keys = sorted;
    return input;
  };
  QueryReservation reservation(1 << 20);

  auto nested = ExecuteRelationalPlanNode(
      LogicalOpKind::kInnerJoin, equality_input(4095), &reservation);
  ASSERT_TRUE(nested.ok()) << nested.status().ToString();
  EXPECT_EQ(nested.ValueOrDie().join_algorithm,
            std::optional<JoinAlgorithm>{JoinAlgorithm::kIndexNestedLoop});
  EXPECT_EQ(nested.ValueOrDie().stream.rows.size(), 1U);

  auto hashed = ExecuteRelationalPlanNode(
      LogicalOpKind::kInnerJoin, equality_input(4096), &reservation);
  ASSERT_TRUE(hashed.ok()) << hashed.status().ToString();
  EXPECT_EQ(hashed.ValueOrDie().join_algorithm,
            std::optional<JoinAlgorithm>{JoinAlgorithm::kHash});

  auto merged = ExecuteRelationalPlanNode(
      LogicalOpKind::kInnerJoin, equality_input(4096, true), &reservation);
  ASSERT_TRUE(merged.ok()) << merged.status().ToString();
  EXPECT_EQ(merged.ValueOrDie().join_algorithm,
            std::optional<JoinAlgorithm>{JoinAlgorithm::kSortMerge});

  RuntimeRelationalInput temporal_input;
  temporal_input.temporal = true;
  temporal_input.left = BatchStream{{TemporalRow(1, 0, 10)}};
  temporal_input.right = BatchStream{{TemporalRow(1, 5, 20)}};
  FragmentBudget fragments(8);
  QueryReservation temporal_reservation(1 << 20);
  auto interval = ExecuteRelationalPlanNode(
      LogicalOpKind::kInnerJoin, std::move(temporal_input),
      &temporal_reservation, &fragments);
  ASSERT_TRUE(interval.ok()) << interval.status().ToString();
  EXPECT_EQ(interval.ValueOrDie().join_algorithm,
            std::optional<JoinAlgorithm>{JoinAlgorithm::kIntervalMerge});
  EXPECT_EQ(interval.ValueOrDie().stream.rows.front().effective,
            (ValidTimeInterval{ValidTime{5}, ValidTime{10}}));

  RuntimeRelationalInput aggregate;
  aggregate.left = BatchStream{{Row(1, 10), Row(1, 20)}};
  aggregate.group_by = {0};
  aggregate.aggregates = {{AggregateKind::kCount, 1},
                          {AggregateKind::kSum, 1}};
  QueryReservation aggregate_reservation(1 << 20);
  auto rows = ExecuteRelationalPlanNode(LogicalOpKind::kAggregateRows,
                                        std::move(aggregate),
                                        &aggregate_reservation);
  ASSERT_TRUE(rows.ok()) << rows.status().ToString();
  EXPECT_EQ(rows.ValueOrDie().stream.rows.front().cells,
            (std::vector<RelationalCell>{Int64(1), Int64(2), Int64(30)}));

  RuntimeRelationalInput temporal_aggregate;
  temporal_aggregate.left =
      BatchStream{{TemporalRow(1, 0, 5), TemporalRow(1, 5, 10)}};
  temporal_aggregate.group_by = {0};
  FragmentBudget aggregate_fragments(8);
  QueryReservation temporal_aggregate_reservation(1 << 20);
  auto temporal = ExecuteRelationalPlanNode(LogicalOpKind::kTemporalAggregate,
                                            std::move(temporal_aggregate),
                                            &temporal_aggregate_reservation,
                                            &aggregate_fragments);
  ASSERT_TRUE(temporal.ok()) << temporal.status().ToString();
  EXPECT_EQ(temporal.ValueOrDie().stream.rows.front().effective,
            (ValidTimeInterval{ValidTime{0}, ValidTime{10}}));
}

TEST(QueryRuntimeRelationalTest,
     EveryMaterializingOperatorHonorsTheRuntimeMemoryReservation) {
  const auto expect_memory_failure = [](LogicalOpKind kind,
                                        RuntimeRelationalInput input,
                                        FragmentBudget* fragments = nullptr) {
    QueryReservation reservation(1);
    auto result = ExecuteRelationalPlanNode(kind, std::move(input),
                                            &reservation, fragments);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().IsResourceExhausted());
    EXPECT_NE(result.status().ToString().find("memory_bytes"),
              std::string::npos);
    EXPECT_LE(reservation.used_bytes(), reservation.limit_bytes());
  };

  const auto binary_input = [] {
    RuntimeRelationalInput input;
    input.left = BatchStream{{Row(1, 10)}};
    input.right = BatchStream{{Row(1, 20)}};
    return input;
  };
  const auto unary_input = [] {
    RuntimeRelationalInput input;
    input.left = BatchStream{{Row(1, 10)}};
    return input;
  };
  expect_memory_failure(LogicalOpKind::kUnionAll, binary_input());
  expect_memory_failure(LogicalOpKind::kDistinct, unary_input());
  auto limited = unary_input();
  limited.count = 1;
  expect_memory_failure(LogicalOpKind::kLimit, std::move(limited));
  auto joined = binary_input();
  joined.estimated_rows = 1;
  expect_memory_failure(LogicalOpKind::kInnerJoin, std::move(joined));
  auto aggregate = unary_input();
  aggregate.group_by = {0};
  aggregate.aggregates = {{AggregateKind::kCount, 1}};
  expect_memory_failure(LogicalOpKind::kAggregateRows, std::move(aggregate));

  RuntimeRelationalInput temporal;
  temporal.left = BatchStream{{TemporalRow(1, 0, 10)}};
  temporal.group_by = {0};
  FragmentBudget aggregate_fragments(8);
  expect_memory_failure(LogicalOpKind::kTemporalAggregate, std::move(temporal),
                        &aggregate_fragments);

  temporal = RuntimeRelationalInput{};
  temporal.left = BatchStream{{TemporalRow(1, 0, 10)}};
  temporal.right = BatchStream{{TemporalRow(1, 0, 10)}};
  temporal.temporal = true;
  temporal.estimated_rows = 1;
  FragmentBudget join_fragments(8);
  expect_memory_failure(LogicalOpKind::kInnerJoin, std::move(temporal),
                        &join_fragments);
}

TEST(QueryRuntimeRelationalTest,
     OutputRowBudgetFailsBeforeRelationalOutputReservation) {
  RuntimeRelationalInput input;
  input.left = BatchStream{{Row(1, 10)}};
  input.right = BatchStream{{Row(1, 20)}};
  input.estimated_rows = 1;
  QueryReservation reservation(1 << 20);

  auto result = ExecuteRelationalPlanNode(LogicalOpKind::kInnerJoin,
                                          std::move(input), &reservation,
                                          nullptr, 0);

  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsResourceExhausted());
  EXPECT_NE(result.status().ToString().find("output row budget"),
            std::string::npos);
  EXPECT_EQ(reservation.peak_bytes(), 0U);
}

}  // namespace
}  // namespace cedar::internal
