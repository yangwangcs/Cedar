#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "cedar/database.h"
#include "query/runtime/relational.h"
#include "query/runtime/query_runtime.h"
#include "query/runtime/vector_kernels.h"

namespace cedar::internal {
namespace {

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

RelationalCell MissingInt64() {
  return RelationalCell::Missing(QueryType::kInt64);
}

RelationalRow Row(int64_t key, int64_t payload) {
  return {{Int64(key), Int64(payload)}, std::nullopt};
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
  BatchStream rows = UnionAll(BatchStream{{Row(2, 20), Row(1, 10)}},
                              BatchStream{{Row(2, 20), Row(1, 11)}});
  EXPECT_EQ(Distinct(rows).rows.size(), 3U);

  QueryReservation sort_reservation(1 << 20);
  auto sorted = Sort(rows, {{0, SortDirection::kAscending}},
                     &sort_reservation);
  ASSERT_TRUE(sorted.ok()) << sorted.status().ToString();
  EXPECT_EQ(sorted.ValueOrDie().rows,
            (std::vector<RelationalRow>{Row(1, 10), Row(1, 11), Row(2, 20),
                                        Row(2, 20)}));
  EXPECT_EQ(Limit(sorted.ValueOrDie(), 1, 2).rows,
            (std::vector<RelationalRow>{Row(1, 11), Row(2, 20)}));
  EXPECT_FALSE(rows.order_specified);
}

TEST(RelationalTest, ImplementsInnerSemiAndAntiEqualityJoins) {
  JoinInput input{{{Row(1, 10), Row(2, 20)}},
                  {{Row(2, 200), Row(3, 300)}}, 0, 0, JoinKind::kInner};
  auto inner = IndexNestedLoopJoin(input);
  ASSERT_TRUE(inner.ok()) << inner.status().ToString();
  ASSERT_EQ(inner.ValueOrDie().rows.size(), 1U);
  EXPECT_EQ(inner.ValueOrDie().rows.front().cells,
            (std::vector<RelationalCell>{Int64(2), Int64(20), Int64(2),
                                         Int64(200)}));

  input.kind = JoinKind::kSemi;
  EXPECT_EQ(IndexNestedLoopJoin(input).ValueOrDie().rows,
            (std::vector<RelationalRow>{Row(2, 20)}));
  input.kind = JoinKind::kAnti;
  EXPECT_EQ(IndexNestedLoopJoin(input).ValueOrDie().rows,
            (std::vector<RelationalRow>{Row(1, 10)}));
}

TEST(RelationalTest, MissingJoinKeysNeverCompareEqual) {
  RelationalRow missing{{MissingInt64(), Int64(10)}, std::nullopt};
  JoinInput input{{{missing}}, {{missing}}, 0, 0, JoinKind::kInner};
  EXPECT_TRUE(IndexNestedLoopJoin(input).ValueOrDie().rows.empty());
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
  EXPECT_EQ(hash_reservation.used_bytes(), 0U);

  QueryReservation sort_reservation(4096);
  auto sorted = SortMergeJoin(input, &sort_reservation);
  ASSERT_TRUE(sorted.ok()) << sorted.status().ToString();
  EXPECT_GE(sort_reservation.peak_bytes(), minimum_output_bytes);
  EXPECT_LE(sort_reservation.peak_bytes(), sort_reservation.limit_bytes());
  EXPECT_EQ(sort_reservation.used_bytes(), 0U);

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
  auto result = IntervalMergeJoin(input, &budget);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().rows.size(), 2U);
  EXPECT_EQ(result.ValueOrDie().rows[0].effective,
            (ValidTimeInterval{ValidTime{5}, ValidTime{10}}));
  EXPECT_EQ(result.ValueOrDie().rows[1].effective,
            (ValidTimeInterval{ValidTime{15}, ValidTime{20}}));
}

TEST(RelationalTest, TemporalAntiJoinDerivesMissingFragments) {
  TemporalJoinInput input{{{TemporalRow(1, 0, 20)}},
                          {{TemporalRow(1, 5, 10), TemporalRow(1, 15, 20)}},
                          0, 0, JoinKind::kAnti};
  FragmentBudget budget(10);
  auto result = IntervalMergeJoin(input, &budget);
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
  auto result = IntervalMergeJoin(input, &budget);
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
  auto result = IntervalMergeJoin(input, &budget);
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
  auto result = TemporalAggregate(input, &budget);
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
  auto result = IntervalMergeJoin(input, &budget);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsResourceExhausted());
  EXPECT_EQ(budget.used_fragments(), 1U);
}

TEST(RelationalTest, RowAggregateCountsAndSumsStrictlyTypedValues) {
  AggregateInput input{{{Row(1, 10), Row(1, 20), Row(2, 7)}},
                       {0}, {{AggregateKind::kCount, 1},
                             {AggregateKind::kSum, 1}}};
  auto result = AggregateRows(input);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.ValueOrDie().rows,
            (std::vector<RelationalRow>{
                {{Int64(1), Int64(2), Int64(30)}, std::nullopt},
                {{Int64(2), Int64(1), Int64(7)}, std::nullopt}}));
}

TEST(QueryRuntimeRelationalTest,
     PullBoundaryDispatchesJoinsAndAggregatesThroughLogicalNodes) {
  RuntimeRelationalInput input;
  input.left = BatchStream{{Row(1, 10)}};
  input.right = BatchStream{{Row(1, 20)}};
  input.left_key = 0;
  input.right_key = 0;
  input.estimated_rows = 4095;
  QueryReservation reservation(1 << 20);

  auto nested = ExecuteRelationalPlanNode(LogicalOpKind::kInnerJoin, input,
                                          &reservation);
  ASSERT_TRUE(nested.ok()) << nested.status().ToString();
  EXPECT_EQ(nested.ValueOrDie().join_algorithm,
            std::optional<JoinAlgorithm>{JoinAlgorithm::kIndexNestedLoop});
  EXPECT_EQ(nested.ValueOrDie().stream.rows.size(), 1U);

  input.estimated_rows = 4096;
  auto hashed = ExecuteRelationalPlanNode(LogicalOpKind::kInnerJoin, input,
                                          &reservation);
  ASSERT_TRUE(hashed.ok()) << hashed.status().ToString();
  EXPECT_EQ(hashed.ValueOrDie().join_algorithm,
            std::optional<JoinAlgorithm>{JoinAlgorithm::kHash});

  input.sorted_keys = true;
  auto merged = ExecuteRelationalPlanNode(LogicalOpKind::kInnerJoin, input,
                                          &reservation);
  ASSERT_TRUE(merged.ok()) << merged.status().ToString();
  EXPECT_EQ(merged.ValueOrDie().join_algorithm,
            std::optional<JoinAlgorithm>{JoinAlgorithm::kSortMerge});

  input.temporal = true;
  input.left = BatchStream{{TemporalRow(1, 0, 10)}};
  input.right = BatchStream{{TemporalRow(1, 5, 20)}};
  FragmentBudget fragments(8);
  auto interval = ExecuteRelationalPlanNode(LogicalOpKind::kInnerJoin, input,
                                            nullptr, &fragments);
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
  auto rows = ExecuteRelationalPlanNode(LogicalOpKind::kAggregateRows,
                                        aggregate, nullptr);
  ASSERT_TRUE(rows.ok()) << rows.status().ToString();
  EXPECT_EQ(rows.ValueOrDie().stream.rows.front().cells,
            (std::vector<RelationalCell>{Int64(1), Int64(2), Int64(30)}));

  aggregate.left = BatchStream{{TemporalRow(1, 0, 5), TemporalRow(1, 5, 10)}};
  FragmentBudget aggregate_fragments(8);
  auto temporal = ExecuteRelationalPlanNode(LogicalOpKind::kTemporalAggregate,
                                            aggregate, nullptr,
                                            &aggregate_fragments);
  ASSERT_TRUE(temporal.ok()) << temporal.status().ToString();
  EXPECT_EQ(temporal.ValueOrDie().stream.rows.front().effective,
            (ValidTimeInterval{ValidTime{0}, ValidTime{10}}));
}

TEST(QueryRuntimeRelationalTest, PullCursorExecutesRelationalPlanNode) {
  char path[] = "/tmp/cedar_query_relational_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  PreparedQueryPlan plan;
  plan.relational_kind = LogicalOpKind::kLimit;
  plan.relational_input = RuntimeRelationalInput{
      .left = BatchStream{{Row(1, 10), Row(2, 20)}}, .offset = 1, .count = 1};
  const Slot<int64_t> key = Slot<int64_t>::WithId(SlotId{1}, "key");
  const Slot<int64_t> payload = Slot<int64_t>::WithId(SlotId{2}, "payload");
  plan.output_columns = {Project(key).column, Project(payload).column};

  auto cursor = QueryRuntime::Execute(
      plan, std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  EXPECT_EQ(batch.ValueOrDie()->row_count(), 1U);
  EXPECT_EQ(batch.ValueOrDie()->Get(key, 0), 2);
  EXPECT_EQ(batch.ValueOrDie()->Get(payload, 0), 20);

  EXPECT_TRUE(database.ValueOrDie()->Close().IsSnapshotPinned());
  EXPECT_TRUE(cursor.ValueOrDie().Close().ok());
  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace cedar::internal
