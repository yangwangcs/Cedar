#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "cedar/database.h"
#include "cedar/transaction.h"
#include "query/runtime/relational.h"
#include "query/runtime/query_runtime.h"
#include "query/runtime/vector_kernels.h"
#include "query/resource/query_scratch.h"

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

RelationalRow TemporalRowUnbounded(int64_t key, uint64_t from,
                                   int64_t payload = 0) {
  return {{Int64(key), Int64(payload)},
          ValidTimeInterval{ValidTime{from}, std::nullopt}};
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

TEST(RelationalTest, MalformedCellCountIsRejectedBeforeVectorAllocation) {
  // One framed row declares UINT32_MAX cells but contains no cell bytes. The
  // decoder must reject the frame based on remaining bytes rather than trying
  // to reserve the attacker-controlled count.
  std::string payload;
  const uint32_t frame_size = 5;
  payload.append(reinterpret_cast<const char*>(&frame_size), sizeof(frame_size));
  const uint32_t cell_count = std::numeric_limits<uint32_t>::max();
  payload.append(reinterpret_cast<const char*>(&cell_count), sizeof(cell_count));
  payload.push_back('\0');

  // Keep the budget below the current implementation's per-frame guard. A
  // valid decoder must reject the malformed count before attempting any
  // reservation or allocation, so this remains a corruption result.
  QueryReservation reservation(64);
  auto decoded = DeserializeRowsForTesting(payload, &reservation);
  ASSERT_FALSE(decoded.ok());
  EXPECT_TRUE(decoded.status().IsCorruption());
  EXPECT_EQ(reservation.used_bytes(), 0U);
}

TEST(RelationalTest, SpilledRowsReserveEveryDecodedCellCapacity) {
  // The frame is valid, but its cell vector is much larger than the row
  // header. Admission must account for the vector capacity before decoding.
  constexpr uint32_t kCells = 128;
  std::string frame;
  frame.append(reinterpret_cast<const char*>(&kCells), sizeof(kCells));
  frame.push_back('\0');
  for (uint32_t i = 0; i < kCells; ++i) {
    frame.push_back(static_cast<char>(QueryType::kInt64));
    frame.push_back('\1');
    frame.push_back(static_cast<char>(2));  // RelationalScalar int64 tag.
    const int64_t value = static_cast<int64_t>(i);
    frame.append(reinterpret_cast<const char*>(&value), sizeof(value));
  }
  std::string payload;
  const uint32_t frame_size = static_cast<uint32_t>(frame.size());
  payload.append(reinterpret_cast<const char*>(&frame_size), sizeof(frame_size));
  payload.append(frame);

  const size_t under_reserved = payload.size() + sizeof(RelationalRow) +
                                 sizeof(RelationalCell);
  QueryReservation reservation(under_reserved);
  auto decoded = DeserializeRowsForTesting(payload, &reservation);
  ASSERT_FALSE(decoded.ok());
  EXPECT_TRUE(IsNeedsSpill(decoded.status()));
  EXPECT_EQ(reservation.used_bytes(), 0U);
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

TEST(RelationalTest, ExternalSpillJoinsMatchCanonicalResults) {
  std::vector<RelationalRow> left_rows{Row(7, 700), Row(99, 990)};
  std::vector<RelationalRow> right_rows;
  for (int64_t key = 0; key < 512; ++key) {
    right_rows.push_back(Row(key, key * 10));
  }
  const auto root = std::filesystem::temp_directory_path() /
                    "cedar-task11-relational-spill";
  std::filesystem::remove_all(root);
  QueryReservation reservation(4096);
  QueryScratch scratch(root, "instance", "external", 1 << 20, &reservation);
  for (JoinKind kind : {JoinKind::kInner, JoinKind::kSemi, JoinKind::kAnti}) {
    JoinInput input{{left_rows}, {right_rows}, 0, 0, kind};
    auto expected = IndexNestedLoopJoin(input, &reservation);
    ASSERT_TRUE(expected.ok()) << expected.status().ToString();
    auto actual = HashJoin(input, &reservation,
                           std::numeric_limits<size_t>::max(), &scratch);
    ASSERT_TRUE(actual.ok()) << actual.status().ToString();
    EXPECT_EQ(actual.ValueOrDie().rows, expected.ValueOrDie().rows);
  }
  JoinInput sort_input{{left_rows}, {right_rows}, 0, 0, JoinKind::kInner};
  auto expected = IndexNestedLoopJoin(sort_input, &reservation);
  ASSERT_TRUE(expected.ok());
  auto actual = SortMergeJoin(sort_input, &reservation,
                              std::numeric_limits<size_t>::max(), &scratch);
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();
  EXPECT_EQ(actual.ValueOrDie().rows, expected.ValueOrDie().rows);
  for (JoinKind kind : {JoinKind::kSemi, JoinKind::kAnti}) {
    sort_input.kind = kind;
    auto sort_expected = IndexNestedLoopJoin(sort_input, &reservation);
    ASSERT_TRUE(sort_expected.ok()) << sort_expected.status().ToString();
    auto sort_actual = SortMergeJoin(sort_input, &reservation,
                                     std::numeric_limits<size_t>::max(),
                                     &scratch);
    ASSERT_TRUE(sort_actual.ok()) << sort_actual.status().ToString();
    EXPECT_EQ(sort_actual.ValueOrDie().rows, sort_expected.ValueOrDie().rows);
  }
  EXPECT_TRUE(std::filesystem::exists(scratch.query_directory()));
  EXPECT_FALSE(std::filesystem::is_empty(scratch.query_directory()));
  size_t verified_runs = 0;
  for (const auto& entry : std::filesystem::directory_iterator(
           scratch.query_directory())) {
    auto payload = scratch.ReadRun(entry.path());
    ASSERT_TRUE(payload.ok()) << payload.status().ToString();
    EXPECT_FALSE(payload.ValueOrDie().starts_with("row:"));
    ++verified_runs;
  }
  EXPECT_GT(verified_runs, 2U);
  EXPECT_TRUE(scratch.Cleanup().ok());
}

TEST(RelationalTest, ExternalSpillJoinRetainsDecodedReservationDuringUse) {
  std::vector<RelationalRow> left_rows{Row(7, 700), Row(99, 990)};
  std::vector<RelationalRow> right_rows;
  for (int64_t key = 0; key < 512; ++key) right_rows.push_back(Row(key, key * 10));
  JoinInput input{{left_rows}, {right_rows}, 0, 0, JoinKind::kInner};

  // Each matching spilled partition needs its decoded-cell lease while the
  // in-memory join reserves its index and output. These limits fit the spill
  // writes but reject the second partition if the decoded lease is retained.
  const auto root = std::filesystem::temp_directory_path() /
                    ("cedar-task11-relational-lease-" +
                     std::to_string(
                         std::chrono::steady_clock::now().time_since_epoch().count()));
  const auto hash_root = root.string() + "-hash";
  const auto sort_root = root.string() + "-sort";
  {
    QueryReservation hash_reservation(800);
    QueryScratch hash_scratch(hash_root, "instance", "hash", 1 << 20,
                              &hash_reservation);
    auto hashed = HashJoin(input, &hash_reservation,
                           std::numeric_limits<size_t>::max(), &hash_scratch);
    ASSERT_FALSE(hashed.ok()) << hashed.status().ToString();
    EXPECT_TRUE(IsNeedsSpill(hashed.status()));
    EXPECT_EQ(hash_reservation.used_bytes(), 0U);
  }
  {
    QueryReservation sort_reservation(1200);
    QueryScratch sort_scratch(sort_root, "instance", "sort", 1 << 20,
                              &sort_reservation);
    auto sorted = SortMergeJoin(input, &sort_reservation,
                                std::numeric_limits<size_t>::max(),
                                &sort_scratch);
    ASSERT_FALSE(sorted.ok()) << sorted.status().ToString();
    EXPECT_TRUE(IsNeedsSpill(sorted.status()));
    EXPECT_EQ(sort_reservation.used_bytes(), 0U);
  }
  std::filesystem::remove_all(hash_root);
  std::filesystem::remove_all(sort_root);
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

TEST(RelationalTest, TemporalOperatorsRejectZeroOutputRowsBeforeReservation) {
  TemporalJoinInput join_input{{{TemporalRow(1, 0, 10)}},
                               {{TemporalRow(1, 0, 10)}}, 0, 0,
                               JoinKind::kInner};
  FragmentBudget join_fragments(8);
  QueryReservation join_reservation(1 << 20);
  auto join = IntervalMergeJoin(join_input, &join_fragments,
                                &join_reservation, 0);
  ASSERT_FALSE(join.ok());
  EXPECT_TRUE(join.status().IsResourceExhausted());
  EXPECT_EQ(join_reservation.peak_bytes(), 0U);

  TemporalAggregateInput aggregate_input{
      BatchStream{{TemporalRow(1, 0, 10)}}, {0}};
  FragmentBudget aggregate_fragments(8);
  QueryReservation aggregate_reservation(1 << 20);
  auto aggregate = TemporalAggregate(aggregate_input, &aggregate_fragments,
                                     &aggregate_reservation, 0);
  ASSERT_FALSE(aggregate.ok());
  EXPECT_TRUE(aggregate.status().IsResourceExhausted());
  EXPECT_EQ(aggregate_reservation.peak_bytes(), 0U);
}

TEST(RelationalTest, TemporalJoinPreflightsExactOutputRowsBeforeReservation) {
  TemporalJoinInput input{
      {{TemporalRow(1, 0, 10), TemporalRow(1, 15, 30)}},
      {{TemporalRow(1, 5, 20)}}, 0, 0, JoinKind::kInner};
  FragmentBudget fragments(8);
  QueryReservation reservation(1 << 20);
  auto result = IntervalMergeJoin(input, &fragments, &reservation, 1);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsResourceExhausted());
  EXPECT_EQ(reservation.peak_bytes(), 0U);
}

TEST(RelationalTest, TemporalAntiJoinAcceptsFullyCoveredLeftInterval) {
  TemporalJoinInput input{{{TemporalRow(1, 0, 10)}},
                          {{TemporalRow(1, 0, 10)}}, 0, 0, JoinKind::kAnti};
  FragmentBudget fragments(0);
  QueryReservation reservation(1 << 20);
  auto result = IntervalMergeJoin(input, &fragments, &reservation);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_TRUE(result.ValueOrDie().rows.empty());
  EXPECT_EQ(reservation.peak_bytes(), 0U);
}

TEST(RelationalTest, TemporalSemiJoinPreflightsCoalescedOutputRows) {
  TemporalJoinInput input{{{TemporalRow(1, 0, 10)}},
                          {{TemporalRow(1, 0, 7), TemporalRow(1, 5, 10)}},
                          0, 0, JoinKind::kSemi};
  FragmentBudget fragments(1);
  QueryReservation reservation(1 << 20);
  auto result = IntervalMergeJoin(input, &fragments, &reservation, 1);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().rows.size(), 1U);
  EXPECT_EQ(result.ValueOrDie().rows.front().effective,
            (ValidTimeInterval{ValidTime{0}, ValidTime{10}}));
}

TEST(RelationalTest, TemporalCoveragePreflightHandlesUnboundedIntervals) {
  TemporalJoinInput semi_input{
      {{TemporalRow(1, 0, 20)}},
      {{TemporalRowUnbounded(1, 5)}}, 0, 0, JoinKind::kSemi};
  FragmentBudget semi_fragments(1);
  QueryReservation semi_reservation(1 << 20);
  auto semi = IntervalMergeJoin(semi_input, &semi_fragments,
                                &semi_reservation, 1);
  ASSERT_TRUE(semi.ok()) << semi.status().ToString();
  ASSERT_EQ(semi.ValueOrDie().rows.size(), 1U);
  EXPECT_EQ(semi.ValueOrDie().rows.front().effective,
            (ValidTimeInterval{ValidTime{5}, ValidTime{20}}));

  TemporalJoinInput anti_input{
      {{TemporalRowUnbounded(1, 0)}},
      {{TemporalRowUnbounded(1, 5)}}, 0, 0, JoinKind::kAnti};
  FragmentBudget anti_fragments(1);
  QueryReservation anti_reservation(1 << 20);
  auto anti = IntervalMergeJoin(anti_input, &anti_fragments,
                                &anti_reservation, 1);
  ASSERT_TRUE(anti.ok()) << anti.status().ToString();
  ASSERT_EQ(anti.ValueOrDie().rows.size(), 1U);
  EXPECT_EQ(anti.ValueOrDie().rows.front().effective,
            (ValidTimeInterval{ValidTime{0}, ValidTime{5}}));
}

TEST(RelationalTest, TemporalJoinPreflightAccountsForCrossLeftCoalescing) {
  TemporalJoinInput input{
      {{TemporalRow(1, 0, 5), TemporalRow(1, 5, 10)}},
      {{TemporalRow(1, 0, 10)}}, 0, 0, JoinKind::kSemi};
  FragmentBudget fragments(1);
  QueryReservation reservation(1 << 20);
  auto result = IntervalMergeJoin(input, &fragments, &reservation, 1);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().rows.size(), 1U);
  EXPECT_EQ(result.ValueOrDie().rows.front().effective,
            (ValidTimeInterval{ValidTime{0}, ValidTime{10}}));
}

TEST(RelationalTest,
     TemporalAggregatePreflightsExactOutputRowsBeforeReservation) {
  TemporalAggregateInput input{
      BatchStream{{TemporalRow(1, 0, 5), TemporalRow(1, 10, 15)}}, {0}};
  FragmentBudget fragments(8);
  QueryReservation reservation(1 << 20);
  auto result = TemporalAggregate(input, &fragments, &reservation, 1);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsResourceExhausted());
  EXPECT_EQ(reservation.peak_bytes(), 0U);
}

TEST(RelationalTest, TemporalAggregatePreflightsPublishedFragments) {
  TemporalAggregateInput input{
      BatchStream{{TemporalRow(1, 0, 5), TemporalRow(1, 5, 10)}}, {0}};
  FragmentBudget fragments(1);
  QueryReservation reservation(1 << 20);
  auto result = TemporalAggregate(input, &fragments, &reservation, 1);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().rows.size(), 1U);
  EXPECT_EQ(result.ValueOrDie().rows.front().effective,
            (ValidTimeInterval{ValidTime{0}, ValidTime{10}}));
}

TEST(RelationalTest, TemporalAggregateMemoryFailuresDiagnoseAllDimensions) {
  TemporalAggregateInput input{
      BatchStream{{TemporalRow(1, 0, 10)}}, {0}};
  FragmentBudget fragments(8);
  QueryReservation reservation(1);
  auto result = TemporalAggregate(input, &fragments, &reservation);
  ASSERT_FALSE(result.ok());
  const std::string diagnostic = result.status().ToString();
  EXPECT_NE(diagnostic.find("memory_bytes="), std::string::npos);
  EXPECT_NE(diagnostic.find("output_rows="), std::string::npos);
  EXPECT_NE(diagnostic.find("output_bytes="), std::string::npos);
  EXPECT_NE(diagnostic.find("interval_fragments="), std::string::npos);
}

TEST(RelationalTest, TemporalBudgetFailuresDiagnoseAllDimensions) {
  TemporalJoinInput input{{{TemporalRow(1, 0, 10)}},
                          {{TemporalRow(1, 0, 10)}}, 0, 0,
                          JoinKind::kInner};
  FragmentBudget fragments(0);
  QueryReservation reservation(1 << 20);
  auto result = IntervalMergeJoin(input, &fragments, &reservation);
  ASSERT_FALSE(result.ok());
  const std::string diagnostic = result.status().ToString();
  EXPECT_NE(diagnostic.find("memory_bytes=", 0), std::string::npos);
  EXPECT_NE(diagnostic.find("output_rows=", 0), std::string::npos);
  EXPECT_NE(diagnostic.find("output_bytes=", 0), std::string::npos);
  EXPECT_NE(diagnostic.find("interval_fragments=", 0), std::string::npos);
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

TEST(QueryRuntimeRelationalTest,
     CopiedPlanRetainsEffectiveOutputSlotForIntervalMaterialization) {
  PreparedQueryPlan original;
  original.output_columns = {
      {SlotId{1}, "key", QueryType::kInt64, false},
      {SlotId{2}, "count", QueryType::kInt64, false},
      {SlotId{3}, "effective", QueryType::kValidTimeInterval, false}};
  original.effective_output_slot = SlotId{3};
  original.delta_reader = [] { return StatusOr<QueryDeltaView>(QueryDeltaView{}); };
  original.bound_delta_view =
      std::make_shared<const QueryDeltaView>(QueryDeltaView{});
  PreparedQueryPlan copied = original;
  EXPECT_EQ(copied.effective_output_slot, std::optional<SlotId>{SlotId{3}});
  PreparedQueryPlan assigned;
  assigned = original;
  EXPECT_EQ(assigned.effective_output_slot,
            std::optional<SlotId>{SlotId{3}});
  EXPECT_TRUE(assigned.delta_reader);
  ASSERT_TRUE(assigned.bound_delta_view);
}

TEST(QueryRuntimeRelationalTest,
     PreparedPlanDeltaMergeMatchesCanonicalAcrossProjectionChains) {
  char pattern[] = "/tmp/cedar_query_runtime_fixture_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  DatabaseOptions database_options;
  database_options.path = path;
  database_options.query_runtime.query_memory_bytes = 32ULL << 20;
  database_options.query_runtime.projection_cache_bytes = 32ULL << 20;
  database_options.query_runtime.query_delta_bytes = 32ULL << 20;
  auto database = Database::Open(std::move(database_options));
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto property = database.ValueOrDie()->RegisterProperty(PropertyDefinition{
      PropertyId{7}, 0, "score", PropertyEntityKind::kVertex,
      PhysicalType::kInt64, 4096});
  ASSERT_TRUE(property.ok()) << property.status().ToString();

  auto first = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  const VertexRef vertex{PartId{0}, VertexId{1}};
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(vertex),
                                         ValidTime{0}).ok());
  ASSERT_TRUE(first.ValueOrDie()
                  ->Set(PropertyFact::Vertex(vertex, PropertyId{7}),
                        ValidTime{0}, Value::Int64(1))
                  .ok());
  ASSERT_TRUE(first.ValueOrDie()
                  ->Set(PropertyFact::Vertex(vertex, PropertyId{7}),
                        ValidTime{10}, Value::Int64(2))
                  .ok());
  const auto first_commit = first.ValueOrDie()->Commit();
  ASSERT_TRUE(first_commit.ok()) << first_commit.status().ToString();

  auto second = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  ASSERT_TRUE(second.ValueOrDie()
                  ->Set(PropertyFact::Vertex(vertex, PropertyId{7}),
                        ValidTime{5}, Value::Int64(3))
                  .ok());
  const auto second_commit = second.ValueOrDie()->Commit();
  ASSERT_TRUE(second_commit.ok()) << second_commit.status().ToString();

  auto third = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(third.ok()) << third.status().ToString();
  ASSERT_TRUE(third.ValueOrDie()
                  ->Set(PropertyFact::Vertex(vertex, PropertyId{7}),
                        ValidTime{15}, Value::Int64(4))
                  .ok());
  const auto third_commit = third.ValueOrDie()->Commit();
  ASSERT_TRUE(third_commit.ok()) << third_commit.status().ToString();

  const Slot<VertexRef> vertex_slot = Slot<VertexRef>::Named("v");
  const OptionalSlot<int64_t> score_slot = OptionalSlot<int64_t>::Named("score");
  auto source = Query::Vertices(
      vertex_slot, History{ValidTimeInterval{ValidTime{0}, ValidTime{20}}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();
  auto bound = source.ValueOrDie().BindVertexProperty(
      vertex_slot, PropertyId{7}, score_slot);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  auto query = bound.ValueOrDie().Select(
      {Project(vertex_slot), Project(score_slot)});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto analyzed = AnalyzeQuery(query.ValueOrDie());
  ASSERT_TRUE(analyzed.ok()) << analyzed.status().ToString();
  PreparedQueryPlan canonical_plan = analyzed.ValueOrDie();
  ASSERT_EQ(canonical_plan.property_bindings.size(), 1U);
  canonical_plan.property_bindings.front().definition = property.ValueOrDie();

  const auto base_seq = first_commit.ValueOrDie().commit_seq;
  const auto snapshot_seq = third_commit.ValueOrDie().commit_seq;
  QueryDeltaView delta{
      base_seq,
      snapshot_seq,
      {FactEvent{FactRef{PartId{0}, FactFamily::kVertexProperty, PropertyId{7},
                           1},
                  ValidTime{5}, second_commit.ValueOrDie().commit_seq,
                  FactOperation::kPut, 0, Value::Int64(3), std::nullopt},
       FactEvent{FactRef{PartId{0}, FactFamily::kVertexProperty, PropertyId{7},
                           1},
                  ValidTime{15}, third_commit.ValueOrDie().commit_seq,
                  FactOperation::kPut, 0, Value::Int64(4), std::nullopt}},
      {},
      {},
      {}};
  canonical_plan.bound_delta_view =
      std::make_shared<const QueryDeltaView>(delta);

  ProjectionChain first_chain;
  first_chain.header.kind = ProjectionKind::kState;
  first_chain.header.base_seq = base_seq;
  first_chain.header.part_id = PartId{0};
  first_chain.header.property_id = PropertyId{7};
  first_chain.intervals = {
      {ValidTimeInterval{ValidTime{0}, ValidTime{15}}, Value::Int64(1), 1}};
  ProjectionChain second_chain = first_chain;
  second_chain.intervals = {
      {ValidTimeInterval{ValidTime{10}, ValidTime{20}}, Value::Int64(2), 1}};

  auto delta_plan = canonical_plan;
  PhysicalPlan physical;
  physical.coverage_slices.push_back(CoverageSlice{
      CoverageSource::kDeltaMerge,
      ValidTimeInterval{ValidTime{0}, ValidTime{20}},
      std::nullopt,
      base_seq,
      ProjectionKind::kState,
      PartId{0},
      PropertyId{7},
      0,
      0,
      UINT64_MAX,
      {}});
  delta_plan.physical_plan = std::make_shared<const PhysicalPlan>(physical);
  delta_plan.projection_reader = [first_chain, second_chain](
                                    const CoverageSlice&) {
    return StatusOr<std::vector<ProjectionChain>>(
        std::vector<ProjectionChain>{first_chain, second_chain});
  };

  auto run = [&](PreparedQueryPlan plan) {
    auto snapshot = database.ValueOrDie()->BeginSnapshot(
        SnapshotOptions{snapshot_seq});
    EXPECT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    return QueryRuntime::Execute(plan,
                                 std::move(snapshot).ConsumeValueOrDie(),
                                 Bindings{}, QueryOptions{});
  };
  auto canonical_cursor = run(canonical_plan);
  ASSERT_TRUE(canonical_cursor.ok()) << canonical_cursor.status().ToString();
  auto canonical_batch = canonical_cursor.ValueOrDie().Next();
  ASSERT_TRUE(canonical_batch.ok()) << canonical_batch.status().ToString();
  ASSERT_TRUE(canonical_batch.ValueOrDie().has_value());
  auto delta_cursor = run(delta_plan);
  ASSERT_TRUE(delta_cursor.ok()) << delta_cursor.status().ToString();
  auto delta_batch = delta_cursor.ValueOrDie().Next();
  ASSERT_TRUE(delta_batch.ok()) << delta_batch.status().ToString();
  ASSERT_TRUE(delta_batch.ValueOrDie().has_value());

  const auto& canonical = canonical_batch.ValueOrDie().value();
  const auto& merged = delta_batch.ValueOrDie().value();
  ASSERT_EQ(canonical.row_count(), 4U);
  ASSERT_EQ(merged.row_count(), canonical.row_count());
  for (size_t index = 0; index < merged.row_count(); ++index) {
    EXPECT_EQ(merged.Get<int64_t>(score_slot, index),
              canonical.Get<int64_t>(score_slot, index));
  }
  EXPECT_EQ(merged.Get<int64_t>(score_slot, 0), 1);
  EXPECT_EQ(merged.Get<int64_t>(score_slot, 1), 3);
  EXPECT_EQ(merged.Get<int64_t>(score_slot, 2), 2);
  EXPECT_EQ(merged.Get<int64_t>(score_slot, 3), 4);

  auto run_at = [&](uint64_t time, bool merged_source) {
    PreparedQueryPlan point_plan = merged_source ? delta_plan : canonical_plan;
    point_plan.scope = At{ValidTime{time}};
    if (merged_source) {
      auto point_physical = *point_plan.physical_plan;
      point_physical.coverage_slices.front().interval =
          ValidTimeInterval{ValidTime{time}, ValidTime{time + 1}};
      point_plan.physical_plan =
          std::make_shared<const PhysicalPlan>(std::move(point_physical));
    }
    auto cursor = run(std::move(point_plan));
    EXPECT_TRUE(cursor.ok()) << cursor.status().ToString();
    auto batch = cursor.ValueOrDie().Next();
    EXPECT_TRUE(batch.ok()) << batch.status().ToString();
    EXPECT_TRUE(batch.ValueOrDie().has_value());
    EXPECT_EQ(batch.ValueOrDie()->row_count(), 1U);
    return batch.ValueOrDie()->Get<int64_t>(score_slot, 0);
  };
  for (const auto& expected : std::vector<std::pair<uint64_t, int64_t>>{
           {4, 1}, {5, 3}, {10, 2}, {15, 4}}) {
    EXPECT_EQ(run_at(expected.first, true), expected.second);
    EXPECT_EQ(run_at(expected.first, false), expected.second);
  }

  PreparedQueryPlan gap_plan = canonical_plan;
  gap_plan.scope = History{ValidTimeInterval{ValidTime{0}, ValidTime{30}}};
  PhysicalPlan gap_physical;
  gap_physical.coverage_slices = {
      CoverageSlice{CoverageSource::kCanonical,
                    ValidTimeInterval{ValidTime{0}, ValidTime{10}}},
      CoverageSlice{CoverageSource::kCanonical,
                    ValidTimeInterval{ValidTime{20}, ValidTime{30}}}};
  gap_plan.physical_plan =
      std::make_shared<const PhysicalPlan>(std::move(gap_physical));
  gap_plan.projection_reader = [](const CoverageSlice&) {
    return StatusOr<std::vector<ProjectionChain>>(
        Status::NotFound("query test", "canonical-only slice"));
  };
  auto gap_cursor = run(std::move(gap_plan));
  ASSERT_TRUE(gap_cursor.ok()) << gap_cursor.status().ToString();
  auto gap_batch = gap_cursor.ValueOrDie().Next();
  ASSERT_TRUE(gap_batch.ok()) << gap_batch.status().ToString();
  ASSERT_TRUE(gap_batch.ValueOrDie().has_value());
  ASSERT_EQ(gap_batch.ValueOrDie()->row_count(), 3U);
  EXPECT_EQ(gap_batch.ValueOrDie()->Get<int64_t>(score_slot, 0), 1);
  EXPECT_EQ(gap_batch.ValueOrDie()->Get<int64_t>(score_slot, 1), 3);
  EXPECT_EQ(gap_batch.ValueOrDie()->Get<int64_t>(score_slot, 2), 4);
  EXPECT_FALSE(gap_cursor.ValueOrDie().Next().ValueOrDie().has_value());
  EXPECT_FALSE(canonical_cursor.ValueOrDie().Next().ValueOrDie().has_value());
  EXPECT_FALSE(delta_cursor.ValueOrDie().Next().ValueOrDie().has_value());
  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace cedar::internal
