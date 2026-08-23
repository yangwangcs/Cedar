#include <gtest/gtest.h>

#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "cedar/database.h"
#include "cedar/query.h"
#include "cedar/transaction.h"
#include "query/runtime/canonical_source.h"
#include "query/runtime/property_binding.h"
#include "query/runtime/temporal_source.h"

namespace cedar {
namespace {

static_assert(std::is_copy_constructible_v<PreparedQuery>);
static_assert(std::is_copy_assignable_v<PreparedQuery>);
static_assert(!std::is_move_assignable_v<const PreparedQuery>);
static_assert(!std::is_copy_constructible_v<QueryCursor>);
static_assert(!std::is_copy_assignable_v<QueryCursor>);
static_assert(std::is_move_constructible_v<QueryCursor>);
static_assert(std::is_move_assignable_v<QueryCursor>);

struct ExpectedEvent {
  uint64_t valid_from;
  FactOperation operation;
  uint64_t commit_seq;

  bool operator==(const ExpectedEvent&) const = default;
};

struct ExpectedChange {
  uint64_t valid_from;
  std::optional<int64_t> before;
  std::optional<int64_t> after;

  bool operator==(const ExpectedChange&) const = default;
};

struct ExpectedState {
  uint64_t from;
  std::optional<uint64_t> to;
  std::optional<int64_t> value;

  bool operator==(const ExpectedState&) const = default;
};

std::optional<int64_t> Int64Of(const std::optional<Value>& value) {
  if (!value.has_value()) return std::nullopt;
  return std::get<int64_t>(value->data());
}

std::vector<ExpectedEvent> ObserveEvents(
    const std::vector<internal::EventRow>& rows) {
  std::vector<ExpectedEvent> observed;
  observed.reserve(rows.size());
  for (const auto& row : rows) {
    observed.push_back(
        {row.valid_from.value, row.operation, row.commit_seq.value});
  }
  return observed;
}

std::vector<ExpectedChange> ObserveChanges(
    const std::vector<internal::ChangeRow>& rows) {
  std::vector<ExpectedChange> observed;
  observed.reserve(rows.size());
  for (const auto& row : rows) {
    observed.push_back(
        {row.valid_from.value, Int64Of(row.before), Int64Of(row.after)});
  }
  return observed;
}

std::vector<ExpectedState> ObserveStates(
    const std::vector<internal::StateRow>& rows) {
  std::vector<ExpectedState> observed;
  observed.reserve(rows.size());
  for (const auto& row : rows) {
    observed.push_back({row.effective.from.value,
                        row.effective.to.has_value()
                            ? std::optional<uint64_t>{row.effective.to->value}
                            : std::nullopt,
                        Int64Of(row.value)});
  }
  return observed;
}

std::vector<ExpectedState> ObserveBindings(
    const std::vector<internal::BoundPropertyRow>& rows) {
  std::vector<ExpectedState> observed;
  observed.reserve(rows.size());
  for (const auto& row : rows) {
    observed.push_back({row.effective.from.value,
                        row.effective.to.has_value()
                            ? std::optional<uint64_t>{row.effective.to->value}
                            : std::nullopt,
                        Int64Of(row.value)});
  }
  return observed;
}

class QueryCanonicalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_query_canonical_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
    auto database = Database::Open(DatabaseOptions{.path = path_});
    ASSERT_TRUE(database.ok()) << database.status().ToString();
    database_ = std::move(database).ConsumeValueOrDie();
  }

  void TearDown() override {
    if (database_) database_->Close().IgnoreError();
    database_.reset();
    std::filesystem::remove_all(path_);
    for (const std::string& other : other_paths_) {
      std::filesystem::remove_all(other);
    }
  }

  CommitSeq AssertVertex(uint64_t vertex_id, uint64_t valid_from) {
    auto transaction = database_->BeginTransaction();
    EXPECT_TRUE(transaction.ok()) << transaction.status().ToString();
    EXPECT_TRUE(transaction.ValueOrDie()
                    ->Assert(EntityFact::Vertex(
                                 VertexRef{PartId{0}, VertexId{vertex_id}}),
                             ValidTime{valid_from})
                    .ok());
    auto committed = transaction.ValueOrDie()->Commit();
    EXPECT_TRUE(committed.ok()) << committed.status().ToString();
    return committed.ValueOrDie().commit_seq;
  }

  CommitSeq RetractVertex(uint64_t vertex_id, uint64_t valid_from) {
    auto transaction = database_->BeginTransaction();
    EXPECT_TRUE(transaction.ok()) << transaction.status().ToString();
    EXPECT_TRUE(transaction.ValueOrDie()
                    ->Retract(EntityFact::Vertex(
                                  VertexRef{PartId{0}, VertexId{vertex_id}}),
                              ValidTime{valid_from})
                    .ok());
    auto committed = transaction.ValueOrDie()->Commit();
    EXPECT_TRUE(committed.ok()) << committed.status().ToString();
    return committed.ValueOrDie().commit_seq;
  }

  CommitSeq CommitPropertyMutation(ValidTime valid_from,
                                   std::optional<int64_t> value) {
    auto transaction = database_->BeginTransaction();
    EXPECT_TRUE(transaction.ok()) << transaction.status().ToString();
    const PropertyFact score = PropertyFact::Vertex(
        VertexRef{PartId{0}, VertexId{1}}, PropertyId{7});
    const Status staged = value.has_value()
                              ? transaction.ValueOrDie()->Set(
                                    score, valid_from, Value::Int64(*value))
                              : transaction.ValueOrDie()->Unset(score, valid_from);
    EXPECT_TRUE(staged.ok()) << staged.ToString();
    auto committed = transaction.ValueOrDie()->Commit();
    EXPECT_TRUE(committed.ok()) << committed.status().ToString();
    return committed.ValueOrDie().commit_seq;
  }

  std::vector<CommitSeq> SeedPropertyHistory() {
    const auto property = database_->RegisterProperty(PropertyDefinition{
        PropertyId{7}, 0, "score", PropertyEntityKind::kVertex,
        PhysicalType::kInt64, 4096});
    EXPECT_TRUE(property.ok()) << property.status().ToString();
    if (property.ok()) score_definition_ = property.ValueOrDie();

    auto first = database_->BeginTransaction();
    EXPECT_TRUE(first.ok()) << first.status().ToString();
    EXPECT_TRUE(first.ValueOrDie()
                    ->Assert(EntityFact::Vertex(
                                 VertexRef{PartId{0}, VertexId{1}}),
                             ValidTime{0})
                    .ok());
    EXPECT_TRUE(first.ValueOrDie()
                    ->Set(PropertyFact::Vertex(
                              VertexRef{PartId{0}, VertexId{1}}, PropertyId{7}),
                          ValidTime{10}, Value::Int64(7))
                    .ok());
    auto committed = first.ValueOrDie()->Commit();
    EXPECT_TRUE(committed.ok()) << committed.status().ToString();

    return {committed.ValueOrDie().commit_seq,
            CommitPropertyMutation(ValidTime{30}, 7),
            CommitPropertyMutation(ValidTime{20}, std::nullopt)};
  }

  StatusOr<size_t> CountRows(const Query& query, QueryOptions options = {}) {
    auto prepared = database_->PrepareQuery(query);
    if (!prepared.ok()) return prepared.status();
    auto snapshot = database_->BeginSnapshot();
    if (!snapshot.ok()) return snapshot.status();
    auto cursor = prepared.ValueOrDie().Execute(
        std::move(snapshot).ConsumeValueOrDie(), Bindings{}, options);
    if (!cursor.ok()) return cursor.status();
    size_t rows = 0;
    while (true) {
      auto batch = cursor.ValueOrDie().Next();
      if (!batch.ok()) return batch.status();
      if (!batch.ValueOrDie().has_value()) break;
      rows += batch.ValueOrDie()->row_count();
    }
    return rows;
  }

  void SeedVertexHistory() {
    EXPECT_EQ(AssertVertex(1, 10), CommitSeq{1});
    EXPECT_EQ(RetractVertex(1, 10), CommitSeq{2});
  }

  StatusOr<Query> VertexQuery(const Slot<VertexRef>& vertex,
                              ValidTime at = ValidTime{15}) {
    auto source = Query::Vertices(vertex, At{at});
    if (!source.ok()) return source.status();
    return source.ValueOrDie().Select({Project(vertex)});
  }

  std::unique_ptr<Database> OpenOtherDatabase() {
    char pattern[] = "/tmp/cedar_query_canonical_other_XXXXXX";
    EXPECT_NE(mkdtemp(pattern), nullptr);
    other_paths_.emplace_back(pattern);
    auto database = Database::Open(DatabaseOptions{.path = other_paths_.back()});
    EXPECT_TRUE(database.ok()) << database.status().ToString();
    return std::move(database).ConsumeValueOrDie();
  }

  std::string path_;
  std::vector<std::string> other_paths_;
  std::unique_ptr<Database> database_;
  std::optional<PropertyDefinition> score_definition_;
};

TEST_F(QueryCanonicalTest, StreamsStateAtFromTheConsumedSnapshot) {
  SeedVertexHistory();
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto query = VertexQuery(vertex);
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database_->BeginSnapshot({.as_of = CommitSeq{1}});
  ASSERT_TRUE(snapshot.ok());
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto first = cursor.ValueOrDie().Next();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(first.ValueOrDie().has_value());
  EXPECT_EQ(first.ValueOrDie()->row_count(), 1U);
  EXPECT_EQ(first.ValueOrDie()->Get<VertexRef>(vertex, 0),
            (VertexRef{PartId{0}, VertexId{1}}));
  EXPECT_FALSE(cursor.ValueOrDie().Next().ValueOrDie().has_value());
  EXPECT_FALSE(cursor.ValueOrDie().Next().ValueOrDie().has_value());
}

TEST_F(QueryCanonicalTest, PinsSnapshotUntilEndOfStreamOrExplicitClose) {
  ASSERT_EQ(AssertVertex(1, 10), CommitSeq{1});
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto query = VertexQuery(vertex);
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();

  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  // Task 15 orders shutdown by cancelling and joining active queries before
  // closing the authoritative store. The cursor remains a valid object, but
  // cannot perform further reads after the database has closed.
  EXPECT_TRUE(database_->Close().ok());
  auto cancelled = cursor.ValueOrDie().Next();
  ASSERT_FALSE(cancelled.ok());
  EXPECT_TRUE(cancelled.status().IsQueryCancelled());
  EXPECT_EQ(cursor.ValueOrDie().terminal_info().state,
            QueryCursorState::kCancelled);
  EXPECT_FALSE(cursor.ValueOrDie().terminal_info().complete);
  EXPECT_TRUE(database_->Close().ok());

  auto other = OpenOtherDatabase();
  auto other_snapshot = other->BeginSnapshot();
  ASSERT_TRUE(other_snapshot.ok()) << other_snapshot.status().ToString();
  auto other_prepared = other->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(other_prepared.ok()) << other_prepared.status().ToString();
  auto other_cursor = other_prepared.ValueOrDie().Execute(
      std::move(other_snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(other_cursor.ok()) << other_cursor.status().ToString();
  EXPECT_TRUE(other_cursor.ValueOrDie().Close().ok());
  EXPECT_TRUE(other->Close().ok());
}

TEST_F(QueryCanonicalTest, RejectsCallsOnMovedFromCursor) {
  ASSERT_EQ(AssertVertex(1, 10), CommitSeq{1});
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto query = VertexQuery(vertex);
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  auto created = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(created.ok()) << created.status().ToString();
  QueryCursor cursor = std::move(created).ConsumeValueOrDie();
  QueryCursor moved = std::move(cursor);
  EXPECT_TRUE(cursor.Next().status().IsInvalidArgument());
  EXPECT_TRUE(cursor.Close().IsInvalidArgument());
  EXPECT_TRUE(moved.Close().ok());
}

TEST_F(QueryCanonicalTest, ReturnedBatchOutlivesCursor) {
  ASSERT_EQ(AssertVertex(1, 10), CommitSeq{1});
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto query = VertexQuery(vertex);
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  auto created = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(created.ok()) << created.status().ToString();

  std::optional<QueryCursor> cursor(
      std::move(created).ConsumeValueOrDie());
  auto first = cursor->Next();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(first.ValueOrDie().has_value());
  QueryBatch batch = std::move(*first.ValueOrDie());
  cursor.reset();
  EXPECT_EQ(batch.Get<VertexRef>(vertex, 0),
            (VertexRef{PartId{0}, VertexId{1}}));
}

TEST_F(QueryCanonicalTest, PreparedQueryRejectsExecuteAfterDatabaseClose) {
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto query = VertexQuery(vertex);
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto other = OpenOtherDatabase();
  auto snapshot = other->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  ASSERT_TRUE(database_->Close().ok());

  const auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  EXPECT_TRUE(cursor.status().IsShutdownInProgress())
      << cursor.status().ToString();
}

TEST_F(QueryCanonicalTest, ConcurrentExecutionsDoNotShareCursorState) {
  ASSERT_EQ(AssertVertex(1, 10), CommitSeq{1});
  ASSERT_EQ(AssertVertex(2, 10), CommitSeq{2});
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto query = VertexQuery(vertex);
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto first_snapshot = database_->BeginSnapshot({.as_of = CommitSeq{1}});
  auto second_snapshot = database_->BeginSnapshot({.as_of = CommitSeq{2}});
  ASSERT_TRUE(first_snapshot.ok()) << first_snapshot.status().ToString();
  ASSERT_TRUE(second_snapshot.ok()) << second_snapshot.status().ToString();
  auto first_cursor = prepared.ValueOrDie().Execute(
      std::move(first_snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  auto second_cursor = prepared.ValueOrDie().Execute(
      std::move(second_snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(first_cursor.ok()) << first_cursor.status().ToString();
  ASSERT_TRUE(second_cursor.ok()) << second_cursor.status().ToString();

  auto first_future = std::async(
      std::launch::async,
      [cursor = std::move(first_cursor).ConsumeValueOrDie()]() mutable {
        return cursor.Next();
      });
  auto second_future = std::async(
      std::launch::async,
      [cursor = std::move(second_cursor).ConsumeValueOrDie()]() mutable {
        return cursor.Next();
      });
  auto first = first_future.get();
  auto second = second_future.get();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  ASSERT_TRUE(first.ValueOrDie().has_value());
  ASSERT_TRUE(second.ValueOrDie().has_value());
  EXPECT_EQ(first.ValueOrDie()->row_count(), 1U);
  EXPECT_EQ(second.ValueOrDie()->row_count(), 2U);
}

TEST_F(QueryCanonicalTest, RejectsSnapshotFromAnotherDatabase) {
  ASSERT_EQ(AssertVertex(1, 10), CommitSeq{1});
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto query = VertexQuery(vertex);
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();

  auto other = OpenOtherDatabase();
  auto transaction = other->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}),
                           ValidTime{10})
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());
  auto snapshot = other->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  const auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  EXPECT_TRUE(cursor.status().IsInvalidArgument()) << cursor.status().ToString();
}

TEST_F(QueryCanonicalTest, IgnoresUnrelatedSchemaChanges) {
  ASSERT_EQ(AssertVertex(1, 10), CommitSeq{1});
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto query = VertexQuery(vertex);
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  ASSERT_TRUE(database_->RegisterProperty(PropertyDefinition{
      PropertyId{9}, 0, "unrelated", PropertyEntityKind::kVertex,
      PhysicalType::kString, 4096}).ok());
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  EXPECT_TRUE(cursor.ValueOrDie().Next().ValueOrDie().has_value());
}

TEST_F(QueryCanonicalTest, RejectsChangedReferencedPropertySchema) {
  const auto property = database_->RegisterProperty(PropertyDefinition{
      PropertyId{7}, 0, "name", PropertyEntityKind::kVertex,
      PhysicalType::kString, 4096});
  ASSERT_TRUE(property.ok()) << property.status().ToString();
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  OptionalSlot<std::string> value = OptionalSlot<std::string>::Named("name");
  auto source = Query::Vertices(vertex, At{ValidTime{15}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();
  auto bound = source.ValueOrDie().BindVertexProperty(
      vertex, PropertyId{7}, value);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  auto query = bound.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  ASSERT_TRUE(database_->RegisterProperty(PropertyDefinition{
      PropertyId{7}, 0, "age", PropertyEntityKind::kVertex,
      PhysicalType::kInt64, 4096}).ok());
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  const auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  EXPECT_TRUE(cursor.status().IsSchemaMismatch())
      << cursor.status().ToString();
}

TEST_F(QueryCanonicalTest, RejectsSnapshotBelowDurableReadableBoundary) {
  ASSERT_EQ(AssertVertex(1, 10), CommitSeq{1});
  ASSERT_EQ(AssertVertex(2, 10), CommitSeq{2});
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto query = VertexQuery(vertex);
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  ASSERT_TRUE(database_->Vacuum(CommitSeq{1}).ok());
  auto other = OpenOtherDatabase();
  auto old_snapshot = other->BeginSnapshot({.as_of = CommitSeq{0}});
  ASSERT_TRUE(old_snapshot.ok()) << old_snapshot.status().ToString();

  const auto cursor = prepared.ValueOrDie().Execute(
      std::move(old_snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  EXPECT_TRUE(cursor.status().IsSnapshotExpired())
      << cursor.status().ToString();
}

TEST_F(QueryCanonicalTest, RepeatsTerminalExecutionError) {
  ASSERT_EQ(AssertVertex(1, 10), CommitSeq{1});
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto query = VertexQuery(vertex);
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  QueryOptions options;
  options.budget.output_rows = 0;
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, options);
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();

  const auto first = cursor.ValueOrDie().Next();
  const auto second = cursor.ValueOrDie().Next();
  ASSERT_FALSE(first.ok());
  ASSERT_FALSE(second.ok());
  EXPECT_TRUE(first.status().IsResourceExhausted());
  EXPECT_EQ(second.status().ToString(), first.status().ToString());
}

TEST_F(QueryCanonicalTest, ReadsCorrectedTemporalSourcesAgainstIndependentRows) {
  const std::vector<CommitSeq> commits = SeedPropertyHistory();
  ASSERT_EQ(commits.size(), 3U);

  auto before_correction =
      database_->BeginSnapshot({.as_of = commits[1]});
  ASSERT_TRUE(before_correction.ok())
      << before_correction.status().ToString();
  const ValidTimeInterval range{ValidTime{10}, ValidTime{40}};
  auto earlier_events = internal::TemporalSource::ReadEvents(
      before_correction.ValueOrDie(), FactFamily::kVertexProperty,
      PropertyId{7}, range);
  auto earlier_changes = internal::TemporalSource::ReadChanges(
      before_correction.ValueOrDie(), FactFamily::kVertexProperty,
      PropertyId{7}, range);
  auto earlier_history = internal::TemporalSource::ReadHistory(
      before_correction.ValueOrDie(), FactFamily::kVertexProperty,
      PropertyId{7}, range);
  ASSERT_TRUE(earlier_events.ok()) << earlier_events.status().ToString();
  ASSERT_TRUE(earlier_changes.ok()) << earlier_changes.status().ToString();
  ASSERT_TRUE(earlier_history.ok()) << earlier_history.status().ToString();
  EXPECT_EQ(ObserveEvents(earlier_events.ValueOrDie()),
            (std::vector<ExpectedEvent>{{10, FactOperation::kPut,
                                         commits[0].value},
                                        {30, FactOperation::kPut,
                                         commits[1].value}}));
  EXPECT_EQ(ObserveChanges(earlier_changes.ValueOrDie()),
            (std::vector<ExpectedChange>{{10, std::nullopt, 7}}));
  EXPECT_EQ(ObserveStates(earlier_history.ValueOrDie()),
            (std::vector<ExpectedState>{{10, 40, 7}}));

  auto corrected = database_->BeginSnapshot({.as_of = commits[2]});
  ASSERT_TRUE(corrected.ok()) << corrected.status().ToString();
  auto events = internal::TemporalSource::ReadEvents(
      corrected.ValueOrDie(), FactFamily::kVertexProperty, PropertyId{7},
      range);
  auto changes = internal::TemporalSource::ReadChanges(
      corrected.ValueOrDie(), FactFamily::kVertexProperty, PropertyId{7},
      range);
  auto history = internal::TemporalSource::ReadHistory(
      corrected.ValueOrDie(), FactFamily::kVertexProperty, PropertyId{7},
      range);
  ASSERT_TRUE(events.ok()) << events.status().ToString();
  ASSERT_TRUE(changes.ok()) << changes.status().ToString();
  ASSERT_TRUE(history.ok()) << history.status().ToString();
  EXPECT_EQ(ObserveEvents(events.ValueOrDie()),
            (std::vector<ExpectedEvent>{{10, FactOperation::kPut,
                                         commits[0].value},
                                        {20, FactOperation::kDelete,
                                         commits[2].value},
                                        {30, FactOperation::kPut,
                                         commits[1].value}}));
  EXPECT_EQ(ObserveChanges(changes.ValueOrDie()),
            (std::vector<ExpectedChange>{{10, std::nullopt, 7},
                                         {20, 7, std::nullopt},
                                         {30, std::nullopt, 7}}));
  EXPECT_EQ(ObserveStates(history.ValueOrDie()),
            (std::vector<ExpectedState>{{10, 20, 7}, {30, 40, 7}}));
}

TEST_F(QueryCanonicalTest, ReadsCanonicalAndPropertiesAcrossHomePartitions) {
  ASSERT_TRUE(database_->RegisterProperty(PropertyDefinition{
      PropertyId{17}, 0, "partition_score", PropertyEntityKind::kVertex,
      PhysicalType::kInt64, 4096}).ok());

  const VertexRef local{PartId{0}, VertexId{42}};
  const VertexRef remote{PartId{7}, VertexId{42}};
  auto transaction = database_->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()->Assert(EntityFact::Vertex(local),
                                               ValidTime{1}).ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Assert(EntityFact::Vertex(remote),
                                               ValidTime{1}).ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Set(PropertyFact::Vertex(local, PropertyId{17}),
                        ValidTime{1}, Value::Int64(100))
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Set(PropertyFact::Vertex(remote, PropertyId{17}),
                        ValidTime{1}, Value::Int64(700))
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());

  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  auto& pinned = snapshot.ValueOrDie();

  auto vertices = internal::CanonicalSource::ReadVerticesAt(pinned, ValidTime{1});
  ASSERT_TRUE(vertices.ok()) << vertices.status().ToString();
  EXPECT_EQ(vertices.ValueOrDie(), (std::vector<VertexRef>{local, remote}));

  const ValidTimeInterval interval{ValidTime{0}, ValidTime{2}};
  auto events = internal::TemporalSource::ReadEvents(
      pinned, FactFamily::kVertexProperty, PropertyId{17}, interval);
  ASSERT_TRUE(events.ok()) << events.status().ToString();
  ASSERT_EQ(events.ValueOrDie().size(), 2U);
  EXPECT_EQ(events.ValueOrDie()[0].ref.part_id(), PartId{0});
  EXPECT_EQ(events.ValueOrDie()[1].ref.part_id(), PartId{7});

  auto at = internal::TemporalSource::ReadAt(
      pinned, FactFamily::kVertexProperty, PropertyId{17}, ValidTime{1});
  ASSERT_TRUE(at.ok()) << at.status().ToString();
  ASSERT_EQ(at.ValueOrDie().size(), 2U);
  EXPECT_EQ(at.ValueOrDie()[0].ref, PropertyFact::Vertex(local, PropertyId{17}).ref());
  EXPECT_EQ(at.ValueOrDie()[1].ref, PropertyFact::Vertex(remote, PropertyId{17}).ref());
  EXPECT_EQ(Int64Of(at.ValueOrDie()[0].value), 100);
  EXPECT_EQ(Int64Of(at.ValueOrDie()[1].value), 700);

  auto history = internal::TemporalSource::ReadHistory(
      pinned, FactFamily::kVertexProperty, PropertyId{17}, std::nullopt);
  ASSERT_TRUE(history.ok()) << history.status().ToString();
  ASSERT_EQ(history.ValueOrDie().size(), 2U);
  EXPECT_EQ(history.ValueOrDie()[0].ref.part_id(), PartId{0});
  EXPECT_EQ(history.ValueOrDie()[1].ref.part_id(), PartId{7});

  Slot<VertexRef> vertex = Slot<VertexRef>::Named("partition_vertex");
  OptionalSlot<int64_t> score = OptionalSlot<int64_t>::Named("partition_score");
  auto query = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  query = query.ValueOrDie().BindVertexProperty(vertex, PropertyId{17}, score);
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  query = query.ValueOrDie().Select({Project(vertex), Project(score)});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  ASSERT_EQ(batch.ValueOrDie()->row_count(), 2U);
  EXPECT_EQ(batch.ValueOrDie()->Get<int64_t>(score, 0), 100);
  EXPECT_EQ(batch.ValueOrDie()->Get<int64_t>(score, 1), 700);
}

TEST_F(QueryCanonicalTest, AppliesExactHalfOpenStateScopes) {
  SeedPropertyHistory();
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  auto overlaps = internal::TemporalSource::ReadOverlaps(
      snapshot.ValueOrDie(), FactFamily::kVertexProperty, PropertyId{7},
      ValidTimeInterval{ValidTime{20}, ValidTime{30}});
  auto throughout = internal::TemporalSource::ReadThroughout(
      snapshot.ValueOrDie(), FactFamily::kVertexProperty, PropertyId{7},
      ValidTimeInterval{ValidTime{10}, ValidTime{20}});
  auto crosses_gap = internal::TemporalSource::ReadThroughout(
      snapshot.ValueOrDie(), FactFamily::kVertexProperty, PropertyId{7},
      ValidTimeInterval{ValidTime{10}, ValidTime{21}});
  ASSERT_TRUE(overlaps.ok()) << overlaps.status().ToString();
  ASSERT_TRUE(throughout.ok()) << throughout.status().ToString();
  ASSERT_TRUE(crosses_gap.ok()) << crosses_gap.status().ToString();
  EXPECT_TRUE(overlaps.ValueOrDie().empty());
  EXPECT_EQ(ObserveStates(throughout.ValueOrDie()),
            (std::vector<ExpectedState>{{10, 20, 7}}));
  EXPECT_TRUE(crosses_gap.ValueOrDie().empty());
}

TEST_F(QueryCanonicalTest, DerivesMissingOnlyInsideEntityIntervals) {
  SeedPropertyHistory();
  ASSERT_TRUE(score_definition_.has_value());
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const ValidTimeInterval range{ValidTime{0}, ValidTime{40}};
  auto entities = internal::TemporalSource::ReadHistory(
      snapshot.ValueOrDie(), FactFamily::kVertexState, PropertyId{}, range);
  ASSERT_TRUE(entities.ok()) << entities.status().ToString();
  auto bound = internal::PropertyBinder::BindIntervals(
      snapshot.ValueOrDie(), entities.ValueOrDie(), *score_definition_);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  EXPECT_EQ(ObserveBindings(bound.ValueOrDie()),
            (std::vector<ExpectedState>{{0, 10, std::nullopt},
                                        {10, 20, 7},
                                        {20, 30, std::nullopt},
                                        {30, 40, 7}}));
}

TEST_F(QueryCanonicalTest, ClipsPredicatesAndKeepsTwoValuedMissing) {
  SeedPropertyHistory();
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  OptionalSlot<int64_t> score = OptionalSlot<int64_t>::Named("score");

  auto source = Query::Vertices(
      vertex, History{ValidTimeInterval{ValidTime{0}, ValidTime{30}}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();
  auto bound = source.ValueOrDie().BindVertexProperty(vertex, PropertyId{7}, score);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  auto missing = bound.ValueOrDie().Where(IsMissing(score));
  ASSERT_TRUE(missing.ok()) << missing.status().ToString();
  auto missing_query = missing.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(missing_query.ok()) << missing_query.status().ToString();
  auto missing_rows = CountRows(missing_query.ValueOrDie());
  ASSERT_TRUE(missing_rows.ok()) << missing_rows.status().ToString();
  EXPECT_EQ(missing_rows.ValueOrDie(), 2U);

  auto at = Query::Vertices(vertex, At{ValidTime{5}});
  ASSERT_TRUE(at.ok()) << at.status().ToString();
  auto at_bound = at.ValueOrDie().BindVertexProperty(vertex, PropertyId{7}, score);
  ASSERT_TRUE(at_bound.ok()) << at_bound.status().ToString();
  auto not_equal_by_not = at_bound.ValueOrDie().Where(
      Not(Equal(ValueOf(score), Literal<int64_t>(7))));
  auto ordinary_not_equal = at_bound.ValueOrDie().Where(
      NotEqual(ValueOf(score), Literal<int64_t>(7)));
  ASSERT_TRUE(not_equal_by_not.ok()) << not_equal_by_not.status().ToString();
  ASSERT_TRUE(ordinary_not_equal.ok())
      << ordinary_not_equal.status().ToString();
  auto not_query = not_equal_by_not.ValueOrDie().Select({Project(vertex)});
  auto not_equal_query =
      ordinary_not_equal.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(not_query.ok()) << not_query.status().ToString();
  ASSERT_TRUE(not_equal_query.ok()) << not_equal_query.status().ToString();
  EXPECT_EQ(CountRows(not_query.ValueOrDie()).ValueOrDie(), 1U);
  EXPECT_EQ(CountRows(not_equal_query.ValueOrDie()).ValueOrDie(), 0U);
}

TEST_F(QueryCanonicalTest, BindsTypedEdgeProperties) {
  const auto property = database_->RegisterProperty(PropertyDefinition{
      PropertyId{8}, 0, "weight", PropertyEntityKind::kEdge,
      PhysicalType::kInt64, 4096});
  ASSERT_TRUE(property.ok()) << property.status().ToString();
  auto transaction = database_->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  const VertexRef source{PartId{0}, VertexId{1}};
  const VertexRef target{PartId{0}, VertexId{2}};
  const EdgeRef edge_ref{PartId{0}, EdgeId{9}};
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(source), ValidTime{0})
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(target), ValidTime{0})
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EdgeIdentity{edge_ref, source, target, 5},
                           ValidTime{0})
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Set(PropertyFact::Edge(edge_ref, PropertyId{8}),
                        ValidTime{0}, Value::Int64(11))
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());

  Slot<EdgeRef> edge = Slot<EdgeRef>::Named("e");
  OptionalSlot<int64_t> weight = OptionalSlot<int64_t>::Named("weight");
  auto source_query = Query::Edges(edge, At{ValidTime{5}});
  ASSERT_TRUE(source_query.ok()) << source_query.status().ToString();
  auto bound = source_query.ValueOrDie().BindEdgeProperty(
      edge, PropertyId{8}, weight);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  auto query = bound.ValueOrDie().Select({Project(edge), Project(weight)});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  ASSERT_EQ(batch.ValueOrDie()->row_count(), 1U);
  EXPECT_EQ(batch.ValueOrDie()->Get<EdgeRef>(edge, 0), edge_ref);
  EXPECT_EQ(batch.ValueOrDie()->Get<int64_t>(weight, 0), 11);
}

TEST_F(QueryCanonicalTest, BindsMultipleCanonicalVertexProperties) {
  ASSERT_TRUE(database_->RegisterProperty(PropertyDefinition{
      PropertyId{10}, 0, "left", PropertyEntityKind::kVertex,
      PhysicalType::kInt64, 4096}).ok());
  ASSERT_TRUE(database_->RegisterProperty(PropertyDefinition{
      PropertyId{11}, 0, "right", PropertyEntityKind::kVertex,
      PhysicalType::kInt64, 4096}).ok());

  const VertexRef vertex_ref{PartId{0}, VertexId{1}};
  auto transaction = database_->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(vertex_ref), ValidTime{0})
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Set(PropertyFact::Vertex(vertex_ref, PropertyId{10}),
                        ValidTime{0}, Value::Int64(101))
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Set(PropertyFact::Vertex(vertex_ref, PropertyId{11}),
                        ValidTime{0}, Value::Int64(202))
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());

  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  OptionalSlot<int64_t> left = OptionalSlot<int64_t>::Named("left");
  OptionalSlot<int64_t> right = OptionalSlot<int64_t>::Named("right");
  auto source = Query::Vertices(
      vertex, History{ValidTimeInterval{ValidTime{0}, ValidTime{10}}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();
  auto with_left = source.ValueOrDie().BindVertexProperty(
      vertex, PropertyId{10}, left);
  ASSERT_TRUE(with_left.ok()) << with_left.status().ToString();
  auto with_right = with_left.ValueOrDie().BindVertexProperty(
      vertex, PropertyId{11}, right);
  ASSERT_TRUE(with_right.ok()) << with_right.status().ToString();
  auto query = with_right.ValueOrDie().Select(
      {Project(vertex), Project(left), Project(right)});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  ASSERT_EQ(batch.ValueOrDie()->row_count(), 1U);
  EXPECT_EQ(batch.ValueOrDie()->Get<VertexRef>(vertex, 0), vertex_ref);
  EXPECT_EQ(batch.ValueOrDie()->Get<int64_t>(left, 0), 101);
  EXPECT_EQ(batch.ValueOrDie()->Get<int64_t>(right, 0), 202);
}

TEST_F(QueryCanonicalTest, BindsMultipleCanonicalEdgeProperties) {
  ASSERT_TRUE(database_->RegisterProperty(PropertyDefinition{
      PropertyId{12}, 0, "weight", PropertyEntityKind::kEdge,
      PhysicalType::kInt64, 4096}).ok());
  ASSERT_TRUE(database_->RegisterProperty(PropertyDefinition{
      PropertyId{13}, 0, "capacity", PropertyEntityKind::kEdge,
      PhysicalType::kInt64, 4096}).ok());

  const VertexRef source_ref{PartId{0}, VertexId{1}};
  const VertexRef target_ref{PartId{0}, VertexId{2}};
  const EdgeRef edge_ref{PartId{0}, EdgeId{9}};
  auto transaction = database_->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(source_ref), ValidTime{0})
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(target_ref), ValidTime{0})
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EdgeIdentity{edge_ref, source_ref, target_ref, 5},
                           ValidTime{0})
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Set(PropertyFact::Edge(edge_ref, PropertyId{12}),
                        ValidTime{0}, Value::Int64(303))
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Set(PropertyFact::Edge(edge_ref, PropertyId{13}),
                        ValidTime{0}, Value::Int64(404))
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());

  Slot<EdgeRef> edge = Slot<EdgeRef>::Named("e");
  OptionalSlot<int64_t> weight = OptionalSlot<int64_t>::Named("weight");
  OptionalSlot<int64_t> capacity = OptionalSlot<int64_t>::Named("capacity");
  auto source = Query::Edges(
      edge, History{ValidTimeInterval{ValidTime{0}, ValidTime{10}}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();
  auto with_weight = source.ValueOrDie().BindEdgeProperty(
      edge, PropertyId{12}, weight);
  ASSERT_TRUE(with_weight.ok()) << with_weight.status().ToString();
  auto with_capacity = with_weight.ValueOrDie().BindEdgeProperty(
      edge, PropertyId{13}, capacity);
  ASSERT_TRUE(with_capacity.ok()) << with_capacity.status().ToString();
  auto query = with_capacity.ValueOrDie().Select(
      {Project(edge), Project(weight), Project(capacity)});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  ASSERT_EQ(batch.ValueOrDie()->row_count(), 1U);
  EXPECT_EQ(batch.ValueOrDie()->Get<EdgeRef>(edge, 0), edge_ref);
  EXPECT_EQ(batch.ValueOrDie()->Get<int64_t>(weight, 0), 303);
  EXPECT_EQ(batch.ValueOrDie()->Get<int64_t>(capacity, 0), 404);
}

TEST_F(QueryCanonicalTest, RejectsPropertyKindAndPhysicalTypeAtPrepare) {
  SeedPropertyHistory();
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  OptionalSlot<std::string> wrong_type =
      OptionalSlot<std::string>::Named("wrong_type");
  auto vertices = Query::Vertices(vertex, At{ValidTime{5}});
  ASSERT_TRUE(vertices.ok()) << vertices.status().ToString();
  auto typed_wrong = vertices.ValueOrDie().BindVertexProperty(
      vertex, PropertyId{7}, wrong_type);
  ASSERT_TRUE(typed_wrong.ok()) << typed_wrong.status().ToString();
  EXPECT_TRUE(database_->PrepareQuery(typed_wrong.ValueOrDie())
                  .status()
                  .IsSchemaMismatch());

  Slot<EdgeRef> edge = Slot<EdgeRef>::Named("e");
  OptionalSlot<int64_t> score = OptionalSlot<int64_t>::Named("score");
  auto edges = Query::Edges(edge, At{ValidTime{5}});
  ASSERT_TRUE(edges.ok()) << edges.status().ToString();
  auto kind_wrong = edges.ValueOrDie().BindEdgeProperty(
      edge, PropertyId{7}, score);
  ASSERT_TRUE(kind_wrong.ok()) << kind_wrong.status().ToString();
  EXPECT_TRUE(database_->PrepareQuery(kind_wrong.ValueOrDie())
                  .status()
                  .IsSchemaMismatch());
}

TEST_F(QueryCanonicalTest, RequiresAnAnalyticalBudgetForUnboundedHistory) {
  ASSERT_EQ(AssertVertex(1, 10), CommitSeq{1});
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto source = Query::Vertices(vertex, History{std::nullopt});
  ASSERT_TRUE(source.ok()) << source.status().ToString();
  auto query = source.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();

  auto default_snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(default_snapshot.ok()) << default_snapshot.status().ToString();
  EXPECT_TRUE(prepared.ValueOrDie()
                  .Execute(std::move(default_snapshot).ConsumeValueOrDie(),
                           Bindings{}, QueryOptions{})
                  .status()
                  .IsInvalidArgument());

  QueryOptions analytical;
  analytical.mode = QueryExecutionMode::kAnalytical;
  auto analytical_snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(analytical_snapshot.ok())
      << analytical_snapshot.status().ToString();
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(analytical_snapshot).ConsumeValueOrDie(), Bindings{},
      analytical);
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  EXPECT_EQ(batch.ValueOrDie()->row_count(), 1U);
}

}  // namespace
}  // namespace cedar
