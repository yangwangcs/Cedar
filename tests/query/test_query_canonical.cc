#include <gtest/gtest.h>

#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "cedar/database.h"
#include "cedar/query.h"
#include "cedar/transaction.h"

namespace cedar {
namespace {

static_assert(std::is_copy_constructible_v<PreparedQuery>);
static_assert(std::is_copy_assignable_v<PreparedQuery>);
static_assert(!std::is_move_assignable_v<const PreparedQuery>);
static_assert(!std::is_copy_constructible_v<QueryCursor>);
static_assert(!std::is_copy_assignable_v<QueryCursor>);
static_assert(std::is_move_constructible_v<QueryCursor>);
static_assert(std::is_move_assignable_v<QueryCursor>);

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
  EXPECT_TRUE(database_->Close().IsSnapshotPinned());
  ASSERT_TRUE(cursor.ValueOrDie().Next().ok());
  EXPECT_TRUE(database_->Close().IsSnapshotPinned());
  ASSERT_FALSE(cursor.ValueOrDie().Next().ValueOrDie().has_value());
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

}  // namespace
}  // namespace cedar
