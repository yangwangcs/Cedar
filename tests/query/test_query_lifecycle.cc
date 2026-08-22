#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <string>
#include <vector>

#include "cedar/database.h"
#include "cedar/query/query.h"

namespace cedar {
namespace {

class QueryLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_query_lifecycle_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
    DatabaseOptions options;
    options.path = path_;
    auto opened = Database::Open(std::move(options));
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    database_ = std::move(opened).ConsumeValueOrDie();
  }
  void TearDown() override {
    database_.reset();
    std::filesystem::remove_all(path_);
  }

  std::string path_;
  std::unique_ptr<Database> database_;
};

TEST_F(QueryLifecycleTest, CleanEndIsIdempotentAndComplete) {
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto scan = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(scan.ok());
  auto query = scan.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{},
      QueryOptions{.mode = QueryExecutionMode::kAnalytical});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  QueryCursor actual = std::move(cursor).ConsumeValueOrDie();
  auto first = actual.Next();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_FALSE(first.ValueOrDie().has_value());
  EXPECT_EQ(actual.terminal_info().state, QueryCursorState::kCleanEnd);
  EXPECT_TRUE(actual.terminal_info().complete);
  auto second = actual.Next();
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  ASSERT_FALSE(second.ValueOrDie().has_value());
  EXPECT_TRUE(actual.Close().ok());
}

TEST_F(QueryLifecycleTest, CancelPublishesIncompleteTerminalState) {
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto scan = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(scan.ok());
  auto query = scan.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(query.ok());
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok());
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{},
      QueryOptions{.mode = QueryExecutionMode::kAnalytical});
  ASSERT_TRUE(cursor.ok());
  QueryCursor actual = std::move(cursor).ConsumeValueOrDie();
  ASSERT_TRUE(actual.Cancel().ok());
  auto next = actual.Next();
  ASSERT_FALSE(next.ok());
  EXPECT_TRUE(next.status().IsQueryCancelled());
  EXPECT_EQ(actual.terminal_info().state, QueryCursorState::kCancelled);
  EXPECT_FALSE(actual.terminal_info().complete);
  EXPECT_TRUE(actual.Close().ok());
}

TEST_F(QueryLifecycleTest, DatabaseCloseCancelsQueryBeforeStoreClose) {
  std::mutex events_mutex;
  std::vector<std::string> events;
  // Reopen with the observer because SetUp intentionally keeps the fixture
  // options minimal for the cursor tests above.
  const Status close_status = database_->Close();
  ASSERT_TRUE(close_status.ok()) << close_status.ToString();
  database_.reset();
  DatabaseOptions options;
  options.path = path_;
  options.shutdown_stage_observer_for_testing =
      [&events_mutex, &events](const char* stage) {
        std::lock_guard<std::mutex> lock(events_mutex);
        events.emplace_back(stage);
      };
  auto opened = Database::Open(std::move(options));
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  database_ = std::move(opened).ConsumeValueOrDie();

  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto scan = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(scan.ok());
  auto query = scan.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(query.ok());
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok());
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{},
      QueryOptions{.mode = QueryExecutionMode::kAnalytical});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  QueryCursor active = std::move(cursor).ConsumeValueOrDie();

  const Status query_close_status = database_->Close();
  ASSERT_TRUE(query_close_status.ok()) << query_close_status.ToString();
  EXPECT_EQ(events,
            (std::vector<std::string>{"admission_closed",
                                      "query_cancel_requested",
                                      "query_tasks_joined",
                                      "accepted_commits_drained",
                                      "query_delta_stopped",
                                      "projection_builders_stopped",
                                      "maintenance_joined", "scratch_cleaned",
                                      "rocksdb_closed"}));
  EXPECT_TRUE(active.Next().status().IsQueryCancelled());
  EXPECT_TRUE(active.Close().ok());
}

TEST_F(QueryLifecycleTest, DeadlineAndResourceFailuresAreIncomplete) {
  auto begun = database_->BeginTransaction();
  ASSERT_TRUE(begun.ok());
  ASSERT_TRUE(begun.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}),
                           ValidTime{1})
                  .ok());
  ASSERT_TRUE(begun.ValueOrDie()->Commit().ok());
  auto second = database_->BeginTransaction();
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(second.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{8}}),
                           ValidTime{1})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()->Commit().ok());

  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto scan = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(scan.ok());
  auto query = scan.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(query.ok());
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok());
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  QueryOptions options;
  options.budget.deadline_us = 1;
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, options);
  ASSERT_TRUE(cursor.ok());
  QueryCursor deadline = std::move(cursor).ConsumeValueOrDie();
  auto next = deadline.Next();
  ASSERT_FALSE(next.ok());
  EXPECT_TRUE(next.status().IsDeadlineExceeded());
  EXPECT_EQ(deadline.terminal_info().state, QueryCursorState::kFailed);
  EXPECT_FALSE(deadline.terminal_info().complete);

  auto second_snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(second_snapshot.ok());
  options = QueryOptions{};
  options.budget.output_rows = 0;
  auto resource_cursor = prepared.ValueOrDie().Execute(
      std::move(second_snapshot).ConsumeValueOrDie(), Bindings{}, options);
  ASSERT_TRUE(resource_cursor.ok());
  QueryCursor resource = std::move(resource_cursor).ConsumeValueOrDie();
  auto resource_next = resource.Next();
  ASSERT_FALSE(resource_next.ok());
  EXPECT_TRUE(resource_next.status().IsResourceExhausted());
  EXPECT_EQ(resource.terminal_info().state, QueryCursorState::kFailed);
  EXPECT_FALSE(resource.terminal_info().complete);
}

TEST_F(QueryLifecycleTest, VacuumRespectsAnalyticalQuerySnapshotPin) {
  auto begun = database_->BeginTransaction();
  ASSERT_TRUE(begun.ok());
  ASSERT_TRUE(begun.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}),
                           ValidTime{1})
                  .ok());
  ASSERT_TRUE(begun.ValueOrDie()->Commit().ok());
  auto second = database_->BeginTransaction();
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(second.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{8}}),
                           ValidTime{1})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()->Commit().ok());
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto scan = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(scan.ok());
  auto query = scan.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(query.ok());
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok());
  auto snapshot = database_->BeginSnapshot({.as_of = CommitSeq{1}});
  ASSERT_TRUE(snapshot.ok());
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{},
      QueryOptions{.mode = QueryExecutionMode::kAnalytical});
  ASSERT_TRUE(cursor.ok());
  QueryCursor active = std::move(cursor).ConsumeValueOrDie();
  EXPECT_TRUE(database_->Vacuum(CommitSeq{2}).IsSnapshotPinned());
  auto batch = active.Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  ASSERT_TRUE(active.Close().ok());
  const Status vacuum_status = database_->Vacuum(CommitSeq{2});
  EXPECT_TRUE(vacuum_status.ok()) << vacuum_status.ToString();
}

}  // namespace
}  // namespace cedar
