#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "cedar/database.h"
#include "cedar/query/query.h"
#include "cedar/transaction.h"

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

TEST_F(QueryLifecycleTest, BindingDoesNotReuseAnotherValidTime) {
  auto transaction = database_->BeginTransaction();
  ASSERT_TRUE(transaction.ok());
  for (uint64_t id : {1ULL, 2ULL}) {
    ASSERT_TRUE(transaction.ValueOrDie()->Assert(
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{id}}),
        ValidTime{1}).ok());
  }
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());
  const auto vertex = Slot<VertexRef>::WithId(SlotId{42}, "v");
  for (uint64_t time : {10ULL, 20ULL, 10ULL, 30ULL}) {
    SCOPED_TRACE(time);
    auto source = Query::Vertices(vertex, At{ValidTime{time}});
    ASSERT_TRUE(source.ok());
    auto query = source.ValueOrDie().Select({Project(vertex)});
    ASSERT_TRUE(query.ok());
    auto prepared = database_->PrepareQuery(query.ValueOrDie());
    ASSERT_TRUE(prepared.ok());
    auto snapshot = database_->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok());
    ASSERT_EQ(snapshot.ValueOrDie().commit_seq(), CommitSeq{1});
    auto cursor = prepared.ValueOrDie().Execute(
        std::move(snapshot).ConsumeValueOrDie(), {}, {});
    ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
    size_t rows = 0;
    while (true) {
      auto batch = cursor.ValueOrDie().Next();
      ASSERT_TRUE(batch.ok()) << batch.status().ToString();
      if (!batch.ValueOrDie()) break;
      rows += batch.ValueOrDie()->row_count();
    }
    EXPECT_EQ(rows, 2U);
    EXPECT_TRUE(cursor.ValueOrDie().terminal_info().complete);
    ASSERT_TRUE(cursor.ValueOrDie().Close().ok());
  }
}

TEST_F(QueryLifecycleTest, BindingUsesCurrentExecutionMode) {
  auto transaction = database_->BeginTransaction();
  ASSERT_TRUE(transaction.ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Assert(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{1}).ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());
  const auto vertex = Slot<VertexRef>::Named("v");
  auto source = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(source.ok());
  auto query = source.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(query.ok());
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok());
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  for (auto mode : {QueryExecutionMode::kInteractive,
                    QueryExecutionMode::kAnalytical,
                    QueryExecutionMode::kInteractive}) {
    auto explanation = prepared.ValueOrDie().ExplainPhysical(
        snapshot.ValueOrDie(), QueryOptions{.mode = mode});
    ASSERT_TRUE(explanation.ok()) << explanation.status().ToString();
    const char* expected = mode == QueryExecutionMode::kAnalytical
                               ? "lane=analytical" : "lane=interactive";
    EXPECT_NE(explanation.ValueOrDie().find(expected), std::string::npos)
        << explanation.ValueOrDie();
  }
}

TEST_F(QueryLifecycleTest, BindingAcrossDatabasesWithoutProjectionUsesCurrentContext) {
  auto other_opened = Database::Open(DatabaseOptions{.path = path_ + "/other"});
  ASSERT_TRUE(other_opened.ok()) << other_opened.status().ToString();
  auto other = other_opened.ConsumeValueOrDie();
  const VertexRef first{PartId{0}, VertexId{1}};
  const VertexRef second{PartId{7}, VertexId{2}};
  for (const auto& [database, vertex, birth] : {
           std::tuple{database_.get(), first, uint64_t{1}},
           std::tuple{other.get(), second, uint64_t{15}}}) {
    auto transaction = database->BeginTransaction();
    ASSERT_TRUE(transaction.ok());
    ASSERT_TRUE(transaction.ValueOrDie()->Assert(
        EntityFact::Vertex(vertex), ValidTime{birth}).ok());
    ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());
  }
  const auto output = Slot<VertexRef>::WithId(SlotId{42}, "v");
  for (const uint64_t time : {20ULL, 10ULL, 20ULL}) {
    SCOPED_TRACE(time);
    auto source = Query::Vertices(output, At{ValidTime{time}});
    ASSERT_TRUE(source.ok());
    auto query = source.ValueOrDie().Select({Project(output)});
    ASSERT_TRUE(query.ok());
    for (Database* database : {database_.get(), other.get(), database_.get()}) {
      const bool is_first = database == database_.get();
      SCOPED_TRACE(is_first ? "first database" : "second database");
      auto prepared = database->PrepareQuery(query.ValueOrDie());
      ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
      auto snapshot = database->BeginSnapshot();
      ASSERT_TRUE(snapshot.ok());
      ASSERT_EQ(snapshot.ValueOrDie().commit_seq(), CommitSeq{1});
      auto cursor = prepared.ValueOrDie().Execute(
          snapshot.ConsumeValueOrDie(), {}, {});
      ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
      std::vector<VertexRef> rows;
      for (;;) {
        auto batch = cursor.ValueOrDie().Next();
        ASSERT_TRUE(batch.ok()) << batch.status().ToString();
        if (!batch.ValueOrDie()) break;
        for (size_t row = 0; row < batch.ValueOrDie()->row_count(); ++row)
          rows.push_back(batch.ValueOrDie()->Get<VertexRef>(output, row));
      }
      if (is_first) EXPECT_EQ(rows, (std::vector<VertexRef>{first}));
      else if (time == 10) EXPECT_TRUE(rows.empty());
      else EXPECT_EQ(rows, (std::vector<VertexRef>{second}));
      EXPECT_TRUE(cursor.ValueOrDie().terminal_info().complete);
      ASSERT_TRUE(cursor.ValueOrDie().Close().ok());
    }
  }
  ASSERT_TRUE(other->Close().ok());
}

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

// Regression for the Close vs query-planning race: Database::Close used to
// reset query_delta/projection_store without Database::Impl::mutex while
// QueryExecutionContextFactory::Bind read those unique_ptrs without any lock.
// The observer parks Bind after it has reached the derived query state and
// before the state is used; a concurrent pinned Close detaches the state in
// that window. With the fix Bind holds a reference taken under the same mutex
// and completes; without it the parked Bind dereferences the reset owner.
TEST_F(QueryLifecycleTest, CloseRacingBindDoesNotDereferenceTornDownQueryState) {
  ASSERT_TRUE(database_->Close().ok());
  database_.reset();

  std::atomic<bool> bind_entered{false};
  std::atomic<bool> bind_release{false};
  DatabaseOptions options;
  options.path = path_;
  options.query_open_stage_observer_for_testing = [&](const char* stage) {
    if (std::string(stage) != "query_bind_state_acquired") return;
    bind_entered.store(true, std::memory_order_release);
    while (!bind_release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  };
  auto reopened = Database::Open(std::move(options));
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database_ = std::move(reopened).ConsumeValueOrDie();

  auto committed = database_->BeginTransaction();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  ASSERT_TRUE(committed.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{11}}),
                           ValidTime{1})
                  .ok());
  ASSERT_TRUE(committed.ValueOrDie()->Commit().ok());

  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto scan = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(scan.ok());
  auto query = scan.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(query.ok());
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  StatusOr<QueryCursor> cursor = Status::InvalidArgument("query", "not executed");
  std::thread executor([&] {
    cursor = prepared.ValueOrDie().Execute(
        std::move(snapshot).ConsumeValueOrDie(), Bindings{},
        QueryOptions{.mode = QueryExecutionMode::kAnalytical});
  });

  const auto entered_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!bind_entered.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < entered_deadline) {
    std::this_thread::yield();
  }
  if (!bind_entered.load(std::memory_order_acquire)) {
    // Never ASSERT while the executor thread is joinable: a fatal assertion
    // returns from the test body and std::thread's destructor would call
    // std::terminate. Unblock the parked hook, join, then fail.
    bind_release.store(true, std::memory_order_release);
    executor.join();
    FAIL() << "Bind did not reach the derived-state acquire stage";
  }

  // The snapshot held by Execute pins the store, so Close must fail with
  // SnapshotPinned. The race under test is the derived query state being
  // detached while Bind is parked between reading it and using it.
  const Status close_status = database_->Close();
  EXPECT_TRUE(close_status.IsSnapshotPinned()) << close_status.ToString();

  bind_release.store(true, std::memory_order_release);
  executor.join();

  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  QueryCursor active = std::move(cursor).ConsumeValueOrDie();
  EXPECT_TRUE(active.Close().ok());
  EXPECT_TRUE(database_->Close().ok());
}

// B1 synchronized-copy invariant: the earlier regression only proved that a
// reference taken before a pinned Close keeps the detached owners alive. This
// test pins the check+capture critical section itself: Bind parks inside
// Database::Impl::mutex (test-only "query_bind_state_captured" stage) while a
// concurrent Close must block on that same mutex. Removing that lock (or moving
// the capture outside it) lets Close race in and finish, so the bounded liveness
// assertion below fails.
TEST_F(QueryLifecycleTest, BindCaptureHoldsLifecycleMutexAgainstClose) {
  ASSERT_TRUE(database_->Close().ok());
  database_.reset();

  std::atomic<bool> bind_captured{false};
  std::atomic<bool> bind_release{false};
  DatabaseOptions options;
  options.path = path_;
  options.query_open_stage_observer_for_testing = [&](const char* stage) {
    if (std::string(stage) != "query_bind_state_captured") return;
    bind_captured.store(true, std::memory_order_release);
    while (!bind_release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  };
  auto reopened = Database::Open(std::move(options));
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database_ = std::move(reopened).ConsumeValueOrDie();

  auto committed = database_->BeginTransaction();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  ASSERT_TRUE(committed.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{3}}),
                           ValidTime{1})
                  .ok());
  ASSERT_TRUE(committed.ValueOrDie()->Commit().ok());

  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto scan = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(scan.ok());
  auto query = scan.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(query.ok());
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto opened_snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(opened_snapshot.ok()) << opened_snapshot.status().ToString();
  std::optional<Snapshot> snapshot(
      std::move(opened_snapshot).ConsumeValueOrDie());

  StatusOr<std::string> explained =
      Status::InvalidArgument("query", "not explained");
  std::thread explainer([&] {
    explained = prepared.ValueOrDie().ExplainPhysical(
        snapshot.value(), QueryOptions{.mode = QueryExecutionMode::kAnalytical});
  });
  const auto captured_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!bind_captured.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < captured_deadline) {
    std::this_thread::yield();
  }
  if (!bind_captured.load(std::memory_order_acquire)) {
    // Joinable-thread safety: release the parked hook and join before failing,
    // otherwise the fatal assertion returns from the test with a joinable
    // std::thread local and std::terminate fires instead of a clean failure.
    bind_release.store(true, std::memory_order_release);
    explainer.join();
    FAIL() << "Bind did not reach the in-mutex capture stage";
  }

  Status close_status = Status::OK();
  std::atomic<bool> close_finished{false};
  std::thread closer([&] {
    close_status = database_->Close();
    close_finished.store(true, std::memory_order_release);
  });
  // Bounded liveness check: Close must block on the mutex Bind holds while it
  // captures the derived-state references. A close without that synchronization
  // fails pinned in milliseconds, so this window discriminates the lock.
  //
  // Limitation: this is a bounded *negative* check, not a proof. On a machine
  // that delays an unsynchronized Close by more than 200 ms (extreme scheduler or
  // I/O pressure) the window would pass vacuously. What pins the invariant is the
  // deterministic red evidence (without the lock, Close finished within
  // milliseconds) together with the pinned-close result asserted below; the
  // 200 ms bound only rules out a same-window false pass on a healthy machine.
  const auto blocked_deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
  while (!close_finished.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < blocked_deadline) {
    std::this_thread::yield();
  }
  EXPECT_FALSE(close_finished.load(std::memory_order_acquire))
      << "Close ran while Bind held Database::Impl::mutex; the derived-state "
         "capture is not synchronized with the Close detach";

  bind_release.store(true, std::memory_order_release);
  explainer.join();
  closer.join();

  EXPECT_TRUE(explained.ok()) << explained.status().ToString();
  EXPECT_TRUE(close_status.IsSnapshotPinned()) << close_status.ToString();
  snapshot.reset();
  EXPECT_TRUE(database_->Close().ok());
}

}  // namespace
}  // namespace cedar
