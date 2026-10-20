#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "cedar/database.h"

namespace cedar {
namespace {

using namespace std::chrono_literals;

class KernelLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_kernel_lifecycle_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
  }

  void TearDown() override {
    database_.reset();
    std::filesystem::remove_all(path_);
  }

  std::unique_ptr<Database> Open(DatabaseOptions options = {}) {
    options.path = path_;
    auto opened = Database::Open(std::move(options));
    EXPECT_TRUE(opened.ok()) << opened.status().ToString();
    return opened.ok() ? std::move(opened).ConsumeValueOrDie() : nullptr;
  }

  std::unique_ptr<Transaction> Begin() {
    auto transaction = database_->BeginTransaction();
    EXPECT_TRUE(transaction.ok()) << transaction.status().ToString();
    return transaction.ok() ? std::move(transaction).ConsumeValueOrDie() : nullptr;
  }

  std::string path_;
  std::unique_ptr<Database> database_;
};

TEST_F(KernelLifecycleTest, UsesIndependentDurableVertexAndEdgeLeases) {
  database_ = Open();
  ASSERT_TRUE(database_);

  const auto vertex_one = database_->AllocateVertexId();
  const auto vertex_two = database_->AllocateVertexId();
  const auto edge_one = database_->AllocateEdgeId();
  const auto edge_two = database_->AllocateEdgeId();
  ASSERT_TRUE(vertex_one.ok()) << vertex_one.status().ToString();
  ASSERT_TRUE(vertex_two.ok()) << vertex_two.status().ToString();
  ASSERT_TRUE(edge_one.ok()) << edge_one.status().ToString();
  ASSERT_TRUE(edge_two.ok()) << edge_two.status().ToString();
  EXPECT_EQ(vertex_one.ValueOrDie(), VertexId{1});
  EXPECT_EQ(vertex_two.ValueOrDie(), VertexId{2});
  EXPECT_EQ(edge_one.ValueOrDie(), EdgeId{1});
  EXPECT_EQ(edge_two.ValueOrDie(), EdgeId{2});

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  database_ = Open();
  ASSERT_TRUE(database_);
  EXPECT_EQ(database_->AllocateVertexId().ValueOrDie(), VertexId{4097});
  EXPECT_EQ(database_->AllocateEdgeId().ValueOrDie(), EdgeId{4097});
}

TEST_F(KernelLifecycleTest, CloseWaitsForAnActiveCommitBeforeClosingStore) {
  struct Gate {
    std::mutex mutex;
    std::condition_variable condition;
    bool entered = false;
    bool release = false;
  } gate;
  database_ = Open(DatabaseOptions{
      .commit_prewrite_fault_injector_for_testing = [&gate] {
        std::unique_lock<std::mutex> lock(gate.mutex);
        gate.entered = true;
        gate.condition.notify_all();
        gate.condition.wait(lock, [&gate] { return gate.release; });
        return Status::OK();
      }});
  ASSERT_TRUE(database_);
  auto transaction = Begin();
  ASSERT_TRUE(transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{1}).ok());

  auto commit = std::async(std::launch::async, [&transaction] {
    return transaction->Commit();
  });
  {
    std::unique_lock<std::mutex> lock(gate.mutex);
    ASSERT_TRUE(gate.condition.wait_for(lock, 5s, [&gate] { return gate.entered; }));
  }

  auto close = std::async(std::launch::async, [this] { return database_->Close(); });
  EXPECT_EQ(close.wait_for(100ms), std::future_status::timeout);

  {
    std::lock_guard<std::mutex> lock(gate.mutex);
    gate.release = true;
  }
  gate.condition.notify_all();
  ASSERT_EQ(commit.wait_for(5s), std::future_status::ready);
  ASSERT_TRUE(commit.get().ok());
  ASSERT_EQ(close.wait_for(5s), std::future_status::ready);
  EXPECT_TRUE(close.get().ok());
  EXPECT_TRUE(database_->BeginSnapshot().status().IsInvalidArgument());
}

TEST_F(KernelLifecycleTest, ClosePublishesDeterministicPipelineShutdownOrder) {
  std::mutex events_mutex;
  std::vector<std::string> events;
  database_ = Open(DatabaseOptions{
      .shutdown_stage_observer_for_testing = [&events_mutex, &events](
          const char* stage) {
        std::lock_guard<std::mutex> lock(events_mutex);
        events.emplace_back(stage);
      }});
  ASSERT_TRUE(database_);

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  EXPECT_EQ(events, (std::vector<std::string>{
                        "queue_admission_closed",
                        "active_commit_resolution",
                        "queue_worker_stop",
                        "preparation_join",
                        "commit_join",
                        "maintenance_join",
                        "sampler_join",
                        "final_runtime_snapshot",
                        "query_delta_stopped",
                        "rocksdb_close",
                    }));
}

TEST_F(KernelLifecycleTest, KernelDataReopensThroughKernelProfile) {
  const auto kernel_options = [] {
    DatabaseOptions options;
    options.storage_profile = StorageProfile::kProductionAppend;
    options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
    options.production.kernel_mode = true;
    options.query_runtime.query_memory_bytes = 32ULL * 1024ULL * 1024ULL;
    options.query_runtime.projection_cache_bytes = 32ULL * 1024ULL * 1024ULL;
    options.query_runtime.query_delta_bytes = 32ULL * 1024ULL * 1024ULL;
    options.runtime_pressure_override_for_testing = [](PressureSample* sample) {
      sample->free_disk_bytes = UINT64_MAX;
      sample->free_disk_percent = 100;
    };
    return options;
  };
  const auto expect_fact_and_transaction = [this](Database* database,
                                                   TxnId txn_id) {
    auto snapshot = database->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto exists = snapshot.ValueOrDie().Exists(
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{17}}), ValidTime{3});
    ASSERT_TRUE(exists.ok()) << exists.status().ToString();
    EXPECT_TRUE(exists.ValueOrDie());
    const auto resolved = database->ResolveTransaction(txn_id);
    ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
    ASSERT_TRUE(resolved.ValueOrDie().has_value());
    EXPECT_EQ(resolved.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  };

  auto kernel = Open(kernel_options());
  ASSERT_TRUE(kernel);
  auto transaction = kernel->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{17}}),
                           ValidTime{3})
                  .ok());
  const auto committed = transaction.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  ASSERT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  const TxnId txn_id = committed.ValueOrDie().txn_id;
  ASSERT_TRUE(kernel->Close().ok());

  auto reopened = Open(kernel_options());
  ASSERT_TRUE(reopened);
  expect_fact_and_transaction(reopened.get(), txn_id);
  ASSERT_TRUE(reopened->Close().ok());
}

TEST_F(KernelLifecycleTest, RecoveryRepairUsesConfiguredQueryDeltaByteBudget) {
  database_ = Open();
  ASSERT_TRUE(database_);
  auto transaction = Begin();
  ASSERT_TRUE(transaction);
  ASSERT_TRUE(transaction
                  ->Assert(EntityFact::Vertex(
                               VertexRef{PartId{0}, VertexId{1}}),
                           ValidTime{1})
                  .ok());
  ASSERT_TRUE(transaction->Commit().ok());
  ASSERT_TRUE(database_->Close().ok());
  database_.reset();

  uint64_t observed_budget = 0;
  DatabaseOptions options;
  options.query_runtime.query_delta_bytes = 1;
  options.query_delta_repair_budget_observer_for_testing =
      [&observed_budget](uint64_t budget) { observed_budget = budget; };
  database_ = Open(std::move(options));
  ASSERT_TRUE(database_);
  EXPECT_EQ(observed_budget, 1U);
}

TEST_F(KernelLifecycleTest, LiveSnapshotPinsCloseButReleaseAllowsRetryAndReopen) {
  database_ = Open();
  ASSERT_TRUE(database_);
  auto begun_snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(begun_snapshot.ok()) << begun_snapshot.status().ToString();
  std::optional<Snapshot> snapshot(
      std::move(begun_snapshot).ConsumeValueOrDie());
  EXPECT_TRUE(database_->Close().IsSnapshotPinned());

  snapshot.reset();
  EXPECT_TRUE(database_->Close().ok());
  EXPECT_TRUE(database_->BeginSnapshot().status().IsInvalidArgument());

  database_.reset();
  database_ = Open();
  ASSERT_TRUE(database_);
  auto transaction = Begin();
  ASSERT_TRUE(transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}), ValidTime{1}).ok());
  const auto committed = transaction->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
}

// B4 regression: a SnapshotPinned close must be non-destructive. Close used to
// stop the append pipeline and executor and detach the derived query state
// before it asked the store whether it could close. When the store reported
// SnapshotPinned, only `closing` was restored: `append_commit_stopping` stayed
// sticky and the executor/workers could never restart, so the database stayed
// open but rejected every Commit/CommitAsync with ShutdownInProgress forever.
// The store's closeable decision must become final before any irreversible
// teardown is performed.
TEST_F(KernelLifecycleTest, PinnedCloseLeavesCommitPipelineAndReadsUsable) {
  database_ = Open();
  ASSERT_TRUE(database_);
  auto begun_snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(begun_snapshot.ok()) << begun_snapshot.status().ToString();
  std::optional<Snapshot> held(std::move(begun_snapshot).ConsumeValueOrDie());

  const Status pinned = database_->Close();
  ASSERT_TRUE(pinned.IsSnapshotPinned()) << pinned.ToString();

  // Synchronous commit must still be accepted, durably published and visible.
  auto synchronous = Begin();
  ASSERT_TRUE(synchronous);
  ASSERT_TRUE(synchronous
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{91}}),
                           ValidTime{1})
                  .ok());
  const auto committed = synchronous->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_FALSE(committed.ValueOrDie().status.IsShutdownInProgress());

  // The async path (mailbox + executor) must still be running as well.
  auto asynchronous = Begin();
  ASSERT_TRUE(asynchronous);
  ASSERT_TRUE(asynchronous
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{92}}),
                           ValidTime{1})
                  .ok());
  auto async_handle = asynchronous->CommitAsync();
  ASSERT_TRUE(async_handle.ok()) << async_handle.status().ToString();
  const auto async_result = async_handle.ValueOrDie().Wait();
  ASSERT_TRUE(async_result.ok()) << async_result.status().ToString();
  EXPECT_EQ(async_result.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_FALSE(async_result.ValueOrDie().status.IsShutdownInProgress());

  // Reads keep working and see both commits.
  {
    auto visible = database_->BeginSnapshot();
    ASSERT_TRUE(visible.ok()) << visible.status().ToString();
    const auto exists_91 = visible.ValueOrDie().Exists(
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{91}}), ValidTime{1});
    ASSERT_TRUE(exists_91.ok()) << exists_91.status().ToString();
    EXPECT_TRUE(exists_91.ValueOrDie());
    const auto exists_92 = visible.ValueOrDie().Exists(
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{92}}), ValidTime{1});
    ASSERT_TRUE(exists_92.ok()) << exists_92.status().ToString();
    EXPECT_TRUE(exists_92.ValueOrDie());
  }

  // A retried close succeeds once the pin is released (and the pinned close did
  // not silently mark the database closed or closed the store underneath it).
  held.reset();
  const Status closed = database_->Close();
  ASSERT_TRUE(closed.ok()) << closed.ToString();
  EXPECT_TRUE(database_->BeginSnapshot().status().IsInvalidArgument());
}


// Regression: RefreshQueryStatistics checked closed/closing under
// Database::Impl::mutex but called FactStore::SchemaFingerprint() after
// releasing it. A concurrent successful Close could reset the property schema
// catalog between the store-handle copy and SchemasSnapshot(), dereferencing a
// null catalog. The fingerprint is now captured in the protected region and is
// itself a registered non-epoch operation, so Close waits for it.
TEST_F(KernelLifecycleTest, CloseRacingQueryStatisticsDoesNotReadReleasedCatalog) {
  std::atomic<bool> fingerprint_entered{false};
  std::atomic<bool> release_fingerprint{false};
  std::mutex refresh_thread_mutex;
  std::thread::id refresh_thread;
  database_ = Open(DatabaseOptions{
      .non_epoch_op_observer_for_testing = [&] {
        {
          std::lock_guard<std::mutex> lock(refresh_thread_mutex);
          if (refresh_thread != std::this_thread::get_id()) return;
        }
        fingerprint_entered.store(true, std::memory_order_release);
        while (!release_fingerprint.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
      }});
  ASSERT_TRUE(database_);

  // RefreshQueryStatistics requires a selected projection generation; build one
  // with the same canonical-validated property index path the public API uses.
  auto property = database_->RegisterProperty(
      PropertyDefinition{PropertyId{7}, 0, "country",
                         PropertyEntityKind::kVertex, PhysicalType::kString,
                         4096});
  ASSERT_TRUE(property.ok()) << property.status().ToString();
  auto prepared_transaction = database_->BeginTransaction();
  ASSERT_TRUE(prepared_transaction.ok())
      << prepared_transaction.status().ToString();
  ASSERT_TRUE(prepared_transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}),
                           ValidTime{0})
                  .ok());
  ASSERT_TRUE(prepared_transaction.ValueOrDie()
                  ->Set(PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}},
                                             PropertyId{7}),
                        ValidTime{0}, Value::String("CN"))
                  .ok());
  ASSERT_TRUE(prepared_transaction.ValueOrDie()->Commit().ok());
  auto index_refresh = database_->RefreshQueryIndexes(ValidTime{0});
  ASSERT_TRUE(index_refresh.ok()) << index_refresh.status().ToString();
  ASSERT_TRUE(index_refresh.ValueOrDie().Await().ok());

  StatusOr<QueryMaintenanceHandle> refresh =
      Status::InvalidArgument("query maintenance", "not executed");
  std::thread refresher([&] {
    {
      std::lock_guard<std::mutex> lock(refresh_thread_mutex);
      refresh_thread = std::this_thread::get_id();
    }
    refresh = database_->RefreshQueryStatistics();
  });
  const auto entered_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!fingerprint_entered.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < entered_deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(fingerprint_entered.load(std::memory_order_acquire));

  Status close_status;
  std::atomic<bool> close_finished{false};
  std::thread closer([&] {
    close_status = database_->Close();
    close_finished.store(true, std::memory_order_release);
  });
  // Bounded liveness check only: with the fingerprint held by the protected
  // region / close drain, Close cannot tear the catalog down yet.
  const auto drained_deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
  while (!close_finished.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < drained_deadline) {
    std::this_thread::yield();
  }
  EXPECT_FALSE(close_finished.load(std::memory_order_acquire))
      << "Close released the property catalog while RefreshQueryStatistics was "
         "fingerprinting it";

  release_fingerprint.store(true, std::memory_order_release);
  refresher.join();
  closer.join();

  EXPECT_TRUE(close_status.ok()) << close_status.ToString();
  EXPECT_TRUE(refresh.ok() || refresh.status().IsShutdownInProgress())
      << refresh.status().ToString();
}

}  // namespace
}  // namespace cedar
