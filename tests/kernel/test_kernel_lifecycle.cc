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

}  // namespace
}  // namespace cedar
