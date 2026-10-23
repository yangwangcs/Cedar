#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
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
#include "cedar/fact/meta_codec.h"
#include "query/runtime/graph_frontier.h"
#include "storage/rocks/rocksdb_config.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>

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

// Opens one already-created database with Cedar's column-family descriptors and
// reads one meta-CF value. Tests use this to observe the durable format marker
// and the raw sequence-record kind without going through the dual-read store.
std::optional<std::string> ReadRawMetaRecord(const std::string& path,
                                             const std::string& key) {
  FactStoreOptions store_options;
  store_options.path = path;
  std::shared_ptr<rocksdb::Cache> cache;
  rocksdb::Options options = internal::MakeRocksDbOptions(
      store_options, /*is_new_database=*/false, /*resolved=*/nullptr, &cache);
  options.create_if_missing = false;
  options.create_missing_column_families = false;
  // The default v1 close may already have flushed Cedar Parquet facts SSTs, so
  // install the same read resources FactStore::Open uses. The flush-suppression
  // flags keep this inspection from rewriting what the next Cedar open sees.
  options.atomic_flush = true;
  options.avoid_flush_during_shutdown = true;
  options.avoid_flush_during_recovery = true;
  auto page_metrics =
      std::make_shared<rocksdb::cedar_parquet::CanonicalPageCacheMetrics>();
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors =
      internal::MakeRocksDbColumnFamilyDescriptors(
          store_options, options, /*resolved=*/nullptr, {cache, page_metrics});
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> database;
  const rocksdb::Status opened =
      rocksdb::DB::Open(options, path, descriptors, &handles, &database);
  if (!opened.ok()) {
    ADD_FAILURE() << "raw open failed: " << opened.ToString();
    return std::nullopt;
  }
  if (handles.size() != 3U) {
    ADD_FAILURE() << "unexpected column family count: " << handles.size();
    for (rocksdb::ColumnFamilyHandle* handle : handles) {
      database->DestroyColumnFamilyHandle(handle);
    }
    return std::nullopt;
  }
  std::string value;
  const rocksdb::Status got = database->Get(rocksdb::ReadOptions(), handles[2],
                                            key, &value);
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    database->DestroyColumnFamilyHandle(handle);
  }
  database.reset();
  if (got.IsNotFound()) return std::nullopt;
  if (!got.ok()) {
    ADD_FAILURE() << "raw meta read failed: " << got.ToString();
    return std::nullopt;
  }
  return value;
}

// Deletes one facts-CF key through a vanilla RocksDB handle. The close and
// recovery flags mirror ReadRawMetaRecord so the raw edit does not flush the
// recovered facts memtable into a block-based SST the Cedar table factory would
// later reject.
bool DeleteRawFactRecord(const std::string& path, const std::string& key) {
  FactStoreOptions store_options;
  store_options.path = path;
  std::shared_ptr<rocksdb::Cache> cache;
  rocksdb::Options options = internal::MakeRocksDbOptions(
      store_options, /*is_new_database=*/false, /*resolved=*/nullptr, &cache);
  options.create_if_missing = false;
  options.create_missing_column_families = false;
  options.atomic_flush = true;
  options.avoid_flush_during_shutdown = true;
  options.avoid_flush_during_recovery = true;
  auto page_metrics =
      std::make_shared<rocksdb::cedar_parquet::CanonicalPageCacheMetrics>();
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors =
      internal::MakeRocksDbColumnFamilyDescriptors(
          store_options, options, /*resolved=*/nullptr, {cache, page_metrics});
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> database;
  const rocksdb::Status opened =
      rocksdb::DB::Open(options, path, descriptors, &handles, &database);
  if (!opened.ok()) {
    ADD_FAILURE() << "raw open failed: " << opened.ToString();
    return false;
  }
  if (handles.size() != 3U) {
    ADD_FAILURE() << "unexpected column family count: " << handles.size();
    for (rocksdb::ColumnFamilyHandle* handle : handles) {
      database->DestroyColumnFamilyHandle(handle);
    }
    return false;
  }
  const rocksdb::Status deleted =
      database->Delete(rocksdb::WriteOptions(), handles[1], key);
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    database->DestroyColumnFamilyHandle(handle);
  }
  database.reset();
  if (!deleted.ok()) {
    ADD_FAILURE() << "raw fact delete failed: " << deleted.ToString();
    return false;
  }
  return true;
}

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

// Regression (a3ecf3b): Database::Open rebuilds the adjacency cache from
// FactStore::ScanFamily(kEdgeIdentity). The bounded prefix-seek scan failed
// once a clean close had flushed the facts CF into Cedar Parquet SSTs, so the
// rebuild was silently skipped and every adjacency posting was missing. A
// reopened kernel must scan both families successfully and publish the
// reopened edge identity into a seekable adjacency index.
TEST_F(KernelLifecycleTest, ReopenScansFlushedFactsAndRebuildsAdjacency) {
  const EdgeIdentity identity{EdgeId{17}, VertexId{7}, VertexId{11}, 3};
  database_ = Open();
  ASSERT_TRUE(database_);
  auto seeded = Begin();
  ASSERT_TRUE(seeded);
  ASSERT_TRUE(seeded
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}),
                           ValidTime{10})
                  .ok());
  ASSERT_TRUE(seeded->Assert(identity, ValidTime{10}).ok());
  const auto committed = seeded->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  database_ = Open();  // Rebuilds the adjacency index from flushed facts.
  ASSERT_TRUE(database_);

  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  const auto index = snapshot.ValueOrDie().adjacency_index();
  ASSERT_NE(index, nullptr)
      << "Database::Open silently skipped the adjacency rebuild";
  const auto postings = index->Seek(
      {VertexRef{PartId{0}, VertexId{7}}}, ExpandDirection::kOut, std::nullopt,
      snapshot.ValueOrDie().commit_seq());
  ASSERT_TRUE(postings.ok()) << postings.status().ToString();
  ASSERT_EQ(postings.ValueOrDie().size(), 1U);
  EXPECT_EQ(postings.ValueOrDie().front(), identity);

  std::vector<FactEvent> vertices;
  const Status vertex_scan = snapshot.ValueOrDie().ScanFamily(
      FactFamily::kVertexState, [&vertices](const FactEvent& event) {
        vertices.push_back(event);
        return Status::OK();
      });
  ASSERT_TRUE(vertex_scan.ok()) << vertex_scan.ToString();
  ASSERT_EQ(vertices.size(), 1U);

  std::vector<FactEvent> identities;
  const Status identity_scan = snapshot.ValueOrDie().ScanFamily(
      FactFamily::kEdgeIdentity, [&identities](const FactEvent& event) {
        identities.push_back(event);
        return Status::OK();
      });
  ASSERT_TRUE(identity_scan.ok()) << identity_scan.ToString();
  ASSERT_EQ(identities.size(), 1U);
  ASSERT_TRUE(identities.front().edge_identity.has_value());
  EXPECT_EQ(*identities.front().edge_identity, identity);
}

// F12: the P3 adjacency rebuild is observable instead of silent. A healthy open
// reports the scanned identity records and elapsed time, and strict mode does
// not change a successful open.
TEST_F(KernelLifecycleTest, AdjacencyRebuildObserverReportsHealthyOpen) {
  const EdgeIdentity identity{EdgeId{17}, VertexId{7}, VertexId{11}, 3};
  database_ = Open();
  ASSERT_TRUE(database_);
  auto seeded = Begin();
  ASSERT_TRUE(seeded);
  ASSERT_TRUE(seeded
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}),
                           ValidTime{10})
                  .ok());
  ASSERT_TRUE(seeded->Assert(identity, ValidTime{10}).ok());
  ASSERT_TRUE(seeded->Commit().ok());
  ASSERT_TRUE(database_->Close().ok());
  database_.reset();

  std::vector<AdjacencyRebuildObservation> observations;
  DatabaseOptions options;
  options.strict_adjacency_rebuild = true;
  options.adjacency_rebuild_observer_for_testing =
      [&observations](const AdjacencyRebuildObservation& observation) {
        observations.push_back(observation);
      };
  database_ = Open(options);
  ASSERT_TRUE(database_);
  ASSERT_EQ(observations.size(), 1U);
  EXPECT_STREQ(observations.front().phase, "adjacency_rebuild");
  EXPECT_EQ(observations.front().records, 1U);
  EXPECT_FALSE(observations.front().failed);

  const auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  ASSERT_NE(snapshot.ValueOrDie().adjacency_index(), nullptr);
  const auto postings = snapshot.ValueOrDie().adjacency_index()->Seek(
      {VertexRef{PartId{0}, VertexId{7}}}, ExpandDirection::kOut, std::nullopt,
      snapshot.ValueOrDie().commit_seq());
  ASSERT_TRUE(postings.ok()) << postings.status().ToString();
  ASSERT_EQ(postings.ValueOrDie().size(), 1U);
  EXPECT_EQ(postings.ValueOrDie().front(), identity);
}

// F12: a tolerated rebuild failure is reported (records/failed) and leaves the
// default open successful with an empty adjacency index, preserving the
// historical non-strict behavior while removing the silence.
TEST_F(KernelLifecycleTest, AdjacencyRebuildFailureIsObservedNotFatalByDefault) {
  std::vector<AdjacencyRebuildObservation> observations;
  DatabaseOptions options;
  options.adjacency_rebuild_fault_injector_for_testing = [] {
    return Status::Corruption("adjacency rebuild",
                              "injected edge-identity scan failure");
  };
  options.adjacency_rebuild_observer_for_testing =
      [&observations](const AdjacencyRebuildObservation& observation) {
        observations.push_back(observation);
      };
  database_ = Open(options);
  ASSERT_TRUE(database_);
  ASSERT_EQ(observations.size(), 1U);
  EXPECT_STREQ(observations.front().phase, "adjacency_rebuild");
  EXPECT_TRUE(observations.front().failed);
  const auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  EXPECT_EQ(snapshot.ValueOrDie().adjacency_index(), nullptr);
}

// F12: strict_adjacency_rebuild propagates the ScanFamily/build failure to
// Database::Open and closes the store so the path can be opened again.
TEST_F(KernelLifecycleTest, StrictAdjacencyRebuildFailsOpenAndCleansUp) {
  std::vector<AdjacencyRebuildObservation> observations;
  DatabaseOptions failing;
  failing.path = path_;
  failing.strict_adjacency_rebuild = true;
  failing.adjacency_rebuild_fault_injector_for_testing = [] {
    return Status::Corruption("adjacency rebuild",
                              "injected edge-identity scan failure");
  };
  failing.adjacency_rebuild_observer_for_testing =
      [&observations](const AdjacencyRebuildObservation& observation) {
        observations.push_back(observation);
      };
  const auto opened = Database::Open(std::move(failing));
  ASSERT_FALSE(opened.ok());
  EXPECT_TRUE(opened.status().IsCorruption()) << opened.status().ToString();
  EXPECT_NE(opened.status().ToString().find("injected edge-identity scan failure"),
            std::string::npos);
  ASSERT_EQ(observations.size(), 1U);
  EXPECT_TRUE(observations.front().failed);

  // The failed open released the RocksDB lock; a healthy open works again.
  database_ = Open();
  ASSERT_TRUE(database_);
  const auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  EXPECT_NE(snapshot.ValueOrDie().adjacency_index(), nullptr)
      << "the re-opened database rebuilt the adjacency index";
}

// Task 10b: DatabaseOptions must forward the opt-in meta sequence format to the
// store. The durable marker and the first committed record are observed through
// a raw Cedar-CF reader; the reopened Database then proves the default v1
// option still reads the upgraded database (the marker stays authoritative).
TEST_F(KernelLifecycleTest, ForwardsMetaSequenceFormatV2ThroughDatabaseOptions) {
  DatabaseOptions options;
  options.meta_sequence_format = kMetaSequenceFormatV2;
  database_ = Open(options);
  ASSERT_TRUE(database_);
  auto transaction = Begin();
  ASSERT_TRUE(transaction);
  ASSERT_TRUE(transaction
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}),
                           ValidTime{1})
                  .ok());
  ASSERT_TRUE(transaction->Commit().ok());
  ASSERT_TRUE(database_->Close().ok());
  database_.reset();

  const auto marker = ReadRawMetaRecord(path_, EncodeSequenceFormatKey());
  ASSERT_TRUE(marker.has_value());
  const auto decoded_marker = DecodeSequenceFormat(*marker);
  ASSERT_TRUE(decoded_marker.ok()) << decoded_marker.status().ToString();
  EXPECT_EQ(decoded_marker.ValueOrDie(), kMetaSequenceFormatV2);

  const auto sequence_key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(sequence_key.ok()) << sequence_key.status().ToString();
  const auto record = ReadRawMetaRecord(path_, sequence_key.ValueOrDie());
  ASSERT_TRUE(record.has_value());
  ASSERT_GE(record->size(), 2U);
  EXPECT_EQ(static_cast<uint8_t>((*record)[1]), 9)  // kSequenceV2
      << "the first committed sequence record was not written in v2 format";

  // The marker is authoritative: a plain Database::Open with the default v1
  // option must dual-read the v2 record and expose the committed fact.
  database_ = Open();
  ASSERT_TRUE(database_);
  const auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto exists = snapshot.ValueOrDie().Exists(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{1});
  ASSERT_TRUE(exists.ok()) << exists.status().ToString();
  EXPECT_TRUE(exists.ValueOrDie());
}

// Task 10b: Database::Open validates the format before constructing the store,
// and the default remains byte-identical v1 (no marker, kind 7 records).
TEST_F(KernelLifecycleTest,
       RejectsUnsupportedMetaSequenceFormatAndKeepsV1Default) {
  DatabaseOptions invalid;
  invalid.path = path_;
  invalid.meta_sequence_format = 3;
  const auto rejected = Database::Open(std::move(invalid));
  ASSERT_FALSE(rejected.ok());
  EXPECT_TRUE(rejected.status().IsInvalidArgument())
      << rejected.status().ToString();
  EXPECT_NE(rejected.status().ToString().find("meta sequence format"),
            std::string::npos)
      << rejected.status().ToString();

  database_ = Open();
  ASSERT_TRUE(database_);
  auto transaction = Begin();
  ASSERT_TRUE(transaction);
  ASSERT_TRUE(transaction
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}),
                           ValidTime{1})
                  .ok());
  ASSERT_TRUE(transaction->Commit().ok());
  ASSERT_TRUE(database_->Close().ok());
  database_.reset();

  EXPECT_FALSE(ReadRawMetaRecord(path_, EncodeSequenceFormatKey()).has_value())
      << "the default v1 writer must not create a format marker";
  const auto sequence_key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(sequence_key.ok()) << sequence_key.status().ToString();
  const auto record = ReadRawMetaRecord(path_, sequence_key.ValueOrDie());
  ASSERT_TRUE(record.has_value());
  ASSERT_GE(record->size(), 2U);
  EXPECT_EQ(static_cast<uint8_t>((*record)[1]), 7)  // kSequence (v1)
      << "the default writer must keep producing v1 sequence records";
}

// F18: DatabaseOptions defaults to the bounded recent fact window, and
// deep_recovery_validation=true is the documented rollback. The raw delete
// removes one old fact that the bounded window skips; the structural sequence
// checks still pass, the kernel opens, and the missing fact is observed on
// read. The rollback open re-checks every fact and fails with the historical
// "missing fact" Corruption.
TEST_F(KernelLifecycleTest, DefaultReopenValidationIsBoundedAndDeepRollsBack) {
  DatabaseOptions bounded;
  bounded.recovery_validation_recent_sequences = 2;
  database_ = Open(bounded);
  ASSERT_TRUE(database_);
  constexpr uint64_t kTransactions = 3;
  for (uint64_t index = 1; index <= kTransactions; ++index) {
    auto transaction = Begin();
    ASSERT_TRUE(transaction);
    ASSERT_TRUE(
        transaction
            ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{index}}),
                     ValidTime{100 + index})
            .ok());
    const auto committed = transaction->Commit();
    ASSERT_TRUE(committed.ok()) << committed.status().ToString();
    ASSERT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  }
  ASSERT_TRUE(database_->Close().ok());
  database_.reset();

  const auto sequence_key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(sequence_key.ok()) << sequence_key.status().ToString();
  const auto sequence_record =
      ReadRawMetaRecord(path_, sequence_key.ValueOrDie());
  ASSERT_TRUE(sequence_record.has_value());
  const auto decoded = DecodeSequenceRecord(*sequence_record);
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  ASSERT_FALSE(decoded.ValueOrDie().fact_keys.empty());
  ASSERT_TRUE(
      DeleteRawFactRecord(path_, decoded.ValueOrDie().fact_keys.front()));

  // Default policy: visible=3 and K=2 put sequence 1 outside the fact window.
  database_ = Open(bounded);
  ASSERT_TRUE(database_)
      << "the default bounded reopen must not fail on an old missing fact";
  {
    const auto snapshot = database_->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto exists = snapshot.ValueOrDie().Exists(
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{101});
    ASSERT_TRUE(exists.ok()) << exists.status().ToString();
    EXPECT_FALSE(exists.ValueOrDie())
        << "the deleted old fact must be observed as missing on read";
  }
  ASSERT_TRUE(database_->Close().ok());
  database_.reset();

  // Rollback option: deep validation covers every fact from the oldest
  // readable boundary and therefore rejects the same database.
  DatabaseOptions rollback = bounded;
  rollback.deep_recovery_validation = true;
  rollback.path = path_;
  const auto rejected = Database::Open(std::move(rollback));
  ASSERT_FALSE(rejected.ok());
  EXPECT_TRUE(rejected.status().IsCorruption())
      << rejected.status().ToString();
  EXPECT_NE(rejected.status().ToString().find("missing fact"),
            std::string::npos)
      << rejected.status().ToString();
}

}  // namespace
}  // namespace cedar
