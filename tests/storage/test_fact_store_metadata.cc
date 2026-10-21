#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "storage/facts/fact_store.h"
#include "cedar/fact/meta_codec.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>

namespace cedar {
namespace {

PropertyDefinition Property(PropertyId property_id, std::string name,
                            PropertyEntityKind entity_kind,
                            PhysicalType physical_type) {
  return {property_id, 0, std::move(name), entity_kind, physical_type, 4096};
}

void PutMetaRecord(const std::string& path, const std::string& key,
                   const std::string& value) {
  rocksdb::Options options;
  options.create_if_missing = false;
  options.atomic_flush = true;
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName,
                           rocksdb::ColumnFamilyOptions(options));
  rocksdb::ColumnFamilyOptions facts_options(options);
  facts_options.prefix_extractor = std::shared_ptr<const rocksdb::SliceTransform>(
      rocksdb::NewFixedPrefixTransform(12));
  descriptors.emplace_back("facts", std::move(facts_options));
  descriptors.emplace_back("meta", rocksdb::ColumnFamilyOptions(options));
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> database;
  ASSERT_TRUE(rocksdb::DB::Open(options, path, descriptors, &handles, &database).ok());
  ASSERT_TRUE(database->Put(rocksdb::WriteOptions(), handles[2], key, value).ok());
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    database->DestroyColumnFamilyHandle(handle);
  }
}

StoreCommitBatch VertexStateBatch(TxnId txn_id, VertexId vertex_id,
                                  uint64_t valid_from) {
  return StoreCommitBatch{
      txn_id,
      valid_from,
      {{EntityFact::Vertex(VertexRef{PartId{0}, vertex_id}).ref(),
        ValidTime{valid_from}, FactOperation::kPut, 0, std::nullopt}},
      {},
      {},
      {},
      false};
}

class FactStoreMetadataTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_fact_store_metadata_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
    store_ = std::make_unique<FactStore>(FactStoreOptions{path_});
    ASSERT_TRUE(store_->Open().ok());
  }

  void TearDown() override {
    store_.reset();
    std::filesystem::remove_all(path_);
  }

  void Reopen() {
    ASSERT_TRUE(store_->Close().ok());
    ASSERT_TRUE(store_->Open().ok());
  }

  std::string path_;
  std::unique_ptr<FactStore> store_;
};

TEST_F(FactStoreMetadataTest, PersistsDefaultLeaseAndSkipsUnusedRangeAfterReopen) {
  const auto first = store_->LeaseIds(IdKind::kVertex, 0);
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  EXPECT_EQ(first.ValueOrDie().kind, IdKind::kVertex);
  EXPECT_EQ(first.ValueOrDie().first_id, 1U);
  EXPECT_EQ(first.ValueOrDie().count, 4096U);

  Reopen();
  const auto second = store_->LeaseIds(IdKind::kVertex, 1);
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  EXPECT_EQ(second.ValueOrDie().first_id, 4097U);
  EXPECT_EQ(second.ValueOrDie().count, 1U);
}

TEST_F(FactStoreMetadataTest, DefaultTransactionLeaseCoversSixtyFiveThousandIds) {
  char pattern[] = "/tmp/cedar_fact_store_default_txn_lease_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> extension_writes{0};
  FactStoreOptions options;
  options.path = path;
  options.transaction_lease_write_observer_for_testing = [&](bool) {
    extension_writes.fetch_add(1, std::memory_order_relaxed);
  };
  {
    FactStore store(options);
    ASSERT_TRUE(store.Open().ok());
    uint64_t last = 0;
    for (uint64_t index = 1; index <= 65536; ++index) {
      const auto id = store.AllocateTransactionId();
      ASSERT_TRUE(id.ok()) << id.status().ToString();
      last = id.ValueOrDie().value;
      ASSERT_EQ(last, index) << "index " << index;
    }
    EXPECT_EQ(last, 65536U);
    // The whole default lease is covered by the single initial extension.
    EXPECT_EQ(extension_writes.load(), 1U);
    const auto next = store.AllocateTransactionId();
    ASSERT_TRUE(next.ok()) << next.status().ToString();
    EXPECT_EQ(next.ValueOrDie().value, 65537U);
    EXPECT_EQ(extension_writes.load(), 2U);
    ASSERT_TRUE(store.Close().ok());
  }
  std::filesystem::remove_all(path);
}

TEST_F(FactStoreMetadataTest, InjectedSmallLeaseKeepsTransactionIdsContiguousAndDurable) {
  char pattern[] = "/tmp/cedar_fact_store_lease_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> extension_writes{0};
  std::atomic<uint32_t> unsynchronized_writes{0};
  FactStoreOptions options;
  options.path = path;
  options.transaction_id_lease_size_for_testing = 4;
  options.transaction_lease_write_observer_for_testing = [&](bool sync) {
    extension_writes.fetch_add(1, std::memory_order_relaxed);
    if (!sync) {
      unsynchronized_writes.fetch_add(1, std::memory_order_relaxed);
    }
  };
  {
    FactStore store(options);
    ASSERT_TRUE(store.Open().ok());
    std::vector<uint64_t> ids;
    for (uint32_t index = 0; index < 17; ++index) {
      const auto id = store.AllocateTransactionId();
      ASSERT_TRUE(id.ok()) << id.status().ToString();
      ids.push_back(id.ValueOrDie().value);
    }
    for (uint32_t index = 0; index < ids.size(); ++index) {
      EXPECT_EQ(ids[index], index + 1) << "index " << index;
    }
    // IDs 1..17 exhaust five injected leases: [1,5) [5,9) [9,13) [13,17) [17,21).
    EXPECT_EQ(extension_writes.load(), 5U);
    EXPECT_EQ(unsynchronized_writes.load(), 0U);
    ASSERT_TRUE(store.Close().ok());
  }
  {
    // Reopen continues after the durably extended allocator (21), proving the
    // extension writes persisted before any ID from those leases was used.
    FactStore store(options);
    ASSERT_TRUE(store.Open().ok());
    const auto id = store.AllocateTransactionId();
    ASSERT_TRUE(id.ok()) << id.status().ToString();
    EXPECT_EQ(id.ValueOrDie().value, 21U);
    EXPECT_EQ(extension_writes.load(), 6U);
    ASSERT_TRUE(store.Close().ok());
  }
  std::filesystem::remove_all(path);
}

TEST_F(FactStoreMetadataTest, ConcurrentAllocationAcrossLeaseExtensionsIsUnique) {
  char pattern[] = "/tmp/cedar_fact_store_lease_concurrent_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> extension_writes{0};
  std::atomic<uint32_t> unsynchronized_writes{0};
  FactStoreOptions options;
  options.path = path;
  options.transaction_id_lease_size_for_testing = 4;
  options.transaction_lease_write_observer_for_testing = [&](bool sync) {
    extension_writes.fetch_add(1, std::memory_order_relaxed);
    if (!sync) {
      unsynchronized_writes.fetch_add(1, std::memory_order_relaxed);
    }
  };
  FactStore store(options);
  ASSERT_TRUE(store.Open().ok());
  constexpr uint32_t kThreads = 4;
  constexpr uint32_t kPerThread = 25;
  std::vector<std::thread> threads;
  std::vector<std::vector<uint64_t>> per_thread(kThreads);
  std::atomic<uint32_t> failures{0};
  for (uint32_t thread = 0; thread < kThreads; ++thread) {
    threads.emplace_back([&, thread] {
      per_thread[thread].reserve(kPerThread);
      for (uint32_t index = 0; index < kPerThread; ++index) {
        const auto id = store.AllocateTransactionId();
        if (!id.ok()) {
          failures.fetch_add(1, std::memory_order_relaxed);
          return;
        }
        per_thread[thread].push_back(id.ValueOrDie().value);
      }
    });
  }
  for (std::thread& thread : threads) thread.join();
  EXPECT_EQ(failures.load(), 0U);

  std::vector<uint64_t> ids;
  ids.reserve(kThreads * kPerThread);
  for (const auto& thread_ids : per_thread) {
    ids.insert(ids.end(), thread_ids.begin(), thread_ids.end());
  }
  std::sort(ids.begin(), ids.end());
  ASSERT_EQ(ids.size(), static_cast<size_t>(kThreads * kPerThread));
  for (uint32_t index = 0; index < ids.size(); ++index) {
    ASSERT_EQ(ids[index], index + 1) << "index " << index;
  }
  // 100 allocations with a 4-ID lease extend exactly 25 times, always synced.
  EXPECT_EQ(extension_writes.load(), 25U);
  EXPECT_EQ(unsynchronized_writes.load(), 0U);
  ASSERT_TRUE(store.Close().ok());
  std::filesystem::remove_all(path);
}

TEST_F(FactStoreMetadataTest, FastPathTransactionAllocationDoesNotWaitForPublisherMutex) {
  char pattern[] = "/tmp/cedar_fact_store_lease_fastpath_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool writer_entered = false;
  bool release_writer = false;
  FactStoreOptions options;
  options.path = path;
  options.transaction_id_lease_size_for_testing = 8;
  options.commit_prewrite_fault_injector_for_testing = [&] {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    writer_entered = true;
    prewrite_cv.notify_all();
    prewrite_cv.wait(lock, [&] { return release_writer; });
    return Status::OK();
  };
  FactStore store(options);
  ASSERT_TRUE(store.Open().ok());

  // Extend the lease before the writer parks, so the fast path has IDs left.
  const auto first = store.AllocateTransactionId();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  EXPECT_EQ(first.ValueOrDie().value, 1U);

  std::future<StatusOr<StoreCommitResult>> committed = std::async(
      std::launch::async,
      [&] { return store.Commit(VertexStateBatch(TxnId{1000}, VertexId{7}, 10)); });
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    ASSERT_TRUE(prewrite_cv.wait_for(lock, std::chrono::seconds(2),
                                     [&] { return writer_entered; }));
  }

  // Bound the fast-path call: a lock regression must fail this test instead of
  // hanging it. The parked writer is released on both paths so neither future
  // outlives the store.
  std::future<StatusOr<TxnId>> second = std::async(
      std::launch::async, [&] { return store.AllocateTransactionId(); });
  const std::future_status second_status =
      second.wait_for(std::chrono::seconds(2));
  {
    std::lock_guard<std::mutex> lock(prewrite_mutex);
    release_writer = true;
  }
  prewrite_cv.notify_all();
  ASSERT_EQ(second_status, std::future_status::ready)
      << "fast-path allocation waited behind the parked publisher";
  const auto second_id = second.get();
  ASSERT_TRUE(second_id.ok()) << second_id.status().ToString();
  EXPECT_EQ(second_id.ValueOrDie().value, 2U);
  ASSERT_TRUE(committed.get().ok());
  ASSERT_TRUE(store.Close().ok());
  std::filesystem::remove_all(path);
}

TEST_F(FactStoreMetadataTest, KeepsVertexAndEdgeLeasesIndependent) {
  const auto vertices = store_->LeaseIds(IdKind::kVertex, 3);
  const auto edges = store_->LeaseIds(IdKind::kEdge, 2);
  ASSERT_TRUE(vertices.ok()) << vertices.status().ToString();
  ASSERT_TRUE(edges.ok()) << edges.status().ToString();
  EXPECT_EQ(vertices.ValueOrDie().first_id, 1U);
  EXPECT_EQ(edges.ValueOrDie().first_id, 1U);
  EXPECT_EQ(edges.ValueOrDie().kind, IdKind::kEdge);
}

TEST_F(FactStoreMetadataTest, RejectsTransactionLeaseWhenIdSpaceIsExhausted) {
  // The transaction lease-extension guard (rocks_adapter.cc:5057-5059) rejects
  // a lease whose end would wrap uint64_t. The existing exhaustion test covers
  // LeaseIds only, so pin the transaction path here: no ID may be handed out
  // and no durable lease-extension write may happen.
  ASSERT_TRUE(store_->Close().ok());
  const auto encoded = EncodeIdAllocatorState(IdAllocatorState{
      IdKind::kTransaction, std::numeric_limits<uint64_t>::max()});
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  PutMetaRecord(path_, EncodeAllocatorMetaKey(IdKind::kTransaction),
                encoded.ValueOrDie());

  std::atomic<uint32_t> extension_writes{0};
  FactStoreOptions options{path_};
  options.transaction_lease_write_observer_for_testing = [&](bool) {
    extension_writes.fetch_add(1, std::memory_order_relaxed);
  };
  store_ = std::make_unique<FactStore>(options);
  ASSERT_TRUE(store_->Open().ok());

  const auto id = store_->AllocateTransactionId();
  EXPECT_TRUE(id.status().IsResourceExhausted()) << id.status().ToString();
  EXPECT_EQ(extension_writes.load(), 0U);
}

TEST_F(FactStoreMetadataTest, RejectsLeaseWhenAllocatorIsExhausted) {
  ASSERT_TRUE(store_->Close().ok());
  const auto encoded = EncodeIdAllocatorState(
      IdAllocatorState{IdKind::kVertex, std::numeric_limits<uint64_t>::max()});
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  PutMetaRecord(path_, EncodeAllocatorMetaKey(IdKind::kVertex), encoded.ValueOrDie());
  ASSERT_TRUE(store_->Open().ok());

  const auto lease = store_->LeaseIds(IdKind::kVertex, 1);
  EXPECT_TRUE(lease.status().IsResourceExhausted()) << lease.status().ToString();
}

TEST_F(FactStoreMetadataTest, RegistersPropertiesIdempotentlyAndPreservesEpochs) {
  const PropertyDefinition requested =
      Property(PropertyId{9}, "name", PropertyEntityKind::kVertex,
               PhysicalType::kString);
  const auto first = store_->RegisterProperty(requested);
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  EXPECT_EQ(first.ValueOrDie().schema_epoch, 1U);

  const auto repeated = store_->RegisterProperty(requested);
  ASSERT_TRUE(repeated.ok()) << repeated.status().ToString();
  EXPECT_EQ(repeated.ValueOrDie(), first.ValueOrDie());

  const auto changed = store_->RegisterProperty(
      Property(PropertyId{9}, "display_name", PropertyEntityKind::kVertex,
               PhysicalType::kString));
  ASSERT_TRUE(changed.ok()) << changed.status().ToString();
  EXPECT_EQ(changed.ValueOrDie().schema_epoch, 2U);

  const auto old = store_->LookupProperty(PropertyId{9}, 1);
  const auto latest = store_->LookupProperty(PropertyId{9});
  ASSERT_TRUE(old.ok()) << old.status().ToString();
  ASSERT_TRUE(latest.ok()) << latest.status().ToString();
  ASSERT_TRUE(old.ValueOrDie().has_value());
  ASSERT_TRUE(latest.ValueOrDie().has_value());
  EXPECT_EQ(old.ValueOrDie()->name, "name");
  EXPECT_EQ(latest.ValueOrDie()->schema_epoch, 2U);

  Reopen();
  const auto reopened_old = store_->LookupProperty(PropertyId{9}, 1);
  ASSERT_TRUE(reopened_old.ok()) << reopened_old.status().ToString();
  ASSERT_TRUE(reopened_old.ValueOrDie().has_value());
  EXPECT_EQ(reopened_old.ValueOrDie()->name, "name");
}

TEST_F(FactStoreMetadataTest,
       RefreshesLiveCurrentEpochSchemasWhenPropertyIsRegistered) {
  // Regression: SnapshotEpoch::schemas used to be captured once at epoch
  // creation. A runtime RegisterProperty does not advance the visible sequence,
  // so every later snapshot at that sequence reused the stale catalog and a
  // lookup of the new property returned nullopt ("property is not registered").
  ASSERT_TRUE(store_->RegisterProperty(
                        Property(PropertyId{9}, "name", PropertyEntityKind::kVertex,
                                 PhysicalType::kString))
                  .ok());
  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  ASSERT_TRUE(store_->RegisterProperty(
                        Property(PropertyId{10}, "display_name",
                                 PropertyEntityKind::kVertex,
                                 PhysicalType::kString))
                  .ok());
  const auto added = store_->LookupProperty(snapshot.ValueOrDie(), PropertyId{10});
  ASSERT_TRUE(added.ok()) << added.status().ToString();
  ASSERT_TRUE(added.ValueOrDie().has_value());
  EXPECT_EQ(added.ValueOrDie()->name, "display_name");
  EXPECT_EQ(added.ValueOrDie()->schema_epoch, 1U);

  // A new schema epoch of an already-known property is refreshed as well, and
  // the previous epoch stays resolvable through the same catalog.
  ASSERT_TRUE(store_->RegisterProperty(
                        Property(PropertyId{10}, "display_name_v2",
                                 PropertyEntityKind::kVertex,
                                 PhysicalType::kString))
                  .ok());
  const auto latest = store_->LookupProperty(snapshot.ValueOrDie(), PropertyId{10});
  ASSERT_TRUE(latest.ok()) << latest.status().ToString();
  ASSERT_TRUE(latest.ValueOrDie().has_value());
  EXPECT_EQ(latest.ValueOrDie()->schema_epoch, 2U);
  const auto previous =
      store_->LookupProperty(snapshot.ValueOrDie(), PropertyId{10}, 1);
  ASSERT_TRUE(previous.ok()) << previous.status().ToString();
  ASSERT_TRUE(previous.ValueOrDie().has_value());
  EXPECT_EQ(previous.ValueOrDie()->name, "display_name");
}

TEST_F(FactStoreMetadataTest,
       RefreshesRetiredAndAsOfEpochSchemasWhenPropertyIsRegistered) {
  // As_of and retired epochs are registered in the same live-epoch registry, so
  // a property publish must repoint them too; otherwise an as_of reader opened
  // before the registration would keep answering from the pre-publish catalog.
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
  const auto retired = store_->BeginSnapshot();
  ASSERT_TRUE(retired.ok()) << retired.status().ToString();
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{2}, VertexId{2}, 11)).ok());
  const auto as_of = store_->BeginSnapshot(SnapshotOptions{CommitSeq{1}});
  ASSERT_TRUE(as_of.ok()) << as_of.status().ToString();

  ASSERT_TRUE(store_->RegisterProperty(
                        Property(PropertyId{31}, "late", PropertyEntityKind::kVertex,
                                 PhysicalType::kString))
                  .ok());

  const auto from_retired =
      store_->LookupProperty(retired.ValueOrDie(), PropertyId{31});
  ASSERT_TRUE(from_retired.ok()) << from_retired.status().ToString();
  ASSERT_TRUE(from_retired.ValueOrDie().has_value());
  EXPECT_EQ(from_retired.ValueOrDie()->name, "late");
  const auto from_as_of = store_->LookupProperty(as_of.ValueOrDie(), PropertyId{31});
  ASSERT_TRUE(from_as_of.ok()) << from_as_of.status().ToString();
  ASSERT_TRUE(from_as_of.ValueOrDie().has_value());
  EXPECT_EQ(from_as_of.ValueOrDie()->name, "late");
}

TEST_F(FactStoreMetadataTest,
       RefreshesCachedAsOfEpochSchemasWhenPropertyIsRegistered) {
  // The 8-slot as_of cache keeps its epoch registered as retired-from-birth, so
  // a property publish must repoint the cached epoch too; otherwise a cache hit
  // after RegisterProperty resolves against the pre-registration catalog.
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{2}, VertexId{2}, 11)).ok());

  int created_epochs = 0;
  FactStoreOptions options{path_};
  options.snapshot_epoch_created_observer_for_testing = [&created_epochs] {
    ++created_epochs;
  };
  ASSERT_TRUE(store_->Close().ok());
  store_ = std::make_unique<FactStore>(options);
  ASSERT_TRUE(store_->Open().ok());

  auto first = store_->BeginCurrentSnapshotAt(CommitSeq{1});
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  std::optional<StoreSnapshot> first_handle(
      std::move(first).ConsumeValueOrDie());
  ASSERT_EQ(created_epochs, 1);

  ASSERT_TRUE(store_->RegisterProperty(
                        Property(PropertyId{41}, "cached_late",
                                 PropertyEntityKind::kVertex,
                                 PhysicalType::kString))
                  .ok());

  // The second exact-sequence pin must be a cache hit (no new epoch) and must
  // resolve the property registered after the cached epoch was created.
  auto cached = store_->BeginCurrentSnapshotAt(CommitSeq{1});
  ASSERT_TRUE(cached.ok()) << cached.status().ToString();
  std::optional<StoreSnapshot> cached_handle(
      std::move(cached).ConsumeValueOrDie());
  EXPECT_EQ(created_epochs, 1);
  const auto from_cached =
      store_->LookupProperty(*cached_handle, PropertyId{41});
  ASSERT_TRUE(from_cached.ok()) << from_cached.status().ToString();
  ASSERT_TRUE(from_cached.ValueOrDie().has_value());
  EXPECT_EQ(from_cached.ValueOrDie()->name, "cached_late");

  // The handle opened before the registration sees the refreshed catalog too:
  // refresh is in place for every live epoch, not a new-epoch-only behavior.
  const auto from_first =
      store_->LookupProperty(*first_handle, PropertyId{41});
  ASSERT_TRUE(from_first.ok()) << from_first.status().ToString();
  ASSERT_TRUE(from_first.ValueOrDie().has_value());
  EXPECT_EQ(from_first.ValueOrDie()->name, "cached_late");

  cached_handle.reset();
  first_handle.reset();
  ASSERT_TRUE(store_->Close().ok());
  store_.reset();
}

TEST_F(FactStoreMetadataTest,
       PropertyRegistrationKeepsVisibleSequenceAndExpirySemantics) {
  // Requirement: refreshing live epoch schemas must not change snapshot
  // visibility. RegisterProperty writes metadata only, so the visible sequence
  // must not move and expiry must still follow the retention boundary.
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{2}, VertexId{2}, 11)).ok());
  {
    const auto before = store_->BeginSnapshot();
    ASSERT_TRUE(before.ok()) << before.status().ToString();
    EXPECT_EQ(before.ValueOrDie().commit_seq().value, 2U);
  }
  {
    const auto as_of = store_->BeginSnapshot(SnapshotOptions{CommitSeq{1}});
    ASSERT_TRUE(as_of.ok()) << as_of.status().ToString();
    EXPECT_EQ(as_of.ValueOrDie().commit_seq().value, 1U);
  }

  ASSERT_TRUE(store_->RegisterProperty(
                        Property(PropertyId{21}, "kept", PropertyEntityKind::kVertex,
                                 PhysicalType::kString))
                  .ok());

  {
    const auto after = store_->BeginSnapshot();
    ASSERT_TRUE(after.ok()) << after.status().ToString();
    EXPECT_EQ(after.ValueOrDie().commit_seq().value, 2U);
  }
  EXPECT_TRUE(store_->BeginSnapshot(SnapshotOptions{CommitSeq{3}})
                  .status()
                  .IsInvalidArgument());

  ASSERT_TRUE(store_->Vacuum(CommitSeq{2}).ok());
  EXPECT_TRUE(store_->BeginSnapshot(SnapshotOptions{CommitSeq{1}})
                  .status()
                  .IsSnapshotExpired());
  const auto retained = store_->BeginSnapshot(SnapshotOptions{CommitSeq{2}});
  ASSERT_TRUE(retained.ok()) << retained.status().ToString();
  EXPECT_EQ(retained.ValueOrDie().commit_seq().value, 2U);
}

TEST_F(FactStoreMetadataTest, HidesEdgeIdentityCreatedAfterLogicalSnapshot) {
  const EdgeIdentity identity{EdgeId{9}, VertexId{1}, VertexId{2}, 7};
  const StoreCommitBatch batch{
      TxnId{1},
      1,
      {{EntityFact::Edge(identity.edge_ref()).ref(), ValidTime{10},
        FactOperation::kPut, 0, std::nullopt}},
      {identity}};
  ASSERT_TRUE(store_->Commit(batch).ok());

  const auto before_creation =
      store_->BeginSnapshot(SnapshotOptions{CommitSeq{0}});
  ASSERT_TRUE(before_creation.ok()) << before_creation.status().ToString();
  const auto hidden = store_->LookupEdgeIdentity(before_creation.ValueOrDie(),
                                                 identity.edge_ref());
  ASSERT_TRUE(hidden.ok()) << hidden.status().ToString();
  EXPECT_FALSE(hidden.ValueOrDie().has_value());

  const auto after_creation =
      store_->BeginSnapshot(SnapshotOptions{CommitSeq{1}});
  ASSERT_TRUE(after_creation.ok()) << after_creation.status().ToString();
  const auto visible = store_->LookupEdgeIdentity(after_creation.ValueOrDie(),
                                                  identity.edge_ref());
  ASSERT_TRUE(visible.ok()) << visible.status().ToString();
  EXPECT_EQ(visible.ValueOrDie(), std::optional<EdgeIdentity>{identity});
}

TEST_F(FactStoreMetadataTest,
       RejectsTypeChangeForPropertyNameAcrossNonAdjacentEpochs) {
  ASSERT_TRUE(store_->RegisterProperty(
                        Property(PropertyId{9}, "name", PropertyEntityKind::kVertex,
                                 PhysicalType::kString))
                  .ok());
  ASSERT_TRUE(store_->RegisterProperty(
                        Property(PropertyId{9}, "display_name",
                                 PropertyEntityKind::kVertex,
                                 PhysicalType::kString))
                  .ok());

  const auto incompatible = store_->RegisterProperty(
      Property(PropertyId{9}, "name", PropertyEntityKind::kVertex,
               PhysicalType::kInt64));
  EXPECT_TRUE(incompatible.status().IsSchemaMismatch())
      << incompatible.status().ToString();
}

TEST_F(FactStoreMetadataTest, RejectsCorruptedAllocatorAndSchemaMetadataOnReopen) {
  ASSERT_TRUE(store_->Close().ok());
  PutMetaRecord(path_, EncodeAllocatorMetaKey(IdKind::kVertex), "corrupt");
  const Status corrupted_allocator = store_->Open();
  EXPECT_TRUE(corrupted_allocator.IsCorruption()) << corrupted_allocator.ToString();

  const auto valid_allocator =
      EncodeIdAllocatorState(IdAllocatorState{IdKind::kVertex, 1});
  ASSERT_TRUE(valid_allocator.ok()) << valid_allocator.status().ToString();
  PutMetaRecord(path_, EncodeAllocatorMetaKey(IdKind::kVertex),
                valid_allocator.ValueOrDie());
  ASSERT_TRUE(store_->Open().ok());
  ASSERT_TRUE(store_->RegisterProperty(
                        Property(PropertyId{9}, "name", PropertyEntityKind::kVertex,
                                 PhysicalType::kString))
                  .ok());
  ASSERT_TRUE(store_->Close().ok());
  PutMetaRecord(path_, EncodeSchemaMetaKey(PropertyId{9}, 1).ValueOrDie(), "corrupt");
  const Status corrupted_schema = store_->Open();
  EXPECT_TRUE(corrupted_schema.IsCorruption()) << corrupted_schema.ToString();
}

}  // namespace
}  // namespace cedar
