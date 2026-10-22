#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "storage/facts/fact_store.h"
#include "storage/rocks/rocksdb_config.h"
#include "cedar/core/crc32c.h"
#include "cedar/database.h"
#include "cedar/fact/fact_codec.h"
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

// Raw meta-CF mutation helpers for reopen-corruption tests. They open the
// database with vanilla RocksDB options, so the embedded Cedar Parquet table
// factory is not installed for the facts CF here; `avoid_flush_during_shutdown`
// keeps this close from flushing a recovered facts memtable into a block-based
// SST that the Cedar factory would later reject. The meta write still reaches
// the WAL, so the next Cedar open replays it.
void PutMetaRecord(const std::string& path, const std::string& key,
                   const std::string& value) {
  rocksdb::Options options;
  options.create_if_missing = false;
  options.atomic_flush = true;
  options.avoid_flush_during_shutdown = true;
  options.avoid_flush_during_recovery = true;
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

void DeleteMetaRecord(const std::string& path, const std::string& key) {
  rocksdb::Options options;
  options.create_if_missing = false;
  options.atomic_flush = true;
  options.avoid_flush_during_shutdown = true;
  options.avoid_flush_during_recovery = true;
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
  ASSERT_TRUE(database->Delete(rocksdb::WriteOptions(), handles[2], key).ok());
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    database->DestroyColumnFamilyHandle(handle);
  }
}

// Raw DB access for fact mutations in reopen-corruption tests. Same vanilla
// options and column-family layout as the meta helpers above; the prefix
// extractor only affects iteration, not point deletes.
struct RawFactDatabase {
  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;

  ~RawFactDatabase() {
    for (rocksdb::ColumnFamilyHandle* handle : handles) {
      if (database) database->DestroyColumnFamilyHandle(handle);
    }
  }
};

// Raw DB access for fact mutations and meta reads in reopen tests. Opening with
// a vanilla table factory works while every fact row still lives in the WAL or
// memtable, but a facts SST flushed by Cedar uses the Cedar Parquet table
// factory, so mirror Cedar's column-family options here. The flush-suppression
// flags keep this inspection from rewriting the files the next Cedar open sees.
bool OpenRawFactDatabase(const std::string& path, RawFactDatabase* out) {
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
  const rocksdb::Status opened = rocksdb::DB::Open(options, path, descriptors,
                                                   &out->handles,
                                                   &out->database);
  if (!opened.ok()) {
    ADD_FAILURE() << "raw open failed: " << opened.ToString();
    return false;
  }
  return true;
}

std::string ReadFactKeyFromSequenceRecord(const std::string& path,
                                          CommitSeq commit_seq) {
  RawFactDatabase raw;
  if (!OpenRawFactDatabase(path, &raw)) {
    ADD_FAILURE() << "failed to open raw database";
    return {};
  }
  const auto meta_key = EncodeSequenceMetaKey(commit_seq);
  if (!meta_key.ok()) {
    ADD_FAILURE() << meta_key.status().ToString();
    return {};
  }
  std::string encoded;
  const rocksdb::Status got = raw.database->Get(
      rocksdb::ReadOptions(), raw.handles[2], meta_key.ValueOrDie(), &encoded);
  if (!got.ok()) {
    ADD_FAILURE() << got.ToString();
    return {};
  }
  const auto record = DecodeSequenceRecord(encoded);
  if (!record.ok() || record.ValueOrDie().fact_keys.empty()) {
    ADD_FAILURE() << "sequence record has no fact keys";
    return {};
  }
  return record.ValueOrDie().fact_keys.front();
}

void DeleteFactRecord(const std::string& path, const std::string& key) {
  RawFactDatabase raw;
  if (!OpenRawFactDatabase(path, &raw)) {
    ADD_FAILURE() << "failed to open raw database";
    return;
  }
  ASSERT_TRUE(
      raw.database->Delete(rocksdb::WriteOptions(), raw.handles[1], key).ok());
}

StoreCommitBatch VertexStateBatch(TxnId txn_id, VertexId vertex_id,
                                  uint64_t valid_from) {  return StoreCommitBatch{
      txn_id,
      valid_from,
      {{EntityFact::Vertex(VertexRef{PartId{0}, vertex_id}).ref(),
        ValidTime{valid_from}, FactOperation::kPut, 0, std::nullopt}},
      {},
      {},
      {},
      false};
}

// Commits `fact_count` distinct vertex identities in one transaction. Reopen
// validation consumes the sequence record once and then cross-checks one fact
// per identity, which lets a counter test distinguish sequence records from
// fact reads (and a bounded window from the full history).
StoreCommitBatch VertexFactsBatch(TxnId txn_id, uint64_t first_vertex,
                                  uint64_t fact_count, uint64_t valid_from) {
  StoreCommitBatch batch{txn_id, valid_from, {}, {}, {}, {}, false};
  batch.mutations.reserve(fact_count);
  for (uint64_t index = 0; index < fact_count; ++index) {
    batch.mutations.push_back(PendingFactMutation{
        EntityFact::Vertex(VertexRef{PartId{0}, first_vertex + index}).ref(),
        ValidTime{valid_from}, FactOperation::kPut, 0, std::nullopt});
  }
  return batch;
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

TEST_F(FactStoreMetadataTest, ObservedEdgeStateLookupMatchesScanningLookup) {
  // F7: callers that already read the visible edge-state fact (the edge
  // visibility walk) may skip the duplicate edge-state scan in
  // LookupEdgeIdentity. The observed-state overload must resolve the same
  // identity, and it must reject an observation that is not the edge-state PUT
  // it claims to be.
  const EdgeIdentity identity{EdgeId{9}, VertexId{1}, VertexId{2}, 7};
  const StoreCommitBatch batch{
      TxnId{1},
      1,
      {{EntityFact::Edge(identity.edge_ref()).ref(), ValidTime{10},
        FactOperation::kPut, 0, std::nullopt}},
      {identity}};
  ASSERT_TRUE(store_->Commit(batch).ok());

  const auto snapshot = store_->BeginSnapshot(SnapshotOptions{CommitSeq{1}});
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto edge_state = store_->Read(
      snapshot.ValueOrDie(), EntityFact::Edge(identity.edge_ref()).ref(),
      ValidTime{10});
  ASSERT_TRUE(edge_state.ok()) << edge_state.status().ToString();
  ASSERT_TRUE(edge_state.ValueOrDie().has_value());

  const auto scanned = store_->LookupEdgeIdentity(snapshot.ValueOrDie(),
                                                  identity.edge_ref());
  ASSERT_TRUE(scanned.ok()) << scanned.status().ToString();
  const auto observed = store_->LookupEdgeIdentity(
      snapshot.ValueOrDie(), identity.edge_ref(), *edge_state.ValueOrDie());
  ASSERT_TRUE(observed.ok()) << observed.status().ToString();
  EXPECT_EQ(observed.ValueOrDie(), scanned.ValueOrDie());
  EXPECT_EQ(observed.ValueOrDie(), std::optional<EdgeIdentity>{identity});

  // A visible vertex fact is not the promised edge-state observation.
  FactEvent mismatched = *edge_state.ValueOrDie();
  mismatched.ref =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref();
  const auto rejected = store_->LookupEdgeIdentity(
      snapshot.ValueOrDie(), identity.edge_ref(), mismatched);
  EXPECT_TRUE(rejected.status().IsInvalidArgument())
      << rejected.status().ToString();

  // An observation above the snapshot sequence cannot be the visible edge-state
  // PUT Read returned (Read filters those out), so the cheap commit-visibility
  // guard must reject it instead of trusting the caller.
  FactEvent too_new = *edge_state.ValueOrDie();
  too_new.commit_seq = CommitSeq{snapshot.ValueOrDie().commit_seq().value + 1};
  const auto rejected_newer = store_->LookupEdgeIdentity(
      snapshot.ValueOrDie(), identity.edge_ref(), too_new);
  EXPECT_TRUE(rejected_newer.status().IsInvalidArgument())
      << rejected_newer.status().ToString();
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

// S1: the reopen scan already holds every committed sequence record in its
// meta-CF iterator, so validation must consume that value instead of issuing a
// second meta Get for the same key (42.5M duplicate Gets per reopen on the
// production dataset). The test hook reports the record source; the count must
// equal the committed sequence count and every source must be the iterator.
TEST_F(FactStoreMetadataTest,
       ReopenValidationReadsEachSequenceRecordFromTheIterator) {
  for (uint64_t index = 1; index <= 3; ++index) {
    ASSERT_TRUE(store_
                    ->Commit(VertexStateBatch(TxnId{index}, VertexId{index},
                                              10 + index))
                    .ok());
  }
  ASSERT_TRUE(store_->Close().ok());
  std::vector<bool> read_sources;
  FactStoreOptions options{path_};
  options.sequence_validation_read_source_observer_for_testing =
      [&read_sources](bool from_iterator) {
        read_sources.push_back(from_iterator);
      };
  store_ = std::make_unique<FactStore>(options);
  ASSERT_TRUE(store_->Open().ok());
  ASSERT_EQ(read_sources.size(), 3U);
  EXPECT_TRUE(std::all_of(read_sources.begin(), read_sources.end(),
                          [](bool from_iterator) { return from_iterator; }))
      << "reopen validation re-read a sequence record the iterator already held";
}

// S1 must not lose a failure: a sequence record whose stored value disagrees
// with its key is still rejected with the same corruption the per-key Get
// produced, even though the validator now trusts the iterator's value.
TEST_F(FactStoreMetadataTest,
       ReopenValidationRejectsSequenceRecordWithWrongCommitSequence) {
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{2}, VertexId{2}, 11)).ok());
  ASSERT_TRUE(store_->Close().ok());
  const FactRef first =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref();
  const auto mismatched = EncodeSequenceRecord(SequenceRecord{
      CommitSeq{2}, TxnId{1}, 10,
      {EncodeFactKey(first, ValidTime{10}, CommitSeq{2})}});
  ASSERT_TRUE(mismatched.ok()) << mismatched.status().ToString();
  const auto key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(key.ok()) << key.status().ToString();
  PutMetaRecord(path_, key.ValueOrDie(), mismatched.ValueOrDie());
  const Status status = store_->Open();
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_NE(status.ToString().find("wrong commit sequence"), std::string::npos)
      << status.ToString();
}

// S1 replaces the canonical-key Get with a comparison against the iterated key;
// a missing record in the middle of the watermark range must still be reported
// as corruption (same failure the Get's NotFound produced).
TEST_F(FactStoreMetadataTest, ReopenValidationRejectsMissingSequenceRecord) {
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{2}, VertexId{2}, 11)).ok());
  ASSERT_TRUE(store_->Close().ok());
  const auto key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(key.ok()) << key.status().ToString();
  DeleteMetaRecord(path_, key.ValueOrDie());
  const Status status = store_->Open();
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_NE(status.ToString().find("missing durable sequence record"),
            std::string::npos)
      << status.ToString();
}

// S3: when the identity-history rebuild is skipped, the filter must end up in
// its always-true state, never an empty filter. An empty filter would report a
// false negative for an identity that exists, and the point read that consults
// it would return no facts at all.
TEST_F(FactStoreMetadataTest,
       ReopenSkipLeavesIdentityHistoryAlwaysTrueAndReadsStayCorrect) {
  const FactRef ref =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref();
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
  ASSERT_TRUE(store_->Close().ok());
  FactStoreOptions options{path_};
  options.identity_history_skip_rebuild_for_testing = true;
  store_ = std::make_unique<FactStore>(options);
  ASSERT_TRUE(store_->Open().ok());
  const auto metrics = store_->SampleValidationCacheMetrics();
  ASSERT_TRUE(metrics.ok()) << metrics.status().ToString();
  EXPECT_TRUE(metrics.ValueOrDie().identity_history_disabled);
  EXPECT_EQ(metrics.ValueOrDie().identity_history_resident_bytes, 0U);

  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto observed =
      store_->Read(snapshot.ValueOrDie(), ref, ValidTime{10});
  ASSERT_TRUE(observed.ok()) << observed.status().ToString();
  ASSERT_TRUE(observed.ValueOrDie().has_value());
  EXPECT_EQ(observed.ValueOrDie()->valid_from.value, 10U);
  // The identity-history short-circuit lives in the temporal-neighborhood path:
  // an always-true filter must never turn an existing chain into "no facts".
  const auto neighborhood =
      store_->ReadTemporalNeighborhood(snapshot.ValueOrDie(), ref, ValidTime{10});
  ASSERT_TRUE(neighborhood.ok()) << neighborhood.status().ToString();
  ASSERT_TRUE(neighborhood.ValueOrDie().observed.has_value())
      << "the skipped filter reported a false negative for an existing identity";

  // The always-true filter falls back to the canonical read on the write path,
  // so a new version of the same identity still chains correctly.
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{2}, VertexId{1}, 20)).ok());
  const auto newer_snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(newer_snapshot.ok()) << newer_snapshot.status().ToString();
  const auto newer =
      store_->Read(newer_snapshot.ValueOrDie(), ref, ValidTime{20});
  ASSERT_TRUE(newer.ok()) << newer.status().ToString();
  ASSERT_TRUE(newer.ValueOrDie().has_value());
  EXPECT_EQ(newer.ValueOrDie()->valid_from.value, 20U);
}

// Negative control for S3: a database whose facts fit the filter keeps the
// existing rebuild (populated filter, not disabled).
TEST_F(FactStoreMetadataTest, ReopenRebuildsIdentityHistoryWhenFactsFitTheFilter) {
  ASSERT_TRUE(store_->Close().ok());
  store_ = std::make_unique<FactStore>(FactStoreOptions{path_});
  ASSERT_TRUE(store_->Open().ok());
  const auto metrics = store_->SampleValidationCacheMetrics();
  ASSERT_TRUE(metrics.ok()) << metrics.status().ToString();
  EXPECT_FALSE(metrics.ValueOrDie().identity_history_disabled);
  EXPECT_GT(metrics.ValueOrDie().identity_history_resident_bytes, 0U);
}

// S3 capacity path, end to end: 4096 validation-cache bytes leave 1536 bytes for
// identity history (128 words = 8192 bits = 1024 useful identities at 8 bits).
// Twelve hundred committed identities exceed that, so reopen must skip the
// rebuild and leave the filter always-true.
TEST_F(FactStoreMetadataTest,
       ReopenSkipsIdentityHistoryRebuildWhenFactsOutgrowFilter) {
  ASSERT_TRUE(store_->Close().ok());
  FactStoreOptions options{path_};
  options.validation_cache_bytes = 4096;
  store_ = std::make_unique<FactStore>(options);
  ASSERT_TRUE(store_->Open().ok());
  std::vector<PendingFactMutation> mutations;
  mutations.reserve(1200);
  for (uint64_t id = 1; id <= 1200; ++id) {
    mutations.push_back(PendingFactMutation{
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{id}}).ref(),
        ValidTime{10}, FactOperation::kPut, 0, std::nullopt});
  }
  ASSERT_TRUE(store_
                  ->Commit(StoreCommitBatch{TxnId{1}, 10, std::move(mutations),
                                            {}, {}, {}, false})
                  .ok());
  ASSERT_TRUE(store_->Close().ok());
  store_ = std::make_unique<FactStore>(options);
  ASSERT_TRUE(store_->Open().ok());
  const auto metrics = store_->SampleValidationCacheMetrics();
  ASSERT_TRUE(metrics.ok()) << metrics.status().ToString();
  EXPECT_TRUE(metrics.ValueOrDie().identity_history_disabled);
  EXPECT_EQ(metrics.ValueOrDie().identity_history_resident_bytes, 0U);

  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto observed = store_->Read(
      snapshot.ValueOrDie(),
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1200}}).ref(),
      ValidTime{10});
  ASSERT_TRUE(observed.ok()) << observed.status().ToString();
  ASSERT_TRUE(observed.ValueOrDie().has_value());
  // Same no-false-negative contract through the identity-history read path.
  const auto neighborhood = store_->ReadTemporalNeighborhood(
      snapshot.ValueOrDie(),
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1200}}).ref(),
      ValidTime{10});
  ASSERT_TRUE(neighborhood.ok()) << neighborhood.status().ToString();
  ASSERT_TRUE(neighborhood.ValueOrDie().observed.has_value())
      << "the skipped filter reported a false negative for an existing identity";
}

// F6b: LookupProperty on a pinned snapshot validates ownership from the snapshot
// handle itself (same lock-free check as Read). A live snapshot owns an epoch
// reference, so a successful close cannot release the schema catalog underneath
// it; the lookup therefore must not queue behind a concurrent close handshake
// that is holding lifecycle_mutex_.
TEST(FactStoreSnapshotSchemaLookupTest,
     DoesNotSerializeBehindStoreLifecycleLock) {
  char pattern[] = "/tmp/cedar_fact_store_schema_lookup_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  bool prepare_close_entered = false;
  bool release_prepare_close = false;
  FactStoreOptions options{path};
  options.prepare_close_locked_observer_for_testing = [&] {
    // Runs with lifecycle_mutex_ held by the close handshake.
    std::unique_lock<std::mutex> lock(gate_mutex);
    prepare_close_entered = true;
    gate_cv.notify_all();
    gate_cv.wait(lock, [&] { return release_prepare_close; });
  };
  FactStore store(std::move(options));
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.RegisterProperty(Property(PropertyId{9}, "name",
                                              PropertyEntityKind::kVertex,
                                              PhysicalType::kString))
                  .ok());
  auto opened_snapshot = store.BeginSnapshot();
  ASSERT_TRUE(opened_snapshot.ok()) << opened_snapshot.status().ToString();
  std::optional<StoreSnapshot> snapshot(
      std::move(opened_snapshot).ConsumeValueOrDie());

  Status prepare_status = Status::OK();
  std::jthread preparing([&] { prepare_status = store.PrepareClose(); });
  bool parked = false;
  {
    std::unique_lock<std::mutex> lock(gate_mutex);
    parked = gate_cv.wait_for(lock, std::chrono::seconds(5),
                              [&] { return prepare_close_entered; });
  }
  if (!parked) {
    {
      std::lock_guard<std::mutex> lock(gate_mutex);
      release_prepare_close = true;
    }
    gate_cv.notify_all();
    preparing.join();
    FAIL() << "the close handshake never reached the lifecycle-locked window";
  }
  // While PrepareClose holds lifecycle_mutex_, the pinned snapshot must still
  // resolve its property catalog without waiting for that mutex.
  auto lookup = std::async(std::launch::async, [&] {
    return store.LookupProperty(*snapshot, PropertyId{9});
  });
  const bool lookup_completed =
      lookup.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
  {
    std::lock_guard<std::mutex> lock(gate_mutex);
    release_prepare_close = true;
  }
  gate_cv.notify_all();
  preparing.join();
  ASSERT_TRUE(lookup_completed)
      << "LookupProperty blocked on the store lifecycle mutex";
  const auto definition = lookup.get();
  ASSERT_TRUE(definition.ok()) << definition.status().ToString();
  ASSERT_TRUE(definition.ValueOrDie().has_value());
  EXPECT_EQ(definition.ValueOrDie()->name, "name");
  // The pinned snapshot made PrepareClose report SnapshotPinned and leave the
  // store usable; the retry after releasing the pin must succeed.
  EXPECT_TRUE(prepare_status.IsSnapshotPinned()) << prepare_status.ToString();
  snapshot.reset();
  ASSERT_TRUE(store.Close().ok());
  std::filesystem::remove_all(path);
}

// Ownership validation is preserved without the lifecycle lock: a snapshot from
// another store is still rejected.
TEST(FactStoreSnapshotSchemaLookupTest, RejectsSnapshotFromAnotherStore) {
  char first_pattern[] = "/tmp/cedar_fact_store_schema_owner_a_XXXXXX";
  char second_pattern[] = "/tmp/cedar_fact_store_schema_owner_b_XXXXXX";
  ASSERT_NE(mkdtemp(first_pattern), nullptr);
  ASSERT_NE(mkdtemp(second_pattern), nullptr);
  const std::string first_path = first_pattern;
  const std::string second_path = second_pattern;
  FactStore first(FactStoreOptions{first_path});
  FactStore second(FactStoreOptions{second_path});
  ASSERT_TRUE(first.Open().ok());
  ASSERT_TRUE(second.Open().ok());
  ASSERT_TRUE(first.RegisterProperty(Property(PropertyId{9}, "name",
                                              PropertyEntityKind::kVertex,
                                              PhysicalType::kString))
                  .ok());
  auto foreign = second.BeginSnapshot();
  ASSERT_TRUE(foreign.ok()) << foreign.status().ToString();
  const auto result =
      first.LookupProperty(foreign.ValueOrDie(), PropertyId{9});
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalidArgument()) << result.status().ToString();
  foreign = StatusOr<StoreSnapshot>(Status::InvalidArgument("test", "released"));
  EXPECT_TRUE(first.Close().ok());
  EXPECT_TRUE(second.Close().ok());
  std::filesystem::remove_all(first_path);
  std::filesystem::remove_all(second_path);
}

TEST(FactStoreMetaCodecSequenceViewTest,
     DecodesPackedSequenceWithoutOwningFactKeys) {
  const FactRef first(PartId{1}, FactFamily::kVertexProperty, PropertyId{2}, 42);
  const FactRef second(PartId{1}, FactFamily::kVertexProperty, PropertyId{3}, 43);
  const std::string key1 =
      EncodeFactKey(first, ValidTime{10}, CommitSeq{7});
  const std::string key2 =
      EncodeFactKey(second, ValidTime{10}, CommitSeq{7});
  ASSERT_EQ(key1.size(), kEncodedFactKeyBytes);
  const SequenceRecord record{CommitSeq{7}, TxnId{9}, 123, {key1, key2}};
  const auto encoded = EncodeSequenceRecord(record);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

  const auto view = DecodeSequenceRecordView(encoded.ValueOrDie());
  ASSERT_TRUE(view.ok()) << view.status().ToString();
  EXPECT_EQ(view.ValueOrDie().commit_seq, CommitSeq{7});
  EXPECT_EQ(view.ValueOrDie().txn_id, TxnId{9});
  EXPECT_EQ(view.ValueOrDie().system_hlc, 123U);
  EXPECT_EQ(view.ValueOrDie().fact_count, 2U);
  ASSERT_EQ(view.ValueOrDie().identities.size(), 2 * kEncodedFactKeyBytes);
  EXPECT_TRUE(view.ValueOrDie().Validate().ok());

  const auto owning = DecodeSequenceRecord(encoded.ValueOrDie());
  ASSERT_TRUE(owning.ok()) << owning.status().ToString();
  EXPECT_EQ(owning.ValueOrDie().fact_keys.size(), 2U);
  EXPECT_EQ(owning.ValueOrDie().commit_seq, CommitSeq{7});
}

TEST(FactStoreMetaCodecSequenceViewTest,
     RejectsCorruptOrInconsistentSequenceRecords) {
  const FactRef first(PartId{1}, FactFamily::kVertexProperty, PropertyId{2}, 42);
  const FactRef other(PartId{1}, FactFamily::kVertexProperty, PropertyId{3}, 43);
  const std::string key1 =
      EncodeFactKey(first, ValidTime{10}, CommitSeq{7});
  const std::string stale_key =
      EncodeFactKey(other, ValidTime{10}, CommitSeq{6});
  const SequenceRecord record{CommitSeq{7}, TxnId{9}, 0, {key1}};
  const auto encoded = EncodeSequenceRecord(record);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  const std::string bytes = encoded.ValueOrDie();

  EXPECT_FALSE(
      DecodeSequenceRecordView(std::string_view(bytes).substr(
          0, bytes.size() - 2))
          .ok());
  std::string bad_crc = bytes;
  bad_crc.back() = static_cast<char>(bad_crc.back() ^ 0x1);
  EXPECT_FALSE(DecodeSequenceRecordView(bad_crc).ok());

  const auto good_view = DecodeSequenceRecordView(bytes);
  ASSERT_TRUE(good_view.ok()) << good_view.status().ToString();
  SequenceRecordView stale_view = good_view.ValueOrDie();
  stale_view.identities = std::string_view(stale_key);
  EXPECT_FALSE(stale_view.Validate().ok());

  const std::string duplicated = key1 + key1;
  SequenceRecordView duplicate_view = good_view.ValueOrDie();
  duplicate_view.fact_count = 2;
  duplicate_view.identities = std::string_view(duplicated);
  EXPECT_FALSE(duplicate_view.Validate().ok());

  SequenceRecordView length_view = good_view.ValueOrDie();
  length_view.fact_count = 2;
  EXPECT_FALSE(length_view.Validate().ok());
}

TEST_F(FactStoreMetadataTest,
       RejectsMissingOutcomeAtOutcomeValidationBatchBoundary) {
  constexpr uint64_t kSequenceCount = 5;
  for (uint64_t index = 1; index <= kSequenceCount; ++index) {
    ASSERT_TRUE(store_
                    ->Commit(VertexStateBatch(TxnId{index}, VertexId{index}, 10))
                    .ok());
  }
  ASSERT_TRUE(store_->Close().ok());
  const auto outcome_key = EncodeTransactionMetaKey(TxnId{kSequenceCount});
  ASSERT_TRUE(outcome_key.ok()) << outcome_key.status().ToString();
  DeleteMetaRecord(path_, outcome_key.ValueOrDie());

  FactStoreOptions options{path_};
  options.sequence_validation_outcome_batch_size_for_testing = 4;
  store_ = std::make_unique<FactStore>(options);
  const Status status = store_->Open();
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_NE(status.ToString().find("missing outcome"), std::string::npos)
      << status.ToString();
}

TEST_F(FactStoreMetadataTest,
       RejectsOutcomeDisagreementInsideOutcomeBatch) {
  for (uint64_t index = 1; index <= 3; ++index) {
    ASSERT_TRUE(store_
                    ->Commit(VertexStateBatch(TxnId{index}, VertexId{index}, 10))
                    .ok());
  }
  ASSERT_TRUE(store_->Close().ok());
  const auto outcome_key = EncodeTransactionMetaKey(TxnId{2});
  ASSERT_TRUE(outcome_key.ok()) << outcome_key.status().ToString();
  const auto wrong_outcome = EncodeTransactionOutcome(TransactionOutcomeRecord{
      TxnId{2}, CommitSeq{3}, TransactionOutcome::kCommitted});
  ASSERT_TRUE(wrong_outcome.ok()) << wrong_outcome.status().ToString();
  PutMetaRecord(path_, outcome_key.ValueOrDie(), wrong_outcome.ValueOrDie());

  const Status status = store_->Open();
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_NE(status.ToString().find("outcome and sequence disagree"),
            std::string::npos)
      << status.ToString();
}

TEST_F(FactStoreMetadataTest,
       BoundedValidationSkipsFactsBeforeTheRecentWindow) {
  constexpr uint64_t kTransactions = 5;
  for (uint64_t index = 1; index <= kTransactions; ++index) {
    ASSERT_TRUE(store_
                    ->Commit(VertexStateBatch(TxnId{index}, VertexId{index},
                                              10 + index))
                    .ok());
  }
  ASSERT_TRUE(store_->Close().ok());
  const std::string old_fact =
      ReadFactKeyFromSequenceRecord(path_, CommitSeq{1});
  ASSERT_FALSE(old_fact.empty());
  DeleteFactRecord(path_, old_fact);

  FactStoreOptions bounded{path_};
  bounded.deep_recovery_validation = false;
  bounded.recovery_validation_recent_sequences = 2;
  store_ = std::make_unique<FactStore>(bounded);
  ASSERT_TRUE(store_->Open().ok());
  ASSERT_TRUE(store_->Close().ok());

  store_ = std::make_unique<FactStore>(FactStoreOptions{path_});
  const Status deep = store_->Open();
  EXPECT_TRUE(deep.IsCorruption()) << deep.ToString();
  EXPECT_NE(deep.ToString().find("missing fact"), std::string::npos)
      << deep.ToString();
}

TEST_F(FactStoreMetadataTest,
       BoundedValidationStillChecksFactsInsideTheRecentWindow) {
  constexpr uint64_t kTransactions = 5;
  for (uint64_t index = 1; index <= kTransactions; ++index) {
    ASSERT_TRUE(store_
                    ->Commit(VertexStateBatch(TxnId{index}, VertexId{index},
                                              10 + index))
                    .ok());
  }
  ASSERT_TRUE(store_->Close().ok());
  const std::string recent_fact = ReadFactKeyFromSequenceRecord(
      path_, CommitSeq{kTransactions});
  ASSERT_FALSE(recent_fact.empty());
  DeleteFactRecord(path_, recent_fact);

  FactStoreOptions bounded{path_};
  bounded.deep_recovery_validation = false;
  bounded.recovery_validation_recent_sequences = 2;
  store_ = std::make_unique<FactStore>(bounded);
  const Status status = store_->Open();
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_NE(status.ToString().find("missing fact"), std::string::npos)
      << status.ToString();
}

// F9 Task 5: reopen observability. Each completed validation pass reports its
// own counters, so the deep and bounded policies can be compared from the
// observer instead of inferred from wall time. P1 consumes one record per
// committed sequence and issues one outcome MultiGet per <=1024 sequences; the
// fact Gets it issues are the whole fact set under deep validation and only the
// recent-window slice under bounded validation.
TEST_F(FactStoreMetadataTest, ReopenValidationCountersReportPhasesAndWindow) {
  constexpr uint64_t kTransactions = 3;
  constexpr uint64_t kFactsPerTransaction = 4;
  constexpr uint64_t kFacts = kTransactions * kFactsPerTransaction;
  for (uint64_t index = 1; index <= kTransactions; ++index) {
    const uint64_t first_vertex = (index - 1) * kFactsPerTransaction + 1;
    ASSERT_TRUE(store_
                    ->Commit(VertexFactsBatch(TxnId{index}, first_vertex,
                                              kFactsPerTransaction, 10 + index))
                    .ok());
  }
  ASSERT_TRUE(store_->Close().ok());

  std::vector<ReopenValidationPhaseCounters> phases;
  const auto collect = [&phases](
                           const ReopenValidationPhaseCounters& counters) {
    phases.push_back(counters);
  };

  FactStoreOptions deep{path_};
  deep.reopen_validation_counters_for_testing = collect;
  store_ = std::make_unique<FactStore>(deep);
  ASSERT_TRUE(store_->Open().ok());
  ASSERT_TRUE(store_->Close().ok());
  ASSERT_EQ(phases.size(), 2U);
  EXPECT_EQ(phases[0].phase, "P1_sequence_validation");
  EXPECT_EQ(phases[0].records, kTransactions);
  EXPECT_EQ(phases[0].fact_gets, kFacts);
  EXPECT_EQ(phases[0].meta_gets, 1U);
  EXPECT_EQ(phases[1].phase, "P2_identity_history");
  EXPECT_EQ(phases[1].records, kFacts);
  EXPECT_EQ(phases[1].fact_gets, 0U);
  EXPECT_EQ(phases[1].meta_gets, 0U);

  phases.clear();
  FactStoreOptions bounded{path_};
  bounded.deep_recovery_validation = false;
  bounded.recovery_validation_recent_sequences = 2;
  bounded.reopen_validation_counters_for_testing = collect;
  store_ = std::make_unique<FactStore>(bounded);
  ASSERT_TRUE(store_->Open().ok());
  ASSERT_TRUE(store_->Close().ok());
  ASSERT_EQ(phases.size(), 2U);
  EXPECT_EQ(phases[0].phase, "P1_sequence_validation");
  EXPECT_EQ(phases[0].records, kTransactions);
  // visible(3) - recent(2) + 1 = 2, so only sequences 2 and 3 cross-check facts.
  EXPECT_EQ(phases[0].fact_gets, 2 * kFactsPerTransaction);
  EXPECT_EQ(phases[0].meta_gets, 1U);
  EXPECT_EQ(phases[1].phase, "P2_identity_history");
  EXPECT_EQ(phases[1].records, kFacts);
}

// F9 Task 5: counters describe one Open, not a process-lifetime total. Two
// consecutive reopens must each report the same per-open counts instead of
// accumulating the previous open's numbers.
TEST_F(FactStoreMetadataTest, ReopenValidationCountersResetBetweenOpens) {
  constexpr uint64_t kTransactions = 3;
  for (uint64_t index = 1; index <= kTransactions; ++index) {
    ASSERT_TRUE(store_
                    ->Commit(VertexStateBatch(TxnId{index}, VertexId{index},
                                              10 + index))
                    .ok());
  }
  ASSERT_TRUE(store_->Close().ok());

  std::vector<ReopenValidationPhaseCounters> phases;
  FactStoreOptions options{path_};
  options.reopen_validation_counters_for_testing =
      [&phases](const ReopenValidationPhaseCounters& counters) {
        phases.push_back(counters);
      };
  for (int open = 0; open < 2; ++open) {
    store_ = std::make_unique<FactStore>(options);
    ASSERT_TRUE(store_->Open().ok());
    ASSERT_TRUE(store_->Close().ok());
  }
  ASSERT_EQ(phases.size(), 4U);
  for (size_t offset = 0; offset < phases.size(); offset += 2) {
    EXPECT_EQ(phases[offset].phase, "P1_sequence_validation");
    EXPECT_EQ(phases[offset].records, kTransactions);
    EXPECT_EQ(phases[offset].fact_gets, kTransactions);
    EXPECT_EQ(phases[offset].meta_gets, 1U);
    EXPECT_EQ(phases[offset + 1].phase, "P2_identity_history");
    EXPECT_EQ(phases[offset + 1].records, kTransactions);
  }
}

// Regression (a3ecf3b): ScanFamily replaced its full canonical scan with
// synthetic (version, part) prefix seeks. Once a clean close has flushed the
// facts CF into Cedar Parquet SSTs, the last-partition "next version" probe
// (a one-byte 0x03 key) cannot be decoded by the parquet comparator, so the
// scan latched Corruption after visiting the first fact. Database::Open then
// silently skipped its adjacency rebuild. ScanFamily must read committed facts
// of both requested families across flushed SSTs.
TEST_F(FactStoreMetadataTest, ScanFamilyReadsFlushedFactsAfterCleanReopen) {
  const EdgeIdentity identity{EdgeId{17}, VertexId{7}, VertexId{11}, 3};
  const StoreCommitBatch batch{
      TxnId{1},
      10,
      {{EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}).ref(),
        ValidTime{10}, FactOperation::kPut, 0, std::nullopt},
       {EntityFact::Edge(EdgeRef{PartId{0}, EdgeId{17}}).ref(), ValidTime{10},
        FactOperation::kPut, 0, std::nullopt}},
      {identity}};
  const auto committed = store_->Commit(batch);
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();

  // Close flushes the facts memtable into Cedar Parquet SSTs; only the reopened
  // store exercises the table comparator on the scan seeks.
  Reopen();

  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  std::vector<FactEvent> vertices;
  const Status vertex_scan = store_->ScanFamily(
      snapshot.ValueOrDie(), FactFamily::kVertexState,
      [&vertices](const FactEvent& event) {
        vertices.push_back(event);
        return Status::OK();
      });
  ASSERT_TRUE(vertex_scan.ok()) << vertex_scan.ToString();
  ASSERT_EQ(vertices.size(), 1U);
  EXPECT_EQ(vertices.front().ref,
            EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}).ref());

  std::vector<FactEvent> identities;
  const Status identity_scan = store_->ScanFamily(
      snapshot.ValueOrDie(), FactFamily::kEdgeIdentity,
      [&identities](const FactEvent& event) {
        identities.push_back(event);
        return Status::OK();
      });
  ASSERT_TRUE(identity_scan.ok()) << identity_scan.ToString();
  ASSERT_EQ(identities.size(), 1U);
  ASSERT_TRUE(identities.front().edge_identity.has_value());
  EXPECT_EQ(*identities.front().edge_identity, identity);
}

// ---------------------------------------------------------------------------
// Meta sequence record v2a (kSequenceV2)
// ---------------------------------------------------------------------------
// A v2 record replaces the packed 32-byte fact keys with 23-byte descriptors:
// 6-byte meta header, payload_version(1), fact_key_version(1), txn_id(8),
// system_hlc(8), count(4), then N x [part(4), family(1), property(2), entity(8),
// ~valid_from(8)]. commit_seq is not in the payload; it is the sequence/<seq>
// record key. Offsets below name the bytes a corruption test mutates.
constexpr size_t kSequenceV2PayloadVersionOffset = 6;
constexpr size_t kSequenceV2FactKeyVersionOffset = 7;
constexpr size_t kSequenceV2CountOffset = 24;
constexpr size_t kSequenceV2FirstFactOffset = 28;
constexpr size_t kSequenceV2FirstFactFamilyOffset =
    kSequenceV2FirstFactOffset + 4;

// Recomputes the trailing CRC32C so a test can mutate a record and have the
// targeted corruption (kind, version, count, descriptor) reach the decoder
// instead of failing the checksum first.
void RewriteMetaChecksum(std::string* encoded) {
  ASSERT_GE(encoded->size(), 10U);
  const uint32_t checksum =
      crc32c::Value(encoded->data(), encoded->size() - 4);
  size_t offset = encoded->size() - 4;
  for (int shift = 24; shift >= 0; shift -= 8) {
    (*encoded)[offset++] = static_cast<char>(checksum >> shift);
  }
}

void SetMetaRecordKind(std::string* encoded, uint8_t kind) {
  ASSERT_GE(encoded->size(), 2U);
  (*encoded)[1] = static_cast<char>(kind);
  RewriteMetaChecksum(encoded);
}

// Mixed vertex/edge, state/property references and per-fact valid times so the
// part/family/property/entity/~valid_from descriptor fields all differ.
FactRef SequenceV2FixtureRef(uint64_t index) {
  const PartId part{static_cast<uint32_t>(index % 5)};
  const uint64_t entity = index + 1;
  const PropertyId property{static_cast<uint16_t>(1 + index % 7)};
  switch (index % 4) {
    case 0:
      return EntityFact::Vertex(VertexRef{part, VertexId{entity}}).ref();
    case 1:
      return PropertyFact::Vertex(VertexRef{part, VertexId{entity}}, property)
          .ref();
    case 2:
      return EntityFact::Edge(EdgeRef{part, EdgeId{entity}}).ref();
    default:
      return PropertyFact::Edge(EdgeRef{part, EdgeId{entity}}, property).ref();
  }
}

SequenceRecord SequenceV2FixtureRecord(uint64_t fact_count,
                                       CommitSeq commit_seq = CommitSeq{7}) {
  SequenceRecord record{commit_seq, TxnId{9}, 123, {}};
  record.fact_keys.reserve(static_cast<size_t>(fact_count));
  for (uint64_t index = 0; index < fact_count; ++index) {
    record.fact_keys.push_back(EncodeFactKey(
        SequenceV2FixtureRef(index), ValidTime{100 + index}, commit_seq));
  }
  return record;
}

TEST(FactStoreMetaCodecSequenceV2Test,
     RoundTripsV2RecordsAndRebuildsByteIdenticalFactKeys) {
  for (uint64_t fact_count : {1U, 18U, 182U}) {
    SCOPED_TRACE(::testing::Message() << "fact_count=" << fact_count);
    const SequenceRecord record = SequenceV2FixtureRecord(fact_count);
    ASSERT_TRUE(record.Validate().ok()) << record.Validate().ToString();
    const auto v1 = EncodeSequenceRecord(record);
    ASSERT_TRUE(v1.ok()) << v1.status().ToString();
    const auto v2 = EncodeSequenceRecordV2(record);
    ASSERT_TRUE(v2.ok()) << v2.status().ToString();

    const auto decoded =
        DecodeSequenceRecord(v2.ValueOrDie(), record.commit_seq);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(decoded.ValueOrDie(), record);
    ASSERT_EQ(decoded.ValueOrDie().fact_keys.size(), fact_count);
    for (uint64_t index = 0; index < fact_count; ++index) {
      const std::string& rebuilt = decoded.ValueOrDie().fact_keys[index];
      EXPECT_EQ(rebuilt.size(), kEncodedFactKeyBytes);
      // Full 32-byte identity: the rebuilt key must equal the v1 key for the
      // same (ref, valid_from, commit_seq), not merely decode to it.
      EXPECT_EQ(rebuilt, record.fact_keys[index]);
      const auto decoded_key = DecodeFactKey(rebuilt);
      ASSERT_TRUE(decoded_key.ok()) << decoded_key.status().ToString();
      EXPECT_EQ(decoded_key.ValueOrDie().commit_seq, record.commit_seq);
      EXPECT_EQ(EncodeFactKey(decoded_key.ValueOrDie().ref,
                              decoded_key.ValueOrDie().valid_from,
                              record.commit_seq),
                rebuilt);
    }

    // The v1 writer's records still decode unchanged.
    const auto decoded_v1 = DecodeSequenceRecord(v1.ValueOrDie());
    ASSERT_TRUE(decoded_v1.ok()) << decoded_v1.status().ToString();
    EXPECT_EQ(decoded_v1.ValueOrDie(), record);
  }
}

TEST(FactStoreMetaCodecSequenceV2Test,
     DecodesBothRecordKindsAndRejectsCorruptV2Records) {
  const SequenceRecord record = SequenceV2FixtureRecord(3);
  const auto v1 = EncodeSequenceRecord(record);
  const auto v2 = EncodeSequenceRecordV2(record);
  ASSERT_TRUE(v1.ok()) << v1.status().ToString();
  ASSERT_TRUE(v2.ok()) << v2.status().ToString();
  const std::string v1_bytes = v1.ValueOrDie();
  const std::string v2_bytes = v2.ValueOrDie();

  // Both kinds decode through the key-aware entry point; v1 keeps its
  // single-argument decoder.
  EXPECT_TRUE(DecodeSequenceRecord(v1_bytes, record.commit_seq).ok());
  EXPECT_TRUE(DecodeSequenceRecord(v1_bytes).ok());
  const auto decoded_v2 =
      DecodeSequenceRecord(v2_bytes, record.commit_seq);
  ASSERT_TRUE(decoded_v2.ok()) << decoded_v2.status().ToString();
  EXPECT_EQ(decoded_v2.ValueOrDie(), record);

  // A v2 record does not carry commit_seq: the sequence key must be supplied.
  // The key-less decoders reject with an actionable corruption instead of
  // guessing and silently rebuilding wrong fact keys.
  const auto keyless = DecodeSequenceRecord(v2_bytes);
  ASSERT_TRUE(keyless.status().IsCorruption()) << keyless.status().ToString();
  EXPECT_NE(keyless.status().ToString().find("commit sequence"),
            std::string::npos)
      << keyless.status().ToString();
  EXPECT_TRUE(DecodeSequenceRecordView(v2_bytes).status().IsCorruption());
  EXPECT_TRUE(
      DecodeSequenceRecordView(v2_bytes, CommitSeq{0}).status().IsCorruption());

  // The materializing view rebuilds the packed identities a v1 view would have
  // borrowed, and still validates (duplicate/commit checks included).
  const auto view = DecodeSequenceRecordView(v2_bytes, record.commit_seq);
  ASSERT_TRUE(view.ok()) << view.status().ToString();
  EXPECT_EQ(view.ValueOrDie().commit_seq, record.commit_seq);
  EXPECT_EQ(view.ValueOrDie().txn_id, record.txn_id);
  EXPECT_EQ(view.ValueOrDie().system_hlc, record.system_hlc);
  EXPECT_EQ(view.ValueOrDie().fact_count, 3U);
  std::string expected_identities;
  for (const std::string& key : record.fact_keys) expected_identities += key;
  EXPECT_EQ(std::string(view.ValueOrDie().identities), expected_identities);
  EXPECT_TRUE(view.ValueOrDie().Validate().ok());
  // v2 materializes the rebuilt keys; v1 stays zero-copy.
  EXPECT_NE(view.ValueOrDie().owned_identities, nullptr);
  const auto v1_view = DecodeSequenceRecordView(v1_bytes);
  ASSERT_TRUE(v1_view.ok()) << v1_view.status().ToString();
  EXPECT_EQ(v1_view.ValueOrDie().owned_identities, nullptr);
  EXPECT_EQ(std::string(v1_view.ValueOrDie().identities), expected_identities);
  EXPECT_TRUE(v1_view.ValueOrDie().Validate().ok());

  // CRC mismatch.
  std::string bad_crc = v2_bytes;
  bad_crc.back() = static_cast<char>(bad_crc.back() ^ 0x01);
  EXPECT_TRUE(
      DecodeSequenceRecord(bad_crc, record.commit_seq).status().IsCorruption());

  // Truncated framing.
  EXPECT_TRUE(DecodeSequenceRecord(v2_bytes.substr(0, v2_bytes.size() - 2),
                                   record.commit_seq)
                  .status()
                  .IsCorruption());

  // Unknown record kind (checksum recomputed so the kind, not the CRC, rejects).
  std::string unknown_kind = v2_bytes;
  SetMetaRecordKind(&unknown_kind, 42);
  EXPECT_TRUE(DecodeSequenceRecord(unknown_kind, record.commit_seq)
                  .status()
                  .IsCorruption());

  // A v2 payload re-tagged as a v1 sequence record must not be silently decoded
  // as a v1 record; the framing checks catch the mismatch.
  std::string retagged_v1 = v2_bytes;
  SetMetaRecordKind(&retagged_v1, 7);
  EXPECT_TRUE(DecodeSequenceRecord(retagged_v1).status().IsCorruption());

  // Unsupported payload version.
  std::string bad_payload_version = v2_bytes;
  bad_payload_version[kSequenceV2PayloadVersionOffset] = 2;
  RewriteMetaChecksum(&bad_payload_version);
  EXPECT_TRUE(DecodeSequenceRecord(bad_payload_version, record.commit_seq)
                  .status()
                  .IsCorruption());

  // Unsupported fact-key version.
  std::string bad_fact_key_version = v2_bytes;
  bad_fact_key_version[kSequenceV2FactKeyVersionOffset] =
      static_cast<char>(kFactKeyVersion + 1);
  RewriteMetaChecksum(&bad_fact_key_version);
  const auto bad_version =
      DecodeSequenceRecord(bad_fact_key_version, record.commit_seq);
  ASSERT_TRUE(bad_version.status().IsCorruption())
      << bad_version.status().ToString();
  EXPECT_NE(bad_version.status().ToString().find("fact key version"),
            std::string::npos)
      << bad_version.status().ToString();

  // Count disagrees with the payload length.
  std::string bad_count = v2_bytes;
  bad_count[kSequenceV2CountOffset] =
      static_cast<char>(bad_count[kSequenceV2CountOffset] + 1);
  RewriteMetaChecksum(&bad_count);
  EXPECT_TRUE(
      DecodeSequenceRecord(bad_count, record.commit_seq).status().IsCorruption());

  // A per-fact descriptor with an invalid fact reference.
  std::string bad_family = v2_bytes;
  bad_family[kSequenceV2FirstFactFamilyOffset] = 0;
  RewriteMetaChecksum(&bad_family);
  EXPECT_TRUE(DecodeSequenceRecord(bad_family, record.commit_seq)
                  .status()
                  .IsCorruption());
}

TEST(FactStoreMetaCodecSequenceV2Test, V2RecordDropsNineBytesPerFact) {
  size_t delta_for_one = 0;
  size_t delta_for_many = 0;
  for (uint64_t fact_count : {1U, 182U}) {
    SCOPED_TRACE(::testing::Message() << "fact_count=" << fact_count);
    const SequenceRecord record = SequenceV2FixtureRecord(fact_count);
    const auto v1 = EncodeSequenceRecord(record);
    const auto v2 = EncodeSequenceRecordV2(record);
    ASSERT_TRUE(v1.ok()) << v1.status().ToString();
    ASSERT_TRUE(v2.ok()) << v2.status().ToString();
    const size_t v1_bytes = v1.ValueOrDie().size();
    const size_t v2_bytes = v2.ValueOrDie().size();
    ASSERT_GT(v1_bytes, v2_bytes);
    const size_t delta = v1_bytes - v2_bytes;
    // Same 10-byte framing. The v1 payload is 28 + 32*N; the v2 payload is
    // 22 + 23*N. v2 drops v1's 8-byte commit_seq and adds two 1-byte version
    // fields (a fixed -6 bytes per record), then saves 9 bytes per fact.
    EXPECT_EQ(delta, 6U + 9U * static_cast<size_t>(fact_count));
    if (fact_count == 1) {
      delta_for_one = delta;
    } else {
      delta_for_many = delta;
    }
  }
  // The per-fact slope is exactly 9 bytes: (32 - 23).
  EXPECT_EQ(delta_for_many - delta_for_one, 9U * (182U - 1U));
}

// Mixed v1/v2 database: one sequence record stays v1 and one is rewritten as
// v2a in the raw meta CF. Reopen validation (default policy), ReadSequence and
// ReadSequenceRange must read both kinds.
TEST_F(FactStoreMetadataTest, ReopenAndReadSequenceAcceptMixedV1AndV2Records) {
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{2}, VertexId{2}, 11)).ok());
  ASSERT_TRUE(store_->Close().ok());

  const auto sequence_key = EncodeSequenceMetaKey(CommitSeq{2});
  ASSERT_TRUE(sequence_key.ok()) << sequence_key.status().ToString();
  SequenceRecord expected;
  std::string encoded_v2;
  {
    RawFactDatabase raw;
    ASSERT_TRUE(OpenRawFactDatabase(path_, &raw));
    std::string encoded_v1;
    const rocksdb::Status got = raw.database->Get(
        rocksdb::ReadOptions(), raw.handles[2], sequence_key.ValueOrDie(),
        &encoded_v1);
    ASSERT_TRUE(got.ok()) << got.ToString();
    const auto decoded = DecodeSequenceRecord(encoded_v1);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    expected = decoded.ValueOrDie();
    // Reopen validation does not cross-check system_hlc, so a sentinel here
    // distinguishes "the reopened store read the v2 record" from "it re-read a
    // stale v1 record" (which would decode to the same fact keys).
    expected.system_hlc = 424242;
    const auto v2 = EncodeSequenceRecordV2(expected);
    ASSERT_TRUE(v2.ok()) << v2.status().ToString();
    encoded_v2 = v2.ValueOrDie();
    ASSERT_TRUE(raw.database
                    ->Put(rocksdb::WriteOptions(), raw.handles[2],
                          sequence_key.ValueOrDie(), encoded_v2)
                    .ok());
    std::string stored;
    const rocksdb::Status stored_status = raw.database->Get(
        rocksdb::ReadOptions(), raw.handles[2], sequence_key.ValueOrDie(),
        &stored);
    ASSERT_TRUE(stored_status.ok()) << stored_status.ToString();
    EXPECT_EQ(stored, encoded_v2);
  }

  // Default reopen policy validates both kinds and succeeds.
  store_ = std::make_unique<FactStore>(FactStoreOptions{path_});
  ASSERT_TRUE(store_->Open().ok());

  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto read_v2 = store_->ReadSequence(snapshot.ValueOrDie(), CommitSeq{2});
  ASSERT_TRUE(read_v2.ok()) << read_v2.status().ToString();
  EXPECT_EQ(read_v2.ValueOrDie(), expected);
  const auto read_v1 = store_->ReadSequence(snapshot.ValueOrDie(), CommitSeq{1});
  ASSERT_TRUE(read_v1.ok()) << read_v1.status().ToString();

  const auto range =
      store_->ReadSequenceRange(snapshot.ValueOrDie(), CommitSeq{1}, CommitSeq{2});
  ASSERT_TRUE(range.ok()) << range.status().ToString();
  ASSERT_EQ(range.ValueOrDie().size(), 2U);
  EXPECT_EQ(range.ValueOrDie()[1], expected);
}

// ---------------------------------------------------------------------------
// Meta sequence format marker + opt-in v2 writer (F10 Task 10)
// ---------------------------------------------------------------------------
// `MetaRecordKind` stays private to meta_codec.cc; these tests pin the on-disk
// kind bytes the same way the v1/v2 codec tests pin payload offsets.
constexpr uint8_t kRawSequenceRecordKindV1 = 7;
constexpr uint8_t kRawSequenceRecordKindV2 = 9;
constexpr uint8_t kRawSequenceFormatMarkerKind = 10;
constexpr size_t kRawMetaRecordHeaderBytes = 6;
constexpr size_t kRawMetaRecordChecksumBytes = 4;
constexpr size_t kSequenceV2FixedPayloadBytes = 1 + 1 + 8 + 8 + 4;  // 22
constexpr size_t kSequenceV2DescriptorBytes = 4 + 1 + 2 + 8 + 8;    // 23

uint8_t RawMetaRecordKind(const std::string& encoded) {
  EXPECT_GE(encoded.size(), 2U);
  return encoded.size() < 2U ? 0 : static_cast<uint8_t>(encoded[1]);
}

std::optional<std::string> ReadMetaRecordIfPresent(const std::string& path,
                                                   const std::string& key) {
  RawFactDatabase raw;
  if (!OpenRawFactDatabase(path, &raw)) {
    ADD_FAILURE() << "failed to open raw database";
    return std::nullopt;
  }
  std::string encoded;
  const rocksdb::Status got = raw.database->Get(
      rocksdb::ReadOptions(), raw.handles[2], key, &encoded);
  if (got.IsNotFound()) return std::nullopt;
  if (!got.ok()) {
    ADD_FAILURE() << got.ToString();
    return std::nullopt;
  }
  return encoded;
}

std::string ReadMetaRecord(const std::string& path, const std::string& key) {
  const auto encoded = ReadMetaRecordIfPresent(path, key);
  EXPECT_TRUE(encoded.has_value()) << "missing meta record: " << key;
  return encoded.value_or(std::string());
}

FactStoreOptions StoreOptionsAt(const std::string& path, uint32_t format) {
  FactStoreOptions options;
  options.path = path;
  options.meta_sequence_format = format;
  return options;
}

TEST(FactStoreMetaSequenceFormatCodecTest,
     RoundTripsFormatMarkersAndRejectsCorruptFraming) {
  // The marker key is part of the durable format contract.
  EXPECT_EQ(EncodeSequenceFormatKey(), "format/sequence-record-format");
  for (uint32_t format : {kMetaSequenceFormatV1, kMetaSequenceFormatV2}) {
    SCOPED_TRACE(::testing::Message() << "format=" << format);
    const auto encoded = EncodeSequenceFormat(format);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    EXPECT_EQ(RawMetaRecordKind(encoded.ValueOrDie()),
              kRawSequenceFormatMarkerKind);
    // Payload is exactly format(4 BE) + the standard 6-byte meta header and
    // 4-byte CRC32C trailer.
    EXPECT_EQ(encoded.ValueOrDie().size(), 14U);
    const auto decoded = DecodeSequenceFormat(encoded.ValueOrDie());
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(decoded.ValueOrDie(), format);
  }

  const auto encoded = EncodeSequenceFormat(kMetaSequenceFormatV2);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  const std::string bytes = encoded.ValueOrDie();

  // Re-tagging the record as a sequence record must not decode as a marker.
  std::string wrong_kind = bytes;
  SetMetaRecordKind(&wrong_kind, kRawSequenceRecordKindV2);
  EXPECT_TRUE(DecodeSequenceFormat(wrong_kind).status().IsCorruption());

  // A corrupted CRC fails before the payload is trusted.
  std::string bad_crc = bytes;
  bad_crc.back() = static_cast<char>(bad_crc.back() ^ 0x01);
  EXPECT_TRUE(DecodeSequenceFormat(bad_crc).status().IsCorruption());

  // Truncated framing.
  EXPECT_TRUE(DecodeSequenceFormat(bytes.substr(0, bytes.size() - 2))
                  .status()
                  .IsCorruption());

  // A structurally valid record whose payload is not exactly four bytes is
  // still corruption (length header disagrees with the framing).
  std::string extra_payload = bytes;
  extra_payload.insert(extra_payload.end() - 4, 1, '\0');
  RewriteMetaChecksum(&extra_payload);
  EXPECT_TRUE(DecodeSequenceFormat(extra_payload).status().IsCorruption());
}

TEST_F(FactStoreMetadataTest,
       OptInWriterWritesDurableMarkerAndV2RecordsOnFreshDatabase) {
  const std::string path = path_ + "/fresh_v2";
  std::atomic<uint32_t> marker_writes{0};
  std::atomic<bool> marker_sync{false};
  {
    FactStoreOptions options = StoreOptionsAt(path, kMetaSequenceFormatV2);
    options.meta_sequence_format_marker_write_observer_for_testing =
        [&](bool sync) {
          marker_writes.fetch_add(1, std::memory_order_relaxed);
          marker_sync.store(sync, std::memory_order_relaxed);
        };
    FactStore store(options);
    ASSERT_TRUE(store.Open().ok());
    // The opt-in upgrade records the marker once, with Sync=true, before the
    // store accepts commits.
    EXPECT_EQ(marker_writes.load(), 1U);
    EXPECT_TRUE(marker_sync.load());
    ASSERT_TRUE(store.Commit(VertexFactsBatch(TxnId{1}, 100, 3, 10)).ok());
    ASSERT_TRUE(store.Close().ok());
  }

  const std::string marker = ReadMetaRecord(path, EncodeSequenceFormatKey());
  ASSERT_FALSE(marker.empty());
  EXPECT_EQ(RawMetaRecordKind(marker), kRawSequenceFormatMarkerKind);
  const auto decoded_marker = DecodeSequenceFormat(marker);
  ASSERT_TRUE(decoded_marker.ok()) << decoded_marker.status().ToString();
  EXPECT_EQ(decoded_marker.ValueOrDie(), kMetaSequenceFormatV2);

  const auto sequence_key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(sequence_key.ok()) << sequence_key.status().ToString();
  const std::string record = ReadMetaRecord(path, sequence_key.ValueOrDie());
  ASSERT_FALSE(record.empty());
  EXPECT_EQ(RawMetaRecordKind(record), kRawSequenceRecordKindV2);
  // v2 record: 6-byte header + 22-byte fixed payload + N x 23 + 4-byte CRC.
  EXPECT_EQ(record.size(), kRawMetaRecordHeaderBytes +
                               kSequenceV2FixedPayloadBytes +
                               3 * kSequenceV2DescriptorBytes +
                               kRawMetaRecordChecksumBytes);

  const auto decoded = DecodeSequenceRecord(record, CommitSeq{1});
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  EXPECT_EQ(decoded.ValueOrDie().commit_seq, CommitSeq{1});
  EXPECT_EQ(decoded.ValueOrDie().txn_id, TxnId{1});
  EXPECT_EQ(decoded.ValueOrDie().system_hlc, 10U);
  ASSERT_EQ(decoded.ValueOrDie().fact_keys.size(), 3U);
  for (uint64_t index = 0; index < 3; ++index) {
    EXPECT_EQ(decoded.ValueOrDie().fact_keys[index],
              EncodeFactKey(
                  EntityFact::Vertex(
                      VertexRef{PartId{0}, VertexId{100 + index}})
                      .ref(),
                  ValidTime{10}, CommitSeq{1}));
  }
}

TEST_F(FactStoreMetadataTest,
       DefaultV1WriterWritesNoMarkerAndKeepsV1RecordBytes) {
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
  ASSERT_TRUE(store_->Close().ok());

  // No opt-in: no marker record, exactly today's v1 meta layout.
  EXPECT_FALSE(
      ReadMetaRecordIfPresent(path_, EncodeSequenceFormatKey()).has_value());

  const auto sequence_key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(sequence_key.ok()) << sequence_key.status().ToString();
  const std::string record = ReadMetaRecord(path_, sequence_key.ValueOrDie());
  ASSERT_FALSE(record.empty());
  EXPECT_EQ(RawMetaRecordKind(record), kRawSequenceRecordKindV1);

  // Byte-identity: the default path still emits the pre-v2 encoder output,
  // 6-byte header + 28-byte fixed payload + one 32-byte key + 4-byte CRC.
  const std::string fact_key =
      EncodeFactKey(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref(),
                    ValidTime{10}, CommitSeq{1});
  const auto expected = EncodeSequenceRecord(
      SequenceRecord{CommitSeq{1}, TxnId{1}, 10, {fact_key}});
  ASSERT_TRUE(expected.ok()) << expected.status().ToString();
  EXPECT_EQ(record, expected.ValueOrDie());
  EXPECT_EQ(record.size(), 38U + 32U);
}

TEST_F(FactStoreMetadataTest,
       ReopenWithV1OptionKeepsWritingV2FromDurableMarker) {
  const std::string path = path_ + "/marker_authoritative";
  {
    FactStore store(StoreOptionsAt(path, kMetaSequenceFormatV2));
    ASSERT_TRUE(store.Open().ok());
    ASSERT_TRUE(store.Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
    ASSERT_TRUE(store.Close().ok());
  }

  std::atomic<uint32_t> marker_writes{0};
  {
    // The option asks for v1, but the durable marker says 2: the format is a
    // one-way door and the store keeps writing v2 without rewriting the marker.
    FactStoreOptions options = StoreOptionsAt(path, kMetaSequenceFormatV1);
    options.meta_sequence_format_marker_write_observer_for_testing =
        [&](bool) { marker_writes.fetch_add(1, std::memory_order_relaxed); };
    FactStore store(options);
    ASSERT_TRUE(store.Open().ok());
    EXPECT_EQ(marker_writes.load(), 0U);
    ASSERT_TRUE(store.Commit(VertexStateBatch(TxnId{2}, VertexId{2}, 20)).ok());
    ASSERT_TRUE(store.Close().ok());
  }

  const auto decoded_marker =
      DecodeSequenceFormat(ReadMetaRecord(path, EncodeSequenceFormatKey()));
  ASSERT_TRUE(decoded_marker.ok()) << decoded_marker.status().ToString();
  EXPECT_EQ(decoded_marker.ValueOrDie(), kMetaSequenceFormatV2);
  for (uint64_t sequence = 1; sequence <= 2; ++sequence) {
    const auto key = EncodeSequenceMetaKey(CommitSeq{sequence});
    ASSERT_TRUE(key.ok()) << key.status().ToString();
    EXPECT_EQ(RawMetaRecordKind(ReadMetaRecord(path, key.ValueOrDie())),
              kRawSequenceRecordKindV2)
        << "sequence " << sequence;
  }

  // All reads and reopen validation still pass on the v2 history.
  {
    FactStore store(FactStoreOptions{path});
    ASSERT_TRUE(store.Open().ok());
    {
      const auto snapshot = store.BeginSnapshot();
      ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
      const auto seq1 =
          store.ReadSequence(snapshot.ValueOrDie(), CommitSeq{1});
      ASSERT_TRUE(seq1.ok()) << seq1.status().ToString();
      EXPECT_EQ(seq1.ValueOrDie().fact_keys.size(), 1U);
      const auto range = store.ReadSequenceRange(snapshot.ValueOrDie(),
                                                 CommitSeq{1}, CommitSeq{2});
      ASSERT_TRUE(range.ok()) << range.status().ToString();
      EXPECT_EQ(range.ValueOrDie().size(), 2U);
    }
    ASSERT_TRUE(store.Close().ok());
  }
}

TEST_F(FactStoreMetadataTest,
       MixedV1HistoryThenV2EnableWritesReadsAndKernelRecovers) {
  const std::string path = path_ + "/mixed_v1_v2";
  // v1 history.
  {
    FactStore store(FactStoreOptions{path});
    ASSERT_TRUE(store.Open().ok());
    ASSERT_TRUE(store.Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
    ASSERT_TRUE(store.Close().ok());
  }
  // Opt-in upgrade: only new records are v2; the v1 history stays untouched.
  {
    FactStore store(StoreOptionsAt(path, kMetaSequenceFormatV2));
    ASSERT_TRUE(store.Open().ok());
    ASSERT_TRUE(store.Commit(VertexStateBatch(TxnId{2}, VertexId{2}, 20)).ok());
    ASSERT_TRUE(store.Close().ok());
  }

  const auto marker =
      DecodeSequenceFormat(ReadMetaRecord(path, EncodeSequenceFormatKey()));
  ASSERT_TRUE(marker.ok()) << marker.status().ToString();
  EXPECT_EQ(marker.ValueOrDie(), kMetaSequenceFormatV2);
  const auto seq1_key = EncodeSequenceMetaKey(CommitSeq{1});
  const auto seq2_key = EncodeSequenceMetaKey(CommitSeq{2});
  ASSERT_TRUE(seq1_key.ok());
  ASSERT_TRUE(seq2_key.ok());
  EXPECT_EQ(RawMetaRecordKind(ReadMetaRecord(path, seq1_key.ValueOrDie())),
            kRawSequenceRecordKindV1);
  EXPECT_EQ(RawMetaRecordKind(ReadMetaRecord(path, seq2_key.ValueOrDie())),
            kRawSequenceRecordKindV2);

  // Default-policy reopen (option stays 1): validation and reads cover both
  // kinds, and the marker keeps the effective writer at v2.
  {
    FactStore store(FactStoreOptions{path});
    ASSERT_TRUE(store.Open().ok());
    {
      const auto snapshot = store.BeginSnapshot();
      ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
      const auto seq1 =
          store.ReadSequence(snapshot.ValueOrDie(), CommitSeq{1});
      ASSERT_TRUE(seq1.ok()) << seq1.status().ToString();
      EXPECT_EQ(seq1.ValueOrDie().fact_keys.size(), 1U);
      const auto seq2 =
          store.ReadSequence(snapshot.ValueOrDie(), CommitSeq{2});
      ASSERT_TRUE(seq2.ok()) << seq2.status().ToString();
      EXPECT_EQ(seq2.ValueOrDie().fact_keys.size(), 1U);
      const auto range = store.ReadSequenceRange(snapshot.ValueOrDie(),
                                                 CommitSeq{1}, CommitSeq{2});
      ASSERT_TRUE(range.ok()) << range.status().ToString();
      EXPECT_EQ(range.ValueOrDie().size(), 2U);
    }
    ASSERT_TRUE(store.Commit(VertexStateBatch(TxnId{3}, VertexId{3}, 30)).ok());
    ASSERT_TRUE(store.Close().ok());
  }
  const auto seq3_key = EncodeSequenceMetaKey(CommitSeq{3});
  ASSERT_TRUE(seq3_key.ok());
  EXPECT_EQ(RawMetaRecordKind(ReadMetaRecord(path, seq3_key.ValueOrDie())),
            kRawSequenceRecordKindV2);

  // Kernel open on the mixed database: the transaction-ID allocator cold path
  // rebuilds from v1+v2 sequence records, and the kernel commit itself goes
  // through the same opt-in v2 writer branch.
  {
    DatabaseOptions options;
    options.path = path;
    auto opened = Database::Open(std::move(options));
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
    auto begun = database->BeginTransaction();
    ASSERT_TRUE(begun.ok()) << begun.status().ToString();
    std::unique_ptr<Transaction> transaction =
        std::move(begun).ConsumeValueOrDie();
    ASSERT_TRUE(transaction
                    ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{4}}),
                             ValidTime{40})
                    .ok());
    const auto committed = transaction->Commit();
    ASSERT_TRUE(committed.ok()) << committed.status().ToString();
    EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
    ASSERT_TRUE(database->Close().ok());
  }
  const auto seq4_key = EncodeSequenceMetaKey(CommitSeq{4});
  ASSERT_TRUE(seq4_key.ok());
  EXPECT_EQ(RawMetaRecordKind(ReadMetaRecord(path, seq4_key.ValueOrDie())),
            kRawSequenceRecordKindV2);

  // Final full reopen: v1 + v2 + kernel-v2 history validates and reads.
  {
    FactStore store(FactStoreOptions{path});
    ASSERT_TRUE(store.Open().ok());
    EXPECT_EQ(store.visible_seq(), CommitSeq{4});
    {
      const auto snapshot = store.BeginSnapshot();
      ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
      const auto range = store.ReadSequenceRange(snapshot.ValueOrDie(),
                                                 CommitSeq{1}, CommitSeq{4});
      ASSERT_TRUE(range.ok()) << range.status().ToString();
      EXPECT_EQ(range.ValueOrDie().size(), 4U);
    }
    ASSERT_TRUE(store.Close().ok());
  }
}

TEST_F(FactStoreMetadataTest, CorruptOrUnsupportedFormatMarkerFailsOpen) {
  const auto create_v1_database = [this](const std::string& name) {
    const std::string path = path_ + "/" + name;
    FactStore store(FactStoreOptions{path});
    EXPECT_TRUE(store.Open().ok());
    EXPECT_TRUE(store.Close().ok());
    return path;
  };
  const std::string marker_key = EncodeSequenceFormatKey();

  // Structurally valid marker with a value this binary does not support.
  {
    const std::string path = create_v1_database("marker_unsupported");
    const auto marker = EncodeSequenceFormat(3);
    ASSERT_TRUE(marker.ok()) << marker.status().ToString();
    PutMetaRecord(path, marker_key, marker.ValueOrDie());
    FactStore store(FactStoreOptions{path});
    const Status opened = store.Open();
    EXPECT_TRUE(opened.IsCorruption()) << opened.ToString();
    EXPECT_NE(opened.ToString().find("unsupported meta sequence format marker"),
              std::string::npos)
        << opened.ToString();
    if (opened.ok()) EXPECT_TRUE(store.Close().ok());
  }

  // CRC corruption.
  {
    const std::string path = create_v1_database("marker_bad_crc");
    const auto marker = EncodeSequenceFormat(kMetaSequenceFormatV2);
    ASSERT_TRUE(marker.ok()) << marker.status().ToString();
    std::string bytes = marker.ValueOrDie();
    bytes.back() = static_cast<char>(bytes.back() ^ 0x01);
    PutMetaRecord(path, marker_key, bytes);
    FactStore store(FactStoreOptions{path});
    const Status opened = store.Open();
    EXPECT_TRUE(opened.IsCorruption()) << opened.ToString();
    if (opened.ok()) EXPECT_TRUE(store.Close().ok());
  }

  // Truncated framing.
  {
    const std::string path = create_v1_database("marker_truncated");
    const auto marker = EncodeSequenceFormat(kMetaSequenceFormatV2);
    ASSERT_TRUE(marker.ok()) << marker.status().ToString();
    const std::string bytes = marker.ValueOrDie();
    PutMetaRecord(path, marker_key, bytes.substr(0, bytes.size() - 2));
    FactStore store(FactStoreOptions{path});
    const Status opened = store.Open();
    EXPECT_TRUE(opened.IsCorruption()) << opened.ToString();
    if (opened.ok()) EXPECT_TRUE(store.Close().ok());
  }
}

TEST_F(FactStoreMetadataTest,
       MarkerWriteFailureFailsOpenAndLeavesV1DatabaseUsable) {
  const std::string path = path_ + "/marker_write_failure";
  FactStoreOptions options = StoreOptionsAt(path, kMetaSequenceFormatV2);
  options.meta_sequence_format_marker_write_fault_injector_for_testing = [] {
    return Status::IOError("test", "marker write failed");
  };
  {
    FactStore store(options);
    const Status opened = store.Open();
    EXPECT_TRUE(opened.IsIOError()) << opened.ToString();
  }
  EXPECT_FALSE(
      ReadMetaRecordIfPresent(path, EncodeSequenceFormatKey()).has_value());
  // No v2 record was written, so the database still opens with the default v1
  // writer and is unchanged from today's behavior.
  {
    FactStore store(FactStoreOptions{path});
    ASSERT_TRUE(store.Open().ok());
    EXPECT_EQ(store.visible_seq(), CommitSeq{0});
    ASSERT_TRUE(store.Close().ok());
  }
}

TEST_F(FactStoreMetadataTest, UnsupportedWriterOptionIsRejectedByOpen) {
  const std::string path = path_ + "/invalid_option";
  FactStoreOptions options = StoreOptionsAt(path, 3);
  FactStore store(options);
  const Status opened = store.Open();
  EXPECT_TRUE(opened.IsInvalidArgument()) << opened.ToString();
}

}  // namespace
}  // namespace cedar
