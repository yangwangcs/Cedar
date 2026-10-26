#include <gtest/gtest.h>

#include <algorithm>
#include <array>
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
#include <tuple>
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
  // Dual-read: F19 makes v2 the default writer format, so this key reader must
  // pass the commit sequence instead of using the v1-only decoder.
  const auto record = DecodeSequenceRecord(encoded, commit_seq);
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

// S1 must not lose a failure: replacing a committed v2 sequence record with
// another valid v2 record still fails reopen validation, because the rebuilt
// fact keys no longer match any stored fact.
TEST_F(FactStoreMetadataTest,
       ReopenValidationRejectsSequenceRecordMovedFromAnotherCommit) {
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{2}, VertexId{2}, 11)).ok());
  ASSERT_TRUE(store_->Close().ok());
  const FactRef second =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}).ref();
  const auto moved = EncodeSequenceRecordV2(SequenceRecord{
      CommitSeq{2}, TxnId{2}, 11,
      {EncodeFactKey(second, ValidTime{11}, CommitSeq{2})}});
  ASSERT_TRUE(moved.ok()) << moved.status().ToString();
  const auto key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(key.ok()) << key.status().ToString();
  PutMetaRecord(path_, key.ValueOrDie(), moved.ValueOrDie());
  const Status status = store_->Open();
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_NE(status.ToString().find("missing fact"), std::string::npos)
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
     DecodesV2SequenceIntoOwnedFactKeys) {
  const FactRef first(PartId{1}, FactFamily::kVertexProperty, PropertyId{2}, 42);
  const FactRef second(PartId{1}, FactFamily::kVertexProperty, PropertyId{3}, 43);
  const std::string key1 =
      EncodeFactKey(first, ValidTime{10}, CommitSeq{7});
  const std::string key2 =
      EncodeFactKey(second, ValidTime{10}, CommitSeq{7});
  ASSERT_EQ(key1.size(), kEncodedFactKeyBytes);
  const SequenceRecord record{CommitSeq{7}, TxnId{9}, 123, {key1, key2}};
  const auto encoded = EncodeSequenceRecordV2(record);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

  const auto view =
      DecodeSequenceRecordView(encoded.ValueOrDie(), record.commit_seq);
  ASSERT_TRUE(view.ok()) << view.status().ToString();
  EXPECT_EQ(view.ValueOrDie().commit_seq, CommitSeq{7});
  EXPECT_EQ(view.ValueOrDie().txn_id, TxnId{9});
  EXPECT_EQ(view.ValueOrDie().system_hlc, 123U);
  EXPECT_EQ(view.ValueOrDie().fact_count, 2U);
  ASSERT_NE(view.ValueOrDie().owned_identities, nullptr);
  EXPECT_EQ(std::string(view.ValueOrDie().identities), key1 + key2);
  EXPECT_TRUE(view.ValueOrDie().Validate().ok());

  const auto owning =
      DecodeSequenceRecord(encoded.ValueOrDie(), record.commit_seq);
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
  const auto encoded = EncodeSequenceRecordV2(record);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  const std::string bytes = encoded.ValueOrDie();

  EXPECT_FALSE(DecodeSequenceRecordView(
                   std::string_view(bytes).substr(0, bytes.size() - 2),
                   record.commit_seq)
                   .ok());
  std::string bad_crc = bytes;
  bad_crc.back() = static_cast<char>(bad_crc.back() ^ 0x1);
  EXPECT_FALSE(
      DecodeSequenceRecordView(bad_crc, record.commit_seq).ok());

  const auto good_view =
      DecodeSequenceRecordView(bytes, record.commit_seq);
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

// The default reopen policy cross-checks every retained fact. The bounded
// recent window remains available as an explicit performance policy.
TEST_F(FactStoreMetadataTest, DefaultReopenValidationUsesFullHistory) {
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

  FactStoreOptions defaults{path_};
  defaults.recovery_validation_recent_sequences = 2;
  store_ = std::make_unique<FactStore>(defaults);
  const Status default_status = store_->Open();
  EXPECT_TRUE(default_status.IsCorruption()) << default_status.ToString();
  EXPECT_NE(default_status.ToString().find("missing fact"), std::string::npos)
      << default_status.ToString();

  FactStoreOptions bounded{path_};
  bounded.deep_recovery_validation = false;
  bounded.recovery_validation_recent_sequences = 2;
  store_ = std::make_unique<FactStore>(bounded);
  ASSERT_TRUE(store_->Open().ok());
  const auto read = store_->ReadCurrent(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref(),
      ValidTime{11});
  ASSERT_TRUE(read.ok()) << read.status().ToString();
  EXPECT_FALSE(read.ValueOrDie().has_value())
      << "the explicitly bounded policy may skip facts outside its window";
  ASSERT_TRUE(store_->Close().ok());
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

  // Full validation rejects the missing retained fact.
  FactStoreOptions deep_options{path_};
  deep_options.deep_recovery_validation = true;
  store_ = std::make_unique<FactStore>(deep_options);
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
  deep.deep_recovery_validation = true;
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
TEST_F(FactStoreMetadataTest, ReopenReadBudgetExhaustionIsNotMissingFactCorruption) {
  const StoreCommitBatch batch{
      TxnId{1}, 10,
      {{EntityFact::Vertex({PartId{0}, VertexId{7}}).ref(), ValidTime{10},
        FactOperation::kPut, 0, std::nullopt}}, {}};
  ASSERT_TRUE(store_->Commit(batch).ok());
  ASSERT_TRUE(store_->Close().ok());
  FactStoreOptions tiny_options{path_};
  tiny_options.storage_read_working_bytes = 1;
  FactStore tiny(tiny_options);
  const Status rejected = tiny.Open();
  EXPECT_TRUE(rejected.IsResourceExhausted()) << rejected.ToString();
  EXPECT_TRUE(store_->Open().ok());
}

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

// The bounded ScanFamily must read every partition of the requested family:
// partitions are discovered with (version, part+1) prefix seeks, empty
// partitions are skipped, part 0 is a real partition, and part 0xffffffff ends
// the version. The same walk must be correct while facts still live in the WAL
// memtable (crash recovery, adjacency rebuild) and after a clean close flushed
// them into Cedar Parquet SSTs.
TEST_F(FactStoreMetadataTest,
       ScanFamilyReadsEveryPartitionInMemtableAndFlushedSst) {
  using ScannedFact =
      std::tuple<uint32_t, uint8_t, uint16_t, uint64_t, uint64_t, uint64_t,
                 uint8_t>;
  const auto collect = [this](FactFamily family) {
    std::vector<ScannedFact> facts;
    const auto snapshot = store_->BeginSnapshot();
    EXPECT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    if (!snapshot.ok()) return facts;
    const Status status = store_->ScanFamily(
        snapshot.ValueOrDie(), family, [&facts](const FactEvent& event) {
          facts.emplace_back(event.ref.part_id().value,
                             static_cast<uint8_t>(event.ref.family()),
                             event.ref.property_id().value,
                             event.ref.entity_id(), event.valid_from.value,
                             event.commit_seq.value,
                             static_cast<uint8_t>(event.operation));
          return Status::OK();
        });
    EXPECT_TRUE(status.ok()) << status.ToString();
    return facts;
  };

  constexpr uint64_t kValidFrom = 10;
  constexpr uint32_t kMaxPart = std::numeric_limits<uint32_t>::max();
  const EdgeIdentity identity{EdgeId{17}, VertexId{7}, VertexId{11}, 3};
  StoreCommitBatch batch{TxnId{1}, kValidFrom, {}, {identity}, {}, {}, false};
  const std::array<std::pair<uint32_t, uint64_t>, 5> vertices = {
      {{0, 1}, {0, 2}, {2, 3}, {5, 4}, {kMaxPart, 5}}};
  for (const auto& [part, entity] : vertices) {
    batch.mutations.push_back(PendingFactMutation{
        EntityFact::Vertex(VertexRef{PartId{part}, VertexId{entity}}).ref(),
        ValidTime{kValidFrom}, FactOperation::kPut, 0, std::nullopt});
  }
  // The identity needs its edge-state assertion; it belongs to other families
  // and must not leak into the vertex-state scan.
  batch.mutations.push_back(PendingFactMutation{
      EntityFact::Edge(EdgeRef{PartId{0}, EdgeId{17}}).ref(),
      ValidTime{kValidFrom}, FactOperation::kPut, 0, std::nullopt});
  const auto committed = store_->Commit(batch);
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  const uint64_t commit_seq = committed.ValueOrDie().commit_seq.value;

  std::vector<ScannedFact> expected_vertices;
  expected_vertices.reserve(vertices.size());
  for (const auto& [part, entity] : vertices) {
    expected_vertices.emplace_back(part, 1, 0, entity, kValidFrom, commit_seq,
                                   1);
  }
  const std::vector<ScannedFact> expected_identities = {
      {0, 3, 0, 17, 0, commit_seq, 1}};

  // Memtable/WAL path: the bounded seeks must be understood by the facts
  // memtable index, not only by the Cedar Parquet tables.
  EXPECT_EQ(collect(FactFamily::kVertexState), expected_vertices);
  EXPECT_EQ(collect(FactFamily::kEdgeIdentity), expected_identities);
  EXPECT_TRUE(collect(FactFamily::kVertexProperty).empty());

  // Clean close flushes the facts CF into Cedar Parquet SSTs; the identical
  // partition walk must return the same rows through the table comparator.
  Reopen();
  EXPECT_EQ(collect(FactFamily::kVertexState), expected_vertices);
  EXPECT_EQ(collect(FactFamily::kEdgeIdentity), expected_identities);
  EXPECT_TRUE(collect(FactFamily::kVertexProperty).empty());
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
      // Full 32-byte identity: the rebuilt key must be byte-identical to the
      // key the writer encoded, not merely decode to the same fields.
      EXPECT_EQ(rebuilt, record.fact_keys[index]);
      const auto decoded_key = DecodeFactKey(rebuilt);
      ASSERT_TRUE(decoded_key.ok()) << decoded_key.status().ToString();
      EXPECT_EQ(decoded_key.ValueOrDie().commit_seq, record.commit_seq);
      EXPECT_EQ(EncodeFactKey(decoded_key.ValueOrDie().ref,
                              decoded_key.ValueOrDie().valid_from,
                              record.commit_seq),
                rebuilt);
    }
  }
}

TEST(FactStoreMetaCodecSequenceV2Test,
     DecodesV2RecordsAndRejectsCorruptRecords) {
  const SequenceRecord record = SequenceV2FixtureRecord(3);
  const auto v2 = EncodeSequenceRecordV2(record);
  ASSERT_TRUE(v2.ok()) << v2.status().ToString();
  const std::string v2_bytes = v2.ValueOrDie();

  const auto decoded_v2 =
      DecodeSequenceRecord(v2_bytes, record.commit_seq);
  ASSERT_TRUE(decoded_v2.ok()) << decoded_v2.status().ToString();
  EXPECT_EQ(decoded_v2.ValueOrDie(), record);

  // A v2 record does not carry commit_seq: the sequence key must be supplied.
  // A zero sequence (no usable key) is rejected rather than rebuilding wrong
  // fact keys.
  EXPECT_TRUE(
      DecodeSequenceRecord(v2_bytes, CommitSeq{0}).status().IsCorruption());
  EXPECT_TRUE(
      DecodeSequenceRecordView(v2_bytes, CommitSeq{0}).status().IsCorruption());

  // The view rebuilds the packed identities into its owned buffer and still
  // validates (duplicate/commit checks included).
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
  EXPECT_NE(view.ValueOrDie().owned_identities, nullptr);

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

  // A v2 payload re-tagged as a retired v1 sequence record is recognized as a
  // legacy kind-7 record and rejected as unsupported; it is never decoded as
  // either format.
  std::string retagged_v1 = v2_bytes;
  SetMetaRecordKind(&retagged_v1, 7);
  const auto legacy = DecodeSequenceRecord(retagged_v1, record.commit_seq);
  ASSERT_FALSE(legacy.ok());
  EXPECT_TRUE(legacy.status().IsNotSupportedError())
      << legacy.status().ToString();
  EXPECT_NE(legacy.status().ToString().find("v1"), std::string::npos)
      << legacy.status().ToString();
  EXPECT_NE(legacy.status().ToString().find("unsupported"), std::string::npos)
      << legacy.status().ToString();
  EXPECT_NE(legacy.status().ToString().find("Delete and recreate the database"),
            std::string::npos)
      << legacy.status().ToString();
  const auto legacy_view =
      DecodeSequenceRecordView(retagged_v1, record.commit_seq);
  ASSERT_FALSE(legacy_view.ok());
  EXPECT_TRUE(legacy_view.status().IsNotSupportedError())
      << legacy_view.status().ToString();

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

// ---------------------------------------------------------------------------
// Meta sequence format marker (v2-only)
// ---------------------------------------------------------------------------
// `MetaRecordKind` stays private to meta_codec.cc; these tests pin the on-disk
// kind bytes the same way the codec tests pin payload offsets.
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
  {
    const auto encoded = EncodeSequenceFormat(kMetaSequenceFormatV2);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    EXPECT_EQ(RawMetaRecordKind(encoded.ValueOrDie()),
              kRawSequenceFormatMarkerKind);
    // Payload is exactly format(4 BE) + the standard 6-byte meta header and
    // 4-byte CRC32C trailer.
    EXPECT_EQ(encoded.ValueOrDie().size(), 14U);
    const auto decoded = DecodeSequenceFormat(encoded.ValueOrDie());
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(decoded.ValueOrDie(), kMetaSequenceFormatV2);
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
       FreshDatabaseWritesMarkerAndV2RecordsForMultipleFacts) {
  const std::string path = path_ + "/fresh_v2";
  {
    FactStore store(FactStoreOptions{path});
    ASSERT_TRUE(store.Open().ok());
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
       DefaultV2WriterWritesDurableMarkerAndV2Records) {
  ASSERT_TRUE(store_->Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
  ASSERT_TRUE(store_->Close().ok());

  // A fresh database with default options is a v2 database: the marker is
  // written with the initialization batch, before the store accepts commits.
  const std::string marker = ReadMetaRecord(path_, EncodeSequenceFormatKey());
  ASSERT_FALSE(marker.empty());
  EXPECT_EQ(RawMetaRecordKind(marker), kRawSequenceFormatMarkerKind);
  const auto decoded_marker = DecodeSequenceFormat(marker);
  ASSERT_TRUE(decoded_marker.ok()) << decoded_marker.status().ToString();
  EXPECT_EQ(decoded_marker.ValueOrDie(), kMetaSequenceFormatV2);

  const auto sequence_key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(sequence_key.ok()) << sequence_key.status().ToString();
  const std::string record = ReadMetaRecord(path_, sequence_key.ValueOrDie());
  ASSERT_FALSE(record.empty());
  EXPECT_EQ(RawMetaRecordKind(record), kRawSequenceRecordKindV2);
  // v2 record: 6-byte header + 22-byte fixed payload + one 23-byte descriptor
  // + 4-byte CRC.
  EXPECT_EQ(record.size(), kRawMetaRecordHeaderBytes +
                               kSequenceV2FixedPayloadBytes +
                               kSequenceV2DescriptorBytes +
                               kRawMetaRecordChecksumBytes);
  const auto decoded = DecodeSequenceRecord(record, CommitSeq{1});
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  EXPECT_EQ(decoded.ValueOrDie().fact_keys.size(), 1U);
}

TEST_F(FactStoreMetadataTest, V1WriterOptionIsRejectedBeforeOpening) {
  const std::string path = path_ + "/explicit_v1";
  FactStore store(StoreOptionsAt(path, kMetaSequenceFormatV1));
  const Status opened = store.Open();
  ASSERT_FALSE(opened.ok());
  EXPECT_TRUE(opened.IsInvalidArgument()) << opened.ToString();
  EXPECT_NE(opened.ToString().find("no longer supported"), std::string::npos)
      << opened.ToString();
  EXPECT_NE(opened.ToString().find("format 2 (v2)"), std::string::npos)
      << opened.ToString();
  // The rejected open must not create a database at the requested path.
  EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(path) / "CURRENT"));
}

TEST_F(FactStoreMetadataTest,
       ReopenWithV1OptionIsRejectedEvenForAV2Database) {
  const std::string path = path_ + "/marker_authoritative";
  {
    FactStore store(FactStoreOptions{path});
    ASSERT_TRUE(store.Open().ok());
    ASSERT_TRUE(store.Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
    ASSERT_TRUE(store.Close().ok());
  }

  // The removed v1 option must not silently reopen an existing v2 database as
  // a read-only or degraded store: it is rejected before the engine is opened.
  FactStoreOptions options = StoreOptionsAt(path, kMetaSequenceFormatV1);
  FactStore store(options);
  const Status opened = store.Open();
  ASSERT_FALSE(opened.ok());
  EXPECT_TRUE(opened.IsInvalidArgument()) << opened.ToString();

  // The database itself is untouched and remains a valid v2 database.
  FactStore reopened{FactStoreOptions{path}};
  ASSERT_TRUE(reopened.Open().ok());
  {
    const auto snapshot = reopened.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto range = reopened.ReadSequenceRange(snapshot.ValueOrDie(),
                                                  CommitSeq{1}, CommitSeq{1});
    ASSERT_TRUE(range.ok()) << range.status().ToString();
    EXPECT_EQ(range.ValueOrDie().size(), 1U);
  }
  ASSERT_TRUE(reopened.Close().ok());
}

// A database written by this release always records marker 2. A marker that
// does not decode (CRC/truncation), a marker with a value this release does not
// know, and a legacy (missing or 1-valued) marker all fail Open before any
// commit is accepted.
TEST_F(FactStoreMetadataTest, CorruptUnsupportedOrLegacyMarkerFailsOpen) {
  const auto create_v2_database = [this](const std::string& name) {
    const std::string path = path_ + "/" + name;
    FactStore store(FactStoreOptions{path});
    EXPECT_TRUE(store.Open().ok());
    EXPECT_TRUE(store.Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
    EXPECT_TRUE(store.Close().ok());
    return path;
  };
  const std::string marker_key = EncodeSequenceFormatKey();

  // Structurally valid marker with a value this binary does not support.
  {
    const std::string path = create_v2_database("marker_unsupported");
    const auto marker = EncodeSequenceFormat(3);
    ASSERT_TRUE(marker.ok()) << marker.status().ToString();
    PutMetaRecord(path, marker_key, marker.ValueOrDie());
    FactStore store(FactStoreOptions{path});
    const Status opened = store.Open();
    EXPECT_TRUE(opened.IsCorruption()) << opened.ToString();
    EXPECT_NE(opened.ToString().find("unsupported meta sequence format marker: 3"),
              std::string::npos)
        << opened.ToString();
    if (opened.ok()) EXPECT_TRUE(store.Close().ok());
  }

  // CRC corruption.
  {
    const std::string path = create_v2_database("marker_bad_crc");
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
    const std::string path = create_v2_database("marker_truncated");
    const auto marker = EncodeSequenceFormat(kMetaSequenceFormatV2);
    ASSERT_TRUE(marker.ok()) << marker.status().ToString();
    const std::string bytes = marker.ValueOrDie();
    PutMetaRecord(path, marker_key, bytes.substr(0, bytes.size() - 2));
    FactStore store(FactStoreOptions{path});
    const Status opened = store.Open();
    EXPECT_TRUE(opened.IsCorruption()) << opened.ToString();
    if (opened.ok()) EXPECT_TRUE(store.Close().ok());
  }

  // A legacy v1 marker is refused as unsupported.
  {
    const std::string path = create_v2_database("marker_v1");
    const auto marker = EncodeSequenceFormat(kMetaSequenceFormatV1);
    ASSERT_TRUE(marker.ok()) << marker.status().ToString();
    PutMetaRecord(path, marker_key, marker.ValueOrDie());
    FactStore store(FactStoreOptions{path});
    const Status opened = store.Open();
    ASSERT_FALSE(opened.ok());
    EXPECT_TRUE(opened.IsNotSupportedError()) << opened.ToString();
    EXPECT_NE(opened.ToString().find("format 1 (v1)"), std::string::npos)
        << opened.ToString();
    EXPECT_NE(opened.ToString().find("unsupported"), std::string::npos)
        << opened.ToString();
    EXPECT_NE(opened.ToString().find("migration/downgrade is not supported"),
              std::string::npos)
        << opened.ToString();
    EXPECT_NE(opened.ToString().find("Delete and recreate the database"),
              std::string::npos)
        << opened.ToString();
    if (opened.ok()) EXPECT_TRUE(store.Close().ok());
  }

  // A database that predates the marker is a legacy v1 database and gets the
  // same unsupported-format error; it is never opened as an empty/v1 store.
  {
    const std::string path = create_v2_database("marker_missing");
    DeleteMetaRecord(path, marker_key);
    FactStore store(FactStoreOptions{path});
    const Status opened = store.Open();
    ASSERT_FALSE(opened.ok());
    EXPECT_TRUE(opened.IsNotSupportedError()) << opened.ToString();
    EXPECT_NE(opened.ToString().find("no durable meta sequence format marker"),
              std::string::npos)
        << opened.ToString();
    EXPECT_NE(opened.ToString().find("Delete and recreate the database"),
              std::string::npos)
        << opened.ToString();
    if (opened.ok()) EXPECT_TRUE(store.Close().ok());
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
