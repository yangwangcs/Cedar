#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/metadata.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/perf_level.h>

#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"
#include "storage/facts/fact_store.h"
#include "storage/rocks/rocksdb_config.h"
#if defined(CEDAR_ROCKSDB_SYNC_POINTS)
#include "test_util/sync_point.h"
#endif

namespace cedar {
namespace {

FactRef Vertex(uint64_t id, uint32_t part = 0) {
  return EntityFact::Vertex(VertexRef{PartId{part}, VertexId{id}}).ref();
}

StoreCommitBatch Put(uint64_t txn, uint64_t vertex, uint64_t time) {
  return {TxnId{txn}, txn,
          {{Vertex(vertex), ValidTime{time}, FactOperation::kPut, 0,
            std::nullopt}}, {}};
}

struct RawStore {
  std::unique_ptr<rocksdb::DB> db;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;

  ~RawStore() {
    for (auto* handle : handles) db->DestroyColumnFamilyHandle(handle);
  }

  rocksdb::Status Open(const std::string& path) {
    FactStoreOptions options{path};
    std::shared_ptr<rocksdb::Cache> cache;
    auto engine_options = internal::MakeRocksDbOptions(
        options, false, nullptr, &cache);
    engine_options.avoid_flush_during_shutdown = true;
    engine_options.avoid_flush_during_recovery = true;
    auto metrics = std::make_shared<
        rocksdb::cedar_parquet::CanonicalPageCacheMetrics>();
    auto descriptors = internal::MakeRocksDbColumnFamilyDescriptors(
        options, engine_options, nullptr, {cache, metrics});
    return rocksdb::DB::Open(engine_options, path, descriptors, &handles, &db);
  }
};

class StorageReviewTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_storage_review_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path = pattern;
  }
  void TearDown() override { std::filesystem::remove_all(path); }
  std::string path;
};

EdgeIdentity ReviewEdge(uint32_t part, uint64_t id) {
  return {EdgeRef{PartId{part}, EdgeId{id}},
          VertexRef{PartId{part}, VertexId{11}},
          VertexRef{PartId{part}, VertexId{12}}, 1};
}

PendingFactMutation ReviewEdgeState(uint32_t part, uint64_t id,
                                   FactOperation operation = FactOperation::kPut) {
  return {EntityFact::Edge(EdgeRef{PartId{part}, EdgeId{id}}).ref(),
          ValidTime{10}, operation, 0, std::nullopt};
}

TEST_F(StorageReviewTest, SameEdgeIdInDifferentHomePartsIsIndependent) {
  FactStore store(FactStoreOptions{path});
  ASSERT_TRUE(store.Open().ok());
  const StoreCommitBatch batch{TxnId{1}, 1,
      {ReviewEdgeState(1, 7), ReviewEdgeState(2, 7)},
      {ReviewEdge(1, 7), ReviewEdge(2, 7)}};
  ASSERT_TRUE(batch.Validate().ok());
  const auto committed = store.Commit(batch);
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  const auto retry = store.Commit(batch);
  ASSERT_TRUE(retry.ok()) << retry.status().ToString();
  EXPECT_EQ(retry.ValueOrDie().commit_seq, committed.ValueOrDie().commit_seq);
  auto reordered = batch;
  std::swap(reordered.mutations[0], reordered.mutations[1]);
  EXPECT_TRUE(store.Commit(reordered).status().IsConflict());
}

TEST_F(StorageReviewTest, MetadataPressureEvictsIdleReadersAndPreservesPinnedIterators) {
  FactStoreOptions settings{path};
  auto metadata = std::make_shared<rocksdb::cedar_parquet::CedarParquetReadBudget>(
      96 << 10, rocksdb::cedar_parquet::CedarParquetReadBudgetKind::kMetadata);
  auto metrics = std::make_shared<rocksdb::cedar_parquet::CanonicalPageCacheMetrics>();
  auto engine_options = internal::MakeRocksDbOptions(settings, true);
  engine_options.max_open_files = 100;
  auto descriptors = internal::MakeRocksDbColumnFamilyDescriptors(
      settings, engine_options, nullptr, {{}, metrics, {}, metadata});
  descriptors[1].options.disable_auto_compactions = true;
  RawStore raw;
  ASSERT_TRUE(rocksdb::DB::Open(engine_options, path, descriptors, &raw.handles, &raw.db).ok());
  std::vector<std::string> keys, values;
  std::unique_ptr<rocksdb::Iterator> pinned;
  std::string pinned_key;
  rocksdb::FlushOptions flush;
  flush.wait = true;
  for (uint64_t id = 1; id <= 12; ++id) {
    keys.push_back(EncodeFactKey(Vertex(id), ValidTime{10}, CommitSeq{id}));
    auto encoded = EncodeFactValue({Vertex(id), ValidTime{10}, CommitSeq{id},
        FactOperation::kPut, 0, std::nullopt});
    ASSERT_TRUE(encoded.ok());
    values.push_back(encoded.ConsumeValueOrDie());
    ASSERT_TRUE(raw.db->Put(rocksdb::WriteOptions(), raw.handles[1], keys.back(), values.back()).ok());
    const auto flushed = raw.db->Flush(flush, raw.handles[1]);
    ASSERT_TRUE(flushed.ok()) << flushed.ToString() << " id=" << id
        << " metadata=" << metadata->current_bytes()
        << " peak=" << metadata->peak_bytes() << " limit=" << metadata->limit_bytes();
    if (id == 1) {
      pinned.reset(raw.db->NewIterator(rocksdb::ReadOptions(), raw.handles[1]));
      pinned->SeekToFirst();
      ASSERT_TRUE(pinned->Valid()) << pinned->status().ToString();
      pinned_key = pinned->key().ToString();
    }
    ASSERT_TRUE(pinned->Valid());
    EXPECT_EQ(pinned->key().ToString(), pinned_key);
  }
  const uint64_t rejected_before = metadata->rejections();
  for (unsigned round = 0; round < 3; ++round) {
    for (size_t index = 0; index < keys.size(); ++index) {
      std::string value;
      const auto status = raw.db->Get(rocksdb::ReadOptions(), raw.handles[1], keys[index], &value);
      ASSERT_TRUE(status.ok()) << status.ToString();
      EXPECT_EQ(value, values[index]);
    }
  }
  EXPECT_GT(metadata->rejections(), rejected_before);
  EXPECT_LE(metadata->peak_bytes(), metadata->limit_bytes());
  // The first reader stayed pinned while later idle readers were reclaimed.
  ASSERT_TRUE(pinned->Valid());
  EXPECT_EQ(pinned->key().ToString(), pinned_key);
  EXPECT_TRUE(pinned->status().ok());
  pinned.reset();
  for (auto* handle : raw.handles) ASSERT_TRUE(raw.db->DestroyColumnFamilyHandle(handle).ok());
  raw.handles.clear();
  raw.db.reset();
  EXPECT_EQ(metadata->current_bytes(), 0U);
  EXPECT_EQ(metrics->reader_metadata_bytes.load(), 0U);
}

TEST_F(StorageReviewTest, IdentityStatePairingRequiresFullEdgeRef) {
  const StoreCommitBatch batch{TxnId{1}, 1,
      {ReviewEdgeState(2, 7)}, {ReviewEdge(1, 7)}};
  EXPECT_FALSE(batch.Validate().ok());
}

TEST_F(StorageReviewTest, ExistingEdgeMutationRetryIsIdempotent) {
  FactStore store(FactStoreOptions{path});
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.Commit(StoreCommitBatch{TxnId{1}, 1,
      {ReviewEdgeState(0, 7)}, {ReviewEdge(0, 7)}}).ok());
  for (const auto operation : {FactOperation::kDelete, FactOperation::kPut}) {
    const uint64_t txn = operation == FactOperation::kDelete ? 2 : 3;
    StoreCommitBatch batch{TxnId{txn}, txn,
        {ReviewEdgeState(0, 7, operation)}, {}};
    const auto committed = store.Commit(batch);
    ASSERT_TRUE(committed.ok()) << committed.status().ToString();
    const auto retry = store.Commit(batch);
    ASSERT_TRUE(retry.ok()) << retry.status().ToString();
    EXPECT_EQ(retry.ValueOrDie().commit_seq, committed.ValueOrDie().commit_seq);
    batch.mutations[0].valid_from = ValidTime{11};
    EXPECT_TRUE(store.Commit(batch).status().IsConflict());
  }
  StoreCommitBatch property{TxnId{4}, 4,
      {{PropertyFact::Edge(EdgeRef{PartId{0}, EdgeId{7}}, PropertyId{7}).ref(),
        ValidTime{20}, FactOperation::kPut, 1, Value::Int64(42)}}, {}};
  const auto committed = store.Commit(property);
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  const auto retry = store.Commit(property);
  ASSERT_TRUE(retry.ok()) << retry.status().ToString();
  EXPECT_EQ(retry.ValueOrDie().commit_seq, committed.ValueOrDie().commit_seq);
  property.mutations[0].value = Value::Int64(43);
  EXPECT_TRUE(store.Commit(property).status().IsConflict());
}

TEST_F(StorageReviewTest, GeneratedIdentityIsVisibleToHistoryBloomImmediately) {
  FactStore store(FactStoreOptions{path});
  ASSERT_TRUE(store.Open().ok());
  const StoreCommitBatch batch{TxnId{1}, 1,
      {ReviewEdgeState(1, 7)}, {ReviewEdge(1, 7)}};
  const FactRef ref(PartId{1}, FactFamily::kEdgeIdentity, PropertyId{}, 7);
  const auto check = [&] {
    auto snapshot = store.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok());
    const auto point = store.Read(snapshot.ValueOrDie(), ref, ValidTime{10});
    ASSERT_TRUE(point.ok());
    ASSERT_TRUE(point.ValueOrDie().has_value());
    const auto history = store.ReadTemporalNeighborhood(
        snapshot.ValueOrDie(), ref, ValidTime{10});
    ASSERT_TRUE(history.ok());
    ASSERT_TRUE(history.ValueOrDie().observed.has_value());
    EXPECT_EQ(history.ValueOrDie().observed->edge_identity,
              point.ValueOrDie()->edge_identity);
  };
  ASSERT_TRUE(store.Commit(batch).ok());
  check();
  ASSERT_TRUE(store.Commit(batch).ok());
  check();
  ASSERT_TRUE(store.Close().ok());
  ASSERT_TRUE(store.Open().ok());
  check();
}

TEST_F(StorageReviewTest, FailedCommitDoesNotPublishGeneratedFacts) {
  bool fail = false;
  FactStoreOptions options{path};
  options.commit_prewrite_fault_injector_for_testing = [&] {
    return fail ? Status::IOError("test", "before durable write") : Status::OK();
  };
  FactStore store(options);
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.Commit(StoreCommitBatch{TxnId{1}, 1,
      {ReviewEdgeState(1, 7)}, {ReviewEdge(1, 7)}}).ok());
  fail = true;
  EXPECT_FALSE(store.Commit(StoreCommitBatch{TxnId{2}, 2,
      {ReviewEdgeState(2, 8)}, {ReviewEdge(2, 8)}}).ok());
  auto snapshot = store.BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  EXPECT_EQ(snapshot.ValueOrDie().commit_seq(), CommitSeq{1});
  for (const auto& [part, id, present] :
       {std::tuple{1U, 7ULL, true}, std::tuple{2U, 8ULL, false}}) {
    const FactRef ref(PartId{part}, FactFamily::kEdgeIdentity, PropertyId{}, id);
    const auto history = store.ReadTemporalNeighborhood(
        snapshot.ValueOrDie(), ref, ValidTime{10});
    ASSERT_TRUE(history.ok());
    EXPECT_EQ(history.ValueOrDie().observed.has_value(), present);
  }
}

TEST_F(StorageReviewTest, FamilyScanIncludesCompactedAndNewFacts) {
  FactStore store(FactStoreOptions{path});
  ASSERT_TRUE(store.Open().ok());
  StoreCommitBatch seed{TxnId{1}, 1, {}, {}};
  uint64_t edge_id = 2;
  for (uint32_t part : {0U, 7U, std::numeric_limits<uint32_t>::max()}) {
    seed.mutations.push_back({Vertex(1, part), ValidTime{10},
                              FactOperation::kPut, 0, std::nullopt});
    const EdgeRef edge_ref{PartId{part}, EdgeId{edge_id}};
    seed.mutations.push_back({
        EntityFact::Edge(edge_ref).ref(),
        ValidTime{10}, FactOperation::kPut, 0, std::nullopt});
    seed.edge_identities.emplace_back(
        edge_ref, VertexRef{PartId{part}, VertexId{edge_id * 2}},
        VertexRef{PartId{part}, VertexId{edge_id * 2 + 1}}, 1);
    ++edge_id;
  }
  ASSERT_TRUE(store.Commit(seed).ok());
  ASSERT_TRUE(store.Close().ok());
  {
    RawStore raw;
    ASSERT_TRUE(raw.Open(path).ok());
    rocksdb::CompactRangeOptions compact;
    compact.change_level = true;
    compact.target_level = 2;
    ASSERT_TRUE(raw.db->CompactRange(
        compact, raw.handles[1], nullptr, nullptr).ok());
    rocksdb::ColumnFamilyMetaData metadata;
    raw.db->GetColumnFamilyMetaData(raw.handles[1], &metadata);
    ASSERT_FALSE(metadata.levels[2].files.empty());
  }
  ASSERT_TRUE(store.Open().ok());
  const auto collect = [&store](const StoreSnapshot& snapshot,
                                FactFamily family) {
    std::vector<uint32_t> parts;
    const auto status = store.ScanFamily(snapshot, family,
        [&](const FactEvent& event) {
          parts.push_back(event.ref.part_id().value);
          return Status::OK();
        });
    EXPECT_TRUE(status.ok()) << status.ToString();
    return parts;
  };
  {
    const auto snapshot = store.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok());
    EXPECT_EQ(collect(snapshot.ValueOrDie(), FactFamily::kVertexState),
              (std::vector<uint32_t>{0, 7, 0xffffffffU}));
    EXPECT_EQ(collect(snapshot.ValueOrDie(), FactFamily::kEdgeState),
              (std::vector<uint32_t>{0, 7, 0xffffffffU}));
    EXPECT_EQ(collect(snapshot.ValueOrDie(), FactFamily::kEdgeIdentity),
              (std::vector<uint32_t>{0, 7, 0xffffffffU}));
  }
  ASSERT_TRUE(store.Commit(Put(2, 3, 20)).ok());
  {
    const auto snapshot = store.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok());
    EXPECT_EQ(collect(snapshot.ValueOrDie(), FactFamily::kVertexState),
              (std::vector<uint32_t>{0, 0, 7, 0xffffffffU}));
  }
  ASSERT_TRUE(store.Close().ok());
  ASSERT_TRUE(store.Open().ok());
  {
    const auto snapshot = store.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok());
    EXPECT_EQ(collect(snapshot.ValueOrDie(), FactFamily::kVertexState),
              (std::vector<uint32_t>{0, 0, 7, 0xffffffffU}));
  }
}

TEST_F(StorageReviewTest, DefaultReopenRejectsMissingOldDelete) {
  FactStoreOptions options{path};
  options.recovery_validation_recent_sequences = 1;
  FactStore store(options);
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.Commit(Put(1, 1, 10)).ok());
  auto deleted = Put(2, 1, 20);
  deleted.mutations.front().operation = FactOperation::kDelete;
  ASSERT_TRUE(store.Commit(deleted).ok());
  ASSERT_TRUE(store.Commit(Put(3, 2, 10)).ok());
  ASSERT_TRUE(store.Close().ok());
  {
    RawStore raw;
    ASSERT_TRUE(raw.Open(path).ok());
    ASSERT_TRUE(raw.db->Delete(rocksdb::WriteOptions(), raw.handles[1],
        EncodeFactKey(Vertex(1), ValidTime{20}, CommitSeq{2})).ok());
  }
  const auto reopened = store.Open();
  EXPECT_TRUE(reopened.IsCorruption()) << reopened.ToString();
}

TEST_F(StorageReviewTest, PrunedCommitRetryReportsExpiredHistory) {
  FactStoreOptions options{path};
  options.prune_sequence_records_on_vacuum = true;
  FactStore store(options);
  ASSERT_TRUE(store.Open().ok());
  const auto first = Put(1, 1, 10);
  ASSERT_TRUE(store.Commit(first).ok());
  ASSERT_TRUE(store.Commit(Put(2, 2, 10)).ok());
  ASSERT_TRUE(store.Commit(first).ok());
  ASSERT_TRUE(store.Vacuum(CommitSeq{2}).ok());
  const auto retry = store.Commit(first);
  EXPECT_TRUE(retry.status().IsSnapshotExpired()) << retry.status().ToString();
  EXPECT_TRUE(store.ResolveTransaction(TxnId{1}).status().IsSnapshotExpired());
  ASSERT_TRUE(store.Commit(Put(2, 2, 10)).ok());
}

TEST_F(StorageReviewTest, DefaultReopenRejectsMissingOldCorrection) {
  FactStoreOptions options{path};
  options.recovery_validation_recent_sequences = 1;
  FactStore store(options);
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.Commit(Put(1, 1, 10)).ok());
  auto deleted = Put(2, 1, 20);
  deleted.mutations.front().operation = FactOperation::kDelete;
  ASSERT_TRUE(store.Commit(deleted).ok());
  // The newest commit at t=20 corrects the earlier DELETE to PUT.
  ASSERT_TRUE(store.Commit(Put(3, 1, 20)).ok());
  ASSERT_TRUE(store.Commit(Put(4, 2, 10)).ok());
  ASSERT_TRUE(store.Close().ok());
  {
    RawStore raw;
    ASSERT_TRUE(raw.Open(path).ok());
    ASSERT_TRUE(raw.db->Delete(rocksdb::WriteOptions(), raw.handles[1],
        EncodeFactKey(Vertex(1), ValidTime{20}, CommitSeq{3})).ok());
  }
  const auto reopened = store.Open();
  EXPECT_TRUE(reopened.IsCorruption()) << reopened.ToString();
}

TEST_F(StorageReviewTest, ResolutionWaitsForPublicationOrReopenRecovery) {
  FactStoreOptions options{path};
  options.commit_fault_injector_for_testing = [] {
    return Status::Indeterminate("test", "durable write was not published");
  };
  FactStore store(options);
  ASSERT_TRUE(store.Open().ok());
  const auto committed = store.Commit(Put(1, 1, 10));
  ASSERT_TRUE(committed.status().IsIndeterminate())
      << committed.status().ToString();
  EXPECT_EQ(store.visible_seq(), CommitSeq{0});
  const auto unpublished = store.ResolveTransaction(TxnId{1});
  ASSERT_TRUE(unpublished.ok()) << unpublished.status().ToString();
  EXPECT_FALSE(unpublished.ValueOrDie().has_value());
  ASSERT_TRUE(store.Close().ok());
  ASSERT_TRUE(store.Open().ok());
  const auto recovered = store.ResolveTransaction(TxnId{1});
  ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
  ASSERT_TRUE(recovered.ValueOrDie().has_value());
  EXPECT_EQ(recovered.ValueOrDie()->commit_seq, CommitSeq{1});
}

TEST_F(StorageReviewTest, SequencePruneDoesNotRescanEarlierDeleteBatches) {
  FactStoreOptions options{path};
  {
    FactStore store(options);
    ASSERT_TRUE(store.Open().ok());
    ASSERT_TRUE(store.Close().ok());
  }
  // A completed fact Vacuum with its optional sequence prune still pending.
  constexpr uint64_t kSequences = 4097;
  {
    RawStore raw;
    ASSERT_TRUE(raw.Open(path).ok());
    rocksdb::WriteBatch batch;
    for (uint64_t seq = 1; seq <= kSequences; ++seq) {
      const auto key = EncodeSequenceMetaKey(CommitSeq{seq});
      const auto value = EncodeSequenceRecordV2({CommitSeq{seq}, TxnId{seq}, seq,
          {EncodeFactKey(Vertex(1), ValidTime{10}, CommitSeq{seq})}});
      const auto txn_key = EncodeTransactionMetaKey(TxnId{seq});
      const auto txn_value = EncodeTransactionOutcome({TxnId{seq}, CommitSeq{seq},
          TransactionOutcome::kCommitted});
      ASSERT_TRUE(value.ok());
      ASSERT_TRUE(txn_value.ok());
      batch.Put(raw.handles[2], key.ValueOrDie(), value.ValueOrDie());
      batch.Put(raw.handles[2], txn_key.ValueOrDie(), txn_value.ValueOrDie());
    }
    const auto fact = EncodeFactValue({Vertex(1), ValidTime{10},
        CommitSeq{kSequences}, FactOperation::kPut, 0, std::nullopt, std::nullopt});
    ASSERT_TRUE(fact.ok());
    batch.Put(raw.handles[1], EncodeFactKey(Vertex(1), ValidTime{10},
        CommitSeq{kSequences}), fact.ValueOrDie());
    const auto boundary = EncodeWatermark(CommitSeq{kSequences});
    batch.Put(raw.handles[2], EncodeVisibleWatermarkKey(), boundary.ValueOrDie());
    batch.Put(raw.handles[2], EncodeOldestReadableWatermarkKey(), boundary.ValueOrDie());
    ASSERT_TRUE(raw.db->Write(rocksdb::WriteOptions(), &batch).ok());
  }
  const auto previous_level = rocksdb::GetPerfLevel();
  uint64_t skipped = 0;
  unsigned batches = 0;
  options.prune_sequence_records_on_vacuum = true;
  options.sequence_prune_sync = false;
  options.vacuum_fault_injector_for_testing = [&](VacuumFaultPoint point) {
    if (point == VacuumFaultPoint::kBeforeSequencePruneWatermark) {
      rocksdb::SetPerfLevel(rocksdb::kEnableCount);
      rocksdb::get_perf_context()->Reset();
    }
    if (point == VacuumFaultPoint::kAfterSequencePruneBatch) ++batches;
    if (point == VacuumFaultPoint::kBeforeSequencePruneCompletion) {
      skipped = rocksdb::get_perf_context()->internal_delete_skipped_count;
      rocksdb::SetPerfLevel(previous_level);
    }
    return Status::OK();
  };
  FactStore store(options);
  const auto opened = store.Open();
  rocksdb::SetPerfLevel(previous_level);
  ASSERT_TRUE(opened.ok()) << opened.ToString();
  EXPECT_EQ(batches, 4U);
  EXPECT_LE(skipped, kSequences);
  const auto snapshot = store.BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  EXPECT_TRUE(store.ReadSequence(snapshot.ValueOrDie(), CommitSeq{1})
                  .status().IsSnapshotExpired());
  EXPECT_TRUE(store.ReadSequence(snapshot.ValueOrDie(), CommitSeq{kSequences}).ok());
}

#if defined(CEDAR_ROCKSDB_SYNC_POINTS)
thread_local unsigned resolver_get_count = 0;
thread_local bool is_resolver = false;
#endif

TEST_F(StorageReviewTest, ConcurrentPruneDoesNotTurnResolutionIntoCorruption) {
#if !defined(CEDAR_ROCKSDB_SYNC_POINTS)
  GTEST_SKIP() << "requires RocksDB Debug SyncPoints";
#else
  FactStoreOptions options{path};
  options.prune_sequence_records_on_vacuum = true;
  FactStore store(options);
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.Commit(Put(1, 1, 10)).ok());
  ASSERT_TRUE(store.Commit(Put(2, 2, 10)).ok());
  std::mutex mutex;
  std::condition_variable cv;
  bool parked = false;
  bool release = false;
  auto* points = rocksdb::SyncPoint::GetInstance();
  points->SetCallBack("DBImpl::GetImpl:AfterAcquireSv", [&](void*) {
    if (!is_resolver || ++resolver_get_count != 2) return;
    std::unique_lock lock(mutex);
    parked = true;
    cv.notify_all();
    cv.wait_for(lock, std::chrono::seconds(15), [&] { return release; });
  });
  points->EnableProcessing();
  auto result = std::async(std::launch::async, [&] {
    is_resolver = true;
    resolver_get_count = 0;
    return store.ResolveTransaction(TxnId{1});
  });
  bool reached;
  {
    std::unique_lock lock(mutex);
    reached = cv.wait_for(lock, std::chrono::seconds(5), [&] { return parked; });
  }
  if (reached) EXPECT_TRUE(store.Vacuum(CommitSeq{2}).ok());
  {
    std::lock_guard lock(mutex);
    release = true;
    cv.notify_all();
  }
  const auto resolved = result.get();
  points->DisableProcessing();
  points->ClearAllCallBacks();
  ASSERT_TRUE(reached);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  ASSERT_TRUE(resolved.ValueOrDie().has_value());
  EXPECT_EQ(resolved.ValueOrDie()->commit_seq, CommitSeq{1});
#endif
}

}  // namespace
}  // namespace cedar
