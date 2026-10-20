#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
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

TEST_F(FactStoreMetadataTest, KeepsVertexAndEdgeLeasesIndependent) {
  const auto vertices = store_->LeaseIds(IdKind::kVertex, 3);
  const auto edges = store_->LeaseIds(IdKind::kEdge, 2);
  ASSERT_TRUE(vertices.ok()) << vertices.status().ToString();
  ASSERT_TRUE(edges.ok()) << edges.status().ToString();
  EXPECT_EQ(vertices.ValueOrDie().first_id, 1U);
  EXPECT_EQ(edges.ValueOrDie().first_id, 1U);
  EXPECT_EQ(edges.ValueOrDie().kind, IdKind::kEdge);
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
