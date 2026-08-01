#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "cedar/fact/fact_store.h"
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

TEST_F(FactStoreMetadataTest, RejectsPropertyTypeChangeForExistingName) {
  ASSERT_TRUE(store_->RegisterProperty(
                        Property(PropertyId{9}, "name", PropertyEntityKind::kVertex,
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
