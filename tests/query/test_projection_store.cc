#include <filesystem>
#include <fstream>

#include <gtest/gtest.h>

#include "cedar/core/crc32c.h"
#include "query/projection/projection_store.h"

namespace cedar::internal {
namespace {
class ProjectionStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = (std::filesystem::temp_directory_path() / "cedar_projection_store_test").string();
    std::filesystem::remove_all(path_);
    ASSERT_TRUE(std::filesystem::create_directories(path_));
  }
  void TearDown() override { std::filesystem::remove_all(path_); }
  ProjectionBuild Build(uint64_t generation, const std::string& id = "seg-a") {
    ProjectionBuild b;
    b.manifest.database_identity = "test-db";
    b.manifest.generation_id = generation;
    b.manifest.base_seq = CommitSeq{generation};
    CoverageRegion r;
    r.kind = ProjectionKind::kState;
    r.part_id = PartId{1};
    r.schema_epoch = 1;
    r.entity_min = 1;
    r.entity_max_exclusive = 100;
    r.valid_time = ValidTimeInterval{ValidTime{0}, std::nullopt};
    SegmentDescriptor d;
    d.segment_id = id;
    d.filename = id + ".csegment";
    d.header.kind = ProjectionKind::kState;
    d.header.generation_id = generation;
    d.header.base_seq = CommitSeq{generation};
    d.header.part_id = PartId{1};
    d.header.schema_epoch = 1;
    d.header.entity_min = 1;
    d.header.entity_max_exclusive = 100;
    d.header.valid_from_min = ValidTime{0};
    d.file_bytes = 4;
    d.checksum = crc32c::Value("data", 4);
    b.segments.push_back(ProjectionSegmentInput{d, "data"});
    r.segments.push_back(d);
    b.manifest.regions.push_back(std::move(r));
    return b;
  }
  std::string path_;
};

TEST_F(ProjectionStoreTest, OrphanSegmentDoesNotImplyCoverage) {
  std::ofstream(path_ + "/orphan.csegment") << "data";
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  CoverageRequest request;
  request.part_id = PartId{1}; request.schema_epoch = 1;
  request.entity_min = 1; request.entity_max_exclusive = 2;
  request.valid_time = ValidTimeInterval{ValidTime{0}, std::nullopt};
  EXPECT_FALSE(opened.ValueOrDie()->Acquire(request).has_value());
}

TEST_F(ProjectionStoreTest, PublishesAndReopensCurrentManifest) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  ASSERT_TRUE(opened.ValueOrDie()->Build(Build(10)).ok());
  CoverageRequest request;
  request.part_id = PartId{1}; request.schema_epoch = 1; request.entity_min = 1; request.entity_max_exclusive = 2;
  request.valid_time = ValidTimeInterval{ValidTime{0}, std::nullopt}; request.snapshot_seq = CommitSeq{10};
  EXPECT_TRUE(opened.ValueOrDie()->Acquire(request).has_value());
  opened.ValueOrDie().reset();
  auto reopened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(reopened.ok());
  EXPECT_EQ(reopened.ValueOrDie()->current_generation_id(), std::optional<uint64_t>(10));
}

TEST_F(ProjectionStoreTest, OldReaderPinsRetiredGeneration) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok()); auto& store = *opened.ValueOrDie();
  ASSERT_TRUE(store.Build(Build(10)).ok());
  CoverageRequest request; request.part_id=PartId{1}; request.schema_epoch=1; request.entity_min=1; request.entity_max_exclusive=2; request.valid_time={ValidTime{0},std::nullopt}; request.snapshot_seq=CommitSeq{10};
  auto pin = store.Acquire(request); ASSERT_TRUE(pin.has_value());
  ASSERT_TRUE(store.Build(Build(20, "seg-b")).ok());
  EXPECT_TRUE(pin->exists());
  pin.reset(); store.CollectRetired();
  EXPECT_FALSE(std::filesystem::exists(path_ + "/seg-a.csegment"));
}

TEST_F(ProjectionStoreTest, BadCurrentDisablesProjections) {
  std::filesystem::create_directories(path_ + "/manifests");
  std::ofstream(path_ + "/PROJECTION-CURRENT") << "bad";
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  EXPECT_FALSE(opened.ValueOrDie()->projections_enabled());
}
}  // namespace
}  // namespace cedar::internal
