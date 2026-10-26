#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <gtest/gtest.h>

#include "cedar/core/crc32c.h"
#include "query/projection/projection_store.h"
#include "query/runtime/relational.h"

namespace cedar::internal {
namespace {
class ProjectionStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // ctest runs each TEST as its own process. A fixed path lets parallel jobs
    // delete each other's directories, so use a unique directory per test.
    char pattern[] = "/tmp/cedar_projection_store_test_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
  }
  void TearDown() override { std::filesystem::remove_all(path_); }
  ProjectionBuild Build(uint64_t generation, const std::string& id = "seg-a",
                        uint64_t base_seq = 0) {
    if (base_seq == 0) base_seq = generation;
    ProjectionBuild b;
    b.manifest.database_identity = "test-db";
    b.manifest.generation_id = generation;
    b.manifest.base_seq = CommitSeq{base_seq};
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
    d.header.base_seq = CommitSeq{base_seq};
    d.header.part_id = PartId{1};
    d.header.schema_epoch = 1;
    d.header.entity_min = 1;
    d.header.entity_max_exclusive = 100;
    d.header.valid_from_min = ValidTime{0};
    d.file_bytes = 4;
    ProjectionChain chain;
    chain.header = d.header;
    chain.intervals.push_back(ProjectionInterval{ValidTimeInterval{ValidTime{0}, std::nullopt}, Value::Int64(1), 1});
    auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
    if (!encoded.ok()) return {};
    d.file_bytes = encoded.ValueOrDie().size();
    d.checksum = crc32c::Value(encoded.ValueOrDie().data(), encoded.ValueOrDie().size());
    b.segments.push_back(ProjectionSegmentInput{d, encoded.ValueOrDie()});
    r.segments.push_back(d);
    b.manifest.regions.push_back(std::move(r));
    return b;
  }
  ProjectionBuild BuildPropertyIndex(uint64_t generation, uint64_t count = 1,
                                      size_t value_bytes = 2) {
    ProjectionBuild b;
    b.manifest.database_identity = "test-db";
    b.manifest.generation_id = generation;
    b.manifest.base_seq = CommitSeq{generation};
    CoverageRegion r;
    r.kind = ProjectionKind::kPropertyIndex;
    r.part_id = PartId{1};
    r.property_id = PropertyId{7};
    r.schema_epoch = 1;
    r.entity_min = 1;
    r.entity_max_exclusive = std::max<uint64_t>(100, 42 + count);
    r.valid_time = {ValidTime{0}, std::nullopt};
    r.built_through = CommitSeq{generation};
    SegmentDescriptor d;
    d.segment_id = "property-index";
    d.filename = "property-index.cpi";
    d.header.kind = ProjectionKind::kPropertyIndex;
    d.header.generation_id = generation;
    d.header.base_seq = CommitSeq{generation};
    d.header.part_id = PartId{1};
    d.header.property_id = PropertyId{7};
    d.header.schema_epoch = 1;
    d.header.entity_min = 1;
    d.header.entity_max_exclusive = r.entity_max_exclusive;
    d.header.valid_from_min = ValidTime{0};
    PropertyIndexSegment index;
    index.generation_id = generation;
    index.base_seq = CommitSeq{generation};
    index.built_through = CommitSeq{generation};
    index.property = PropertyId{7};
    index.part_id = PartId{1};
    index.schema_epoch = 1;
    for (uint64_t row = 0; row < count; ++row) {
      index.postings.push_back({VertexRef{PartId{1}, VertexId{42 + row}},
          ValidTimeInterval{ValidTime{0}, std::nullopt}, CommitSeq{generation},
          Value::String(value_bytes == 2 ? "CN" : std::string(value_bytes, 'x'))});
    }
    auto encoded = EncodePropertyIndexSegment(index);
    if (!encoded.ok()) return {};
    d.file_bytes = encoded.ValueOrDie().size();
    d.checksum = crc32c::Value(encoded.ValueOrDie().data(), encoded.ValueOrDie().size());
    b.segments.push_back({d, encoded.ValueOrDie()});
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

TEST_F(ProjectionStoreTest, PublishesAndReadsPropertyIndexGeneration) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  const auto property_build = opened.ValueOrDie()->Build(BuildPropertyIndex(10));
  ASSERT_TRUE(property_build.ok()) << property_build.ToString();
  CoverageRequest request;
  request.kind = ProjectionKind::kPropertyIndex;
  request.part_id = PartId{1};
  request.property_id = PropertyId{7};
  request.schema_epoch = 1;
  request.entity_min = 1;
  request.entity_max_exclusive = 100;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{10};
  request.generation_id = 10;
  request.database_identity = "test-db";
  auto generation = opened.ValueOrDie()->Acquire(request);
  ASSERT_TRUE(generation.has_value());
  auto index = opened.ValueOrDie()->ReadPropertyIndex(request, *generation);
  ASSERT_TRUE(index.ok()) << index.status().ToString();
  ASSERT_EQ(index.ValueOrDie().postings.size(), 1U);
  EXPECT_EQ(index.ValueOrDie().postings.front().vertex.vertex_id.value, 42U);
  EXPECT_EQ(index.ValueOrDie().postings.front().value, Value::String("CN"));
  opened.ValueOrDie().reset();
  auto reopened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(reopened.ok());
  auto pin = reopened.ValueOrDie()->Acquire(request);
  ASSERT_TRUE(pin.has_value());
  auto reopened_index = reopened.ValueOrDie()->ReadPropertyIndex(request, *pin);
  ASSERT_TRUE(reopened_index.ok());
  EXPECT_EQ(reopened_index.ValueOrDie().postings.front().vertex.vertex_id.value, 42U);
}

TEST_F(ProjectionStoreTest, PropertyIndexBudgetFailureDoesNotPoisonGeneration) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  ASSERT_TRUE(opened.ValueOrDie()->Build(BuildPropertyIndex(10, 512, 256)).ok());
  CoverageRequest request;
  request.kind = ProjectionKind::kPropertyIndex;
  request.part_id = PartId{1};
  request.property_id = PropertyId{7};
  request.schema_epoch = 1;
  request.entity_min = 1;
  request.entity_max_exclusive = 554;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{10};
  request.generation_id = 10;
  request.database_identity = "test-db";
  auto generation = opened.ValueOrDie()->Acquire(request);
  ASSERT_TRUE(generation.has_value());
  QueryReservation small(256 << 10);
  auto rejected = opened.ValueOrDie()->ReadBudgetedPropertyIndex(request, *generation, small);
  ASSERT_FALSE(rejected.ok());
  EXPECT_TRUE(rejected.status().IsResourceExhausted()) << rejected.status().ToString();
  EXPECT_EQ(small.used(ResourceDimension::kMemory), 0U);
  QueryReservation reservation(8 << 20);
  auto read = opened.ValueOrDie()->ReadBudgetedPropertyIndex(request, *generation, reservation);
  ASSERT_TRUE(read.ok()) << read.status().ToString();
  ASSERT_EQ(read.ValueOrDie().segment.postings.size(), 512U);
  EXPECT_EQ(read.ValueOrDie().segment.postings.back().vertex.vertex_id.value, 553U);
  EXPECT_EQ(read.ValueOrDie().segment.postings.back().value, Value::String(std::string(256, 'x')));
  EXPECT_GT(reservation.used(ResourceDimension::kMemory), 0U);
  read = Status::NotFound("test", "released property index");
  EXPECT_EQ(reservation.used(ResourceDimension::kMemory), 0U);
}

TEST_F(ProjectionStoreTest, PropertyIndexReadSelectsEntityRegionAndBindingIdentity) {
  auto build = BuildPropertyIndex(10);
  auto second = BuildPropertyIndex(10);
  auto decoded = DecodePropertyIndexSegment(second.segments.front().bytes);
  ASSERT_TRUE(decoded.ok());
  decoded.ValueOrDie().postings.front().vertex.vertex_id = VertexId{142};
  decoded.ValueOrDie().pages.clear();
  auto encoded = EncodePropertyIndexSegment(decoded.ValueOrDie());
  ASSERT_TRUE(encoded.ok());
  auto& region = second.manifest.regions.front();
  region.entity_min = 100;
  region.entity_max_exclusive = 200;
  auto& descriptor = second.segments.front().descriptor;
  descriptor.segment_id = "property-index-second";
  descriptor.filename = "property-index-second.cpi";
  descriptor.header.entity_min = 100;
  descriptor.header.entity_max_exclusive = 200;
  descriptor.file_bytes = encoded.ValueOrDie().size();
  descriptor.checksum = crc32c::Value(encoded.ValueOrDie().data(), encoded.ValueOrDie().size());
  second.segments.front().bytes = encoded.ValueOrDie();
  region.segments = {descriptor};
  build.segments.push_back(std::move(second.segments.front()));
  build.manifest.regions.push_back(std::move(region));
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  const auto status = opened.ValueOrDie()->Build(build);
  ASSERT_TRUE(status.ok()) << status.ToString();
  CoverageRequest request;
  request.kind = ProjectionKind::kPropertyIndex;
  request.part_id = PartId{1};
  request.property_id = PropertyId{7};
  request.schema_epoch = 1;
  request.entity_min = 100;
  request.entity_max_exclusive = 200;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{10};
  request.generation_id = 10;
  request.expected_base_seq = CommitSeq{10};
  request.database_identity = "test-db";
  auto generation = opened.ValueOrDie()->Acquire(request);
  ASSERT_TRUE(generation.has_value());
  auto index = opened.ValueOrDie()->ReadPropertyIndex(request, *generation);
  ASSERT_TRUE(index.ok()) << index.status().ToString();
  ASSERT_EQ(index.ValueOrDie().postings.size(), 1U);
  EXPECT_EQ(index.ValueOrDie().postings.front().vertex.vertex_id.value, 142U);
  request.entity_min = 1;
  request.entity_max_exclusive = 100;
  index = opened.ValueOrDie()->ReadPropertyIndex(request, *generation);
  ASSERT_TRUE(index.ok());
  EXPECT_EQ(index.ValueOrDie().postings.front().vertex.vertex_id.value, 42U);
  request.expected_base_seq = CommitSeq{9};
  EXPECT_TRUE(opened.ValueOrDie()->ReadPropertyIndex(request, *generation).status().IsConflict());
  request.expected_base_seq = CommitSeq{10};
  request.database_identity = "different-db";
  EXPECT_TRUE(opened.ValueOrDie()->ReadPropertyIndex(request, *generation).status().IsIdentityConflict());
}

TEST_F(ProjectionStoreTest, ReadsPublishedChainsForRuntimeProjectionSlice) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  ASSERT_TRUE(opened.ValueOrDie()->Build(Build(10)).ok());
  CoverageRequest request;
  request.part_id = PartId{1};
  request.schema_epoch = 1;
  request.entity_min = 1;
  request.entity_max_exclusive = 2;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{10};
  request.generation_id = 10;
  request.expected_base_seq = CommitSeq{10};
  request.database_identity = "test-db";
  auto chains = opened.ValueOrDie()->ReadChains(request);
  ASSERT_TRUE(chains.ok()) << chains.status().ToString();
  ASSERT_EQ(chains.ValueOrDie().size(), 1U);
  ASSERT_EQ(chains.ValueOrDie().front().intervals.size(), 1U);
  EXPECT_EQ(chains.ValueOrDie().front().intervals.front().entity_id, 1U);
  request.generation_id = 9;
  EXPECT_TRUE(opened.ValueOrDie()->ReadChains(request).status().IsConflict());
}

TEST_F(ProjectionStoreTest, ReadChainsReportsSlicePhysicalEvidence) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  ASSERT_TRUE(opened.ValueOrDie()->Build(Build(10)).ok());
  CoverageRequest request;
  request.part_id = PartId{1};
  request.schema_epoch = 1;
  request.entity_min = 1;
  request.entity_max_exclusive = 2;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{10};
  request.generation_id = 10;
  request.expected_base_seq = CommitSeq{10};
  request.database_identity = "test-db";
  ProjectionReadStats stats;
  request.stats = &stats;
  auto chains = opened.ValueOrDie()->ReadChains(request);
  ASSERT_TRUE(chains.ok()) << chains.status().ToString();
  EXPECT_GT(stats.pages_read, 0U);
  EXPECT_GT(stats.physical_bytes, 0U);
  EXPECT_GT(stats.decoded_bytes, 0U);
}

TEST_F(ProjectionStoreTest, ReadChainsReportsSkippedPagesForOutOfRangeSlice) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  ASSERT_TRUE(opened.ValueOrDie()->Build(Build(10)).ok());
  CoverageRequest request;
  request.part_id = PartId{99};
  request.schema_epoch = 1;
  request.entity_min = 1;
  request.entity_max_exclusive = 2;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{10};
  request.generation_id = 10;
  request.expected_base_seq = CommitSeq{10};
  request.database_identity = "test-db";
  ProjectionReadStats stats;
  request.stats = &stats;
  EXPECT_TRUE(opened.ValueOrDie()->ReadChains(request).status().IsNotFound());
  EXPECT_EQ(stats.pages_read, 0U);
  EXPECT_GT(stats.pages_skipped, 0U);
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

TEST_F(ProjectionStoreTest, HotPageCacheRespectsGenerationAndConfiguredCapacity) {
  ProjectionStoreOptions options{path_, "test-db", {}};
  options.projection_cache_bytes = 16384;
  auto opened = QueryProjectionStore::Open(options);
  ASSERT_TRUE(opened.ok());
  auto& store = *opened.ValueOrDie();
  ASSERT_TRUE(store.Build(Build(10)).ok());
  CoverageRequest request;
  request.part_id = PartId{1}; request.schema_epoch = 1;
  request.entity_min = 1; request.entity_max_exclusive = 2;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{20};
  request.admit_pages = true;
  ProjectionReadStats stats;
  request.stats = &stats;
  auto pin = store.Acquire(request);
  ASSERT_TRUE(pin);
  QueryReservation query(65536);
  const auto visit = [&](const ProjectionChain& chain) {
    EXPECT_EQ(chain.header.generation_id, request.generation_id.value_or(10));
    EXPECT_EQ(chain.intervals.size(), 1U);
    return Status::OK();
  };
  ASSERT_TRUE(store.VisitChains(request, *pin, query, visit).ok());
  EXPECT_GT(stats.physical_bytes, 0U);
  EXPECT_GT(store.SamplePageCacheMetrics().resident_bytes, 0U);
  stats = {};
  ASSERT_TRUE(store.VisitChains(request, *pin, query, visit).ok());
  EXPECT_EQ(stats.physical_bytes, 0U);
  EXPECT_EQ(stats.decoded_bytes, 0U);
  EXPECT_GT(store.SamplePageCacheMetrics().hits, 0U);
  EXPECT_EQ(query.used_bytes(), 0U);
  ASSERT_TRUE(store.Build(Build(20, "seg-b")).ok());
  auto next = store.Acquire(request);
  ASSERT_TRUE(next);
  stats = {};
  request.generation_id = 20;
  ASSERT_TRUE(store.VisitChains(request, *next, query, visit).ok());
  EXPECT_GT(stats.physical_bytes, 0U);
  EXPECT_LE(store.SamplePageCacheMetrics().resident_bytes, 16384U);
  pin.reset();
  store.CollectRetired();
  EXPECT_FALSE(std::filesystem::exists(path_ + "/seg-a.csegment"));
}

TEST_F(ProjectionStoreTest, VisitorRetainsGenerationAndDoesNotQuarantineResourceFailures) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  auto& store = *opened.ValueOrDie();
  ASSERT_TRUE(store.Build(Build(10)).ok());
  CoverageRequest request;
  request.part_id = PartId{1}; request.schema_epoch = 1;
  request.entity_min = 1; request.entity_max_exclusive = 2;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{10};
  auto pin = store.Acquire(request);
  ASSERT_TRUE(pin.has_value());
  QueryReservation small(100);
  auto status = store.VisitChains(request, *pin, small,
      [](const ProjectionChain&) { return Status::OK(); });
  EXPECT_TRUE(status.IsResourceExhausted()) << status.ToString();
  EXPECT_EQ(small.used_bytes(), 0U);
  EXPECT_EQ(store.pending_rebuild_requests(), 0U);
  EXPECT_TRUE(store.projections_enabled());
  QueryReservation adequate(65536);
  status = store.VisitChains(request, *pin, adequate,
      [](const ProjectionChain&) { return Status::IOError("test", "visitor failure"); });
  EXPECT_TRUE(status.IsIOError());
  EXPECT_EQ(adequate.used_bytes(), 0U);
  EXPECT_EQ(store.pending_rebuild_requests(), 0U);
  ASSERT_TRUE(store.Build(Build(20, "seg-b")).ok());
  size_t visited = 0;
  status = store.VisitChains(request, *pin, adequate,
      [&](const ProjectionChain& chain) {
        EXPECT_EQ(chain.header.generation_id, 10U);
        ++visited;
        return Status::OK();
      });
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(visited, 1U);
  EXPECT_EQ(adequate.used_bytes(), 0U);
}

TEST_F(ProjectionStoreTest, DebugBoundsPreserveRolloverCoverageAndCleanup) {
  ProjectionStoreOptions options{path_, "test-db", {}};
  options.page_bytes = 4ULL << 10;
  options.commits_per_generation = 16;
  options.visible_seq = CommitSeq{20};
  auto opened = QueryProjectionStore::Open(std::move(options));
  ASSERT_TRUE(opened.ok());
  auto& store = *opened.ValueOrDie();
  ASSERT_TRUE(store.Build(Build(10)).ok());
  EXPECT_TRUE(store.Build(Build(30, "seg-limit", /*base_seq=*/4))
                  .IsResourceExhausted());
  CoverageRequest request;
  request.part_id = PartId{1};
  request.schema_epoch = 1;
  request.entity_min = 1;
  request.entity_max_exclusive = 2;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{10};
  auto pin = store.Acquire(request);
  ASSERT_TRUE(pin.has_value());
  ASSERT_TRUE(store.Build(Build(20, "seg-b")).ok());
  EXPECT_EQ(store.current_generation_id(), std::optional<uint64_t>(20));
  EXPECT_TRUE(pin->exists());
  pin.reset();
  store.CollectRetired();
  EXPECT_FALSE(std::filesystem::exists(path_ + "/seg-a.csegment"));
  EXPECT_TRUE(std::filesystem::exists(path_ + "/seg-b.csegment"));
}

TEST_F(ProjectionStoreTest, PinnedReaderRetainsRetiredGeneration) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  auto& store = *opened.ValueOrDie();
  ASSERT_TRUE(store.Build(Build(10)).ok());
  CoverageRequest request;
  request.part_id = PartId{1};
  request.schema_epoch = 1;
  request.entity_min = 1;
  request.entity_max_exclusive = 2;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{10};
  request.generation_id = 10;
  request.expected_base_seq = CommitSeq{10};
  request.database_identity = "test-db";
  auto pin = store.Acquire(request);
  ASSERT_TRUE(pin.has_value());
  ASSERT_TRUE(store.Build(Build(20, "seg-b")).ok());
  request.snapshot_seq = CommitSeq{20};

  auto chains = store.ReadChains(request, *pin);
  ASSERT_TRUE(chains.ok()) << chains.status().ToString();
  ASSERT_EQ(chains.ValueOrDie().size(), 1U);
  EXPECT_EQ(chains.ValueOrDie().front().header.generation_id, 10U);
  EXPECT_TRUE(store.ReadChains(request).status().IsConflict());
}

TEST_F(ProjectionStoreTest, PinnedReaderFailsAfterStoreClose) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  ASSERT_TRUE(opened.ValueOrDie()->Build(Build(10)).ok());
  CoverageRequest request;
  request.part_id = PartId{1};
  request.schema_epoch = 1;
  request.entity_min = 1;
  request.entity_max_exclusive = 2;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{10};
  auto pin = opened.ValueOrDie()->Acquire(request);
  ASSERT_TRUE(pin.has_value());
  opened.ValueOrDie().reset();
  EXPECT_FALSE(pin->exists());
}

TEST_F(ProjectionStoreTest, InvalidPinnedReaderReturnsNotFound) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  ASSERT_TRUE(opened.ValueOrDie()->Build(Build(10)).ok());
  CoverageRequest request;
  request.part_id = PartId{1};
  request.schema_epoch = 1;
  request.entity_min = 1;
  request.entity_max_exclusive = 2;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{10};
  ProjectionGeneration invalid;
  EXPECT_TRUE(opened.ValueOrDie()->ReadChains(request, invalid).status().IsNotFound());
}

TEST_F(ProjectionStoreTest, BadCurrentDisablesProjections) {
  std::filesystem::create_directories(path_ + "/manifests");
  std::ofstream(path_ + "/PROJECTION-CURRENT") << "bad";
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  EXPECT_FALSE(opened.ValueOrDie()->projections_enabled());
}

TEST_F(ProjectionStoreTest, RetirePersistsDisabledStateAcrossReopen) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  ASSERT_TRUE(opened.ValueOrDie()->Build(Build(10)).ok());
  ASSERT_TRUE(opened.ValueOrDie()->RetireBefore(CommitSeq{20}).ok());
  opened.ValueOrDie().reset();
  auto reopened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(reopened.ok());
  EXPECT_FALSE(reopened.ValueOrDie()->projections_enabled());
  EXPECT_FALSE(reopened.ValueOrDie()->current_generation_id().has_value());
}

TEST_F(ProjectionStoreTest, RetireRejectsStaleDurableCurrent) {
  auto first = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(first.ValueOrDie()->Build(Build(10)).ok());

  // Simulate a second process that opened before the first process advanced
  // the durable pointer to generation 20.
  auto stale = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(stale.ok());
  ASSERT_EQ(stale.ValueOrDie()->current_generation_id(),
            std::optional<uint64_t>(10));
  ASSERT_TRUE(first.ValueOrDie()->Build(Build(20, "seg-b")).ok());

  EXPECT_TRUE(stale.ValueOrDie()->RetireBefore(CommitSeq{20}).IsConflict());
  auto reopened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(reopened.ok());
  EXPECT_EQ(reopened.ValueOrDie()->current_generation_id(),
            std::optional<uint64_t>(20));
}

TEST_F(ProjectionStoreTest, RejectsGenerationFilenameCollisionAndPinnedQuarantine) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  ASSERT_TRUE(opened.ValueOrDie()->Build(Build(10)).ok());
  CoverageRequest request; request.part_id=PartId{1}; request.schema_epoch=1; request.entity_min=1; request.entity_max_exclusive=2; request.valid_time={ValidTime{0},std::nullopt}; request.snapshot_seq=CommitSeq{10};
  auto pin = opened.ValueOrDie()->Acquire(request);
  EXPECT_TRUE(pin.has_value());
  EXPECT_TRUE(opened.ValueOrDie()->Quarantine("seg-a.csegment").IsConflict());
  EXPECT_TRUE(opened.ValueOrDie()->Build(Build(20, "seg-a")).IsConflict());
}

TEST_F(ProjectionStoreTest, PartialCorruptionFallsBackAndQueuesRebuild) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  auto& store = *opened.ValueOrDie();
  ASSERT_TRUE(store.Build(Build(10)).ok());
  CoverageRequest request;
  request.part_id = PartId{1};
  request.schema_epoch = 1;
  request.entity_min = 1;
  request.entity_max_exclusive = 2;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{10};
  ASSERT_TRUE(std::filesystem::remove(path_ + "/seg-a.csegment"));

  auto chains = store.ReadChains(request);
  EXPECT_TRUE(chains.status().IsNotFound());
  EXPECT_FALSE(store.projections_enabled());
  EXPECT_EQ(store.pending_rebuild_requests(), 1U);
  store.CollectRetired();
  EXPECT_FALSE(std::filesystem::exists(path_ + "/seg-a.csegment"));
}

TEST_F(ProjectionStoreTest, PinnedCorruptionRetiresAndQuarantinesGeneration) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  auto& store = *opened.ValueOrDie();
  ASSERT_TRUE(store.Build(Build(10)).ok());
  CoverageRequest request;
  request.part_id = PartId{1};
  request.schema_epoch = 1;
  request.entity_min = 1;
  request.entity_max_exclusive = 2;
  request.valid_time = {ValidTime{0}, std::nullopt};
  request.snapshot_seq = CommitSeq{10};
  auto pin = store.Acquire(request);
  ASSERT_TRUE(pin.has_value());
  std::ofstream(path_ + "/seg-a.csegment", std::ios::binary | std::ios::trunc)
      << "corrupt";
  EXPECT_TRUE(store.ReadChains(request, *pin).status().IsNotFound());
  EXPECT_FALSE(store.projections_enabled());
  EXPECT_EQ(store.pending_rebuild_requests(), 1U);
  pin.reset();
  store.CollectRetired();
  EXPECT_TRUE(std::filesystem::exists(path_ + "/quarantine/seg-a.csegment"));
}

TEST_F(ProjectionStoreTest, FaultAfterSegmentSyncLeavesCurrentUnchanged) {
  bool fail = true;
  auto opened = QueryProjectionStore::Open({path_, "test-db", [&](ProjectionStoreFaultPoint point) {
    return point == ProjectionStoreFaultPoint::kAfterSegmentSync && fail
               ? (fail = false, Status::IOError("test", "injected"))
               : Status::OK();
  }});
  ASSERT_TRUE(opened.ok());
  EXPECT_TRUE(opened.ValueOrDie()->Build(Build(10)).IsIOError());
  EXPECT_FALSE(opened.ValueOrDie()->projections_enabled());
}

TEST_F(ProjectionStoreTest, RejectsControlNamesGenerationRollbackAndTemps) {
  auto opened = QueryProjectionStore::Open({path_, "test-db", {}});
  ASSERT_TRUE(opened.ok());
  auto control = Build(10);
  control.manifest.regions[0].segments[0].filename = "PROJECTION-CURRENT";
  control.segments[0].descriptor.filename = "PROJECTION-CURRENT";
  EXPECT_TRUE(opened.ValueOrDie()->Build(control).IsInvalidArgument());
  ASSERT_TRUE(opened.ValueOrDie()->Build(Build(10)).ok());
  EXPECT_TRUE(opened.ValueOrDie()->Build(Build(9, "seg-b")).IsConflict());
  std::ofstream(path_ + "/seg-c.csegment.tmp") << "leftover";
  auto temporary = Build(20, "seg-c");
  EXPECT_TRUE(opened.ValueOrDie()->Build(temporary).IsConflict());
}
}  // namespace
}  // namespace cedar::internal
