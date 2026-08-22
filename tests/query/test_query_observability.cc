#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <sys/file.h>
#include <unistd.h>
#include <atomic>
#include <thread>
#include <gtest/gtest.h>

#include "cedar/core/crc32c.h"
#include "query/observability/query_metrics.h"
#include "query/projection/projection_manifest.h"

namespace cedar::internal {
namespace {

TEST(QueryObservabilityTest, StatisticsRoundTripIsGenerationBoundAndBounded) {
  QueryStatisticsSnapshot input;
  input.database_identity = "db";
  input.schema_fingerprint = "schema";
  input.generation_id = 7;
  input.base_seq = CommitSeq{11};
  QueryColumnStatistics column;
  column.rows = 10;
  column.pages = 2;
  column.bytes = 4096;
  column.entity_range = EntityRange{1, 100};
  column.valid_time_range = ValidTimeInterval{ValidTime{5}, ValidTime{20}};
  column.distinct.registers.assign(1u << column.distinct.precision, 1);
  column.histogram.push_back({Value::Int64(10), 10});
  column.top_values.push_back({Value::Int64(10), 4});
  input.columns.push_back(column);
  auto encoded = EncodeQueryStatistics(input);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto decoded = DecodeQueryStatistics(encoded.ValueOrDie());
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  EXPECT_EQ(decoded.ValueOrDie().database_identity, "db");
  EXPECT_EQ(decoded.ValueOrDie().generation_id, 7U);
  EXPECT_EQ(decoded.ValueOrDie().columns.front().histogram.size(), 1U);
  EXPECT_TRUE(DecodeQueryStatistics(encoded.ValueOrDie() + "x").status().IsCorruption());
  QueryStatisticsSnapshot no_schema = input;
  no_schema.schema_fingerprint.clear();
  no_schema.complete = true;
  auto normalized = EncodeQueryStatistics(no_schema);
  ASSERT_TRUE(normalized.ok()) << normalized.status().ToString();
  auto normalized_decoded = DecodeQueryStatistics(normalized.ValueOrDie());
  ASSERT_TRUE(normalized_decoded.ok()) << normalized_decoded.status().ToString();
  EXPECT_FALSE(normalized_decoded.ValueOrDie().complete);
}

TEST(QueryObservabilityTest, MetricsExposeOnlyBoundedEnumLabels) {
  QueryMetrics metrics;
  EXPECT_TRUE(metrics.RegisterLabel(QueryMetricOperator::kScan).ok());
  metrics.AddBatch(QueryMetricOperator::kScan, 4, 10, 20, 2);
  metrics.AddTerminal(QueryMetricTerminal::kComplete);
  metrics.AddFallback(QueryMetricFallback::kCanonical);
  metrics.AddSpillBytes(8);
  const auto snapshot = metrics.Snapshot();
  EXPECT_EQ(snapshot.operator_rows[static_cast<size_t>(QueryMetricOperator::kScan)], 4U);
  EXPECT_EQ(snapshot.batches, 1U);
  EXPECT_EQ(snapshot.spill_bytes, 8U);
}

TEST(QueryObservabilityTest, MetricsCoverRequiredDimensionsWithFixedStorage) {
  QueryMetrics metrics;
  metrics.AddAdmission(QueryMetricAdmission::kQueued);
  metrics.AddProjection(QueryMetricProjection::kHit);
  metrics.AddProjectionHealth(QueryMetricProjectionHealth::kHealthy);
  metrics.AddAdjacencyPruning(QueryMetricAdjacencyPruning::kPruned);
  metrics.AddLabelDominance(QueryMetricLabelDominance::kDominant);
  metrics.AddMemoryBytes(32);
  metrics.AddScratchBytes(16);
  metrics.ObserveLatencyUs(1000);
  metrics.ObserveAdmissionWaitUs(4);
  metrics.ObserveWorkerWaitUs(8);
  metrics.ObserveIoWaitUs(16);
  metrics.ObserveDeltaLag(2);
  const auto snapshot = metrics.Snapshot();
  EXPECT_EQ(snapshot.admission[static_cast<size_t>(QueryMetricAdmission::kQueued)], 1U);
  EXPECT_EQ(snapshot.projection[static_cast<size_t>(QueryMetricProjection::kHit)], 1U);
  EXPECT_EQ(snapshot.projection_health[static_cast<size_t>(QueryMetricProjectionHealth::kHealthy)], 1U);
  EXPECT_EQ(snapshot.adjacency_pruning[static_cast<size_t>(QueryMetricAdjacencyPruning::kPruned)], 1U);
  EXPECT_EQ(snapshot.label_dominance[static_cast<size_t>(QueryMetricLabelDominance::kDominant)], 1U);
  EXPECT_EQ(snapshot.memory_bytes, 32U);
  EXPECT_EQ(snapshot.scratch_bytes, 16U);
  EXPECT_EQ(snapshot.latency_us[9], 1U);
  EXPECT_EQ(snapshot.admission_wait_us[2], 1U);
}

TEST(QueryObservabilityTest, StatisticsStorePublishesCstats) {
  const auto root = std::filesystem::temp_directory_path() / "cedar_stats_test";
  std::filesystem::remove_all(root);
  ProjectionManifest manifest;
  manifest.database_identity = root.string();
  manifest.generation_id = 3;
  manifest.base_seq = CommitSeq{9};
  auto store = QueryStatisticsStore(root.string(), root.string());
  ASSERT_TRUE(store.Refresh(manifest, "schema").ok());
  EXPECT_TRUE(std::filesystem::exists(root / "generation-3.cstats"));
  // Refresh publishes the generation manifest and its linked statistics
  // reference before replacing CSTATS-CURRENT.
  ASSERT_TRUE(std::filesystem::exists(root / "manifests" / "3.cmanifest"));
  {
    std::ifstream in(root / "manifests" / "3.cmanifest", std::ios::binary);
    const std::string bytes((std::istreambuf_iterator<char>(in)),
                            std::istreambuf_iterator<char>());
    auto linked_manifest = DecodeProjectionManifest(bytes, root.string());
    ASSERT_TRUE(linked_manifest.ok()) << linked_manifest.status().ToString();
    ASSERT_TRUE(linked_manifest.ValueOrDie().statistics.has_value());
    EXPECT_EQ(linked_manifest.ValueOrDie().statistics->filename,
              "generation-3.cstats");
  }
  auto loaded = store.Load(3, CommitSeq{9}, "schema");
  ASSERT_TRUE(loaded.ok()) << loaded.status().ToString();
  EXPECT_EQ(loaded.ValueOrDie().generation_id, 3U);
  EXPECT_TRUE(store.Load(2, CommitSeq{9}).status().IsNotFound());
  std::filesystem::remove(root / "manifests" / "3.cmanifest");
  EXPECT_TRUE(store.Load(3, CommitSeq{9}).status().IsNotFound());
  std::filesystem::remove_all(root);
}

TEST(QueryObservabilityTest, RefreshRejectsOlderGenerationAfterCurrentAdvances) {
  const auto root = std::filesystem::temp_directory_path() /
                    "cedar_stats_generation_guard_test";
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root / "manifests");
  ProjectionManifest current;
  current.database_identity = root.string();
  current.generation_id = 6;
  current.base_seq = CommitSeq{12};
  auto manifest_bytes = EncodeProjectionManifest(current);
  ASSERT_TRUE(manifest_bytes.ok()) << manifest_bytes.status().ToString();
  { std::ofstream out(root / "manifests" / "6.cmanifest", std::ios::binary);
    out.write(manifest_bytes.ValueOrDie().data(), manifest_bytes.ValueOrDie().size()); }
  std::string pointer("CPC1", 4);
  for (int i = 0; i < 8; ++i) pointer.push_back(char(current.generation_id >> (i * 8)));
  const uint32_t pointer_crc = crc32c::Value(pointer.data(), pointer.size());
  for (int i = 0; i < 4; ++i) pointer.push_back(char(pointer_crc >> (i * 8)));
  { std::ofstream out(root / "PROJECTION-CURRENT", std::ios::binary);
    out.write(pointer.data(), pointer.size()); }
  ProjectionManifest stale = current;
  stale.generation_id = 5;
  stale.base_seq = CommitSeq{11};
  QueryStatisticsStore store(root.string(), root.string());
  EXPECT_TRUE(store.Refresh(stale, "schema").IsConflict());
  EXPECT_FALSE(std::filesystem::exists(root / "CSTATS-CURRENT"));
  std::filesystem::remove_all(root);
}

TEST(QueryObservabilityTest, RefreshRejectsForeignOrSchemaMismatchedManifest) {
  const auto root = std::filesystem::temp_directory_path() /
                    "cedar_stats_manifest_identity_test";
  std::filesystem::remove_all(root);
  QueryStatisticsStore store(root.string(), root.string());

  ProjectionManifest foreign;
  foreign.database_identity = root.string() + ".foreign";
  foreign.generation_id = 21;
  foreign.base_seq = CommitSeq{30};
  EXPECT_TRUE(store.Refresh(foreign, "schema").IsIdentityConflict());
  EXPECT_FALSE(std::filesystem::exists(root / "CSTATS-CURRENT"));

  ProjectionManifest mismatched;
  mismatched.database_identity = root.string();
  mismatched.generation_id = 22;
  mismatched.base_seq = CommitSeq{31};
  mismatched.schema_fingerprints = {"schema-v1"};
  EXPECT_TRUE(store.Refresh(mismatched, "schema-v2").IsConflict());
  EXPECT_FALSE(std::filesystem::exists(root / "CSTATS-CURRENT"));

  std::filesystem::remove_all(root);
}

TEST(QueryObservabilityTest, RefreshPublicationLockSerializesProjectionAdvance) {
  const auto root = std::filesystem::temp_directory_path() /
                    "cedar_stats_publication_lock_test";
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root / "manifests");
  ProjectionManifest stale;
  stale.database_identity = root.string();
  stale.generation_id = 31;
  stale.base_seq = CommitSeq{40};
  auto stale_bytes = EncodeProjectionManifest(stale);
  ASSERT_TRUE(stale_bytes.ok()) << stale_bytes.status().ToString();
  { std::ofstream out(root / "manifests" / "31.cmanifest", std::ios::binary);
    out.write(stale_bytes.ValueOrDie().data(), stale_bytes.ValueOrDie().size()); }

  const int lock_fd = ::open((root / "CQUERY-PUBLISH.lock").c_str(), O_RDWR | O_CREAT, 0644);
  ASSERT_GE(lock_fd, 0);
  ASSERT_EQ(::flock(lock_fd, LOCK_EX), 0);
  std::atomic<bool> done{false};
  Status refresh_status;
  QueryStatisticsStore store(root.string(), root.string());
  std::thread refresh_thread([&] {
    refresh_status = store.Refresh(stale, "schema");
    done.store(true, std::memory_order_release);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(done.load(std::memory_order_acquire));

  ProjectionManifest newer = stale;
  newer.generation_id = 32;
  newer.base_seq = CommitSeq{41};
  auto newer_bytes = EncodeProjectionManifest(newer);
  ASSERT_TRUE(newer_bytes.ok()) << newer_bytes.status().ToString();
  { std::ofstream out(root / "manifests" / "32.cmanifest", std::ios::binary);
    out.write(newer_bytes.ValueOrDie().data(), newer_bytes.ValueOrDie().size()); }
  std::string pointer("CPC1", 4);
  for (int i = 0; i < 8; ++i) pointer.push_back(char(newer.generation_id >> (i * 8)));
  const uint32_t crc = crc32c::Value(pointer.data(), pointer.size());
  for (int i = 0; i < 4; ++i) pointer.push_back(char(crc >> (i * 8)));
  { std::ofstream out(root / "PROJECTION-CURRENT", std::ios::binary | std::ios::trunc);
    out.write(pointer.data(), pointer.size()); }
  ASSERT_EQ(::flock(lock_fd, LOCK_UN), 0);
  ::close(lock_fd);
  refresh_thread.join();
  EXPECT_TRUE(refresh_status.IsConflict()) << refresh_status.ToString();
  EXPECT_FALSE(std::filesystem::exists(root / "CSTATS-CURRENT"));
  std::filesystem::remove_all(root);
}

TEST(QueryObservabilityTest, EmptySchemaNeverAdvertisesCompleteStatistics) {
  const auto root = std::filesystem::temp_directory_path() /
                    "cedar_stats_empty_schema_test";
  std::filesystem::remove_all(root);
  ProjectionManifest manifest;
  manifest.database_identity = root.string();
  manifest.generation_id = 10;
  manifest.base_seq = CommitSeq{14};
  QueryStatisticsStore store(root.string(), root.string());
  ASSERT_TRUE(store.Refresh(manifest, "").ok());
  auto loaded = store.Load(10, CommitSeq{14});
  ASSERT_TRUE(loaded.ok()) << loaded.status().ToString();
  EXPECT_FALSE(loaded.ValueOrDie().complete);
  std::filesystem::remove_all(root);
}

TEST(QueryObservabilityTest, RefreshDecodesReferencedSegmentsIntoCompleteStats) {
  const auto root = std::filesystem::temp_directory_path() / "cedar_stats_decode_test";
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root);
  ProjectionChain chain;
  chain.header.kind = ProjectionKind::kAdjacency;
  chain.header.generation_id = 8;
  chain.header.base_seq = CommitSeq{12};
  chain.header.part_id = PartId{1};
  chain.header.entity_min = 1;
  chain.header.entity_max_exclusive = 3;
  chain.header.valid_from_min = ValidTime{10};
  chain.intervals.push_back({ValidTimeInterval{ValidTime{10}, ValidTime{20}}, Value::Int64(7), 1});
  chain.intervals.push_back({ValidTimeInterval{ValidTime{12}, std::nullopt}, Value::Int64(7), 2});
  chain.intervals.push_back({ValidTimeInterval{ValidTime{14}, std::nullopt}, Value::Int64(8), 2});
  auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  const auto segment_path = root / "segment-1.cadj";
  { std::ofstream out(segment_path, std::ios::binary); out.write(encoded.ValueOrDie().data(), encoded.ValueOrDie().size()); }
  ProjectionManifest manifest;
  manifest.database_identity = root.string();
  manifest.generation_id = 8;
  manifest.base_seq = CommitSeq{12};
  CoverageRegion region;
  region.kind = ProjectionKind::kAdjacency;
  region.part_id = PartId{1};
  region.entity_min = 1;
  region.entity_max_exclusive = 3;
  region.valid_time = ValidTimeInterval{ValidTime{10}, std::nullopt};
  SegmentDescriptor descriptor;
  descriptor.segment_id = "s1";
  descriptor.filename = "segment-1.cadj";
  descriptor.header = chain.header;
  descriptor.file_bytes = encoded.ValueOrDie().size();
  descriptor.checksum = crc32c::Value(encoded.ValueOrDie().data(), encoded.ValueOrDie().size());
  region.segments.push_back(descriptor);
  manifest.regions.push_back(region);
  QueryStatisticsStore store(root.string(), root.string());
  ASSERT_TRUE(store.Refresh(manifest, "schema").ok());
  auto loaded = store.Load(8, CommitSeq{12}, "schema");
  ASSERT_TRUE(loaded.ok()) << loaded.status().ToString();
  ASSERT_TRUE(loaded.ValueOrDie().complete);
  ASSERT_EQ(loaded.ValueOrDie().columns.size(), 1U);
  EXPECT_EQ(loaded.ValueOrDie().columns.front().rows, 3U);
  EXPECT_EQ(loaded.ValueOrDie().columns.front().interval_count, 3U);
  EXPECT_EQ(loaded.ValueOrDie().columns.front().edge_count, 3U);
  ASSERT_EQ(loaded.ValueOrDie().columns.front().top_values.size(), 2U);
  EXPECT_EQ(loaded.ValueOrDie().columns.front().top_values[0].value,
            Value::Int64(7));
  EXPECT_EQ(loaded.ValueOrDie().columns.front().top_values[1].value,
            Value::Int64(8));
  std::filesystem::remove_all(root);
}

TEST(QueryObservabilityTest, RefreshIsSerializedAndRepeatable) {
  const auto root = std::filesystem::temp_directory_path() / "cedar_stats_concurrent_test";
  std::filesystem::remove_all(root);
  ProjectionManifest manifest;
  manifest.database_identity = root.string();
  manifest.generation_id = 9;
  manifest.base_seq = CommitSeq{13};
  QueryStatisticsStore store(root.string(), root.string());
  std::vector<std::thread> workers;
  std::vector<Status> statuses(8);
  for (size_t i = 0; i < statuses.size(); ++i) {
    workers.emplace_back([&store, &manifest, &statuses, i] {
      statuses[i] = store.Refresh(manifest, "schema");
    });
  }
  for (auto& worker : workers) worker.join();
  for (const auto& status : statuses) EXPECT_TRUE(status.ok()) << status.ToString();
  for (int i = 0; i < 5; ++i) EXPECT_TRUE(store.Refresh(manifest, "schema").ok());
  auto loaded = store.Load(9, CommitSeq{13}, "schema");
  ASSERT_TRUE(loaded.ok()) << loaded.status().ToString();
  EXPECT_TRUE(loaded.ValueOrDie().complete);
  ProjectionManifest stale = manifest;
  stale.generation_id = 8;
  stale.base_seq = CommitSeq{12};
  EXPECT_TRUE(store.Refresh(stale, "schema").IsConflict());
  std::filesystem::remove_all(root);
}

}  // namespace
}  // namespace cedar::internal
