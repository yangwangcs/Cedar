#include <filesystem>
#include <fstream>
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
  EXPECT_EQ(loaded.ValueOrDie().columns.front().rows, 2U);
  EXPECT_EQ(loaded.ValueOrDie().columns.front().interval_count, 2U);
  EXPECT_EQ(loaded.ValueOrDie().columns.front().edge_count, 2U);
  EXPECT_FALSE(loaded.ValueOrDie().columns.front().top_values.empty());
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
  std::filesystem::remove_all(root);
}

}  // namespace
}  // namespace cedar::internal
