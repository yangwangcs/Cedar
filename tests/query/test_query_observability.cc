#include <filesystem>
#include <gtest/gtest.h>

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
  ASSERT_TRUE(store.Refresh(manifest).ok());
  EXPECT_TRUE(std::filesystem::exists(root / "generation-3.cstats"));
  auto loaded = store.Load(3, CommitSeq{9});
  ASSERT_TRUE(loaded.ok()) << loaded.status().ToString();
  EXPECT_EQ(loaded.ValueOrDie().generation_id, 3U);
  EXPECT_TRUE(store.Load(2, CommitSeq{9}).status().IsNotFound());
  std::filesystem::remove_all(root);
}

}  // namespace
}  // namespace cedar::internal
