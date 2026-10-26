#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>

#include "benchmarks/columnar_cache_bench_options.h"

namespace cedar::benchmarks {
namespace {

TEST(ColumnarCacheBenchOptionsTest, PreservesLegacyPositionalDefaults) {
  CacheBenchOptions options;
  std::string error;
  ASSERT_TRUE(ParseCacheBenchOptions({"2", "123"}, &options, &error)) << error;
  EXPECT_EQ(options.seconds, 2U);
  EXPECT_EQ(options.entities, 123U);
  EXPECT_TRUE(options.db_path.empty());
  EXPECT_EQ(options.admission, "lookup-only");
  // Zero keeps the benchmark's automatic/factory reader-metadata fallback.
  EXPECT_EQ(options.storage_reader_metadata_bytes, 0U);
}

TEST(ColumnarCacheBenchOptionsTest, AcceptsExplicitReproducibleDatasetOptions) {
  CacheBenchOptions options;
  std::string error;
  ASSERT_TRUE(ParseCacheBenchOptions({"--db-path", "/tmp/cache dataset",
      "--read-existing", "--cache-admission", "admit", "--workload", "temporal",
      "--seed", "18446744073709551615", "--sessions", "4"}, &options, &error)) << error;
  EXPECT_TRUE(options.read_existing);
  EXPECT_FALSE(options.setup_only);
  EXPECT_EQ(options.db_path, "/tmp/cache dataset");
  EXPECT_EQ(options.workload, "temporal");
  EXPECT_EQ(options.admission, "admit");
  EXPECT_EQ(options.seed, UINT64_MAX);
  EXPECT_EQ(options.sessions, 4U);
}

TEST(ColumnarCacheBenchOptionsTest, RejectsConflictsMissingAndInvalidArguments) {
  const std::vector<std::vector<std::string>> invalid = {
      {"--setup-only"}, {"--read-existing"},
      {"--db-path", "/tmp/x", "--setup-only", "--read-existing"},
      {"--db-path"}, {"--db-path", "--read-existing"},
      {"--db-path", "--setup-only"}, {"--unknown"}, {"--workload", "bogus"},
      {"--cache-admission", "bogus"}, {"--seconds", "0"},
      {"--sessions", "65"}, {"--sessions", "-1"}, {"--entities", "4294967296"},
      {"--seed", "18446744073709551616"}, {"--seed", "12x"},
      {"--part-scope", "bogus"}, {"1", "2", "3"}, {"--seconds", ""}};
  for (const auto& arguments : invalid) {
    CacheBenchOptions options;
    std::string error;
    EXPECT_FALSE(ParseCacheBenchOptions(arguments, &options, &error));
    EXPECT_FALSE(error.empty());
  }
}

TEST(ColumnarCacheBenchOptionsTest, FailureDoesNotPublishPartialOptions) {
  CacheBenchOptions options;
  options.seconds = 9;
  std::string error;
  EXPECT_FALSE(ParseCacheBenchOptions({"--seconds", "2", "--seed", "bad"},
                                    &options, &error));
  EXPECT_EQ(options.seconds, 9U);
}

TEST(ColumnarCacheBenchOptionsTest, WritableConcurrentModeRequiresExplicitOptIn) {
  CacheBenchOptions options;
  std::string error;
  EXPECT_TRUE(ParseCacheBenchOptions({"--db-path", "/tmp/dedicated-copy", "--write-existing",
      "--workload", "concurrent"}, &options, &error)) << error;
  EXPECT_TRUE(options.write_existing);
  EXPECT_FALSE(options.read_existing);
  const std::vector<std::vector<std::string>> invalid = {
      {"--workload", "concurrent"}, {"--write-existing"},
      {"--write-existing", "--workload", "concurrent"},
      {"--db-path", "/tmp/x", "--read-existing", "--workload", "concurrent"},
      {"--db-path", "/tmp/x", "--write-existing", "--read-existing", "--workload", "concurrent"},
      {"--db-path", "/tmp/x", "--write-existing", "--setup-only", "--workload", "concurrent"},
      {"--db-path", "/tmp/x", "--write-existing", "--sessions", "2", "--workload", "concurrent"},
      {"--db-path", "/tmp/x", "--write-existing", "--warmup-operations", "1", "--workload", "concurrent"}};
  for (const auto& arguments : invalid)
    EXPECT_FALSE(ParseCacheBenchOptions(arguments, &options, &error));
}

TEST(ColumnarCacheBenchOptionsTest, ExplicitPathsNeverReuseOrCreateAnExistingDataset) {
  const auto directory = std::filesystem::temp_directory_path() /
      ("cedar-cache-options-" + std::to_string(
          std::chrono::steady_clock::now().time_since_epoch().count()));
  ASSERT_TRUE(std::filesystem::create_directory(directory));
  struct Cleanup {
    std::filesystem::path directory;
    ~Cleanup() { std::filesystem::remove_all(directory); }
  } cleanup{directory};
  CacheBenchOptions options;
  options.db_path = directory.string();
  std::string error;
  EXPECT_TRUE(ValidateCacheBenchPath(options, &error)) << error;
  options.read_existing = true;
  EXPECT_FALSE(ValidateCacheBenchPath(options, &error));
  EXPECT_TRUE(std::filesystem::is_empty(directory));
  std::ofstream(directory / "CURRENT") << "MANIFEST-test\n";
  EXPECT_FALSE(ValidateCacheBenchPath(options, &error));
  std::ofstream(directory / "cedar-cache-bench-manifest.json") << "{}\n";
  EXPECT_TRUE(ValidateCacheBenchPath(options, &error)) << error;
  options.read_existing = false;
  EXPECT_FALSE(ValidateCacheBenchPath(options, &error));
  EXPECT_EQ(std::filesystem::file_size(directory / "CURRENT"), 14U);
  options.db_path = (directory / "new").string();
  EXPECT_TRUE(ValidateCacheBenchPath(options, &error));
  EXPECT_FALSE(std::filesystem::exists(options.db_path));
  options.read_existing = true;
  EXPECT_FALSE(ValidateCacheBenchPath(options, &error));
  EXPECT_FALSE(std::filesystem::exists(options.db_path));
}

TEST(ColumnarCacheBenchOptionsTest, DatasetManifestRequiresMatchingFlushedDataset) {
  CacheBenchOptions options;
  options.entities = 100;
  options.seed = 42;
  options.part_id = 7;
  const auto manifest = CacheBenchDatasetManifest(options);
  EXPECT_TRUE(ValidateCacheBenchDatasetManifest(manifest, options));
  EXPECT_FALSE(ValidateCacheBenchDatasetManifest("{}\n", options));
  auto changed = options;
  changed.entities = 101;
  EXPECT_FALSE(ValidateCacheBenchDatasetManifest(manifest, changed));
  changed = options;
  changed.seed = 43;
  EXPECT_FALSE(ValidateCacheBenchDatasetManifest(manifest, changed));
  changed = options;
  changed.part_id = 8;
  EXPECT_FALSE(ValidateCacheBenchDatasetManifest(manifest, changed));
  changed = options;
  changed.workload = "point";
  changed.admission = "admit";
  EXPECT_TRUE(ValidateCacheBenchDatasetManifest(manifest, changed));
  auto not_flushed = manifest;
  const auto offset = not_flushed.find("true");
  ASSERT_NE(offset, std::string::npos);
  not_flushed.replace(offset, 4, "false");
  EXPECT_FALSE(ValidateCacheBenchDatasetManifest(not_flushed, options));
}

TEST(ColumnarCacheBenchOptionsTest, AcceptsIndependentRuntimeCapacityControls) {
  CacheBenchOptions options;
  std::string error;
  EXPECT_TRUE(ParseCacheBenchOptions({"--cache-bytes", "0", "--storage-read-working-bytes",
      "65536", "--storage-reader-metadata-bytes", "16777216",
      "--query-memory-bytes", "131072"}, &options, &error)) << error;
  EXPECT_EQ(options.cache_bytes, 0U);
  EXPECT_EQ(options.working_bytes, 65536U);
  EXPECT_EQ(options.storage_reader_metadata_bytes, 16777216U);
  EXPECT_EQ(options.query_memory_bytes, 131072U);
  EXPECT_FALSE(ParseCacheBenchOptions({"--storage-read-working-bytes", "0"}, &options, &error));
  EXPECT_FALSE(ParseCacheBenchOptions({"--query-memory-bytes", "0"}, &options, &error));
  // Explicit zero is valid and keeps the automatic profile/factory fallback.
  ASSERT_TRUE(ParseCacheBenchOptions({"--storage-reader-metadata-bytes", "0"}, &options,
                                     &error)) << error;
  EXPECT_EQ(options.storage_reader_metadata_bytes, 0U);
  EXPECT_FALSE(ParseCacheBenchOptions(
      {"--storage-reader-metadata-bytes", "18446744073709551616"}, &options, &error));
  EXPECT_FALSE(ParseCacheBenchOptions({"--storage-reader-metadata-bytes", "-1"}, &options,
                                      &error));
  EXPECT_FALSE(ParseCacheBenchOptions({"--storage-reader-metadata-bytes", "12x"}, &options,
                                      &error));
}

}  // namespace
}  // namespace cedar::benchmarks
