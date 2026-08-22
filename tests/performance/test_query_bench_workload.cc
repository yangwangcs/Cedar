#include <filesystem>
#include <algorithm>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "benchmarks/cedar_query_bench_workload.h"

namespace cedar::benchmark {
namespace {
std::string TestPath(const char* suffix) {
  return (std::filesystem::temp_directory_path() /
          (std::string("cedar-query-existing-") + suffix)).string();
}
}

TEST(QueryBenchWorkload, VerifiesExistingDatabaseAcrossReopen) {
  const std::string path = TestPath("database");
  std::filesystem::remove_all(path);
  QueryBenchmarkOptions create;
  create.path = path;
  create.degree = 1;
  create.readers = 1;
  create.duration_seconds = 1;
  create.facts_per_txn = 1;
  create.verify_reopen = true;
  auto created = RunQueryBenchmark(create);
  ASSERT_TRUE(created.ok()) << created.status().ToString();
  ASSERT_GT(created.ValueOrDie().facts, 0U);

  QueryBenchmarkOptions verify;
  verify.path = path;
  verify.verify_existing = true;
  verify.expected_facts = created.ValueOrDie().facts;
  verify.expected_checksum = created.ValueOrDie().dataset_checksum;
  auto verified = RunQueryBenchmark(verify);
  ASSERT_TRUE(verified.ok()) << verified.status().ToString();
  EXPECT_TRUE(verified.ValueOrDie().reopen_verified);
  EXPECT_TRUE(verified.ValueOrDie().hard_gate_pass)
      << verified.ValueOrDie().terminal_status;
  EXPECT_EQ(verified.ValueOrDie().facts, verify.expected_facts);
  EXPECT_EQ(verified.ValueOrDie().dataset_checksum, verify.expected_checksum);
  EXPECT_DOUBLE_EQ(
      verified.ValueOrDie().space_amplification,
      verified.ValueOrDie().authoritative_bytes == 0
          ? 0.0
          : static_cast<double>(verified.ValueOrDie().derived_bytes) /
                verified.ValueOrDie().authoritative_bytes);
  EXPECT_DOUBLE_EQ(
      verified.ValueOrDie().total_space_amplification,
      verified.ValueOrDie().authoritative_bytes == 0
          ? 0.0
          : static_cast<double>(verified.ValueOrDie().total_bytes) /
                verified.ValueOrDie().authoritative_bytes);

  verify.expected_checksum++;
  auto mismatch = RunQueryBenchmark(verify);
  ASSERT_TRUE(mismatch.ok()) << mismatch.status().ToString();
  EXPECT_FALSE(mismatch.ValueOrDie().hard_gate_pass);
  EXPECT_FALSE(mismatch.ValueOrDie().reopen_verified);
  EXPECT_EQ(mismatch.ValueOrDie().terminal_status, "reopen verification failed");
  std::filesystem::remove_all(path);
}

TEST(QueryBenchWorkload, SpaceAmplificationUsesDerivedProjectionBytes) {
  QueryBenchmarkOptions options;
  QueryBenchmarkResult result;
  result.authoritative_bytes = 100;
  result.derived_bytes = 0;
  result.total_bytes = 1000;
  result.space_amplification = 0;
  result.total_space_amplification = 10;
  const std::string csv = QueryBenchmarkCsvRow(options, result);
  std::vector<std::string> header_fields;
  std::vector<std::string> row_fields;
  std::stringstream header_stream(QueryBenchmarkCsvHeader());
  for (std::string field; std::getline(header_stream, field, ',');)
    header_fields.push_back(field);
  std::stringstream row_stream(csv);
  for (std::string field; std::getline(row_stream, field, ',');)
    row_fields.push_back(field);
  auto field = [&](const char* name) {
    const auto it = std::find(header_fields.begin(), header_fields.end(), name);
    return it == header_fields.end() ? std::string() : row_fields[it - header_fields.begin()];
  };
  EXPECT_EQ(field("space_amplification"), "0");
  EXPECT_EQ(field("total_space_amplification"), "10");
}
}  // namespace cedar::benchmark
