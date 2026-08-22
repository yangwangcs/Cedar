#include <filesystem>
#include <string>

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

  verify.expected_checksum++;
  auto mismatch = RunQueryBenchmark(verify);
  ASSERT_TRUE(mismatch.ok()) << mismatch.status().ToString();
  EXPECT_FALSE(mismatch.ValueOrDie().hard_gate_pass);
  EXPECT_FALSE(mismatch.ValueOrDie().reopen_verified);
  EXPECT_EQ(mismatch.ValueOrDie().terminal_status, "reopen verification failed");
  std::filesystem::remove_all(path);
}
}  // namespace cedar::benchmark
