#include <filesystem>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "benchmarks/cedar_query_bench_workload.h"
#include "cedar/database.h"

namespace cedar::benchmark {
namespace {
std::string TestPath(const char* suffix) {
  return (std::filesystem::temp_directory_path() /
          (std::string("cedar-query-existing-") + suffix)).string();
}

class ScopedPathCleanup {
 public:
  explicit ScopedPathCleanup(std::string path) : path_(std::move(path)) {}
  ~ScopedPathCleanup() { std::filesystem::remove_all(path_); }
  ScopedPathCleanup(const ScopedPathCleanup&) = delete;
  ScopedPathCleanup& operator=(const ScopedPathCleanup&) = delete;

 private:
  std::string path_;
};
}

TEST(QueryBenchWorkload, VerifiesExistingDatabaseAcrossReopen) {
  const std::string path = TestPath("database");
  ScopedPathCleanup cleanup(path);
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

TEST(QueryBenchWorkload, AppendAdmissionMetadataIncludesConfiguredControls) {
  QueryBenchmarkOptions options;
  options.commit_deadline_us = 500000;
  options.group_queue_requests = 2048;
  options.group_queue_bytes = 33554432;
  QueryBenchmarkResult result;
  const std::string header = QueryBenchmarkCsvHeader();
  const std::string row = QueryBenchmarkCsvRow(options, result);
  EXPECT_NE(header.find("commit_deadline_us"), std::string::npos);
  EXPECT_NE(header.find("group_queue_requests"), std::string::npos);
  EXPECT_NE(header.find("group_queue_bytes"), std::string::npos);
  EXPECT_NE(row.find("500000"), std::string::npos);
  EXPECT_NE(row.find("2048"), std::string::npos);
  EXPECT_NE(row.find("33554432"), std::string::npos);
  const std::string json = QueryBenchmarkJson(options, result);
  EXPECT_NE(json.find("\"commit_deadline_us\":500000"), std::string::npos);
  EXPECT_NE(json.find("\"group_queue_requests\":2048"), std::string::npos);
  EXPECT_NE(json.find("\"group_queue_bytes\":33554432"), std::string::npos);
}

TEST(QueryBenchWorkload, BoundedAdmissionCoversAllSetupWrites) {
  const std::string path = TestPath("bounded-setup-admission");
  ScopedPathCleanup cleanup(path);
  std::mutex mutex;
  std::condition_variable cv;
  bool collection_entered = false;
  bool release_collection = false;
  bool setup_admission_entered = false;

  DatabaseOptions database_options;
  database_options.path = path;
  database_options.storage_profile = StorageProfile::kProductionAppend;
  database_options.production.memory_budget_bytes = 1ULL << 30;
  database_options.production.kernel_mode = true;
  database_options.query_runtime.query_memory_bytes = 32ULL * 1024ULL * 1024ULL;
  database_options.query_runtime.projection_cache_bytes = 32ULL * 1024ULL * 1024ULL;
  database_options.query_runtime.query_delta_bytes = 32ULL * 1024ULL * 1024ULL;
  database_options.group_commit_max_batch_size = 1;
  database_options.group_commit_window_us = 0;
  database_options.group_commit_max_queue_requests = 1;
  database_options.group_commit_max_queue_bytes = 16ULL * 1024ULL * 1024ULL;
  database_options.append_commit_collection_observer_for_testing = [&] {
    std::unique_lock<std::mutex> lock(mutex);
    if (!collection_entered) {
      collection_entered = true;
      cv.notify_all();
      cv.wait(lock, [&] { return release_collection; });
    }
  };
  database_options.foreground_admission_observer_for_testing = [&] {
    std::lock_guard<std::mutex> lock(mutex);
    setup_admission_entered = true;
    cv.notify_all();
  };
  auto opened = Database::Open(database_options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  ASSERT_TRUE(database->RegisterProperty(PropertyDefinition{
      PropertyId{7}, 0, "duration", PropertyEntityKind::kEdge,
      PhysicalType::kInt64, 4096}).ok());
  ASSERT_TRUE(database->RegisterProperty(PropertyDefinition{
      PropertyId{8}, 0, "score", PropertyEntityKind::kVertex,
      PhysicalType::kInt64, 4096}).ok());

  auto blocker = database->BeginTransaction();
  ASSERT_TRUE(blocker.ok()) << blocker.status().ToString();
  ASSERT_TRUE(blocker.ValueOrDie()->Assert(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{900001}}),
      ValidTime{1}).ok());
  Status blocker_status = Status::InvalidArgument("test", "not attempted");
  std::thread blocker_thread([&] {
    const auto committed = blocker.ValueOrDie()->Commit();
    blocker_status = committed.ok() ? committed.ValueOrDie().status
                                    : committed.status();
  });
  bool collection_observed = false;
  {
    std::unique_lock<std::mutex> lock(mutex);
    collection_observed = cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return collection_entered; });
    if (!collection_observed) release_collection = true;
  }
  cv.notify_all();
  ASSERT_TRUE(collection_observed);

  std::atomic<bool> setup_done = false;
  Status setup_status = Status::InvalidArgument("test", "not attempted");
  std::thread setup([&] {
    setup_status = SeedQueryBenchmarkSetupForTesting(database.get(), 500000);
    setup_done.store(true);
    cv.notify_all();
  });
  bool setup_admission_observed = false;
  {
    std::unique_lock<std::mutex> lock(mutex);
    setup_admission_observed = cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return setup_admission_entered; });
    release_collection = true;
  }
  cv.notify_all();
  setup.join();
  EXPECT_TRUE(setup_admission_observed);
  ASSERT_TRUE(setup_done.load());
  EXPECT_TRUE(setup_status.ok()) << setup_status.ToString();
  blocker_thread.join();
  EXPECT_TRUE(blocker_status.ok()) << blocker_status.ToString();
  ASSERT_TRUE(database->Close().ok());
}
}  // namespace cedar::benchmark
