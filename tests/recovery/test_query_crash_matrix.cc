#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "query/projection/projection_store.h"

namespace cedar::internal {
namespace {

class QueryCrashMatrixTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = (std::filesystem::temp_directory_path() /
             "cedar_query_crash_matrix_test")
                .string();
    std::filesystem::remove_all(path_);
    ASSERT_TRUE(std::filesystem::create_directories(path_));
  }
  void TearDown() override { std::filesystem::remove_all(path_); }
  std::string path_;
};

TEST_F(QueryCrashMatrixTest, CorruptCurrentDisablesDerivedReaders) {
  std::ofstream(path_ + "/PROJECTION-CURRENT") << "corrupt";
  auto opened = QueryProjectionStore::Open({path_, "query-db", {}});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  EXPECT_FALSE(opened.ValueOrDie()->projections_enabled());
  EXPECT_FALSE(opened.ValueOrDie()->current_generation_id().has_value());
}

TEST_F(QueryCrashMatrixTest, UnpublishedTemporaryFilesAreIgnoredOnOpen) {
  std::ofstream(path_ + "/orphan.csegment.tmp") << "unpublished";
  std::filesystem::create_directories(path_ + "/manifests");
  std::ofstream(path_ + "/manifests/9.cmanifest.tmp") << "unpublished";
  auto opened = QueryProjectionStore::Open({path_, "query-db", {}});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  EXPECT_FALSE(opened.ValueOrDie()->projections_enabled());
}

}  // namespace
}  // namespace cedar::internal
