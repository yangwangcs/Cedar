#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "query/projection/projection_store.h"

namespace cedar::internal {
namespace {

std::optional<std::string> ArgValue(const char* prefix) {
  for (const std::string& arg : ::testing::internal::GetArgvs()) {
    if (arg.rfind(prefix, 0) == 0) return arg.substr(std::strlen(prefix));
  }
  return std::nullopt;
}

void RunCrashChildIfRequested() {
  const auto phase = ArgValue("--query-crash-phase=");
  const auto db = ArgValue("--query-db=");
  const auto ready = ArgValue("--query-ready-fd=");
  if (!phase || !db || !ready) return;
  std::filesystem::create_directories(*db + "/manifests");
  // Each phase represents the point after a durable write but before its
  // publication edge. The child intentionally remains alive until SIGKILL;
  // the parent then reopens the directory and validates canonical recovery.
  std::ofstream(*db + "/" + *phase + ".csegment.tmp") << "unpublished";
  std::ofstream(*db + "/manifests/7.cmanifest.tmp") << "unpublished";
  const int fd = std::stoi(*ready);
  const char byte = 'R';
  (void)::write(fd, &byte, 1);
  for (;;) ::pause();
}

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

TEST_F(QueryCrashMatrixTest, CrashPhaseArgumentsSurviveSigkillAndReopen) {
  RunCrashChildIfRequested();
  const std::vector<std::string> phases = {"segment_sync", "manifest_sync",
                                           "current_replace", "delta_enqueue",
                                           "scratch_write"};
  const auto argv = ::testing::internal::GetArgvs();
  ASSERT_FALSE(argv.empty());
  for (const std::string& phase : phases) {
    const std::string child_db = path_ + "/" + phase;
    std::filesystem::create_directories(child_db);
    int ready_pipe[2];
    ASSERT_EQ(::pipe(ready_pipe), 0);
    const pid_t pid = ::fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
      ::close(ready_pipe[0]);
      const std::string phase_arg = "--query-crash-phase=" + phase;
      const std::string db_arg = "--query-db=" + child_db;
      const std::string fd_arg = "--query-ready-fd=" + std::to_string(ready_pipe[1]);
      ::execl(argv[0].c_str(), argv[0].c_str(), phase_arg.c_str(), db_arg.c_str(),
              fd_arg.c_str(), "--gtest_filter=QueryCrashMatrixTest.CrashPhaseArgumentsSurviveSigkillAndReopen",
              static_cast<char*>(nullptr));
      _exit(127);
    }
    ::close(ready_pipe[1]);
    char ready = 0;
    ASSERT_EQ(::read(ready_pipe[0], &ready, 1), 1);
    ::close(ready_pipe[0]);
    ASSERT_EQ(::kill(pid, SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(pid, &status, 0), pid);
    EXPECT_TRUE(WIFSIGNALED(status));
    EXPECT_EQ(WTERMSIG(status), SIGKILL);
    auto reopened = QueryProjectionStore::Open({child_db, "query-db", {}});
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    EXPECT_FALSE(reopened.ValueOrDie()->projections_enabled());
    for (const auto& entry : std::filesystem::recursive_directory_iterator(child_db)) {
      EXPECT_NE(entry.path().extension(), ".tmp");
    }
  }
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
