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

#include "cedar/database.h"
#include "cedar/core/crc32c.h"
#include "cedar/query/query.h"
#include "query/projection/projection_store.h"
#include "query/projection/projection_format.h"
#include "query/projection/query_delta.h"
#include "query/resource/query_scratch.h"

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
  cedar::DatabaseOptions options;
  options.path = *db;
  const int fd = std::stoi(*ready);
  auto crash_injector = [phase = *phase, fd](const char* point) {
    if (phase != point) return cedar::Status::OK();
    const char byte = 'R';
    (void)::write(fd, &byte, 1);
    for (;;) ::pause();
  };
  options.query_crash_fault_injector_for_testing = crash_injector;
  auto opened = cedar::Database::Open(std::move(options));
  if (!opened.ok()) _exit(120);
  auto database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  if (!transaction.ok() ||
      !transaction.ValueOrDie()
           ->Assert(cedar::EntityFact::Vertex(
                        cedar::VertexRef{cedar::PartId{0}, cedar::VertexId{42}}),
                    cedar::ValidTime{1})
           .ok() ||
      !transaction.ValueOrDie()->Commit().ok()) {
    _exit(121);
  }
  // Exercise the actual Cedar publication owners. The callback pauses only
  // after the requested durable boundary has been reached.
  if (*phase == "scratch_write") {
    QueryScratch scratch(*db, "active", "crash", 1 << 20);
    scratch.SetCrashFaultInjector(crash_injector);
    (void)scratch.WriteRun("run-0", "payload");
  } else if (*phase == "delta_enqueue") {
    QueryDelta delta(QueryDeltaOptions{CommitSeq{0}, 16, 256ULL << 20,
                                       512ULL << 20, 262144, 30,
                                       crash_injector});
    QueryDeltaCommit descriptor(CommitSeq{1});
    (void)delta.EnqueuePublished(descriptor);
  } else {
    ProjectionStoreOptions projection_options;
    projection_options.path = *db + "/projections";
    projection_options.database_identity = *db;
    projection_options.crash_fault_injector = crash_injector;
    auto store = QueryProjectionStore::Open(std::move(projection_options));
    if (!store.ok()) _exit(122);
    ProjectionBuild build;
    build.manifest.database_identity = *db;
    build.manifest.generation_id = 1;
    build.manifest.base_seq = CommitSeq{1};
    CoverageRegion region;
    region.kind = ProjectionKind::kState;
    region.part_id = PartId{0};
    region.schema_epoch = 0;
    region.entity_min = 0;
    region.entity_max_exclusive = 100;
    region.valid_time = ValidTimeInterval{ValidTime{0}, std::nullopt};
    SegmentDescriptor descriptor;
    descriptor.segment_id = "crash-segment";
    descriptor.filename = "crash-segment.csegment";
    descriptor.header.kind = ProjectionKind::kState;
    descriptor.header.generation_id = 1;
    descriptor.header.base_seq = CommitSeq{1};
    descriptor.header.part_id = PartId{0};
    descriptor.header.schema_epoch = 0;
    descriptor.header.entity_min = 0;
    descriptor.header.entity_max_exclusive = 100;
    descriptor.header.valid_from_min = ValidTime{0};
    ProjectionChain chain;
    chain.header = descriptor.header;
    chain.intervals.push_back(ProjectionInterval{
        ValidTimeInterval{ValidTime{0}, std::nullopt}, Value::Int64(1), 42});
    auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
    if (!encoded.ok()) _exit(123);
    descriptor.file_bytes = encoded.ValueOrDie().size();
    descriptor.checksum = crc32c::Value(encoded.ValueOrDie().data(), encoded.ValueOrDie().size());
    region.segments.push_back(descriptor);
    build.manifest.regions.push_back(region);
    build.segments.push_back(ProjectionSegmentInput{descriptor, encoded.ValueOrDie()});
    (void)store.ValueOrDie()->Build(build);
  }
  _exit(124);  // The selected hook must have paused before reaching here.
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
    std::vector<std::string> stages;
    cedar::DatabaseOptions options;
    options.path = child_db;
    options.query_open_stage_observer_for_testing =
        [&stages](const char* stage) { stages.emplace_back(stage); };
    auto reopened = cedar::Database::Open(std::move(options));
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    auto database = std::move(reopened).ConsumeValueOrDie();
    ASSERT_GE(stages.size(), 3U);
    EXPECT_EQ(stages[0], "authoritative_recovery");
    EXPECT_EQ(stages[1], "query_delta_repaired");
    EXPECT_EQ(stages[2], "derived_loaded");
    auto snapshot = database->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    EXPECT_GE(snapshot.ValueOrDie().commit_seq().value, 1U);
    auto vertex = cedar::Slot<cedar::VertexRef>::Named("v");
    auto scan = cedar::Query::Vertices(vertex, cedar::At{cedar::ValidTime{1}});
    ASSERT_TRUE(scan.ok()) << scan.status().ToString();
    auto query = scan.ValueOrDie().Select({cedar::Project(vertex)});
    ASSERT_TRUE(query.ok()) << query.status().ToString();
    auto prepared = database->PrepareQuery(query.ValueOrDie());
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
    auto cursor = prepared.ValueOrDie().Execute(
        std::move(snapshot).ConsumeValueOrDie(), cedar::Bindings{},
        cedar::QueryOptions{});
    ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
    auto batch = std::move(cursor).ConsumeValueOrDie().Next();
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    ASSERT_TRUE(batch.ValueOrDie().has_value());
    EXPECT_GE(batch.ValueOrDie()->row_count(), 1U);
    ASSERT_TRUE(database->Close().ok());
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

TEST_F(QueryCrashMatrixTest, ProjectionBitFlipDeletionAndTruncationFailClosed) {
  ProjectionChain chain;
  chain.header.kind = ProjectionKind::kState;
  chain.header.generation_id = 1;
  chain.header.base_seq = CommitSeq{1};
  chain.header.part_id = PartId{0};
  chain.header.entity_min = 1;
  chain.header.entity_max_exclusive = 2;
  chain.header.valid_from_min = ValidTime{0};
  chain.intervals.push_back({ValidTimeInterval{ValidTime{0}, std::nullopt},
                             Value::Int64(7), 1});
  auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  const std::string good = encoded.ValueOrDie();
  std::string flipped = good;
  ASSERT_GT(flipped.size(), 16U);
  flipped[16] ^= 0x01;
  EXPECT_TRUE(DecodeProjectionPage(flipped).status().IsCorruption());
  EXPECT_TRUE(DecodeProjectionPage(good.substr(0, good.size() - 1))
                  .status()
                  .IsCorruption());
  EXPECT_TRUE(DecodeProjectionPage(std::string{}).status().IsCorruption());
  std::ofstream(path_ + "/segment.csegment", std::ios::binary)
      .write(flipped.data(), static_cast<std::streamsize>(flipped.size()));
  auto opened = QueryProjectionStore::Open({path_, "query-db", {}});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  EXPECT_FALSE(opened.ValueOrDie()->projections_enabled());
}

}  // namespace
}  // namespace cedar::internal
