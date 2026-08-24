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
#include <atomic>
#include <thread>

#include "cedar/database.h"
#include "cedar/core/crc32c.h"
#include "cedar/storage_files.h"
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
  // Exercise the real publication lanes concurrently before entering the
  // selected fault boundary. This keeps the crash matrix meaningful when a
  // hook is reached after ordinary commit/query activity.
  std::atomic<bool> workload_done{false};
  std::atomic<uint32_t> lanes_ready{0};
  std::atomic<bool> start_lanes{false};
  // Start derived publication before the commit/query workers so the crash
  // matrix exercises all three Cedar-owned lanes concurrently.
  std::thread projection_thread([&] {
    lanes_ready.fetch_add(1, std::memory_order_release);
    while (!start_lanes.load(std::memory_order_acquire)) std::this_thread::yield();
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
  });
  std::thread commit_thread([&] {
    lanes_ready.fetch_add(1, std::memory_order_release);
    while (!start_lanes.load(std::memory_order_acquire)) std::this_thread::yield();
    for (uint64_t id = 100; id < 108; ++id) {
      auto tx = database->BeginTransaction();
      if (!tx.ok() || !tx.ValueOrDie()
                         ->Assert(cedar::EntityFact::Vertex(
                                      cedar::VertexRef{cedar::PartId{0}, id}),
                                  cedar::ValidTime{1})
                         .ok() ||
          !tx.ValueOrDie()->Commit().ok()) {
        _exit(126);
      }
    }
  });
  std::thread query_thread([&] {
    lanes_ready.fetch_add(1, std::memory_order_release);
    while (!start_lanes.load(std::memory_order_acquire)) std::this_thread::yield();
    auto vertex = cedar::Slot<cedar::VertexRef>::Named("workload_vertex");
    auto query = cedar::Query::Vertices(vertex, cedar::At{cedar::ValidTime{1}});
    if (!query.ok()) _exit(127);
    auto selected = query.ValueOrDie().Select({cedar::Project(vertex)});
    auto prepared = selected.ok() ? database->PrepareQuery(selected.ValueOrDie())
                                  : cedar::StatusOr<cedar::PreparedQuery>(selected.status());
    if (!prepared.ok()) _exit(127);
    auto snapshot = database->BeginSnapshot();
    if (!snapshot.ok()) _exit(127);
    auto cursor = prepared.ValueOrDie().Execute(
        std::move(snapshot).ConsumeValueOrDie(), cedar::Bindings{},
        cedar::QueryOptions{});
    if (!cursor.ok()) _exit(127);
    for (;;) {
      auto batch = cursor.ValueOrDie().Next();
      if (!batch.ok() || !batch.ValueOrDie().has_value()) break;
    }
    workload_done.store(true, std::memory_order_release);
  });
  while (lanes_ready.load(std::memory_order_acquire) != 3) {
    std::this_thread::yield();
  }
  start_lanes.store(true, std::memory_order_release);
  commit_thread.join();
  query_thread.join();
  if (!workload_done.load(std::memory_order_acquire)) _exit(128);
  // Exercise the actual Cedar publication owners. The callback pauses only
  // after the requested durable boundary has been reached.
  if (*phase == "cursor_cancel" || *phase == "cursor_close_before" ||
      *phase == "cursor_close_after") {
    auto vertex = cedar::Slot<cedar::VertexRef>::Named("crash_vertex");
    auto query = cedar::Query::Vertices(vertex, cedar::At{cedar::ValidTime{1}});
    if (!query.ok()) _exit(125);
    auto selected = query.ValueOrDie().Select({cedar::Project(vertex)});
    if (!selected.ok()) _exit(125);
    auto prepared = database->PrepareQuery(selected.ValueOrDie());
    auto snapshot = database->BeginSnapshot();
    if (!prepared.ok() || !snapshot.ok()) _exit(125);
    auto cursor = prepared.ValueOrDie().Execute(
        std::move(snapshot).ConsumeValueOrDie(), cedar::Bindings{},
        cedar::QueryOptions{});
    if (!cursor.ok()) _exit(125);
    if (*phase == "cursor_cancel") {
      (void)cursor.ValueOrDie().Cancel();
    } else {
      (void)cursor.ValueOrDie().Close();
    }
  } else if (phase->rfind("scratch_", 0) == 0 || *phase == "scratch_write") {
    QueryScratch scratch(*db, "active", "crash", 1 << 20);
    scratch.SetCrashFaultInjector(crash_injector);
    (void)scratch.WriteRun("run-0", "payload");
  } else if (*phase == "delta_enqueue") {
    QueryDelta delta(QueryDeltaOptions{CommitSeq{0}, 16, 256ULL << 20,
                                       512ULL << 20, 262144, 30,
                                       crash_injector});
    QueryDeltaCommit descriptor(CommitSeq{1});
    (void)delta.EnqueuePublished(descriptor);
  }
  projection_thread.join();
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
  const std::vector<std::string> phases = {
      "segment_write_before", "segment_write_after", "segment_sync_before",
      "segment_sync_after", "segment_rename_before", "segment_rename_after",
      "manifest_write_before", "manifest_write_after", "manifest_sync_before",
      "manifest_sync_after", "manifest_rename_before", "manifest_rename_after",
      "current_write_before", "current_write_after", "current_rename_before",
      "current_rename_after", "current_replace", "delta_enqueue",
      "scratch_write_before", "scratch_write_after", "scratch_rename_before",
      "scratch_rename_after", "cursor_cancel", "cursor_close_before",
      "cursor_close_after"};
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
    const CommitSeq reopened_cut = snapshot.ValueOrDie().commit_seq();
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
    auto cursor_again = prepared.ValueOrDie().Execute(
        [&]() {
          auto next_snapshot = database->BeginSnapshot();
          EXPECT_TRUE(next_snapshot.ok()) << next_snapshot.status().ToString();
          return std::move(next_snapshot).ConsumeValueOrDie();
        }(),
        cedar::Bindings{}, cedar::QueryOptions{});
    ASSERT_TRUE(cursor_again.ok()) << cursor_again.status().ToString();
    for (;;) {
      auto next = cursor_again.ValueOrDie().Next();
      ASSERT_TRUE(next.ok()) << next.status().ToString();
      if (!next.ValueOrDie().has_value()) break;
    }
    EXPECT_TRUE(cursor_again.ValueOrDie().terminal_info().complete);
    auto projection_store = QueryProjectionStore::Open(
        ProjectionStoreOptions{child_db + "/projections", child_db, {}});
    ASSERT_TRUE(projection_store.ok()) << projection_store.status().ToString();
    if (projection_store.ValueOrDie()->projections_enabled()) {
      const auto current_base = projection_store.ValueOrDie()->current_base_seq();
      ASSERT_TRUE(current_base.has_value());
      EXPECT_LE(current_base->value, reopened_cut.value);
    }
    ASSERT_TRUE(database->Close().ok());
    for (const auto& entry : std::filesystem::recursive_directory_iterator(child_db)) {
      EXPECT_NE(entry.path().extension(), ".tmp");
      EXPECT_NE(entry.path().extension(), ".cscratch");
      EXPECT_EQ(entry.path().string().find("/query/scratch/"), std::string::npos);
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

TEST_F(QueryCrashMatrixTest, CloseAndVacuumRemainIdempotentAcrossRepeatedPins) {
  const std::string database_path = path_ + "/lifecycle";
  auto opened = cedar::Database::Open(cedar::DatabaseOptions{.path = database_path});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(cedar::EntityFact::Vertex(
                               cedar::VertexRef{cedar::PartId{0}, cedar::VertexId{9}}),
                           cedar::ValidTime{0})
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());
  auto vertex = cedar::Slot<cedar::VertexRef>::Named("lifecycle_vertex");
  auto query = cedar::Query::Vertices(vertex, cedar::At{cedar::ValidTime{1}});
  ASSERT_TRUE(query.ok());
  auto selected = query.ValueOrDie().Select({cedar::Project(vertex)});
  ASSERT_TRUE(selected.ok());
  auto prepared = database->PrepareQuery(selected.ValueOrDie());
  ASSERT_TRUE(prepared.ok());
  for (int iteration = 0; iteration < 100; ++iteration) {
    auto snapshot = database->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok());
    auto cursor = prepared.ValueOrDie().Execute(
        std::move(snapshot).ConsumeValueOrDie(), cedar::Bindings{},
        cedar::QueryOptions{});
    ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
    if ((iteration & 1) == 0) {
      EXPECT_TRUE(cursor.ValueOrDie().Cancel().ok());
    }
    EXPECT_TRUE(cursor.ValueOrDie().Close().ok());
    EXPECT_TRUE(database->Vacuum(cedar::CommitSeq{1}).ok())
        << "iteration=" << iteration;
  }
  EXPECT_TRUE(database->Close().ok());
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

TEST_F(QueryCrashMatrixTest,
       AuthoritativeFactsCorruptionRequiresRecoveryWhileProjectionFailsClosed) {
  const std::vector<std::string> mutations = {"bitflip", "truncate", "delete"};
  for (const std::string& mutation : mutations) {
    const std::string database_path = path_ + "/facts-" + mutation;
    DatabaseOptions options;
    options.path = database_path;
    options.storage_profile = StorageProfile::kDebugSmallThresholds;
    auto opened = Database::Open(std::move(options));
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    auto database = std::move(opened).ConsumeValueOrDie();
    for (uint64_t first_vertex = 1; first_vertex <= 4096; first_vertex += 128) {
      auto transaction = database->BeginTransaction();
      ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
      for (uint64_t vertex = first_vertex; vertex < first_vertex + 128; ++vertex) {
        ASSERT_TRUE(transaction.ValueOrDie()
                        ->Assert(EntityFact::Vertex(
                                     VertexRef{PartId{0}, VertexId{vertex}}),
                                 ValidTime{1})
                        .ok());
      }
      ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());
    }
    ASSERT_TRUE(database->Close().ok());

    auto inspected = InspectStorageFiles({.path = database_path,
                                          .storage_profile =
                                              StorageProfile::kDebugSmallThresholds});
    ASSERT_TRUE(inspected.ok()) << inspected.status().ToString();
    std::filesystem::path facts_sst;
    for (const StorageFileInfo& file : inspected.ValueOrDie()) {
      if (file.role == StorageFileRole::kAuthoritativeFacts &&
          file.table_format == StorageTableFormat::kCedarParquet) {
        facts_sst = std::filesystem::path(database_path) / file.relative_filename;
        break;
      }
    }
    ASSERT_FALSE(facts_sst.empty()) << "no authoritative facts SST for " << mutation
                                    << " files=" << inspected.ValueOrDie().size();
    ASSERT_TRUE(std::filesystem::exists(facts_sst));
    if (mutation == "bitflip") {
      std::fstream file(facts_sst, std::ios::in | std::ios::out | std::ios::binary);
      ASSERT_TRUE(file.good());
      file.seekg(0, std::ios::end);
      const auto size = file.tellg();
      ASSERT_GT(size, 32);
      // Corrupt the table header so the first canonical scan must observe it.
      file.seekg(0);
      char byte = 0;
      file.read(&byte, 1);
      byte ^= static_cast<char>(0x01);
      file.seekp(0);
      file.write(&byte, 1);
      file.flush();
    } else if (mutation == "truncate") {
      const uintmax_t size = std::filesystem::file_size(facts_sst);
      ASSERT_GT(size, 32U);
      std::filesystem::resize_file(facts_sst, size / 2);
    } else {
      ASSERT_TRUE(std::filesystem::remove(facts_sst));
    }

    auto reopened = Database::Open(DatabaseOptions{.path = database_path});
    Status canonical_status = reopened.ok() ? Status::OK() : reopened.status();
    if (reopened.ok()) {
      auto database = std::move(reopened).ConsumeValueOrDie();
      auto snapshot = database->BeginSnapshot();
      if (!snapshot.ok()) {
        canonical_status = snapshot.status();
      } else {
        auto vertex = Slot<VertexRef>::Named("canonical_corrupt_vertex");
        auto scan = Query::Vertices(vertex, At{ValidTime{1}});
        auto selected = scan.ok() ? scan.ValueOrDie().Select({Project(vertex)})
                                  : StatusOr<Query>{scan.status()};
        auto prepared = selected.ok() ? database->PrepareQuery(selected.ValueOrDie())
                                      : StatusOr<PreparedQuery>{selected.status()};
        auto cursor = prepared.ok()
                          ? prepared.ValueOrDie().Execute(
                                std::move(snapshot).ConsumeValueOrDie(), Bindings{},
                                QueryOptions{})
                          : StatusOr<QueryCursor>{prepared.status()};
        if (!cursor.ok()) {
          canonical_status = cursor.status();
        } else {
          for (;;) {
            auto batch = cursor.ValueOrDie().Next();
            if (!batch.ok()) {
              canonical_status = batch.status();
              break;
            }
            if (!batch.ValueOrDie().has_value()) break;
          }
        }
      }
      database->Close().IgnoreError();
    }
    EXPECT_TRUE(canonical_status.IsRecoveryRequired() ||
                canonical_status.IsCorruption() || canonical_status.IsIOError())
        << mutation << ": canonical corruption was not surfaced: "
        << canonical_status.ToString();

    const std::string projection_path = path_ + "/projection-" + mutation;
    auto projection = QueryProjectionStore::Open(
        ProjectionStoreOptions{projection_path, "query-db", {}});
    ASSERT_TRUE(projection.ok()) << projection.status().ToString();
    ProjectionBuild build;
    build.manifest.database_identity = "query-db";
    build.manifest.generation_id = 1;
    build.manifest.base_seq = CommitSeq{1};
    CoverageRegion region;
    region.kind = ProjectionKind::kState;
    region.part_id = PartId{0};
    region.entity_max_exclusive = 2;
    region.valid_time = {ValidTime{0}, std::nullopt};
    SegmentDescriptor descriptor;
    descriptor.segment_id = "derived-corrupt";
    descriptor.filename = "derived-corrupt.csegment";
    descriptor.header.kind = ProjectionKind::kState;
    descriptor.header.generation_id = 1;
    descriptor.header.base_seq = CommitSeq{1};
    descriptor.header.part_id = PartId{0};
    descriptor.header.entity_max_exclusive = 2;
    descriptor.header.valid_from_min = ValidTime{0};
    ProjectionChain chain;
    chain.header = descriptor.header;
    chain.intervals.push_back({{ValidTime{0}, std::nullopt}, Value::Int64(1), 1});
    auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    descriptor.file_bytes = encoded.ValueOrDie().size();
    descriptor.checksum = crc32c::Value(encoded.ValueOrDie().data(),
                                        encoded.ValueOrDie().size());
    region.segments.push_back(descriptor);
    build.manifest.regions.push_back(region);
    build.segments.push_back({descriptor, encoded.ValueOrDie()});
    ASSERT_TRUE(projection.ValueOrDie()->Build(build).ok());
    ASSERT_TRUE(std::filesystem::remove(projection_path + "/" + descriptor.filename));
    CoverageRequest request;
    request.kind = ProjectionKind::kState;
    request.part_id = PartId{0};
    request.entity_min = 0;
    request.entity_max_exclusive = 2;
    request.valid_time = {ValidTime{0}, std::nullopt};
    request.snapshot_seq = CommitSeq{1};
    EXPECT_TRUE(projection.ValueOrDie()->ReadChains(request).status().IsNotFound());
    EXPECT_FALSE(projection.ValueOrDie()->projections_enabled());
    EXPECT_EQ(projection.ValueOrDie()->pending_rebuild_requests(), 1U);
  }
}

}  // namespace
}  // namespace cedar::internal
