// Cedar 0.1 持久化 / 墓碑 / 恢复回归测试（Task 1.1-1.3）
//
// 覆盖审计条目：
//   3.2 [P0] Flush 丢弃 tombstone；读路径不过滤墓碑 → 删除复活
//   3.3 [P0] 没有 WAL 重放；Commit 先写 memtable 后写 WAL → 已提交数据不可恢复
//   3.4 [P1] 没有自动 flush 触发；ForceFlush 失败直接丢数据
//   3.5 [P0] 活跃事务/读路径持有已被 flush 销毁的裸 memtable 指针
//
// 说明（审计 10.1 隔离）：单行 SST 的时间戳 zone 恰好 8 字节时解码为空，
// 由 Task 1.4（列式格式）修复，不属于本测试的验证目标。凡需要 flush 墓碑的
// 用例都在同一 (entity_type, column_id) 分组内附带至少一行其它数据，使本次
// flush 的 SST 不是单行文件；10.1 本身由
// ZoneColumnarRoundtripTest.SingleRowSstIsReadable 覆盖。

#include <gtest/gtest.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "cedar/core/env.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/vsl_memtable.h"
#include "cedar/storage/wal_replay.h"
#include "cedar/transaction/occ_transaction.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "sst_test_utils.h"

namespace {

using namespace cedar;

constexpr uint16_t kColumn = 3;
constexpr uint64_t kEntity = 42;

// 统计 LSM 中所有层级的 SST 行数（flush 产物）
uint64_t CountSstRows(const LsmEngine& engine) {
  uint64_t rows = 0;
  for (int level = 0; level < 7; ++level) {
    for (const auto& meta : engine.GetFrondFiles(level)) {
      rows += meta.num_entries;
    }
  }
  return rows;
}

bool IsTombstone(const MemTableEntry& entry) {
  return entry.descriptor.IsTombstone();
}

// 统计 db 目录里的 SST 文件数。崩溃模拟用例用它校验「数据只在 WAL」的前提：
// 子进程一旦走 LsmEngine 析构（Close → ForceFlush），数据就会落到 SST，
// 即使 WAL 重放有缺陷也会被 SST 掩盖（单行 SST 修好前尤其隐蔽）。
size_t CountSstFiles(const std::string& dir) {
  size_t count = 0;
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
    if (entry.is_regular_file() && entry.path().extension() == ".frond") {
      ++count;
    }
  }
  return count;
}

// 临时修改目录权限（失败注入），析构时一定恢复，避免只读目录让 /tmp 清理失败
class ScopedDirMode {
 public:
  ScopedDirMode(const std::string& dir, mode_t mode) : dir_(dir) {
    ::chmod(dir_.c_str(), mode);
  }
  ~ScopedDirMode() { Restore(); }

  ScopedDirMode(const ScopedDirMode&) = delete;
  ScopedDirMode& operator=(const ScopedDirMode&) = delete;

  void Restore() { ::chmod(dir_.c_str(), 0755); }

 private:
  std::string dir_;
};

CedarOptions MakeOptions() {
  CedarOptions options;
  options.create_if_missing = true;
  options.env = Env::Default();
  return options;
}

// =============================================================================
// Task 1.1：墓碑持久化（审计 3.2）
// =============================================================================

TEST(FlushRecoveryTest, DeleteSurvivesFlushAndReopen) {
  cedar_test::ScopedTempDir temp_dir("flush_recovery");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kOlderTs = 1000;
  constexpr uint64_t kNewerTs = 1001;

  const CedarKey older_key = CedarKey::Vertex(kEntity, kColumn, Timestamp(kOlderTs));
  const CedarKey newer_key = CedarKey::Vertex(kEntity, kColumn, Timestamp(kNewerTs));
  // 与墓碑同属 (Vertex, kColumn) 分组的第二行：保证第二次 flush 的 SST 不是
  // 单行文件（见文件头关于审计 10.1 的说明）。
  const CedarKey companion_key = CedarKey::Vertex(43, kColumn, Timestamp(2000));

  // 第一段生命周期：Put(2 版本) -> flush -> Delete(最新) -> flush -> Close
  {
    CedarOptions options = MakeOptions();
    LsmEngine engine(dir, options, Env::Default());
    ASSERT_TRUE(engine.Open().ok());

    ASSERT_TRUE(
        engine.Put(older_key, Descriptor::InlineInt(kColumn, 100), Timestamp(1)).ok());
    ASSERT_TRUE(
        engine.Put(newer_key, Descriptor::InlineInt(kColumn, 123), Timestamp(2)).ok());

    ASSERT_TRUE(engine.ForceFlush().ok()) << "第一次 flush 失败";

    auto flushed_older = engine.Get(older_key);
    ASSERT_TRUE(flushed_older.has_value())
        << "flush 后旧版本应当可读（若失败，说明是审计 10.1 的 SST 解码问题而非 3.2）";
    EXPECT_EQ(flushed_older->AsInlineInt().value_or(-1), 100);
    auto flushed_newer = engine.Get(newer_key);
    ASSERT_TRUE(flushed_newer.has_value());
    EXPECT_EQ(flushed_newer->AsInlineInt().value_or(-1), 123);

    ASSERT_TRUE(engine.Delete(newer_key, Timestamp(3)).ok());
    ASSERT_TRUE(
        engine.Put(companion_key, Descriptor::InlineInt(kColumn, 7), Timestamp(4)).ok());
    ASSERT_TRUE(engine.ForceFlush().ok()) << "第二次 flush 失败";

    EXPECT_FALSE(engine.Get(newer_key).has_value())
        << "Delete + flush 后仍能读到值（审计 3.2：flush 丢弃 tombstone）";
    auto companion = engine.Get(companion_key);
    ASSERT_TRUE(companion.has_value());
    EXPECT_EQ(companion->AsInlineInt().value_or(-1), 7);

    ASSERT_TRUE(engine.Close().ok());
  }

  // 第二段生命周期：重开数据库，删除必须仍然生效
  {
    CedarOptions options = MakeOptions();
    LsmEngine engine(dir, options, Env::Default());
    ASSERT_TRUE(engine.Open().ok());

    auto after_reopen = engine.Get(newer_key);
    EXPECT_FALSE(after_reopen.has_value())
        << "删除的 key 在 flush + 重开后复活（审计 3.2：tombstone 未落盘，"
        << "且读路径不过滤墓碑）；value="
        << (after_reopen.has_value() ? after_reopen->AsInlineInt().value_or(-1) : -1);

    auto companion = engine.Get(companion_key);
    ASSERT_TRUE(companion.has_value());
    EXPECT_EQ(companion->AsInlineInt().value_or(-1), 7);

    engine.Close();
  }
}

TEST(FlushRecoveryTest, TombstoneHiddenFromAllReadPaths) {
  cedar_test::ScopedTempDir temp_dir("tombstone_read_paths");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  const CedarKey k100 = CedarKey::Vertex(kEntity, kColumn, Timestamp(100));
  const CedarKey k200 = CedarKey::Vertex(kEntity, kColumn, Timestamp(200));
  const CedarKey k300 = CedarKey::Vertex(kEntity, kColumn, Timestamp(300));
  const CedarKey kTomb = CedarKey::Vertex(kEntity, kColumn, Timestamp(400));
  const CedarKey kCompanion = CedarKey::Vertex(7, kColumn, Timestamp(500));

  CedarOptions options = MakeOptions();
  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  ASSERT_TRUE(engine.Put(k100, Descriptor::InlineInt(kColumn, 1), Timestamp(1)).ok());
  ASSERT_TRUE(engine.Put(k200, Descriptor::InlineInt(kColumn, 2), Timestamp(2)).ok());
  ASSERT_TRUE(engine.Put(k300, Descriptor::InlineInt(kColumn, 3), Timestamp(3)).ok());
  ASSERT_TRUE(engine.ForceFlush().ok());

  // 删除 epoch 400（墓碑与历史版本同组，flush 后文件非单行）
  ASSERT_TRUE(engine.Delete(kTomb, Timestamp(4)).ok());
  ASSERT_TRUE(
      engine.Put(kCompanion, Descriptor::InlineInt(kColumn, 9), Timestamp(5)).ok());
  ASSERT_TRUE(engine.ForceFlush().ok());

  // 1) 精确 key 点查：墓碑必须视为 not-found
  EXPECT_FALSE(engine.Get(kTomb).has_value()) << "点查未过滤墓碑（审计 3.2）";

  // 2) 历史版本仍然可查（墓碑只影响它自己覆盖的版本）
  auto at_350 = engine.GetAtTime(kEntity, EntityType::Vertex, kColumn, Timestamp(350));
  ASSERT_TRUE(at_350.has_value());
  EXPECT_EQ(at_350->AsInlineInt().value_or(-1), 3);

  // 3) GetAtTime 命中墓碑时返回 not-found，而不是回退到更老版本
  auto at_400 = engine.GetAtTime(kEntity, EntityType::Vertex, kColumn, Timestamp(400));
  EXPECT_FALSE(at_400.has_value())
      << "GetAtTime 命中墓碑后回退到更老版本（删除被绕过）";

  // 4) GetAll / GetRange 不返回墓碑条目，但保留历史值
  auto all = engine.GetAll(kEntity, EntityType::Vertex, kColumn);
  for (const auto& entry : all) {
    EXPECT_FALSE(IsTombstone(entry)) << "GetAll 返回了墓碑条目（审计 3.2）";
  }
  EXPECT_EQ(all.size(), 3u) << "GetAll 应返回 3 个历史版本（墓碑不返回）";

  auto range = engine.GetRange(kEntity, EntityType::Vertex, kColumn,
                               Timestamp(0), Timestamp::Max());
  for (const auto& entry : range) {
    EXPECT_FALSE(IsTombstone(entry)) << "GetRange 返回了墓碑条目（审计 3.2）";
  }
  EXPECT_EQ(range.size(), 3u);

  // 5) 重开后语义一致
  ASSERT_TRUE(engine.Close().ok());
  LsmEngine reopened(dir, options, Env::Default());
  ASSERT_TRUE(reopened.Open().ok());
  EXPECT_FALSE(reopened.Get(kTomb).has_value());
  EXPECT_FALSE(reopened.GetAtTime(kEntity, EntityType::Vertex, kColumn,
                                  Timestamp(400)).has_value());
  auto reopened_all = reopened.GetAll(kEntity, EntityType::Vertex, kColumn);
  EXPECT_EQ(reopened_all.size(), 3u);
  for (const auto& entry : reopened_all) {
    EXPECT_FALSE(IsTombstone(entry));
  }
  reopened.Close();
}

// =============================================================================
// Task 1.3：Flush 失败注入与重试（审计 3.4）
// =============================================================================

TEST(FlushRecoveryTest, ForceFlushFailureKeepsDataAndRetrySucceeds) {
  if (::geteuid() == 0) {
    GTEST_SKIP() << "以 root 运行时目录权限无法拒绝写入，跳过失败注入";
  }

  cedar_test::ScopedTempDir temp_dir("flush_inject");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  const CedarKey key = CedarKey::Vertex(kEntity, kColumn, Timestamp(1000));

  CedarOptions options = MakeOptions();
  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  ASSERT_TRUE(engine.Put(key, Descriptor::InlineInt(kColumn, 55), Timestamp(1)).ok());
  // 让第二次 flush 的组非单行（审计 10.1 隔离）
  ASSERT_TRUE(
      engine.Put(CedarKey::Vertex(9, kColumn, Timestamp(1000)),
                 Descriptor::InlineInt(kColumn, 66), Timestamp(2)).ok());

  // 注入 IO 失败：目录只读 → NewWritableFile 失败
  ScopedDirMode readonly(dir, 0555);
  Status failed = engine.ForceFlush();
  EXPECT_FALSE(failed.ok()) << "ForceFlush 在 IO 失败时必须返回错误（审计 3.4）";
  if (failed.ok()) {
    GTEST_SKIP() << "当前文件系统未拒绝只读目录写入，无法注入 flush 失败";
  }

  // 失败后数据必须仍然可读（imm_ 不得被销毁）
  auto still_there = engine.Get(key);
  ASSERT_TRUE(still_there.has_value())
      << "ForceFlush 失败后数据不可读（审计 3.4：imm_ 被 reset）";
  EXPECT_EQ(still_there->AsInlineInt().value_or(-1), 55);

  // 恢复写权限后重试必须成功并把数据落盘
  readonly.Restore();
  Status retried = engine.ForceFlush();
  ASSERT_TRUE(retried.ok()) << retried.ToString();
  EXPECT_GT(CountSstRows(engine), 0u) << "重试成功后仍没有 SST 文件";

  auto after_retry = engine.Get(key);
  ASSERT_TRUE(after_retry.has_value());
  EXPECT_EQ(after_retry->AsInlineInt().value_or(-1), 55);

  ASSERT_TRUE(engine.Close().ok());
}

// =============================================================================
// Task 1.2：WAL-first 与崩溃重放（审计 3.3）
// =============================================================================

namespace {

struct CrashChildResult {
  int exit_code = 0;
  int term_signal = 0;
  bool signaled = false;
};

// 在子进程中执行 body()，完成后 SIGKILL 自己（不调用 Close / 不 flush），
// 模拟进程被杀死；父进程用 WAL 重放恢复。
template <typename Body>
CrashChildResult RunInCrashingChild(Body body) {
  CrashChildResult result;
  ::fflush(nullptr);
  const pid_t pid = ::fork();
  if (pid == 0) {
    // 子进程：任何异常都以退出码表达给父进程
    body();
    ::kill(::getpid(), SIGKILL);
    ::_exit(1);  // 不可达
  }
  if (pid < 0) {
    result.exit_code = -1;
    return result;
  }
  int status = 0;
  ::waitpid(pid, &status, 0);
  if (WIFSIGNALED(status)) {
    result.signaled = true;
    result.term_signal = WTERMSIG(status);
  } else if (WIFEXITED(status)) {
    result.exit_code = WEXITSTATUS(status);
  }
  return result;
}

}  // namespace

TEST(FlushRecoveryTest, WalReplayRecoversCommittedTransactionAfterCrash) {
  cedar_test::ScopedTempDir temp_dir("wal_replay_crash");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 5000;
  const CedarKey first = CedarKey::Vertex(kEntity, kColumn, Timestamp(kTs));
  const CedarKey second = CedarKey::Vertex(77, kColumn, Timestamp(kTs));

  const CrashChildResult child = RunInCrashingChild([&dir]() {
    CedarOptions options = MakeOptions();
    // 关键：engine 必须泄漏（new 不 delete）。若用栈对象，body() 返回时析构会走
    // Close() → ForceFlush()，在 SIGKILL 之前把 memtable 落成 SST，
    // 「数据只存在于 WAL」的前提就不成立（重放缺陷会被 SST 掩盖）。
    auto* engine = new LsmEngine(dir, options, Env::Default());
    if (!engine->Open().ok()) ::_exit(10);
    auto txn = engine->BeginTransaction();
    if (!txn) ::_exit(11);
    if (!txn->Put(kEntity, EntityType::Vertex, kColumn,
                  Descriptor::InlineInt(kColumn, 777), Timestamp(kTs)).ok()) {
      ::_exit(12);
    }
    if (!txn->Put(77, EntityType::Vertex, kColumn,
                  Descriptor::InlineInt(kColumn, 888), Timestamp(kTs)).ok()) {
      ::_exit(13);
    }
    if (!txn->Commit().ok()) ::_exit(14);
    // 不 Close、不 ForceFlush：数据只存在于 WAL，随后被 SIGKILL 打断
  });

  ASSERT_TRUE(child.signaled) << "子进程未按预期被 SIGKILL（exit="
                              << child.exit_code << "）";
  EXPECT_EQ(child.term_signal, SIGKILL);
  ASSERT_EQ(child.exit_code, 0) << "子进程在提交前失败，退出码 " << child.exit_code;
  // 前提校验：崩溃时数据只能存在于 WAL（子进程不得触发过 flush）
  ASSERT_EQ(CountSstFiles(dir), 0u)
      << "子进程在 SIGKILL 之前把 memtable flush 成了 SST（崩溃模拟失效："
      << "engine 析构 → Close → ForceFlush），本用例的「数据只在 WAL」前提不成立";

  CedarOptions options = MakeOptions();
  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok()) << "重开数据库失败（WAL 重放不应导致 Open 失败）";

  const WalReplayStats replay = engine.GetWalReplayStats();
  ASSERT_GT(replay.records, 0u)
      << "WAL 里没有读到任何 record（files=" << replay.files
      << " corrupt=" << replay.corrupted_records << "）";
  ASSERT_GT(replay.committed_records, 0u)
      << "WAL 里没有带提交围栏的 record（uncommitted="
      << replay.uncommitted_records << "）";
  ASSERT_EQ(replay.ops_applied, 2u)
      << "重放应写入 2 条 op（applied=" << replay.ops_applied << "）";

  auto first_value = engine.Get(first);
  ASSERT_TRUE(first_value.has_value())
      << "崩溃后已提交事务不可见（审计 3.3：没有 WAL 重放）";
  EXPECT_EQ(first_value->AsInlineInt().value_or(-1), 777);

  auto second_value = engine.Get(second);
  ASSERT_TRUE(second_value.has_value());
  EXPECT_EQ(second_value->AsInlineInt().value_or(-1), 888);

  // 重放应发生在接受写之前：写集里的两个 key 都在 memtable 中
  auto* memtable = engine.GetMemTable();
  ASSERT_NE(memtable, nullptr);
  EXPECT_GE(memtable->size(), 2u) << "WAL 重放没有写入 memtable";

  engine.Close();
}

TEST(FlushRecoveryTest, WalReplaySkipsUncommittedTransaction) {
  cedar_test::ScopedTempDir temp_dir("wal_replay_uncommitted");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 6000;
  const CedarKey key = CedarKey::Vertex(kEntity, kColumn, Timestamp(kTs));

  const CrashChildResult child = RunInCrashingChild([&dir]() {
    CedarOptions options = MakeOptions();
    // engine 泄漏，避免 body() 返回时析构触发 flush
    auto* engine = new LsmEngine(dir, options, Env::Default());
    if (!engine->Open().ok()) ::_exit(10);
    auto txn = engine->BeginTransaction();
    if (!txn) ::_exit(11);
    if (!txn->Put(kEntity, EntityType::Vertex, kColumn,
                  Descriptor::InlineInt(kColumn, 321), Timestamp(kTs)).ok()) {
      ::_exit(12);
    }
    // 不 Commit 就崩溃：写入不得落表（未提交事务不写 WAL）
  });

  ASSERT_TRUE(child.signaled);
  ASSERT_EQ(child.exit_code, 0) << "子进程在提交前失败，退出码 " << child.exit_code;
  ASSERT_EQ(CountSstFiles(dir), 0u)
      << "子进程在 SIGKILL 之前把 memtable flush 成了 SST（崩溃模拟失效）";

  CedarOptions options = MakeOptions();
  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  // 未提交事务的写集根本不进 WAL：重放应读到 0 条 record
  EXPECT_EQ(engine.GetWalReplayStats().records, 0u)
      << "未提交事务在 WAL 里留下了 record";
  EXPECT_FALSE(engine.Get(key).has_value())
      << "未提交事务的写入被 WAL 重放（违反 WAL-first 的提交边界）";
  engine.Close();
}

TEST(FlushRecoveryTest, WalFileListingIsNumericallyOrdered) {
  cedar_test::ScopedTempDir temp_dir("wal_listing");
  ASSERT_TRUE(temp_dir.ok());
  const std::string wal_dir = temp_dir.path() + "/wal";
  ASSERT_TRUE(std::filesystem::create_directories(wal_dir));

  // 文件名按字典序会把 10.wal 排在 2.wal 前面；重放必须按数字文件号排序
  const std::vector<uint64_t> numbers = {0, 2, 10, 1, 11, 3};
  for (uint64_t number : numbers) {
    const std::string path = wal_dir + "/" + std::to_string(number) + ".wal";
    std::ofstream(path, std::ios::binary).put('x');
  }
  std::ofstream(wal_dir + "/notes.txt", std::ios::binary).put('x');

  std::vector<std::string> files;
  Status s = WalReplay::ListWalFiles(wal_dir, Env::Default(), &files);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(files.size(), numbers.size());

  const std::vector<std::string> expected = {"0.wal", "1.wal", "2.wal",
                                             "3.wal", "10.wal", "11.wal"};
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(std::filesystem::path(files[i]).filename().string(), expected[i]);
  }
}

TEST(FlushRecoveryTest, TruncatedWalTailDoesNotBreakOpen) {
  cedar_test::ScopedTempDir temp_dir("wal_truncated");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 9000;
  const CedarKey key = CedarKey::Vertex(kEntity, kColumn, Timestamp(kTs));

  const CrashChildResult child = RunInCrashingChild([&dir]() {
    CedarOptions options = MakeOptions();
    // engine 泄漏，避免 body() 返回时析构触发 flush
    auto* engine = new LsmEngine(dir, options, Env::Default());
    if (!engine->Open().ok()) ::_exit(10);
    auto txn = engine->BeginTransaction();
    if (!txn) ::_exit(11);
    if (!txn->Put(kEntity, EntityType::Vertex, kColumn,
                  Descriptor::InlineInt(kColumn, 31), Timestamp(kTs)).ok()) {
      ::_exit(12);
    }
    if (!txn->Commit().ok()) ::_exit(13);
  });
  ASSERT_TRUE(child.signaled);
  ASSERT_EQ(child.exit_code, 0);
  ASSERT_EQ(CountSstFiles(dir), 0u)
      << "子进程在 SIGKILL 之前把 memtable flush 成了 SST（崩溃模拟失效）";

  // 找到含记录的那个 WAL 文件并截断尾部 4 字节（模拟写到一半被杀）
  std::string target;
  uint64_t largest = 0;
  for (const auto& entry : std::filesystem::directory_iterator(dir + "/wal")) {
    if (!entry.is_regular_file()) continue;
    if (entry.path().extension() != ".wal") continue;
    const uint64_t size = entry.file_size();
    if (size > largest) {
      largest = size;
      target = entry.path().string();
    }
  }
  ASSERT_FALSE(target.empty()) << "没有找到非空 WAL 文件";
  ASSERT_GT(largest, 4u);
  std::error_code ec;
  std::filesystem::resize_file(target, largest - 4, ec);
  ASSERT_FALSE(ec) << ec.message();

  // 截断的 record 必须被视为崩溃尾巴：Open 成功，且不应用半条记录
  CedarOptions options = MakeOptions();
  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok()) << "WAL 尾部截断导致 Open 失败（应容忍崩溃尾巴）";
  const WalReplayStats replay = engine.GetWalReplayStats();
  EXPECT_GT(replay.corrupted_records, 0u);
  EXPECT_EQ(replay.ops_applied, 0u) << "截断的 record 仍被部分应用";
  EXPECT_FALSE(engine.Get(key).has_value())
      << "CRC 不完整的 WAL record 被应用（应整条丢弃）";
  engine.Close();
}

TEST(FlushRecoveryTest, WalReplayIsIdempotentAcrossReopen) {
  cedar_test::ScopedTempDir temp_dir("wal_replay_idempotent");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 7000;
  const CedarKey key = CedarKey::Vertex(kEntity, kColumn, Timestamp(kTs));

  const CrashChildResult child = RunInCrashingChild([&dir]() {
    CedarOptions options = MakeOptions();
    // engine 泄漏，避免 body() 返回时析构触发 flush
    auto* engine = new LsmEngine(dir, options, Env::Default());
    if (!engine->Open().ok()) ::_exit(10);
    auto txn = engine->BeginTransaction();
    if (!txn) ::_exit(11);
    if (!txn->Put(kEntity, EntityType::Vertex, kColumn,
                  Descriptor::InlineInt(kColumn, 4242), Timestamp(kTs)).ok()) {
      ::_exit(12);
    }
    if (!txn->Commit().ok()) ::_exit(13);
  });
  ASSERT_TRUE(child.signaled);
  ASSERT_EQ(child.exit_code, 0);
  ASSERT_EQ(CountSstFiles(dir), 0u)
      << "子进程在 SIGKILL 之前把 memtable flush 成了 SST（崩溃模拟失效）";

  CedarOptions options = MakeOptions();
  const std::string wal_dir = dir + "/wal";

  // 第一次 Open：WAL 重放 → memtable 恰好 1 行
  {
    LsmEngine engine(dir, options, Env::Default());
    ASSERT_TRUE(engine.Open().ok());
    EXPECT_EQ(engine.GetWalReplayStats().ops_applied, 1u);
    ASSERT_NE(engine.GetMemTable(), nullptr);
    EXPECT_EQ(engine.GetMemTable()->size(), 1u);
    ASSERT_TRUE(engine.Get(key).has_value());

    // 去重机制（同一次重放内的幂等性）：把同一个 WAL 段复制成两个段，
    // 重放时必须按 (key, txn_version, descriptor) 去重，memtable 仍只有 1 行。
    {
      const std::string wal_dir_path = dir + "/wal";
      std::vector<std::string> wal_files;
      ASSERT_TRUE(
          WalReplay::ListWalFiles(wal_dir_path, Env::Default(), &wal_files).ok());
      // Open 会新建一个空活动段；找有数据的那个段作为复制源
      std::string source;
      for (const auto& file : wal_files) {
        uint64_t size = 0;
        if (Env::Default()->GetFileSize(file, &size).ok() && size > 0) {
          source = file;
          break;
        }
      }
      ASSERT_FALSE(source.empty()) << "崩溃后没有留下非空 WAL 段";
      const std::string duplicated = wal_dir_path + "/100000.wal";
      std::ifstream src(source, std::ios::binary);
      std::ofstream dst(duplicated, std::ios::binary);
      ASSERT_TRUE(src.good() && dst.good());
      dst << src.rdbuf();
      dst.close();

      VSLMemTable replayed;
      WalReplayStats stats;
      ASSERT_TRUE(
          WalReplay::Replay(wal_dir_path, Env::Default(), &replayed, &stats).ok());
      EXPECT_EQ(stats.committed_records, 2u);
      EXPECT_EQ(stats.ops_applied, 1u) << "跨段重复 record 没有去重";
      EXPECT_EQ(stats.duplicate_ops_skipped, 1u);
      EXPECT_EQ(replayed.size(), 1u) << "重复重放同一 WAL 记录（重放不幂等）";
      ASSERT_TRUE(Env::Default()->RemoveFile(duplicated).ok());
    }

    ASSERT_TRUE(engine.Close().ok());
  }

  // clean Close（t10）：数据已 flush + fsync，WAL 被截断 → 第二次 Open 不重放，
  // 数据从 SST 读取。这条语义固定在下面的断言里。
  {
    LsmEngine engine(dir, options, Env::Default());
    ASSERT_TRUE(engine.Open().ok());
    const WalReplayStats replay = engine.GetWalReplayStats();
    EXPECT_EQ(replay.ops_applied, 0u)
        << "clean close 之后 WAL 仍被重放（应已按 checkpoint 截断）";
    EXPECT_EQ(replay.files, 0u);
    EXPECT_GT(engine.GetWALCheckpointWatermark(), 0u);
    auto value = engine.Get(key);
    ASSERT_TRUE(value.has_value()) << "截断后数据必须仍可从 SST 读到";
    EXPECT_EQ(value->AsInlineInt().value_or(-1), 4242);
    ASSERT_TRUE(engine.Close().ok());
  }
}

// =============================================================================
// Task 1.2：WAL 失败注入（WAL-first）
// =============================================================================

namespace {

// 对 WAL 目录下的可写文件注入 Append 失败（其它文件透传给 Env::Default()）。
// 说明：仓库里的 EnvWrapper 只有声明没有实现（链接期缺符号），
// 因此这里自带一个完整的转发实现，避免改动 core/env。
class WalFaultEnv : public Env {
 public:
  WalFaultEnv() : target_(Env::Default()) {}

  std::atomic<bool> fail_wal_appends{false};
  // t10 崩溃注入：flush 写 .frond 时 Sync 失败（模拟"flush 成功但 SST 未落盘"）
  std::atomic<bool> fail_sst_sync{false};
  // t10 崩溃注入：flush 写 .frond 时 Append 失败（模拟半截 SST）
  std::atomic<bool> fail_sst_appends{false};
  // t10 崩溃注入：checkpoint 文件写入失败（flush 成功但水位没法推进）
  std::atomic<bool> fail_checkpoint_write{false};

  Status NewWritableFile(const std::string& fname, WritableFile** result) override {
    if (fail_checkpoint_write.load() &&
        fname.find("MANIFEST-wal-checkpoint") != std::string::npos) {
      return Status::IOError("WalFaultEnv", "injected checkpoint write failure");
    }
    Status s = target_->NewWritableFile(fname, result);
    if (!s.ok()) return s;
    const bool is_sst =
        fname.size() > 6 && fname.compare(fname.size() - 6, 6, ".frond") == 0;
    if (is_sst && (fail_sst_sync.load() || fail_sst_appends.load())) {
      *result = new SstFaultWritableFile(*result, this);
    } else if (fname.find("/wal/") != std::string::npos) {
      *result = new FailingWritableFile(*result, this);
    }
    return Status::OK();
  }

  Status NewSequentialFile(const std::string& f, SequentialFile** r) override {
    return target_->NewSequentialFile(f, r);
  }
  Status NewRandomAccessFile(const std::string& f, RandomAccessFile** r) override {
    return target_->NewRandomAccessFile(f, r);
  }
  Status NewAppendableFile(const std::string& f, WritableFile** r) override {
    return target_->NewAppendableFile(f, r);
  }
  bool FileExists(const std::string& f) override { return target_->FileExists(f); }
  Status GetChildren(const std::string& d, std::vector<std::string>* r) override {
    return target_->GetChildren(d, r);
  }
  Status RemoveFile(const std::string& f) override { return target_->RemoveFile(f); }
  Status CreateDir(const std::string& d) override { return target_->CreateDir(d); }
  Status RemoveDir(const std::string& d) override { return target_->RemoveDir(d); }
  Status GetFileSize(const std::string& f, uint64_t* s) override {
    return target_->GetFileSize(f, s);
  }
  Status RenameFile(const std::string& s, const std::string& t) override {
    return target_->RenameFile(s, t);
  }
  Status LockFile(const std::string& f, FileLock** l) override {
    return target_->LockFile(f, l);
  }
  Status UnlockFile(FileLock* l) override { return target_->UnlockFile(l); }
  void Schedule(void (*fn)(void*), void* a) override { target_->Schedule(fn, a); }
  void StartThread(void (*fn)(void*), void* a) override { target_->StartThread(fn, a); }
  Status GetTestDirectory(std::string* p) override {
    return target_->GetTestDirectory(p);
  }
  Status NewLogger(const std::string& f, Logger** r) override {
    return target_->NewLogger(f, r);
  }
  uint64_t NowMicros() override { return target_->NowMicros(); }
  void SleepForMicroseconds(int micros) override {
    target_->SleepForMicroseconds(micros);
  }

 private:
  class FailingWritableFile : public WritableFile {
   public:
    FailingWritableFile(WritableFile* inner, WalFaultEnv* env)
        : inner_(inner), env_(env) {}
    ~FailingWritableFile() override { delete inner_; }

    Status Append(const Slice& data) override {
      if (env_->fail_wal_appends.load()) {
        return Status::IOError("WalFaultEnv", "injected WAL append failure");
      }
      return inner_->Append(data);
    }
    Status Close() override { return inner_->Close(); }
    Status Flush() override { return inner_->Flush(); }
    Status Sync() override { return inner_->Sync(); }

   private:
    WritableFile* inner_;
    WalFaultEnv* env_;
  };

  class SstFaultWritableFile : public WritableFile {
   public:
    SstFaultWritableFile(WritableFile* inner, WalFaultEnv* env)
        : inner_(inner), env_(env) {}
    ~SstFaultWritableFile() override { delete inner_; }

    Status Append(const Slice& data) override {
      if (env_->fail_sst_appends.load()) {
        return Status::IOError("WalFaultEnv", "injected SST append failure");
      }
      return inner_->Append(data);
    }
    Status Close() override { return inner_->Close(); }
    Status Flush() override { return inner_->Flush(); }
    Status Sync() override {
      if (env_->fail_sst_sync.load()) {
        return Status::IOError("WalFaultEnv", "injected SST sync failure");
      }
      return inner_->Sync();
    }

   private:
    WritableFile* inner_;
    WalFaultEnv* env_;
  };

  Env* target_;
};

}  // namespace

TEST(FlushRecoveryTest, WalFailureKeepsDataInvisible) {
  cedar_test::ScopedTempDir temp_dir("wal_failure_injection");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 8000;
  const CedarKey key = CedarKey::Vertex(kEntity, kColumn, Timestamp(kTs));

  WalFaultEnv fault_env;

  CedarOptions options = MakeOptions();
  options.env = &fault_env;

  LsmEngine engine(dir, options, &fault_env);
  ASSERT_TRUE(engine.Open().ok());

  auto txn = engine.BeginTransaction();
  ASSERT_NE(txn, nullptr);
  ASSERT_TRUE(txn->Put(kEntity, EntityType::Vertex, kColumn,
                       Descriptor::InlineInt(kColumn, 999), Timestamp(kTs)).ok());

  fault_env.fail_wal_appends.store(true);
  Status commit_status = txn->Commit();
  EXPECT_FALSE(commit_status.ok())
      << "WAL 写入失败后 Commit 仍返回成功（违反 WAL-first）";

  EXPECT_FALSE(engine.Get(key).has_value())
      << "WAL 写入失败时数据仍然可见（先写 memtable 后写 WAL）";

  fault_env.fail_wal_appends.store(false);

  // 恢复后重试提交必须成功，且数据可见
  auto retry_txn = engine.BeginTransaction();
  ASSERT_NE(retry_txn, nullptr);
  ASSERT_TRUE(retry_txn->Put(kEntity, EntityType::Vertex, kColumn,
                            Descriptor::InlineInt(kColumn, 999), Timestamp(kTs)).ok());
  ASSERT_TRUE(retry_txn->Commit().ok());
  auto value = engine.Get(key);
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(value->AsInlineInt().value_or(-1), 999);

  ASSERT_TRUE(engine.Close().ok());
}

// =============================================================================
// Task 1.3：按 ApproximateMemoryUsage 自动切换 + 并发 flush（审计 3.4/3.5）
// =============================================================================

TEST(FlushRecoveryTest, AutoSwitchTriggeredByMemoryUsage) {
  cedar_test::ScopedTempDir temp_dir("auto_flush");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  CedarOptions options = MakeOptions();
  // 注意：VSLMemTable::ApproximateMemoryUsage() 按 1MB slab 容量统计（Task 2.2），
  // 阈值必须大于一个 slab，否则每次 Put 都会触发换表（每文件 1 行）。
  options.write_buffer_size = 2 * 1024 * 1024;  // 2MB → 约 2 个 slab 后自动切换

  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  constexpr uint64_t kRows = 20000;
  for (uint64_t i = 0; i < kRows; ++i) {
    ASSERT_TRUE(engine.Put(CedarKey::Vertex(i, kColumn, Timestamp(1000)),
                           Descriptor::InlineInt(kColumn, static_cast<int32_t>(i)),
                           Timestamp(1))
                    .ok());
  }

  EXPECT_GT(CountSstRows(engine), 0u)
      << "Put 达到 write_buffer_size 后没有自动切换/落盘（审计 3.4）";

  // 自动切换后所有数据仍然可读（flush 的旧表不能丢数据）
  for (uint64_t i = 0; i < kRows; i += 97) {
    auto value = engine.Get(CedarKey::Vertex(i, kColumn, Timestamp(1000)));
    ASSERT_TRUE(value.has_value()) << "自动 flush 后丢失 key=" << i;
    EXPECT_EQ(value->AsInlineInt().value_or(-1), static_cast<int32_t>(i));
  }

  ASSERT_TRUE(engine.Close().ok());
}

TEST(FlushRecoveryTest, ConcurrentPutAndFlushKeepsAllRows) {
  cedar_test::ScopedTempDir temp_dir("concurrent_flush");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr int kThreads = 4;
  constexpr uint64_t kPerThread = 3000;
  constexpr uint64_t kExpected = kThreads * kPerThread;

  CedarOptions options = MakeOptions();
  // 同上：阈值需高于 1MB slab 粒度，避免每 Put 换表
  options.write_buffer_size = 2 * 1024 * 1024;

  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  std::atomic<bool> stop_flusher{false};
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&engine, t]() {
      for (uint64_t i = 0; i < kPerThread; ++i) {
        const uint64_t entity = i * kThreads + static_cast<uint64_t>(t);
        engine.Put(CedarKey::Vertex(entity, kColumn, Timestamp(1000)),
                   Descriptor::InlineInt(kColumn, static_cast<int32_t>(i)),
                   Timestamp(1));
      }
    });
  }
  threads.emplace_back([&engine, &stop_flusher]() {
    while (!stop_flusher.load()) {
      engine.ForceFlush();  // 与写/自动 flush 竞争，成功或 "flush already in progress" 均可
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  });
  // 读线程：验证读路径在 flush 换表/销毁期间持有 memtable 引用（审计 3.5，TSan 观察点）。
  // 两个读者分工，避免两个线程同时走 SstReaderCache：
  //   A：Get/GetAll/GetStats —— 每次新建 SST reader，不经过缓存；
  //   B：GetAtTime/GetRange/GetRangeLimit —— 复用缓存 reader。
  // （两个线程共享缓存 reader 会命中审计 10.8 的 frond 解码器惰性建索引竞争，
  //   属于 Phase 3 范围，不应混入本任务的 TSan 证据。）
  std::atomic<bool> stop_readers{false};
  threads.emplace_back([&engine, &stop_readers]() {
    while (!stop_readers.load()) {
      engine.Get(CedarKey::Vertex(0, kColumn, Timestamp(1000)));
      engine.Get(CedarKey::Vertex(1, kColumn, Timestamp(1000)));
      engine.GetAll(0, EntityType::Vertex, kColumn);
      engine.GetStats();
    }
  });
  threads.emplace_back([&engine, &stop_readers]() {
    while (!stop_readers.load()) {
      engine.GetAtTime(0, EntityType::Vertex, kColumn, Timestamp(1000));
      engine.GetRange(0, EntityType::Vertex, kColumn, Timestamp(0), Timestamp(2000));
      engine.GetRangeLimit(0, EntityType::Vertex, kColumn, Timestamp(0),
                           Timestamp(2000), 16);
    }
  });

  for (int t = 0; t < kThreads; ++t) threads[static_cast<size_t>(t)].join();
  stop_flusher.store(true);
  stop_readers.store(true);
  for (size_t i = static_cast<size_t>(kThreads); i < threads.size(); ++i) {
    threads[i].join();
  }

  ASSERT_TRUE(engine.ForceFlush().ok()) << "收尾 flush 失败";
  EXPECT_EQ(CountSstRows(engine), kExpected)
      << "并发 Put/Flush 丢数据（审计 3.4/3.5）";

  // 全部可读（memtable + imm + SST 任一路径）
  for (uint64_t i = 0; i < kPerThread; i += 37) {
    const uint64_t entity = i * kThreads;
    auto value = engine.Get(CedarKey::Vertex(entity, kColumn, Timestamp(1000)));
    ASSERT_TRUE(value.has_value()) << "并发 flush 后丢失 entity=" << entity;
  }

  ASSERT_TRUE(engine.Close().ok());
}


// =============================================================================
// t9：直接写路径 WAL 化 + 组提交
// =============================================================================

namespace {

// 子进程正常 _exit(0)（跳过析构，因此不会 Close/ForceFlush）——用于验证
// "提交返回即持久化"：调用方一返回就被杀死，恢复只能靠 WAL。
void ExpectCleanExit(const CrashChildResult& child) {
  ASSERT_FALSE(child.signaled) << "子进程被信号杀死（signal=" << child.term_signal
                               << "），应走 _exit(0)";
  ASSERT_EQ(child.exit_code, 0);
}

// 直接写（无事务）用的 key
CedarKey DirectKey(uint64_t entity, uint64_t ts) {
  return CedarKey::Vertex(entity, kColumn, Timestamp(ts));
}

}  // namespace

TEST(FlushRecoveryTest, DirectPutSurvivesCrashWithoutFlush) {
  cedar_test::ScopedTempDir temp_dir("direct_put_wal");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 11000;
  const CedarKey key = DirectKey(kEntity, kTs);

  const CrashChildResult child = RunInCrashingChild([&dir, &key]() {
    CedarOptions options = MakeOptions();
    // engine 泄漏：避免 body() 返回时析构触发 Close/ForceFlush
    auto* engine = new LsmEngine(dir, options, Env::Default());
    if (!engine->Open().ok()) ::_exit(10);
    if (!engine->Put(key, Descriptor::InlineInt(kColumn, 4321), Timestamp(1)).ok()) {
      ::_exit(11);
    }
    // 直接写也必须是 WAL-first：不 flush、不 Close，随后 SIGKILL
  });

  ASSERT_TRUE(child.signaled);
  EXPECT_EQ(child.term_signal, SIGKILL);
  ASSERT_EQ(child.exit_code, 0) << "子进程在写之前失败，退出码 " << child.exit_code;
  ASSERT_EQ(CountSstFiles(dir), 0u) << "子进程崩溃前把 memtable flush 成了 SST";

  CedarOptions options = MakeOptions();
  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  const WalReplayStats replay = engine.GetWalReplayStats();
  EXPECT_EQ(replay.direct_records, 1u)
      << "直接写的单条 record 没有被当作已提交记录重放";
  EXPECT_EQ(replay.uncommitted_records, 0u);
  auto value = engine.Get(key);
  ASSERT_TRUE(value.has_value())
      << "直接 Put 未写 WAL（审计 3.3 后半：崩溃后不可恢复）";
  EXPECT_EQ(value->AsInlineInt().value_or(-1), 4321);
  engine.Close();
}

TEST(FlushRecoveryTest, DirectDeleteSurvivesCrashInWalOrder) {
  cedar_test::ScopedTempDir temp_dir("direct_delete_wal");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 12000;
  const CedarKey key = DirectKey(kEntity, kTs);

  const CrashChildResult child = RunInCrashingChild([&dir, &key]() {
    CedarOptions options = MakeOptions();
    auto* engine = new LsmEngine(dir, options, Env::Default());
    if (!engine->Open().ok()) ::_exit(10);
    if (!engine->Put(key, Descriptor::InlineInt(kColumn, 7), Timestamp(1)).ok()) {
      ::_exit(11);
    }
    if (!engine->Delete(key, Timestamp(2)).ok()) ::_exit(12);
  });

  ASSERT_TRUE(child.signaled);
  ASSERT_EQ(child.exit_code, 0);
  ASSERT_EQ(CountSstFiles(dir), 0u);

  CedarOptions options = MakeOptions();
  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  EXPECT_EQ(engine.GetWalReplayStats().direct_records, 2u);
  EXPECT_FALSE(engine.Get(key).has_value())
      << "直接 Delete 的墓碑没有按 WAL 顺序覆盖先前的 Put（删除复活）";
  engine.Close();
}

TEST(FlushRecoveryTest, MixedDirectAndTransactionalReplayOrder) {
  cedar_test::ScopedTempDir temp_dir("mixed_wal_order");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 13000;
  const CedarKey direct_key = DirectKey(kEntity, kTs);
  const CedarKey txn_key = DirectKey(88, kTs);
  const CedarKey same_key = DirectKey(99, kTs);

  const CrashChildResult child = RunInCrashingChild(
      [&dir, &direct_key, &txn_key, &same_key]() {
    CedarOptions options = MakeOptions();
    auto* engine = new LsmEngine(dir, options, Env::Default());
    if (!engine->Open().ok()) ::_exit(10);

    // 1) 直接写
    if (!engine->Put(direct_key, Descriptor::InlineInt(kColumn, 100), Timestamp(1)).ok()) {
      ::_exit(11);
    }
    // 2) 事务写
    auto txn = engine->BeginTransaction();
    if (!txn) ::_exit(12);
    if (!txn->Put(88, EntityType::Vertex, kColumn, Descriptor::InlineInt(kColumn, 200),
                  Timestamp(kTs)).ok()) {
      ::_exit(13);
    }
    if (!txn->Commit().ok()) ::_exit(14);
    // 3) 直接写先落，事务再覆盖同一 key（验证 WAL 顺序 = 可见顺序）
    if (!engine->Put(same_key, Descriptor::InlineInt(kColumn, 300), Timestamp(2)).ok()) {
      ::_exit(15);
    }
    auto txn2 = engine->BeginTransaction();
    if (!txn2) ::_exit(16);
    if (!txn2->Put(99, EntityType::Vertex, kColumn, Descriptor::InlineInt(kColumn, 400),
                   Timestamp(kTs)).ok()) {
      ::_exit(17);
    }
    if (!txn2->Commit().ok()) ::_exit(18);
  });

  ASSERT_TRUE(child.signaled);
  ASSERT_EQ(child.exit_code, 0) << "子进程失败，退出码 " << child.exit_code;
  ASSERT_EQ(CountSstFiles(dir), 0u);

  CedarOptions options = MakeOptions();
  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  const WalReplayStats replay = engine.GetWalReplayStats();
  EXPECT_EQ(replay.direct_records, 2u);
  EXPECT_EQ(replay.committed_records, 2u);
  EXPECT_EQ(replay.uncommitted_records, 0u);

  auto direct_value = engine.Get(direct_key);
  ASSERT_TRUE(direct_value.has_value());
  EXPECT_EQ(direct_value->AsInlineInt().value_or(-1), 100);
  auto txn_value = engine.Get(txn_key);
  ASSERT_TRUE(txn_value.has_value());
  EXPECT_EQ(txn_value->AsInlineInt().value_or(-1), 200);
  // 同一 key：事务 400 在直接写 300 之后，重放后必须读到 400
  auto same_value = engine.Get(same_key);
  ASSERT_TRUE(same_value.has_value());
  EXPECT_EQ(same_value->AsInlineInt().value_or(-1), 400)
      << "直接写与事务写的重放顺序错误（直接写覆盖了后提交的事务）";
  engine.Close();
}

TEST(FlushRecoveryTest, LegacyWalWithoutFencePolicy) {
  cedar_test::ScopedTempDir temp_dir("legacy_wal");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 14000;
  const CedarKey single_key = DirectKey(1, kTs);
  const CedarKey multi_a = DirectKey(2, kTs);
  const CedarKey multi_b = DirectKey(3, kTs);

  // 用 WalWriter 手工构造"旧版本"WAL：没有提交围栏
  {
    WalOptions wal_options;
    wal_options.group_commit_timeout_us = 0;  // 同步写，不依赖组提交线程
    WalWriter writer(dir + "/wal", Env::Default(), wal_options);
    ASSERT_TRUE(writer.Open().ok());

    WalBatch single;
    single.Put(single_key, Descriptor::InlineInt(kColumn, 11), Timestamp(1));
    ASSERT_TRUE(writer.WriteBatch(single).ok());

    WalBatch multi;
    multi.Put(multi_a, Descriptor::InlineInt(kColumn, 22), Timestamp(2));
    multi.Put(multi_b, Descriptor::InlineInt(kColumn, 33), Timestamp(2));
    ASSERT_TRUE(writer.WriteBatch(multi).ok());
    ASSERT_TRUE(writer.Close().ok());
  }

  CedarOptions options = MakeOptions();
  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  const WalReplayStats replay = engine.GetWalReplayStats();
  EXPECT_EQ(replay.direct_records, 1u) << "单条无围栏 record 应视为已提交的直接写";
  EXPECT_EQ(replay.uncommitted_records, 1u) << "多条无围栏 record 应整条跳过";

  auto single_value = engine.Get(single_key);
  ASSERT_TRUE(single_value.has_value())
      << "升级后旧 WAL 里的单条 kPut 被丢弃（应视为已提交，避免升级即丢数据）";
  EXPECT_EQ(single_value->AsInlineInt().value_or(-1), 11);
  EXPECT_FALSE(engine.Get(multi_a).has_value());
  EXPECT_FALSE(engine.Get(multi_b).has_value());
  engine.Close();
}

TEST(FlushRecoveryTest, GroupCommitDurableAtEveryCommitPoint) {
  // 每个提交点：Commit 返回后立刻 _exit(0)（不 Close、不 flush），
  // 重开必须看到恰好这么多条已提交事务（组提交"等到自己的批次持久化"）。
  const std::vector<int> commit_points = {1, 7, 20};
  for (int commits : commit_points) {
    cedar_test::ScopedTempDir temp_dir("group_commit_durable");
    ASSERT_TRUE(temp_dir.ok());
    const std::string& dir = temp_dir.path();
    constexpr uint64_t kTs = 15000;
    constexpr uint64_t kBase = 1000;

    const CrashChildResult child = RunInCrashingChild([&dir, commits, kBase]() {
      CedarOptions options = MakeOptions();
      LsmEngine engine(dir, options, Env::Default());
      if (!engine.Open().ok()) ::_exit(10);
      for (int i = 0; i < commits; ++i) {
        auto txn = engine.BeginTransaction();
        if (!txn) ::_exit(11);
        const uint64_t entity = kBase + static_cast<uint64_t>(i);
        if (!txn->Put(entity, EntityType::Vertex, kColumn,
                      Descriptor::InlineInt(kColumn, static_cast<int32_t>(i)),
                      Timestamp(kTs)).ok()) {
          ::_exit(12);
        }
        if (!txn->Commit().ok()) ::_exit(13);
      }
      ::_exit(0);  // 跳过析构：只有已 sync 的 WAL 能恢复数据
    });

    ExpectCleanExit(child);
    ASSERT_EQ(CountSstFiles(dir), 0u) << "commits=" << commits;

    CedarOptions options = MakeOptions();
    LsmEngine engine(dir, options, Env::Default());
    ASSERT_TRUE(engine.Open().ok()) << "commits=" << commits;
    for (int i = 0; i < commits; ++i) {
      const CedarKey key = DirectKey(kBase + static_cast<uint64_t>(i), kTs);
      auto value = engine.Get(key);
      ASSERT_TRUE(value.has_value())
          << "第 " << i << " 个提交点在 Commit 返回后不持久（commits=" << commits << "）";
      EXPECT_EQ(value->AsInlineInt().value_or(-1), i);
    }
    engine.Close();
  }
}

TEST(FlushRecoveryTest, GroupCommitConcurrentCommitsAllDurable) {
  cedar_test::ScopedTempDir temp_dir("group_commit_concurrent");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr int kThreads = 8;
  constexpr int kPerThread = 25;
  constexpr uint64_t kTs = 16000;
  constexpr uint64_t kBase = 100000;

  const CrashChildResult child = RunInCrashingChild([&dir, kBase]() {
    CedarOptions options = MakeOptions();
    LsmEngine engine(dir, options, Env::Default());
    if (!engine.Open().ok()) ::_exit(10);

    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
      threads.emplace_back([&engine, &failures, t, kBase]() {
        for (int i = 0; i < kPerThread; ++i) {
          auto txn = engine.BeginTransaction();
          if (!txn) { failures.fetch_add(1); continue; }
          const uint64_t entity = kBase + static_cast<uint64_t>(t) * kPerThread +
                                  static_cast<uint64_t>(i);
          if (!txn->Put(entity, EntityType::Vertex, kColumn,
                        Descriptor::InlineInt(kColumn, t * 100 + i), Timestamp(kTs)).ok()) {
            failures.fetch_add(1);
            continue;
          }
          if (!txn->Commit().ok()) failures.fetch_add(1);
        }
      });
    }
    for (auto& worker : threads) worker.join();
    if (failures.load() != 0) ::_exit(20);
    ::_exit(0);  // 跳过析构
  });

  ExpectCleanExit(child);
  ASSERT_EQ(CountSstFiles(dir), 0u);

  CedarOptions options = MakeOptions();
  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kPerThread; ++i) {
      const uint64_t entity = kBase + static_cast<uint64_t>(t) * kPerThread +
                              static_cast<uint64_t>(i);
      auto value = engine.Get(DirectKey(entity, kTs));
      ASSERT_TRUE(value.has_value()) << "组提交并发提交点丢失 t=" << t << " i=" << i;
      EXPECT_EQ(value->AsInlineInt().value_or(-1), t * 100 + i);
    }
  }
  engine.Close();
}

TEST(FlushRecoveryTest, DirectPutWalFailureKeepsDataInvisible) {
  cedar_test::ScopedTempDir temp_dir("direct_wal_failure");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 17000;
  const CedarKey key = DirectKey(kEntity, kTs);

  WalFaultEnv fault_env;
  CedarOptions options = MakeOptions();
  options.env = &fault_env;

  LsmEngine engine(dir, options, &fault_env);
  ASSERT_TRUE(engine.Open().ok());

  fault_env.fail_wal_appends.store(true);
  Status put_status = engine.Put(key, Descriptor::InlineInt(kColumn, 555), Timestamp(1));
  EXPECT_FALSE(put_status.ok()) << "直接写 WAL 失败后 Put 仍返回成功（违反 WAL-first）";
  EXPECT_FALSE(engine.Get(key).has_value())
      << "直接写 WAL 失败时数据仍然可见（先写 memtable 后写 WAL）";

  fault_env.fail_wal_appends.store(false);
  ASSERT_TRUE(engine.Put(key, Descriptor::InlineInt(kColumn, 555), Timestamp(1)).ok());
  auto value = engine.Get(key);
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(value->AsInlineInt().value_or(-1), 555);

  ASSERT_TRUE(engine.Close().ok());
}

TEST(FlushRecoveryTest, GroupCommitBatchesConcurrentCommits) {
  // 进程内并发提交：验证组提交把 N 个调用合并成少数几次 fsync，
  // 且每个调用的数据都可见（崩溃持久化在 GroupCommit*Durable* 用例里验证）。
  cedar_test::ScopedTempDir temp_dir("group_commit_batches");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr int kThreads = 8;
  constexpr int kPerThread = 25;
  constexpr uint64_t kTs = 18000;
  constexpr uint64_t kBase = 200000;

  CedarOptions options = MakeOptions();
  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&engine, &failures, t]() {
      for (int i = 0; i < kPerThread; ++i) {
        auto txn = engine.BeginTransaction();
        if (!txn) { failures.fetch_add(1); continue; }
        const uint64_t entity = kBase + static_cast<uint64_t>(t) * kPerThread +
                                static_cast<uint64_t>(i);
        if (!txn->Put(entity, EntityType::Vertex, kColumn,
                      Descriptor::InlineInt(kColumn, t * 100 + i), Timestamp(kTs))
                 .ok()) {
          failures.fetch_add(1);
          continue;
        }
        if (!txn->Commit().ok()) failures.fetch_add(1);
      }
    });
  }
  for (auto& worker : threads) worker.join();
  EXPECT_EQ(failures.load(), 0);

  const WalStats wal = engine.GetWalWriter()->GetStats();
  const uint64_t records = wal.records_written.load();
  const uint64_t syncs = wal.syncs.load();
  EXPECT_EQ(records, static_cast<uint64_t>(kThreads) * kPerThread);
  EXPECT_LE(syncs, records) << "syncs 不应超过 record 数";
  EXPECT_LT(syncs, records)
      << "组提交没有合并任何提交（records=" << records << " syncs=" << syncs << "）";

  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kPerThread; ++i) {
      const uint64_t entity = kBase + static_cast<uint64_t>(t) * kPerThread +
                              static_cast<uint64_t>(i);
      auto value = engine.Get(CedarKey::Vertex(entity, kColumn, Timestamp(kTs)));
      ASSERT_TRUE(value.has_value()) << "并发提交丢失 t=" << t << " i=" << i;
      EXPECT_EQ(value->AsInlineInt().value_or(-1), t * 100 + i);
    }
  }
  ASSERT_TRUE(engine.Close().ok());
}

// =============================================================================
// t10：WAL checkpoint / 截断
// =============================================================================

namespace {

// wal 目录里 .wal 段数（含空活动段）
size_t CountWalFiles(const std::string& dir) {
  const std::string wal_dir = dir + "/wal";
  std::vector<std::string> children;
  if (!Env::Default()->GetChildren(wal_dir, &children).ok()) return 0;
  size_t count = 0;
  for (const auto& child : children) {
    if (child.size() > 4 && child.compare(child.size() - 4, 4, ".wal") == 0) {
      ++count;
    }
  }
  return count;
}

// wal 目录字节数（WAL 空间下界）
uint64_t WalDirBytes(const std::string& dir) {
  const std::string wal_dir = dir + "/wal";
  std::vector<std::string> children;
  if (!Env::Default()->GetChildren(wal_dir, &children).ok()) return 0;
  uint64_t total = 0;
  for (const auto& child : children) {
    uint64_t size = 0;
    if (Env::Default()->GetFileSize(wal_dir + "/" + child, &size).ok()) {
      total += size;
    }
  }
  return total;
}

bool DirectoryHasFile(const std::string& dir, const std::string& name) {
  return Env::Default()->FileExists(dir + "/" + name);
}

std::vector<std::string> ListFilesWithSuffix(const std::string& dir,
                                            const std::string& suffix) {
  std::vector<std::string> result;
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
    if (!entry.is_regular_file()) continue;
    const std::string path = entry.path().string();
    if (path.size() >= suffix.size() &&
        path.compare(path.size() - suffix.size(), suffix.size(), suffix) == 0) {
      result.push_back(path);
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

uint64_t WalFileNumber(const std::string& path) {
  const std::string name = std::filesystem::path(path).filename().string();
  if (name.size() <= 4) return 0;
  return std::strtoull(name.substr(0, name.size() - 4).c_str(), nullptr, 10);
}

}  // namespace

// flush 成功（SST fsync）后才允许推进水位、删段；重放只读尾部。
TEST(FlushRecoveryTest, WalTruncatedAfterFlushLeavesOnlyTailReplayed) {
  cedar_test::ScopedTempDir temp_dir("wal_checkpoint_tail");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 20000;
  const CedarKey key_a1 = CedarKey::Vertex(3001, kColumn, Timestamp(kTs));
  const CedarKey key_a2 = CedarKey::Vertex(3002, kColumn, Timestamp(kTs));
  const CedarKey key_b = CedarKey::Vertex(3003, kColumn, Timestamp(kTs));
  const CedarKey key_c = CedarKey::Vertex(3004, kColumn, Timestamp(kTs));

  const CrashChildResult child = RunInCrashingChild(
      [&dir, &key_a1, &key_a2, &key_b, &key_c]() {
        auto* engine = new LsmEngine(dir, MakeOptions(), Env::Default());
        if (!engine->Open().ok()) ::_exit(10);
        // A：第一条事务（写 0.wal）
        auto txn_a = engine->BeginTransaction();
        if (!txn_a) ::_exit(11);
        if (!txn_a->Put(key_a1.entity_id(), EntityType::Vertex, kColumn,
                        Descriptor::InlineInt(kColumn, 11), Timestamp(kTs))
                 .ok() ||
            !txn_a->Put(key_a2.entity_id(), EntityType::Vertex, kColumn,
                        Descriptor::InlineInt(kColumn, 12), Timestamp(kTs))
                 .ok() ||
            !txn_a->Commit().ok()) {
          ::_exit(12);
        }
        // flush 1：封印 0.wal（2 轮滞后，暂不删）
        if (!engine->ForceFlush().ok()) ::_exit(13);
        // B：第二条事务（写 1.wal）
        auto txn_b = engine->BeginTransaction();
        if (!txn_b) ::_exit(14);
        if (!txn_b->Put(key_b.entity_id(), EntityType::Vertex, kColumn,
                        Descriptor::InlineInt(kColumn, 21), Timestamp(kTs))
                 .ok() ||
            !txn_b->Commit().ok()) {
          ::_exit(15);
        }
        // flush 2：封印 1.wal → 水位覆盖 0.wal，删除它
        if (!engine->ForceFlush().ok()) ::_exit(16);
        // C：第三条事务（写 2.wal，活动段）
        auto txn_c = engine->BeginTransaction();
        if (!txn_c) ::_exit(17);
        if (!txn_c->Put(key_c.entity_id(), EntityType::Vertex, kColumn,
                        Descriptor::InlineInt(kColumn, 31), Timestamp(kTs))
                 .ok() ||
            !txn_c->Commit().ok()) {
          ::_exit(18);
        }
        // 不 Close：模拟崩溃（引擎泄漏，避免析构 flush）；
        // body 返回后 RunInCrashingChild 会 SIGKILL 子进程。
      });
  ASSERT_TRUE(child.signaled);
  ASSERT_EQ(child.exit_code, 0);

  // 崩溃后磁盘状态：0.wal 已被 checkpoint 删除，1.wal / 2.wal 还在
  EXPECT_EQ(CountWalFiles(dir), 2u) << "水位只应覆盖第一轮 flush 封印的段";
  EXPECT_EQ(CountSstFiles(dir), 2u) << "两次 flush 应各留下一个 SST";

  LsmEngine engine(dir, MakeOptions(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  const WalReplayStats replay = engine.GetWalReplayStats();
  EXPECT_EQ(replay.checkpoint_watermark, 1u) << "水位应等于第一轮封印段号 + 1";
  EXPECT_EQ(replay.records, 2u) << "只应重放 B、C 两条事务 record（A 已落 SST）";
  EXPECT_EQ(replay.ops_applied, 2u);
  EXPECT_EQ(replay.committed_records, 2u);

  // 三条事务的数据都必须在：A 来自 SST，B/C 来自尾部重放
  ASSERT_TRUE(engine.Get(key_a1).has_value());
  EXPECT_EQ(engine.Get(key_a1)->AsInlineInt().value_or(-1), 11);
  ASSERT_TRUE(engine.Get(key_a2).has_value());
  EXPECT_EQ(engine.Get(key_a2)->AsInlineInt().value_or(-1), 12);
  ASSERT_TRUE(engine.Get(key_b).has_value());
  EXPECT_EQ(engine.Get(key_b)->AsInlineInt().value_or(-1), 21);
  ASSERT_TRUE(engine.Get(key_c).has_value());
  EXPECT_EQ(engine.Get(key_c)->AsInlineInt().value_or(-1), 31);

  ASSERT_TRUE(engine.Close().ok());
  // clean Close：全部落 SST 后截断到只剩活动段
  EXPECT_EQ(CountWalFiles(dir), 1u);
  EXPECT_GT(engine.GetWALCheckpointWatermark(), replay.checkpoint_watermark);
}

// 长跑：100 轮"写 + flush"后 WAL 目录与重放量都必须有界。
TEST(FlushRecoveryTest, WalDirectoryBoundedOverManyFlushCycles) {
  cedar_test::ScopedTempDir temp_dir("wal_checkpoint_long_run");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr int kRounds = 100;
  constexpr int kRowsPerRound = 3;
  constexpr uint64_t kTs = 21000;
  constexpr uint64_t kEntityBase = 4000;

  CedarOptions options = MakeOptions();
  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  size_t max_wal_files = 0;
  uint64_t max_wal_bytes = 0;
  for (int round = 0; round < kRounds; ++round) {
    for (int row = 0; row < kRowsPerRound; ++row) {
      const uint64_t entity = kEntityBase + static_cast<uint64_t>(round) * 10 +
                              static_cast<uint64_t>(row);
      ASSERT_TRUE(engine
                      .Put(CedarKey::Vertex(entity, kColumn, Timestamp(kTs)),
                           Descriptor::InlineInt(kColumn, round * 10 + row),
                           Timestamp(static_cast<uint64_t>(round) + 1))
                      .ok());
    }
    ASSERT_TRUE(engine.ForceFlush().ok()) << "第 " << round << " 轮 flush 失败";
    max_wal_files = std::max(max_wal_files, CountWalFiles(dir));
    max_wal_bytes = std::max(max_wal_bytes, WalDirBytes(dir));
  }

  // 界：2 轮滞后 + 活动段 → 最多 3 个段；每轮 3 条小 record，远小于 64KB
  EXPECT_LE(max_wal_files, 3u) << "WAL 段数没有随 flush 回收（max="
                               << max_wal_files << "）";
  EXPECT_LE(max_wal_bytes, 64u * 1024u)
      << "WAL 目录字节数没有界（max=" << max_wal_bytes << "）";

  ASSERT_TRUE(engine.Close().ok());
  EXPECT_EQ(CountWalFiles(dir), 1u) << "clean close 后应只剩活动段";

  // 重开：数据完整（全部在 SST），重放量为 0
  LsmEngine reopened(dir, options, Env::Default());
  ASSERT_TRUE(reopened.Open().ok());
  EXPECT_EQ(reopened.GetWalReplayStats().ops_applied, 0u);
  for (int round = 0; round < kRounds; ++round) {
    for (int row = 0; row < kRowsPerRound; ++row) {
      const uint64_t entity = kEntityBase + static_cast<uint64_t>(round) * 10 +
                              static_cast<uint64_t>(row);
      auto value =
          reopened.Get(CedarKey::Vertex(entity, kColumn, Timestamp(kTs)));
      ASSERT_TRUE(value.has_value()) << "round=" << round << " row=" << row;
      EXPECT_EQ(value->AsInlineInt().value_or(-1), round * 10 + row);
    }
  }
  ASSERT_TRUE(reopened.Close().ok());
}

// 崩溃注入：flush 的 SST fsync 失败 → 不得删 WAL，重开后数据从 WAL 恢复，
// 且未落盘的半截 SST 不会被引用。
TEST(FlushRecoveryTest, SstSyncFailureKeepsWalAndData) {
  cedar_test::ScopedTempDir temp_dir("wal_sst_sync_failure");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 22000;
  const CedarKey key = CedarKey::Vertex(5001, kColumn, Timestamp(kTs));

  WalFaultEnv fault_env;
  CedarOptions options = MakeOptions();
  options.env = &fault_env;

  LsmEngine engine(dir, options, &fault_env);
  ASSERT_TRUE(engine.Open().ok());
  ASSERT_TRUE(
      engine.Put(key, Descriptor::InlineInt(kColumn, 77), Timestamp(1)).ok());

  fault_env.fail_sst_sync.store(true);
  const Status flush_status = engine.ForceFlush();
  EXPECT_FALSE(flush_status.ok()) << "SST fsync 失败必须让 flush 失败";
  // 顺序保证：flush 没成功 → 不推进水位、不删 WAL
  EXPECT_EQ(CountWalFiles(dir), 1u) << "flush 失败时不得删除 WAL 段";
  EXPECT_EQ(CountSstFiles(dir), 0u) << "失败的半截 SST 必须被清理";
  // 数据仍可读（imm_ 保留）
  ASSERT_TRUE(engine.Get(key).has_value());
  EXPECT_EQ(engine.Get(key)->AsInlineInt().value_or(-1), 77);
  EXPECT_EQ(engine.GetWALCheckpointWatermark(), 0u);

  fault_env.fail_sst_sync.store(false);
  ASSERT_TRUE(engine.Close().ok()) << "故障恢复后 Close 应能重试 flush";

  // 重开（正常 Env）：数据可见、没有半截 SST 被引用
  LsmEngine reopened(dir, MakeOptions(), Env::Default());
  ASSERT_TRUE(reopened.Open().ok());
  ASSERT_TRUE(reopened.Get(key).has_value());
  EXPECT_EQ(reopened.Get(key)->AsInlineInt().value_or(-1), 77);
  ASSERT_TRUE(reopened.Close().ok());
}

// 崩溃注入：半截 SST（Append 失败）→ 清理 + 保留 WAL + 重开后数据仍在。
TEST(FlushRecoveryTest, HalfWrittenSstIsRemovedAndWalKept) {
  cedar_test::ScopedTempDir temp_dir("wal_half_sst");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 23000;
  const CedarKey key = CedarKey::Vertex(5002, kColumn, Timestamp(kTs));

  WalFaultEnv fault_env;
  CedarOptions options = MakeOptions();
  options.env = &fault_env;

  LsmEngine engine(dir, options, &fault_env);
  ASSERT_TRUE(engine.Open().ok());
  ASSERT_TRUE(
      engine.Put(key, Descriptor::InlineInt(kColumn, 88), Timestamp(1)).ok());

  fault_env.fail_sst_appends.store(true);
  EXPECT_FALSE(engine.ForceFlush().ok());
  EXPECT_EQ(CountSstFiles(dir), 0u) << "半截 SST 不得留在目录里";
  EXPECT_EQ(CountWalFiles(dir), 1u) << "flush 失败不得删 WAL";

  fault_env.fail_sst_appends.store(false);
  ASSERT_TRUE(engine.Close().ok());

  LsmEngine reopened(dir, MakeOptions(), Env::Default());
  ASSERT_TRUE(reopened.Open().ok());
  ASSERT_TRUE(reopened.Get(key).has_value());
  EXPECT_EQ(reopened.Get(key)->AsInlineInt().value_or(-1), 88);
  ASSERT_TRUE(reopened.Close().ok());
}

// 崩溃注入：checkpoint 写入失败 → 水位没推进 → 不删段（重开仍能重放补齐）。
TEST(FlushRecoveryTest, CheckpointWriteFailureKeepsWal) {
  cedar_test::ScopedTempDir temp_dir("wal_checkpoint_write_failure");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 24000;
  const CedarKey key_a = CedarKey::Vertex(6001, kColumn, Timestamp(kTs));
  const CedarKey key_b = CedarKey::Vertex(6002, kColumn, Timestamp(kTs));

  WalFaultEnv fault_env;
  CedarOptions options = MakeOptions();
  options.env = &fault_env;

  LsmEngine engine(dir, options, &fault_env);
  ASSERT_TRUE(engine.Open().ok());
  ASSERT_TRUE(
      engine.Put(key_a, Descriptor::InlineInt(kColumn, 1), Timestamp(1)).ok());
  ASSERT_TRUE(engine.ForceFlush().ok());  // 第一轮：只封印，不需要 checkpoint

  ASSERT_TRUE(
      engine.Put(key_b, Descriptor::InlineInt(kColumn, 2), Timestamp(2)).ok());
  fault_env.fail_checkpoint_write.store(true);
  // 第二轮：SST 落盘成功，但水位没法持久化 → 不删段；flush 本身仍算成功
  EXPECT_TRUE(engine.ForceFlush().ok())
      << "checkpoint 写失败不应让已落盘的 flush 失败";
  EXPECT_EQ(engine.GetWALCheckpointWatermark(), 0u)
      << "水位不得在 checkpoint 落盘前推进";
  EXPECT_GE(CountWalFiles(dir), 2u) << "checkpoint 失败时不得删除 WAL 段";

  fault_env.fail_checkpoint_write.store(false);
  ASSERT_TRUE(engine.Close().ok());

  // 重开：数据完整（A/B 都可能来自 SST 或重放，读语义必须正确）
  LsmEngine reopened(dir, MakeOptions(), Env::Default());
  ASSERT_TRUE(reopened.Open().ok());
  ASSERT_TRUE(reopened.Get(key_a).has_value());
  EXPECT_EQ(reopened.Get(key_a)->AsInlineInt().value_or(-1), 1);
  ASSERT_TRUE(reopened.Get(key_b).has_value());
  EXPECT_EQ(reopened.Get(key_b)->AsInlineInt().value_or(-1), 2);
  ASSERT_TRUE(reopened.Close().ok());
}

// t7 窗口回归：关闭 WAL 截断（SST 只 Flush 不 fsync，WAL 不回收）时崩溃，
// 重开必须从 WAL 恢复，且不得引用半截 SST。
TEST(FlushRecoveryTest, FlushWithoutSstFsyncCrashRecoversFromWal) {
  cedar_test::ScopedTempDir temp_dir("wal_t7_window_crash");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 25000;
  const CedarKey key = CedarKey::Vertex(7001, kColumn, Timestamp(kTs));

  const CrashChildResult child = RunInCrashingChild([&dir, &key]() {
    auto* engine = new LsmEngine(dir, MakeOptions(), Env::Default());
    // 必须在 Open() 之前关闭截断：走 t7 的 flush 快路径（Sync 退化为 Flush）
    engine->SetWALTruncationEnabled(false);
    if (!engine->Open().ok()) ::_exit(10);
    if (!engine->Put(key, Descriptor::InlineInt(kColumn, 99), Timestamp(1)).ok()) {
      ::_exit(11);
    }
    if (!engine->ForceFlush().ok()) ::_exit(12);
    // flush 已返回（SST 未 fsync、WAL 未截断）→ body 返回后立刻 SIGKILL
  });
  ASSERT_TRUE(child.signaled);
  ASSERT_EQ(child.exit_code, 0);

  // 前提：SST 已写出，但 WAL 里的 record 还在（截断关闭）
  ASSERT_EQ(CountSstFiles(dir), 1u);
  EXPECT_EQ(CountWalFiles(dir), 1u);
  EXPECT_FALSE(DirectoryHasFile(dir, "MANIFEST-wal-checkpoint"))
      << "关闭截断时不应写 checkpoint";

  // 父进程：只用 WAL 就能恢复，且现有 SST 不是半截文件（能被 reader 打开）
  LsmEngine engine(dir, MakeOptions(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  EXPECT_EQ(engine.GetWalReplayStats().ops_applied, 1u)
      << "崩溃后必须从 WAL 重放补齐";
  ASSERT_TRUE(engine.Get(key).has_value());
  EXPECT_EQ(engine.Get(key)->AsInlineInt().value_or(-1), 99);
  for (const auto& file : ListFilesWithSuffix(dir, ".frond")) {
    ZoneColumnarSstReader reader(file);
    EXPECT_TRUE(reader.Open().ok()) << "引用了半截 SST: " << file;
  }
  ASSERT_TRUE(engine.Close().ok());
}

// 段号单调：即使 WAL 目录被整体清理，新段号也不得回绕到 checkpoint 水位之下，
// 否则重放会把新记录当成"已覆盖"而跳过（静默丢数据）。
TEST(FlushRecoveryTest, WalFileNumbersStayMonotonicAcrossTruncation) {
  cedar_test::ScopedTempDir temp_dir("wal_number_monotonic");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 26000;
  const CedarKey key_a = CedarKey::Vertex(8001, kColumn, Timestamp(kTs));
  const CedarKey key_b = CedarKey::Vertex(8002, kColumn, Timestamp(kTs));

  CedarOptions options = MakeOptions();
  uint64_t watermark_after_close = 0;
  {
    LsmEngine engine(dir, options, Env::Default());
    ASSERT_TRUE(engine.Open().ok());
    ASSERT_TRUE(
        engine.Put(key_a, Descriptor::InlineInt(kColumn, 1), Timestamp(1)).ok());
    ASSERT_TRUE(engine.ForceFlush().ok());
    ASSERT_TRUE(
        engine.Put(key_b, Descriptor::InlineInt(kColumn, 2), Timestamp(2)).ok());
    ASSERT_TRUE(engine.ForceFlush().ok());
    ASSERT_TRUE(engine.Close().ok());
    watermark_after_close = engine.GetWALCheckpointWatermark();
    EXPECT_GT(watermark_after_close, 1u);
  }

  // 模拟运维清理：把 wal 目录里的段全部删掉（checkpoint 保留）
  {
    const std::string wal_dir = dir + "/wal";
    std::vector<std::string> children;
    ASSERT_TRUE(Env::Default()->GetChildren(wal_dir, &children).ok());
    for (const auto& child : children) {
      if (child.size() <= 4 || child.compare(child.size() - 4, 4, ".wal") != 0) {
        continue;
      }
      ASSERT_TRUE(Env::Default()->RemoveFile(wal_dir + "/" + child).ok());
    }
    EXPECT_EQ(CountWalFiles(dir), 0u);
  }

  // 重开 + 再写：新段号必须 >= 水位，重放不能丢这条新记录
  {
    LsmEngine engine(dir, options, Env::Default());
    ASSERT_TRUE(engine.Open().ok());
    ASSERT_TRUE(
        engine.Put(key_b, Descriptor::InlineInt(kColumn, 22), Timestamp(3)).ok());
    ASSERT_TRUE(engine.ForceFlush().ok());
    const auto wal_files = ListFilesWithSuffix(dir + "/wal", ".wal");
    ASSERT_FALSE(wal_files.empty());
    for (const auto& file : wal_files) {
      EXPECT_GE(WalFileNumber(file), watermark_after_close)
          << "WAL 段号回绕到水位之下（重放会跳过新记录）";
    }
    ASSERT_TRUE(engine.Close().ok());
  }

  LsmEngine reopened(dir, options, Env::Default());
  ASSERT_TRUE(reopened.Open().ok());
  ASSERT_TRUE(reopened.Get(key_b).has_value());
  EXPECT_EQ(reopened.Get(key_b)->AsInlineInt().value_or(-1), 22);
  ASSERT_TRUE(reopened.Close().ok());
}

// checkpoint 损坏/被截断 → 不能阻止 Open：删除 checkpoint 并按无水位重放全部段。
TEST(FlushRecoveryTest, CorruptCheckpointFallsBackToFullReplay) {
  cedar_test::ScopedTempDir temp_dir("wal_corrupt_checkpoint");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kTs = 27000;
  const CedarKey key_a = CedarKey::Vertex(9001, kColumn, Timestamp(kTs));
  const CedarKey key_b = CedarKey::Vertex(9002, kColumn, Timestamp(kTs));

  CedarOptions options = MakeOptions();
  {
    LsmEngine engine(dir, options, Env::Default());
    ASSERT_TRUE(engine.Open().ok());
    ASSERT_TRUE(
        engine.Put(key_a, Descriptor::InlineInt(kColumn, 1), Timestamp(1)).ok());
    ASSERT_TRUE(engine.ForceFlush().ok());
    ASSERT_TRUE(
        engine.Put(key_b, Descriptor::InlineInt(kColumn, 2), Timestamp(2)).ok());
    ASSERT_TRUE(engine.ForceFlush().ok());
    ASSERT_TRUE(engine.Close().ok());
    ASSERT_TRUE(DirectoryHasFile(dir, "MANIFEST-wal-checkpoint"));
  }

  // 破坏 checkpoint（截断成半截）→ Open 必须把它当作"不存在"
  {
    const std::string path = dir + "/MANIFEST-wal-checkpoint";
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.good());
    out << "WALC";  // 只有 magic，长度不符 + CRC 不符
    out.close();
  }

  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok()) << "损坏的 checkpoint 不应让 Open 失败";
  EXPECT_EQ(engine.GetWALCheckpointWatermark(), 0u)
      << "损坏 checkpoint 必须按无水位处理（重放全部，安全方向）";
  ASSERT_TRUE(engine.Get(key_a).has_value());
  EXPECT_EQ(engine.Get(key_a)->AsInlineInt().value_or(-1), 1);
  ASSERT_TRUE(engine.Get(key_b).has_value());
  EXPECT_EQ(engine.Get(key_b)->AsInlineInt().value_or(-1), 2);
  ASSERT_TRUE(engine.Close().ok());
}

}  // namespace
