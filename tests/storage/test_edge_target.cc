// Cedar 0.1 边 target 语义回归测试（Task 0.2 / 1.5 / 1.6；审计 2.4、2.5、8.1、8.2）
//
// 红测试（Task 0.2 建立）：同 src / 同 edge_type、不同 dst 的两条边，分别经
// MemTable 与 SST 读取，必须精确命中各自的 dst。
//
// Task 1.5 扩展：target 必须贯穿 key → memtable → SST → 查询 API：
//   - 点查 Get / 历史 GetAtTime(dst) / 范围 GetAll、GetRange / 版本链 GetVersionChain(dst)
//   - CedarKey::ReconstructKey(row) 用 header + zone2 + zone3 重建完整 key
//   - Vertex 的 extension（存放在 target_id）不丢
//   - CedarGraphStorage::GetEdge / ScanEdges / GetDynamicEdge 参数（dst/edge_type）修正
// Task 1.6：WriteBatch 批量边保留 dst，WAL 与 memtable 使用同一个 key（崩溃重放后仍是两条）。

#include <gtest/gtest.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "cedar/frond/zone_columnar_format.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/wal_replay.h"
#include "cedar/transaction/batch_api.h"
#include "cedar/transaction/occ_transaction.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "sst_test_utils.h"

namespace {

using namespace cedar;

CedarOptions MakeOptions() {
  CedarOptions options;
  options.create_if_missing = true;
  options.env = Env::Default();
  return options;
}

// Task 3.5 测试需要精确控制"每 flush 一个文件"，因此关闭自动 compaction。
CedarOptions MakeNoCompactionOptions() {
  CedarOptions options = MakeOptions();
  options.size_tiered_config.enable_background_compaction = false;
  options.size_tiered_config.l0_max_files = 100000;
  options.size_tiered_config.l0_max_size = 1ull << 44;
  options.size_tiered_config.level_size_trigger_ratio = 1e9;
  return options;
}

// dst_id -> value；没有 dst 的条目（Vertex 无 extension）不计入
std::map<uint64_t, int> DstValues(const std::vector<MemTableEntry>& entries) {
  std::map<uint64_t, int> out;
  for (const auto& entry : entries) {
    if (!entry.dst_id.has_value()) continue;
    out[*entry.dst_id] = entry.descriptor.AsInlineInt().value_or(-1);
  }
  return out;
}

constexpr uint64_t kSrc = 1000;
constexpr uint64_t kDstA = 11;
constexpr uint64_t kDstB = 22;
constexpr uint64_t kDstC = 33;
constexpr uint16_t kEdgeType = 7;
constexpr uint64_t kTimestamp = 2000;

// =============================================================================
// Task 0.2 / 1.5：MemTable 与 SST 两条路径都必须精确命中 dst
// =============================================================================

TEST(EdgeTargetTest, EdgeTargetDistinctInMemTableAndSst) {
  cedar_test::ScopedTempDir temp_dir("edge_target");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr int kValueA = 111;
  constexpr int kValueB = 222;

  const CedarKey key_a =
      CedarKey::EdgeOut(kSrc, kDstA, kEdgeType, Timestamp(kTimestamp));
  const CedarKey key_b =
      CedarKey::EdgeOut(kSrc, kDstB, kEdgeType, Timestamp(kTimestamp));

  LsmEngine engine(dir, MakeOptions(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  ASSERT_TRUE(engine.Put(key_a, Descriptor::InlineInt(kEdgeType, kValueA),
                         Timestamp(1))
                  .ok());
  ASSERT_TRUE(engine.Put(key_b, Descriptor::InlineInt(kEdgeType, kValueB),
                         Timestamp(1))
                  .ok());

  auto check_both_edges = [&](const char* phase) {
    auto got_a = engine.Get(key_a);
    ASSERT_TRUE(got_a.has_value()) << phase << "：dst=" << kDstA << " 的边缺失";
    EXPECT_EQ(got_a->AsInlineInt().value_or(-1), kValueA)
        << phase << "：dst=" << kDstA << " 读到了错误的值（审计 2.4/8.2）";

    auto got_b = engine.Get(key_b);
    ASSERT_TRUE(got_b.has_value()) << phase << "：dst=" << kDstB << " 的边缺失";
    EXPECT_EQ(got_b->AsInlineInt().value_or(-1), kValueB)
        << phase << "：dst=" << kDstB
        << " 读到了另一条边的值（审计 2.4/8.2：点查忽略 target_id）";
  };

  {
    SCOPED_TRACE("MemTable 路径");
    check_both_edges("MemTable");
  }

  ASSERT_TRUE(engine.ForceFlush().ok()) << "flush 失败";

  {
    SCOPED_TRACE("SST 路径");
    check_both_edges("SST");
  }

  // 范围查询必须保留 dst（审计 2.5：GetRange 用 Vertex 重建 key）
  auto all = engine.GetAll(kSrc, EntityType::EdgeOut, kEdgeType);
  ASSERT_EQ(all.size(), 2u) << "SST 中应有两个不同 dst 的边";
  std::set<uint64_t> dst_ids;
  for (const auto& entry : all) {
    if (entry.dst_id.has_value()) {
      dst_ids.insert(*entry.dst_id);
    }
  }
  EXPECT_EQ(dst_ids.count(kDstA), 1u)
      << "GetAll 丢失 dst=" << kDstA << "（审计 2.5）";
  EXPECT_EQ(dst_ids.count(kDstB), 1u)
      << "GetAll 丢失 dst=" << kDstB << "（审计 2.5）";

  engine.Close();
}

// =============================================================================
// Task 1.5：三条边（同 src/type/ts、不同 dst）在所有读路径上都正确
// =============================================================================

TEST(EdgeTargetTest, AllReadPathsPreserveEdgeTarget) {
  cedar_test::ScopedTempDir temp_dir("edge_read_paths");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  const std::vector<std::pair<uint64_t, int>> edges = {
      {kDstA, 111}, {kDstB, 222}, {kDstC, 333}};
  const std::map<uint64_t, int> expected = {
      {kDstA, 111}, {kDstB, 222}, {kDstC, 333}};

  LsmEngine engine(dir, MakeOptions(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  for (const auto& [dst, value] : edges) {
    ASSERT_TRUE(engine
                    .Put(CedarKey::EdgeOut(kSrc, dst, kEdgeType,
                                           Timestamp(kTimestamp)),
                         Descriptor::InlineInt(kEdgeType, value), Timestamp(1))
                    .ok());
  }

  auto check_phase = [&](const char* phase, bool memtable_chain) {
    // 点查
    for (const auto& [dst, value] : edges) {
      auto got = engine.Get(
          CedarKey::EdgeOut(kSrc, dst, kEdgeType, Timestamp(kTimestamp)));
      ASSERT_TRUE(got.has_value()) << phase << "：dst=" << dst << " 点查缺失";
      EXPECT_EQ(got->AsInlineInt().value_or(-1), value)
          << phase << "：dst=" << dst << " 点查值错";
    }

    // 历史查询（带 target 的重载）
    for (const auto& [dst, value] : edges) {
      auto got = engine.GetAtTime(kSrc, EntityType::EdgeOut, kEdgeType, dst,
                                  Timestamp(kTimestamp));
      ASSERT_TRUE(got.has_value()) << phase << "：dst=" << dst << " 历史查询缺失";
      EXPECT_EQ(got->AsInlineInt().value_or(-1), value)
          << phase << "：dst=" << dst << " 历史查询值错";
    }

    // 范围查询：三条边都要在，dst 不能丢
    auto range = engine.GetRange(kSrc, EntityType::EdgeOut, kEdgeType,
                                 Timestamp(0), Timestamp(UINT64_MAX));
    EXPECT_EQ(DstValues(range), expected) << phase << "：GetRange 丢失/串读了边";

    auto all = engine.GetAll(kSrc, EntityType::EdgeOut, kEdgeType);
    EXPECT_EQ(DstValues(all), expected) << phase << "：GetAll 丢失/串读了边";

    // 指定 target 的版本链（memtable 才可能有内容）
    if (memtable_chain) {
      for (const auto& [dst, value] : edges) {
        auto chain = engine.GetVersionChain(kSrc, EntityType::EdgeOut, kEdgeType,
                                            dst);
        ASSERT_FALSE(chain.empty()) << phase << "：dst=" << dst << " 版本链为空";
        for (const auto& entry : chain) {
          EXPECT_EQ(entry.dst_id.value_or(0), dst)
              << phase << "：版本链串到了别的 dst";
          EXPECT_EQ(entry.descriptor.AsInlineInt().value_or(-1), value);
        }
      }
    }
  };

  check_phase("MemTable", /*memtable_chain=*/true);

  ASSERT_TRUE(engine.ForceFlush().ok());
  check_phase("SST", /*memtable_chain=*/false);

  engine.Close();
}

// =============================================================================
// Task 1.5：Vertex 的 extension（存在 target_id）在点查/范围/落盘后不丢
// =============================================================================

TEST(EdgeTargetTest, VertexExtensionPreservedAcrossReadPaths) {
  cedar_test::ScopedTempDir temp_dir("vertex_extension");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kEntity = 777;
  constexpr uint16_t kColumn = 3;
  constexpr uint64_t kExtension = 0xABCDEFULL;
  constexpr int kValue = 99;

  const CedarKey key = CedarKey::Vertex(kEntity, kColumn, Timestamp(kTimestamp),
                                        /*seq=*/0, kExtension);
  LsmEngine engine(dir, MakeOptions(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  ASSERT_TRUE(
      engine.Put(key, Descriptor::InlineInt(kColumn, kValue), Timestamp(1)).ok());

  auto check_phase = [&](const char* phase) {
    auto got = engine.Get(key);
    ASSERT_TRUE(got.has_value()) << phase << "：Vertex extension 点查缺失";
    EXPECT_EQ(got->AsInlineInt().value_or(-1), kValue);

    auto all = engine.GetAll(kEntity, EntityType::Vertex, kColumn);
    ASSERT_EQ(all.size(), 1u) << phase << "：Vertex 版本数错误";
    ASSERT_TRUE(all[0].dst_id.has_value())
        << phase << "：GetAll 丢了 Vertex extension（审计 2.5）";
    EXPECT_EQ(*all[0].dst_id, kExtension);

    auto at = engine.GetAtTime(kEntity, EntityType::Vertex, kColumn, kExtension,
                               Timestamp(kTimestamp));
    ASSERT_TRUE(at.has_value()) << phase << "：带 extension 的历史查询缺失";
    EXPECT_EQ(at->AsInlineInt().value_or(-1), kValue);

    // 完整 key 语义：extension 不匹配时不得命中
    auto wrong = engine.GetAtTime(kEntity, EntityType::Vertex, kColumn,
                                  kExtension + 1, Timestamp(kTimestamp));
    EXPECT_FALSE(wrong.has_value()) << phase << "：extension 不匹配却命中";
  };

  check_phase("MemTable");
  ASSERT_TRUE(engine.ForceFlush().ok());
  check_phase("SST");
  engine.Close();
}

// =============================================================================
// Task 1.5：SST reader 的 ReconstructKey / TargetAt 与写入的完整 key 字节一致
// =============================================================================

TEST(EdgeTargetTest, ReaderReconstructKeyMatchesFullKey) {
  cedar_test::ScopedTempDir temp_dir("edge_reconstruct");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  // CedarKey 排序：同 entity/type/column 时 target 升序 → timestamp 降序
  std::vector<std::pair<CedarKey, Descriptor>> rows;
  rows.emplace_back(CedarKey::EdgeOut(kSrc, kDstA, kEdgeType, Timestamp(kTimestamp)),
                    Descriptor::InlineInt(kEdgeType, 111));
  rows.emplace_back(CedarKey::EdgeOut(kSrc, kDstA, kEdgeType, Timestamp(kTimestamp - 1)),
                    Descriptor::InlineInt(kEdgeType, 112));
  rows.emplace_back(CedarKey::EdgeOut(kSrc, kDstB, kEdgeType, Timestamp(kTimestamp)),
                    Descriptor::InlineInt(kEdgeType, 222));
  rows.emplace_back(CedarKey::EdgeOut(kSrc, kDstC, kEdgeType, Timestamp(kTimestamp)),
                    Descriptor::InlineInt(kEdgeType, 333));

  std::string path;
  ASSERT_TRUE(cedar_test::BuildSstFile(dir, /*file_number=*/1, kEdgeType, rows,
                                       &path));
  ZoneColumnarSstReader reader(path);
  ASSERT_TRUE(reader.Open().ok());
  ASSERT_EQ(reader.NumEntries(), rows.size());

  for (size_t i = 0; i < rows.size(); ++i) {
    const CedarKey rebuilt = reader.ReconstructKey(i);
    EXPECT_EQ(rebuilt.Encode(), rows[i].first.Encode())
        << "第 " << i << " 行 ReconstructKey 与写入 key 字节不一致"
        << "（type/column/target/ts/seq/flags 必须完整）";
    EXPECT_EQ(reader.ReconstructKey(i, reader.Header()).Encode(),
              rows[i].first.Encode())
        << "ReconstructKey(row, header) 与单参重载不一致";
    EXPECT_EQ(reader.TargetAt(i), rows[i].first.target_id());
  }

  // 越界行返回空 key / target 0
  EXPECT_EQ(reader.ReconstructKey(rows.size()).Encode(), CedarKey().Encode());
  EXPECT_EQ(reader.TargetAt(rows.size()), 0u);

  // Vertex extension 同样完整重建
  std::vector<std::pair<CedarKey, Descriptor>> vertex_rows;
  vertex_rows.emplace_back(
      CedarKey::Vertex(kSrc, kEdgeType, Timestamp(kTimestamp), 0, 0xFEED),
      Descriptor::InlineInt(kEdgeType, 7));
  std::string vertex_path;
  ASSERT_TRUE(cedar_test::BuildSstFile(dir, /*file_number=*/2, kEdgeType,
                                       vertex_rows, &vertex_path));
  ZoneColumnarSstReader vertex_reader(vertex_path);
  ASSERT_TRUE(vertex_reader.Open().ok());
  EXPECT_EQ(vertex_reader.ReconstructKey(0).Encode(),
            vertex_rows[0].first.Encode());
  reader.Close();
  vertex_reader.Close();
}

// =============================================================================
// Task 1.5 / 审计 8.2：CedarGraphStorage 的边 API 参数（dst/edge_type）修正
// =============================================================================

TEST(EdgeTargetTest, GraphStorageEdgeApisUseTargetAndEdgeType) {
  cedar_test::ScopedTempDir temp_dir("graph_edge_api");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  CedarGraphStorage* storage = nullptr;
  ASSERT_TRUE(CedarGraphStorage::Open(MakeOptions(), dir, &storage).ok());
  ASSERT_NE(storage, nullptr);

  constexpr uint16_t kProperty = 5;

  ASSERT_TRUE(storage
                  ->PutEdge(kSrc, kDstA, kEdgeType, Timestamp(1000),
                            Descriptor::InlineInt(0, 111), Timestamp(1))
                  .ok());
  ASSERT_TRUE(storage
                  ->PutEdge(kSrc, kDstB, kEdgeType, Timestamp(1000),
                            Descriptor::InlineInt(0, 222), Timestamp(1))
                  .ok());

  auto edge_a = storage->GetEdge(kSrc, kDstA, kEdgeType, Timestamp(1000));
  ASSERT_TRUE(edge_a.has_value()) << "GetEdge(dst=11) 缺失";
  EXPECT_EQ(edge_a->AsInlineInt().value_or(-1), 111)
      << "GetEdge 读到别的 dst 的边（审计 8.2）";

  auto edge_b = storage->GetEdge(kSrc, kDstB, kEdgeType, Timestamp(1000));
  ASSERT_TRUE(edge_b.has_value()) << "GetEdge(dst=22) 缺失";
  EXPECT_EQ(edge_b->AsInlineInt().value_or(-1), 222);

  // 动态边属性：key 的 column 是 edge_type，dst 在 target，property 在 descriptor
  ASSERT_TRUE(storage
                  ->PutDynamicEdge(kSrc, kDstA, kEdgeType, kProperty,
                                   Timestamp(2000), Descriptor::InlineInt(0, 555),
                                   Timestamp(1))
                  .ok());
  auto dynamic_edge = storage->GetDynamicEdge(kSrc, kDstA, kEdgeType, kProperty,
                                              Timestamp(2000));
  ASSERT_TRUE(dynamic_edge.has_value())
      << "GetDynamicEdge 参数错位（审计 8.2：不能用 property_id 当 column）";
  EXPECT_EQ(dynamic_edge->AsInlineInt().value_or(-1), 555);

  // dst 不匹配时不得命中
  auto wrong_dst = storage->GetDynamicEdge(kSrc, kDstB, kEdgeType, kProperty,
                                           Timestamp(2000));
  EXPECT_FALSE(wrong_dst.has_value()) << "GetDynamicEdge 忽略了 dst";
  // property 不匹配时不得命中
  auto wrong_prop = storage->GetDynamicEdge(kSrc, kDstA, kEdgeType,
                                            kProperty + 1, Timestamp(2000));
  EXPECT_FALSE(wrong_prop.has_value()) << "GetDynamicEdge 忽略了 property_id";

  // 动态边范围扫描同样按 dst + property 过滤（审计 8.2 同根因）
  auto dyn_scan = storage->ScanDynamicEdge(kSrc, kDstA, kEdgeType, kProperty,
                                           Timestamp(0), Timestamp(UINT64_MAX));
  ASSERT_EQ(dyn_scan.size(), 1u) << "ScanDynamicEdge 缺少 dst 过滤/参数错位";
  EXPECT_EQ(dyn_scan[0].second.AsInlineInt().value_or(-1), 555);

  auto dyn_scan_other = storage->ScanDynamicEdge(
      kSrc, kDstB, kEdgeType, kProperty, Timestamp(0), Timestamp(UINT64_MAX));
  EXPECT_TRUE(dyn_scan_other.empty()) << "ScanDynamicEdge 忽略了 dst";

  // ScanEdges 必须返回真实 dst，而不是 src 占位
  // 注意：本用例放在最后，避免 LsmEngine::GetRangeLimit 的全范围跨查询缓存
  // 用旧结果掩盖动态属性写入（缓存失效是既有独立问题，不属于 Task 1.4-1.6）。
  auto scanned = storage->ScanEdges(kSrc, kEdgeType, Timestamp(0),
                                    Timestamp(UINT64_MAX));
  std::set<uint64_t> scanned_dsts;
  for (const auto& [dst, ts, desc] : scanned) {
    scanned_dsts.insert(dst);
  }
  EXPECT_EQ(scanned_dsts.count(kSrc), 0u)
      << "ScanEdges 仍用 src 占位（审计 8.2）";
  EXPECT_EQ(scanned_dsts.count(kDstA), 1u);
  EXPECT_EQ(scanned_dsts.count(kDstB), 1u);

  delete storage;
}

// =============================================================================
// Task 1.6 / 审计 8.1：WriteBatch 批量边保留 dst，WAL 与 memtable key 一致
// =============================================================================

namespace {

struct CrashChildResult {
  int exit_code = 0;
  int term_signal = 0;
  bool signaled = false;
};

template <typename Body>
CrashChildResult RunInCrashingChild(Body body) {
  CrashChildResult result;
  ::fflush(nullptr);
  const pid_t pid = ::fork();
  if (pid == 0) {
    body();
    ::kill(::getpid(), SIGKILL);
    ::_exit(1);
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

TEST(EdgeTargetTest, WriteBatchEdgesPreserveTargetAcrossWalReplay) {
  cedar_test::ScopedTempDir temp_dir("batch_edge_wal");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr int kValueA = 111;
  constexpr int kValueB = 222;
  constexpr uint64_t kTxnId = 7;
  const Timestamp kBusinessTs(kTimestamp);

  // ToWriteSet 把 txn_id 放进 CedarKey.sequence（既有设计），用于校验
  // WAL/memtable 共用的 key 字节与目标边的完整 key 一致。
  const CedarKey expected_a =
      CedarKey(kSrc, EntityType::EdgeOut, kEdgeType, kBusinessTs,
               static_cast<uint16_t>(kTxnId), kDstA);
  const CedarKey expected_b =
      CedarKey(kSrc, EntityType::EdgeOut, kEdgeType, kBusinessTs,
               static_cast<uint16_t>(kTxnId), kDstB);

  // 1) ToWriteSet 必须把 dst 放进 WriteSetEntry.target_id，且 entry.key 的字节
  //    与真实边 key 完全一致（WAL 与 memtable 共用 entry.key）。
  {
    WriteBatch batch;
    batch.PutEdge(kSrc, kDstA, "friend", kEdgeType,
                  Descriptor::InlineInt(kEdgeType, kValueA));
    batch.PutEdge(kSrc, kDstB, "friend", kEdgeType,
                  Descriptor::InlineInt(kEdgeType, kValueB));
    auto write_set = batch.ToWriteSet(kBusinessTs, kTxnId, Timestamp(1));
    ASSERT_EQ(write_set.size(), 2u);
    EXPECT_EQ(write_set[0].target_id, kDstA);
    EXPECT_EQ(write_set[1].target_id, kDstB);
    EXPECT_EQ(write_set[0].key.Encode(), expected_a.Encode());
    EXPECT_EQ(write_set[1].key.Encode(), expected_b.Encode());
  }

  // 2) 子进程通过 WriteBatch 事务提交两条边，然后被 SIGKILL（不 Close/不 flush），
  //    数据只在 WAL；父进程重开后必须仍是两条不同 dst 的边。
  const CrashChildResult child = RunInCrashingChild([&dir]() {
    // engine 放在堆上并有意泄漏：栈对象在 body() 返回时会析构 → Close() →
    // flush + WAL checkpoint 截断，崩溃模拟就失效了（t10 暴露的测试前提缺陷）。
    auto* engine = new LsmEngine(dir, MakeOptions(), Env::Default());
    if (!engine->Open().ok()) ::_exit(10);

    BatchExecutor executor(engine);
    WriteBatch batch;
    batch.PutEdge(kSrc, kDstA, "friend", kEdgeType,
                  Descriptor::InlineInt(kEdgeType, kValueA));
    batch.PutEdge(kSrc, kDstB, "friend", kEdgeType,
                  Descriptor::InlineInt(kEdgeType, kValueB));

    TransactionOptions txn_options;
    BatchResult result = executor.WriteTransactional(batch, txn_options);
    if (!result.status.ok()) ::_exit(11);
    // 故意不 Close、不 ForceFlush
  });
  ASSERT_TRUE(child.signaled) << "子进程未按预期被 SIGKILL（exit="
                              << child.exit_code << "）";
  ASSERT_EQ(child.exit_code, 0) << "子进程提交失败，退出码 " << child.exit_code;

  LsmEngine engine(dir, MakeOptions(), Env::Default());
  ASSERT_TRUE(engine.Open().ok()) << "WAL 重放后重开失败";

  const WalReplayStats replay = engine.GetWalReplayStats();
  ASSERT_GT(replay.committed_records, 0u)
      << "WAL 里没有已提交 record（files=" << replay.files
      << " records=" << replay.records
      << " corrupt=" << replay.corrupted_records << "）";
  ASSERT_EQ(engine.GetMemTable()->size(), 2u)
      << "WAL 重放后 memtable 行数应为 2（实际 " << engine.GetMemTable()->size()
      << "）";

  // WriteTransactional 使用 Timestamp::Now() 作为业务时间戳（Batch API 不接受
  // 外部时间戳），因此按 target + 时间上限查询最新版本。
  auto got_a = engine.GetAtTime(kSrc, EntityType::EdgeOut, kEdgeType, kDstA,
                                Timestamp::Max());
  ASSERT_TRUE(got_a.has_value())
      << "WAL 重放后 dst=" << kDstA << " 的边丢失（审计 8.1）";
  EXPECT_EQ(got_a->AsInlineInt().value_or(-1), kValueA);

  auto got_b = engine.GetAtTime(kSrc, EntityType::EdgeOut, kEdgeType, kDstB,
                                Timestamp::Max());
  ASSERT_TRUE(got_b.has_value())
      << "WAL 重放后 dst=" << kDstB << " 的边丢失（两条边被塌缩成一条）";
  EXPECT_EQ(got_b->AsInlineInt().value_or(-1), kValueB);

  auto all = engine.GetAll(kSrc, EntityType::EdgeOut, kEdgeType);
  EXPECT_EQ(DstValues(all),
            (std::map<uint64_t, int>{{kDstA, kValueA}, {kDstB, kValueB}}))
      << "WAL 重放后 GetAll 的 dst 不完整";

  engine.Close();
}

// =============================================================================
// Task 3.5：>10 个文件不再静默截断；limit 在归并中截断且"最新优先"
// =============================================================================

TEST(EdgeTargetTest, RangeLimitAcrossManyFilesIsCompleteAndNewestFirst) {
  cedar_test::ScopedTempDir temp_dir("range_many_files");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kEntity = 42;
  constexpr uint16_t kColumn = 3;
  constexpr size_t kFiles = 15;  // > 旧实现的 kMaxFilesToQuery=10

  LsmEngine engine(dir, MakeNoCompactionOptions(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  for (size_t i = 0; i < kFiles; ++i) {
    ASSERT_TRUE(engine
                    .Put(CedarKey::Vertex(kEntity, kColumn, Timestamp(1000 + i)),
                         Descriptor::InlineInt(kColumn, static_cast<int32_t>(i)),
                         Timestamp(1))
                    .ok());
    ASSERT_TRUE(engine.ForceFlush().ok()) << "flush " << i;
  }

  // 完整范围：15 个版本一个都不能少
  auto all = engine.GetRangeLimit(kEntity, EntityType::Vertex, kColumn,
                                  Timestamp(0), Timestamp(UINT64_MAX), SIZE_MAX);
  ASSERT_EQ(all.size(), kFiles) << ">10 文件被静默截断";
  for (size_t i = 0; i < all.size(); ++i) {
    EXPECT_EQ(all[i].timestamp.value(), 1000 + (kFiles - 1 - i))
        << "结果必须最新优先";
    EXPECT_EQ(all[i].descriptor.AsInlineInt().value_or(-1),
              static_cast<int32_t>(kFiles - 1 - i));
  }

  // limit：返回最新 5 条（1014..1010），而不是最旧或前 10 个文件里的任意 5 条
  auto limited = engine.GetRangeLimit(kEntity, EntityType::Vertex, kColumn,
                                      Timestamp(0), Timestamp(UINT64_MAX), 5);
  ASSERT_EQ(limited.size(), 5u);
  for (size_t i = 0; i < limited.size(); ++i) {
    EXPECT_EQ(limited[i].timestamp.value(), 1014 - i);
    EXPECT_EQ(limited[i].descriptor.AsInlineInt().value_or(-1),
              static_cast<int32_t>(14 - i));
  }

  // GetAll 同样完整
  auto all_versions = engine.GetAll(kEntity, EntityType::Vertex, kColumn);
  EXPECT_EQ(all_versions.size(), kFiles);
  engine.Close();
}

// =============================================================================
// Task 3.5（读一致性）：写入/删除后跨查询缓存必须失效
// =============================================================================

TEST(EdgeTargetTest, CrossQueryCacheInvalidatedOnWrite) {
  cedar_test::ScopedTempDir temp_dir("cross_cache_invalidate");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint64_t kEntity = 7;
  constexpr uint16_t kColumn = 3;

  LsmEngine engine(dir, MakeNoCompactionOptions(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  ASSERT_TRUE(engine
                  .Put(CedarKey::Vertex(kEntity, kColumn, Timestamp(1000)),
                       Descriptor::InlineInt(kColumn, 1), Timestamp(1))
                  .ok());

  auto read_all = [&]() {
    return engine.GetRangeLimit(kEntity, EntityType::Vertex, kColumn,
                                Timestamp(0), Timestamp(UINT64_MAX), SIZE_MAX);
  };

  // 第一次全范围读会写入跨查询缓存（TTL 60s）
  auto first = read_all();
  ASSERT_EQ(first.size(), 1u);
  EXPECT_EQ(first[0].descriptor.AsInlineInt().value_or(-1), 1);

  // 再写一个新版本：缓存若不失效，会返回写前的 1 条旧结果
  ASSERT_TRUE(engine
                  .Put(CedarKey::Vertex(kEntity, kColumn, Timestamp(2000)),
                       Descriptor::InlineInt(kColumn, 2), Timestamp(1))
                  .ok());
  auto second = read_all();
  ASSERT_EQ(second.size(), 2u)
      << "写入后仍读到缓存旧结果（cross_query_cache 未失效）";
  EXPECT_EQ(second[0].descriptor.AsInlineInt().value_or(-1), 2);

  // 同一 key 覆盖写：值必须更新
  ASSERT_TRUE(engine
                  .Put(CedarKey::Vertex(kEntity, kColumn, Timestamp(2000)),
                       Descriptor::InlineInt(kColumn, 3), Timestamp(1))
                  .ok());
  auto third = read_all();
  ASSERT_EQ(third.size(), 2u);
  EXPECT_EQ(third[0].descriptor.AsInlineInt().value_or(-1), 3);

  // 删除：缓存必须反映墓碑
  ASSERT_TRUE(
      engine.Delete(CedarKey::Vertex(kEntity, kColumn, Timestamp(2000)), Timestamp(1))
          .ok());
  auto fourth = read_all();
  ASSERT_EQ(fourth.size(), 1u) << "删除后仍读到已删除版本（缓存未失效）";
  EXPECT_EQ(fourth[0].timestamp.value(), 1000u);

  // flush 后再读仍然一致
  ASSERT_TRUE(engine.ForceFlush().ok());
  auto fifth = read_all();
  ASSERT_EQ(fifth.size(), 1u);
  EXPECT_EQ(fifth[0].descriptor.AsInlineInt().value_or(-1), 1);

  engine.Close();
}

// =============================================================================
// Task 3.5：批量查询使用所有 entity_ids 的文件并集（不再只看 entity_ids[0]）
// =============================================================================

TEST(EdgeTargetTest, BatchGetRangeUsesAllEntityFiles) {
  cedar_test::ScopedTempDir temp_dir("batch_union_files");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint16_t kColumn = 3;
  constexpr uint64_t kEntityA = 111;
  constexpr uint64_t kEntityB = 222;

  LsmEngine engine(dir, MakeNoCompactionOptions(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  // A 和 B 分属两个不同的 SST 文件
  ASSERT_TRUE(engine
                  .Put(CedarKey::Vertex(kEntityA, kColumn, Timestamp(1000)),
                       Descriptor::InlineInt(kColumn, 11), Timestamp(1))
                  .ok());
  ASSERT_TRUE(engine.ForceFlush().ok());
  ASSERT_TRUE(engine
                  .Put(CedarKey::Vertex(kEntityB, kColumn, Timestamp(1000)),
                       Descriptor::InlineInt(kColumn, 22), Timestamp(1))
                  .ok());
  ASSERT_TRUE(engine.ForceFlush().ok());

  auto results = engine.BatchGetRangeOptimized({kEntityA, kEntityB},
                                                EntityType::Vertex, kColumn,
                                                Timestamp(0), Timestamp(UINT64_MAX),
                                                /*max_results_per_entity=*/10);
  ASSERT_EQ(results.count(kEntityA), 1u);
  ASSERT_EQ(results.count(kEntityB), 1u);
  ASSERT_EQ(results[kEntityA].size(), 1u)
      << "entity A 的结果缺失（批量查询文件并集错误）";
  ASSERT_EQ(results[kEntityB].size(), 1u)
      << "entity B 的结果缺失（旧实现只用 entity_ids[0] 的文件列表）";
  EXPECT_EQ(results[kEntityA][0].descriptor.AsInlineInt().value_or(-1), 11);
  EXPECT_EQ(results[kEntityB][0].descriptor.AsInlineInt().value_or(-1), 22);

  // 单个 entity 的 GetAll/GetRange 也走区间索引，结果一致
  auto all_b = engine.GetAll(kEntityB, EntityType::Vertex, kColumn);
  ASSERT_EQ(all_b.size(), 1u);
  EXPECT_EQ(all_b[0].descriptor.AsInlineInt().value_or(-1), 22);

  engine.Close();
}

}  // namespace
