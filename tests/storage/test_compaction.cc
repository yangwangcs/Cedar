// Cedar 0.1 Compaction 正确性回归测试（Task 0.2 / 审计 3.1、9.1、9.2、9.3 / Task 1.7）
//
// 红测试场景：两个列各 2 个 L0 文件 + 1 个 L1 重叠文件，执行一次（或按列分组的多次）
// 压缩后必须满足：
//   1. 不同 (entity_type, column_id) 的文件不得被合并进同一个输出文件（审计 9.1）；
//   2. L1 重叠文件的内容必须参与合并并被保留，不得合并前直接删除（审计 3.1）；
//   3. 文件号唯一、元数据不指向不存在/已被覆盖的文件（审计 9.2/9.3）。
//
// Task 1.7 扩展覆盖：
//   - PickNextCompaction 按 (entity_type, column_id) 分组，一个任务只处理一个组；
//   - FindOverlapping 按列/类型过滤，不相关的下层文件既不合并不删除；
//   - MergeZones 的输入集合 = input_files ∪ overlapping_files；
//   - 单一 FileNumberAllocator（LsmEngine 持有）供 flush 与 compaction 共享，
//     取号水位写入 MANIFEST 并在重开时恢复；
//   - 先提交 MANIFEST 再删除输入物理文件；失败时保留输入、清理输出；
//   - 压缩后 GetFilesForEntity 的元数据集合与磁盘 .frond 集合一致；
//   - 重开（MANIFEST 已有 L1 文件）不会把同一文件号重复登记到 L0/L1（审计 9.3）。
//
// Task 4.1-4.5 扩展覆盖：
//   - K 路堆归并 + 同 key 取舍（ts 同取 sequence 大；完整相同取文件号大）；
//   - 输出按 l0_file_size 切分、每段 ≤ 目标 + 一个 block；
//   - 任务按 size tier 选相似大小文件；
//   - 并发调度无重复任务 / pending 有界 / 保留集一致；失败指数退避；
//   - MANIFEST 半截 tmp 忽略、损坏/越界 MANIFEST 拒绝（崩溃注入）；
//   - flush 向量分组：每组一个文件、组内有序、列不串。

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <chrono>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cedar/core/env.h"
#include "cedar/frond/zone_columnar_format.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/size_tiered_compaction.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "sst_test_utils.h"

namespace {

using namespace cedar;

constexpr int kMaxLevels = 7;

// 只让 L0 触发合并：L0 阈值 1 字节（必然触发），L1+ 阈值 = 1 * 1e9 字节（远大于
// 测试数据）；关闭后台线程，测试自己驱动 Pick/Execute，避免级联到高层。
SizeTieredConfig MakeL0OnlyConfig() {
  SizeTieredConfig config;
  config.enable_background_compaction = false;
  config.compaction_threads = 0;
  config.l0_max_size = 1;
  config.level_size_trigger_ratio = 1.0;
  config.size_ratio = 1e9;
  config.max_levels = kMaxLevels;
  return config;
}

std::pair<CedarKey, Descriptor> VertexRow(uint64_t entity, uint16_t column,
                                          uint64_t ts, int value) {
  return {CedarKey(entity, EntityType::Vertex, column, Timestamp(ts)),
          Descriptor::InlineInt(column, value)};
}

std::pair<CedarKey, Descriptor> EdgeRow(uint64_t src, uint64_t dst,
                                        uint16_t edge_type, uint64_t ts, int value) {
  return {CedarKey::EdgeOut(src, dst, edge_type, Timestamp(ts)),
          Descriptor::InlineInt(edge_type, value)};
}

// 只构造元数据（不写文件），用于 LevelState::FindOverlapping 的纯逻辑测试。
ZoneSstMeta MetaOnly(uint64_t file_number, uint16_t column, EntityType type,
                     uint64_t min_entity, uint64_t max_entity, uint64_t min_ts,
                     uint64_t max_ts) {
  ZoneSstMeta meta;
  meta.file_number = file_number;
  meta.file_size = 1024;
  meta.level = 1;
  meta.column_id = column;
  meta.entity_type = static_cast<uint8_t>(type);
  meta.min_entity_id = min_entity;
  meta.max_entity_id = max_entity;
  meta.min_timestamp = min_ts;
  meta.max_timestamp = max_ts;
  return meta;
}

// 构建并登记一个 SST 文件；元数据范围由真实行计算。
// rows 必须同属 (column, type)：Zone-Columnar 的文件头只保存一组列/类型。
bool AddSstFile(SizeTieredCompactionEngine* engine, const std::string& dir,
                uint64_t file_number, int level, uint16_t column,
                EntityType type,
                std::vector<std::pair<CedarKey, Descriptor>> rows) {
  if (rows.empty()) return false;
  std::sort(rows.begin(), rows.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });

  std::string path;
  if (!cedar_test::BuildSstFile(dir, file_number, column, rows, &path)) {
    return false;
  }

  uint64_t min_entity = UINT64_MAX;
  uint64_t max_entity = 0;
  uint64_t min_ts = UINT64_MAX;
  uint64_t max_ts = 0;
  for (const auto& row : rows) {
    min_entity = std::min(min_entity, row.first.entity_id());
    max_entity = std::max(max_entity, row.first.entity_id());
    min_ts = std::min(min_ts, row.first.timestamp().value());
    max_ts = std::max(max_ts, row.first.timestamp().value());
  }
  const ZoneSstMeta meta = cedar_test::MakeSstMeta(
      path, file_number, level, column, type, min_entity, max_entity, min_ts,
      max_ts, rows.size());
  return engine->AddSSTFile(meta).ok();
}

// 反复 Pick + Execute 直到没有可执行任务（最多 max_rounds 轮，防不收敛）。
Status RunAllCompactions(SizeTieredCompactionEngine* engine,
                         std::vector<CompactionTask>* executed,
                         int max_rounds = 16) {
  for (int round = 0; round < max_rounds; ++round) {
    auto task = engine->PickNextCompaction();
    if (!task.has_value()) {
      return Status::OK();
    }
    const Status s = engine->ExecuteCompaction(*task);
    if (!s.ok()) {
      return s;
    }
    if (executed != nullptr) {
      executed->push_back(*task);
    }
  }
  return Status::InvalidArgument("test", "compaction did not converge");
}

// 所有层级的文件元数据（meta.level 回填为真实层级）。
std::vector<ZoneSstMeta> AllFiles(const SizeTieredCompactionEngine& engine) {
  std::vector<ZoneSstMeta> all;
  for (int level = 0; level < kMaxLevels; ++level) {
    for (ZoneSstMeta meta : engine.GetLevelFiles(level)) {
      meta.level = level;
      all.push_back(meta);
    }
  }
  return all;
}

std::vector<ZoneSstMeta> FilesOfGroup(const SizeTieredCompactionEngine& engine,
                                      uint16_t column, EntityType type) {
  std::vector<ZoneSstMeta> out;
  for (const auto& meta : AllFiles(engine)) {
    if (meta.column_id == column &&
        meta.entity_type == static_cast<uint8_t>(type)) {
      out.push_back(meta);
    }
  }
  return out;
}

bool HasValue(const std::vector<ZoneSstMeta>& files, const CedarKey& key,
              int want) {
  for (const auto& meta : files) {
    ZoneColumnarSstReader reader(meta.path);
    if (!reader.Open().ok()) continue;
    auto got = reader.Get(key);
    if (got.has_value() && got->AsInlineInt().value_or(-1) == want) {
      return true;
    }
  }
  return false;
}

// 文件里每一行的完整 key 必须属于该文件头的 (column, entity_type)。
bool AllRowsMatchGroup(const ZoneSstMeta& meta, uint16_t column,
                       EntityType type) {
  ZoneColumnarSstReader reader(meta.path);
  if (!reader.Open().ok()) return false;
  std::unique_ptr<ZoneColumnarIterator> it(reader.NewIterator());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const CedarKey key = it->Key();
    if (key.column_id() != column || key.entity_type() != type) {
      return false;
    }
  }
  return true;
}

std::set<uint64_t> MetaFileNumbers(const SizeTieredCompactionEngine& engine) {
  std::set<uint64_t> numbers;
  for (const auto& meta : AllFiles(engine)) {
    numbers.insert(meta.file_number);
  }
  return numbers;
}

std::set<uint64_t> DiskFileNumbers(const std::string& dir) {
  std::set<uint64_t> numbers;
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
    if (!entry.is_regular_file()) continue;
    if (entry.path().extension() != ".frond") continue;
    const std::string name = entry.path().stem().string();
    if (name.empty() ||
        !std::all_of(name.begin(), name.end(), [](unsigned char c) {
          return std::isdigit(c) != 0;
        })) {
      continue;
    }
    numbers.insert(std::stoull(name));
  }
  return numbers;
}

// =============================================================================
// Task 0.2 / 审计 3.1、9.1：原始红测试（列隔离 + 下层保留 + 文件号唯一）
// =============================================================================

TEST(CompactionTest, MixedColumnAndOverlapPreserved) {
  cedar_test::ScopedTempDir temp_dir("compaction");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint16_t kColumnA = 10;
  constexpr uint16_t kColumnB = 11;
  constexpr uint64_t kOverlapEntity = 2;
  constexpr uint64_t kOverlapTs = 103;
  constexpr int kOverlapValue = 999;

  SizeTieredConfig config = MakeL0OnlyConfig();

  SizeTieredCompactionEngine engine(dir, config, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  // 构建并登记一个文件；rows 为 (entity_id, timestamp, value)
  auto add_file = [&](uint64_t file_number, int level, uint16_t column,
                      const std::vector<std::pair<uint64_t, uint64_t>>& entity_ts,
                      int value) -> bool {
    std::vector<std::pair<CedarKey, Descriptor>> rows;
    rows.reserve(entity_ts.size());
    for (const auto& [entity, ts] : entity_ts) {
      rows.emplace_back(
          CedarKey(entity, EntityType::Vertex, column, Timestamp(ts)),
          Descriptor::InlineInt(column, value));
    }
    std::sort(rows.begin(), rows.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    std::string path;
    if (!cedar_test::BuildSstFile(dir, file_number, column, rows, &path)) {
      return false;
    }
    uint64_t min_entity = UINT64_MAX;
    uint64_t max_entity = 0;
    uint64_t min_ts = UINT64_MAX;
    uint64_t max_ts = 0;
    for (const auto& row : rows) {
      min_entity = std::min(min_entity, row.first.entity_id());
      max_entity = std::max(max_entity, row.first.entity_id());
      min_ts = std::min(min_ts, row.first.timestamp().value());
      max_ts = std::max(max_ts, row.first.timestamp().value());
    }
    const ZoneSstMeta meta = cedar_test::MakeSstMeta(
        path, file_number, level, column, EntityType::Vertex, min_entity,
        max_entity, min_ts, max_ts, rows.size());
    return engine.AddSSTFile(meta).ok();
  };

  // L0：列 A 两个文件（entity 1..6），列 B 两个文件（entity 7..12）
  ASSERT_TRUE(add_file(1, 0, kColumnA,
                       {{1, 100}, {2, 101}, {3, 102}}, 101));
  ASSERT_TRUE(add_file(2, 0, kColumnA,
                       {{4, 101}, {5, 102}, {6, 103}}, 102));
  ASSERT_TRUE(add_file(3, 0, kColumnB,
                       {{7, 100}, {8, 101}, {9, 102}}, 201));
  ASSERT_TRUE(add_file(4, 0, kColumnB,
                       {{10, 101}, {11, 102}, {12, 103}}, 202));

  // L1：与 L0 的 entity/时间范围都重叠的文件，其 (entity=2, ts=103) 在 L0 中不存在
  ASSERT_TRUE(add_file(5, 1, kColumnA,
                       {{kOverlapEntity, kOverlapTs}}, kOverlapValue));

  // 执行合并（正确实现会按列分组，可能产生多个任务）
  for (int round = 0; round < 10; ++round) {
    auto task = engine.PickNextCompaction();
    if (!task.has_value()) break;
    Status s = engine.ExecuteCompaction(*task);
    ASSERT_TRUE(s.ok()) << "compaction 失败: " << s.ToString()
                        << " task=" << task->DebugString();
  }

  // 1) 列隔离：列 B 的 entity 必须仍能按 (column_id=11, Vertex) 找到文件
  auto files_for_b =
      engine.GetFilesForEntity(7, kColumnB, static_cast<uint8_t>(EntityType::Vertex));
  EXPECT_FALSE(files_for_b.empty())
      << "压缩后列 B 的文件元数据消失（审计 9.1：压缩混合不同 column_id 的文件）";

  bool found_column_b_value = false;
  for (const auto& meta : files_for_b) {
    ZoneColumnarSstReader reader(meta.path);
    if (!reader.Open().ok()) continue;
    auto got = reader.Get(
        CedarKey(7, EntityType::Vertex, kColumnB, Timestamp(100)));
    if (got.has_value() && got->AsInlineInt().value_or(-1) == 201) {
      found_column_b_value = true;
    }
  }
  EXPECT_TRUE(found_column_b_value)
      << "列 B 的数据在压缩后被改写或隐藏（审计 9.1：输出 header 只保留首文件列）";

  // 2) 下层重叠文件的数据必须被保留（参与合并），不能直接删除
  bool found_overlap_value = false;
  for (int level = 0; level < 7; ++level) {
    for (const auto& meta : engine.GetLevelFiles(level)) {
      if (meta.column_id != kColumnA) continue;
      ZoneColumnarSstReader reader(meta.path);
      if (!reader.Open().ok()) continue;
      auto got = reader.Get(CedarKey(static_cast<uint64_t>(kOverlapEntity),
                                     EntityType::Vertex, kColumnA,
                                     Timestamp(kOverlapTs)));
      if (got.has_value() && got->AsInlineInt().value_or(-1) == kOverlapValue) {
        found_overlap_value = true;
      }
    }
  }
  EXPECT_TRUE(found_overlap_value)
      << "L1 重叠文件未参与合并就被删除，数据丢失（审计 3.1/9.1）";

  // 3) 元数据一致性：文件号唯一、路径存在
  std::set<uint64_t> file_numbers;
  size_t total_files = 0;
  for (int level = 0; level < 7; ++level) {
    for (const auto& meta : engine.GetLevelFiles(level)) {
      ++total_files;
      EXPECT_TRUE(std::filesystem::exists(meta.path))
          << "元数据指向不存在的文件: " << meta.path;
      EXPECT_TRUE(file_numbers.insert(meta.file_number).second)
          << "文件号重复（审计 9.2）：" << meta.file_number;
    }
  }
  EXPECT_GT(total_files, 0u) << "压缩后不应没有任何文件";

  engine.Close();
}

// =============================================================================
// Task 1.7：FindOverlapping 必须按 (entity_type, column_id) 过滤
// =============================================================================

TEST(CompactionTest, FindOverlappingFiltersColumnAndType) {
  LevelState level;
  level.level = 1;
  level.files.push_back(MetaOnly(1, 10, EntityType::Vertex, 1, 10, 100, 200));
  level.files.push_back(MetaOnly(2, 11, EntityType::Vertex, 1, 10, 100, 200));
  level.files.push_back(MetaOnly(3, 10, EntityType::EdgeOut, 1, 10, 100, 200));
  level.files.push_back(MetaOnly(4, 10, EntityType::Vertex, 100, 200, 100, 200));

  // 旧签名（无列/类型过滤）：前三个与范围重叠
  const auto range_only = level.FindOverlapping(1, 10, 100, 200);
  EXPECT_EQ(range_only.size(), 3u);

  // 过滤后：只剩同 (column=10, Vertex) 且范围重叠的那个文件
  const auto vertex_col10 = level.FindOverlapping(
      1, 10, 100, 200, 10, static_cast<uint8_t>(EntityType::Vertex));
  ASSERT_EQ(vertex_col10.size(), 1u);
  EXPECT_EQ(vertex_col10[0].file_number, 1u);

  const auto edge_col10 = level.FindOverlapping(
      1, 10, 100, 200, 10, static_cast<uint8_t>(EntityType::EdgeOut));
  ASSERT_EQ(edge_col10.size(), 1u);
  EXPECT_EQ(edge_col10[0].file_number, 3u);

  const auto vertex_col11 = level.FindOverlapping(
      1, 10, 100, 200, 11, static_cast<uint8_t>(EntityType::Vertex));
  ASSERT_EQ(vertex_col11.size(), 1u);
  EXPECT_EQ(vertex_col11[0].file_number, 2u);

  // 类型不匹配：同一列但有 Vertex/EdgeOut 两种文件，过滤后只返回对应类型
  const auto vertex_only = level.FindOverlapping(
      1, 10, 100, 200, 10, static_cast<uint8_t>(EntityType::EdgeIn));
  EXPECT_TRUE(vertex_only.empty());
}

// =============================================================================
// Task 1.7：PickNextCompaction 按 (entity_type, column_id) 分组
// =============================================================================

TEST(CompactionTest, PickGroupsByColumnAndType) {
  cedar_test::ScopedTempDir temp_dir("compaction_grouping");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint16_t kColumnA = 10;
  constexpr uint16_t kColumnB = 11;
  constexpr uint16_t kEdgeType = 10;

  SizeTieredCompactionEngine engine(dir, MakeL0OnlyConfig(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  // L0：列 A 两个 Vertex 文件、列 B 一个 Vertex 文件、一个 EdgeOut 文件
  ASSERT_TRUE(AddSstFile(&engine, dir, 1, 0, kColumnA, EntityType::Vertex,
                         {VertexRow(1, kColumnA, 100, 101),
                          VertexRow(2, kColumnA, 101, 102)}));
  ASSERT_TRUE(AddSstFile(&engine, dir, 2, 0, kColumnA, EntityType::Vertex,
                         {VertexRow(3, kColumnA, 102, 103)}));
  ASSERT_TRUE(AddSstFile(&engine, dir, 3, 0, kColumnB, EntityType::Vertex,
                         {VertexRow(4, kColumnB, 100, 201)}));
  ASSERT_TRUE(AddSstFile(&engine, dir, 4, 0, kColumnA, EntityType::EdgeOut,
                         {EdgeRow(5, 900, kEdgeType, 100, 301)}));

  // L1：列 A Vertex 的重叠文件（entity 2 / ts 105），压缩时必须参与合并
  ASSERT_TRUE(AddSstFile(&engine, dir, 5, 1, kColumnA, EntityType::Vertex,
                         {VertexRow(2, kColumnA, 105, 111)}));

  std::vector<CompactionTask> tasks;
  const Status s = RunAllCompactions(&engine, &tasks);
  ASSERT_TRUE(s.ok()) << "compaction 失败: " << s.ToString();
  ASSERT_EQ(tasks.size(), 3u) << "应按 (type,column) 分成 3 个任务";

  // 每个任务只能包含一个组的文件（输入与重叠都是）
  std::set<std::pair<uint8_t, uint16_t>> groups;
  for (const auto& task : tasks) {
    ASSERT_FALSE(task.input_files.empty());
    const auto group = std::make_pair(task.input_files[0].entity_type,
                                      task.input_files[0].column_id);
    for (const auto& meta : task.input_files) {
      EXPECT_EQ(meta.entity_type, group.first) << "任务混入了不同 entity_type";
      EXPECT_EQ(meta.column_id, group.second) << "任务混入了不同 column_id";
    }
    for (const auto& meta : task.overlapping_files) {
      EXPECT_EQ(meta.entity_type, group.first) << "重叠文件混入了不同 entity_type";
      EXPECT_EQ(meta.column_id, group.second) << "重叠文件混入了不同 column_id";
    }
    groups.insert(group);
  }
  EXPECT_EQ(groups.size(), 3u);
  EXPECT_TRUE(groups.count({static_cast<uint8_t>(EntityType::Vertex), kColumnA}) == 1);
  EXPECT_TRUE(groups.count({static_cast<uint8_t>(EntityType::Vertex), kColumnB}) == 1);
  EXPECT_TRUE(groups.count({static_cast<uint8_t>(EntityType::EdgeOut), kColumnA}) == 1);

  // 列不混：每个输出文件的所有行都必须属于该文件头的 (column, type)
  for (const auto& meta : AllFiles(engine)) {
    const bool ok = AllRowsMatchGroup(
        meta, meta.column_id, static_cast<EntityType>(meta.entity_type));
    EXPECT_TRUE(ok) << "输出文件 " << meta.file_number
                    << " 含有不属于 (" << meta.column_id << ","
                    << static_cast<int>(meta.entity_type) << ") 的行（审计 9.1）";
  }

  // 三组数据压缩后都必须仍可读（含仅存在于 L1 的 (2,105)）
  EXPECT_TRUE(HasValue(FilesOfGroup(engine, kColumnA, EntityType::Vertex),
                       CedarKey(1, EntityType::Vertex, kColumnA, Timestamp(100)),
                       101));
  EXPECT_TRUE(HasValue(FilesOfGroup(engine, kColumnA, EntityType::Vertex),
                       CedarKey(2, EntityType::Vertex, kColumnA, Timestamp(105)),
                       111));
  EXPECT_TRUE(HasValue(FilesOfGroup(engine, kColumnB, EntityType::Vertex),
                       CedarKey(4, EntityType::Vertex, kColumnB, Timestamp(100)),
                       201));
  EXPECT_TRUE(HasValue(FilesOfGroup(engine, kColumnA, EntityType::EdgeOut),
                       CedarKey::EdgeOut(5, 900, kEdgeType, Timestamp(100)),
                       301));

  // L0 应被清空，所有文件号唯一且路径存在
  EXPECT_TRUE(engine.GetLevelFiles(0).empty());
  std::set<uint64_t> numbers;
  for (const auto& meta : AllFiles(engine)) {
    EXPECT_TRUE(std::filesystem::exists(meta.path)) << meta.path;
    EXPECT_TRUE(numbers.insert(meta.file_number).second) << meta.file_number;
  }

  engine.Close();
}

// =============================================================================
// Task 1.7：FindOverlapping 过滤 + MergeZones(inputs ∪ overlaps)
//   不相关的下层文件（不同列/类型/范围）既不能被合并，也不能被删除。
// =============================================================================

TEST(CompactionTest, OverlappingFilteredByColumnAndType) {
  cedar_test::ScopedTempDir temp_dir("compaction_overlap_filter");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint16_t kColumnA = 20;
  constexpr uint16_t kColumnB = 21;
  constexpr uint16_t kEdgeType = 20;

  SizeTieredCompactionEngine engine(dir, MakeL0OnlyConfig(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  // L1：与 L0 范围重叠的列 A Vertex 文件（必须参与合并）。
  // (entity=2, ts=102) 落在 L0 的 [1,3]x[100,102] 范围内，但 L0 没有这个 key。
  ASSERT_TRUE(AddSstFile(&engine, dir, 20, 1, kColumnA, EntityType::Vertex,
                         {VertexRow(2, kColumnA, 102, 777)}));
  // L1：同范围但不同列 / 不同类型（必须原样保留）
  ASSERT_TRUE(AddSstFile(&engine, dir, 21, 1, kColumnB, EntityType::Vertex,
                         {VertexRow(50, kColumnB, 100, 888)}));
  ASSERT_TRUE(AddSstFile(&engine, dir, 22, 1, kColumnA, EntityType::EdgeOut,
                         {EdgeRow(50, 900, kEdgeType, 100, 999)}));
  // L1：同列同类型但 entity 范围不重叠（必须原样保留）
  ASSERT_TRUE(AddSstFile(&engine, dir, 23, 1, kColumnA, EntityType::Vertex,
                         {VertexRow(500, kColumnA, 100, 555)}));

  // L0：列 A Vertex 两个小文件
  ASSERT_TRUE(AddSstFile(&engine, dir, 30, 0, kColumnA, EntityType::Vertex,
                         {VertexRow(1, kColumnA, 100, 101),
                          VertexRow(2, kColumnA, 101, 102)}));
  ASSERT_TRUE(AddSstFile(&engine, dir, 31, 0, kColumnA, EntityType::Vertex,
                         {VertexRow(3, kColumnA, 102, 103)}));

  std::vector<CompactionTask> tasks;
  const Status s = RunAllCompactions(&engine, &tasks);
  ASSERT_TRUE(s.ok()) << "compaction 失败: " << s.ToString();
  ASSERT_EQ(tasks.size(), 1u);
  ASSERT_EQ(tasks[0].overlapping_files.size(), 1u)
      << "只应选中同 (column,type) 且范围重叠的文件 20";
  EXPECT_EQ(tasks[0].overlapping_files[0].file_number, 20u);

  // 文件 20 的数据通过合并保留下来
  EXPECT_TRUE(HasValue(FilesOfGroup(engine, kColumnA, EntityType::Vertex),
                       CedarKey(2, EntityType::Vertex, kColumnA, Timestamp(102)),
                       777))
      << "下层重叠文件未参与 MergeZones 就消失（审计 3.1）";

  // 不相关的三层文件必须仍在元数据里、仍在磁盘上、数据未变
  const auto all = AllFiles(engine);
  auto find_file = [&all](uint64_t number) -> const ZoneSstMeta* {
    for (const auto& meta : all) {
      if (meta.file_number == number) return &meta;
    }
    return nullptr;
  };
  for (uint64_t number : {21u, 22u, 23u}) {
    const ZoneSstMeta* meta = find_file(number);
    ASSERT_NE(meta, nullptr) << "不相关的下层文件 " << number << " 被压缩删除";
    EXPECT_TRUE(std::filesystem::exists(meta->path));
    EXPECT_EQ(meta->level, 1);
  }
  EXPECT_TRUE(HasValue(FilesOfGroup(engine, kColumnB, EntityType::Vertex),
                       CedarKey(50, EntityType::Vertex, kColumnB, Timestamp(100)),
                       888));
  EXPECT_TRUE(HasValue(FilesOfGroup(engine, kColumnA, EntityType::EdgeOut),
                       CedarKey::EdgeOut(50, 900, kEdgeType, Timestamp(100)), 999));
  EXPECT_TRUE(HasValue(FilesOfGroup(engine, kColumnA, EntityType::Vertex),
                       CedarKey(500, EntityType::Vertex, kColumnA, Timestamp(100)),
                       555));

  // 压缩后 GetFilesForEntity 与磁盘一致
  EXPECT_EQ(MetaFileNumbers(engine), DiskFileNumbers(dir));
  const auto files_for_entity_a =
      engine.GetFilesForEntity(2, kColumnA, static_cast<uint8_t>(EntityType::Vertex));
  EXPECT_FALSE(files_for_entity_a.empty());
  const auto files_for_entity_b =
      engine.GetFilesForEntity(50, kColumnB, static_cast<uint8_t>(EntityType::Vertex));
  EXPECT_FALSE(files_for_entity_b.empty());
  for (const auto* group : {&files_for_entity_a, &files_for_entity_b}) {
    for (const auto& meta : *group) {
      EXPECT_TRUE(std::filesystem::exists(meta.path));
    }
  }

  engine.Close();
}

// =============================================================================
// Task 1.7：单一 FileNumberAllocator（flush/compaction 共享，MANIFEST 持久化）
// =============================================================================

TEST(CompactionTest, SingleFileNumberAllocatorSharedAndPersisted) {
  cedar_test::ScopedTempDir temp_dir("compaction_file_numbers");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  const SizeTieredConfig config = MakeL0OnlyConfig();
  std::atomic<uint64_t> shared_counter{100};

  uint64_t expected_next = 0;
  {
    SizeTieredCompactionEngine engine(dir, config, Env::Default());
    engine.BindFileNumberAllocator(&shared_counter);
    ASSERT_TRUE(engine.Open().ok());

    // 绑定后即以共享计数器为准（不是引擎内部计数器）
    EXPECT_EQ(engine.PeekNextFileNumber(), 100u);

    // 登记文件号小的老文件不得把共享计数器拉回去（只前进不后退）
    ASSERT_TRUE(AddSstFile(&engine, dir, 1, 0, 10, EntityType::Vertex,
                           {VertexRow(1, 10, 100, 1), VertexRow(2, 10, 101, 2)}));
    ASSERT_TRUE(AddSstFile(&engine, dir, 2, 0, 10, EntityType::Vertex,
                           {VertexRow(3, 10, 102, 3)}));
    EXPECT_EQ(engine.PeekNextFileNumber(), 100u);

    std::vector<CompactionTask> tasks;
    ASSERT_TRUE(RunAllCompactions(&engine, &tasks).ok());
    ASSERT_EQ(tasks.size(), 1u);

    // 压缩输出取号 100，共享计数器推进到 101
    const auto files = AllFiles(engine);
    ASSERT_EQ(files.size(), 1u);
    EXPECT_EQ(files[0].file_number, 100u);
    EXPECT_EQ(engine.PeekNextFileNumber(), 101u);
    EXPECT_EQ(shared_counter.load(), 101u);

    // flush 路径（同一个分配器）继续取号，不会与压缩输出撞号
    const uint64_t flush_number = engine.NewFileNumber();
    EXPECT_EQ(flush_number, 101u);
    EXPECT_EQ(shared_counter.load(), 102u);
    expected_next = 102;

    // 压缩后元数据集合与磁盘集合一致（输入已删除、无孤儿文件）
    EXPECT_EQ(MetaFileNumbers(engine), DiskFileNumbers(dir));

    ASSERT_TRUE(engine.Close().ok());
  }

  // 重开：MANIFEST 里的取号水位恢复到共享计数器（不会复用已用文件号）
  std::atomic<uint64_t> reopened_counter{1};
  {
    SizeTieredCompactionEngine engine(dir, config, Env::Default());
    engine.BindFileNumberAllocator(&reopened_counter);
    ASSERT_TRUE(engine.Open().ok());
    EXPECT_GE(engine.PeekNextFileNumber(), expected_next)
        << "MANIFEST 未持久化文件号水位（审计 9.2）";
    EXPECT_GE(reopened_counter.load(), expected_next);

    // 重开后再次取号：必须是全新号码
    const uint64_t next_number = engine.NewFileNumber();
    EXPECT_GE(next_number, expected_next);
    EXPECT_TRUE(reopened_counter.load() > next_number);

    // 重开后元数据仍与磁盘一致
    EXPECT_EQ(MetaFileNumbers(engine), DiskFileNumbers(dir));
    engine.Close();
  }
}

// =============================================================================
// Task 1.7：先提交 MANIFEST 再删除输入文件；失败时保留输入、清理输出
// =============================================================================

TEST(CompactionTest, ManifestCommittedBeforeInputFilesDeleted) {
  cedar_test::ScopedTempDir temp_dir("compaction_manifest_order");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  const SizeTieredConfig config = MakeL0OnlyConfig();
  SizeTieredCompactionEngine engine(dir, config, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  ASSERT_TRUE(AddSstFile(&engine, dir, 1, 0, 10, EntityType::Vertex,
                         {VertexRow(1, 10, 100, 1), VertexRow(2, 10, 101, 2)}));
  ASSERT_TRUE(AddSstFile(&engine, dir, 2, 0, 10, EntityType::Vertex,
                         {VertexRow(3, 10, 102, 3)}));
  // L1 重叠文件：(entity=2, ts=102) 与 L0 输入范围 [1,3]x[100,102] 相交
  ASSERT_TRUE(AddSstFile(&engine, dir, 3, 1, 10, EntityType::Vertex,
                         {VertexRow(2, 10, 102, 9)}));

  std::vector<CompactionTask> tasks;
  ASSERT_TRUE(RunAllCompactions(&engine, &tasks).ok());

  // 输入文件 1/2/3 只在 MANIFEST 提交后才被删除：此刻元数据与磁盘必须完全一致，
  // 既没有“元数据还在、文件已删”，也没有“文件还在、元数据已删”的孤儿。
  EXPECT_EQ(MetaFileNumbers(engine), DiskFileNumbers(dir));
  EXPECT_TRUE(DiskFileNumbers(dir).count(1u) == 0);
  EXPECT_TRUE(DiskFileNumbers(dir).count(2u) == 0);
  EXPECT_TRUE(DiskFileNumbers(dir).count(3u) == 0);
  ASSERT_EQ(DiskFileNumbers(dir).size(), 1u);

  // MANIFEST 落盘：新开引擎从 MANIFEST 恢复的层级状态与关闭前的内存状态一致
  const std::set<uint64_t> before_close = MetaFileNumbers(engine);
  ASSERT_TRUE(engine.Close().ok());

  SizeTieredCompactionEngine reopened(dir, config, Env::Default());
  ASSERT_TRUE(reopened.Open().ok());
  EXPECT_EQ(MetaFileNumbers(reopened), before_close);
  EXPECT_EQ(MetaFileNumbers(reopened), DiskFileNumbers(dir));
  reopened.Close();
}

TEST(CompactionTest, FailedCompactionKeepsInputsAndCleansOutput) {
  cedar_test::ScopedTempDir temp_dir("compaction_failure");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  SizeTieredCompactionEngine engine(dir, MakeL0OnlyConfig(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  // 元数据存在但物理输入文件缺失：合并必然失败
  ZoneSstMeta missing_a = MetaOnly(1, 10, EntityType::Vertex, 1, 3, 100, 103);
  missing_a.level = 0;
  missing_a.path = dir + "/1.frond";
  ZoneSstMeta missing_b = MetaOnly(2, 10, EntityType::Vertex, 4, 6, 100, 103);
  missing_b.level = 0;
  missing_b.path = dir + "/2.frond";
  ASSERT_TRUE(engine.AddSSTFile(missing_a).ok());
  ASSERT_TRUE(engine.AddSSTFile(missing_b).ok());

  auto task = engine.PickNextCompaction();
  ASSERT_TRUE(task.has_value());
  const Status s = engine.ExecuteCompaction(*task);
  EXPECT_FALSE(s.ok()) << "输入文件缺失时压缩必须失败而不是吞掉输入";

  // 失败保留输入（元数据仍在），输出文件/元数据都不残留
  EXPECT_EQ(engine.GetLevelFiles(0).size(), 2u);
  EXPECT_TRUE(engine.GetLevelFiles(1).empty());
  EXPECT_TRUE(DiskFileNumbers(dir).empty()) << "失败路径留下了孤儿输出文件";

  // 释放保留（reserved）后同一组还能再次被选中（不会永久卡死）
  auto retry = engine.PickNextCompaction();
  EXPECT_TRUE(retry.has_value()) << "失败后文件仍被 reserved，调度永久卡死";

  engine.Close();
}

// =============================================================================
// Task 1.7：CompactAll 必须执行所有被选中的任务（不能丢弃 reserved 任务）
// =============================================================================

TEST(CompactionTest, CompactAllCascadesWithoutLeakingReservations) {
  cedar_test::ScopedTempDir temp_dir("compaction_compact_all");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  // 每层阈值都是 1 字节（size_ratio=1），因此一次 CompactAll 会从 L0 一路
  // 级联到最底层。任何被 Pick 选中却未执行的任务都会把文件永久留在原层。
  SizeTieredConfig config;
  config.enable_background_compaction = false;
  config.compaction_threads = 0;
  config.l0_max_size = 1;
  config.level_size_trigger_ratio = 1.0;
  config.size_ratio = 1.0;
  config.max_levels = kMaxLevels;

  SizeTieredCompactionEngine engine(dir, config, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  ASSERT_TRUE(AddSstFile(&engine, dir, 1, 0, 10, EntityType::Vertex,
                         {VertexRow(1, 10, 100, 1), VertexRow(2, 10, 101, 2)}));
  ASSERT_TRUE(AddSstFile(&engine, dir, 2, 0, 10, EntityType::Vertex,
                         {VertexRow(3, 10, 102, 3)}));

  ASSERT_TRUE(engine.CompactAll().ok());

  // 级联完成：唯一输出文件在最底层，低层全部为空
  const int bottom_level = kMaxLevels - 1;
  EXPECT_EQ(engine.GetLevelFiles(bottom_level).size(), 1u);
  for (int level = 0; level < bottom_level; ++level) {
    EXPECT_TRUE(engine.GetLevelFiles(level).empty())
        << "L" << level << " 仍有文件：CompactAll 丢弃了已保留的任务";
  }

  // 没有遗留的 reserved 文件：数据可读、文件号唯一、元数据与磁盘一致
  const auto files = AllFiles(engine);
  ASSERT_EQ(files.size(), 1u);
  EXPECT_TRUE(AllRowsMatchGroup(files[0], 10, EntityType::Vertex));
  EXPECT_TRUE(HasValue(files, CedarKey(1, EntityType::Vertex, 10, Timestamp(100)), 1));
  EXPECT_EQ(MetaFileNumbers(engine), DiskFileNumbers(dir));
  EXPECT_FALSE(engine.PickNextCompaction().has_value());

  engine.Close();
}

// =============================================================================
// Task 1.7 / 审计 9.2 端到端：flush 与 compaction 共享分配器，flush 不覆盖压缩输出
// =============================================================================

TEST(CompactionTest, FlushDoesNotOverwriteCompactionOutput) {
  cedar_test::ScopedTempDir temp_dir("compaction_flush_numbers");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  CedarOptions options;
  options.create_if_missing = true;
  options.env = Env::Default();
  // L0 阈值 1 字节触发合并；L1+ 阈值极大，避免级联。
  options.size_tiered_config.l0_max_size = 1;
  options.size_tiered_config.level_size_trigger_ratio = 1.0;
  options.size_tiered_config.size_ratio = 1e9;
  options.size_tiered_config.max_levels = kMaxLevels;
  options.size_tiered_config.enable_background_compaction = false;

  constexpr uint16_t kColumnA = 10;
  constexpr uint16_t kColumnB = 11;

  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  // 两个列各写两行并 flush：L0 产生两个文件
  ASSERT_TRUE(engine.Put(CedarKey(1, EntityType::Vertex, kColumnA, Timestamp(100)),
                         Descriptor::InlineInt(kColumnA, 101), Timestamp(1)).ok());
  ASSERT_TRUE(engine.Put(CedarKey(2, EntityType::Vertex, kColumnA, Timestamp(101)),
                         Descriptor::InlineInt(kColumnA, 102), Timestamp(2)).ok());
  ASSERT_TRUE(engine.Put(CedarKey(3, EntityType::Vertex, kColumnB, Timestamp(100)),
                         Descriptor::InlineInt(kColumnB, 201), Timestamp(3)).ok());
  ASSERT_TRUE(engine.Put(CedarKey(4, EntityType::Vertex, kColumnB, Timestamp(101)),
                         Descriptor::InlineInt(kColumnB, 202), Timestamp(4)).ok());
  ASSERT_TRUE(engine.ForceFlush().ok());

  auto* compaction = engine.GetCompactionEngine();
  ASSERT_NE(compaction, nullptr);

  // 全量合并：两个 (Vertex,column) 组各一个任务 → L1 两个输出文件
  ASSERT_TRUE(engine.CompactAll().ok());
  const auto l1_files = compaction->GetLevelFiles(1);
  ASSERT_EQ(l1_files.size(), 2u) << "应按列分组产生 2 个压缩输出";
  std::set<uint64_t> compaction_outputs;
  for (const auto& meta : l1_files) {
    EXPECT_TRUE(std::filesystem::exists(meta.path));
    compaction_outputs.insert(meta.file_number);
  }
  ASSERT_EQ(compaction_outputs.size(), 2u);

  // 审计 9.2：下一次 flush 必须取新文件号，不得覆盖压缩输出
  ASSERT_TRUE(engine.Put(CedarKey(5, EntityType::Vertex, kColumnA, Timestamp(102)),
                         Descriptor::InlineInt(kColumnA, 103), Timestamp(5)).ok());
  ASSERT_TRUE(engine.ForceFlush().ok());

  for (uint64_t number : compaction_outputs) {
    const std::string path = dir + "/" + std::to_string(number) + ".frond";
    ASSERT_TRUE(std::filesystem::exists(path)) << "压缩输出被 flush 覆盖: " << path;
    ZoneColumnarSstReader reader(path);
    EXPECT_TRUE(reader.Open().ok()) << "压缩输出被 flush 覆盖/截断: " << path;
  }

  // 再压一次让后台线程静默，然后做稳定状态断言（不会再有并发压缩）
  ASSERT_TRUE(engine.CompactAll().ok());

  // 压缩输出里的原始数据没有被后续 flush 改写
  const auto files_a = compaction->GetFilesForEntity(
      1, kColumnA, static_cast<uint8_t>(EntityType::Vertex));
  EXPECT_TRUE(HasValue(files_a, CedarKey(1, EntityType::Vertex, kColumnA, Timestamp(100)),
                       101))
      << "压缩输出被后续 flush 覆盖（审计 9.2）";
  const auto files_b = compaction->GetFilesForEntity(
      3, kColumnB, static_cast<uint8_t>(EntityType::Vertex));
  EXPECT_TRUE(HasValue(files_b, CedarKey(3, EntityType::Vertex, kColumnB, Timestamp(100)),
                       201));

  // 所有层级文件号唯一，元数据集合与磁盘一致
  std::set<uint64_t> numbers;
  for (int level = 0; level < kMaxLevels; ++level) {
    for (const auto& meta : compaction->GetLevelFiles(level)) {
      EXPECT_TRUE(numbers.insert(meta.file_number).second)
          << "文件号重复（审计 9.2）: " << meta.file_number;
      EXPECT_TRUE(std::filesystem::exists(meta.path));
    }
  }
  EXPECT_EQ(MetaFileNumbers(*compaction), DiskFileNumbers(dir));

  ASSERT_TRUE(engine.Close().ok());
}

// =============================================================================
// Task 1.7 / 审计 9.3：重开时同一文件号不得同时登记到 L0 与 L1
// =============================================================================

TEST(CompactionTest, ReopenKeepsCompactionOutputOnlyInItsLevel) {
  cedar_test::ScopedTempDir temp_dir("compaction_reopen_levels");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint16_t kColumn = 30;

  // 第一段：用压缩引擎生成一个 L1 文件并写入 MANIFEST
  {
    SizeTieredCompactionEngine engine(dir, MakeL0OnlyConfig(), Env::Default());
    ASSERT_TRUE(engine.Open().ok());
    ASSERT_TRUE(AddSstFile(&engine, dir, 1, 0, kColumn, EntityType::Vertex,
                           {VertexRow(1, kColumn, 100, 11),
                            VertexRow(2, kColumn, 101, 12)}));
    ASSERT_TRUE(AddSstFile(&engine, dir, 2, 0, kColumn, EntityType::Vertex,
                           {VertexRow(3, kColumn, 102, 13)}));
    std::vector<CompactionTask> tasks;
    ASSERT_TRUE(RunAllCompactions(&engine, &tasks).ok());
    ASSERT_EQ(tasks.size(), 1u);
    ASSERT_TRUE(engine.Close().ok());
  }

  // 第二段：LsmEngine 重开。LoadSstFiles 会把目录里所有 .frond 当成 L0，
  // 随后 compaction engine 从 MANIFEST 恢复到 L1。迁移时不得重复登记。
  CedarOptions options;
  options.create_if_missing = true;
  options.env = Env::Default();
  {
    LsmEngine engine(dir, options, Env::Default());
    ASSERT_TRUE(engine.Open().ok());

    auto* compaction_engine = engine.GetCompactionEngine();
    ASSERT_NE(compaction_engine, nullptr);

    // 找一个 .frond 文件号（压缩输出），断言它在压缩层级里只出现一次且不是 L0
    const auto disk_files = DiskFileNumbers(dir);
    ASSERT_EQ(disk_files.size(), 1u);
    const uint64_t output_number = *disk_files.begin();
    EXPECT_TRUE(compaction_engine->HasFile(output_number));

    size_t occurrences = 0;
    for (int level = 0; level < kMaxLevels; ++level) {
      for (const auto& meta : compaction_engine->GetLevelFiles(level)) {
        if (meta.file_number == output_number) {
          ++occurrences;
          EXPECT_EQ(level, 1) << "压缩输出被重复登记到错误的层级（审计 9.3）";
        }
      }
    }
    EXPECT_EQ(occurrences, 1u) << "同一文件号被登记到多个层级（审计 9.3）";

    // 查询仍能读到该文件的数据（LsmEngine 的文件索引不因去重而丢失数据）
    auto got = engine.Get(
        CedarKey(1, EntityType::Vertex, kColumn, Timestamp(100)));
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->AsInlineInt().value_or(-1), 11);

    engine.Close();
  }
}

// =============================================================================
// Task 4.1：K 路堆归并 —— 顺序 + 同 key 取舍规则
//   - 同 user key 且同 timestamp：只保留 sequence 更大的版本；
//   - 完整 key 完全相同：只保留输入文件号更大的那份；
//   - 输出按 CedarKey::Compare 严格递增。
// =============================================================================

TEST(CompactionTest, MergeHeapMatchesReferenceDedupeRules) {
  cedar_test::ScopedTempDir temp_dir("compaction_merge_heap");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint16_t kColumn = 40;
  SizeTieredCompactionEngine engine(dir, MakeL0OnlyConfig(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  constexpr int kFiles = 8;
  constexpr uint64_t kEntities = 300;

  struct RowInfo {
    CedarKey key;
    int value = 0;
    uint64_t file_number = 0;
  };
  std::vector<RowInfo> all_rows;

  uint64_t file_number = 1;
  for (int f = 0; f < kFiles; ++f) {
    std::vector<RowInfo> file_rows;
    file_rows.reserve(kEntities + 1);
    // 同一个 (entity, ts) 在 8 个文件里各有一个版本，sequence = f
    for (uint64_t e = 1; e <= kEntities; ++e) {
      const uint64_t ts = 1000 + (e % 7) * 10;
      file_rows.push_back(
          {CedarKey(e, EntityType::Vertex, kColumn, Timestamp(ts),
                    static_cast<uint16_t>(f)),
           static_cast<int>(f * 1000 + static_cast<int>(e)), file_number});
    }
    // 完整相同的 key 出现在两个文件里：保留文件号更大的
    if (f == 2 || f == 6) {
      file_rows.push_back(
          {CedarKey(900000, EntityType::Vertex, kColumn, Timestamp(5000), 3),
           f == 2 ? 222 : 666, file_number});
    }

    std::vector<std::pair<CedarKey, Descriptor>> rows;
    rows.reserve(file_rows.size());
    for (const auto& r : file_rows) {
      rows.emplace_back(r.key, Descriptor::InlineInt(kColumn, r.value));
    }
    ASSERT_TRUE(AddSstFile(&engine, dir, file_number, 0, kColumn,
                           EntityType::Vertex, rows));
    all_rows.insert(all_rows.end(), file_rows.begin(), file_rows.end());
    ++file_number;
  }

  // 参考实现：按完整 key 归并 + 两条取舍规则
  std::map<std::string, RowInfo> by_full_key;
  for (const auto& r : all_rows) {
    const std::string encoded = r.key.Encode();
    auto it = by_full_key.find(encoded);
    if (it == by_full_key.end()) {
      by_full_key.emplace(encoded, r);
      continue;
    }
    // 同 user key + 同 ts 的不同 sequence 是不同完整 key；这里先按
    // (user key, ts) 收敛，再按文件号决定完整相同的胜者。
    by_full_key.emplace(encoded, r);  // 占位（下面统一处理）
  }

  // 先按 (user key bytes, ts) 分组，再按 rule 选胜者
  struct UserTsKey {
    std::string user_key;
    uint64_t ts = 0;
    bool operator<(const UserTsKey& o) const {
      return user_key < o.user_key || (user_key == o.user_key && ts < o.ts);
    }
  };
  std::map<UserTsKey, RowInfo> winners;
  for (const auto& r : all_rows) {
    const UserTsKey key{r.key.GetUserKeyBytes(), r.key.timestamp().value()};
    auto it = winners.find(key);
    if (it == winners.end()) {
      winners.emplace(key, r);
      continue;
    }
    RowInfo& current = it->second;
    if (r.key.sequence() > current.key.sequence() ||
        (r.key.sequence() == current.key.sequence() &&
         r.file_number > current.file_number)) {
      current = r;
    }
  }

  std::vector<RowInfo> expected;
  expected.reserve(winners.size());
  for (const auto& [key, row] : winners) {
    expected.push_back(row);
  }
  std::sort(expected.begin(), expected.end(),
            [](const RowInfo& a, const RowInfo& b) { return a.key < b.key; });

  std::vector<CompactionTask> tasks;
  ASSERT_TRUE(RunAllCompactions(&engine, &tasks).ok());
  ASSERT_EQ(tasks.size(), 1u);

  const auto outputs = FilesOfGroup(engine, kColumn, EntityType::Vertex);
  ASSERT_FALSE(outputs.empty());

  std::vector<std::pair<CedarKey, Descriptor>> out_rows;
  for (const auto& meta : outputs) {
    ZoneColumnarSstReader reader(meta.path);
    ASSERT_TRUE(reader.Open().ok());
    std::unique_ptr<ZoneColumnarIterator> it(reader.NewIterator());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      out_rows.emplace_back(it->Key(), it->Value());
    }
  }

  ASSERT_EQ(out_rows.size(), expected.size())
      << "堆归并去重结果数量不符（同 ts 应只留最大 sequence，完整相同应只留一份）";
  for (size_t i = 0; i < out_rows.size(); ++i) {
    EXPECT_EQ(out_rows[i].first.Encode(), expected[i].key.Encode())
        << "第 " << i << " 行 key 顺序/取舍不符";
    EXPECT_EQ(out_rows[i].second.AsInlineInt().value_or(-1), expected[i].value)
        << "第 " << i << " 行 value 不符";
    if (i > 0) {
      EXPECT_LT(out_rows[i - 1].first, out_rows[i].first)
          << "输出未按 CedarKey::Compare 严格递增";
    }
  }

  EXPECT_TRUE(engine.GetLevelFiles(0).empty());
  EXPECT_EQ(MetaFileNumbers(engine), DiskFileNumbers(dir));
  engine.Close();
}

// =============================================================================
// Task 4.2：输出按 l0_file_size 切分；每段 ≤ 目标 + 一个 block 的字节量
// =============================================================================

TEST(CompactionTest, OutputSplitsByTargetFileSize) {
  cedar_test::ScopedTempDir temp_dir("compaction_output_split");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint16_t kColumn = 41;
  constexpr uint64_t kRowsPerFile = 20000;
  constexpr uint64_t kTargetFileSize = 64 * 1024;

  SizeTieredConfig config = MakeL0OnlyConfig();
  config.l0_file_size = kTargetFileSize;

  SizeTieredCompactionEngine engine(dir, config, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  for (int f = 0; f < 2; ++f) {
    std::vector<std::pair<CedarKey, Descriptor>> rows;
    rows.reserve(kRowsPerFile);
    for (uint64_t i = 0; i < kRowsPerFile; ++i) {
      const uint64_t entity = i * 2 + static_cast<uint64_t>(f) + 1;
      rows.emplace_back(CedarKey(entity, EntityType::Vertex, kColumn,
                                 Timestamp(1000 + i)),
                        Descriptor::InlineInt(kColumn, static_cast<int32_t>(i)));
    }
    ASSERT_TRUE(AddSstFile(&engine, dir, 1 + f, 0, kColumn, EntityType::Vertex,
                           rows));
  }

  std::vector<CompactionTask> tasks;
  ASSERT_TRUE(RunAllCompactions(&engine, &tasks).ok());
  ASSERT_EQ(tasks.size(), 1u);

  const auto outputs = FilesOfGroup(engine, kColumn, EntityType::Vertex);
  EXPECT_GE(outputs.size(), 2u) << "未按 l0_file_size 切分输出";

  uint64_t total_rows = 0;
  uint64_t total_bytes = 0;
  for (const auto& meta : outputs) {
    total_rows += meta.num_entries;
    total_bytes += meta.file_size;
  }
  EXPECT_EQ(total_rows, 2 * kRowsPerFile);
  const double bytes_per_row =
      total_rows > 0 ? static_cast<double>(total_bytes) / total_rows : 24.0;
  // 每段 ≤ 目标 + 一个 block（8192 行）的编码字节量
  const double upper_bound = static_cast<double>(kTargetFileSize) + 8192.0 * bytes_per_row;
  for (const auto& meta : outputs) {
    EXPECT_GT(meta.num_entries, 0u);
    EXPECT_LE(static_cast<double>(meta.file_size), upper_bound)
        << "分段 " << meta.file_number << " 超过 目标+一个 block: "
        << meta.file_size << " > " << upper_bound;
  }

  // 分段拼接后仍按完整 key 严格递增，且数据无丢失
  std::vector<std::pair<CedarKey, Descriptor>> all_rows;
  for (const auto& meta : outputs) {
    ZoneColumnarSstReader reader(meta.path);
    ASSERT_TRUE(reader.Open().ok());
    std::unique_ptr<ZoneColumnarIterator> it(reader.NewIterator());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      all_rows.emplace_back(it->Key(), it->Value());
    }
  }
  ASSERT_EQ(all_rows.size(), 2 * kRowsPerFile);
  for (size_t i = 1; i < all_rows.size(); ++i) {
    EXPECT_LT(all_rows[i - 1].first, all_rows[i].first)
        << "分段边界处 key 顺序不连续";
  }
  // 每个分段里的 entity 都能通过 GetFilesForEntity 找到（元数据与磁盘一致）
  for (const auto& meta : outputs) {
    ASSERT_GT(meta.num_entries, 0u);
    const auto files = engine.GetFilesForEntity(
        meta.min_entity_id, kColumn, static_cast<uint8_t>(EntityType::Vertex));
    EXPECT_FALSE(files.empty());
  }
  EXPECT_EQ(MetaFileNumbers(engine), DiskFileNumbers(dir));
  engine.Close();
}

// =============================================================================
// Task 4.2：任务按 size tier 选择（相似大小优先，不再整层按大小截断）
// =============================================================================

TEST(CompactionTest, TieredPickChoosesSimilarSizedFiles) {
  cedar_test::ScopedTempDir temp_dir("compaction_tier_pick");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint16_t kColumn = 42;
  SizeTieredCompactionEngine engine(dir, MakeL0OnlyConfig(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  auto add_rows = [&](uint64_t file_number, uint64_t rows,
                      uint64_t first_entity, int value_base) {
    std::vector<std::pair<CedarKey, Descriptor>> entries;
    entries.reserve(rows);
    for (uint64_t i = 0; i < rows; ++i) {
      const uint64_t entity = first_entity + i;
      entries.emplace_back(CedarKey(entity, EntityType::Vertex, kColumn,
                                    Timestamp(1000 + i)),
                           Descriptor::InlineInt(kColumn, value_base + static_cast<int>(i)));
    }
    return AddSstFile(&engine, dir, file_number, 0, kColumn, EntityType::Vertex,
                      entries);
  };

  // 两个小文件（100 行，entity 1..100）+ 两个大文件（4000 行，entity 1001..5000）
  // 两个 tier 的 entity 范围不重叠（避免第二次合并把第一组当作重叠文件再合并）
  ASSERT_TRUE(add_rows(1, 100, 1, 100));
  ASSERT_TRUE(add_rows(2, 100, 1, 200));
  ASSERT_TRUE(add_rows(3, 4000, 1001, 300));
  ASSERT_TRUE(add_rows(4, 4000, 1001, 400));

  auto first = engine.PickNextCompaction();
  ASSERT_TRUE(first.has_value());
  ASSERT_EQ(first->input_files.size(), 2u);
  for (const auto& meta : first->input_files) {
    EXPECT_EQ(meta.num_entries, 100u)
        << "tier 选择应优先选最小的、相似大小的两个文件（而不是混入大文件）";
  }
  ASSERT_TRUE(engine.ExecuteCompaction(*first).ok());

  auto second = engine.PickNextCompaction();
  ASSERT_TRUE(second.has_value());
  ASSERT_EQ(second->input_files.size(), 2u);
  for (const auto& meta : second->input_files) {
    EXPECT_EQ(meta.num_entries, 4000u);
  }
  ASSERT_TRUE(engine.ExecuteCompaction(*second).ok());

  EXPECT_FALSE(engine.PickNextCompaction().has_value());
  EXPECT_TRUE(engine.GetLevelFiles(0).empty());

  // 两组数据都完整保留。同一 tier 的两个文件里 key 完全相同（同一批 entity/ts），
  // 按 4.1 的规则保留文件号更大的那份（即 value_base=200 / 400 的文件）。
  const auto files = FilesOfGroup(engine, kColumn, EntityType::Vertex);
  EXPECT_TRUE(HasValue(files, CedarKey(1, EntityType::Vertex, kColumn, Timestamp(1000)),
                       200));
  EXPECT_TRUE(HasValue(files,
                       CedarKey(1001, EntityType::Vertex, kColumn, Timestamp(1000)),
                       400));
  EXPECT_TRUE(HasValue(files,
                       CedarKey(5000, EntityType::Vertex, kColumn, Timestamp(4999)),
                       400 + 3999));
  EXPECT_EQ(MetaFileNumbers(engine), DiskFileNumbers(dir));
  engine.Close();
}

// =============================================================================
// Task 4.3：并发调度无重复任务 + pending 有界 + 保留集与队列一致
// =============================================================================

TEST(CompactionTest, ConcurrentSchedulingHoldsNoDuplicateTasks) {
  cedar_test::ScopedTempDir temp_dir("compaction_concurrent_schedule");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  SizeTieredConfig config;
  config.enable_background_compaction = true;  // ScheduleCompaction 生效
  config.compaction_threads = 0;               // 没有消费者：队列可确定性检查
  config.l0_max_size = 1;
  config.level_size_trigger_ratio = 1.0;
  config.size_ratio = 1e9;
  config.max_levels = kMaxLevels;
  config.max_pending_tasks = 8;

  SizeTieredCompactionEngine engine(dir, config, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  constexpr int kGroups = 8;
  constexpr uint64_t kRowsPerFile = 50;
  uint64_t file_number = 1;
  for (int col = 0; col < kGroups; ++col) {
    for (int f = 0; f < 2; ++f) {
      std::vector<std::pair<CedarKey, Descriptor>> rows;
      rows.reserve(kRowsPerFile);
      for (uint64_t i = 0; i < kRowsPerFile; ++i) {
        const uint64_t entity = i * 2 + static_cast<uint64_t>(f) + 1;
        rows.emplace_back(CedarKey(entity, EntityType::Vertex,
                                   static_cast<uint16_t>(col), Timestamp(1000 + i)),
                          Descriptor::InlineInt(static_cast<uint16_t>(col),
                                                static_cast<int32_t>(i)));
      }
      ASSERT_TRUE(AddSstFile(&engine, dir, file_number++, 0,
                             static_cast<uint16_t>(col), EntityType::Vertex, rows));
    }
  }

  // 并发触发调度：同一文件绝不能出现在两个任务里
  std::vector<std::thread> threads;
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&engine] {
      for (int i = 0; i < 20; ++i) {
        engine.ScheduleCompaction();
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  const auto pending = engine.PendingTasksSnapshot();
  EXPECT_LE(pending.size(), config.max_pending_tasks)
      << "pending_tasks_ 未按 max_pending_tasks 有界";
  EXPECT_GT(pending.size(), 0u);

  std::set<uint64_t> reserved_in_tasks;
  for (const auto& task : pending) {
    for (const auto* files : {&task.input_files, &task.overlapping_files}) {
      for (const auto& meta : *files) {
        EXPECT_TRUE(reserved_in_tasks.insert(meta.file_number).second)
            << "文件 " << meta.file_number << " 被两个任务同时持有（重复任务）";
      }
    }
  }
  EXPECT_EQ(engine.ReservedFileCount(), reserved_in_tasks.size())
      << "保留集与 pending 任务不一致（保留泄漏）";

  // 排空队列并完成剩余组：数据完整、保留全部释放
  engine.WaitForCompactions();
  ASSERT_TRUE(engine.CompactAll().ok());
  EXPECT_EQ(engine.ReservedFileCount(), 0u);
  EXPECT_TRUE(engine.GetLevelFiles(0).empty());

  uint64_t rows_on_disk = 0;
  for (int level = 0; level < kMaxLevels; ++level) {
    for (const auto& meta : engine.GetLevelFiles(level)) {
      rows_on_disk += meta.num_entries;
    }
  }
  EXPECT_EQ(rows_on_disk, static_cast<uint64_t>(kGroups) * 2 * kRowsPerFile);
  EXPECT_EQ(MetaFileNumbers(engine), DiskFileNumbers(dir));
  engine.Close();
}

// =============================================================================
// Task 4.3：失败指数退避（失败不紧循环）+ 退避期间不调度
// =============================================================================

TEST(CompactionTest, SchedulerBacksOffAfterFailure) {
  cedar_test::ScopedTempDir temp_dir("compaction_backoff");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  SizeTieredConfig config;
  config.enable_background_compaction = true;
  config.compaction_threads = 0;  // 无消费者：手动驱动 Schedule/WaitForCompactions
  config.l0_max_size = 1;
  config.level_size_trigger_ratio = 1.0;
  config.size_ratio = 1e9;
  config.max_levels = kMaxLevels;
  config.failure_backoff_base_ms = 60;
  config.failure_backoff_max_ms = 200;

  SizeTieredCompactionEngine engine(dir, config, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  // 物理输入缺失 → ExecuteCompaction 必失败。AddSSTFile 在 L0 触发容量时会
  // 自动 ScheduleCompaction（单一调度器入口），队列里会有一个任务。
  ZoneSstMeta missing_a = MetaOnly(1, 43, EntityType::Vertex, 1, 3, 100, 103);
  missing_a.level = 0;
  missing_a.path = dir + "/1.frond";
  ZoneSstMeta missing_b = MetaOnly(2, 43, EntityType::Vertex, 4, 6, 100, 103);
  missing_b.level = 0;
  missing_b.path = dir + "/2.frond";
  ASSERT_TRUE(engine.AddSSTFile(missing_a).ok());
  ASSERT_TRUE(engine.AddSSTFile(missing_b).ok());
  EXPECT_GE(engine.PendingTaskCount(), 1u);

  // 排空（无 worker 时 WaitForCompactions 在调用线程执行）→ 失败 + 开始退避
  engine.WaitForCompactions();
  EXPECT_EQ(engine.PendingTaskCount(), 0u);
  EXPECT_EQ(engine.ConsecutiveCompactionFailures(), 1u);
  EXPECT_GT(engine.BackoffRemainingMicros(), 0u);
  EXPECT_EQ(engine.ReservedFileCount(), 0u);

  // 退避窗口内：调度不产生新任务（失败不紧循环）
  for (int i = 0; i < 5; ++i) {
    engine.ScheduleCompaction();
  }
  EXPECT_EQ(engine.PendingTaskCount(), 0u) << "退避窗口内仍然入队（会紧循环失败）";

  // 等退避窗口过期后调度 → 再次失败：连续失败计数递增，退避指数增长
  std::this_thread::sleep_for(std::chrono::milliseconds(
      static_cast<int>(config.failure_backoff_base_ms) + 20));
  EXPECT_EQ(engine.BackoffRemainingMicros(), 0u);
  engine.ScheduleCompaction();
  EXPECT_EQ(engine.PendingTaskCount(), 1u);
  engine.WaitForCompactions();
  EXPECT_EQ(engine.PendingTaskCount(), 0u);
  EXPECT_EQ(engine.ConsecutiveCompactionFailures(), 2u);
  // 第 2 次退避 = base * 2 = 120ms（> 第 1 次的 60ms）
  EXPECT_GT(engine.BackoffRemainingMicros(),
            config.failure_backoff_base_ms * 1000);
  EXPECT_EQ(engine.ReservedFileCount(), 0u);

  engine.ResetCompactionBackoff();
  EXPECT_EQ(engine.BackoffRemainingMicros(), 0u);
  EXPECT_TRUE(DiskFileNumbers(dir).empty());
  engine.Close();
}

// =============================================================================
// Task 4.4：MANIFEST 崩溃注入（半截 tmp 被忽略；损坏/非法 manifest 被拒绝）
// =============================================================================

TEST(CompactionTest, ManifestCrashInjectionAndValidation) {
  cedar_test::ScopedTempDir temp_dir("compaction_manifest_crash");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  constexpr uint16_t kColumn = 44;
  SizeTieredConfig config = MakeL0OnlyConfig();

  std::set<uint64_t> committed_meta;
  {
    SizeTieredCompactionEngine engine(dir, config, Env::Default());
    ASSERT_TRUE(engine.Open().ok());
    ASSERT_TRUE(AddSstFile(&engine, dir, 1, 0, kColumn, EntityType::Vertex,
                           {VertexRow(1, kColumn, 100, 11),
                            VertexRow(2, kColumn, 101, 12)}));
    ASSERT_TRUE(AddSstFile(&engine, dir, 2, 0, kColumn, EntityType::Vertex,
                           {VertexRow(3, kColumn, 102, 13)}));
    std::vector<CompactionTask> tasks;
    ASSERT_TRUE(RunAllCompactions(&engine, &tasks).ok());
    ASSERT_EQ(tasks.size(), 1u);
    committed_meta = MetaFileNumbers(engine);
    ASSERT_TRUE(engine.Close().ok());
  }

  // 模拟 manifest 写一半崩溃：残留半截 tmp（新命名 + 旧命名）都必须被忽略
  {
    const std::string manifest = dir + "/MANIFEST-zc";
    std::ofstream partial(manifest + ".tmp", std::ios::binary | std::ios::trunc);
    partial.write("MANI", 4);  // 半截 magic/字段
    partial.close();
    std::ofstream legacy_tmp(dir + "/zc-manifest.tmp",
                             std::ios::binary | std::ios::trunc);
    legacy_tmp.write("partial", 7);
    legacy_tmp.close();
  }

  // 重开：用已提交的 MANIFEST，忽略所有 tmp，数据/元数据与磁盘一致
  {
    SizeTieredCompactionEngine engine(dir, config, Env::Default());
    ASSERT_TRUE(engine.Open().ok()) << "存在半截 manifest tmp 时重开失败";
    EXPECT_EQ(MetaFileNumbers(engine), committed_meta);
    EXPECT_EQ(MetaFileNumbers(engine), DiskFileNumbers(dir));
    const auto files = FilesOfGroup(engine, kColumn, EntityType::Vertex);
    EXPECT_TRUE(HasValue(files, CedarKey(1, EntityType::Vertex, kColumn, Timestamp(100)),
                         11));
    engine.Close();
  }

  // 真 MANIFEST-zc 被截断：Open 必须返回错误（不崩溃、不加载半截状态）
  {
    const std::string manifest = dir + "/MANIFEST-zc";
    std::ofstream trunc(manifest, std::ios::binary | std::ios::trunc);
    trunc.write("abc", 3);
    trunc.close();

    SizeTieredCompactionEngine engine(dir, config, Env::Default());
    const Status s = engine.Open();
    EXPECT_FALSE(s.ok()) << "截断的 MANIFEST 未报错（可能加载了半截状态）";
  }

  // 层级数越界：同样必须拒绝
  {
    const std::string manifest = dir + "/MANIFEST-zc";
    std::string buf;
    uint64_t next_file = 1;
    int32_t num_levels = 999;
    buf.append(reinterpret_cast<const char*>(&next_file), sizeof(next_file));
    buf.append(reinterpret_cast<const char*>(&num_levels), sizeof(num_levels));
    std::ofstream bad(manifest, std::ios::binary | std::ios::trunc);
    bad.write(buf.data(), static_cast<std::streamsize>(buf.size()));
    bad.close();

    SizeTieredCompactionEngine engine(dir, config, Env::Default());
    const Status s = engine.Open();
    EXPECT_FALSE(s.ok()) << "越界 level count 的 MANIFEST 未被拒绝";
  }
}

// =============================================================================
// Task 4.5：flush 分组（向量分组 + 原地排序 + 每组一个文件）正确性
// =============================================================================

TEST(CompactionTest, FlushWritesOneSortedFilePerGroup) {
  cedar_test::ScopedTempDir temp_dir("flush_grouping");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  CedarOptions options;
  options.create_if_missing = true;
  options.env = Env::Default();

  constexpr uint16_t kColumns = 16;
  constexpr uint64_t kRowsPerColumn = 256;

  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());

  for (uint64_t i = 0; i < kRowsPerColumn; ++i) {
    for (uint16_t col = 0; col < kColumns; ++col) {
      const uint64_t entity = i + 1;
      ASSERT_TRUE(engine.Put(
          CedarKey(entity, EntityType::Vertex, col,
                   Timestamp(1000 + i)),
          Descriptor::InlineInt(col, static_cast<int32_t>(i * 100 + col)),
          Timestamp(i + 1)).ok());
    }
  }
  ASSERT_TRUE(engine.ForceFlush().ok());

  uint64_t file_count = 0;
  uint64_t rows_on_disk = 0;
  std::set<uint16_t> columns_seen;
  for (int level = 0; level < kMaxLevels; ++level) {
    for (const auto& meta : engine.GetFrondFiles(level)) {
      ++file_count;
      rows_on_disk += meta.num_entries;
      columns_seen.insert(meta.column_id);
      const std::string path =
          dir + "/" + std::to_string(meta.file_number) + ".frond";
      EXPECT_TRUE(std::filesystem::exists(path));
      // 每个文件的列分组正确 + 文件内行的 column 一致 + value 可读
      ZoneColumnarSstReader reader(path);
      ASSERT_TRUE(reader.Open().ok());
      std::unique_ptr<ZoneColumnarIterator> it(reader.NewIterator());
      uint64_t rows = 0;
      for (it->SeekToFirst(); it->Valid(); it->Next(), ++rows) {
        EXPECT_EQ(it->Key().column_id(), meta.column_id)
            << "flush 分组串列（列 " << meta.column_id << "）";
        EXPECT_EQ(it->Value().AsInlineInt().value_or(-1),
                  static_cast<int32_t>(it->Key().timestamp().value() - 1000) * 100 +
                      meta.column_id);
      }
      EXPECT_EQ(rows, meta.num_entries);
    }
  }
  EXPECT_EQ(file_count, static_cast<uint64_t>(kColumns));
  EXPECT_EQ(rows_on_disk, static_cast<uint64_t>(kColumns) * kRowsPerColumn);
  EXPECT_EQ(columns_seen.size(), static_cast<size_t>(kColumns));

  // 读路径仍能命中（memtable + SST 一致）
  auto got = engine.Get(CedarKey(7, EntityType::Vertex, 3, Timestamp(1006)));
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->AsInlineInt().value_or(-1), 600 + 3);

  // 关闭时会再 flush 一次空 memtable；不应产生新文件
  ASSERT_TRUE(engine.Close().ok());
  size_t after_close = 0;
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    if (entry.is_regular_file() && entry.path().extension() == ".frond") {
      ++after_close;
    }
  }
  EXPECT_EQ(after_close, static_cast<size_t>(kColumns));
}

TEST(CompactionTest, FlushWorksWithoutWal) {
  cedar_test::ScopedTempDir temp_dir("flush_without_wal");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  CedarOptions options;
  options.create_if_missing = true;
  options.env = Env::Default();
  options.enable_wal = false;  // 无 WAL 时 flush 必须走真实 fsync（仍要可读）

  LsmEngine engine(dir, options, Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  constexpr uint16_t kColumn = 45;
  for (uint64_t i = 0; i < 8; ++i) {
    ASSERT_TRUE(engine.Put(CedarKey(i + 1, EntityType::Vertex, kColumn,
                                   Timestamp(1000 + i)),
                           Descriptor::InlineInt(kColumn, static_cast<int32_t>(i)),
                           Timestamp(i + 1)).ok());
  }
  ASSERT_TRUE(engine.ForceFlush().ok());
  auto got = engine.Get(CedarKey(3, EntityType::Vertex, kColumn, Timestamp(1002)));
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->AsInlineInt().value_or(-1), 2);
  ASSERT_TRUE(engine.Close().ok());
}

}  // namespace
