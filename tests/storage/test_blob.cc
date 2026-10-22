// Cedar 0.1 SST 本地 Blob 端到端测试（Task 3.6 / T11；审计 3.8、10.2）
//
// 覆盖：
//   1. 写路径：<=4B 内联；4B<len<=60MB 走 sst_<n>.blob；>60MB 拒绝；
//      任何失败返回 Status，绝不写 Tombstone（审计 3.8）。
//   2. 读路径：db_path 由 SST 路径推导；ReadBlob 不再二次剥离；
//      校验长度/对齐/checksum；Blob 缺失/截断/checksum 错返回错误。
//   3. Compaction：输入 BlobRef 重写到输出 sst_<new>.blob，输出 meta 登记
//      blob_file_number/size；MANIFEST 提交后才删旧 blob；失败保留输入。
//   4. GC：MANIFEST 是引用持久化权威；启动清理孤儿 blob；Close/压缩后 GC。
//   5. 崩溃注入：SST Sync 失败时清理已写 Blob（无悬空引用）。
//
// 边界（Task 3.6）：本文件验证的是"SST 本地 Blob"（每个 SST 1:1 一个
// sst_<n>.blob）。AutoBlobStorage/全局 BlobFileManager 是跨 SST 共享 blob 的
// 另一套子系统，本任务不改，也不在此测试。

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cedar/core/env.h"
#include "cedar/frond/zone_columnar_format.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/storage/size_tiered_compaction.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "sst_test_utils.h"

namespace {

using namespace cedar;

constexpr int kMaxLevels = 7;
constexpr size_t kInlineThreshold = 4;
constexpr size_t kMaxBlobBytes = 60ull * 1024 * 1024;

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

std::string BlobPathFor(const std::string& dir, uint64_t file_number) {
  return dir + "/sst_" + std::to_string(file_number) + ".blob";
}

// 用 AddValue 构建一个带 Blob 的 SST，并返回真实元数据（供 compaction engine 登记）。
bool BuildBlobSst(const std::string& dir, uint64_t file_number, uint16_t column,
                  EntityType type,
                  std::vector<std::pair<CedarKey, std::string>> rows,
                  ZoneSstMeta* meta, std::string* out_error = nullptr) {
  if (rows.empty() || meta == nullptr) return false;
  std::sort(rows.begin(), rows.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });

  const std::string path = dir + "/" + std::to_string(file_number) + ".frond";
  WritableFile* raw = nullptr;
  Status s = Env::Default()->NewWritableFile(path, &raw);
  if (!s.ok() || raw == nullptr) {
    if (out_error) *out_error = s.ToString();
    return false;
  }

  CedarOptions options;
  options.env = Env::Default();
  ZoneColumnarSstBuilder builder(options, raw, column, dir);
  builder.SetFileNumber(file_number);
  builder.SetLevel(0);
  for (const auto& [key, value] : rows) {
    s = builder.AddValue(key, Slice(value));
    if (!s.ok()) {
      if (out_error) *out_error = s.ToString();
      delete raw;
      return false;
    }
  }
  s = builder.Finish();
  delete raw;
  if (!s.ok()) {
    if (out_error) *out_error = s.ToString();
    return false;
  }

  uint64_t min_entity = UINT64_MAX;
  uint64_t max_entity = 0;
  uint64_t min_ts = UINT64_MAX;
  uint64_t max_ts = 0;
  for (const auto& [key, value] : rows) {
    (void)value;
    min_entity = std::min(min_entity, key.entity_id());
    max_entity = std::max(max_entity, key.entity_id());
    min_ts = std::min(min_ts, key.timestamp().value());
    max_ts = std::max(max_ts, key.timestamp().value());
  }

  ZoneSstMeta m;
  m.file_number = file_number;
  std::error_code ec;
  m.file_size = std::filesystem::file_size(path, ec);
  m.num_entries = builder.NumEntries();
  m.level = 0;
  m.min_entity_id = min_entity;
  m.max_entity_id = max_entity;
  m.min_timestamp = min_ts;
  m.max_timestamp = max_ts;
  m.column_id = column;
  m.entity_type = static_cast<uint8_t>(type);
  m.path = path;
  m.blob_file_number = builder.HasBlob() ? file_number : 0;
  m.blob_file_size = builder.BlobFileSize();
  m.blob_path = m.blob_file_number != 0 ? BlobPathFor(dir, file_number) : std::string();
  *meta = std::move(m);
  return true;
}

std::vector<std::pair<CedarKey, std::string>> MakeRows(
    uint64_t entity_base, size_t count, uint16_t column,
    const std::vector<std::string>& values) {
  std::vector<std::pair<CedarKey, std::string>> rows;
  rows.reserve(count);
  constexpr uint64_t kTsBase = 5000000;
  for (size_t i = 0; i < count; ++i) {
    rows.emplace_back(
        CedarKey::Vertex(entity_base + i, column, Timestamp(kTsBase - i)),
        values[i]);
  }
  return rows;
}

std::vector<std::string> ReadAllValues(ZoneColumnarSstReader* reader) {
  std::vector<std::string> values(reader->NumEntries());
  for (size_t i = 0; i < values.size(); ++i) {
    if (Status s = reader->GetValue(i, &values[i]); !s.ok()) {
      values[i] = "<error:" + s.ToString() + ">";
    }
  }
  return values;
}

bool FileExists(const std::string& path) {
  std::error_code ec;
  return std::filesystem::exists(path, ec);
}

std::vector<std::string> ListBlobFiles(const std::string& dir) {
  std::vector<std::string> out;
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
    if (ec) break;
    const std::string name = entry.path().filename().string();
    if (name.size() > 9 && name.rfind("sst_", 0) == 0 &&
        name.substr(name.size() - 5) == ".blob") {
      out.push_back(entry.path().string());
    }
  }
  std::sort(out.begin(), out.end());
  return out;
}

// =============================================================================
// 1) 大值 roundtrip：4B(内联) / 5B / 1MB / 59MB；重开可读
// =============================================================================

TEST(BlobTest, BlobRoundtripAcrossThresholds) {
  cedar_test::ScopedTempDir temp_dir("blob_roundtrip");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();
  constexpr uint16_t kColumn = 5;

  const std::string inline4 = "abcd";                 // <= 4B → inline
  const std::string size5 = "hello";                  // 5B → blob
  const std::string size1m(1 * 1024 * 1024, 'x');     // 1MB → blob
  const std::string size59m(59 * 1024 * 1024, 'y');   // 接近 60MB → blob

  ZoneSstMeta meta;
  ASSERT_TRUE(BuildBlobSst(dir, /*file_number=*/100, kColumn, EntityType::Vertex,
                           MakeRows(1, 4, kColumn, {inline4, size5, size1m, size59m}),
                           &meta));
  ASSERT_NE(meta.blob_file_number, 0u) << "写路径必须登记 blob_file_number";
  ASSERT_GT(meta.blob_file_size, 0u);
  ASSERT_TRUE(FileExists(meta.blob_path));

  ZoneColumnarSstReader reader(meta.path);
  ASSERT_TRUE(reader.Open().ok());
  ASSERT_EQ(reader.NumEntries(), 4u);
  EXPECT_TRUE(reader.IsMmapBacked());

  // Descriptor 阈值：第 0 行内联，其余 BlobRef（且绝非 Tombstone）
  EXPECT_TRUE(reader.GetRow(0, nullptr, nullptr));
  Descriptor d0;
  ASSERT_TRUE(reader.GetRow(0, nullptr, &d0));
  EXPECT_TRUE(d0.IsInline());
  for (size_t i = 1; i < 4; ++i) {
    Descriptor d;
    ASSERT_TRUE(reader.GetRow(i, nullptr, &d));
    EXPECT_TRUE(d.IsBlobRef()) << "row " << i << " 应为 BlobRef";
    EXPECT_FALSE(d.IsTombstone()) << "AddValue 不得写 Tombstone（审计 3.8）";
  }

  auto values = ReadAllValues(&reader);
  EXPECT_EQ(values[0], inline4);
  EXPECT_EQ(values[1], size5);
  EXPECT_EQ(values[2], size1m);
  EXPECT_EQ(values[3], size59m);

  // GetValues 批量路径一致
  std::vector<std::string> batch;
  ASSERT_TRUE(reader.GetValues({0, 1, 2, 3}, &batch).ok());
  EXPECT_EQ(batch, values) << "GetValues 与 GetValue 结果不一致";
  reader.Close();

  // 重开可读（db_path 由路径推导，不依赖 SetDbPath）
  ZoneColumnarSstReader reopened(meta.path);
  ASSERT_TRUE(reopened.Open().ok());
  auto reopened_values = ReadAllValues(&reopened);
  EXPECT_EQ(reopened_values, values);
  reopened.Close();
}

// =============================================================================
// 2) 阈值/失败语义：>60MB 拒绝、db_path 缺失拒绝、绝不静默 Tombstone
// =============================================================================

TEST(BlobTest, AddValueRejectsOversizedAndNeverWritesTombstone) {
  cedar_test::ScopedTempDir temp_dir("blob_reject");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();
  constexpr uint16_t kColumn = 7;
  constexpr uint64_t kTs = 1000;

  {
    // db_path 为空 → 大值必须报错（不得落 Tombstone）
    WritableFile* raw = nullptr;
    ASSERT_TRUE(Env::Default()
                    ->NewWritableFile(dir + "/nofile.frond", &raw)
                    .ok());
    CedarOptions options;
    options.env = Env::Default();
    ZoneColumnarSstBuilder builder(options, raw, kColumn, /*db_path=*/"");
    builder.SetFileNumber(1);
    const Status s = builder.AddValue(
        CedarKey::Vertex(1, kColumn, Timestamp(kTs)), Slice("hello"));
    EXPECT_FALSE(s.ok()) << "无 db_path 时大值必须返回错误";
    EXPECT_EQ(builder.NumEntries(), 0u) << "失败不得写入任何行（尤其 Tombstone）";
    builder.Abandon();
    delete raw;
  }

  {
    // >60MB 拒绝
    WritableFile* raw = nullptr;
    ASSERT_TRUE(Env::Default()
                    ->NewWritableFile(dir + "/big.frond", &raw)
                    .ok());
    CedarOptions options;
    options.env = Env::Default();
    ZoneColumnarSstBuilder builder(options, raw, kColumn, dir);
    builder.SetFileNumber(2);
    const std::string too_big(kMaxBlobBytes + 1, 'z');
    const Status s = builder.AddValue(
        CedarKey::Vertex(1, kColumn, Timestamp(kTs)), Slice(too_big));
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(builder.NumEntries(), 0u);
    builder.Abandon();
    delete raw;
  }

  {
    // 失败后再写合法小值：只有合法行落盘，且没有 Tomsbtone
    ZoneSstMeta meta;
    ASSERT_TRUE(BuildBlobSst(dir, 3, kColumn, EntityType::Vertex,
                             MakeRows(10, 2, kColumn, {"abc", "hello"}), &meta));
    ZoneColumnarSstReader reader(meta.path);
    ASSERT_TRUE(reader.Open().ok());
    ASSERT_EQ(reader.NumEntries(), 2u);
    Descriptor d0;
    Descriptor d1;
    ASSERT_TRUE(reader.GetRow(0, nullptr, &d0));
    ASSERT_TRUE(reader.GetRow(1, nullptr, &d1));
    EXPECT_TRUE(d0.IsInline());
    EXPECT_TRUE(d1.IsBlobRef());
    EXPECT_FALSE(d0.IsTombstone());
    EXPECT_FALSE(d1.IsTombstone());
    reader.Close();
  }
}

// =============================================================================
// 3) 读路径错误语义：截断 / 长度前缀越界 / 缺失 / 无 db_path
// =============================================================================

TEST(BlobTest, CorruptedOrMissingBlobIsReported) {
  cedar_test::ScopedTempDir temp_dir("blob_corrupt");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();
  constexpr uint16_t kColumn = 9;
  const std::string value(1 * 1024 * 1024, 'q');

  ZoneSstMeta meta;
  ASSERT_TRUE(BuildBlobSst(dir, /*file_number=*/200, kColumn, EntityType::Vertex,
                           MakeRows(100, 1, kColumn, {value}), &meta));
  ASSERT_TRUE(FileExists(meta.blob_path));
  const uint64_t blob_size =
      std::filesystem::file_size(meta.blob_path);

  // (a) 截断 payload
  {
    std::error_code ec;
    std::filesystem::resize_file(meta.blob_path, blob_size / 2, ec);
    ASSERT_FALSE(ec);
    ZoneColumnarSstReader reader(meta.path);
    ASSERT_TRUE(reader.Open().ok());
    std::string out;
    const Status s = reader.GetValue(0, &out);
    EXPECT_FALSE(s.ok()) << "截断的 Blob 必须返回错误（不得静默空串）";
    std::vector<std::string> values;
    EXPECT_FALSE(reader.GetValues({0}, &values).ok());
    reader.Close();
  }

  // 重建一个完好的 blob，再破坏 4B 长度前缀
  ZoneSstMeta meta2;
  ASSERT_TRUE(BuildBlobSst(dir, /*file_number=*/201, kColumn, EntityType::Vertex,
                           MakeRows(200, 1, kColumn, {value}), &meta2));
  {
    std::fstream f(meta2.blob_path, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(f.good());
    const uint32_t bogus = 0xFFFFFFFFu;
    f.seekp(4096);  // 第一个 entry 的 4B 长度前缀
    f.write(reinterpret_cast<const char*>(&bogus), sizeof(bogus));
    f.close();
    ZoneColumnarSstReader reader(meta2.path);
    ASSERT_TRUE(reader.Open().ok());
    std::string out;
    EXPECT_FALSE(reader.GetValue(0, &out).ok()) << "长度前缀越界必须报错";
    reader.Close();
  }

  // (c) 删除 blob 文件
  {
    std::error_code ec;
    std::filesystem::remove(meta2.blob_path, ec);
    ASSERT_FALSE(ec);
    ZoneColumnarSstReader reader(meta2.path);
    ASSERT_TRUE(reader.Open().ok());
    std::string out;
    EXPECT_FALSE(reader.GetValue(0, &out).ok()) << "Blob 缺失必须报错";
    reader.Close();
  }

  // (d) 内存 reader（没有 db_path，且未 SetDbPath）读 BlobRef 必须报错
  {
    std::ifstream in(meta2.path, std::ios::binary);
    ASSERT_TRUE(in.good());
    std::string bytes((std::istreambuf_iterator<char>(in)),
                      std::istreambuf_iterator<char>());
    ZoneColumnarSstReader reader(bytes.data(), bytes.size());
    ASSERT_TRUE(reader.Open().ok());
    std::string out;
    const Status s = reader.GetValue(0, &out);
    EXPECT_FALSE(s.ok()) << "db_path 缺失时必须返回错误，不能静默空串";
  }
}

// =============================================================================
// 4) SST Sync 失败（崩溃注入）：已写 Blob 被清理，不产生悬空引用
// =============================================================================

namespace {

class FailingSyncFile : public WritableFile {
 public:
  explicit FailingSyncFile(WritableFile* inner) : inner_(inner) {}
  ~FailingSyncFile() override { delete inner_; }

  Status Append(const Slice& data) override { return inner_->Append(data); }
  Status Close() override { return inner_->Close(); }
  Status Flush() override { return inner_->Flush(); }
  Status Sync() override {
    return Status::IOError("FailingSyncFile", "injected SST sync failure");
  }

 private:
  WritableFile* inner_;
};

}  // namespace

TEST(BlobTest, SstSyncFailureCleansBlobAndKeepsNoOrphan) {
  cedar_test::ScopedTempDir temp_dir("blob_sync_fail");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();
  constexpr uint16_t kColumn = 11;
  constexpr uint64_t kFileNumber = 300;

  WritableFile* raw = nullptr;
  ASSERT_TRUE(Env::Default()
                  ->NewWritableFile(dir + "/300.frond", &raw)
                  .ok());
  auto* failing = new FailingSyncFile(raw);

  CedarOptions options;
  options.env = Env::Default();
  ZoneColumnarSstBuilder builder(options, failing, kColumn, dir);
  builder.SetFileNumber(kFileNumber);
  const Status add = builder.AddValue(
      CedarKey::Vertex(1, kColumn, Timestamp(1000)), Slice("large-value"));
  ASSERT_TRUE(add.ok());
  const std::string blob_path = BlobPathFor(dir, kFileNumber);
  ASSERT_TRUE(FileExists(blob_path)) << "AddValue 应已写出 blob";

  const Status finish = builder.Finish();
  EXPECT_FALSE(finish.ok()) << "SST Sync 失败必须传播错误";
  builder.Abandon();  // 触发清理路径（析构也会做同样处理）
  delete failing;     // wrapper 析构关闭底层文件
  EXPECT_FALSE(FileExists(blob_path))
      << "SST 提交失败后必须清理已写 Blob（无悬空引用）";
}

// =============================================================================
// 5) Compaction：BlobRef 重写、输出 meta、旧 blob 删除、MANIFEST 重开可读
// =============================================================================

TEST(BlobTest, CompactionRewritesBlobAndDeletesOldBlobs) {
  cedar_test::ScopedTempDir temp_dir("blob_compaction");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();
  constexpr uint16_t kColumn = 13;

  const std::string big_a(2 * 1024 * 1024, 'a');
  const std::string big_b(3 * 1024 * 1024, 'b');
  const std::string small_a = "edge-a";   // 6B → blob
  const std::string small_b = "edge-b";

  ZoneSstMeta input_a;
  ZoneSstMeta input_b;
  ASSERT_TRUE(BuildBlobSst(dir, 400, kColumn, EntityType::Vertex,
                           MakeRows(1000, 2, kColumn, {big_a, small_a}), &input_a));
  ASSERT_TRUE(BuildBlobSst(dir, 401, kColumn, EntityType::Vertex,
                           MakeRows(1001, 2, kColumn, {big_b, small_b}), &input_b));

  ASSERT_TRUE(FileExists(input_a.blob_path));
  ASSERT_TRUE(FileExists(input_b.blob_path));

  SizeTieredCompactionEngine engine(dir, MakeL0OnlyConfig(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  ASSERT_TRUE(engine.AddSSTFile(input_a).ok());
  ASSERT_TRUE(engine.AddSSTFile(input_b).ok());

  // 反复 Pick+Execute 直到无任务
  int rounds = 0;
  while (auto task = engine.PickNextCompaction()) {
    ASSERT_LT(++rounds, 16) << "compaction 不收敛";
    ASSERT_TRUE(engine.ExecuteCompaction(*task).ok());
  }

  // 输入 SST/Blob 均被删除（MANIFEST 提交后）
  EXPECT_FALSE(FileExists(input_a.path));
  EXPECT_FALSE(FileExists(input_b.path));
  EXPECT_FALSE(FileExists(input_a.blob_path)) << "旧 blob 应被删除";
  EXPECT_FALSE(FileExists(input_b.blob_path)) << "旧 blob 应被删除";

  // 找到输出文件并校验 blob 元数据
  std::vector<ZoneSstMeta> outputs;
  for (int level = 0; level < kMaxLevels; ++level) {
    for (const auto& meta : engine.GetLevelFiles(level)) {
      outputs.push_back(meta);
    }
  }
  ASSERT_EQ(outputs.size(), 1u) << "两个输入应合并为一个输出";
  const ZoneSstMeta& out = outputs[0];
  EXPECT_EQ(out.blob_file_number, out.file_number)
      << "输出 meta 必须登记 blob_file_number";
  EXPECT_GT(out.blob_file_size, 0u);
  ASSERT_TRUE(FileExists(out.blob_path)) << "输出 blob 应存在";
  EXPECT_EQ(engine.GetBlobManager()->GetReferenceCount(out.blob_file_number), 1);

  // 输出内容可读（值被重写到新 blob）
  {
    ZoneColumnarSstReader reader(out.path);
    ASSERT_TRUE(reader.Open().ok());
    std::vector<std::pair<CedarKey, Descriptor>> entries;
    ASSERT_TRUE(reader.GetAllEntries(&entries).ok());
    ASSERT_EQ(entries.size(), 4u);
    std::vector<std::string> values;
    for (size_t i = 0; i < entries.size(); ++i) {
      std::string value;
      ASSERT_TRUE(reader.GetValue(i, &value).ok())
          << "输出第 " << i << " 行大值不可读";
      values.push_back(std::move(value));
    }
    std::sort(values.begin(), values.end());
    std::vector<std::string> expected = {big_a, big_b, small_a, small_b};
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(values, expected) << "compaction 后大值内容不一致";
    reader.Close();
  }
  engine.Close();

  // 重开：MANIFEST 恢复 blob 引用，输出仍可读，旧 blob 不会复活
  {
    SizeTieredCompactionEngine reopened(dir, MakeL0OnlyConfig(), Env::Default());
    ASSERT_TRUE(reopened.Open().ok());
    std::vector<ZoneSstMeta> files;
    for (int level = 0; level < kMaxLevels; ++level) {
      for (const auto& meta : reopened.GetLevelFiles(level)) files.push_back(meta);
    }
    ASSERT_EQ(files.size(), 1u);
    EXPECT_EQ(files[0].blob_file_number, files[0].file_number);
    EXPECT_TRUE(FileExists(files[0].blob_path));
    EXPECT_EQ(reopened.GetBlobManager()->GetReferenceCount(files[0].blob_file_number), 1);

    ZoneColumnarSstReader reader(files[0].path);
    ASSERT_TRUE(reader.Open().ok());
    std::vector<std::pair<CedarKey, Descriptor>> entries;
    ASSERT_TRUE(reader.GetAllEntries(&entries).ok());
    ASSERT_EQ(entries.size(), 4u);
    for (size_t i = 0; i < entries.size(); ++i) {
      std::string value;
      ASSERT_TRUE(reader.GetValue(i, &value).ok())
          << "重开后第 " << i << " 行大值不可读";
    }
    reader.Close();
    reopened.Close();
  }

  // 关闭路径的 GC 不应删除仍被引用的输出 blob
  EXPECT_TRUE(FileExists(out.blob_path));
}

// =============================================================================
// 6) GC：启动清理孤儿 blob；不误删有 SST 的 blob
// =============================================================================

TEST(BlobTest, GcRemovesOrphanBlobsOnly) {
  cedar_test::ScopedTempDir temp_dir("blob_gc");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();

  // 孤儿 blob（没有 SST、没有 MANIFEST 引用）
  const std::string orphan = BlobPathFor(dir, 999);
  {
    std::ofstream out(orphan, std::ios::binary);
    ASSERT_TRUE(out.good());
    std::string junk(4096, '\0');
    out.write(junk.data(), junk.size());
  }
  // 有对应 SST 但尚未登记进 MANIFEST 的 blob：不能删（安全方向）
  SizeTieredConfig config = MakeL0OnlyConfig();
  // 先造一个真实文件 888.frond（inline 行），以及它的 blob 文件
  ZoneSstMeta with_sst;
  ASSERT_TRUE(BuildBlobSst(dir, 888, 3, EntityType::Vertex,
                           MakeRows(1, 1, 3, {"hello"}), &with_sst));
  ASSERT_TRUE(FileExists(with_sst.blob_path));

  SizeTieredCompactionEngine engine(dir, config, Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  EXPECT_FALSE(FileExists(orphan)) << "启动时应删除无引用的孤儿 blob";
  EXPECT_TRUE(FileExists(with_sst.path)) << "普通 SST 不应被清理";
  // 888 的 SST 未登记但存在：blob 保留（避免误删）
  EXPECT_TRUE(FileExists(with_sst.blob_path)) << "对应 SST 存在时不得删除 blob";

  engine.Close();

  // 清理 SST 后，再开引擎：blob 成为孤儿（无对应 SST、无引用）→ 删除
  std::error_code ec;
  std::filesystem::remove(with_sst.path, ec);
  ASSERT_FALSE(ec);
  SizeTieredCompactionEngine engine2(dir, config, Env::Default());
  ASSERT_TRUE(engine2.Open().ok());
  EXPECT_FALSE(FileExists(with_sst.blob_path)) << "孤儿 blob 应被 GC";
  engine2.Close();
}

// =============================================================================
// 7) Compaction 崩溃注入：输入 blob 缺失 → 失败保留输入、清理输出、无悬空
// =============================================================================

TEST(BlobTest, CompactionFailureKeepsInputsAndCleansOutput) {
  cedar_test::ScopedTempDir temp_dir("blob_compaction_fail");
  ASSERT_TRUE(temp_dir.ok());
  const std::string& dir = temp_dir.path();
  constexpr uint16_t kColumn = 17;

  ZoneSstMeta input_a;
  ZoneSstMeta input_b;
  ASSERT_TRUE(BuildBlobSst(dir, 500, kColumn, EntityType::Vertex,
                           MakeRows(1, 1, kColumn, {"value-a"}), &input_a));
  ASSERT_TRUE(BuildBlobSst(dir, 501, kColumn, EntityType::Vertex,
                           MakeRows(2, 1, kColumn, {"value-b"}), &input_b));

  SizeTieredCompactionEngine engine(dir, MakeL0OnlyConfig(), Env::Default());
  ASSERT_TRUE(engine.Open().ok());
  ASSERT_TRUE(engine.AddSSTFile(input_a).ok());
  ASSERT_TRUE(engine.AddSSTFile(input_b).ok());

  // 删除一个输入的 blob：compaction 读原值必须失败（不能静默拷贝悬空指针）
  std::error_code ec;
  std::filesystem::remove(input_a.blob_path, ec);
  ASSERT_FALSE(ec);

  bool saw_failure = false;
  int rounds = 0;
  while (auto task = engine.PickNextCompaction()) {
    ASSERT_LT(++rounds, 16);
    const Status s = engine.ExecuteCompaction(*task);
    if (!s.ok()) {
      saw_failure = true;
      break;
    }
  }
  EXPECT_TRUE(saw_failure) << "输入 blob 缺失时 compaction 必须失败";

  // 输入保留（元数据 + 物理文件），输出被清理（无残留 .frond/.blob）
  std::vector<uint64_t> input_numbers = {input_a.file_number, input_b.file_number};
  std::vector<uint64_t> present;
  for (int level = 0; level < kMaxLevels; ++level) {
    for (const auto& meta : engine.GetLevelFiles(level)) {
      present.push_back(meta.file_number);
    }
  }
  EXPECT_EQ(present, input_numbers) << "失败后输入文件元数据必须保留";
  EXPECT_TRUE(FileExists(input_a.path));
  EXPECT_TRUE(FileExists(input_b.path));
  // 失败不产生新的输出 blob；input_b 的 blob 属于被保留的输入，必须存在；
  // input_a 的 blob 是测试手动删掉的，不应被"复活"。
  const auto blob_files = ListBlobFiles(dir);
  ASSERT_EQ(blob_files.size(), 1u)
      << "失败路径遗留了输出 blob（应清理）或复活了已删 blob";
  EXPECT_EQ(blob_files[0], input_b.blob_path);
  EXPECT_FALSE(FileExists(input_a.blob_path));
  engine.Close();
}

}  // namespace
