// Cedar 存储层测试共享工具（tests/storage/）
//
// 提供临时目录与 Zone-Columnar SST 构建/元数据工具。
// 仅使用仓库现有公开 API，不引入新依赖。

#ifndef CEDAR_TESTS_STORAGE_SST_TEST_UTILS_H_
#define CEDAR_TESTS_STORAGE_SST_TEST_UTILS_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <unistd.h>

#include "cedar/core/env.h"
#include "cedar/frond/zone_columnar_format.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/storage/size_tiered_compaction.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar_test {

using namespace cedar;

// 生成唯一的临时目录（时间戳 + pid + 序号）
inline std::string MakeTempDir(const std::string& tag) {
  static std::atomic<uint64_t> counter{0};
  const uint64_t seq = counter.fetch_add(1);
  const int64_t now = std::chrono::steady_clock::now().time_since_epoch().count();
  const std::string dir = "/tmp/cedar_test_" + tag + "_" +
                          std::to_string(::getpid()) + "_" +
                          std::to_string(now) + "_" + std::to_string(seq);
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);
  if (ec) {
    return std::string();
  }
  return dir;
}

inline void CleanupDir(const std::string& dir) {
  if (dir.empty()) return;
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

// RAII 临时目录：即使 ASSERT 提前返回（红测试）也会清理，避免 /tmp 残留。
class ScopedTempDir {
 public:
  explicit ScopedTempDir(const std::string& tag) : path_(MakeTempDir(tag)) {}
  ~ScopedTempDir() { CleanupDir(path_); }

  ScopedTempDir(const ScopedTempDir&) = delete;
  ScopedTempDir& operator=(const ScopedTempDir&) = delete;

  bool ok() const { return !path_.empty(); }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

// 写一个 Zone-Columnar SST 文件。
// rows 需按 CedarKey::Compare 排序（entity 升序；同 entity 时 timestamp 降序）。
inline bool BuildSstFile(const std::string& dir,
                         uint64_t file_number,
                         uint16_t column_id,
                         const std::vector<std::pair<CedarKey, Descriptor>>& rows,
                         std::string* out_path) {
  const std::string path = dir + "/" + std::to_string(file_number) + ".frond";
  cedar::WritableFile* file = nullptr;
  Status s = Env::Default()->NewWritableFile(path, &file);
  if (!s.ok() || file == nullptr) {
    return false;
  }

  CedarOptions options;
  options.env = Env::Default();
  ZoneColumnarSstBuilder builder(options, file, column_id, /*db_path=*/"");
  builder.SetFileNumber(file_number);
  builder.SetLevel(0);
  for (const auto& row : rows) {
    builder.Add(row.first, row.second);
  }
  s = builder.Finish();
  delete file;  // 析构时关闭文件
  if (!s.ok()) {
    return false;
  }
  *out_path = path;
  return true;
}

// 根据实际写入的行构造 ZoneSstMeta（供 SizeTieredCompactionEngine 使用）
inline ZoneSstMeta MakeSstMeta(const std::string& path,
                               uint64_t file_number,
                               int level,
                               uint16_t column_id,
                               EntityType type,
                               uint64_t min_entity_id,
                               uint64_t max_entity_id,
                               uint64_t min_ts,
                               uint64_t max_ts,
                               uint64_t num_entries) {
  ZoneSstMeta meta;
  meta.file_number = file_number;
  std::error_code ec;
  const auto size = std::filesystem::file_size(path, ec);
  meta.file_size = ec ? 0 : static_cast<uint64_t>(size);
  meta.num_entries = num_entries;
  meta.level = level;
  meta.min_entity_id = min_entity_id;
  meta.max_entity_id = max_entity_id;
  meta.min_timestamp = min_ts;
  meta.max_timestamp = max_ts;
  meta.column_id = column_id;
  meta.entity_type = static_cast<uint8_t>(type);
  meta.path = path;
  meta.blob_path = path + ".blob";
  return meta;
}

}  // namespace cedar_test

#endif  // CEDAR_TESTS_STORAGE_SST_TEST_UTILS_H_
