// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// =============================================================================
// Zone-Columnar SST Format (Frond v2) - 文件格式定义
// =============================================================================
// 文件结构：
// ┌──────────────────────────────────────────────────────────────┐
// │ Header (256 bytes)                                           │
// ├──────────────────────────────────────────────────────────────┤
// │ Zone 0: Entity IDs (RLE 编码)                                │
// ├──────────────────────────────────────────────────────────────┤
// │ Zone 1: Timestamps (Delta-of-Delta)                          │
// ├──────────────────────────────────────────────────────────────┤
// │ Zone 2: Target IDs (Delta/RLE)                               │
// ├──────────────────────────────────────────────────────────────┤
// │ Zone 3: Values (Dictionary/LZ4)                              │
// ├──────────────────────────────────────────────────────────────┤
// │ Zone Maps (每 Zone 统计信息)                                 │
// ├──────────────────────────────────────────────────────────────┤
// │ Restart Points (稀疏索引)                                    │
// ├──────────────────────────────────────────────────────────────┤
// │ Bloom Filter (可选)                                          │
// ├──────────────────────────────────────────────────────────────┤
// │ Footer (128 bytes)                                           │
// └──────────────────────────────────────────────────────────────┘
// =============================================================================

#ifndef FERN_ZONE_COLUMNAR_FORMAT_H_
#define FERN_ZONE_COLUMNAR_FORMAT_H_

#include <cstdint>
#include <string>
#include <vector>
#include <array>

#include "cedar/core/slice.h"
#include "cedar/core/status.h"
#include "cedar/frond/bloom_filter.h"
#include "cedar/frond/zone_encoder.h"
#include "cedar/storage/block_cache.h"
#include "cedar/storage/cedar_options.h"
// WritableFile is in env.h which is included by cedar_options.h

namespace cedar {

// 前向声明：SST 专用的 Blob 管理器（定义在 zone_columnar_format.cc 中）
class SimpleSSTBlobManager;

// Zone-Columnar 文件魔数
static constexpr uint32_t kZoneColumnarMagic = 0x5A434F4C;  // "ZCOL"
static constexpr uint32_t kZoneColumnarVersion = 1;  // 版本 1

// =============================================================================
// Zone-Columnar Header (256 bytes)
// =============================================================================
#pragma pack(push, 1)
struct ZoneColumnarHeader {
  // ===== Magic & Version (8 bytes) =====
  uint32_t magic = kZoneColumnarMagic;
  uint32_t version = kZoneColumnarVersion;

  // ===== 文件元信息 (16 bytes) =====
  uint32_t flags = 0;               // 文件标志
  uint16_t column_id = 0;           // 列 ID / 边类型 ID
  uint8_t  entity_type = 0;         // 0=Vertex, 1=EdgeOut, 2=EdgeIn
  uint8_t  reserved1 = 0;
  uint32_t row_count = 0;           // 总行数
  uint32_t block_size = 65536;      // 块大小（默认 64KB）

  // ===== Zone 信息 (64 bytes = 4 zones * 16 bytes) =====
  // 每个 Zone 的编码类型和压缩类型
  struct ZoneInfo {
    uint8_t encoding_type = 0;      // 编码类型
    uint8_t compression_type = 0;   // 压缩类型
    uint16_t reserved = 0;
    uint32_t data_offset = 0;       // 数据偏移（相对于文件头）
    uint32_t data_size = 0;         // 压缩后数据大小
    uint32_t uncompressed_size = 0; // 解压后大小
  } zone0;  // EntityIds
  
  struct ZoneInfo1 {
    uint8_t encoding_type = 0;
    uint8_t compression_type = 0;
    uint16_t reserved = 0;
    uint32_t data_offset = 0;
    uint32_t data_size = 0;
    uint32_t uncompressed_size = 0;
  } zone1;  // Timestamps
  
  struct ZoneInfo2 {
    uint8_t encoding_type = 0;
    uint8_t compression_type = 0;
    uint16_t reserved = 0;
    uint32_t data_offset = 0;
    uint32_t data_size = 0;
    uint32_t uncompressed_size = 0;
  } zone2;  // TargetIds
  
  struct ZoneInfo3 {
    uint8_t encoding_type = 0;
    uint8_t compression_type = 0;
    uint16_t reserved = 0;
    uint32_t seq_rle_offset = 0;      // Sequence RLE 数据偏移
    uint32_t seq_rle_size = 0;        // Sequence RLE 大小
    uint32_t flags_bitmap_size = 0;   // Flags Bitmap 大小
  } zone3;  // Key Metadata (Sequence + Flags)
  
  struct ZoneInfo4 {
    uint8_t encoding_type = 0;
    uint8_t compression_type = 0;
    uint16_t reserved = 0;
    uint32_t data_offset = 0;         // Value 数据偏移
    uint32_t data_size = 0;           // Value 数据大小
    uint32_t uncompressed_size = 0;   // 解压后大小
  } zone4;  // Values

  // ===== Zone Maps 偏移 (16 bytes) =====
  uint32_t zone_maps_offset = 0;    // Zone Maps 起始偏移
  uint32_t zone_maps_size = 0;      // Zone Maps 大小
  uint32_t restart_points_offset = 0;  // 重启点偏移
  uint32_t restart_points_count = 0;   // 重启点数量

  // ===== Bloom Filter 偏移 (8 bytes) =====
  uint32_t bloom_filter_offset = 0;
  uint32_t bloom_filter_size = 0;

  // ===== Footer 偏移 (8 bytes) =====
  uint32_t footer_offset = 0;
  uint32_t reserved2 = 0;
  
  // ===== Entity Index 偏移 (8 bytes) - 持久化倒排索引 =====
  uint32_t entity_index_offset = 0;
  uint32_t entity_index_size = 0;

  // ===== 时间戳范围（用于快速过滤）(16 bytes) =====
  uint64_t min_timestamp = 0;
  uint64_t max_timestamp = 0;

  // ===== Entity ID 范围（用于快速过滤）(16 bytes) =====
  uint64_t min_entity_id = 0;
  uint64_t max_entity_id = 0;

  // ===== 保留字段 (80 bytes) =====
  uint8_t reserved[80] = {};

  static constexpr size_t kEncodedSize = 256;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};
static_assert(sizeof(ZoneColumnarHeader) == 256, "ZoneColumnarHeader must be 256 bytes");
#pragma pack(pop)

// =============================================================================
// Zone Map 条目 (32 bytes per zone)
// =============================================================================
#pragma pack(push, 1)
struct ZoneMapEntry {
  uint64_t min_value = 0;
  uint64_t max_value = 0;
  uint64_t count = 0;
  uint64_t distinct_count = 0;

  static constexpr size_t kEncodedSize = 32;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};
static_assert(sizeof(ZoneMapEntry) == 32, "ZoneMapEntry must be 32 bytes");
#pragma pack(pop)

// =============================================================================
// Restart Point 条目 (16 bytes)
// =============================================================================
// 每 N 行记录一个，支持二分查找
#pragma pack(push, 1)
struct ZoneRestartPoint {
  uint64_t entity_id;               // 该重启点的首个 entity_id
  uint32_t timestamp_hi;            // 时间戳高 32 位
  uint32_t row_index;               // 行索引（相对于块起始）

  static constexpr size_t kEncodedSize = 16;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};
static_assert(sizeof(ZoneRestartPoint) == 16, "ZoneRestartPoint must be 16 bytes");
#pragma pack(pop)

// =============================================================================
// Zone-Columnar Footer (128 bytes)
// =============================================================================
#pragma pack(push, 1)
struct ZoneColumnarFooter {
  // ===== 校验和 (16 bytes) =====
  uint64_t data_checksum = 0;       // 数据区 CRC64
  uint64_t header_checksum = 0;     // Header CRC64

  // ===== 统计信息 (32 bytes) =====
  uint64_t entry_count = 0;         // 总条目数
  uint64_t uncompressed_size = 0;   // 解压前总大小
  uint64_t compressed_size = 0;     // 压缩后总大小
  uint64_t index_size = 0;          // 索引总大小

  // ===== 版本链 (16 bytes) =====
  uint64_t file_number = 0;         // 当前文件号
  uint64_t prev_file_number = 0;    // 前一个文件号（版本链）

  // ===== 层级信息 (8 bytes) =====
  uint32_t level = 0;               // SST 层级
  uint32_t sequence = 0;            // 文件序列号

  // ===== 编码统计 (16 bytes) =====
  float compression_ratio = 0.0f;   // 压缩率
  uint32_t encoding_time_us = 0;    // 编码耗时（微秒）
  uint32_t reserved1 = 0;
  uint32_t reserved2 = 0;

  // ===== 保留字段 (40 bytes) =====
  uint8_t reserved[40] = {};

  static constexpr size_t kEncodedSize = 128;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};
static_assert(sizeof(ZoneColumnarFooter) == 128, "ZoneColumnarFooter must be 128 bytes");
#pragma pack(pop)

// =============================================================================
// 前向声明
// =============================================================================
class ZoneColumnarIterator;

// =============================================================================
// Zone-Columnar SST Builder (支持 Blob 存储)
// =============================================================================
class ZoneColumnarSstBuilder {
 public:
  ZoneColumnarSstBuilder(const CedarOptions& options,
                         WritableFile* file,
                         uint16_t column_id,
                         const std::string& db_path = "");
  
  ~ZoneColumnarSstBuilder();

  ZoneColumnarSstBuilder(const ZoneColumnarSstBuilder&) = delete;
  ZoneColumnarSstBuilder& operator=(const ZoneColumnarSstBuilder&) = delete;

  // 添加一个条目
  void Add(const CedarKey& key, const Descriptor& descriptor);
  
  // 添加原始数据（自动决定内联或 Blob 存储）
  void AddValue(const CedarKey& key, const Slice& raw_value);
  
  // 完成构建
  Status Finish();
  
  // 放弃构建
  void Abandon();
  
  // 获取当前文件大小
  uint64_t FileSize() const { return file_size_; }
  
  // 获取条目数
  uint64_t NumEntries() const { return num_entries_; }
  
  // 设置文件号（用于版本链）
  void SetFileNumber(uint64_t file_number) { file_number_ = file_number; }
  void SetPrevFileNumber(uint64_t file_number) { prev_file_number_ = file_number; }
  
  // 设置层级
  void SetLevel(uint32_t level) { level_ = level; }
  
  // 启用/禁用 Blob 存储
  void SetEnableBlob(bool enable) { enable_blob_ = enable; }

  // 检查状态
  bool ok() const { return status_.ok(); }
  Status status() const { return status_; }

 private:
  // 刷新当前块到文件
  Status FlushBlock();
  
  // 写入 Zone Maps
  Status WriteZoneMaps();
  
  // 写入重启点索引
  Status WriteRestartPoints();
  
  // 写入 Bloom Filter
  Status WriteBloomFilter();
  
  // OPTIMIZATION: 写入 Entity Index（持久化倒排索引）
  Status WriteEntityIndex();
  
  // 写入 Footer
  Status WriteFooter();
  
  // 计算校验和
  uint64_t CalculateCRC64(const Slice& data) const;

  WritableFile* file_;
  CedarOptions options_;
  uint16_t column_id_;
  std::string db_path_;
  
  // Header 和 Footer
  ZoneColumnarHeader header_;
  ZoneColumnarFooter footer_;
  
  // 使用 ZoneColumnarBuilder 积累数据
  ZoneColumnarBuilder zone_builder_;
  std::vector<ZoneData> current_zones_;
  
  // 缓存所有 block 的 zone 数据（确保相同 zone 的数据连续）
  std::vector<std::string> zone0_chunks_;
  std::vector<std::string> zone1_chunks_;
  std::vector<std::string> zone2_chunks_;
  std::vector<std::string> zone3_seq_rle_chunks_;
  std::vector<std::string> zone3_flags_chunks_;
  std::vector<std::string> zone4_chunks_;
  ValueZoneEncoder::EncodingType zone4_encoding_type_ = ValueZoneEncoder::EncodingType::kRaw;
  
  // 缓存原始 timestamps（用于统一编码）
  std::vector<uint64_t> all_timestamps_;
  
  // Blob 文件管理器（大值外部存储）
  std::unique_ptr<SimpleSSTBlobManager> blob_manager_;
  bool enable_blob_ = true;
  
  // 元数据
  uint64_t file_size_ = 0;
  uint64_t num_entries_ = 0;
  Status status_;
  bool closed_ = false;
  
  // 统计信息
  uint64_t min_timestamp_ = UINT64_MAX;
  uint64_t max_timestamp_ = 0;
  uint64_t min_entity_id_ = UINT64_MAX;
  uint64_t max_entity_id_ = 0;
  
  // Bloom Filter
  BloomFilter bloom_filter_;
  
  // 重启点
  std::vector<ZoneRestartPoint> restart_points_;
  static constexpr size_t kRestartInterval = 8192;
  
  // 版本链
  uint64_t file_number_ = 0;
  uint64_t prev_file_number_ = 0;
  uint32_t level_ = 0;
  
  // 第一条和最后一条 Key
  bool has_first_key_ = false;
  CedarKey first_key_;
  CedarKey last_key_;
  
  // OPTIMIZATION: 构建 Entity Index 的临时数据结构
  // 格式: entity_id -> [positions...]
  std::unordered_map<uint64_t, std::vector<uint32_t>> entity_index_builder_;
};

// =============================================================================
// Zone-Columnar SST Reader
// =============================================================================
class ZoneColumnarSstReader {
 public:
  // 从文件打开
  explicit ZoneColumnarSstReader(const std::string& file_path);
  
  // 从内存缓冲区打开（用于测试）
  ZoneColumnarSstReader(const char* data, size_t size);
  
  ~ZoneColumnarSstReader();

  ZoneColumnarSstReader(const ZoneColumnarSstReader&) = delete;
  ZoneColumnarSstReader& operator=(const ZoneColumnarSstReader&) = delete;

  // 打开并解析文件
  Status Open();
  
  // 关闭文件
  void Close();

  // ==================== 迭代器 ====================
  
  // 创建迭代器（调用者负责 delete）
  ZoneColumnarIterator* NewIterator() const;
  
  // ==================== 查询接口 ====================
  
  // 点查：获取指定 Key 的 Value
  std::optional<Descriptor> Get(const CedarKey& key) const;
  
  // 获取指定时间的最新版本
  std::optional<Descriptor> GetAtTime(uint64_t entity_id, 
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp timestamp) const;
  
  // 获取时间范围
  std::vector<std::pair<CedarKey, Descriptor>> GetRange(
      uint64_t entity_id,
      EntityType entity_type,
      uint16_t column_id,
      Timestamp start,
      Timestamp end) const;

  // ==================== 延迟物化查询 ====================
  
  // 扫描时间范围（延迟物化）
  void ScanTemporalRange(uint64_t entity_id,
                         Timestamp start,
                         Timestamp end,
                         std::function<void(const CedarKey&, const Descriptor&)> callback) const;
  
  // 批量查询（延迟物化优化）
  void BatchGet(const std::vector<CedarKey>& keys,
                std::vector<std::optional<Descriptor>>* results) const;
  
  // OPTIMIZATION: P0 - 批量时间范围查询
  // 一次扫描时间范围，返回多个 entity 的数据
  // 参数：entity_ids - 要查询的实体ID列表
  //       entity_type, column_id - 实体类型和列ID
  //       start, end - 时间范围
  // 返回：entity_id -> [(CedarKey, Descriptor)] 的映射
  std::unordered_map<uint64_t, std::vector<std::pair<CedarKey, Descriptor>>> 
  BatchGetRange(const std::vector<uint64_t>& entity_ids,
                EntityType entity_type,
                uint16_t column_id,
                Timestamp start,
                Timestamp end) const;
  
  // 获取所有 entries（用于 Compaction）
  Status GetAllEntries(std::vector<std::pair<CedarKey, Descriptor>>* entries) const;

  // ==================== Zone Map 过滤 ====================
  
  // 快速检查 SST 是否可能包含指定 Entity ID
  bool MayContainEntity(uint64_t entity_id) const;
  
  // 快速检查 SST 是否可能与时间范围有交集
  bool MayContainTimeRange(uint64_t start_ts, uint64_t end_ts) const;

  // ==================== Blob 读取支持 ====================
  
  // 读取完整的值（自动处理内联和 Blob）
  // 返回：完整数据（内联值或从 Blob 读取的大值）
  Status GetValue(size_t row_idx, std::string* out_value) const;
  
  // 批量读取 Blobs（优化顺序扫描）
  Status GetValues(const std::vector<size_t>& row_indices,
                   std::vector<std::string>* out_values) const;
  
  // 设置 SST 所在目录（用于定位 Blob 文件）
  void SetDbPath(const std::string& db_path) { db_path_ = db_path; }

  // ==================== 元数据访问 ====================
  
  const ZoneColumnarHeader& Header() const { return header_; }
  const ZoneColumnarFooter& Footer() const { return footer_; }
  
  uint64_t NumEntries() const { return header_.row_count; }
  uint64_t MinTimestamp() const { return header_.min_timestamp; }
  uint64_t MaxTimestamp() const { return header_.max_timestamp; }
  uint64_t MinEntityId() const { return header_.min_entity_id; }
  uint64_t MaxEntityId() const { return header_.max_entity_id; }
  uint16_t ColumnId() const { return header_.column_id; }
  uint8_t GetEntityType() const { return header_.entity_type; }
  
  // 友元类，允许迭代器访问私有成员
  friend class ZoneColumnarIterator;

 private:
  // 加载 Zone 数据
  Status LoadZones();
  
  // 解压 Zone 数据
  Status DecompressZone(ZoneType type, std::string* out);
  
  // 使用重启点二分查找
  size_t BinarySearchRestartPoint(uint64_t entity_id) const;
  
  // 验证文件
  Status ValidateFile();
  
  // OPTIMIZATION: 加载持久化的 Entity Index
  Status LoadEntityIndex();
  
  // OPTIMIZATION: 使用持久化索引快速查找 entity positions
  std::vector<size_t> FindEntityPositionsFromIndex(uint64_t entity_id) const;

  std::string file_path_;
  std::string db_path_;
  const char* buffer_data_ = nullptr;
  size_t buffer_size_ = 0;
  
  bool owns_buffer_ = false;
  bool opened_ = false;
  
  ZoneColumnarHeader header_;
  ZoneColumnarFooter footer_;
  std::array<ZoneMapEntry, 5> zone_maps_;
  std::vector<ZoneRestartPoint> restart_points_;
  
  // Zone 解码器
  std::optional<EntityIdZoneEncoder::Decoder> entity_decoder_;
  std::optional<TimestampZoneEncoder::Decoder> timestamp_decoder_;
  std::optional<TargetIdZoneEncoder::Decoder> target_decoder_;
  std::optional<KeyMetadataZoneEncoder::Decoder> metadata_decoder_;
  std::optional<ValueZoneEncoder::Decoder> value_decoder_;
  
  // Zone 原始数据（可能需要解压）
  std::array<std::string, 5> zone_data_;
  
  // Bloom Filter（用于快速排除不存在的 key）
  mutable BloomFilter bloom_filter_;
  
  // Blob 文件管理器（懒加载）
  mutable std::unique_ptr<SimpleSSTBlobManager> blob_manager_;
  
  // OPTIMIZATION: 持久化 Entity Index (entity_id -> [positions])
  // 格式: [num_entries:4B] [entity_id:8B] [num_positions:4B] [pos:4B...]...
  std::unordered_map<uint64_t, std::vector<uint32_t>> entity_index_;
  bool has_entity_index_ = false;
};

// =============================================================================
// Zone-Columnar 迭代器
// =============================================================================
class ZoneColumnarIterator {
 public:
  explicit ZoneColumnarIterator(const ZoneColumnarSstReader* reader);

  // 定位到第一行
  void SeekToFirst();
  
  // 定位到指定 Key
  void Seek(const CedarKey& key);
  
  // 移动到下一行
  void Next();
  
  // 是否有效
  bool Valid() const { return valid_ && current_idx_ < total_count_; }
  
  // 获取当前 Key
  CedarKey Key() const;
  
  // 获取当前 Value
  Descriptor Value() const;
  
  // 获取当前行号
  size_t RowIndex() const { return current_idx_; }

 private:
  const ZoneColumnarSstReader* reader_;
  size_t current_idx_ = 0;
  size_t total_count_ = 0;
  bool valid_ = false;
};

}  // namespace cedar

#endif  // FERN_ZONE_COLUMNAR_FORMAT_H_
