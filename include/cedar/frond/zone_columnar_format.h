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

#include <array>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "cedar/core/slice.h"
#include "cedar/core/status.h"
#include "cedar/frond/bloom_filter.h"
#include "cedar/frond/zone_encoder.h"
#include "cedar/frond/zone_reader_view.h"
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
// Header flags 保留位（Task 1.4，格式兼容用；不改变 v1 布局）
// =============================================================================
// 旧写端：target zone 按 block 编码（每块 1B encoding_type tag 首尾拼接），
//         header.flags == 0。
// 新写端：target zone 整文件一次编码（只有一个 1B tag），置位该标志；读端据此
//         选择文件级解码，未置位时按旧的逐块格式解码（旧文件仍可读）。
static constexpr uint32_t kFlagTargetZoneFileLevel = 1u << 0;

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

  // 添加原始数据（Task 3.6：自动决定内联或 Blob 存储）
  //   <= 4B            → InlineShortStr
  //   4B < len <= 60MB → sst_<file_number>.blob（[4B len][payload]，4KB 对齐）
  //   > 60MB           → InvalidArgument（Descriptor::BlobRef.size_kb 是 uint16）
  // 任何失败都返回错误，绝不写 Tombstone 静默降级。
  Status AddValue(const CedarKey& key, const Slice& raw_value);

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

  // Task 3.6：Blob 输出元数据（供 flush/compaction 写入 ZoneSstMeta）
  bool HasBlob() const { return blob_written_; }
  uint64_t BlobFileNumber() const { return blob_file_number_; }
  uint64_t BlobFileSize() const { return blob_file_size_; }

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

  // Task 3.6：SST 构建失败/放弃时删除本次创建的 Blob（避免孤儿 blob）
  void DiscardBlob();

  // Task 3.6：Blob 输出状态（仅当本次确实写过 blob entry 时 blob_written_=true）
  bool blob_written_ = false;
  uint64_t blob_file_number_ = 0;
  uint64_t blob_file_size_ = 0;
  // Finish 成功后置位；析构/Abandon 据此决定是否清理可能残留的 Blob 文件。
  bool finished_ok_ = false;

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
  std::vector<std::string> zone3_seq_rle_chunks_;
  std::vector<std::string> zone3_flags_chunks_;
  std::vector<std::string> zone4_chunks_;
  ValueZoneEncoder::EncodingType zone4_encoding_type_ = ValueZoneEncoder::EncodingType::kRaw;

  // 缓存原始 timestamps（用于统一编码）
  std::vector<uint64_t> all_timestamps_;

  // 缓存原始 target_ids（Task 1.4：target zone 整文件一次编码）。
  // 文件级编码固定 kRaw，不需要 entity_ids（旧格式的 per-block delta 只出现在
  // 读端兼容路径，写端不再产生）。
  std::vector<uint64_t> all_target_ids_;

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

  // Task 3.2/3.3：Bloom 的 entity 集合。行按 entity 升序写入，只在 entity
  // 变化时追加，内存 ≈ 唯一 entity 数 × 8B（不再为每行维护倒排 map）。
  std::vector<uint64_t> bloom_entity_ids_;

  // Task 3.3：持久化紧凑 Entity Index：(entity_id, first_row) 每 entity 一条，
  // 文件内 entity 连续，因此 [first_row, next_first_row) 即该 entity 的行区间。
  std::vector<std::pair<uint64_t, uint32_t>> entity_runs_;

  // 输入必须按 entity 升序（flush/compaction 保证）；若检测到乱序，则放弃写
  // 持久索引，读端回退到 entity zone 线性扫描（绝不返回错误空区间）。
  bool entity_runs_sorted_ = true;
  bool has_last_index_entity_ = false;
  uint64_t last_index_entity_ = 0;
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

  // 获取指定时间的最新版本（旧签名：Vertex 语义，不区分 target）
  std::optional<Descriptor> GetAtTime(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp timestamp) const;

  // 获取指定时间的最新版本（Task 1.5：按完整 user key 定位，边查询必须用它，
  // 否则同 (entity,type,column) 不同 target 的边会互相串读 —— 审计 2.4）
  std::optional<Descriptor> GetAtTime(uint64_t entity_id,
                                      EntityType entity_type,
                                      uint16_t column_id,
                                      uint64_t target_id,
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
  //
  // Task 3.6 边界：本节是"SST 本地 Blob"子系统（每个 SST 1:1 对应
  // sst_<file_number>.blob，只为该文件的大值服务）。AutoBlobStorage 使用的
  // 全局 BlobFileManager 是另一套"跨 SST 共享 blob"子系统，本任务不改变它；
  // 两者都用 Descriptor::ExternalRef 表示引用，禁止在同一个 (entity,column)
  // 数据源上混用。

  // 读取完整的值（自动处理内联和 Blob）。
  // Task 3.6：db_path 由 SST 路径自动推导（不再依赖调用方 SetDbPath）；
  // Blob 缺失/截断/checksum 错返回错误，绝不静默返回空串。
  Status GetValue(size_t row_idx, std::string* out_value) const;

  // Task 3.6：直接读取一个 BlobRef 指向的原始大值（compaction 重写使用）。
  // 校验 offset/size_kb 对齐、长度前缀、payload 完整性、checksum。
  Status ReadBlobValue(const Descriptor& desc, std::string* out_value) const;

  // 批量读取 Blobs（优化顺序扫描）
  Status GetValues(const std::vector<size_t>& row_indices,
                   std::vector<std::string>* out_values) const;

  // 设置 SST 所在目录（用于定位 Blob 文件）
  void SetDbPath(const std::string& db_path) { db_path_ = db_path; }

  // ==================== 完整 Key 重建 / Target 解码（Task 1.4/1.5） ====================

  // 重建第 row_idx 行的完整 32B CedarKey：
  // entity_id + header.entity_type + header.column_id + target + ts + seq + flags。
  // 旧实现（GetRange/BatchGetRange）用 CedarKey::Vertex 重建，丢掉 target/entity_type。
  // 两参重载允许用外部 header（如迁移/校验工具）重建，不读本 reader 的 header_。
  CedarKey ReconstructKey(size_t row_idx) const;
  CedarKey ReconstructKey(size_t row_idx, const ZoneColumnarHeader& header) const;

  // 第 row_idx 行的 target_id（Edge 的 dst/src 或 Vertex 的 extension）。
  // 懒解码（one-time）：新格式文件级一次线性展开，旧格式按 restart point 分块解码。
  uint64_t TargetAt(size_t row_idx) const;

  // ==================== Task 3.2/3.3：随机访问与 entity 区间 ====================

  // 按行号随机访问（完整 key + value）；越界返回 false。
  bool GetRow(size_t row_idx, CedarKey* key, Descriptor* value) const;

  // entity 在本文件中的行区间 [first, last+1)；不存在返回 {0, 0}。
  // 有持久化 Entity Index 时 O(log n)，否则回退到 entity zone 线性扫描（结果一致）。
  std::pair<size_t, size_t> FindEntityRowRange(uint64_t entity_id) const;

  // 元数据开关（测试/诊断）
  bool HasEntityIndex() const { return has_entity_index_; }
  bool HasBloomFilter() const { return bloom_filter_.NumKeys() > 0; }
  bool IsMmapBacked() const { return view_.mapped(); }

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

  // Task 3.6：确保 Blob 读管理器已打开（db_path 由 SST 路径推导；缺失则报错）
  Status EnsureBlobManagerForRead() const;
  uint64_t BlobFileNumberForRead() const;

  // 从 SST 路径推导 db 目录（去掉文件名；用于定位 sst_<n>.blob）
  static std::string DeriveDbPath(const std::string& sst_path);
  // 从 SST 路径解析文件号（<file_number>.frond；失败返回 0）
  static uint64_t ParseFileNumberFromPath(const std::string& sst_path);

  // 解压 Zone 数据
  Status DecompressZone(ZoneType type, std::string* out);

  // Task 1.4/1.5 + 3.2：把 target zone 展开成行对齐的 vector（懒加载，one-time）。
  //   新格式（flags & kFlagTargetZoneFileLevel）：文件级单条流；
  //   旧格式：按 restart point 切块，每块一个 1B encoding_type tag（兼容旧文件）。
  void EnsureTargetValues() const;
  void DecodeTargetZone() const;

  // 用已知的 entity_id 与行内其它 zone 拼完整 key（GetRange/Iterator 快路径，
  // 不再逐行解码 entity zone）。
  CedarKey MakeKeyForRow(size_t row_idx, uint64_t entity_id) const;

  // GetAtTime 的公共实现；target 为空表示旧签名（Vertex 语义，不区分 target）。
  std::optional<Descriptor> GetAtTimeImpl(uint64_t entity_id,
                                          EntityType entity_type,
                                          uint16_t column_id,
                                          const std::optional<uint64_t>& target_id,
                                          Timestamp timestamp) const;

  // 使用重启点二分查找
  size_t BinarySearchRestartPoint(uint64_t entity_id) const;

  // 验证文件
  Status ValidateFile();

  // Task 3.3：加载持久化的紧凑 Entity Index（零拷贝视图）
  Status LoadEntityIndex();

  // Task 3.3：使用持久化索引快速查找 entity positions；无索引返回空。
  std::vector<size_t> FindEntityPositionsFromIndex(uint64_t entity_id) const;

  std::string file_path_;
  // Task 3.6：默认由 file_path_ 的目录推导；SetDbPath 可覆盖。
  std::string db_path_;
  // Task 3.6：blob 文件名中的 sst 号（从路径推导，footer 兜底）
  uint64_t blob_file_number_ = 0;
  const char* buffer_data_ = nullptr;
  size_t buffer_size_ = 0;

  bool owns_buffer_ = false;
  bool opened_ = false;

  // Task 3.1：文件只读视图（mmap；回退 pread 到堆）
  ZoneReaderView view_;

  ZoneColumnarHeader header_;
  ZoneColumnarFooter footer_;
  std::array<ZoneMapEntry, 5> zone_maps_;
  std::vector<ZoneRestartPoint> restart_points_;

  // Zone 解码器
  std::optional<EntityIdZoneEncoder::Decoder> entity_decoder_;
  std::optional<TimestampZoneEncoder::Decoder> timestamp_decoder_;
  std::optional<KeyMetadataZoneEncoder::Decoder> metadata_decoder_;
  std::optional<ValueZoneEncoder::Decoder> value_decoder_;

  // Zone 原始数据：mmap 视图上的 Slice（Task 3.1，零拷贝；不再复制整份 zone）
  std::array<Slice, 5> zone_data_;

  // 行对齐的 target_id（懒解码，Task 1.4/3.2；one-time，O(n) 内存换掉逐行 O(n) 扫描）
  mutable std::once_flag target_once_;
  mutable std::vector<uint64_t> target_values_;

  // Bloom Filter（用于快速排除不存在的 key；Task 3.3 写入真实 bitmap，读端零拷贝视图）
  mutable BloomFilter bloom_filter_;

  // Blob 文件管理器（懒加载）
  mutable std::unique_ptr<SimpleSSTBlobManager> blob_manager_;

  // Task 3.3：持久化紧凑 Entity Index 的零拷贝视图：
  //   [magic:4B="EIDX"][count:4B][entity_id:8B][first_row:4B] * count
  Slice entity_index_slice_;
  uint32_t entity_index_count_ = 0;
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

  // Task 3.2：顺序访问 entity zone 的独立游标（每行 O(1)，不修改 reader 共享状态）。
  // Key() 是 const：游标为 mutable（迭代器本身不是线程共享对象）。
  mutable std::unique_ptr<EntityIdZoneEncoder::Decoder::Cursor> entity_cursor_;
  mutable uint64_t entity_value_ = 0;
};

}  // namespace cedar

#endif  // FERN_ZONE_COLUMNAR_FORMAT_H_
