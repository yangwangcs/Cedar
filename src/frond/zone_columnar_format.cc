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

#include "cedar/frond/zone_columnar_format.h"

#include <cstring>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <unordered_set>
#include <unordered_map>

#include "cedar/core/crc32c.h"
#include "cedar/frond/blob_file.h"
#include "cedar/types/descriptor.h"

#include <optional>

namespace cedar {

// =============================================================================
// SimpleSSTBlobManager - 每个 SST 对应一个 Blob 文件 (1:1 映射)
// =============================================================================
class SimpleSSTBlobManager {
 public:
  SimpleSSTBlobManager(const std::string& db_path, uint32_t sst_id)
      : db_path_(db_path), sst_id_(sst_id) {}
  
  ~SimpleSSTBlobManager() { Close(); }

  Status OpenForWrite() {
    if (writer_) return Status::OK();
    writer_ = std::make_unique<BlobFileWriter>(GetBlobPath(), sst_id_);
    return writer_->Open();
  }
  
  Status OpenForRead() {
    if (reader_) return Status::OK();
    reader_ = std::make_unique<BlobFileReader>(GetBlobPath());
    return reader_->Open();
  }
  
  Status Close() {
    if (writer_) {
      writer_->Close();
      writer_.reset();
    }
    if (reader_) {
      reader_->Close();
      reader_.reset();
    }
    return Status::OK();
  }
  
  std::optional<Descriptor::BlobRef> WriteBlob(const Slice& data) {
    if (!writer_) return std::nullopt;
    if (data.size() <= 6) return std::nullopt;  // 小值内联
    
    uint32_t offset = 0;
    Status s = writer_->Append(data, &offset);
    if (!s.ok()) return std::nullopt;
    
    uint32_t aligned_size = ((4 + data.size() + 4095) / 4096) * 4096;
    uint16_t size_kb = static_cast<uint16_t>(aligned_size / 1024);
    uint8_t checksum = ComputeChecksum(data);
    
    return Descriptor::BlobRef{offset, size_kb, checksum};
  }
  
  Status ReadBlob(uint32_t offset, uint16_t size_kb, std::string* out_data) {
    if (!reader_) return Status::IOError("SimpleSSTBlobManager", "not opened for read");
    
    uint32_t read_size = size_kb * 1024;
    std::vector<char> buffer(read_size);
    
    Status s = reader_->Read(offset, read_size, buffer.data());
    if (!s.ok()) return s;
    
    uint32_t actual_size = *reinterpret_cast<uint32_t*>(buffer.data());
    out_data->assign(buffer.data() + 4, actual_size);
    return Status::OK();
  }
  
  Status ReadBlobs(const std::vector<std::pair<uint32_t, uint16_t>>& offsets_sizes,
                   std::vector<std::string>* out_datas) {
    if (!reader_) return Status::IOError("SimpleSSTBlobManager", "not opened for read");
    
    std::vector<uint32_t> offsets;
    offsets.reserve(offsets_sizes.size());
    for (const auto& [offset, _] : offsets_sizes) {
      offsets.push_back(offset);
    }
    reader_->Prefetch(offsets);
    
    out_datas->clear();
    out_datas->reserve(offsets_sizes.size());
    
    for (const auto& [offset, size_kb] : offsets_sizes) {
      std::string data;
      Status s = ReadBlob(offset, size_kb, &data);
      if (!s.ok()) return s;
      out_datas->push_back(std::move(data));
    }
    return Status::OK();
  }
  
  std::string GetBlobPath() const {
    return db_path_ + "/sst_" + std::to_string(sst_id_) + ".blob";
  }

 private:
  static uint8_t ComputeChecksum(const Slice& data) {
    uint8_t checksum = 0;
    for (size_t i = 0; i < data.size(); ++i) {
      checksum ^= static_cast<uint8_t>(data[i]);
      checksum = (checksum << 1) | (checksum >> 7);
    }
    return checksum;
  }

  std::string db_path_;
  uint32_t sst_id_;
  std::unique_ptr<BlobFileWriter> writer_;
  std::unique_ptr<BlobFileReader> reader_;
};

// =============================================================================
// Helper functions for encoding/decoding
// =============================================================================

static void EncodeFixed16(char* buf, uint16_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
}

static void EncodeFixed32(char* buf, uint32_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
}

static void EncodeFixed64(char* buf, uint64_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
  buf[4] = (value >> 32) & 0xff;
  buf[5] = (value >> 40) & 0xff;
  buf[6] = (value >> 48) & 0xff;
  buf[7] = (value >> 56) & 0xff;
}

static uint16_t DecodeFixed16(const char* ptr) {
  return static_cast<uint16_t>(static_cast<unsigned char>(ptr[0]))
       | (static_cast<uint16_t>(static_cast<unsigned char>(ptr[1])) << 8);
}

static uint32_t DecodeFixed32(const char* ptr) {
  return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
      | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
      | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
      | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
}

static uint64_t DecodeFixed64(const char* ptr) {
  uint64_t lo = DecodeFixed32(ptr);
  uint64_t hi = DecodeFixed32(ptr + 4);
  return (hi << 32) | lo;
}

// =============================================================================
// ZoneColumnarHeader 实现
// =============================================================================

void ZoneColumnarHeader::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  size_t pos = 0;

  // Magic & Version (8 bytes)
  EncodeFixed32(buf + pos, magic);
  pos += 4;
  EncodeFixed32(buf + pos, version);
  pos += 4;

  // 文件元信息 (16 bytes)
  EncodeFixed32(buf + pos, flags);
  pos += 4;
  EncodeFixed16(buf + pos, column_id);
  pos += 2;
  buf[pos++] = entity_type;
  buf[pos++] = reserved1;
  EncodeFixed32(buf + pos, row_count);
  pos += 4;
  EncodeFixed32(buf + pos, block_size);
  pos += 4;

  // Zone 0: EntityIds (16 bytes)
  buf[pos++] = zone0.encoding_type;
  buf[pos++] = zone0.compression_type;
  EncodeFixed16(buf + pos, zone0.reserved);
  pos += 2;
  EncodeFixed32(buf + pos, zone0.data_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone0.data_size);
  pos += 4;
  EncodeFixed32(buf + pos, zone0.uncompressed_size);
  pos += 4;

  // Zone 1: Timestamps (16 bytes)
  buf[pos++] = zone1.encoding_type;
  buf[pos++] = zone1.compression_type;
  EncodeFixed16(buf + pos, zone1.reserved);
  pos += 2;
  EncodeFixed32(buf + pos, zone1.data_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone1.data_size);
  pos += 4;
  EncodeFixed32(buf + pos, zone1.uncompressed_size);
  pos += 4;

  // Zone 2: TargetIds (16 bytes)
  buf[pos++] = zone2.encoding_type;
  buf[pos++] = zone2.compression_type;
  EncodeFixed16(buf + pos, zone2.reserved);
  pos += 2;
  EncodeFixed32(buf + pos, zone2.data_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone2.data_size);
  pos += 4;
  EncodeFixed32(buf + pos, zone2.uncompressed_size);
  pos += 4;

  // Zone 3: Key Metadata (16 bytes)
  buf[pos++] = zone3.encoding_type;
  buf[pos++] = zone3.compression_type;
  EncodeFixed16(buf + pos, zone3.reserved);
  pos += 2;
  EncodeFixed32(buf + pos, zone3.seq_rle_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone3.seq_rle_size);
  pos += 4;
  EncodeFixed32(buf + pos, zone3.flags_bitmap_size);
  pos += 4;

  // Zone 4: Values (16 bytes)
  buf[pos++] = zone4.encoding_type;
  buf[pos++] = zone4.compression_type;
  EncodeFixed16(buf + pos, zone4.reserved);
  pos += 2;
  EncodeFixed32(buf + pos, zone4.data_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone4.data_size);
  pos += 4;
  EncodeFixed32(buf + pos, zone4.uncompressed_size);
  pos += 4;

  // Zone Maps 偏移 (16 bytes)
  EncodeFixed32(buf + pos, zone_maps_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone_maps_size);
  pos += 4;
  EncodeFixed32(buf + pos, restart_points_offset);
  pos += 4;
  EncodeFixed32(buf + pos, restart_points_count);
  pos += 4;

  // Bloom Filter 偏移 (8 bytes)
  EncodeFixed32(buf + pos, bloom_filter_offset);
  pos += 4;
  EncodeFixed32(buf + pos, bloom_filter_size);
  pos += 4;

  // Footer 偏移 (8 bytes)
  EncodeFixed32(buf + pos, footer_offset);
  pos += 4;
  EncodeFixed32(buf + pos, reserved2);
  pos += 4;
  
  // Entity Index 偏移 (8 bytes) - OPTIMIZATION: 持久化倒排索引
  EncodeFixed32(buf + pos, entity_index_offset);
  pos += 4;
  EncodeFixed32(buf + pos, entity_index_size);
  pos += 4;

  // 时间戳范围 (16 bytes)
  EncodeFixed64(buf + pos, min_timestamp);
  pos += 8;
  EncodeFixed64(buf + pos, max_timestamp);
  pos += 8;

  // Entity ID 范围 (16 bytes)
  EncodeFixed64(buf + pos, min_entity_id);
  pos += 8;
  EncodeFixed64(buf + pos, max_entity_id);
  pos += 8;

  // 保留字段 (80 bytes)
  memset(buf + pos, 0, 80);
  pos += 80;

  dst->append(buf, kEncodedSize);
}

Status ZoneColumnarHeader::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("ZoneColumnarHeader", "truncated header");
  }

  const char* p = input->data();
  size_t pos = 0;

  magic = DecodeFixed32(p + pos);
  pos += 4;
  version = DecodeFixed32(p + pos);
  pos += 4;
  flags = DecodeFixed32(p + pos);
  pos += 4;
  column_id = DecodeFixed16(p + pos);
  pos += 2;
  entity_type = p[pos++];
  reserved1 = p[pos++];
  row_count = DecodeFixed32(p + pos);
  pos += 4;
  block_size = DecodeFixed32(p + pos);
  pos += 4;

  // Zone 0
  zone0.encoding_type = p[pos++];
  zone0.compression_type = p[pos++];
  zone0.reserved = DecodeFixed16(p + pos);
  pos += 2;
  zone0.data_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone0.data_size = DecodeFixed32(p + pos);
  pos += 4;
  zone0.uncompressed_size = DecodeFixed32(p + pos);
  pos += 4;

  // Zone 1
  zone1.encoding_type = p[pos++];
  zone1.compression_type = p[pos++];
  zone1.reserved = DecodeFixed16(p + pos);
  pos += 2;
  zone1.data_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone1.data_size = DecodeFixed32(p + pos);
  pos += 4;
  zone1.uncompressed_size = DecodeFixed32(p + pos);
  pos += 4;

  // Zone 2
  zone2.encoding_type = p[pos++];
  zone2.compression_type = p[pos++];
  zone2.reserved = DecodeFixed16(p + pos);
  pos += 2;
  zone2.data_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone2.data_size = DecodeFixed32(p + pos);
  pos += 4;
  zone2.uncompressed_size = DecodeFixed32(p + pos);
  pos += 4;

  // Zone 3: Key Metadata
  zone3.encoding_type = p[pos++];
  zone3.compression_type = p[pos++];
  zone3.reserved = DecodeFixed16(p + pos);
  pos += 2;
  zone3.seq_rle_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone3.seq_rle_size = DecodeFixed32(p + pos);
  pos += 4;
  zone3.flags_bitmap_size = DecodeFixed32(p + pos);
  pos += 4;

  // Zone 4: Values
  zone4.encoding_type = p[pos++];
  zone4.compression_type = p[pos++];
  zone4.reserved = DecodeFixed16(p + pos);
  pos += 2;
  zone4.data_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone4.data_size = DecodeFixed32(p + pos);
  pos += 4;
  zone4.uncompressed_size = DecodeFixed32(p + pos);
  pos += 4;

  zone_maps_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone_maps_size = DecodeFixed32(p + pos);
  pos += 4;
  restart_points_offset = DecodeFixed32(p + pos);
  pos += 4;
  restart_points_count = DecodeFixed32(p + pos);
  pos += 4;

  bloom_filter_offset = DecodeFixed32(p + pos);
  pos += 4;
  bloom_filter_size = DecodeFixed32(p + pos);
  pos += 4;

  footer_offset = DecodeFixed32(p + pos);
  pos += 4;
  reserved2 = DecodeFixed32(p + pos);
  pos += 4;
  
  // Entity Index 偏移 - OPTIMIZATION
  entity_index_offset = DecodeFixed32(p + pos);
  pos += 4;
  entity_index_size = DecodeFixed32(p + pos);
  pos += 4;

  min_timestamp = DecodeFixed64(p + pos);
  pos += 8;
  max_timestamp = DecodeFixed64(p + pos);
  pos += 8;

  min_entity_id = DecodeFixed64(p + pos);
  pos += 8;
  max_entity_id = DecodeFixed64(p + pos);
  pos += 8;

  pos += 80;  // Skip reserved (80 bytes)

  input->remove_prefix(kEncodedSize);

  if (magic != kZoneColumnarMagic) {
    return Status::Corruption("ZoneColumnarHeader", "invalid magic number");
  }

  return Status::OK();
}

// =============================================================================
// ZoneMapEntry 实现
// =============================================================================

void ZoneMapEntry::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  EncodeFixed64(buf, min_value);
  EncodeFixed64(buf + 8, max_value);
  EncodeFixed64(buf + 16, count);
  EncodeFixed64(buf + 24, distinct_count);
  dst->append(buf, kEncodedSize);
}

Status ZoneMapEntry::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("ZoneMapEntry", "truncated");
  }

  const char* p = input->data();
  min_value = DecodeFixed64(p);
  max_value = DecodeFixed64(p + 8);
  count = DecodeFixed64(p + 16);
  distinct_count = DecodeFixed64(p + 24);

  input->remove_prefix(kEncodedSize);
  return Status::OK();
}

// =============================================================================
// ZoneRestartPoint 实现
// =============================================================================

void ZoneRestartPoint::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  
  EncodeFixed64(buf, entity_id);
  EncodeFixed32(buf + 8, timestamp_hi);
  EncodeFixed32(buf + 12, row_index);
  
  dst->append(buf, kEncodedSize);
}

Status ZoneRestartPoint::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("ZoneRestartPoint", "truncated");
  }

  const char* p = input->data();
  entity_id = DecodeFixed64(p);
  timestamp_hi = DecodeFixed32(p + 8);
  row_index = DecodeFixed32(p + 12);

  input->remove_prefix(kEncodedSize);
  return Status::OK();
}

// =============================================================================
// ZoneColumnarFooter 实现
// =============================================================================

void ZoneColumnarFooter::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  size_t pos = 0;

  EncodeFixed64(buf + pos, data_checksum);
  pos += 8;
  EncodeFixed64(buf + pos, header_checksum);
  pos += 8;

  EncodeFixed64(buf + pos, entry_count);
  pos += 8;
  EncodeFixed64(buf + pos, uncompressed_size);
  pos += 8;
  EncodeFixed64(buf + pos, compressed_size);
  pos += 8;
  EncodeFixed64(buf + pos, index_size);
  pos += 8;

  EncodeFixed64(buf + pos, file_number);
  pos += 8;
  EncodeFixed64(buf + pos, prev_file_number);
  pos += 8;

  EncodeFixed32(buf + pos, level);
  pos += 4;
  EncodeFixed32(buf + pos, sequence);
  pos += 4;

  memcpy(buf + pos, &compression_ratio, sizeof(float));
  pos += 4;
  EncodeFixed32(buf + pos, encoding_time_us);
  pos += 4;
  EncodeFixed32(buf + pos, reserved1);
  pos += 4;
  EncodeFixed32(buf + pos, reserved2);
  pos += 4;

  memset(buf + pos, 0, 40);
  pos += 40;

  dst->append(buf, kEncodedSize);
}

Status ZoneColumnarFooter::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("ZoneColumnarFooter", "truncated footer");
  }

  const char* p = input->data();
  size_t pos = 0;

  data_checksum = DecodeFixed64(p + pos);
  pos += 8;
  header_checksum = DecodeFixed64(p + pos);
  pos += 8;

  entry_count = DecodeFixed64(p + pos);
  pos += 8;
  uncompressed_size = DecodeFixed64(p + pos);
  pos += 8;
  compressed_size = DecodeFixed64(p + pos);
  pos += 8;
  index_size = DecodeFixed64(p + pos);
  pos += 8;

  file_number = DecodeFixed64(p + pos);
  pos += 8;
  prev_file_number = DecodeFixed64(p + pos);
  pos += 8;

  level = DecodeFixed32(p + pos);
  pos += 4;
  sequence = DecodeFixed32(p + pos);
  pos += 4;

  memcpy(&compression_ratio, p + pos, sizeof(float));
  pos += 4;
  encoding_time_us = DecodeFixed32(p + pos);
  pos += 4;
  reserved1 = DecodeFixed32(p + pos);
  pos += 4;
  reserved2 = DecodeFixed32(p + pos);
  pos += 4;

  pos += 40;  // Skip reserved

  input->remove_prefix(kEncodedSize);
  return Status::OK();
}

// =============================================================================
// ZoneColumnarSstBuilder 实现
// =============================================================================

ZoneColumnarSstBuilder::ZoneColumnarSstBuilder(const CedarOptions& options,
                                               WritableFile* file,
                                               uint16_t column_id,
                                               const std::string& db_path)
    : file_(file),
      options_(options),
      column_id_(column_id),
      db_path_(db_path),
      bloom_filter_(10),
      enable_blob_(!db_path.empty()) {
  // 如果指定了 db_path，初始化 BlobFileManager
  if (enable_blob_ && !db_path_.empty()) {
    // 注意：file_number_ 需要在 SetFileNumber 后才能确定 SST ID
    // 暂时延迟初始化到 AddValue 或 Finish
  }
}

ZoneColumnarSstBuilder::~ZoneColumnarSstBuilder() {
  if (!closed_) {
    Abandon();
  }
}

void ZoneColumnarSstBuilder::Add(const CedarKey& key, const Descriptor& descriptor) {
  if (closed_) return;
  
  if (!has_first_key_) {
    first_key_ = key;
    has_first_key_ = true;
    // Set entity_type from first key
    header_.entity_type = static_cast<uint8_t>(key.entity_type());
  }
  last_key_ = key;
  
  // 更新统计信息
  min_timestamp_ = std::min(min_timestamp_, key.timestamp().value());
  max_timestamp_ = std::max(max_timestamp_, key.timestamp().value());
  min_entity_id_ = std::min(min_entity_id_, key.entity_id());
  max_entity_id_ = std::max(max_entity_id_, key.entity_id());
  
  // 添加到 Bloom Filter
  bloom_filter_.Add(key.entity_id());
  
  // OPTIMIZATION: 构建 Entity Index
  // 使用当前 zone_builder 的计数作为位置索引
  entity_index_builder_[key.entity_id()].push_back(
      static_cast<uint32_t>(num_entries_));
  
  // 添加到 Zone Builder
  zone_builder_.Add(key, descriptor);
  num_entries_++;
  
  // 检查是否需要刷新块
  if (zone_builder_.Count() >= kRestartInterval) {
    FlushBlock();
  }
}

void ZoneColumnarSstBuilder::AddValue(const CedarKey& key, const Slice& raw_value) {
  if (closed_) return;
  
  Descriptor desc;
  
  if (raw_value.size() <= 6) {
    // 小值内联存储
    // 使用 InlineShortStr 或内联整数
    auto opt_desc = Descriptor::InlineShortStr(column_id_, raw_value);
    if (opt_desc.has_value()) {
      desc = *opt_desc;
    } else {
      desc = Descriptor();  // Fallback to empty/tombstone
    }
  } else if (enable_blob_ && !db_path_.empty() && file_number_ > 0) {
    // 大值存储到 Blob 文件
    if (!blob_manager_) {
      blob_manager_ = std::make_unique<SimpleSSTBlobManager>(db_path_, file_number_);
      Status s = blob_manager_->OpenForWrite();
      if (!s.ok()) {
        // Blob 初始化失败，回退到内联空值
        desc = Descriptor();
        Add(key, desc);
        return;
      }
    }
    
    // 写入 Blob
    auto ref = blob_manager_->WriteBlob(raw_value);
    if (ref.has_value()) {
      // 创建 Blob 引用 Descriptor
      desc = Descriptor::MakeBlobRef(ref->offset, ref->size_kb, ref->checksum);
    } else {
      // Blob 写入失败，回退到内联空值
      desc = Descriptor();
    }
  } else {
    // Blob 不可用，尝试内联（如果值 <= 4B）否则空值
    if (raw_value.size() <= 4) {
      auto opt_desc = Descriptor::InlineShortStr(column_id_, raw_value);
      if (opt_desc.has_value()) {
        desc = *opt_desc;
      } else {
        desc = Descriptor();
      }
    } else {
      desc = Descriptor();  // 空值标记
    }
  }
  
  Add(key, desc);
}

Status ZoneColumnarSstBuilder::FlushBlock() {
  if (zone_builder_.Count() == 0) {
    return Status::OK();
  }
  
  // 记录重启点
  ZoneRestartPoint rp;
  rp.entity_id = first_key_.entity_id();
  rp.timestamp_hi = static_cast<uint32_t>(first_key_.timestamp().value() >> 32);
  rp.row_index = current_zones_.empty() ? 0 : header_.row_count;
  restart_points_.push_back(rp);
  
  // 获取 Zone 数据
  auto zones = zone_builder_.Finish();
  
  // 缓存各 Zone 数据（而不是立即写入）
  // 这样可以确保相同 zone 的数据在文件中是连续的
  for (size_t i = 0; i < zones.size(); ++i) {
    auto& zone = zones[i];
    
    if (i == 0) {
      // Zone 0: Entity IDs
      zone0_chunks_.push_back(std::move(zone.encoded_data));
      header_.zone0.uncompressed_size += static_cast<uint32_t>(zone0_chunks_.back().size());
    } else if (i == 1) {
      // Zone 1: Timestamps - 缓存原始时间戳，稍后在 Finish 中统一编码
      all_timestamps_.insert(all_timestamps_.end(), 
                             zone.raw_timestamps.begin(), 
                             zone.raw_timestamps.end());
    } else if (i == 2) {
      // Zone 2: Target IDs
      zone2_chunks_.push_back(std::move(zone.encoded_data));
      header_.zone2.uncompressed_size += static_cast<uint32_t>(zone2_chunks_.back().size());
    } else if (i == 3) {
      // Zone 3: Key Metadata (Sequence RLE + Flags Bitmap)
      zone3_seq_rle_chunks_.push_back(std::move(zone.sequence_rle));
      zone3_flags_chunks_.push_back(std::move(zone.flags_bitmap));
    } else if (i == 4) {
      // Zone 4: Values
      zone4_encoding_type_ = zone.encoding.value_encoding;
      zone4_chunks_.push_back(std::move(zone.encoded_data));
      header_.zone4.uncompressed_size += static_cast<uint32_t>(zone4_chunks_.back().size());
    }
  }
  
  header_.row_count += static_cast<uint32_t>(zones[0].zone_map.count);
  current_zones_ = std::move(zones);
  zone_builder_.Reset();
  
  return Status::OK();
}

Status ZoneColumnarSstBuilder::Finish() {
  if (closed_) return status_;
  closed_ = true;
  
  // 刷新剩余的块
  Status s = FlushBlock();
  if (!s.ok()) return s;
  
  // 更新 Header
  header_.magic = kZoneColumnarMagic;
  header_.version = kZoneColumnarVersion;
  header_.column_id = column_id_;
  header_.min_timestamp = min_timestamp_;
  header_.max_timestamp = max_timestamp_;
  header_.min_entity_id = min_entity_id_;
  header_.max_entity_id = max_entity_id_;
  header_.restart_points_count = static_cast<uint32_t>(restart_points_.size());
  
  // 统一写入所有缓存的 Zone 数据（确保相同 zone 的数据连续）
  // Zone 0: Entity IDs
  if (!zone0_chunks_.empty()) {
    header_.zone0.data_offset = static_cast<uint32_t>(file_size_);
    for (auto& chunk : zone0_chunks_) {
      header_.zone0.data_size += static_cast<uint32_t>(chunk.size());
      s = file_->Append(Slice(chunk));
      if (!s.ok()) return s;
      file_size_ += chunk.size();
    }
    zone0_chunks_.clear();
  }
  
  // Zone 1: Timestamps - 统一编码所有时间戳
  if (!all_timestamps_.empty()) {
    ZoneMap ts_map;
    std::string ts_data = TimestampZoneEncoder::Encode(all_timestamps_, &ts_map);
    header_.zone1.data_offset = static_cast<uint32_t>(file_size_);
    header_.zone1.data_size = static_cast<uint32_t>(ts_data.size());
    header_.zone1.uncompressed_size = static_cast<uint32_t>(ts_data.size());
    s = file_->Append(Slice(ts_data));
    if (!s.ok()) return s;
    file_size_ += ts_data.size();
    all_timestamps_.clear();
  } else if (!zone1_chunks_.empty()) {
    // 兼容旧逻辑：如果有缓存的 chunks 但没有原始时间戳
    header_.zone1.data_offset = static_cast<uint32_t>(file_size_);
    for (auto& chunk : zone1_chunks_) {
      header_.zone1.data_size += static_cast<uint32_t>(chunk.size());
      s = file_->Append(Slice(chunk));
      if (!s.ok()) return s;
      file_size_ += chunk.size();
    }
    zone1_chunks_.clear();
  }
  
  // Zone 2: Target IDs
  if (!zone2_chunks_.empty()) {
    header_.zone2.data_offset = static_cast<uint32_t>(file_size_);
    for (auto& chunk : zone2_chunks_) {
      header_.zone2.data_size += static_cast<uint32_t>(chunk.size());
      s = file_->Append(Slice(chunk));
      if (!s.ok()) return s;
      file_size_ += chunk.size();
    }
    zone2_chunks_.clear();
  }
  
  // Zone 3: Key Metadata (Sequence RLE + Flags Bitmap)
  if (!zone3_seq_rle_chunks_.empty()) {
    header_.zone3.seq_rle_offset = static_cast<uint32_t>(file_size_);
    for (auto& chunk : zone3_seq_rle_chunks_) {
      header_.zone3.seq_rle_size += static_cast<uint32_t>(chunk.size());
      s = file_->Append(Slice(chunk));
      if (!s.ok()) return s;
      file_size_ += chunk.size();
    }
    for (auto& chunk : zone3_flags_chunks_) {
      header_.zone3.flags_bitmap_size += static_cast<uint32_t>(chunk.size());
      s = file_->Append(Slice(chunk));
      if (!s.ok()) return s;
      file_size_ += chunk.size();
    }
    zone3_seq_rle_chunks_.clear();
    zone3_flags_chunks_.clear();
  }
  
  // Zone 4: Values
  if (!zone4_chunks_.empty()) {
    header_.zone4.encoding_type = static_cast<uint8_t>(zone4_encoding_type_);
    header_.zone4.data_offset = static_cast<uint32_t>(file_size_);
    for (auto& chunk : zone4_chunks_) {
      header_.zone4.data_size += static_cast<uint32_t>(chunk.size());
      s = file_->Append(Slice(chunk));
      if (!s.ok()) return s;
      file_size_ += chunk.size();
    }
    zone4_chunks_.clear();
  }
  
  // 写入 Zone Maps
  s = WriteZoneMaps();
  if (!s.ok()) return s;
  
  // 写入重启点索引
  s = WriteRestartPoints();
  if (!s.ok()) return s;
  
  // 写入 Bloom Filter
  s = WriteBloomFilter();
  if (!s.ok()) return s;
  
  // OPTIMIZATION: 暂时禁用 Entity Index 写入
  // s = WriteEntityIndex();
  // if (!s.ok()) return s;
  header_.entity_index_offset = 0;
  header_.entity_index_size = 0;
  
  // 写入 Footer
  s = WriteFooter();
  if (!s.ok()) return s;
  
  // 最后写入 Header（在文件末尾，简化读取）
  std::string header_buf;
  header_.EncodeTo(&header_buf);
  s = file_->Append(Slice(header_buf));
  if (!s.ok()) return s;
  file_size_ += header_buf.size();
  
  s = file_->Sync();
  if (!s.ok()) return s;
  
  // 同步 Blob 文件（如果启用了 Blob 存储）
  if (blob_manager_) {
    s = blob_manager_->Close();
    if (!s.ok()) return s;
  }
  
  return Status::OK();
}

Status ZoneColumnarSstBuilder::WriteZoneMaps() {
  header_.zone_maps_offset = static_cast<uint32_t>(file_size_);
  
  std::string buf;
  for (const auto& zone : current_zones_) {
    ZoneMapEntry entry;
    entry.min_value = zone.zone_map.min_value;
    entry.max_value = zone.zone_map.max_value;
    entry.count = zone.zone_map.count;
    entry.distinct_count = zone.zone_map.distinct_count;
    entry.EncodeTo(&buf);
  }
  
  header_.zone_maps_size = static_cast<uint32_t>(buf.size());
  
  Status s = file_->Append(Slice(buf));
  if (s.ok()) {
    file_size_ += buf.size();
  }
  return s;
}

Status ZoneColumnarSstBuilder::WriteRestartPoints() {
  header_.restart_points_offset = static_cast<uint32_t>(file_size_);
  
  std::string buf;
  for (const auto& rp : restart_points_) {
    rp.EncodeTo(&buf);
  }
  
  Status s = file_->Append(Slice(buf));
  if (s.ok()) {
    file_size_ += buf.size();
  }
  return s;
}

Status ZoneColumnarSstBuilder::WriteBloomFilter() {
  header_.bloom_filter_offset = static_cast<uint32_t>(file_size_);
  
  std::string buf;
  bloom_filter_.EncodeTo(&buf);
  header_.bloom_filter_size = static_cast<uint32_t>(buf.size());
  
  Status s = file_->Append(Slice(buf));
  if (s.ok()) {
    file_size_ += buf.size();
  }
  return s;
}

// OPTIMIZATION: 写入持久化的 Entity Index
Status ZoneColumnarSstBuilder::WriteEntityIndex() {
  if (entity_index_builder_.empty()) {
    header_.entity_index_offset = 0;
    header_.entity_index_size = 0;
    return Status::OK();
  }
  
  header_.entity_index_offset = static_cast<uint32_t>(file_size_);
  
  // 格式: [num_entries:4B] [entity_id:8B] [num_positions:4B] [pos:4B...]...
  std::string buf;
  uint32_t num_entries = static_cast<uint32_t>(entity_index_builder_.size());
  buf.append(reinterpret_cast<const char*>(&num_entries), 4);
  
  for (const auto& [entity_id, positions] : entity_index_builder_) {
    // Entity ID
    buf.append(reinterpret_cast<const char*>(&entity_id), 8);
    // 位置数量
    uint32_t num_positions = static_cast<uint32_t>(positions.size());
    buf.append(reinterpret_cast<const char*>(&num_positions), 4);
    // 位置列表
    for (uint32_t pos : positions) {
      buf.append(reinterpret_cast<const char*>(&pos), 4);
    }
  }
  
  header_.entity_index_size = static_cast<uint32_t>(buf.size());
  
  Status s = file_->Append(Slice(buf));
  if (s.ok()) {
    file_size_ += buf.size();
  }
  return s;
}

Status ZoneColumnarSstBuilder::WriteFooter() {
  header_.footer_offset = static_cast<uint32_t>(file_size_);
  
  footer_.entry_count = num_entries_;
  footer_.file_number = file_number_;
  footer_.prev_file_number = prev_file_number_;
  footer_.level = level_;
  
  if (footer_.uncompressed_size > 0) {
    footer_.compression_ratio = static_cast<float>(footer_.compressed_size) / 
                                static_cast<float>(footer_.uncompressed_size);
  }
  
  std::string buf;
  footer_.EncodeTo(&buf);
  
  Status s = file_->Append(Slice(buf));
  if (s.ok()) {
    file_size_ += buf.size();
  }
  return s;
}

void ZoneColumnarSstBuilder::Abandon() {
  closed_ = true;
  status_ = Status::IOError("ZoneColumnarSstBuilder: abandoned");
}

// =============================================================================
// ZoneColumnarSstReader 实现
// =============================================================================

ZoneColumnarSstReader::ZoneColumnarSstReader(const std::string& file_path)
    : file_path_(file_path),
      owns_buffer_(true) {
}

ZoneColumnarSstReader::ZoneColumnarSstReader(const char* data, size_t size)
    : buffer_data_(data),
      buffer_size_(size),
      owns_buffer_(false) {
}

ZoneColumnarSstReader::~ZoneColumnarSstReader() {
  Close();
}

Status ZoneColumnarSstReader::Open() {
  if (opened_) return Status::OK();
  
  // 读取文件内容
  if (owns_buffer_) {
    std::ifstream file(file_path_, std::ios::binary | std::ios::ate);
    if (!file) {
      return Status::IOError("ZoneColumnarSstReader", "cannot open file: " + file_path_);
    }
    
    buffer_size_ = file.tellg();
    file.seekg(0, std::ios::beg);
    
    char* buf = new char[buffer_size_];
    if (!file.read(buf, buffer_size_)) {
      delete[] buf;
      return Status::IOError("ZoneColumnarSstReader", "cannot read file: " + file_path_);
    }
    buffer_data_ = buf;
  }
  
  // 验证文件大小
  if (buffer_size_ < ZoneColumnarHeader::kEncodedSize + ZoneColumnarFooter::kEncodedSize) {
    return Status::Corruption("ZoneColumnarSstReader", "file too small");
  }
  
  // 读取 Header（在文件末尾）
  Slice header_slice(buffer_data_ + buffer_size_ - ZoneColumnarHeader::kEncodedSize,
                     ZoneColumnarHeader::kEncodedSize);
  Status s = header_.DecodeFrom(&header_slice);
  if (!s.ok()) return s;
  
  // 读取 Footer
  if (header_.footer_offset + ZoneColumnarFooter::kEncodedSize > buffer_size_) {
    return Status::Corruption("ZoneColumnarSstReader", "invalid footer offset");
  }
  Slice footer_slice(buffer_data_ + header_.footer_offset,
                     ZoneColumnarFooter::kEncodedSize);
  s = footer_.DecodeFrom(&footer_slice);
  if (!s.ok()) return s;
  
  // 读取 Zone Maps (现在支持 5 个 Zone)
  if (header_.zone_maps_offset > 0 && header_.zone_maps_size > 0) {
    size_t num_entries = header_.zone_maps_size / ZoneMapEntry::kEncodedSize;
    Slice zm_slice(buffer_data_ + header_.zone_maps_offset, header_.zone_maps_size);
    for (size_t i = 0; i < num_entries && i < 5; ++i) {
      zone_maps_[i].DecodeFrom(&zm_slice);
    }
  }
  
  // 读取 Restart Points
  if (header_.restart_points_offset > 0 && header_.restart_points_count > 0) {
    restart_points_.reserve(header_.restart_points_count);
    Slice rp_slice(buffer_data_ + header_.restart_points_offset,
                   header_.restart_points_count * ZoneRestartPoint::kEncodedSize);
    for (uint32_t i = 0; i < header_.restart_points_count; ++i) {
      ZoneRestartPoint rp;
      rp.DecodeFrom(&rp_slice);
      restart_points_.push_back(rp);
    }
  }
  
  // 加载 Bloom Filter
  if (header_.bloom_filter_offset > 0 && header_.bloom_filter_size > 0) {
    if (header_.bloom_filter_offset + header_.bloom_filter_size <= buffer_size_) {
      Slice bf_slice(buffer_data_ + header_.bloom_filter_offset, header_.bloom_filter_size);
      // Estimate number of keys from footer
      size_t estimated_keys = std::max(size_t(1), static_cast<size_t>(footer_.entry_count));
      bloom_filter_.DecodeFrom(&bf_slice, estimated_keys);
    }
  }
  
  // 加载 Zone 数据
  s = LoadZones();
  if (!s.ok()) return s;
  
  // OPTIMIZATION: 加载持久化的 Entity Index
  // NOTE: 暂时禁用持久化索引，使用内存中的索引
  // s = LoadEntityIndex();
  // if (!s.ok()) {
  //   // Entity Index 加载失败不是致命错误，可以继续使用线性扫描
  // }
  
  opened_ = true;
  return Status::OK();
}

Status ZoneColumnarSstReader::LoadZones() {

  
  // 读取各 Zone 数据
  // Zone 0: Entity IDs
  if (header_.zone0.data_size > 0) {
    if (header_.zone0.data_offset + header_.zone0.data_size > buffer_size_) {
      return Status::Corruption("ZoneColumnarSstReader", "invalid zone0 data offset");
    }
    zone_data_[0] = std::string(buffer_data_ + header_.zone0.data_offset, header_.zone0.data_size);
    entity_decoder_.emplace(zone_data_[0]);
    

  }
  
  // Zone 1: Timestamps
  if (header_.zone1.data_size > 0) {
    if (header_.zone1.data_offset + header_.zone1.data_size > buffer_size_) {
      return Status::Corruption("ZoneColumnarSstReader", "invalid zone1 data offset");
    }
    zone_data_[1] = std::string(buffer_data_ + header_.zone1.data_offset, header_.zone1.data_size);
    timestamp_decoder_.emplace(zone_data_[1]);
  }
  
  // Zone 2: Target IDs
  if (header_.zone2.data_size > 0) {
    if (header_.zone2.data_offset + header_.zone2.data_size > buffer_size_) {
      return Status::Corruption("ZoneColumnarSstReader", "invalid zone2 data offset");
    }
    zone_data_[2] = std::string(buffer_data_ + header_.zone2.data_offset, header_.zone2.data_size);
    target_decoder_.emplace(zone_data_[2], TargetIdZoneEncoder::EncodingType::kRaw);
  }
  
  // Zone 3: Key Metadata (Sequence RLE + Flags Bitmap)
  if (header_.zone3.seq_rle_offset > 0 && header_.zone3.seq_rle_size > 0) {
    if (header_.zone3.seq_rle_offset + header_.zone3.seq_rle_size + header_.zone3.flags_bitmap_size > buffer_size_) {
      return Status::Corruption("ZoneColumnarSstReader", "invalid zone3 data offset");
    }
    std::string seq_rle(buffer_data_ + header_.zone3.seq_rle_offset, header_.zone3.seq_rle_size);
    std::string flags_bm(buffer_data_ + header_.zone3.seq_rle_offset + header_.zone3.seq_rle_size, 
                         header_.zone3.flags_bitmap_size);
    metadata_decoder_.emplace(seq_rle, flags_bm, header_.row_count);
  }
  
  // Zone 4: Values
  if (header_.zone4.data_size > 0) {
    if (header_.zone4.data_offset + header_.zone4.data_size > buffer_size_) {
      return Status::Corruption("ZoneColumnarSstReader", "invalid zone4 data offset");
    }
    zone_data_[3] = std::string(buffer_data_ + header_.zone4.data_offset, header_.zone4.data_size);
    auto encoding_type = static_cast<ValueZoneEncoder::EncodingType>(header_.zone4.encoding_type);
    value_decoder_.emplace(zone_data_[3], encoding_type);
  }
  
  return Status::OK();
}

// OPTIMIZATION: 加载持久化的 Entity Index
Status ZoneColumnarSstReader::LoadEntityIndex() {
  if (header_.entity_index_offset == 0 || header_.entity_index_size == 0) {
    // 文件没有 Entity Index，不是错误
    has_entity_index_ = false;
    return Status::OK();
  }
  
  // 检查范围
  if (header_.entity_index_offset + header_.entity_index_size > buffer_size_) {
    return Status::Corruption("ZoneColumnarSstReader", "invalid entity index offset");
  }
  
  const char* p = buffer_data_ + header_.entity_index_offset;
  size_t pos = 0;
  
  // 读取条目数量
  if (pos + 4 > header_.entity_index_size) {
    return Status::Corruption("ZoneColumnarSstReader", "truncated entity index");
  }
  uint32_t num_entries = *reinterpret_cast<const uint32_t*>(p + pos);
  pos += 4;
  
  entity_index_.reserve(num_entries);
  
  // 读取每个 entry
  for (uint32_t i = 0; i < num_entries; ++i) {
    if (pos + 12 > header_.entity_index_size) {
      return Status::Corruption("ZoneColumnarSstReader", "truncated entity index entry");
    }
    
    uint64_t entity_id = *reinterpret_cast<const uint64_t*>(p + pos);
    pos += 8;
    uint32_t num_positions = *reinterpret_cast<const uint32_t*>(p + pos);
    pos += 4;
    
    if (pos + num_positions * 4 > header_.entity_index_size) {
      return Status::Corruption("ZoneColumnarSstReader", "truncated entity index positions");
    }
    
    auto& positions = entity_index_[entity_id];
    positions.reserve(num_positions);
    for (uint32_t j = 0; j < num_positions; ++j) {
      positions.push_back(*reinterpret_cast<const uint32_t*>(p + pos));
      pos += 4;
    }
  }
  
  has_entity_index_ = true;
  return Status::OK();
}

// OPTIMIZATION: 使用持久化索引查找 entity positions
std::vector<size_t> ZoneColumnarSstReader::FindEntityPositionsFromIndex(
    uint64_t entity_id) const {
  std::vector<size_t> result;
  
  if (!has_entity_index_) {
    return result;
  }
  
  auto it = entity_index_.find(entity_id);
  if (it != entity_index_.end()) {
    result.reserve(it->second.size());
    for (uint32_t pos : it->second) {
      result.push_back(static_cast<size_t>(pos));
    }
  }
  
  return result;
}

void ZoneColumnarSstReader::Close() {
  if (owns_buffer_ && buffer_data_) {
    delete[] buffer_data_;
    buffer_data_ = nullptr;
  }
  if (blob_manager_) {
    blob_manager_->Close();
    blob_manager_.reset();
  }
  opened_ = false;
}

// =============================================================================
// Blob 读取支持
// =============================================================================

Status ZoneColumnarSstReader::GetValue(size_t row_idx, std::string* out_value) const {
  if (!opened_) return Status::IOError("ZoneColumnarSstReader::GetValue", "not opened");
  if (!value_decoder_) return Status::IOError("ZoneColumnarSstReader::GetValue", "no value decoder");
  if (row_idx >= header_.row_count) return Status::InvalidArgument("ZoneColumnarSstReader::GetValue", "invalid row_idx");
  
  Descriptor desc = value_decoder_->Get(row_idx);
  
  if (desc.IsInline()) {
    // 内联值：直接返回
    std::string val = desc.AsInlineShortStr();
    *out_value = std::move(val);
    return Status::OK();
  } else if (desc.IsBlobRef()) {
    // Blob 引用：从 Blob 文件读取
    auto ref = desc.GetBlobRef();
    
    // 延迟初始化 BlobFileManager
    if (!blob_manager_ && !db_path_.empty()) {
      uint32_t sst_id = static_cast<uint32_t>(footer_.file_number);
      blob_manager_ = std::make_unique<SimpleSSTBlobManager>(db_path_, sst_id);
      Status s = blob_manager_->OpenForRead();
      if (!s.ok()) return s;
    }
    
    if (!blob_manager_) {
      return Status::IOError("ZoneColumnarSstReader::GetValue", "blob not available");
    }
    
    return blob_manager_->ReadBlob(ref.offset, ref.size_kb, out_value);
  }
  
  return Status::OK();  // Tombstone 或空值
}

Status ZoneColumnarSstReader::GetValues(
    const std::vector<size_t>& row_indices,
    std::vector<std::string>* out_values) const {
  out_values->clear();
  out_values->reserve(row_indices.size());
  
  // 分离内联值和 Blob 引用
  std::vector<std::pair<size_t, size_t>> blob_refs;  // (row_idx, out_idx)
  
  for (size_t i = 0; i < row_indices.size(); ++i) {
    size_t row_idx = row_indices[i];
    
    if (row_idx >= header_.row_count) {
      out_values->push_back("");
      continue;
    }
    
    Descriptor desc = value_decoder_->Get(row_idx);
    
    if (desc.IsInline()) {
      out_values->push_back(desc.AsInlineShortStr());
    } else if (desc.IsBlobRef()) {
      // 标记为需要 Blob 读取
      out_values->push_back("");  // 占位
      blob_refs.push_back({row_idx, i});
    } else {
      out_values->push_back("");  // Tombstone
    }
  }
  
  // 批量读取 Blobs
  if (!blob_refs.empty() && !db_path_.empty()) {
    if (!blob_manager_) {
      uint32_t sst_id = static_cast<uint32_t>(footer_.file_number);
      blob_manager_ = std::make_unique<SimpleSSTBlobManager>(db_path_, sst_id);
      Status s = blob_manager_->OpenForRead();
      if (!s.ok()) return s;
    }
    
    std::vector<std::pair<uint32_t, uint16_t>> offsets_sizes;
    offsets_sizes.reserve(blob_refs.size());
    
    for (const auto& [row_idx, _] : blob_refs) {
      Descriptor desc = value_decoder_->Get(row_idx);
      auto ref = desc.GetBlobRef();
      offsets_sizes.push_back({ref.offset, ref.size_kb});
    }
    
    std::vector<std::string> blob_values;
    Status s = blob_manager_->ReadBlobs(offsets_sizes, &blob_values);
    if (!s.ok()) return s;
    
    // 回填结果
    for (size_t i = 0; i < blob_refs.size(); ++i) {
      size_t out_idx = blob_refs[i].second;
      (*out_values)[out_idx] = std::move(blob_values[i]);
    }
  }
  
  return Status::OK();
}

std::optional<Descriptor> ZoneColumnarSstReader::Get(const CedarKey& key) const {
  if (!opened_) return std::nullopt;
  
  // Zone Map 快速过滤
  if (!MayContainEntity(key.entity_id())) return std::nullopt;
  
  // 检查解码器是否已初始化
  if (!entity_decoder_.has_value() || !timestamp_decoder_.has_value() || 
      !value_decoder_.has_value()) {
    return std::nullopt;
  }
  
  // OPTIMIZATION: 优先使用持久化的 Entity Index
  // NOTE: 暂时只使用内存索引
  auto positions = entity_decoder_->FindEntityPositions(key.entity_id());
  
  for (size_t idx : positions) {
    uint64_t ts = timestamp_decoder_->Get(idx);
    if (ts == key.timestamp().value()) {
      return value_decoder_->Get(idx);
    }
  }
  
  return std::nullopt;
}

std::optional<Descriptor> ZoneColumnarSstReader::GetAtTime(
    uint64_t entity_id,
    enum EntityType entity_type,
    uint16_t column_id,
    Timestamp timestamp) const {
  if (!opened_) return std::nullopt;
  
  // Zone Map 快速过滤
  if (!MayContainEntity(entity_id)) return std::nullopt;
  if (entity_type != static_cast<enum EntityType>(header_.entity_type)) return std::nullopt;
  if (column_id != header_.column_id) return std::nullopt;
  
  // OPTIMIZATION: 优先使用持久化的 Entity Index
  // NOTE: 暂时只使用内存索引
  auto positions = entity_decoder_->FindEntityPositions(entity_id);
  
  // 按时间戳降序查找（最新的优先）
  std::optional<Descriptor> result;
  uint64_t best_ts = 0;
  
  for (size_t idx : positions) {
    uint64_t ts = timestamp_decoder_->Get(idx);
    if (ts <= timestamp.value() && ts > best_ts) {
      best_ts = ts;
      result = value_decoder_->Get(idx);
    }
  }
  
  return result;
}

std::vector<std::pair<CedarKey, Descriptor>> ZoneColumnarSstReader::GetRange(
    uint64_t entity_id,
    enum EntityType entity_type,
    uint16_t column_id,
    Timestamp start,
    Timestamp end) const {
  std::vector<std::pair<CedarKey, Descriptor>> results;
  
  if (!opened_) return results;
  if (!MayContainTimeRange(start.value(), end.value())) return results;
  if (entity_type != static_cast<enum EntityType>(header_.entity_type)) return results;
  if (column_id != header_.column_id) return results;
  
  // OPTIMIZATION: 使用 ScanTemporalRange 方式（先按时间过滤，再匹配 entity）
  // 这比 FindEntityPositions 快得多，因为时间范围通常只包含少量记录
  // 而 entity 可能在文件中有大量记录需要线性扫描
  
  // 1. 使用时间戳索引找到时间范围内的位置（高效）
  auto positions = timestamp_decoder_->FindTimeRange(start.value(), end.value());
  
  // 2. 在这些位置中过滤 entity_id（延迟物化）
  results.reserve(std::min(positions.size(), size_t(256)));  // 预分配合理大小
  for (size_t idx : positions) {
    uint64_t eid = entity_decoder_->Get(idx);
    if (eid == entity_id) {
      uint64_t ts = timestamp_decoder_->Get(idx);
      CedarKey key = CedarKey::Vertex(entity_id, column_id, Timestamp(ts));
      Descriptor desc = value_decoder_->Get(idx);
      results.emplace_back(key, desc);
    }
  }
  
  return results;
}

void ZoneColumnarSstReader::ScanTemporalRange(
    uint64_t entity_id,
    Timestamp start,
    Timestamp end,
    std::function<void(const CedarKey&, const Descriptor&)> callback) const {
  if (!opened_) return;
  if (!MayContainTimeRange(start.value(), end.value())) return;
  
  // 使用时间戳索引找到范围内的位置
  auto positions = timestamp_decoder_->FindTimeRange(start.value(), end.value());
  
  // 延迟物化：只读取需要的 Value
  for (size_t idx : positions) {
    uint64_t eid = entity_decoder_->Get(idx);
    if (eid == entity_id) {
      uint64_t ts = timestamp_decoder_->Get(idx);
      CedarKey key = CedarKey::Vertex(entity_id, header_.column_id, Timestamp(ts));
      Descriptor desc = value_decoder_->Get(idx);
      callback(key, desc);
    }
  }
}

void ZoneColumnarSstReader::BatchGet(
    const std::vector<CedarKey>& keys,
    std::vector<std::optional<Descriptor>>* results) const {
  results->clear();
  results->reserve(keys.size());
  
  for (const auto& key : keys) {
    results->push_back(Get(key));
  }
}

// OPTIMIZATION: P0 - 批量时间范围查询
// 一次扫描时间范围，返回多个 entity 的数据
std::unordered_map<uint64_t, std::vector<std::pair<CedarKey, Descriptor>>> 
ZoneColumnarSstReader::BatchGetRange(
    const std::vector<uint64_t>& entity_ids,
    EntityType entity_type,
    uint16_t column_id,
    Timestamp start,
    Timestamp end) const {
  
  std::unordered_map<uint64_t, std::vector<std::pair<CedarKey, Descriptor>>> results;
  
  if (!opened_) return results;
  if (!MayContainTimeRange(start.value(), end.value())) return results;
  if (entity_type != static_cast<enum EntityType>(header_.entity_type)) return results;
  if (column_id != header_.column_id) return results;
  
  // 快速过滤：将 entity_ids 转换为 unordered_set 加速查找
  std::unordered_set<uint64_t> entity_set(entity_ids.begin(), entity_ids.end());
  
  // 检查文件是否可能包含这些 entity（使用 Zone Map）
  bool may_contain_any = false;
  for (uint64_t eid : entity_ids) {
    if (MayContainEntity(eid)) {
      may_contain_any = true;
      break;
    }
  }
  if (!may_contain_any) return results;
  
  // 一次扫描时间范围，收集所有匹配的数据
  // 使用二分查找优化的时间范围查询
  auto positions = timestamp_decoder_->FindTimeRange(start.value(), end.value());
  
  for (size_t idx : positions) {
    uint64_t eid = entity_decoder_->Get(idx);
    
    // 只处理在查询列表中的 entity
    if (entity_set.find(eid) != entity_set.end()) {
      uint64_t ts = timestamp_decoder_->Get(idx);
      CedarKey key = CedarKey::Vertex(eid, column_id, Timestamp(ts));
      Descriptor desc = value_decoder_->Get(idx);
      results[eid].emplace_back(key, desc);
    }
  }
  
  return results;
}

bool ZoneColumnarSstReader::MayContainEntity(uint64_t entity_id) const {
  // 首先检查范围
  if (entity_id < header_.min_entity_id || entity_id > header_.max_entity_id) {
    return false;
  }
  // 然后检查 Bloom Filter（如果已加载）
  if (bloom_filter_.NumKeys() > 0) {
    return bloom_filter_.MayContain(entity_id);
  }
  return true;
}

bool ZoneColumnarSstReader::MayContainTimeRange(uint64_t start_ts, uint64_t end_ts) const {
  return !(end_ts < header_.min_timestamp || start_ts > header_.max_timestamp);
}

// =============================================================================
// ZoneColumnarIterator 实现
// =============================================================================

ZoneColumnarIterator::ZoneColumnarIterator(const ZoneColumnarSstReader* reader)
    : reader_(reader), total_count_(reader->NumEntries()) {
}

void ZoneColumnarIterator::SeekToFirst() {
  current_idx_ = 0;
  valid_ = total_count_ > 0;
}

void ZoneColumnarIterator::Seek(const CedarKey& key) {
  SeekToFirst();
  while (Valid()) {
    CedarKey current = Key();
    if (current.entity_id() >= key.entity_id() && 
        current.timestamp().value() <= key.timestamp().value()) {
      return;
    }
    Next();
  }
}

void ZoneColumnarIterator::Next() {
  if (Valid()) {
    current_idx_++;
  }
}

CedarKey ZoneColumnarIterator::Key() const {
  if (!Valid()) return CedarKey();
  
  // 从各 Zone 解码当前行的数据
  uint64_t entity_id = reader_->entity_decoder_->Get(current_idx_);
  uint64_t ts = reader_->timestamp_decoder_->Get(current_idx_);
  uint64_t target_id = reader_->target_decoder_ ? reader_->target_decoder_->Get(current_idx_, {}) : 0;
  uint16_t seq = reader_->metadata_decoder_ ? reader_->metadata_decoder_->GetSequence(current_idx_) : 0;
  uint8_t flags = reader_->metadata_decoder_ ? reader_->metadata_decoder_->GetFlags(current_idx_) : 0;
  
  return CedarKey(entity_id, static_cast<EntityType>(reader_->header_.entity_type), 
                 reader_->header_.column_id, Timestamp(ts), seq, target_id, flags);
}

Descriptor ZoneColumnarIterator::Value() const {
  if (!Valid()) return Descriptor();
  return reader_->value_decoder_->Get(current_idx_);
}

// =============================================================================
// ZoneColumnarSstReader 新增方法
// =============================================================================

ZoneColumnarIterator* ZoneColumnarSstReader::NewIterator() const {
  if (!opened_) return nullptr;
  return new ZoneColumnarIterator(this);
}

Status ZoneColumnarSstReader::GetAllEntries(
    std::vector<std::pair<CedarKey, Descriptor>>* entries) const {
  if (!opened_) {
    return Status::IOError("ZoneColumnarSstReader::GetAllEntries", "not opened");
  }
  
  entries->clear();
  entries->reserve(header_.row_count);
  
  std::unique_ptr<ZoneColumnarIterator> iter(NewIterator());
  if (!iter) {
    return Status::IOError("ZoneColumnarSstReader::GetAllEntries", "failed to create iterator");
  }
  
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    entries->emplace_back(iter->Key(), iter->Value());
  }
  
  return Status::OK();
}

}  // namespace cedar
