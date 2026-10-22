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

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cedar/core/crc32c.h"
#include "cedar/frond/blob_file.h"
#include "cedar/types/descriptor.h"

#include <optional>

namespace cedar {

// =============================================================================
// ZoneReaderView（Task 3.1）：mmap 只读视图，失败回退 pread
// =============================================================================
Status ZoneReaderView::OpenFile(const std::string& path) {
  Close();

  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return Status::IOError("ZoneReaderView", "cannot open file: " + path);
  }

  struct stat st;
  if (::fstat(fd, &st) != 0 || st.st_size <= 0) {
    ::close(fd);
    return Status::IOError("ZoneReaderView", "cannot stat file: " + path);
  }
  size_ = static_cast<size_t>(st.st_size);

  void* addr = ::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd, 0);
  if (addr != MAP_FAILED) {
    map_addr_ = addr;
    data_ = static_cast<const char*>(addr);
    mapped_ = true;
    ::close(fd);  // mmap 后不再需要 fd（映射仍有效）
    return Status::OK();
  }

  // 回退：整文件 pread 到堆（保持功能可用，但无零拷贝收益）
  owned_ = new char[size_];
  size_t read_total = 0;
  while (read_total < size_) {
    const ssize_t n = ::pread(fd, owned_ + read_total, size_ - read_total,
                              static_cast<off_t>(read_total));
    if (n <= 0) break;
    read_total += static_cast<size_t>(n);
  }
  ::close(fd);
  if (read_total != size_) {
    delete[] owned_;
    owned_ = nullptr;
    size_ = 0;
    return Status::IOError("ZoneReaderView", "short read: " + path);
  }
  data_ = owned_;
  mapped_ = false;
  return Status::OK();
}

void ZoneReaderView::Close() {
  if (map_addr_ != nullptr) {
    ::munmap(map_addr_, size_);
    map_addr_ = nullptr;
  }
  if (owned_ != nullptr) {
    delete[] owned_;
    owned_ = nullptr;
  }
  data_ = nullptr;
  size_ = 0;
  mapped_ = false;
}

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

  // Task 3.3：Bloom 的 entity 集合 + 紧凑 Entity Index（每 entity 一条，增量）。
  if (has_last_index_entity_ && key.entity_id() < last_index_entity_) {
    entity_runs_sorted_ = false;  // 输入乱序：不写持久索引，读端走 fallback
  }
  if (!has_last_index_entity_ || key.entity_id() != last_index_entity_) {
    bloom_entity_ids_.push_back(key.entity_id());
    if (entity_runs_sorted_) {
      entity_runs_.emplace_back(key.entity_id(),
                                static_cast<uint32_t>(num_entries_));
    }
    last_index_entity_ = key.entity_id();
    has_last_index_entity_ = true;
  }

  // 添加到 Zone Builder
  zone_builder_.Add(key, descriptor);

  // Task 1.4：target zone 改为整文件一次编码（审计 2.1）。
  // 收集全文件的 target，Finish 时统一写成一条 zone2 流。
  all_target_ids_.push_back(key.target_id());

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
      // Zone 2: Target IDs —— Task 1.4 起改为整文件一次编码（见 Finish），
      // 这里丢弃 per-block 的编码结果，避免旧格式的逐块 tag 混入 zone2。
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

  // Zone 2: Target IDs —— 整文件一次编码（Task 1.4，审计 2.1）
  // 新写法：一条连续的 target 流（文件级唯一的 1B encoding_type 前缀），
  // header.flags 置 kFlagTargetZoneFileLevel，header.zone2.encoding_type 写真实类型；
  // 旧文件 flags 未置位，读端按逐块格式 fallback。
  if (!all_target_ids_.empty()) {
    ZoneMap target_map;
    std::string target_data = TargetIdZoneEncoder::EncodeFileLevel(all_target_ids_,
                                                                   &target_map);
    header_.flags |= kFlagTargetZoneFileLevel;
    header_.zone2.encoding_type =
        static_cast<uint8_t>(TargetIdZoneEncoder::EncodingType::kRaw);
    header_.zone2.data_offset = static_cast<uint32_t>(file_size_);
    header_.zone2.data_size = static_cast<uint32_t>(target_data.size());
    header_.zone2.uncompressed_size = static_cast<uint32_t>(target_data.size());
    s = file_->Append(Slice(target_data));
    if (!s.ok()) return s;
    file_size_ += target_data.size();
    all_target_ids_.clear();
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

  // Task 3.3：启用持久化 Entity Index（紧凑格式，零拷贝读）
  s = WriteEntityIndex();
  if (!s.ok()) return s;

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
  // Task 3.3（审计 2.10）：用真实的 entity 数量在 Finish 时分配位图并写入真实
  // num_keys；旧实现用 BloomFilter(10)（num_keys=0）导致 bitmap 从未分配。
  header_.bloom_filter_offset = static_cast<uint32_t>(file_size_);
  header_.bloom_filter_size = 0;
  if (bloom_entity_ids_.empty() || !options_.enable_bloom_filter) {
    return Status::OK();
  }

  BloomFilter filter(10, bloom_entity_ids_.size());
  for (uint64_t entity_id : bloom_entity_ids_) {
    filter.Add(entity_id);
  }
  std::string buf;
  filter.EncodeTo(&buf);
  header_.bloom_filter_size = static_cast<uint32_t>(buf.size());

  Status s = file_->Append(Slice(buf));
  if (s.ok()) {
    file_size_ += buf.size();
  }
  return s;
}

// Task 3.3：写入持久化紧凑 Entity Index
// 格式：[magic:4B "EIDX"][count:4B][entity_id:8B][first_row:4B] * count
// 文件内 entity 连续（key 升序），因此每个 entity 的行区间是
// [first_row, next.first_row)（最后一条到 row_count）。
Status ZoneColumnarSstBuilder::WriteEntityIndex() {
  header_.entity_index_offset = 0;
  header_.entity_index_size = 0;
  if (!entity_runs_sorted_ || entity_runs_.empty()) {
    return Status::OK();  // 乱序/空文件：不写索引，读端 fallback
  }

  header_.entity_index_offset = static_cast<uint32_t>(file_size_);

  std::string buf;
  buf.reserve(8 + entity_runs_.size() * 12);
  const uint32_t magic = 0x58444945;  // "EIDX" (little-endian)
  const uint32_t count = static_cast<uint32_t>(entity_runs_.size());
  buf.append(reinterpret_cast<const char*>(&magic), sizeof(magic));
  buf.append(reinterpret_cast<const char*>(&count), sizeof(count));
  for (const auto& [entity_id, first_row] : entity_runs_) {
    buf.append(reinterpret_cast<const char*>(&entity_id), sizeof(entity_id));
    buf.append(reinterpret_cast<const char*>(&first_row), sizeof(first_row));
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

  // Task 3.1：mmap 零拷贝打开（mmap 失败时 ZoneReaderView 内部回退 pread）。
  // 后续所有 zone 都是该视图上的 Slice，decoder 不复制整份 zone。
  if (owns_buffer_) {
    Status vs = view_.OpenFile(file_path_);
    if (!vs.ok()) return vs;
    buffer_data_ = view_.data();
    buffer_size_ = view_.size();
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

  // 加载 Bloom Filter（Task 3.3：真实 bitmap，零拷贝视图）
  if (header_.bloom_filter_offset > 0 && header_.bloom_filter_size > 0) {
    if (header_.bloom_filter_offset + header_.bloom_filter_size <= buffer_size_) {
      Slice bf_slice(buffer_data_ + header_.bloom_filter_offset,
                     header_.bloom_filter_size);
      size_t estimated_keys = std::max(size_t(1), static_cast<size_t>(footer_.entry_count));
      bloom_filter_.DecodeView(bf_slice.data(), bf_slice.size(), estimated_keys);
    }
  }

  // 加载 Zone 数据
  s = LoadZones();
  if (!s.ok()) return s;

  // Task 3.3：加载持久化的紧凑 Entity Index（失败/缺失则回退线性扫描，不致命）
  s = LoadEntityIndex();
  if (!s.ok()) {
    has_entity_index_ = false;
    entity_index_slice_ = Slice();
    entity_index_count_ = 0;
  }

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
    zone_data_[0] = Slice(buffer_data_ + header_.zone0.data_offset, header_.zone0.data_size);
    entity_decoder_.emplace(zone_data_[0]);
  }

  // Zone 1: Timestamps
  if (header_.zone1.data_size > 0) {
    if (header_.zone1.data_offset + header_.zone1.data_size > buffer_size_) {
      return Status::Corruption("ZoneColumnarSstReader", "invalid zone1 data offset");
    }
    zone_data_[1] = Slice(buffer_data_ + header_.zone1.data_offset, header_.zone1.data_size);
    timestamp_decoder_.emplace(zone_data_[1]);
  }

  // Zone 2: Target IDs
  if (header_.zone2.data_size > 0) {
    if (header_.zone2.data_offset + header_.zone2.data_size > buffer_size_) {
      return Status::Corruption("ZoneColumnarSstReader", "invalid zone2 data offset");
    }
    zone_data_[2] = Slice(buffer_data_ + header_.zone2.data_offset, header_.zone2.data_size);
  }

  // Zone 3: Key Metadata (Sequence RLE + Flags Bitmap)
  if (header_.zone3.seq_rle_offset > 0 && header_.zone3.seq_rle_size > 0) {
    if (header_.zone3.seq_rle_offset + header_.zone3.seq_rle_size + header_.zone3.flags_bitmap_size > buffer_size_) {
      return Status::Corruption("ZoneColumnarSstReader", "invalid zone3 data offset");
    }
    Slice seq_rle(buffer_data_ + header_.zone3.seq_rle_offset, header_.zone3.seq_rle_size);
    Slice flags_bm(buffer_data_ + header_.zone3.seq_rle_offset + header_.zone3.seq_rle_size,
                   header_.zone3.flags_bitmap_size);
    metadata_decoder_.emplace(seq_rle, flags_bm, header_.row_count);
  }

  // Zone 4: Values
  if (header_.zone4.data_size > 0) {
    if (header_.zone4.data_offset + header_.zone4.data_size > buffer_size_) {
      return Status::Corruption("ZoneColumnarSstReader", "invalid zone4 data offset");
    }
    zone_data_[3] = Slice(buffer_data_ + header_.zone4.data_offset, header_.zone4.data_size);
    auto encoding_type = static_cast<ValueZoneEncoder::EncodingType>(header_.zone4.encoding_type);
    value_decoder_.emplace(zone_data_[3], encoding_type);
  }

  // Task 3.2：target zone 改为首次访问时懒展开（Open 不再触碰整个 zone，
  // 降低打开大文件的峰值 RSS）。
  return Status::OK();
}

// Task 1.4/1.5 + 3.2：把 target zone 解码成行对齐的 target_values_（懒加载）。
void ZoneColumnarSstReader::EnsureTargetValues() const {
  std::call_once(target_once_, [this]() { DecodeTargetZone(); });
}

void ZoneColumnarSstReader::DecodeTargetZone() const {
  target_values_.assign(header_.row_count, 0);
  if (header_.row_count == 0 || zone_data_[2].empty()) {
    return;
  }

  const Slice data = zone_data_[2];
  const auto embedded_type = [&data]() {
    return static_cast<TargetIdZoneEncoder::EncodingType>(
        static_cast<uint8_t>(data[0]));
  };

  if ((header_.flags & kFlagTargetZoneFileLevel) != 0) {
    const auto type =
        static_cast<TargetIdZoneEncoder::EncodingType>(header_.zone2.encoding_type);
    std::vector<uint64_t> entity_ids;
    if (type == TargetIdZoneEncoder::EncodingType::kDelta && entity_decoder_) {
      entity_ids.resize(header_.row_count);
      for (size_t i = 0; i < header_.row_count; ++i) {
        entity_ids[i] = entity_decoder_->Get(i);
      }
    }
    target_values_ = TargetIdZoneEncoder::DecodeStream(
        data.data(), data.size(), type, header_.row_count, entity_ids);
    return;
  }

  // 旧格式：按 block 切片解码。没有 restart point 时退化为单块。
  std::vector<size_t> block_starts;
  block_starts.reserve(restart_points_.size());
  for (const auto& rp : restart_points_) {
    if (rp.row_index < header_.row_count) {
      block_starts.push_back(rp.row_index);
    }
  }
  if (block_starts.empty()) {
    block_starts.push_back(0);
  }
  std::sort(block_starts.begin(), block_starts.end());
  block_starts.erase(std::unique(block_starts.begin(), block_starts.end()),
                     block_starts.end());

  size_t pos = 0;
  for (size_t b = 0; b < block_starts.size() && pos < data.size(); ++b) {
    const size_t begin = block_starts[b];
    const size_t end =
        (b + 1 < block_starts.size()) ? block_starts[b + 1] : header_.row_count;
    if (begin >= end) continue;

    const auto type = embedded_type();
    std::vector<uint64_t> entity_ids;
    if (type == TargetIdZoneEncoder::EncodingType::kDelta && entity_decoder_) {
      entity_ids.reserve(end - begin);
      for (size_t i = begin; i < end; ++i) {
        entity_ids.push_back(entity_decoder_->Get(i));
      }
    }

    size_t consumed = 0;
    const auto values = TargetIdZoneEncoder::DecodeStream(
        data.data() + pos, data.size() - pos, type, end - begin, entity_ids,
        &consumed);
    for (size_t i = 0; i < end - begin && i < values.size(); ++i) {
      target_values_[begin + i] = values[i];
    }
    if (consumed == 0) break;
    pos += consumed;
  }
}

uint64_t ZoneColumnarSstReader::TargetAt(size_t row_idx) const {
  EnsureTargetValues();
  if (row_idx < target_values_.size()) {
    return target_values_[row_idx];
  }
  return 0;
}

// Task 3.3：加载持久化的紧凑 Entity Index（零拷贝视图）
// 格式：[magic:4B "EIDX"][count:4B][entity_id:8B][first_row:4B] * count
Status ZoneColumnarSstReader::LoadEntityIndex() {
  has_entity_index_ = false;
  entity_index_slice_ = Slice();
  entity_index_count_ = 0;

  if (header_.entity_index_offset == 0 || header_.entity_index_size < 8) {
    return Status::OK();  // 旧文件/小文件没有索引：读端回退线性扫描
  }
  if (header_.entity_index_offset + header_.entity_index_size > buffer_size_) {
    return Status::Corruption("ZoneColumnarSstReader", "invalid entity index offset");
  }

  const char* p = buffer_data_ + header_.entity_index_offset;
  uint32_t magic = 0;
  uint32_t count = 0;
  std::memcpy(&magic, p, 4);
  std::memcpy(&count, p + 4, 4);
  if (magic != 0x58444945) {  // "EIDX"
    return Status::OK();  // 未知格式（更老版本）：回退线性扫描
  }
  if (8 + static_cast<size_t>(count) * 12 != header_.entity_index_size) {
    return Status::Corruption("ZoneColumnarSstReader", "truncated entity index");
  }

  entity_index_slice_ = Slice(p + 8, header_.entity_index_size - 8);
  entity_index_count_ = count;
  has_entity_index_ = true;
  return Status::OK();
}

// Task 3.3：使用持久化索引查找 entity positions（O(log n)）。
// 文件内 entity 连续，[first_row, next.first_row) 即该 entity 的全部行。
std::vector<size_t> ZoneColumnarSstReader::FindEntityPositionsFromIndex(
    uint64_t entity_id) const {
  std::vector<size_t> result;
  const auto range = FindEntityRowRange(entity_id);
  if (range.first >= range.second) return result;
  result.reserve(range.second - range.first);
  for (size_t row = range.first; row < range.second; ++row) {
    result.push_back(row);
  }
  return result;
}

// Task 3.3：entity 行区间；有索引 O(log n)，无索引回退 entity zone 线性扫描。
std::pair<size_t, size_t> ZoneColumnarSstReader::FindEntityRowRange(
    uint64_t entity_id) const {
  if (header_.row_count == 0) return {0, 0};

  if (has_entity_index_ && entity_index_count_ > 0) {
    size_t lo = 0;
    size_t hi = entity_index_count_;
    while (lo < hi) {
      const size_t mid = lo + (hi - lo) / 2;
      const char* entry = entity_index_slice_.data() + mid * 12;
      uint64_t value = 0;
      std::memcpy(&value, entry, 8);
      if (value < entity_id) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    if (lo >= entity_index_count_) return {0, 0};
    const char* entry = entity_index_slice_.data() + lo * 12;
    uint64_t value = 0;
    uint32_t first_row = 0;
    std::memcpy(&value, entry, 8);
    std::memcpy(&first_row, entry + 8, 4);
    if (value != entity_id) return {0, 0};
    size_t end_row = header_.row_count;
    if (lo + 1 < entity_index_count_) {
      const char* next = entity_index_slice_.data() + (lo + 1) * 12;
      uint32_t next_first = 0;
      std::memcpy(&next_first, next + 8, 4);
      end_row = next_first;
    }
    return {first_row, std::min<size_t>(end_row, header_.row_count)};
  }

  // Fallback（旧文件/乱序输入）：线性扫描 entity zone，返回连续区间。
  if (!entity_decoder_) return {0, 0};
  const auto positions = entity_decoder_->FindEntityPositions(entity_id);
  if (positions.empty()) return {0, 0};
  return {positions.front(), positions.back() + 1};
}

void ZoneColumnarSstReader::Close() {
  if (owns_buffer_) {
    view_.Close();
    buffer_data_ = nullptr;
    buffer_size_ = 0;
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

  // 文件头固定了 (entity_type, column_id)；不匹配说明查错文件。
  if (key.entity_type() != static_cast<EntityType>(header_.entity_type)) {
    return std::nullopt;
  }
  if (key.column_id() != header_.column_id) {
    return std::nullopt;
  }

  // Task 1.5（审计 2.4）：点查必须比较完整 user key，否则同 entity/ts、
  // 不同 target（dst/extension）的边会互相串读。
  auto match = [&](size_t idx) -> std::optional<Descriptor> {
    if (timestamp_decoder_->Get(idx) != key.timestamp().value()) return std::nullopt;
    if (TargetAt(idx) != key.target_id()) return std::nullopt;
    if (metadata_decoder_.has_value()) {
      if (metadata_decoder_->GetSequence(idx) != key.sequence()) return std::nullopt;
      if (metadata_decoder_->GetFlags(idx) != key.flags()) return std::nullopt;
    }
    return value_decoder_->Get(idx);
  };

  // Task 3.3：优先用持久化 Entity Index 的行区间（O(log n)），不再复制 positions。
  if (has_entity_index_) {
    const auto range = FindEntityRowRange(key.entity_id());
    for (size_t idx = range.first; idx < range.second; ++idx) {
      if (auto got = match(idx)) return got;
    }
    return std::nullopt;
  }

  // Fallback（旧文件）：entity zone 线性扫描。
  const auto positions = entity_decoder_->FindEntityPositions(key.entity_id());
  for (size_t idx : positions) {
    if (auto got = match(idx)) return got;
  }
  return std::nullopt;
}

std::optional<Descriptor> ZoneColumnarSstReader::GetAtTimeImpl(
    uint64_t entity_id,
    enum EntityType entity_type,
    uint16_t column_id,
    const std::optional<uint64_t>& target_id,
    Timestamp timestamp) const {
  if (!opened_) return std::nullopt;

  // Zone Map 快速过滤
  if (!MayContainEntity(entity_id)) return std::nullopt;
  if (entity_type != static_cast<enum EntityType>(header_.entity_type)) return std::nullopt;
  if (column_id != header_.column_id) return std::nullopt;
  if (!entity_decoder_.has_value() || !timestamp_decoder_.has_value() ||
      !value_decoder_.has_value()) {
    return std::nullopt;
  }

  // 按时间戳降序查找（最新的优先）；target_id 有值时只在该 target 的版本里找
  std::optional<Descriptor> result;
  std::optional<uint64_t> best_ts;
  auto consider = [&](size_t idx) {
    if (target_id.has_value() && TargetAt(idx) != *target_id) return;
    const uint64_t ts = timestamp_decoder_->Get(idx);
    if (ts > timestamp.value()) return;
    if (!best_ts.has_value() || ts > *best_ts) {
      best_ts = ts;
      result = value_decoder_->Get(idx);
    }
  };

  if (has_entity_index_) {
    const auto range = FindEntityRowRange(entity_id);
    for (size_t idx = range.first; idx < range.second; ++idx) consider(idx);
    return result;
  }
  const auto positions = entity_decoder_->FindEntityPositions(entity_id);
  for (size_t idx : positions) consider(idx);
  return result;
}

std::optional<Descriptor> ZoneColumnarSstReader::GetAtTime(
    uint64_t entity_id,
    enum EntityType entity_type,
    uint16_t column_id,
    Timestamp timestamp) const {
  // 旧签名：Vertex 语义，不区分 target（保持既有行为）
  return GetAtTimeImpl(entity_id, entity_type, column_id, std::nullopt, timestamp);
}

std::optional<Descriptor> ZoneColumnarSstReader::GetAtTime(
    uint64_t entity_id,
    enum EntityType entity_type,
    uint16_t column_id,
    uint64_t target_id,
    Timestamp timestamp) const {
  // Task 1.5：按完整 user key（含 target）定位
  return GetAtTimeImpl(entity_id, entity_type, column_id, target_id, timestamp);
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
  if (!entity_decoder_.has_value() || !timestamp_decoder_.has_value() ||
      !value_decoder_.has_value()) {
    return results;
  }

  // Task 3.2/3.3：直接走 entity 行区间（O(log n) 定位 + 只扫该 entity 的行），
  // 不再全文件 FindTimeRange 后逐行过滤（旧实现在大文件上是 O(n)）。
  auto emit = [&](size_t idx) {
    const uint64_t ts = timestamp_decoder_->Get(idx);
    if (ts < start.value() || ts > end.value()) return;
    results.emplace_back(MakeKeyForRow(idx, entity_id), value_decoder_->Get(idx));
  };
  if (has_entity_index_) {
    const auto range = FindEntityRowRange(entity_id);
    for (size_t idx = range.first; idx < range.second; ++idx) emit(idx);
    return results;
  }
  for (size_t idx : entity_decoder_->FindEntityPositions(entity_id)) emit(idx);
  return results;
}

void ZoneColumnarSstReader::ScanTemporalRange(
    uint64_t entity_id,
    Timestamp start,
    Timestamp end,
    std::function<void(const CedarKey&, const Descriptor&)> callback) const {
  if (!opened_) return;
  if (!MayContainTimeRange(start.value(), end.value())) return;
  if (!entity_decoder_.has_value() || !timestamp_decoder_.has_value() ||
      !value_decoder_.has_value()) {
    return;
  }

  auto emit = [&](size_t idx) {
    const uint64_t ts = timestamp_decoder_->Get(idx);
    if (ts < start.value() || ts > end.value()) return;
    callback(MakeKeyForRow(idx, entity_id), value_decoder_->Get(idx));
  };
  if (has_entity_index_) {
    const auto range = FindEntityRowRange(entity_id);
    for (size_t idx = range.first; idx < range.second; ++idx) emit(idx);
    return;
  }
  for (size_t idx : entity_decoder_->FindEntityPositions(entity_id)) emit(idx);
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

// Task 3.2/3.3：批量范围查询按 entity 区间走，而不是全文件时间扫描。
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
  if (!entity_decoder_.has_value() || !timestamp_decoder_.has_value() ||
      !value_decoder_.has_value()) {
    return results;
  }

  for (uint64_t eid : entity_ids) {
    if (!MayContainEntity(eid)) continue;  // Task 3.3：Bloom 直接剪掉文件
    auto emit = [&](size_t idx) {
      const uint64_t ts = timestamp_decoder_->Get(idx);
      if (ts < start.value() || ts > end.value()) return;
      results[eid].emplace_back(MakeKeyForRow(idx, eid), value_decoder_->Get(idx));
    };
    if (has_entity_index_) {
      const auto range = FindEntityRowRange(eid);
      for (size_t idx = range.first; idx < range.second; ++idx) emit(idx);
    } else {
      for (size_t idx : entity_decoder_->FindEntityPositions(eid)) emit(idx);
    }
  }

  return results;
}

bool ZoneColumnarSstReader::MayContainEntity(uint64_t entity_id) const {
  // 首先检查范围
  if (entity_id < header_.min_entity_id || entity_id > header_.max_entity_id) {
    return false;
  }
  // Task 3.3（审计 2.10）：真实 Bloom bitmap（零拷贝视图）；无 filter 时保守返回 true
  return bloom_filter_.MayContain(entity_id);
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
  valid_ = total_count_ > 0 && reader_->entity_decoder_.has_value();
  // Task 3.2：entity 游标懒同步——只读 Value 的扫描（bench/compaction）不付
  // entity 解码成本；Key() 首次调用时才建立游标并前进到当前行。
  entity_cursor_.reset();
  entity_value_ = 0;
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
  if (!Valid()) return;
  ++current_idx_;
  if (current_idx_ >= total_count_) {
    valid_ = false;
  }
}

CedarKey ZoneColumnarIterator::Key() const {
  if (!Valid()) return CedarKey();
  // 顺序扫描时每行只前进 1 步（O(1)）；随机 Seek 时从最近块重新定位。
  if (!entity_cursor_) {
    entity_cursor_ = std::make_unique<EntityIdZoneEncoder::Decoder::Cursor>(
        &*reader_->entity_decoder_);
    if (!entity_cursor_->Seek(current_idx_, &entity_value_)) {
      return CedarKey();
    }
  } else if (entity_cursor_->index() != current_idx_) {
    if (!entity_cursor_->Advance(current_idx_, &entity_value_)) {
      return CedarKey();
    }
  }
  return reader_->MakeKeyForRow(current_idx_, entity_value_);
}

Descriptor ZoneColumnarIterator::Value() const {
  if (!Valid()) return Descriptor();
  return reader_->value_decoder_->Get(current_idx_);
}
// =============================================================================
// ZoneColumnarSstReader 新增方法
// =============================================================================

CedarKey ZoneColumnarSstReader::MakeKeyForRow(size_t row_idx,
                                               uint64_t entity_id) const {
  const uint64_t ts = timestamp_decoder_ ? timestamp_decoder_->Get(row_idx) : 0;
  const uint16_t seq =
      metadata_decoder_ ? metadata_decoder_->GetSequence(row_idx) : 0;
  const uint8_t flags =
      metadata_decoder_ ? metadata_decoder_->GetFlags(row_idx) : 0;
  return CedarKey(entity_id, static_cast<EntityType>(header_.entity_type),
                  header_.column_id, Timestamp(ts), seq, TargetAt(row_idx), flags);
}

CedarKey ZoneColumnarSstReader::ReconstructKey(size_t row_idx) const {
  return ReconstructKey(row_idx, header_);
}

CedarKey ZoneColumnarSstReader::ReconstructKey(
    size_t row_idx, const ZoneColumnarHeader& header) const {
  if (row_idx >= header.row_count) return CedarKey();

  // Task 1.5（审计 2.4/2.5）：用 header + zone2/zone3 重建完整 32B key，
  // 不能再用 CedarKey::Vertex 默认值（会丢 entity_type/target/seq/flags）。
  const uint64_t entity_id =
      entity_decoder_ ? entity_decoder_->Get(row_idx) : 0;
  const uint64_t ts = timestamp_decoder_ ? timestamp_decoder_->Get(row_idx) : 0;
  const uint16_t seq =
      metadata_decoder_ ? metadata_decoder_->GetSequence(row_idx) : 0;
  const uint8_t flags =
      metadata_decoder_ ? metadata_decoder_->GetFlags(row_idx) : 0;

  return CedarKey(entity_id, static_cast<EntityType>(header.entity_type),
                  header.column_id, Timestamp(ts), seq, TargetAt(row_idx), flags);
}

// Task 3.2：按行号随机访问（完整 key + value）
bool ZoneColumnarSstReader::GetRow(size_t row_idx, CedarKey* key,
                                   Descriptor* value) const {
  if (!opened_ || row_idx >= header_.row_count) return false;
  if (!entity_decoder_.has_value() || !value_decoder_.has_value()) return false;
  if (key != nullptr) *key = ReconstructKey(row_idx);
  if (value != nullptr) *value = value_decoder_->Get(row_idx);
  return true;
}

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
