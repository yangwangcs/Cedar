// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/columnar/sst.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <queue>

#include "cedar/cache/cache_manager.h"
#include "cedar/core/crc32c.h"
#include "cedar/index/canonical_value.h"
#include "cedar/runtime/io_governor.h"

namespace cedar {
namespace {
constexpr uint32_t kSstHeaderMagic = 0x54535343U;  // CSST
constexpr uint32_t kFooterMagic = 0x54465343U;     // CSFT
constexpr uint32_t kIndexMagic = 0x58494243U;      // CBIX
constexpr uint32_t kBloomMagic = 0x314d4c42U;   // BLM1
constexpr uint16_t kSstEncodingVersion = 10;
constexpr uint16_t kHeaderSize = 80;
constexpr uint16_t kFooterSize = 136;
constexpr uint32_t kKnownRequiredFeatures = 0;
constexpr uint32_t kKnownOptionalFeatures = 0;
constexpr uint64_t kMaxStatisticsBytes = 1ULL << 20;
constexpr size_t kMaxTypedStatisticValueBytes = 1024;
constexpr uint32_t kMaxBlobRefCount = 1000000;
constexpr uint64_t kMaxBlobRefSetBytes =
    sizeof(uint32_t) + uint64_t{kMaxBlobRefCount} * 32;
constexpr size_t kMaxRowsPerGranuleBlock = 8192;
constexpr uint64_t kTargetUncompressedBlockBytes = 1ULL << 20;
constexpr uint64_t kHardMaxBlockBytes = 4ULL << 20;
constexpr uint8_t kBloomHashCount = 7;
constexpr uint32_t kMinimumBloomBits = 512;
constexpr uint32_t kMaximumBloomBits = 1U << 30;
constexpr uint64_t kMaxFileBloomBytes =
    sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint8_t) +
    uint64_t{kMaximumBloomBits} / 8;
constexpr uint64_t kBlockIndexHeaderBytes = 8;
constexpr uint64_t kBlockIndexEntryBytes = 146;
constexpr uint32_t kGranuleMagic = 0x354b4247U;  // GBK5
constexpr uint16_t kGranuleVersion = 5;
constexpr uint16_t kGranuleHeaderSize = 28;
constexpr size_t kBlobReferenceBytes = 60;

uint64_t SaturatingAdd(uint64_t left, uint64_t right) {
  return right > UINT64_MAX - left ? UINT64_MAX : left + right;
}

bool CheckedRangeEnd(uint64_t offset, uint64_t length, uint64_t limit,
                     uint64_t* end) {
  if (offset > limit || length > limit - offset) return false;
  *end = offset + length;
  return true;
}

uint64_t ElapsedSteadyNs(std::chrono::steady_clock::time_point start) {
  return std::max<uint64_t>(
      1, static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now() - start).count()));
}

void P8(std::string* out, uint8_t value) { out->push_back(static_cast<char>(value)); }
void P16(std::string* out, uint16_t value) { P8(out, value); P8(out, value >> 8); }
void P32(std::string* out, uint32_t value) { for (uint32_t shift = 0; shift < 32; shift += 8) P8(out, value >> shift); }
void P64(std::string* out, uint64_t value) { for (uint32_t shift = 0; shift < 64; shift += 8) P8(out, value >> shift); }
bool G8(const std::string& in, size_t* offset, uint8_t* value) { if (*offset >= in.size()) return false; *value = static_cast<uint8_t>(in[(*offset)++]); return true; }
bool G16(const std::string& in, size_t* offset, uint16_t* value) { uint8_t low, high; if (!G8(in, offset, &low) || !G8(in, offset, &high)) return false; *value = low | (static_cast<uint16_t>(high) << 8); return true; }
bool G32(const std::string& in, size_t* offset, uint32_t* value) { *value = 0; for (uint32_t shift = 0; shift < 32; shift += 8) { uint8_t byte; if (!G8(in, offset, &byte)) return false; *value |= static_cast<uint32_t>(byte) << shift; } return true; }
bool G64(const std::string& in, size_t* offset, uint64_t* value) { *value = 0; for (uint32_t shift = 0; shift < 64; shift += 8) { uint8_t byte; if (!G8(in, offset, &byte)) return false; *value |= static_cast<uint64_t>(byte) << shift; } return true; }
bool ValidPhysical(uint8_t type) { return type >= static_cast<uint8_t>(PhysicalType::kBool) && type <= static_cast<uint8_t>(PhysicalType::kBinary); }
bool ValidEntity(uint8_t value) {
  return value <= static_cast<uint8_t>(EntityType::EdgeIn);
}
bool ValidKeyKind(uint8_t value) {
  return value <= static_cast<uint8_t>(LogicalKeyKind::kProperty);
}
bool ValidCompression(uint8_t value) {
  return value >= static_cast<uint8_t>(CompressionId::kNone) &&
      value <= static_cast<uint8_t>(CompressionId::kZstd);
}

struct BlockIndexEntry {
  uint64_t offset = 0;
  uint64_t length = 0;
  LogicalKey first_key = LogicalKey::VertexExistence(0);
  LogicalKey last_key = LogicalKey::VertexExistence(0);
  uint64_t min_valid_from = 0;
  uint64_t max_valid_from = 0;
  uint64_t min_commit_seq = 0;
  uint64_t max_commit_seq = 0;
  uint32_t row_count = 0;
  bool continuation_before = false;
  bool continuation_after = false;
  BlobHash content_hash;
};

struct SstHeader {
  BlockPartition partition;
  uint32_t required_features = 0;
  uint32_t optional_features = 0;
  uint32_t block_count = 0;
  SstFormatDescriptor format;
  SstFileIdentity identity;
};

bool ValidFormatDescriptor(const SstFormatDescriptor& format) {
  return format.sort_order_id ==
             SstSortOrderId::kLogicalKeyValidFromCommitSeq &&
      format.hash_algorithm_id == SstHashAlgorithmId::kBlake3_256 &&
      format.encoding_registry_id ==
          SstEncodingRegistryId::kCedarPageCodecs &&
      format.compression_registry_id ==
          SstCompressionRegistryId::kCedarPageCompression &&
      format.checksum_algorithm_id == SstChecksumAlgorithmId::kCrc32c;
}

bool IsZeroIdentity(const SstFileIdentity& identity) {
  return std::all_of(identity.bytes.begin(), identity.bytes.end(),
                     [](uint8_t byte) { return byte == 0; });
}

std::string IdentityHex(const SstFileIdentity& identity) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string encoded;
  encoded.reserve(identity.bytes.size() * 2);
  for (uint8_t byte : identity.bytes) {
    encoded.push_back(kHex[byte >> 4]);
    encoded.push_back(kHex[byte & 0x0f]);
  }
  return encoded;
}

std::string EncodeSstHeader(const BlockPartition& partition,
                            uint32_t block_count,
                            const SstFormatDescriptor& format,
                            const SstFileIdentity& identity) {
  std::string header;
  P32(&header, kSstHeaderMagic);
  P16(&header, kSstEncodingVersion);
  P16(&header, kHeaderSize);
  P32(&header, kKnownRequiredFeatures);
  P32(&header, kKnownOptionalFeatures);
  P32(&header, partition.storage_shard_id);
  P8(&header, static_cast<uint8_t>(partition.entity_type));
  P8(&header, static_cast<uint8_t>(partition.key_kind));
  P16(&header, partition.column_id);
  P32(&header, partition.schema_epoch);
  P16(&header, partition.logical_type_id);
  P8(&header, static_cast<uint8_t>(partition.physical_type));
  P16(&header, partition.edge_type);
  P8(&header, static_cast<uint8_t>(partition.compression_id));
  P8(&header, static_cast<uint8_t>(format.sort_order_id));
  P8(&header, static_cast<uint8_t>(format.hash_algorithm_id));
  P8(&header, static_cast<uint8_t>(format.encoding_registry_id));
  P8(&header, static_cast<uint8_t>(format.compression_registry_id));
  P8(&header, static_cast<uint8_t>(format.checksum_algorithm_id));
  P32(&header, block_count);
  header.append(reinterpret_cast<const char*>(identity.bytes.data()),
                identity.bytes.size());
  P8(&header, 0);
  P32(&header, crc32c::Value(header.data(), header.size()));
  return header;
}

StatusOr<SstHeader> DecodeSstHeader(const std::string& bytes) {
  if (bytes.size() < kHeaderSize) {
    return Status::Corruption("sst", "truncated header");
  }
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t epoch = 0;
  uint32_t required_features = 0;
  uint32_t optional_features = 0;
  uint32_t storage_shard_id = 0;
  uint32_t block_count = 0;
  uint32_t stored_checksum = 0;
  uint16_t version = 0;
  uint16_t header_size = 0;
  uint16_t column = 0;
  uint16_t edge_type = 0;
  uint16_t logical_type_id = 0;
  uint8_t entity = 0;
  uint8_t key_kind = 0;
  uint8_t physical = 0;
  uint8_t compression = 0;
  uint8_t sort_order = 0;
  uint8_t hash_algorithm = 0;
  uint8_t encoding_registry = 0;
  uint8_t compression_registry = 0;
  uint8_t checksum_algorithm = 0;
  uint8_t reserved = 0;
  SstFileIdentity identity;
  if (!G32(bytes, &offset, &magic) || !G16(bytes, &offset, &version) ||
      !G16(bytes, &offset, &header_size) ||
      !G32(bytes, &offset, &required_features) ||
      !G32(bytes, &offset, &optional_features) ||
      !G32(bytes, &offset, &storage_shard_id) ||
      !G8(bytes, &offset, &entity) ||
      !G8(bytes, &offset, &key_kind) || !G16(bytes, &offset, &column) ||
      !G32(bytes, &offset, &epoch) ||
      !G16(bytes, &offset, &logical_type_id) ||
      !G8(bytes, &offset, &physical) ||
      !G16(bytes, &offset, &edge_type) || !G8(bytes, &offset, &compression) ||
      !G8(bytes, &offset, &sort_order) ||
      !G8(bytes, &offset, &hash_algorithm) ||
      !G8(bytes, &offset, &encoding_registry) ||
      !G8(bytes, &offset, &compression_registry) ||
      !G8(bytes, &offset, &checksum_algorithm) ||
      !G32(bytes, &offset, &block_count) ||
      offset + identity.bytes.size() > bytes.size()) {
    return Status::Corruption("sst", "invalid header");
  }
  std::memcpy(identity.bytes.data(), bytes.data() + offset,
              identity.bytes.size());
  offset += identity.bytes.size();
  if (!G8(bytes, &offset, &reserved) ||
      !G32(bytes, &offset, &stored_checksum) || offset != kHeaderSize ||
      magic != kSstHeaderMagic || version != kSstEncodingVersion || header_size != kHeaderSize ||
      block_count == 0 || !ValidEntity(entity) || !ValidKeyKind(key_kind) ||
      !ValidPhysical(physical) || !ValidCompression(compression) ||
      logical_type_id == 0 || reserved != 0 || IsZeroIdentity(identity) ||
      stored_checksum != crc32c::Value(bytes.data(), kHeaderSize - sizeof(uint32_t))) {
    return Status::Corruption("sst", "invalid header");
  }
  if ((required_features & ~kKnownRequiredFeatures) != 0) {
    return Status::NotSupported("sst", "unknown required feature bits");
  }
  const SstFormatDescriptor format{
      static_cast<SstSortOrderId>(sort_order),
      static_cast<SstHashAlgorithmId>(hash_algorithm),
      static_cast<SstEncodingRegistryId>(encoding_registry),
      static_cast<SstCompressionRegistryId>(compression_registry),
      static_cast<SstChecksumAlgorithmId>(checksum_algorithm)};
  if (!ValidFormatDescriptor(format)) {
    return Status::NotSupported("sst", "unknown format algorithm ID");
  }
  return SstHeader{
      BlockPartition{static_cast<EntityType>(entity), column, epoch,
                     static_cast<PhysicalType>(physical), edge_type,
                     static_cast<CompressionId>(compression),
                     static_cast<LogicalKeyKind>(key_kind), storage_shard_id,
                     logical_type_id},
      required_features, optional_features, block_count, format, identity};
}

void PutLogicalKey(std::string* out, const LogicalKey& key) {
  P8(out, static_cast<uint8_t>(key.entity_type()));
  P8(out, static_cast<uint8_t>(key.kind()));
  P64(out, key.entity_id()); P64(out, key.target_id());
  P16(out, key.column_id()); P16(out, key.edge_type()); P64(out, key.edge_id());
}

bool GetLogicalKey(const std::string& in, size_t* offset, LogicalKey* key) {
  uint8_t entity = 0;
  uint8_t kind = 0;
  uint64_t entity_id = 0;
  uint64_t target_id = 0;
  uint16_t column = 0;
  uint16_t edge_type = 0;
  uint64_t edge_id = 0;
  if (!G8(in, offset, &entity) || !G8(in, offset, &kind) ||
      !G64(in, offset, &entity_id) || !G64(in, offset, &target_id) ||
      !G16(in, offset, &column) || !G16(in, offset, &edge_type) ||
      !G64(in, offset, &edge_id) || entity > static_cast<uint8_t>(EntityType::EdgeIn) ||
      kind > static_cast<uint8_t>(LogicalKeyKind::kProperty)) return false;
  const EntityType type = static_cast<EntityType>(entity);
  if (kind == static_cast<uint8_t>(LogicalKeyKind::kExistence)) {
    *key = type == EntityType::Vertex ? LogicalKey::VertexExistence(entity_id)
        : LogicalKey::EdgeExistence(entity_id, target_id, edge_type, edge_id, type);
  } else {
    *key = type == EntityType::Vertex ? LogicalKey::VertexProperty(entity_id, column)
        : LogicalKey::EdgeProperty(entity_id, target_id, edge_type, edge_id, column, type);
  }
  return true;
}

void AccumulateFileStatistics(const std::vector<TemporalEvent>& ordered_events,
                              SstFileStatistics* statistics) {
  for (const TemporalEvent& event : ordered_events) {
    if (statistics->row_count == 0) {
      statistics->first_key = event.logical_key();
      statistics->min_valid_from = statistics->max_valid_from =
          event.valid_from();
      statistics->min_commit_seq = statistics->max_commit_seq =
          event.commit_seq();
    }
    statistics->last_key = event.logical_key();
    statistics->min_valid_from =
        std::min(statistics->min_valid_from, event.valid_from());
    statistics->max_valid_from =
        std::max(statistics->max_valid_from, event.valid_from());
    statistics->min_commit_seq =
        std::min(statistics->min_commit_seq, event.commit_seq());
    statistics->max_commit_seq =
        std::max(statistics->max_commit_seq, event.commit_seq());
    ++statistics->row_count;
    if (event.is_delete()) {
      ++statistics->delete_count;
      continue;
    }
    ++statistics->put_count;
    if (event.is_blob_reference()) {
      ++statistics->blob_reference_count;
      statistics->typed_min_max_complete = false;
      statistics->typed_min.reset();
      statistics->typed_max.reset();
      continue;
    }
    ++statistics->inline_value_count;
    ++statistics->typed_value_count;
    const auto canonical = EncodeIndexCanonicalValue(event.value());
    if (!canonical.ok()) {
      if (event.value().type() == PhysicalType::kFloat32 ||
          event.value().type() == PhysicalType::kFloat64) {
        ++statistics->nan_count;
      }
      continue;
    }
    if (!statistics->typed_min_max_complete) continue;
    if (event.value().Encode().size() > kMaxTypedStatisticValueBytes) {
      statistics->typed_min_max_complete = false;
      statistics->typed_min.reset();
      statistics->typed_max.reset();
      continue;
    }
    if (!statistics->typed_min.has_value()) {
      statistics->typed_min = event.value();
      statistics->typed_max = event.value();
      continue;
    }
    const auto canonical_min =
        EncodeIndexCanonicalValue(*statistics->typed_min);
    const auto canonical_max =
        EncodeIndexCanonicalValue(*statistics->typed_max);
    if (canonical_min.ok() &&
        CompareIndexCanonicalValues(canonical.ValueOrDie(),
                                    canonical_min.ValueOrDie()) < 0) {
      statistics->typed_min = event.value();
    }
    if (canonical_max.ok() &&
        CompareIndexCanonicalValues(canonical.ValueOrDie(),
                                    canonical_max.ValueOrDie()) > 0) {
      statistics->typed_max = event.value();
    }
  }
}

SstFileStatistics BuildFileStatistics(
    const std::vector<TemporalEvent>& ordered_events) {
  SstFileStatistics statistics;
  AccumulateFileStatistics(ordered_events, &statistics);
  return statistics;
}

std::string EncodeFileStatistics(const SstFileStatistics& statistics) {
  std::string encoded;
  P32(&encoded, 0x31535453U);  // STS1
  P16(&encoded, 1);
  P16(&encoded, statistics.typed_min_max_complete ? 0 : 1);
  PutLogicalKey(&encoded, statistics.first_key);
  PutLogicalKey(&encoded, statistics.last_key);
  P64(&encoded, statistics.min_valid_from);
  P64(&encoded, statistics.max_valid_from);
  P64(&encoded, statistics.min_commit_seq);
  P64(&encoded, statistics.max_commit_seq);
  P64(&encoded, statistics.row_count);
  P64(&encoded, statistics.put_count);
  P64(&encoded, statistics.delete_count);
  P64(&encoded, statistics.inline_value_count);
  P64(&encoded, statistics.blob_reference_count);
  P64(&encoded, statistics.typed_value_count);
  P64(&encoded, statistics.nan_count);
  const auto append_value = [&encoded](const std::optional<Value>& value) {
    if (!value.has_value()) {
      P32(&encoded, 0);
      return;
    }
    const std::string bytes = value->Encode();
    P32(&encoded, static_cast<uint32_t>(bytes.size()));
    encoded.append(bytes);
  };
  append_value(statistics.typed_min);
  append_value(statistics.typed_max);
  return encoded;
}

StatusOr<SstFileStatistics> DecodeFileStatistics(
    const std::string& encoded, const BlockPartition& partition) {
  if (encoded.empty() || encoded.size() > kMaxStatisticsBytes) {
    return Status::Corruption("sst", "invalid statistics region size");
  }
  size_t offset = 0;
  uint32_t magic = 0;
  uint16_t version = 0;
  uint16_t flags = 0;
  SstFileStatistics statistics;
  if (!G32(encoded, &offset, &magic) || !G16(encoded, &offset, &version) ||
      !G16(encoded, &offset, &flags) || magic != 0x31535453U ||
      version != 1 || (flags & ~uint16_t{1}) != 0 ||
      !GetLogicalKey(encoded, &offset, &statistics.first_key) ||
      !GetLogicalKey(encoded, &offset, &statistics.last_key) ||
      !G64(encoded, &offset, &statistics.min_valid_from) ||
      !G64(encoded, &offset, &statistics.max_valid_from) ||
      !G64(encoded, &offset, &statistics.min_commit_seq) ||
      !G64(encoded, &offset, &statistics.max_commit_seq) ||
      !G64(encoded, &offset, &statistics.row_count) ||
      !G64(encoded, &offset, &statistics.put_count) ||
      !G64(encoded, &offset, &statistics.delete_count) ||
      !G64(encoded, &offset, &statistics.inline_value_count) ||
      !G64(encoded, &offset, &statistics.blob_reference_count) ||
      !G64(encoded, &offset, &statistics.typed_value_count) ||
      !G64(encoded, &offset, &statistics.nan_count)) {
    return Status::Corruption("sst", "invalid statistics region");
  }
  statistics.typed_min_max_complete = (flags & 1) == 0;
  const auto decode_value = [&](std::optional<Value>* value) -> bool {
    uint32_t length = 0;
    if (!G32(encoded, &offset, &length) || length > encoded.size() - offset) {
      return false;
    }
    if (length == 0) {
      value->reset();
      return true;
    }
    *value = Value::Decode(encoded.substr(offset, length));
    offset += length;
    return value->has_value();
  };
  if (!decode_value(&statistics.typed_min) ||
      !decode_value(&statistics.typed_max) || offset != encoded.size() ||
      statistics.row_count == 0 || statistics.last_key < statistics.first_key ||
      statistics.max_valid_from < statistics.min_valid_from ||
      statistics.max_commit_seq < statistics.min_commit_seq ||
      statistics.put_count > statistics.row_count ||
      statistics.delete_count > statistics.row_count ||
      statistics.delete_count !=
          statistics.row_count - statistics.put_count ||
      statistics.inline_value_count > statistics.put_count ||
      statistics.blob_reference_count !=
          statistics.put_count - statistics.inline_value_count ||
      statistics.typed_value_count != statistics.inline_value_count ||
      statistics.nan_count > statistics.typed_value_count ||
      statistics.typed_min.has_value() != statistics.typed_max.has_value() ||
      (!statistics.typed_min_max_complete &&
       statistics.typed_min.has_value()) ||
      (statistics.typed_min.has_value() &&
       (statistics.typed_min->type() != partition.physical_type ||
        statistics.typed_max->type() != partition.physical_type))) {
    return Status::Corruption("sst", "inconsistent statistics region");
  }
  const uint64_t comparable_count =
      statistics.typed_value_count - statistics.nan_count;
  if (statistics.typed_min_max_complete &&
      ((comparable_count == 0) != !statistics.typed_min.has_value())) {
    return Status::Corruption("sst", "invalid typed statistics presence");
  }
  if (statistics.typed_min.has_value()) {
    const auto minimum = EncodeIndexCanonicalValue(*statistics.typed_min);
    const auto maximum = EncodeIndexCanonicalValue(*statistics.typed_max);
    if (!minimum.ok() || !maximum.ok() ||
        CompareIndexCanonicalValues(minimum.ValueOrDie(),
                                    maximum.ValueOrDie()) > 0) {
      return Status::Corruption("sst", "invalid typed statistics range");
    }
  }
  return statistics;
}

SstFileIdentity BuildSstIdentity(
    const BlockPartition& partition,
    const std::vector<BlobHash>& block_hashes,
    const std::string& encoded_blob_refs,
    const std::string& encoded_bloom,
    const std::string& encoded_statistics,
    const std::string& encoded_index) {
  std::string material;
  P32(&material, partition.storage_shard_id);
  P8(&material, static_cast<uint8_t>(partition.entity_type));
  P8(&material, static_cast<uint8_t>(partition.key_kind));
  P16(&material, partition.column_id);
  P32(&material, partition.schema_epoch);
  P16(&material, partition.logical_type_id);
  P8(&material, static_cast<uint8_t>(partition.physical_type));
  P16(&material, partition.edge_type);
  P8(&material, static_cast<uint8_t>(partition.compression_id));
  const SstFormatDescriptor format;
  P8(&material, static_cast<uint8_t>(format.sort_order_id));
  P8(&material, static_cast<uint8_t>(format.hash_algorithm_id));
  P8(&material, static_cast<uint8_t>(format.encoding_registry_id));
  P8(&material, static_cast<uint8_t>(format.compression_registry_id));
  P8(&material, static_cast<uint8_t>(format.checksum_algorithm_id));
  P32(&material, static_cast<uint32_t>(block_hashes.size()));
  for (const BlobHash& hash : block_hashes) {
    material.append(reinterpret_cast<const char*>(hash.bytes.data()),
                    hash.bytes.size());
  }
  P64(&material, encoded_blob_refs.size());
  material.append(encoded_blob_refs);
  P64(&material, encoded_bloom.size());
  material.append(encoded_bloom);
  P64(&material, encoded_statistics.size());
  material.append(encoded_statistics);
  P64(&material, encoded_index.size());
  material.append(encoded_index);
  SstFileIdentity identity;
  identity.bytes = Blake3Hash(material).bytes;
  return identity;
}

std::string EncodeBlockIndex(const std::vector<BlockIndexEntry>& entries) {
  std::string index;
  P32(&index, kIndexMagic);
  P32(&index, static_cast<uint32_t>(entries.size()));
  for (const BlockIndexEntry& entry : entries) {
    P64(&index, entry.offset); P64(&index, entry.length);
    PutLogicalKey(&index, entry.first_key); PutLogicalKey(&index, entry.last_key);
    P64(&index, entry.min_valid_from); P64(&index, entry.max_valid_from);
    P64(&index, entry.min_commit_seq); P64(&index, entry.max_commit_seq);
    P32(&index, entry.row_count);
    P8(&index, entry.continuation_before ? 1 : 0);
    P8(&index, entry.continuation_after ? 1 : 0);
    index.append(reinterpret_cast<const char*>(entry.content_hash.bytes.data()),
                 entry.content_hash.bytes.size());
  }
  return index;
}

StatusOr<std::vector<BlockIndexEntry>> DecodeBlockIndex(const std::string& index,
                                                         uint32_t expected_count) {
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t count = 0;
  if (!G32(index, &offset, &magic) || !G32(index, &offset, &count) ||
      magic != kIndexMagic || count != expected_count ||
      index.size() - offset != static_cast<size_t>(count) *
          kBlockIndexEntryBytes) {
    return Status::Corruption("sst", "invalid block index");
  }
  std::vector<BlockIndexEntry> entries;
  entries.reserve(count);
  for (uint32_t position = 0; position < count; ++position) {
    BlockIndexEntry entry;
    if (!G64(index, &offset, &entry.offset) || !G64(index, &offset, &entry.length) ||
        !GetLogicalKey(index, &offset, &entry.first_key) ||
        !GetLogicalKey(index, &offset, &entry.last_key) ||
        !G64(index, &offset, &entry.min_valid_from) ||
        !G64(index, &offset, &entry.max_valid_from) ||
        !G64(index, &offset, &entry.min_commit_seq) ||
        !G64(index, &offset, &entry.max_commit_seq) ||
        !G32(index, &offset, &entry.row_count)) {
      return Status::Corruption("sst", "invalid block index entry");
    }
    uint8_t continuation_before = 0;
    uint8_t continuation_after = 0;
    if (!G8(index, &offset, &continuation_before) ||
        !G8(index, &offset, &continuation_after) ||
        offset + entry.content_hash.bytes.size() > index.size() ||
        continuation_before > 1 ||
        continuation_after > 1 || entry.length == 0 ||
        entry.row_count == 0 || entry.row_count > kMaxRowsPerGranuleBlock ||
        entry.last_key < entry.first_key || entry.max_valid_from < entry.min_valid_from ||
        entry.max_commit_seq < entry.min_commit_seq ||
        (!entries.empty() && entry.first_key < entries.back().first_key) ||
        (!entries.empty() &&
         ((continuation_before == 1) != (entries.back().continuation_after) ||
          (continuation_before == 1) != (entry.first_key == entries.back().last_key)))) {
      return Status::Corruption("sst", "invalid block index entry");
    }
    std::memcpy(entry.content_hash.bytes.data(), index.data() + offset,
                entry.content_hash.bytes.size());
    offset += entry.content_hash.bytes.size();
    if (std::all_of(entry.content_hash.bytes.begin(),
                    entry.content_hash.bytes.end(),
                    [](uint8_t byte) { return byte == 0; })) {
      return Status::Corruption("sst", "zero block content hash");
    }
    entry.continuation_before = continuation_before == 1;
    entry.continuation_after = continuation_after == 1;
    if (entries.empty() && entry.continuation_before) {
      return Status::Corruption("sst", "first block cannot continue");
    }
    entries.push_back(std::move(entry));
  }
  if (!entries.empty() && entries.back().continuation_after) {
    return Status::Corruption("sst", "last block cannot continue");
  }
  return entries;
}

Status ValidatePersistedFileIdentity(
    const BlockPartition& partition,
    const std::vector<BlockIndexEntry>& block_index,
    const std::string& encoded_blob_refs, const std::string& encoded_bloom,
    const std::string& encoded_statistics, const std::string& encoded_index,
    const SstFileIdentity& expected_identity) {
  std::vector<BlobHash> block_hashes;
  block_hashes.reserve(block_index.size());
  for (const BlockIndexEntry& block : block_index) {
    block_hashes.push_back(block.content_hash);
  }
  if (BuildSstIdentity(partition, block_hashes, encoded_blob_refs,
                       encoded_bloom, encoded_statistics, encoded_index) !=
      expected_identity) {
    return Status::Corruption("sst", "persisted file identity mismatch");
  }
  return Status::OK();
}

uint64_t EstimateEventBytes(const TemporalEvent& event) {
  constexpr uint64_t kSystemColumnBytes = 128;
  return kSystemColumnBytes + (event.is_blob_reference()
      ? 60ULL
      : static_cast<uint64_t>(event.value().Encode().size()));
}

size_t FindAdaptiveBlockEnd(const std::vector<TemporalEvent>& events, size_t begin) {
  size_t end = begin;
  uint64_t estimated_bytes = 0;
  while (end < events.size()) {
    size_t group_end = end + 1;
    uint64_t group_bytes = EstimateEventBytes(events[end]);
    while (group_end < events.size() &&
           events[group_end].logical_key() == events[end].logical_key()) {
      group_bytes += EstimateEventBytes(events[group_end++]);
    }
    const size_t group_rows = group_end - end;
    if (end != begin &&
        (end - begin + group_rows > kMaxRowsPerGranuleBlock ||
         estimated_bytes + group_bytes > kTargetUncompressedBlockBytes)) {
      break;
    }
    if (end == begin &&
        (group_rows > kMaxRowsPerGranuleBlock || group_bytes > kHardMaxBlockBytes)) {
      // A single long version chain may cross the hard boundary. Keep each
      // continuation bounded where possible and mark it in the block index.
      size_t forced_end = begin;
      uint64_t forced_bytes = 0;
      while (forced_end < group_end && forced_end - begin < kMaxRowsPerGranuleBlock) {
        const uint64_t next_bytes = EstimateEventBytes(events[forced_end]);
        if (forced_end != begin && forced_bytes + next_bytes > kHardMaxBlockBytes) break;
        forced_bytes += next_bytes;
        ++forced_end;
      }
      return forced_end;
    }
    end = group_end;
    estimated_bytes += group_bytes;
  }
  return end;
}

std::vector<BlobHash> CollectBlobRefs(const std::vector<TemporalEvent>& events) {
  std::vector<BlobHash> hashes;
  for (const TemporalEvent& event : events) if (event.blob_ref().has_value()) hashes.push_back(event.blob_ref()->content_hash);
  std::sort(hashes.begin(), hashes.end(), [](const BlobHash& left, const BlobHash& right) { return left.bytes < right.bytes; });
  hashes.erase(std::unique(hashes.begin(), hashes.end()), hashes.end());
  return hashes;
}
std::string EncodeBlobRefs(const std::vector<BlobHash>& hashes) {
  std::string encoded;
  P32(&encoded, static_cast<uint32_t>(hashes.size()));
  for (const BlobHash& hash : hashes) encoded.append(reinterpret_cast<const char*>(hash.bytes.data()), hash.bytes.size());
  return encoded;
}
StatusOr<std::vector<BlobHash>> DecodeBlobRefs(const std::string& encoded) {
  size_t offset = 0;
  uint32_t count;
  if (!G32(encoded, &offset, &count) || count > kMaxBlobRefCount || encoded.size() - offset != static_cast<size_t>(count) * 32) return Status::Corruption("sst", "invalid BlobRefSet");
  std::vector<BlobHash> hashes;
  hashes.reserve(count);
  for (uint32_t index = 0; index < count; ++index) {
    BlobHash hash;
    std::memcpy(hash.bytes.data(), encoded.data() + offset, hash.bytes.size());
    offset += hash.bytes.size();
    if (!hashes.empty() && !(hashes.back().bytes < hash.bytes)) return Status::Corruption("sst", "BlobRefSet is not sorted and unique");
    hashes.push_back(hash);
  }
  return hashes;
}
struct Footer {
  uint64_t index_offset = 0;
  uint64_t index_length = 0;
  uint64_t blob_refs_offset = 0;
  uint64_t blob_refs_length = 0;
  uint64_t bloom_offset = 0;
  uint64_t bloom_length = 0;
  uint64_t statistics_offset = 0;
  uint64_t statistics_length = 0;
  uint32_t index_crc = 0;
  uint32_t blob_refs_crc = 0;
  uint32_t bloom_crc = 0;
  uint32_t statistics_crc = 0;
  uint64_t row_count = 0;
  uint32_t block_count = 0;
  SstFileIdentity identity;
};

struct FileBloom {
  uint32_t bit_count = 0;
  uint8_t hash_count = 0;
  std::string bits;
};

uint64_t BloomHash(const LogicalKey& key, uint64_t seed) {
  return key.StableHash(seed);
}

std::string EncodeFileBloom(const std::vector<TemporalEvent>& events) {
  const uint64_t requested_bits = std::max<uint64_t>(
      kMinimumBloomBits, static_cast<uint64_t>(events.size()) * 10);
  const uint32_t bit_count = static_cast<uint32_t>(std::min<uint64_t>(
      kMaximumBloomBits, (requested_bits + 7) & ~uint64_t{7}));
  std::string bits(bit_count / 8, '\0');
  for (const TemporalEvent& event : events) {
    const uint64_t first = BloomHash(event.logical_key(), 0x9e3779b97f4a7c15ULL);
    uint64_t second = BloomHash(event.logical_key(), 0xd6e8feb86659fd93ULL);
    if (second == 0) second = 0x94d049bb133111ebULL;
    for (uint8_t hash = 0; hash < kBloomHashCount; ++hash) {
      const uint32_t bit = static_cast<uint32_t>((first + hash * second) % bit_count);
      bits[bit / 8] = static_cast<char>(static_cast<uint8_t>(bits[bit / 8]) |
                                        (uint8_t{1} << (bit % 8)));
    }
  }
  std::string encoded;
  P32(&encoded, kBloomMagic);
  P32(&encoded, bit_count);
  P8(&encoded, kBloomHashCount);
  encoded.append(bits);
  return encoded;
}

StatusOr<FileBloom> DecodeFileBloom(const std::string& encoded) {
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t bit_count = 0;
  uint8_t hash_count = 0;
  if (!G32(encoded, &offset, &magic) || !G32(encoded, &offset, &bit_count) ||
      !G8(encoded, &offset, &hash_count) || magic != kBloomMagic ||
      bit_count < kMinimumBloomBits || bit_count > kMaximumBloomBits ||
      bit_count % 8 != 0 || hash_count == 0 || hash_count > 16 ||
      encoded.size() - offset != bit_count / 8) {
    return Status::Corruption("sst", "invalid file Bloom");
  }
  return FileBloom{bit_count, hash_count, encoded.substr(offset)};
}

bool MayContain(const FileBloom& bloom, const LogicalKey& key) {
  const uint64_t first = BloomHash(key, 0x9e3779b97f4a7c15ULL);
  uint64_t second = BloomHash(key, 0xd6e8feb86659fd93ULL);
  if (second == 0) second = 0x94d049bb133111ebULL;
  for (uint8_t hash = 0; hash < bloom.hash_count; ++hash) {
    const uint32_t bit = static_cast<uint32_t>((first + hash * second) % bloom.bit_count);
    if ((static_cast<uint8_t>(bloom.bits[bit / 8]) & (uint8_t{1} << (bit % 8))) == 0) {
      return false;
    }
  }
  return true;
}

std::string EncodeFooter(const Footer& footer) {
  std::string encoded;
  P32(&encoded, kFooterMagic);
  P16(&encoded, kSstEncodingVersion);
  P16(&encoded, kFooterSize);
  P64(&encoded, footer.index_offset);
  P64(&encoded, footer.index_length);
  P64(&encoded, footer.blob_refs_offset);
  P64(&encoded, footer.blob_refs_length);
  P64(&encoded, footer.bloom_offset);
  P64(&encoded, footer.bloom_length);
  P64(&encoded, footer.statistics_offset);
  P64(&encoded, footer.statistics_length);
  P32(&encoded, footer.index_crc);
  P32(&encoded, footer.blob_refs_crc);
  P32(&encoded, footer.bloom_crc);
  P32(&encoded, footer.statistics_crc);
  P64(&encoded, footer.row_count);
  P32(&encoded, footer.block_count);
  encoded.append(reinterpret_cast<const char*>(footer.identity.bytes.data()),
                 footer.identity.bytes.size());
  P32(&encoded, crc32c::Value(encoded.data(), encoded.size()));
  return encoded;
}

StatusOr<Footer> DecodeFooterBytes(const std::string& bytes,
                                   uint64_t file_size) {
  if (bytes.size() != kFooterSize || file_size < kHeaderSize + kFooterSize) {
    return Status::Corruption("sst", "truncated footer");
  }
  size_t offset = 0;
  uint32_t magic = 0;
  uint16_t version = 0;
  uint16_t size = 0;
  uint32_t stored_checksum = 0;
  Footer footer;
  if (!G32(bytes, &offset, &magic) || !G16(bytes, &offset, &version) ||
      !G16(bytes, &offset, &size) ||
      !G64(bytes, &offset, &footer.index_offset) ||
      !G64(bytes, &offset, &footer.index_length) ||
      !G64(bytes, &offset, &footer.blob_refs_offset) ||
      !G64(bytes, &offset, &footer.blob_refs_length) ||
      !G64(bytes, &offset, &footer.bloom_offset) ||
      !G64(bytes, &offset, &footer.bloom_length) ||
      !G64(bytes, &offset, &footer.statistics_offset) ||
      !G64(bytes, &offset, &footer.statistics_length) ||
      !G32(bytes, &offset, &footer.index_crc) ||
      !G32(bytes, &offset, &footer.blob_refs_crc) ||
      !G32(bytes, &offset, &footer.bloom_crc) ||
      !G32(bytes, &offset, &footer.statistics_crc) ||
      !G64(bytes, &offset, &footer.row_count) ||
      !G32(bytes, &offset, &footer.block_count) ||
      offset + footer.identity.bytes.size() > bytes.size()) {
    return Status::Corruption("sst", "invalid footer");
  }
  std::memcpy(footer.identity.bytes.data(), bytes.data() + offset,
              footer.identity.bytes.size());
  offset += footer.identity.bytes.size();
  const uint64_t metadata_end = file_size - kFooterSize;
  uint64_t blob_refs_end = 0;
  uint64_t bloom_end = 0;
  uint64_t statistics_end = 0;
  uint64_t index_end = 0;
  const uint64_t expected_index_length =
      kBlockIndexHeaderBytes +
      static_cast<uint64_t>(footer.block_count) * kBlockIndexEntryBytes;
  if (!G32(bytes, &offset, &stored_checksum) || offset != kFooterSize ||
      magic != kFooterMagic || version != kSstEncodingVersion ||
      size != kFooterSize || footer.block_count == 0 ||
      footer.row_count == 0 || IsZeroIdentity(footer.identity) ||
      stored_checksum !=
          crc32c::Value(bytes.data(), kFooterSize - sizeof(uint32_t)) ||
      footer.blob_refs_offset < kHeaderSize ||
      footer.blob_refs_length > kMaxBlobRefSetBytes ||
      !CheckedRangeEnd(footer.blob_refs_offset, footer.blob_refs_length,
                       metadata_end, &blob_refs_end) ||
      footer.bloom_offset != blob_refs_end ||
      footer.bloom_length > kMaxFileBloomBytes ||
      !CheckedRangeEnd(footer.bloom_offset, footer.bloom_length,
                       metadata_end, &bloom_end) ||
      footer.statistics_offset != bloom_end ||
      footer.statistics_length == 0 ||
      footer.statistics_length > kMaxStatisticsBytes ||
      !CheckedRangeEnd(footer.statistics_offset, footer.statistics_length,
                       metadata_end, &statistics_end) ||
      footer.index_offset != statistics_end ||
      footer.index_length != expected_index_length ||
      !CheckedRangeEnd(footer.index_offset, footer.index_length,
                       metadata_end, &index_end) ||
      index_end != metadata_end) {
    return Status::Corruption("sst", "invalid footer");
  }
  return footer;
}

StatusOr<Footer> ReadFooter(const std::string& bytes) {
  if (bytes.size() < kHeaderSize + kFooterSize) {
    return Status::Corruption("sst", "truncated file");
  }
  return DecodeFooterBytes(bytes.substr(bytes.size() - kFooterSize),
                           bytes.size());
}

Status ValidateHeaderAndFooter(const SstHeader& header,
                               const Footer& footer) {
  if (header.block_count != footer.block_count ||
      header.identity != footer.identity) {
    return Status::Corruption("sst", "header/footer ownership mismatch");
  }
  return Status::OK();
}

Status ValidateIndexStatistics(
    const std::vector<BlockIndexEntry>& index,
    const SstFileStatistics& statistics) {
  if (index.empty() || index.front().first_key != statistics.first_key ||
      index.back().last_key != statistics.last_key) {
    return Status::Corruption("sst", "statistics key range mismatch");
  }
  uint64_t min_valid_from = index.front().min_valid_from;
  uint64_t max_valid_from = index.front().max_valid_from;
  uint64_t min_commit_seq = index.front().min_commit_seq;
  uint64_t max_commit_seq = index.front().max_commit_seq;
  uint64_t row_count = 0;
  for (const BlockIndexEntry& block : index) {
    min_valid_from = std::min(min_valid_from, block.min_valid_from);
    max_valid_from = std::max(max_valid_from, block.max_valid_from);
    min_commit_seq = std::min(min_commit_seq, block.min_commit_seq);
    max_commit_seq = std::max(max_commit_seq, block.max_commit_seq);
    if (block.row_count > UINT64_MAX - row_count) {
      return Status::Corruption("sst", "block row count overflow");
    }
    row_count += block.row_count;
  }
  if (min_valid_from != statistics.min_valid_from ||
      max_valid_from != statistics.max_valid_from ||
      min_commit_seq != statistics.min_commit_seq ||
      max_commit_seq != statistics.max_commit_seq ||
      row_count != statistics.row_count) {
    return Status::Corruption("sst", "statistics temporal range mismatch");
  }
  return Status::OK();
}
Status ReadRange(int fd, uint64_t offset, uint64_t length, const std::string& path,
                 std::string* bytes) {
  if (length > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
    return Status::Corruption("sst", "requested read is too large");
  }
  bytes->assign(static_cast<size_t>(length), '\0');
  size_t copied = 0;
  while (copied < bytes->size()) {
    const ssize_t count = ::pread(fd, bytes->data() + copied, bytes->size() - copied,
                                  static_cast<off_t>(offset + copied));
    if (count < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    if (count == 0) return Status::Corruption("sst", "unexpected end of file");
    copied += static_cast<size_t>(count);
  }
  return Status::OK();
}

PhysicalType ExpectedPagePhysicalType(PageType type,
                                      const BlockPartition& partition) {
  switch (type) {
    case PageType::kEntityId:
    case PageType::kTargetId:
    case PageType::kCommitSeq:
    case PageType::kEdgeId:
      return PhysicalType::kInt64;
    case PageType::kValidFrom:
      return PhysicalType::kTimestamp64;
    case PageType::kOperation:
    case PageType::kInlinePresence:
    case PageType::kBlobPresence:
      return PhysicalType::kBool;
    case PageType::kValueClass:
      return PhysicalType::kInt32;
    case PageType::kTypedValue:
      return partition.physical_type;
    case PageType::kBlobRef:
      return PhysicalType::kBinary;
  }
  return partition.physical_type;
}

LogicalKey MakePartitionKey(const BlockPartition& partition, uint64_t entity_id,
                            uint64_t target_id, uint64_t edge_id) {
  if (partition.entity_type == EntityType::Vertex) {
    return partition.key_kind == LogicalKeyKind::kExistence
        ? LogicalKey::VertexExistence(entity_id)
        : LogicalKey::VertexProperty(entity_id, partition.column_id);
  }
  return partition.key_kind == LogicalKeyKind::kExistence
      ? LogicalKey::EdgeExistence(entity_id, target_id, partition.edge_type,
                                  edge_id, partition.entity_type)
      : LogicalKey::EdgeProperty(entity_id, target_id, partition.edge_type,
                                 edge_id, partition.column_id,
                                 partition.entity_type);
}

bool ReadU64Payload(const std::string& payload, uint32_t rows,
                    std::vector<uint64_t>* values) {
  if (payload.size() != static_cast<size_t>(rows) * sizeof(uint64_t)) return false;
  size_t offset = 0;
  values->clear();
  values->reserve(rows);
  for (uint32_t row = 0; row < rows; ++row) {
    uint64_t value = 0;
    if (!G64(payload, &offset, &value)) return false;
    values->push_back(value);
  }
  return true;
}

bool ReadBlobReference(const std::string& payload, size_t* offset,
                       BlobRef* reference) {
  if (payload.size() - *offset < kBlobReferenceBytes) return false;
  std::memcpy(reference->content_hash.bytes.data(), payload.data() + *offset,
              reference->content_hash.bytes.size());
  *offset += reference->content_hash.bytes.size();
  return G64(payload, offset, &reference->raw_length) &&
         G32(payload, offset, &reference->hint.shard_id) &&
         G64(payload, offset, &reference->hint.segment_id) &&
         G64(payload, offset, &reference->hint.offset);
}

StatusOr<Page> ReadSelectedPage(
    int fd, const std::string& path, uint64_t block_offset,
    const PageDirectoryEntry& entry, const SstFileIdentity& file_identity,
    CacheManager* cache_manager, SstReadStats* stats, bool value_page) {
  const CacheKey key{CacheKind::kPage,
      path + ":" + IdentityHex(file_identity) + ":" +
      std::to_string(block_offset) + ":" +
      std::to_string(static_cast<uint8_t>(entry.page_type)) + ":" +
      std::to_string(entry.ordinal) + ":v" + std::to_string(kSstEncodingVersion)};
  std::string encoded;
  const CacheHandle cached = cache_manager == nullptr
      ? CacheHandle{} : cache_manager->Lookup(key);
  if (cached) {
    encoded = *cached.value();
  } else {
    const Status read = ReadRange(fd, block_offset + entry.offset, entry.length,
                                  path, &encoded);
    if (!read.ok()) return read;
    if (stats != nullptr) stats->bytes_read += entry.length;
    if (cache_manager != nullptr) {
      const auto inserted = cache_manager->Insert(
          key, std::make_shared<const std::string>(encoded),
          CacheAdmission::kPointRead);
      if (!inserted.ok() && !inserted.status().IsQueryMemoryLimit()) {
        return inserted.status();
      }
    }
  }
  if (Blake3Hash(encoded).bytes != entry.content_hash) {
    return Status::Corruption("sst", "page content hash mismatch");
  }
  const auto decode_started = std::chrono::steady_clock::now();
  const auto page = DecodePage(encoded);
  const uint64_t decode_latency_ns = ElapsedSteadyNs(decode_started);
  if (!page.ok()) return page.status();
  if (page.ValueOrDie().header.page_type != entry.page_type) {
    return Status::Corruption("sst", "page directory type mismatch");
  }
  if (stats != nullptr) {
    if (value_page) ++stats->value_pages_read;
    else ++stats->system_pages_read;
    stats->page_bytes_decoded += page.ValueOrDie().header.uncompressed_size;
    ++stats->page_decode_count;
    stats->page_decode_latency_ns += decode_latency_ns;
  }
  return page;
}

StatusOr<std::vector<TemporalEvent>> ReadGranuleCandidatesForKey(
    int fd, const std::string& path, uint64_t block_offset,
    uint64_t block_length, const BlockPartition& partition,
    const LogicalKey& key, const SstFileIdentity& file_identity,
    const BlobHash& expected_block_identity, CacheManager* cache_manager,
    SstReadStats* stats) {
  if (block_length < kGranuleHeaderSize) {
    return Status::Corruption("granule", "truncated header");
  }
  std::string header;
  Status status = ReadRange(fd, block_offset, kGranuleHeaderSize, path, &header);
  if (!status.ok()) return status;
  if (stats != nullptr) stats->bytes_read += kGranuleHeaderSize;
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t rows = 0;
  uint16_t version = 0;
  uint16_t header_size = 0;
  uint64_t directory_offset = 0;
  uint64_t directory_length = 0;
  if (!G32(header, &offset, &magic) || !G16(header, &offset, &version) ||
      !G16(header, &offset, &header_size) || !G32(header, &offset, &rows) ||
      !G64(header, &offset, &directory_offset) ||
      !G64(header, &offset, &directory_length) || magic != kGranuleMagic ||
      version != kGranuleVersion || header_size != kGranuleHeaderSize || rows == 0 ||
      directory_offset < kGranuleHeaderSize || directory_offset > block_length ||
      directory_length > block_length - directory_offset) {
    return Status::Corruption("granule", "invalid header");
  }
  std::string encoded_directory;
  status = ReadRange(fd, block_offset + directory_offset, directory_length,
                     path, &encoded_directory);
  if (!status.ok()) return status;
  if (stats != nullptr) stats->bytes_read += directory_length;
  const auto decoded_directory = DecodePageDirectory(encoded_directory);
  if (!decoded_directory.ok()) return decoded_directory.status();
  if (ComputeGranuleBlockIdentity(header, encoded_directory) !=
      expected_block_identity) {
    return Status::Corruption("sst", "GranuleBlock identity mismatch");
  }

  std::map<PageType, std::vector<PageDirectoryEntry>> entries_by_type;
  for (const PageDirectoryEntry& entry : decoded_directory.ValueOrDie()) {
    if (entry.offset < kGranuleHeaderSize || entry.offset > directory_offset ||
        entry.length > directory_offset - entry.offset ||
        entry.length < kPageFormatHeaderSize) {
      return Status::Corruption("granule", "invalid page location");
    }
    entries_by_type[entry.page_type].push_back(entry);
  }
  for (auto& typed_entries : entries_by_type) {
    std::sort(typed_entries.second.begin(), typed_entries.second.end(),
              [](const PageDirectoryEntry& left,
                 const PageDirectoryEntry& right) {
      return left.ordinal < right.ordinal;
    });
    for (uint32_t ordinal = 0; ordinal < typed_entries.second.size(); ++ordinal) {
      if (typed_entries.second[ordinal].ordinal != ordinal) {
        return Status::Corruption("granule", "non-contiguous page ordinals");
      }
    }
  }

  std::map<PageType, Page> system_pages;
  std::vector<PageType> required = {
      PageType::kEntityId, PageType::kValidFrom, PageType::kCommitSeq,
      PageType::kOperation, PageType::kValueClass,
      PageType::kInlinePresence, PageType::kBlobPresence};
  if (partition.entity_type != EntityType::Vertex) {
    required.push_back(PageType::kTargetId);
    required.push_back(PageType::kEdgeId);
  }
  for (PageType type : required) {
    const auto found = entries_by_type.find(type);
    if (found == entries_by_type.end() || found->second.size() != 1) {
      return Status::Corruption("granule", "required system page missing");
    }
    const auto page = ReadSelectedPage(
        fd, path, block_offset, found->second.front(), file_identity,
        cache_manager, stats, false);
    if (!page.ok()) return page.status();
    if (page.ValueOrDie().header.physical_type !=
            ExpectedPagePhysicalType(type, partition) ||
        page.ValueOrDie().header.first_row != 0 ||
        page.ValueOrDie().header.row_count != rows ||
        page.ValueOrDie().header.value_count != rows) {
      return Status::Corruption("granule", "invalid system page metadata");
    }
    system_pages.emplace(type, page.ValueOrDie());
  }

  std::vector<uint64_t> entity_ids;
  std::vector<uint64_t> target_ids(rows, 0);
  std::vector<uint64_t> edge_ids(rows, 0);
  std::vector<uint64_t> valid_from;
  std::vector<uint64_t> commit_seq;
  if (!ReadU64Payload(system_pages.at(PageType::kEntityId).payload, rows,
                      &entity_ids) ||
      !ReadU64Payload(system_pages.at(PageType::kValidFrom).payload, rows,
                      &valid_from) ||
      !ReadU64Payload(system_pages.at(PageType::kCommitSeq).payload, rows,
                      &commit_seq) ||
      system_pages.at(PageType::kOperation).payload.size() != rows ||
      system_pages.at(PageType::kValueClass).payload.size() != rows ||
      system_pages.at(PageType::kInlinePresence).payload.size() != rows ||
      system_pages.at(PageType::kBlobPresence).payload.size() != rows) {
    return Status::Corruption("granule", "invalid system page payload");
  }
  if (partition.entity_type != EntityType::Vertex &&
      (!ReadU64Payload(system_pages.at(PageType::kTargetId).payload, rows,
                       &target_ids) ||
       !ReadU64Payload(system_pages.at(PageType::kEdgeId).payload, rows,
                       &edge_ids))) {
    return Status::Corruption("granule", "invalid edge identity page");
  }
  const std::string& operations = system_pages.at(PageType::kOperation).payload;
  const std::string& value_classes = system_pages.at(PageType::kValueClass).payload;
  const std::string& inline_presence =
      system_pages.at(PageType::kInlinePresence).payload;
  const std::string& blob_presence =
      system_pages.at(PageType::kBlobPresence).payload;
  std::vector<uint32_t> selected_rows;
  for (uint32_t row = 0; row < rows; ++row) {
    const uint8_t operation = static_cast<uint8_t>(operations[row]);
    const uint8_t value_class = static_cast<uint8_t>(value_classes[row]);
    if (operation > 1 || (operation == 1 && value_class != 0) ||
        (operation == 0 && value_class != 1 && value_class != 2)) {
      return Status::Corruption("granule", "invalid value class");
    }
    if ((inline_presence[row] != 0 && inline_presence[row] != 1) ||
        (blob_presence[row] != 0 && blob_presence[row] != 1) ||
        inline_presence[row] != static_cast<char>(value_class == 1) ||
        blob_presence[row] != static_cast<char>(value_class == 2) ||
        (inline_presence[row] != 0 && blob_presence[row] != 0)) {
      return Status::Corruption("granule", "presence bitmap disagrees with value class");
    }
    if (MakePartitionKey(partition, entity_ids[row], target_ids[row],
                         edge_ids[row]) == key) {
      selected_rows.push_back(row);
    }
  }
  if (selected_rows.empty()) return std::vector<TemporalEvent>{};

  std::map<uint32_t, Value> inline_values;
  std::map<uint32_t, BlobRef> blob_references;
  for (PageType type : {PageType::kTypedValue, PageType::kBlobRef}) {
    const auto found = entries_by_type.find(type);
    if (found == entries_by_type.end() || found->second.empty()) {
      return Status::Corruption("granule", "required value page missing");
    }
    for (const PageDirectoryEntry& entry : found->second) {
      std::string encoded_header;
      status = ReadRange(fd, block_offset + entry.offset,
                         kPageFormatHeaderSize, path, &encoded_header);
      if (!status.ok()) return status;
      if (stats != nullptr) stats->bytes_read += kPageFormatHeaderSize;
      const auto page_header = DecodePageHeader(encoded_header);
      if (!page_header.ok()) return page_header.status();
      if (page_header.ValueOrDie().page_type != type ||
          page_header.ValueOrDie().physical_type !=
              ExpectedPagePhysicalType(type, partition) ||
          page_header.ValueOrDie().first_row > rows ||
          page_header.ValueOrDie().row_count >
              rows - page_header.ValueOrDie().first_row ||
          page_header.ValueOrDie().compressed_size !=
              entry.length - kPageFormatHeaderSize) {
        return Status::Corruption("granule", "invalid value page metadata");
      }
      const uint64_t first_row = page_header.ValueOrDie().first_row;
      const uint64_t row_end = first_row + page_header.ValueOrDie().row_count;
      const uint8_t wanted_class = type == PageType::kTypedValue ? 1 : 2;
      const bool needed = std::any_of(
          selected_rows.begin(), selected_rows.end(), [&](uint32_t row) {
        return row >= first_row && row < row_end &&
               static_cast<uint8_t>(value_classes[row]) == wanted_class;
      });
      if (!needed) {
        if (stats != nullptr) {
          stats->page_bytes_skipped +=
              entry.length - kPageFormatHeaderSize;
        }
        continue;
      }
      const auto page = ReadSelectedPage(fd, path, block_offset, entry,
                                         file_identity, cache_manager, stats,
                                         true);
      if (!page.ok()) return page.status();
      size_t payload_offset = 0;
      uint32_t value_count = 0;
      for (uint32_t row = static_cast<uint32_t>(first_row); row < row_end; ++row) {
        if (static_cast<uint8_t>(value_classes[row]) != wanted_class) continue;
        ++value_count;
        if (type == PageType::kTypedValue) {
          std::optional<Value> value;
          if (!DecodeGranuleInlineValue(page.ValueOrDie().payload,
                                        &payload_offset,
                                        partition.physical_type, &value)) {
            return Status::Corruption("granule", "invalid inline value");
          }
          inline_values.emplace(row, *value);
        } else {
          BlobRef reference;
          if (!ReadBlobReference(page.ValueOrDie().payload, &payload_offset,
                                 &reference)) {
            return Status::Corruption("granule", "invalid blob reference");
          }
          blob_references.emplace(row, std::move(reference));
        }
      }
      if (value_count != page.ValueOrDie().header.value_count ||
          payload_offset != page.ValueOrDie().payload.size()) {
        return Status::Corruption("granule", "invalid value page cardinality");
      }
    }
  }

  std::vector<TemporalEvent> events;
  events.reserve(selected_rows.size());
  for (uint32_t row : selected_rows) {
    const LogicalKey row_key = MakePartitionKey(
        partition, entity_ids[row], target_ids[row], edge_ids[row]);
    if (static_cast<uint8_t>(operations[row]) == 1) {
      events.push_back(TemporalEvent::Delete(
          row_key, valid_from[row], commit_seq[row], partition.schema_epoch));
    } else if (static_cast<uint8_t>(value_classes[row]) == 1) {
      const auto value = inline_values.find(row);
      if (value == inline_values.end()) {
        return Status::Corruption("granule", "selected inline value missing");
      }
      events.push_back(TemporalEvent::Put(
          row_key, valid_from[row], commit_seq[row], partition.schema_epoch,
          value->second));
    } else {
      const auto reference = blob_references.find(row);
      if (reference == blob_references.end()) {
        return Status::Corruption("granule", "selected BlobRef missing");
      }
      events.push_back(TemporalEvent::PutBlob(
          row_key, valid_from[row], commit_seq[row], partition.schema_epoch,
          reference->second));
    }
  }
  return events;
}
}  // namespace

StatusOr<ResourceProfile> EstimateSstDecodeResources(
    const SstFileStatistics& statistics, uint64_t source_file_bytes,
    const ColumnSchema& schema) {
  if (source_file_bytes == 0 || !ValidateColumnSchema(schema, true).ok()) {
    return Status::InvalidArgument("sst decode estimate",
                                   "invalid source or schema");
  }
  const auto checked_add = [](uint64_t left, uint64_t right,
                              const char* dimension) -> StatusOr<uint64_t> {
    if (right > std::numeric_limits<uint64_t>::max() - left) {
      return Status::InvalidArgument(
          "sst decode estimate", std::string(dimension) + " overflow");
    }
    return left + right;
  };
  const auto checked_multiply = [](uint64_t left, uint64_t right,
                                   const char* dimension)
      -> StatusOr<uint64_t> {
    if (left != 0 && right > std::numeric_limits<uint64_t>::max() / left) {
      return Status::InvalidArgument(
          "sst decode estimate", std::string(dimension) + " overflow");
    }
    return left * right;
  };
  uint64_t value_bytes = 0;
  switch (schema.physical_type) {
    case PhysicalType::kBool:
      value_bytes = 1;
      break;
    case PhysicalType::kInt32:
    case PhysicalType::kFloat32:
      value_bytes = 4;
      break;
    case PhysicalType::kInt64:
    case PhysicalType::kFloat64:
    case PhysicalType::kTimestamp64:
      value_bytes = 8;
      break;
    case PhysicalType::kString:
    case PhysicalType::kBinary:
      value_bytes = std::max<uint64_t>(schema.blob_threshold, 40);
      break;
  }
  const auto event_bytes = checked_add(
      static_cast<uint64_t>(sizeof(TemporalEvent)), value_bytes,
      "event bytes");
  if (!event_bytes.ok()) return event_bytes.status();
  const auto retained_events = checked_multiply(
      statistics.row_count, event_bytes.ValueOrDie(), "retained events");
  if (!retained_events.ok()) return retained_events.status();
  const auto block_events = checked_multiply(
      std::min<uint64_t>(statistics.row_count, kMaxRowsPerGranuleBlock),
      event_bytes.ValueOrDie(), "block transient");
  if (!block_events.ok()) return block_events.status();
  const auto with_events = checked_add(
      source_file_bytes, retained_events.ValueOrDie(), "file and events");
  if (!with_events.ok()) return with_events.status();
  const auto peak = checked_add(
      with_events.ValueOrDie(), block_events.ValueOrDie(), "peak memory");
  if (!peak.ok()) return peak.status();
  return ResourceProfile{peak.ValueOrDie(), 0, 1, 0, 1,
                         source_file_bytes, 0, 0, 1};
}

StatusOr<SstFile> BuildSst(const BlockPartition& partition, const std::vector<TemporalEvent>& events) {
  if (events.empty()) return Status::InvalidArgument("sst", "empty SST event set");
  std::vector<TemporalEvent> ordered_events = events;
  std::sort(ordered_events.begin(), ordered_events.end(), [](const TemporalEvent& left,
                                                              const TemporalEvent& right) {
    if (left.logical_key() != right.logical_key()) return left.logical_key() < right.logical_key();
    if (left.valid_from() != right.valid_from()) return left.valid_from() > right.valid_from();
    return left.commit_seq() > right.commit_seq();
  });
  const std::vector<BlobHash> blob_refs = CollectBlobRefs(ordered_events);
  std::string bytes(kHeaderSize, '\0');
  std::vector<BlockIndexEntry> blocks;
  std::vector<BlobHash> block_hashes;
  PageCompressionStats compression;
  for (size_t begin = 0; begin < ordered_events.size();) {
    size_t end = FindAdaptiveBlockEnd(ordered_events, begin);
    auto block = BuildGranuleBlock(partition, std::vector<TemporalEvent>(
        ordered_events.begin() + begin, ordered_events.begin() + end));
    if (!block.ok()) return block.status();
    // Estimates determine normal boundaries. Recheck the actual serialized
    // block so a variable-width page cannot exceed the on-disk hard limit.
    while (block.ValueOrDie().bytes.size() > kHardMaxBlockBytes && end - begin > 1) {
      size_t reduced_end = end;
      while (reduced_end > begin &&
             ordered_events[reduced_end - 1].logical_key() ==
                 ordered_events[end - 1].logical_key()) {
        --reduced_end;
      }
      if (reduced_end == begin) {
        reduced_end = begin + std::max<size_t>(1, (end - begin) / 2);
      }
      end = reduced_end;
      block = BuildGranuleBlock(partition, std::vector<TemporalEvent>(
          ordered_events.begin() + begin, ordered_events.begin() + end));
      if (!block.ok()) return block.status();
    }
    BlockIndexEntry index;
    index.offset = bytes.size();
    index.length = block.ValueOrDie().bytes.size();
    index.first_key = ordered_events[begin].logical_key();
    index.last_key = ordered_events[end - 1].logical_key();
    index.min_valid_from = index.max_valid_from = ordered_events[begin].valid_from();
    index.min_commit_seq = index.max_commit_seq = ordered_events[begin].commit_seq();
    index.row_count = static_cast<uint32_t>(end - begin);
    for (size_t event = begin + 1; event < end; ++event) {
      index.min_valid_from = std::min(index.min_valid_from, ordered_events[event].valid_from());
      index.max_valid_from = std::max(index.max_valid_from, ordered_events[event].valid_from());
      index.min_commit_seq = std::min(index.min_commit_seq, ordered_events[event].commit_seq());
      index.max_commit_seq = std::max(index.max_commit_seq, ordered_events[event].commit_seq());
    }
    index.continuation_before = begin != 0 &&
        ordered_events[begin - 1].logical_key() == ordered_events[begin].logical_key();
    index.continuation_after = end != ordered_events.size() &&
        ordered_events[end - 1].logical_key() == ordered_events[end].logical_key();
    index.content_hash = block.ValueOrDie().identity;
    blocks.push_back(std::move(index));
    block_hashes.push_back(block.ValueOrDie().identity);
    for (size_t slot = 0; slot < kPageTypeMetricSlots; ++slot) {
      compression.uncompressed_bytes[slot] = SaturatingAdd(
          compression.uncompressed_bytes[slot],
          block.ValueOrDie().compression.uncompressed_bytes[slot]);
      compression.stored_bytes[slot] = SaturatingAdd(
          compression.stored_bytes[slot],
          block.ValueOrDie().compression.stored_bytes[slot]);
    }
    bytes.append(block.ValueOrDie().bytes);
    begin = end;
  }
  if (blocks.size() > UINT32_MAX) {
    return Status::InvalidArgument("sst", "block count exceeds UInt32");
  }
  const uint64_t blob_refs_offset = bytes.size();
  const std::string encoded_blob_refs = EncodeBlobRefs(blob_refs);
  bytes.append(encoded_blob_refs);
  const uint64_t bloom_offset = bytes.size();
  const std::string encoded_bloom = EncodeFileBloom(ordered_events);
  bytes.append(encoded_bloom);
  const SstFileStatistics statistics = BuildFileStatistics(ordered_events);
  const uint64_t statistics_offset = bytes.size();
  const std::string encoded_statistics = EncodeFileStatistics(statistics);
  bytes.append(encoded_statistics);
  const uint64_t index_offset = bytes.size();
  const std::string index = EncodeBlockIndex(blocks);
  bytes.append(index);
  const SstFileIdentity identity = BuildSstIdentity(
      partition, block_hashes, encoded_blob_refs, encoded_bloom,
      encoded_statistics, index);
  Footer footer;
  footer.index_offset = index_offset;
  footer.index_length = index.size();
  footer.blob_refs_offset = blob_refs_offset;
  footer.blob_refs_length = encoded_blob_refs.size();
  footer.bloom_offset = bloom_offset;
  footer.bloom_length = encoded_bloom.size();
  footer.statistics_offset = statistics_offset;
  footer.statistics_length = encoded_statistics.size();
  footer.index_crc = crc32c::Value(index.data(), index.size());
  footer.blob_refs_crc =
      crc32c::Value(encoded_blob_refs.data(), encoded_blob_refs.size());
  footer.bloom_crc =
      crc32c::Value(encoded_bloom.data(), encoded_bloom.size());
  footer.statistics_crc =
      crc32c::Value(encoded_statistics.data(), encoded_statistics.size());
  footer.row_count = statistics.row_count;
  footer.block_count = static_cast<uint32_t>(blocks.size());
  footer.identity = identity;
  bytes.append(EncodeFooter(footer));
  const SstFormatDescriptor format;
  const std::string header = EncodeSstHeader(
      partition, static_cast<uint32_t>(blocks.size()), format, identity);
  bytes.replace(0, kHeaderSize, header);
  SstMetadata metadata{partition, static_cast<uint32_t>(blocks.size()),
                       statistics.max_commit_seq, blob_refs,
                       format, identity, statistics, footer.statistics_crc};
  return SstFile{std::move(bytes), partition,
                 static_cast<uint32_t>(blocks.size()), blob_refs,
                 compression, std::move(metadata)};
}

StatusOr<std::vector<BlobHash>> ReadSstBlobRefs(const std::string& bytes) {
  const auto header = DecodeSstHeader(bytes);
  if (!header.ok()) return header.status();
  const auto footer = ReadFooter(bytes);
  if (!footer.ok()) return footer.status();
  const Status ownership = ValidateHeaderAndFooter(
      header.ValueOrDie(), footer.ValueOrDie());
  if (!ownership.ok()) return ownership;
  const std::string encoded_bloom = bytes.substr(footer.ValueOrDie().bloom_offset,
                                                 footer.ValueOrDie().bloom_length);
  if (crc32c::Value(encoded_bloom.data(), encoded_bloom.size()) !=
          footer.ValueOrDie().bloom_crc ||
      !DecodeFileBloom(encoded_bloom).ok()) {
    return Status::Corruption("sst", "file Bloom checksum mismatch");
  }
  const std::string encoded = bytes.substr(footer.ValueOrDie().blob_refs_offset, footer.ValueOrDie().blob_refs_length);
  if (crc32c::Value(encoded.data(), encoded.size()) != footer.ValueOrDie().blob_refs_crc) return Status::Corruption("sst", "BlobRefSet checksum mismatch");
  return DecodeBlobRefs(encoded);
}

StatusOr<std::vector<TemporalEvent>> ReadSst(const std::string& bytes) {
  if (bytes.size() < kHeaderSize + kFooterSize) return Status::Corruption("sst", "truncated file");
  const auto header = DecodeSstHeader(bytes);
  if (!header.ok()) return header.status();
  const uint32_t count = header.ValueOrDie().block_count;
  const auto footer = ReadFooter(bytes);
  if (!footer.ok()) return footer.status();
  const Status ownership = ValidateHeaderAndFooter(
      header.ValueOrDie(), footer.ValueOrDie());
  if (!ownership.ok()) return ownership;
  const std::string bloom = bytes.substr(footer.ValueOrDie().bloom_offset,
                                         footer.ValueOrDie().bloom_length);
  if (crc32c::Value(bloom.data(), bloom.size()) != footer.ValueOrDie().bloom_crc ||
      !DecodeFileBloom(bloom).ok()) return Status::Corruption("sst", "file Bloom checksum mismatch");
  const std::string encoded_blob_refs = bytes.substr(
      footer.ValueOrDie().blob_refs_offset,
      footer.ValueOrDie().blob_refs_length);
  if (crc32c::Value(encoded_blob_refs.data(), encoded_blob_refs.size()) !=
          footer.ValueOrDie().blob_refs_crc ||
      !DecodeBlobRefs(encoded_blob_refs).ok()) {
    return Status::Corruption("sst", "BlobRefSet checksum mismatch");
  }
  const std::string index = bytes.substr(footer.ValueOrDie().index_offset, footer.ValueOrDie().index_length); if (crc32c::Value(index.data(), index.size()) != footer.ValueOrDie().index_crc) return Status::Corruption("sst", "block index checksum mismatch");
  const auto block_index = DecodeBlockIndex(index, count);
  if (!block_index.ok()) return block_index.status();
  const BlockPartition& partition = header.ValueOrDie().partition;
  const std::string encoded_statistics = bytes.substr(
      footer.ValueOrDie().statistics_offset,
      footer.ValueOrDie().statistics_length);
  if (crc32c::Value(encoded_statistics.data(), encoded_statistics.size()) !=
      footer.ValueOrDie().statistics_crc) {
    return Status::Corruption("sst", "statistics checksum mismatch");
  }
  const auto statistics = DecodeFileStatistics(encoded_statistics, partition);
  if (!statistics.ok()) return statistics.status();
  if (statistics.ValueOrDie().row_count != footer.ValueOrDie().row_count) {
    return Status::Corruption("sst", "statistics row count mismatch");
  }
  const Status index_statistics = ValidateIndexStatistics(
      block_index.ValueOrDie(), statistics.ValueOrDie());
  if (!index_statistics.ok()) return index_statistics;
  const Status identity = ValidatePersistedFileIdentity(
      partition, block_index.ValueOrDie(), encoded_blob_refs, bloom,
      encoded_statistics, index, header.ValueOrDie().identity);
  if (!identity.ok()) return identity;
  std::vector<TemporalEvent> events;
  for (const BlockIndexEntry& entry : block_index.ValueOrDie()) {
    if (entry.offset < kHeaderSize || entry.offset > footer.ValueOrDie().blob_refs_offset ||
        entry.length > footer.ValueOrDie().blob_refs_offset - entry.offset) {
      return Status::Corruption("sst", "invalid block location");
    }
    const std::string block_bytes = bytes.substr(entry.offset, entry.length);
    const Status identity =
        VerifyGranuleBlockIdentity(block_bytes, entry.content_hash);
    if (!identity.ok()) return identity;
    const auto block = DecodeGranuleBlock(block_bytes, partition);
    if (!block.ok()) return block.status();
    if (block.ValueOrDie().size() != entry.row_count) {
      return Status::Corruption("sst", "block row count mismatch");
    }
    events.insert(events.end(), block.ValueOrDie().begin(), block.ValueOrDie().end());
  }
  if (events.size() != footer.ValueOrDie().row_count) {
    return Status::Corruption("sst", "decoded row count mismatch");
  }
  return events;
}

Status WriteSstFile(
    const std::string& path, const BlockPartition& partition,
    const std::vector<TemporalEvent>& events, SstFile* written,
    std::function<Status(SstPublicationFaultPoint)> fault_injector) {
  const auto built = BuildSst(partition, events); if (!built.ok()) return built.status();
  std::error_code error; const std::filesystem::path target(path); std::filesystem::create_directories(target.parent_path(), error); if (error) return Status::IOError(path, error.message());
  const std::string temporary = path + ".tmp"; const int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644); if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  const std::string& bytes = built.ValueOrDie().bytes; const char* data = bytes.data(); size_t remaining = bytes.size(); Status status = Status::OK();
  while (remaining > 0) { const ssize_t count = ::write(fd, data, remaining); if (count < 0) { if (errno == EINTR) continue; status = Status::IOError(temporary, std::strerror(errno)); break; } data += count; remaining -= static_cast<size_t>(count); }
  if (status.ok() && ::fsync(fd) != 0) status = Status::IOError(temporary, std::strerror(errno)); if (::close(fd) != 0 && status.ok()) status = Status::IOError(temporary, std::strerror(errno)); if (!status.ok()) return status;
  if (fault_injector) {
    const Status injected = fault_injector(
        SstPublicationFaultPoint::kAfterFileFsync);
    if (!injected.ok()) return injected;
  }
  if (::rename(temporary.c_str(), path.c_str()) != 0) return Status::IOError(path, std::strerror(errno));
  if (fault_injector) {
    const Status injected = fault_injector(
        SstPublicationFaultPoint::kAfterRename);
    if (!injected.ok()) return Status::Indeterminate("sst publication", injected.ToString());
  }
  const std::string directory = target.parent_path().string(); const int directory_fd = ::open(directory.c_str(), O_RDONLY); if (directory_fd < 0) return Status::IOError(directory, std::strerror(errno)); if (::fsync(directory_fd) != 0) { const Status fsync_status = Status::IOError(directory, std::strerror(errno)); ::close(directory_fd); return fsync_status; } if (::close(directory_fd) != 0) return Status::IOError(directory, std::strerror(errno));
  if (fault_injector) {
    const Status injected = fault_injector(
        SstPublicationFaultPoint::kAfterDirectoryFsync);
    if (!injected.ok()) return Status::Indeterminate("sst publication", injected.ToString());
  }
  if (written != nullptr) *written = built.ValueOrDie(); return Status::OK();
}

namespace {

StatusOr<std::vector<TemporalEvent>> ReadSstFileInternal(
    const std::string& path, const LogicalKey* key_filter,
    CacheManager* cache_manager, SstMetadata* metadata, bool metadata_only,
    SstReadStats* stats,
    const std::function<Status(const TemporalEvent&)>* event_visitor = nullptr,
    SstCursorStats* cursor_stats = nullptr,
    IoGovernor* io_governor = nullptr) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError(path, std::strerror(errno));
  struct stat file_stat {};
  if (::fstat(fd, &file_stat) != 0) {
    const Status status = Status::IOError(path, std::strerror(errno));
    ::close(fd);
    return status;
  }
  const uint64_t file_size = static_cast<uint64_t>(file_stat.st_size);
  if (file_size < kHeaderSize + kFooterSize) {
    ::close(fd);
    return Status::Corruption("sst", "truncated file");
  }
  std::string header;
  std::string footer_bytes;
  Status status = ReadRange(fd, 0, kHeaderSize, path, &header);
  if (!status.ok()) {
    ::close(fd);
    return status;
  }
  if (stats != nullptr) stats->bytes_read += kHeaderSize;
  const auto decoded_header = DecodeSstHeader(header);
  if (!decoded_header.ok()) {
    ::close(fd);
    return decoded_header.status();
  }
  const CacheKey metadata_key{
      CacheKind::kMetadata,
      path + ":" + std::to_string(file_size) + ":v" +
          std::to_string(kSstEncodingVersion) + ":" +
          IdentityHex(decoded_header.ValueOrDie().identity)};
  const CacheHandle cached_metadata = cache_manager == nullptr
      ? CacheHandle{} : cache_manager->Lookup(metadata_key);
  if (cached_metadata) {
    const std::string& encoded = *cached_metadata.value();
    if (encoded.size() != kFooterSize) {
      ::close(fd);
      return Status::Corruption("sst", "invalid cached metadata envelope");
    }
    footer_bytes = encoded;
  } else {
    status = ReadRange(fd, file_size - kFooterSize, kFooterSize,
                       path, &footer_bytes);
    if (status.ok() && cache_manager != nullptr) {
      const auto inserted = cache_manager->Insert(
          metadata_key,
          std::make_shared<const std::string>(footer_bytes),
          CacheAdmission::kMetadata);
      if (!inserted.ok() && !inserted.status().IsQueryMemoryLimit()) {
        ::close(fd);
        return inserted.status();
      }
    }
  }
  if (!status.ok()) {
    ::close(fd);
    return status;
  }
  if (stats != nullptr && !cached_metadata) stats->bytes_read += kFooterSize;
  const uint32_t block_count = decoded_header.ValueOrDie().block_count;
  const auto decoded_footer = DecodeFooterBytes(footer_bytes, file_size);
  if (!decoded_footer.ok()) {
    ::close(fd);
    return decoded_footer.status();
  }
  const Footer footer = decoded_footer.ValueOrDie();
  const Status ownership = ValidateHeaderAndFooter(
      decoded_header.ValueOrDie(), footer);
  if (!ownership.ok()) {
    ::close(fd);
    return ownership;
  }
  std::string blob_refs;
  std::string bloom;
  std::string encoded_statistics;
  std::string index;
  status = ReadRange(fd, footer.blob_refs_offset, footer.blob_refs_length, path, &blob_refs);
  if (status.ok()) status = ReadRange(fd, footer.bloom_offset, footer.bloom_length, path, &bloom);
  if (status.ok()) {
    status = ReadRange(fd, footer.statistics_offset, footer.statistics_length,
                       path, &encoded_statistics);
  }
  if (status.ok()) status = ReadRange(fd, footer.index_offset, footer.index_length, path, &index);
  if (!status.ok()) {
    ::close(fd);
    return status;
  }
  if (stats != nullptr) {
    stats->bytes_read += footer.blob_refs_length + footer.bloom_length +
                         footer.statistics_length + footer.index_length;
  }
  const auto decoded_blob_refs = DecodeBlobRefs(blob_refs);
  const BlockPartition& partition = decoded_header.ValueOrDie().partition;
  const auto decoded_statistics =
      DecodeFileStatistics(encoded_statistics, partition);
  if (crc32c::Value(blob_refs.data(), blob_refs.size()) != footer.blob_refs_crc ||
      !decoded_blob_refs.ok() || crc32c::Value(bloom.data(), bloom.size()) != footer.bloom_crc ||
      !DecodeFileBloom(bloom).ok() ||
      crc32c::Value(encoded_statistics.data(), encoded_statistics.size()) !=
          footer.statistics_crc ||
      !decoded_statistics.ok() ||
      crc32c::Value(index.data(), index.size()) != footer.index_crc) {
    ::close(fd);
    return Status::Corruption("sst", "invalid metadata checksum");
  }
  const auto block_index = DecodeBlockIndex(index, block_count);
  if (!block_index.ok()) {
    ::close(fd);
    return block_index.status();
  }
  if (decoded_statistics.ValueOrDie().row_count != footer.row_count) {
    ::close(fd);
    return Status::Corruption("sst", "statistics row count mismatch");
  }
  const Status index_statistics = ValidateIndexStatistics(
      block_index.ValueOrDie(), decoded_statistics.ValueOrDie());
  if (!index_statistics.ok()) {
    ::close(fd);
    return index_statistics;
  }
  const Status persisted_identity = ValidatePersistedFileIdentity(
      partition, block_index.ValueOrDie(), blob_refs, bloom,
      encoded_statistics, index, decoded_header.ValueOrDie().identity);
  if (!persisted_identity.ok()) {
    ::close(fd);
    return persisted_identity;
  }
  if (metadata != nullptr) {
    metadata->partition = partition;
    metadata->block_count = block_count;
    metadata->max_commit_seq = decoded_statistics.ValueOrDie().max_commit_seq;
    metadata->blob_refs = decoded_blob_refs.ValueOrDie();
    metadata->format = decoded_header.ValueOrDie().format;
    metadata->identity = decoded_header.ValueOrDie().identity;
    metadata->statistics = decoded_statistics.ValueOrDie();
    metadata->statistics_crc32c = footer.statistics_crc;
  }
  if (metadata_only) {
    if (::close(fd) != 0) return Status::IOError(path, std::strerror(errno));
    return std::vector<TemporalEvent>{};
  }
  const FileBloom file_bloom = DecodeFileBloom(bloom).ValueOrDie();
  if (key_filter != nullptr && !MayContain(file_bloom, *key_filter)) {
    if (::close(fd) != 0) return Status::IOError(path, std::strerror(errno));
    return std::vector<TemporalEvent>{};
  }
  std::vector<TemporalEvent> events;
  const std::vector<BlockIndexEntry>& entries = block_index.ValueOrDie();
  auto entry = entries.begin();
  if (key_filter != nullptr) {
    entry = std::lower_bound(entries.begin(), entries.end(), *key_filter,
        [](const BlockIndexEntry& candidate, const LogicalKey& key) {
          return candidate.last_key < key;
        });
  }
  for (; entry != entries.end(); ++entry) {
    if (key_filter != nullptr && *key_filter < entry->first_key) break;
    const BlockIndexEntry& selected = *entry;
    if (selected.offset < kHeaderSize || selected.offset > footer.blob_refs_offset ||
        selected.length > footer.blob_refs_offset - selected.offset) {
      ::close(fd);
      return Status::Corruption("sst", "invalid block location");
    }
    if (key_filter != nullptr) {
      if (io_governor != nullptr) {
        const uint64_t now_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
        const Status acquired = io_governor->TryAcquire(
            IoTokenRequest{0, 1, 0, 0, false}, now_ns);
        if (!acquired.ok()) {
          ::close(fd);
          return acquired;
        }
      }
      const auto block = ReadGranuleCandidatesForKey(
          fd, path, selected.offset, selected.length, partition, *key_filter,
          decoded_header.ValueOrDie().identity, selected.content_hash,
          cache_manager, stats);
      if (!block.ok()) {
        ::close(fd);
        return block.status();
      }
      if (stats != nullptr) ++stats->blocks_read;
      if (cursor_stats != nullptr) {
        ++cursor_stats->blocks_read;
        cursor_stats->peak_buffered_events = std::max<uint64_t>(
            cursor_stats->peak_buffered_events, block.ValueOrDie().size());
      }
      if (event_visitor != nullptr) {
        for (const TemporalEvent& event : block.ValueOrDie()) {
          const Status visited = (*event_visitor)(event);
          if (!visited.ok()) {
            ::close(fd);
            return visited;
          }
          if (cursor_stats != nullptr) ++cursor_stats->events_visited;
        }
      } else {
        events.insert(events.end(), block.ValueOrDie().begin(),
                      block.ValueOrDie().end());
      }
      continue;
    }
    std::string block_bytes;
    const CacheKey cache_key{CacheKind::kPage,
                             path + ":" +
                                 IdentityHex(decoded_header.ValueOrDie().identity) +
                                 ":" + std::to_string(selected.offset) + ":" +
                                 std::to_string(selected.length)};
    const CacheHandle cached = cache_manager == nullptr ? CacheHandle{} :
        cache_manager->Lookup(cache_key);
    if (cached) {
      block_bytes = *cached.value();
    } else {
      status = ReadRange(fd, selected.offset, selected.length, path, &block_bytes);
      if (!status.ok()) {
        ::close(fd);
        return status;
      }
      if (cache_manager != nullptr) {
        const auto inserted = cache_manager->Insert(
            cache_key, std::make_shared<const std::string>(block_bytes),
            CacheAdmission::kPointRead);
        if (!inserted.ok() && !inserted.status().IsQueryMemoryLimit()) {
          ::close(fd);
          return inserted.status();
        }
      }
    }
    const Status block_identity =
        VerifyGranuleBlockIdentity(block_bytes, selected.content_hash);
    if (!block_identity.ok()) {
      ::close(fd);
      return block_identity;
    }
    const auto block = DecodeGranuleBlock(block_bytes, partition);
    if (!block.ok()) {
      ::close(fd);
      return block.status();
    }
    if (block.ValueOrDie().size() != selected.row_count) {
      ::close(fd);
      return Status::Corruption("sst", "block row count mismatch");
    }
    events.insert(events.end(), block.ValueOrDie().begin(), block.ValueOrDie().end());
  }
  if (::close(fd) != 0) return Status::IOError(path, std::strerror(errno));
  return events;
}

}  // namespace

namespace {

bool SameBlockPartition(const BlockPartition& left,
                        const BlockPartition& right) {
  return left.entity_type == right.entity_type &&
      left.column_id == right.column_id &&
      left.schema_epoch == right.schema_epoch &&
      left.physical_type == right.physical_type &&
      left.edge_type == right.edge_type &&
      left.compression_id == right.compression_id &&
      left.key_kind == right.key_kind &&
      left.storage_shard_id == right.storage_shard_id &&
      left.logical_type_id == right.logical_type_id;
}

bool EventBefore(const TemporalEvent& left, const TemporalEvent& right) {
  if (left.logical_key() != right.logical_key()) {
    return left.logical_key() < right.logical_key();
  }
  if (left.valid_from() != right.valid_from()) {
    return left.valid_from() > right.valid_from();
  }
  return left.commit_seq() > right.commit_seq();
}

Status WriteAll(int fd, const std::string& path, const std::string& bytes) {
  const char* cursor = bytes.data();
  size_t remaining = bytes.size();
  while (remaining != 0) {
    const ssize_t written = ::write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    cursor += written;
    remaining -= static_cast<size_t>(written);
  }
  return Status::OK();
}

Status PwriteAll(int fd, const std::string& path, uint64_t offset,
                 const std::string& bytes) {
  size_t written = 0;
  while (written != bytes.size()) {
    const ssize_t count = ::pwrite(
        fd, bytes.data() + written, bytes.size() - written,
        static_cast<off_t>(offset + written));
    if (count < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    written += static_cast<size_t>(count);
  }
  return Status::OK();
}

Status SyncDirectory(const std::filesystem::path& path) {
  const std::string directory = path.parent_path().string();
  const int directory_fd = ::open(directory.c_str(), O_RDONLY);
  if (directory_fd < 0) {
    return Status::IOError(directory, std::strerror(errno));
  }
  if (::fsync(directory_fd) != 0) {
    const Status status = Status::IOError(directory, std::strerror(errno));
    ::close(directory_fd);
    return status;
  }
  if (::close(directory_fd) != 0) {
    return Status::IOError(directory, std::strerror(errno));
  }
  return Status::OK();
}

StatusOr<uint64_t> GranuleBlockMemoryCharge(
    int fd, const std::string& path, const BlockIndexEntry& block,
    const char* subsystem, uint64_t* page_count,
    uint64_t* page_bytes_decoded, uint64_t* bytes_read) {
  std::array<char, kGranuleHeaderSize> header{};
  size_t copied = 0;
  while (copied < header.size()) {
    const ssize_t count = ::pread(
        fd, header.data() + copied, header.size() - copied,
        static_cast<off_t>(block.offset + copied));
    if (count < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    if (count == 0) {
      return Status::Corruption(subsystem, "truncated GranuleBlock header");
    }
    copied += static_cast<size_t>(count);
  }
  std::string encoded(header.data(), header.size());
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t rows = 0;
  uint16_t version = 0;
  uint16_t header_size = 0;
  uint64_t directory_offset = 0;
  uint64_t directory_length = 0;
  if (!G32(encoded, &offset, &magic) || !G16(encoded, &offset, &version) ||
      !G16(encoded, &offset, &header_size) || !G32(encoded, &offset, &rows) ||
      !G64(encoded, &offset, &directory_offset) ||
      !G64(encoded, &offset, &directory_length) || magic != kGranuleMagic ||
      version != kGranuleVersion || header_size != kGranuleHeaderSize ||
      rows == 0 || rows > kMaxRowsPerGranuleBlock ||
      directory_offset > block.length ||
      directory_length > block.length - directory_offset) {
    return Status::Corruption(subsystem, "invalid GranuleBlock header");
  }
  std::string encoded_directory;
  Status status = ReadRange(fd, block.offset + directory_offset,
                            directory_length, path, &encoded_directory);
  if (!status.ok()) return status;
  const auto directory = DecodePageDirectory(encoded_directory);
  if (!directory.ok()) return directory.status();
  if (page_count != nullptr) *page_count = directory.ValueOrDie().size();

  uint64_t decoded_payload_bytes = 0;
  uint64_t encoded_payload_bytes = 0;
  uint64_t compressed_payload_bytes = 0;
  for (const PageDirectoryEntry& entry : directory.ValueOrDie()) {
    if (entry.offset < kGranuleHeaderSize || entry.offset > directory_offset ||
        entry.length > directory_offset - entry.offset ||
        entry.length < kPageFormatHeaderSize) {
      return Status::Corruption(subsystem, "invalid page directory entry");
    }
    std::string encoded_page_header;
    status = ReadRange(fd, block.offset + entry.offset, kPageFormatHeaderSize,
                       path, &encoded_page_header);
    if (!status.ok()) return status;
    const auto page_header = DecodePageHeader(encoded_page_header);
    if (!page_header.ok()) return page_header.status();
    if (page_header.ValueOrDie().page_type != entry.page_type ||
        page_header.ValueOrDie().compressed_size !=
            entry.length - kPageFormatHeaderSize) {
      return Status::Corruption(subsystem,
                                "page header differs from directory");
    }
    const auto add = [](uint64_t value, uint64_t* total) -> bool {
      if (value > std::numeric_limits<uint64_t>::max() - *total) return false;
      *total += value;
      return true;
    };
    if (!add(page_header.ValueOrDie().uncompressed_size,
             &decoded_payload_bytes) ||
        !add(page_header.ValueOrDie().encoded_size, &encoded_payload_bytes) ||
        !add(page_header.ValueOrDie().compressed_size,
             &compressed_payload_bytes)) {
      return Status::Corruption(subsystem, "page memory charge overflow");
    }
  }
  constexpr uint64_t kDecodedRowCharge = 256;
  uint64_t charge = 0;
  const auto add_charge = [&charge](uint64_t value) -> bool {
    if (value > std::numeric_limits<uint64_t>::max() - charge) return false;
    charge += value;
    return true;
  };
  if (!add_charge(block.length) || !add_charge(block.length) ||
      !add_charge(decoded_payload_bytes) ||
      !add_charge(decoded_payload_bytes) ||
      !add_charge(encoded_payload_bytes) ||
      !add_charge(compressed_payload_bytes) ||
      !add_charge(static_cast<uint64_t>(rows) * kDecodedRowCharge)) {
    return Status::Corruption(subsystem, "block memory charge overflow");
  }
  if (page_bytes_decoded != nullptr) {
    *page_bytes_decoded = decoded_payload_bytes;
  }
  if (bytes_read != nullptr) {
    *bytes_read = kGranuleHeaderSize + directory_length +
        directory.ValueOrDie().size() * kPageFormatHeaderSize;
  }
  return charge;
}

class StreamingInputCursor {
 public:
  explicit StreamingInputCursor(
      std::string path,
      std::shared_ptr<QueryCancellation> cancellation = nullptr,
      std::shared_ptr<QueryMemoryAccount> memory_account = nullptr,
      IoGovernor* io_governor = nullptr,
      bool prefetch_next_block = false)
      : path_(std::move(path)), cancellation_(std::move(cancellation)),
        memory_account_(std::move(memory_account)),
        io_governor_(io_governor),
        prefetch_next_block_(prefetch_next_block) {}
  ~StreamingInputCursor() {
    ReleaseBlockMemory();
    ReleasePrefetchMemory();
    if (fd_ >= 0) ::close(fd_);
  }

  Status Open(const BlockPartition& expected_partition,
              std::optional<LogicalKey> exact_key = std::nullopt) {
    exact_key_ = std::move(exact_key);
    Status cancelled = CheckCancelled();
    if (!cancelled.ok()) return cancelled;
    const auto metadata = ReadSstFileMetadata(path_);
    if (!metadata.ok()) return metadata.status();
    if (!SameBlockPartition(metadata.ValueOrDie().partition,
                            expected_partition)) {
      return Status::Corruption("sst streaming cursor",
                                "input partition differs from compaction closure");
    }
    metadata_ = metadata.ValueOrDie();
    fd_ = ::open(path_.c_str(), O_RDONLY);
    if (fd_ < 0) return Status::IOError(path_, std::strerror(errno));
    struct stat file_stat {};
    if (::fstat(fd_, &file_stat) != 0) {
      return Status::IOError(path_, std::strerror(errno));
    }
    file_size_ = static_cast<uint64_t>(file_stat.st_size);
    std::string footer_bytes;
    Status status = ReadRange(fd_, file_size_ - kFooterSize, kFooterSize,
                              path_, &footer_bytes);
    if (!status.ok()) return status;
    const auto decoded_footer = DecodeFooterBytes(footer_bytes, file_size_);
    if (!decoded_footer.ok()) return decoded_footer.status();
    footer_ = decoded_footer.ValueOrDie();
    if (footer_.block_count != metadata_.block_count ||
        footer_.identity != metadata_.identity) {
      return Status::Corruption("sst streaming cursor",
                                "metadata ownership mismatch");
    }
    std::string encoded_index;
    status = ReadRange(fd_, footer_.index_offset, footer_.index_length,
                       path_, &encoded_index);
    if (!status.ok()) return status;
    if (crc32c::Value(encoded_index.data(), encoded_index.size()) !=
        footer_.index_crc) {
      return Status::Corruption("sst streaming cursor",
                                "block index checksum mismatch");
    }
    const auto decoded = DecodeBlockIndex(encoded_index, footer_.block_count);
    if (!decoded.ok()) {
      ReleaseBlockMemory();
      return decoded.status();
    }
    blocks_ = decoded.ValueOrDie();
    physical_bytes_read_ = kHeaderSize + 2 * kFooterSize +
        footer_.blob_refs_length + footer_.bloom_length +
        footer_.statistics_length +
        2 * footer_.index_length;
    if (exact_key_.has_value()) {
      std::string encoded_bloom;
      status = ReadRange(fd_, footer_.bloom_offset, footer_.bloom_length,
                         path_, &encoded_bloom);
      if (!status.ok()) return status;
      if (crc32c::Value(encoded_bloom.data(), encoded_bloom.size()) !=
          footer_.bloom_crc) {
        return Status::Corruption("sst streaming cursor",
                                  "file Bloom checksum mismatch");
      }
      const auto bloom = DecodeFileBloom(encoded_bloom);
      if (!bloom.ok()) return bloom.status();
      physical_bytes_read_ += footer_.bloom_length;
      if (!MayContain(bloom.ValueOrDie(), *exact_key_)) {
        block_position_ = blocks_.size();
        return Status::OK();
      }
      const auto positioned = std::lower_bound(
          blocks_.begin(), blocks_.end(), *exact_key_,
          [](const BlockIndexEntry& entry, const LogicalKey& key) {
            return entry.last_key < key;
          });
      block_position_ = static_cast<size_t>(positioned - blocks_.begin());
      if (positioned == blocks_.end() || *exact_key_ < positioned->first_key) {
        block_position_ = blocks_.size();
        return Status::OK();
      }
    }
    return LoadBlock();
  }

  bool valid() const { return event_position_ < events_.size(); }
  const TemporalEvent& current() const { return events_[event_position_]; }
  uint64_t buffered_events() const { return events_.size(); }
  uint64_t buffered_bytes() const { return retained_block_bytes_; }
  uint64_t block_attempts() const { return block_attempts_; }
  uint64_t page_reads() const { return page_reads_; }
  uint64_t peak_attempted_bytes() const { return peak_attempted_bytes_; }
  uint64_t file_size() const { return file_size_; }
  uint64_t physical_bytes_read() const { return physical_bytes_read_; }
  uint64_t page_bytes_decoded() const { return page_bytes_decoded_; }
  uint64_t page_bytes_skipped() const { return page_bytes_skipped_; }
  uint64_t page_decode_count() const { return page_decode_count_; }
  uint64_t page_decode_latency_ns() const { return page_decode_latency_ns_; }
  uint64_t coalesced_read_ops() const { return coalesced_read_ops_; }
  uint64_t prefetched_blocks() const { return prefetched_blocks_; }
  uint64_t prefetched_bytes() const { return prefetched_bytes_; }
  const std::vector<BlobHash>& blob_refs() const { return metadata_.blob_refs; }

  Status Advance(bool* loaded_block) {
    *loaded_block = false;
    if (!valid()) return Status::NotFound("sst streaming cursor", "end of input");
    ++event_position_;
    if (event_position_ < events_.size()) return Status::OK();
    ++block_position_;
    if (block_position_ == blocks_.size() ||
        (exact_key_.has_value() &&
         (blocks_[block_position_].last_key < *exact_key_ ||
          *exact_key_ < blocks_[block_position_].first_key))) {
      std::vector<TemporalEvent>().swap(events_);
      ReleaseBlockMemory();
      event_position_ = 0;
      return Status::OK();
    }
    const Status status = LoadBlock();
    if (status.ok()) *loaded_block = true;
    return status;
  }

 private:
  Status CheckCancelled() const {
    return cancellation_ && cancellation_->IsCancelled()
        ? Status::QueryCancelled("sst streaming cursor", "query cancelled before block I/O")
        : Status::OK();
  }

  StatusOr<uint64_t> BlockMemoryCharge(
      const BlockIndexEntry& block, uint64_t* page_count,
      uint64_t* page_bytes_decoded, uint64_t* bytes_read) const {
    return GranuleBlockMemoryCharge(
        fd_, path_, block, "sst streaming cursor", page_count,
        page_bytes_decoded, bytes_read);
  }

  void ReleaseBlockMemory() {
    if (memory_account_ && retained_block_bytes_ != 0) {
      memory_account_->Release(retained_block_bytes_);
    }
    retained_block_bytes_ = 0;
  }

  void ReleasePrefetchMemory() {
    if (memory_account_ && prefetched_retained_bytes_ != 0) {
      memory_account_->Release(prefetched_retained_bytes_);
    }
    prefetched_retained_bytes_ = 0;
    prefetched_block_position_ = std::numeric_limits<size_t>::max();
    std::string().swap(prefetched_block_bytes_);
  }

  Status LoadBlock() {
    if (block_position_ >= blocks_.size()) return Status::OK();
    Status cancelled = CheckCancelled();
    if (!cancelled.ok()) return cancelled;
    const BlockIndexEntry& block = blocks_[block_position_];
    if (block.offset < kHeaderSize || block.offset > footer_.blob_refs_offset ||
        block.length > footer_.blob_refs_offset - block.offset) {
      return Status::Corruption("sst streaming cursor", "invalid block location");
    }
    ++block_attempts_;
    const bool consume_prefetch =
        prefetched_block_position_ == block_position_ &&
        !prefetched_block_bytes_.empty();
    std::string prefetched_input;
    if (consume_prefetch) {
      prefetched_input = std::move(prefetched_block_bytes_);
      prefetched_block_position_ = std::numeric_limits<size_t>::max();
    }
    std::vector<TemporalEvent>().swap(events_);
    ReleaseBlockMemory();
    if (consume_prefetch) {
      retained_block_bytes_ = prefetched_retained_bytes_;
      prefetched_retained_bytes_ = 0;
    } else if (memory_account_) {
      const Status reserved = memory_account_->Reserve(block.length);
      if (!reserved.ok()) return reserved;
    }
    retained_block_bytes_ = block.length;
    peak_attempted_bytes_ = std::max(peak_attempted_bytes_, retained_block_bytes_);
    uint64_t block_page_count = 0;
    uint64_t block_page_bytes_decoded = 0;
    uint64_t charge_bytes_read = 0;
    const auto charge = BlockMemoryCharge(
        block, &block_page_count, &block_page_bytes_decoded,
        &charge_bytes_read);
    physical_bytes_read_ += charge_bytes_read;
    if (!charge.ok()) {
      ReleaseBlockMemory();
      return charge.status();
    }
    if (charge.ValueOrDie() > retained_block_bytes_ && memory_account_) {
      const Status reserved = memory_account_->Reserve(
          charge.ValueOrDie() - retained_block_bytes_);
      if (!reserved.ok()) {
        ReleaseBlockMemory();
        return reserved;
      }
    }
    retained_block_bytes_ = charge.ValueOrDie();
    peak_attempted_bytes_ = std::max(peak_attempted_bytes_, retained_block_bytes_);
    bool coalesce_next = false;
    uint64_t payload_read_bytes = block.length;
    if (!consume_prefetch && !exact_key_.has_value() &&
        prefetch_next_block_ && block_position_ + 1 < blocks_.size()) {
      const BlockIndexEntry& next = blocks_[block_position_ + 1];
      if (block.offset + block.length == next.offset &&
          next.length <= std::numeric_limits<uint64_t>::max() - block.length) {
        const Status reserved = memory_account_
            ? memory_account_->Reserve(next.length) : Status::OK();
        if (reserved.ok()) {
          prefetched_retained_bytes_ = next.length;
          prefetched_block_position_ = block_position_ + 1;
          payload_read_bytes += next.length;
          coalesce_next = true;
          peak_attempted_bytes_ = std::max(
              peak_attempted_bytes_,
              retained_block_bytes_ + prefetched_retained_bytes_);
        }
      }
    }
    if (!consume_prefetch && io_governor_ != nullptr) {
      IoTokenRequest request;
      if (exact_key_.has_value()) request.random_read_ops = 1;
      else request.sequential_read_bytes = payload_read_bytes;
      const uint64_t now_ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now().time_since_epoch()).count());
      Status acquired = io_governor_->TryAcquire(request, now_ns);
      if (!acquired.ok() && coalesce_next) {
        ReleasePrefetchMemory();
        coalesce_next = false;
        payload_read_bytes = block.length;
        request.sequential_read_bytes = block.length;
        acquired = io_governor_->TryAcquire(request, now_ns);
      }
      if (!acquired.ok()) {
        ReleaseBlockMemory();
        return acquired;
      }
    }
    StatusOr<std::vector<TemporalEvent>> decoded =
        Status::Corruption("sst streaming cursor", "uninitialized block decode");
    if (exact_key_.has_value()) {
      SstReadStats read_stats;
      decoded = ReadGranuleCandidatesForKey(
          fd_, path_, block.offset, block.length, metadata_.partition,
          *exact_key_, metadata_.identity, block.content_hash, nullptr,
          &read_stats);
      page_reads_ +=
          read_stats.system_pages_read + read_stats.value_pages_read;
      physical_bytes_read_ += read_stats.bytes_read;
      page_bytes_decoded_ += read_stats.page_bytes_decoded;
      page_bytes_skipped_ += read_stats.page_bytes_skipped;
      page_decode_count_ += read_stats.page_decode_count;
      page_decode_latency_ns_ += read_stats.page_decode_latency_ns;
    } else {
      std::string bytes;
      if (consume_prefetch) {
        bytes = std::move(prefetched_input);
      } else {
        std::string payload;
        Status status = ReadRange(
            fd_, block.offset, payload_read_bytes, path_, &payload);
        if (!status.ok()) {
          ReleaseBlockMemory();
          ReleasePrefetchMemory();
          return status;
        }
        physical_bytes_read_ += payload_read_bytes;
        if (coalesce_next) {
          bytes.assign(payload.data(), static_cast<size_t>(block.length));
          prefetched_block_bytes_.assign(
              payload.data() + block.length,
              static_cast<size_t>(payload_read_bytes - block.length));
          ++coalesced_read_ops_;
          ++prefetched_blocks_;
          prefetched_bytes_ += payload_read_bytes - block.length;
        } else {
          bytes = std::move(payload);
        }
      }
      const auto decode_started = std::chrono::steady_clock::now();
      const Status block_identity =
          VerifyGranuleBlockIdentity(bytes, block.content_hash);
      if (!block_identity.ok()) {
        ReleaseBlockMemory();
        ReleasePrefetchMemory();
        return block_identity;
      }
      decoded = DecodeGranuleBlock(bytes, metadata_.partition);
      if (decoded.ok() && decoded.ValueOrDie().size() != block.row_count) {
        decoded = Status::Corruption("sst streaming cursor",
                                     "block row count mismatch");
      }
      page_decode_latency_ns_ += ElapsedSteadyNs(decode_started);
      page_reads_ += block_page_count;
      page_bytes_decoded_ += block_page_bytes_decoded;
      page_decode_count_ += block_page_count;
    }
    if (!decoded.ok()) {
      ReleaseBlockMemory();
      return decoded.status();
    }
    if (decoded.ValueOrDie().empty() && !exact_key_.has_value()) {
      ReleaseBlockMemory();
      return Status::Corruption("sst streaming cursor", "empty GranuleBlock");
    }
    if (decoded.ValueOrDie().empty()) {
      ReleaseBlockMemory();
      ++block_position_;
      return block_position_ < blocks_.size() &&
                     !(blocks_[block_position_].last_key < *exact_key_) &&
                     !(*exact_key_ < blocks_[block_position_].first_key)
          ? LoadBlock() : Status::OK();
    }
    events_ = decoded.ValueOrDie();
    if ((!exact_key_.has_value() &&
         (events_.front().logical_key() != block.first_key ||
          events_.back().logical_key() != block.last_key)) ||
        (exact_key_.has_value() &&
         (events_.front().logical_key() != *exact_key_ ||
          events_.back().logical_key() != *exact_key_)) ||
        !std::is_sorted(events_.begin(), events_.end(), EventBefore) ||
        (last_event_.has_value() && EventBefore(events_.front(), *last_event_))) {
      std::vector<TemporalEvent>().swap(events_);
      ReleaseBlockMemory();
      return Status::Corruption("sst streaming cursor",
                                "block rows violate persisted sort order");
    }
    last_event_ = events_.back();
    event_position_ = 0;
    return Status::OK();
  }

  std::string path_;
  int fd_ = -1;
  uint64_t file_size_ = 0;
  Footer footer_{};
  SstMetadata metadata_{};
  std::vector<BlockIndexEntry> blocks_;
  size_t block_position_ = 0;
  std::vector<TemporalEvent> events_;
  size_t event_position_ = 0;
  std::optional<TemporalEvent> last_event_;
  std::optional<LogicalKey> exact_key_;
  std::shared_ptr<QueryCancellation> cancellation_;
  std::shared_ptr<QueryMemoryAccount> memory_account_;
  IoGovernor* io_governor_ = nullptr;
  bool prefetch_next_block_ = false;
  uint64_t retained_block_bytes_ = 0;
  std::string prefetched_block_bytes_;
  size_t prefetched_block_position_ = std::numeric_limits<size_t>::max();
  uint64_t prefetched_retained_bytes_ = 0;
  uint64_t block_attempts_ = 0;
  uint64_t page_reads_ = 0;
  uint64_t peak_attempted_bytes_ = 0;
  uint64_t physical_bytes_read_ = 0;
  uint64_t page_bytes_decoded_ = 0;
  uint64_t page_bytes_skipped_ = 0;
  uint64_t page_decode_count_ = 0;
  uint64_t page_decode_latency_ns_ = 0;
  uint64_t coalesced_read_ops_ = 0;
  uint64_t prefetched_blocks_ = 0;
  uint64_t prefetched_bytes_ = 0;
};

class IncrementalSstWriter {
 public:
  IncrementalSstWriter(std::string path, BlockPartition partition,
                       uint64_t bloom_size_hint)
      : path_(std::move(path)), temporary_(path_ + ".tmp"),
        partition_(partition) {
    const uint64_t requested = std::max<uint64_t>(kMinimumBloomBits,
        std::min<uint64_t>(kMaximumBloomBits, bloom_size_hint));
    bloom_.bit_count = static_cast<uint32_t>((requested + 7) & ~uint64_t{7});
    bloom_.hash_count = kBloomHashCount;
    bloom_.bits.assign(bloom_.bit_count / 8, '\0');
  }
  ~IncrementalSstWriter() {
    if (fd_ >= 0) ::close(fd_);
    if (!finished_ && !temporary_.empty()) {
      std::error_code ignored;
      std::filesystem::remove(temporary_, ignored);
    }
  }

  Status Open() {
    std::error_code error;
    const std::filesystem::path target(path_);
    std::filesystem::create_directories(target.parent_path(), error);
    if (error) return Status::IOError(path_, error.message());
    fd_ = ::open(temporary_.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd_ < 0) return Status::IOError(temporary_, std::strerror(errno));
    offset_ = kHeaderSize;
    const Status written = WriteAll(fd_, temporary_, std::string(kHeaderSize, '\0'));
    if (written.ok()) bytes_written_ += kHeaderSize;
    return written;
  }

  Status AppendBlock(const std::vector<TemporalEvent>& events,
                     bool continuation_before, bool continuation_after) {
    if (events.empty()) {
      return Status::InvalidArgument("sst streaming writer", "empty output block");
    }
    const auto block = BuildGranuleBlock(partition_, events);
    if (!block.ok()) return block.status();
    if (block.ValueOrDie().bytes.size() > kHardMaxBlockBytes) {
      return Status::ResourceExhausted("sst streaming writer",
                                       "serialized block exceeds hard limit");
    }
    if (blocks_.size() == UINT32_MAX) {
      return Status::ResourceExhausted("sst streaming writer",
                                       "block count exceeds UInt32");
    }
    BlockIndexEntry index;
    index.offset = offset_;
    index.length = block.ValueOrDie().bytes.size();
    index.first_key = events.front().logical_key();
    index.last_key = events.back().logical_key();
    index.min_valid_from = index.max_valid_from = events.front().valid_from();
    index.min_commit_seq = index.max_commit_seq = events.front().commit_seq();
    index.row_count = static_cast<uint32_t>(events.size());
    index.continuation_before = continuation_before;
    index.continuation_after = continuation_after;
    index.content_hash = block.ValueOrDie().identity;
    for (const TemporalEvent& event : events) {
      index.min_valid_from = std::min(index.min_valid_from, event.valid_from());
      index.max_valid_from = std::max(index.max_valid_from, event.valid_from());
      index.min_commit_seq = std::min(index.min_commit_seq, event.commit_seq());
      index.max_commit_seq = std::max(index.max_commit_seq, event.commit_seq());
      max_commit_seq_ = std::max(max_commit_seq_, event.commit_seq());
      const uint64_t first = BloomHash(event.logical_key(), 0x9e3779b97f4a7c15ULL);
      uint64_t second = BloomHash(event.logical_key(), 0xd6e8feb86659fd93ULL);
      if (second == 0) second = 0x94d049bb133111ebULL;
      for (uint8_t hash = 0; hash < bloom_.hash_count; ++hash) {
        const uint32_t bit = static_cast<uint32_t>(
            (first + hash * second) % bloom_.bit_count);
        bloom_.bits[bit / 8] = static_cast<char>(
            static_cast<uint8_t>(bloom_.bits[bit / 8]) |
            (uint8_t{1} << (bit % 8)));
      }
    }
    const Status written = WriteAll(fd_, temporary_, block.ValueOrDie().bytes);
    if (!written.ok()) return written;
    block_hashes_.push_back(block.ValueOrDie().identity);
    AccumulateFileStatistics(events, &statistics_);
    for (size_t slot = 0; slot < kPageTypeMetricSlots; ++slot) {
      compression_.uncompressed_bytes[slot] = SaturatingAdd(
          compression_.uncompressed_bytes[slot],
          block.ValueOrDie().compression.uncompressed_bytes[slot]);
      compression_.stored_bytes[slot] = SaturatingAdd(
          compression_.stored_bytes[slot],
          block.ValueOrDie().compression.stored_bytes[slot]);
    }
    bytes_written_ += block.ValueOrDie().bytes.size();
    offset_ += block.ValueOrDie().bytes.size();
    blocks_.push_back(std::move(index));
    return Status::OK();
  }

  StatusOr<SstMetadata> Finish(std::vector<BlobHash> blob_refs) {
    if (blocks_.empty()) {
      return Status::InvalidArgument("sst streaming writer", "no output blocks");
    }
    std::sort(blob_refs.begin(), blob_refs.end(),
              [](const BlobHash& left, const BlobHash& right) {
                return left.bytes < right.bytes;
              });
    blob_refs.erase(std::unique(blob_refs.begin(), blob_refs.end()),
                    blob_refs.end());
    if (blob_refs.size() > 1000000) {
      return Status::ResourceExhausted("sst streaming writer",
                                       "BlobRefSet exceeds format limit");
    }
    const uint64_t blob_refs_offset = offset_;
    const std::string encoded_blob_refs = EncodeBlobRefs(blob_refs);
    Status status = WriteAll(fd_, temporary_, encoded_blob_refs);
    if (!status.ok()) return status;
    bytes_written_ += encoded_blob_refs.size();
    offset_ += encoded_blob_refs.size();

    const uint64_t bloom_offset = offset_;
    std::string encoded_bloom;
    P32(&encoded_bloom, kBloomMagic);
    P32(&encoded_bloom, bloom_.bit_count);
    P8(&encoded_bloom, bloom_.hash_count);
    encoded_bloom.append(bloom_.bits);
    status = WriteAll(fd_, temporary_, encoded_bloom);
    if (!status.ok()) return status;
    bytes_written_ += encoded_bloom.size();
    offset_ += encoded_bloom.size();

    const uint64_t statistics_offset = offset_;
    const std::string encoded_statistics = EncodeFileStatistics(statistics_);
    status = WriteAll(fd_, temporary_, encoded_statistics);
    if (!status.ok()) return status;
    bytes_written_ += encoded_statistics.size();
    offset_ += encoded_statistics.size();

    const uint64_t index_offset = offset_;
    const std::string encoded_index = EncodeBlockIndex(blocks_);
    status = WriteAll(fd_, temporary_, encoded_index);
    if (!status.ok()) return status;
    bytes_written_ += encoded_index.size();
    offset_ += encoded_index.size();

    const SstFileIdentity identity = BuildSstIdentity(
        partition_, block_hashes_, encoded_blob_refs, encoded_bloom,
        encoded_statistics, encoded_index);
    Footer decoded_footer;
    decoded_footer.index_offset = index_offset;
    decoded_footer.index_length = encoded_index.size();
    decoded_footer.blob_refs_offset = blob_refs_offset;
    decoded_footer.blob_refs_length = encoded_blob_refs.size();
    decoded_footer.bloom_offset = bloom_offset;
    decoded_footer.bloom_length = encoded_bloom.size();
    decoded_footer.statistics_offset = statistics_offset;
    decoded_footer.statistics_length = encoded_statistics.size();
    decoded_footer.index_crc =
        crc32c::Value(encoded_index.data(), encoded_index.size());
    decoded_footer.blob_refs_crc =
        crc32c::Value(encoded_blob_refs.data(), encoded_blob_refs.size());
    decoded_footer.bloom_crc =
        crc32c::Value(encoded_bloom.data(), encoded_bloom.size());
    decoded_footer.statistics_crc =
        crc32c::Value(encoded_statistics.data(), encoded_statistics.size());
    decoded_footer.row_count = statistics_.row_count;
    decoded_footer.block_count = static_cast<uint32_t>(blocks_.size());
    decoded_footer.identity = identity;
    const std::string footer = EncodeFooter(decoded_footer);
    status = WriteAll(fd_, temporary_, footer);
    if (!status.ok()) return status;
    bytes_written_ += footer.size();

    const SstFormatDescriptor format;
    const std::string header = EncodeSstHeader(
        partition_, static_cast<uint32_t>(blocks_.size()), format, identity);
    status = PwriteAll(fd_, temporary_, 0, header);
    if (!status.ok()) return status;
    if (::fsync(fd_) != 0) return Status::IOError(temporary_, std::strerror(errno));
    if (::close(fd_) != 0) {
      fd_ = -1;
      return Status::IOError(temporary_, std::strerror(errno));
    }
    fd_ = -1;
    if (::rename(temporary_.c_str(), path_.c_str()) != 0) {
      return Status::IOError(path_, std::strerror(errno));
    }
    status = SyncDirectory(std::filesystem::path(path_));
    if (!status.ok()) return status;
    finished_ = true;
    return SstMetadata{partition_, static_cast<uint32_t>(blocks_.size()),
                       max_commit_seq_, std::move(blob_refs), format,
                       identity, statistics_, decoded_footer.statistics_crc};
  }

  uint64_t bytes_written() const { return bytes_written_; }
  const PageCompressionStats& compression() const { return compression_; }

 private:
  std::string path_;
  std::string temporary_;
  BlockPartition partition_;
  int fd_ = -1;
  uint64_t offset_ = 0;
  uint64_t max_commit_seq_ = 0;
  uint64_t bytes_written_ = 0;
  PageCompressionStats compression_;
  FileBloom bloom_;
  std::vector<BlockIndexEntry> blocks_;
  std::vector<BlobHash> block_hashes_;
  SstFileStatistics statistics_;
  bool finished_ = false;
};

}  // namespace

struct SstEventCursor::Impl {
  Impl(std::string path, SstCursorOptions options)
      : cursor(std::move(path), std::move(options.cancellation),
               std::move(options.memory_account), options.io_governor,
               options.prefetch_next_block),
        expected_partition(std::move(options.expected_partition)),
        exact_key(std::move(options.exact_key)) {}

  StreamingInputCursor cursor;
  BlockPartition expected_partition;
  std::optional<LogicalKey> exact_key;
  SstCursorStats stats;
  Status terminal_status = Status::OK();

  void SyncStats() {
    stats.blocks_read = cursor.block_attempts();
    stats.pages_read = cursor.page_reads();
    stats.bytes_read = cursor.physical_bytes_read();
    stats.page_bytes_decoded = cursor.page_bytes_decoded();
    stats.page_bytes_skipped = cursor.page_bytes_skipped();
    stats.page_decode_count = cursor.page_decode_count();
    stats.page_decode_latency_ns = cursor.page_decode_latency_ns();
    stats.coalesced_read_ops = cursor.coalesced_read_ops();
    stats.prefetched_blocks = cursor.prefetched_blocks();
    stats.prefetched_bytes = cursor.prefetched_bytes();
    stats.peak_buffered_bytes = std::max(
        stats.peak_buffered_bytes, cursor.peak_attempted_bytes());
    if (cursor.valid()) {
      stats.peak_buffered_events = std::max(
          stats.peak_buffered_events, cursor.buffered_events());
      stats.peak_buffered_bytes = std::max(
          stats.peak_buffered_bytes, cursor.buffered_bytes());
    }
  }
};

SstEventCursor::SstEventCursor(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
SstEventCursor::SstEventCursor(SstEventCursor&&) noexcept = default;
SstEventCursor& SstEventCursor::operator=(SstEventCursor&&) noexcept = default;
SstEventCursor::~SstEventCursor() = default;

bool SstEventCursor::valid() const {
  return impl_ != nullptr && impl_->terminal_status.ok() && impl_->cursor.valid();
}

const TemporalEvent& SstEventCursor::current() const {
  return impl_->cursor.current();
}

Status SstEventCursor::Advance() {
  if (!impl_) return Status::InvalidArgument("sst cursor", "cursor was moved from");
  if (!impl_->terminal_status.ok()) return impl_->terminal_status;
  if (!impl_->cursor.valid()) return Status::NotFound("sst cursor", "end of input");
  ++impl_->stats.events_visited;
  bool loaded_block = false;
  const Status status = impl_->cursor.Advance(&loaded_block);
  impl_->SyncStats();
  if (!status.ok()) {
    impl_->terminal_status = status;
    return status;
  }
  return Status::OK();
}

const SstCursorStats& SstEventCursor::stats() const { return impl_->stats; }
const Status& SstEventCursor::terminal_status() const {
  return impl_->terminal_status;
}

StatusOr<SstEventCursor> OpenSstEventCursor(
    const std::string& path, SstCursorOptions options) {
  auto impl = std::make_unique<SstEventCursor::Impl>(path, std::move(options));
  const Status status = impl->cursor.Open(impl->expected_partition, impl->exact_key);
  if (!status.ok()) return status;
  impl->SyncStats();
  return SstEventCursor(std::move(impl));
}

StatusOr<SstMetadata> MergeSstFilesStreaming(
    const std::string& output_path, const BlockPartition& partition,
    const std::vector<std::string>& input_paths,
    SstStreamingWriteStats* stats,
    std::shared_ptr<WorkCancellation> cancellation) {
  if (input_paths.empty()) {
    return Status::InvalidArgument("sst streaming merge", "input files are required");
  }
  SstStreamingWriteStats local_stats;
  std::vector<std::unique_ptr<StreamingInputCursor>> cursors;
  std::vector<BlobHash> blob_refs;
  uint64_t bloom_size_hint = 0;
  for (const std::string& path : input_paths) {
    if (cancellation != nullptr) {
      const Status checkpoint =
          cancellation->Checkpoint("sst streaming merge");
      if (!checkpoint.ok()) return checkpoint;
    }
    auto cursor = std::make_unique<StreamingInputCursor>(path);
    const Status opened = cursor->Open(partition);
    if (!opened.ok()) return opened;
    ++local_stats.input_blocks_read;
    bloom_size_hint = cursor->file_size() > kMaximumBloomBits - bloom_size_hint
        ? kMaximumBloomBits : bloom_size_hint + cursor->file_size();
    blob_refs.insert(blob_refs.end(), cursor->blob_refs().begin(),
                     cursor->blob_refs().end());
    cursors.push_back(std::move(cursor));
  }

  struct HeapEntry { size_t cursor = 0; };
  const auto later = [&cursors](const HeapEntry& left, const HeapEntry& right) {
    const TemporalEvent& left_event = cursors[left.cursor]->current();
    const TemporalEvent& right_event = cursors[right.cursor]->current();
    if (left_event.logical_key() == right_event.logical_key() &&
        left_event.valid_from() == right_event.valid_from() &&
        left_event.commit_seq() == right_event.commit_seq()) {
      return left.cursor > right.cursor;
    }
    return EventBefore(right_event, left_event);
  };
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, decltype(later)> heap(later);
  for (size_t cursor = 0; cursor < cursors.size(); ++cursor) {
    if (cursors[cursor]->valid()) heap.push(HeapEntry{cursor});
  }
  if (heap.empty()) {
    return Status::Corruption("sst streaming merge", "input SSTs contain no events");
  }

  IncrementalSstWriter writer(output_path, partition, bloom_size_hint);
  Status status = writer.Open();
  if (!status.ok()) return status;
  std::vector<TemporalEvent> output_block;
  std::vector<TemporalEvent> chain;
  uint64_t output_bytes = 0;
  uint64_t chain_bytes = 0;
  bool chain_continuation_before = false;
  bool output_continuation_before = false;

  const auto update_peak = [&]() {
    uint64_t buffered = output_block.size() + chain.size();
    for (const auto& cursor : cursors) buffered += cursor->buffered_events();
    local_stats.peak_buffered_events = std::max(
        local_stats.peak_buffered_events, buffered);
  };
  const auto flush_output = [&](bool continuation_after) -> Status {
    if (output_block.empty()) return Status::OK();
    if (cancellation != nullptr) {
      const Status checkpoint =
          cancellation->Checkpoint("sst streaming merge");
      if (!checkpoint.ok()) return checkpoint;
    }
    const Status appended = writer.AppendBlock(
        output_block, output_continuation_before, continuation_after);
    if (!appended.ok()) return appended;
    ++local_stats.output_blocks_written;
    output_block.clear();
    output_bytes = 0;
    output_continuation_before = continuation_after;
    return Status::OK();
  };
  const auto finish_chain = [&]() -> Status {
    if (chain.empty()) return Status::OK();
    if (chain_continuation_before && !output_block.empty()) {
      const Status flushed = flush_output(false);
      if (!flushed.ok()) return flushed;
    }
    if (!output_block.empty() &&
        (output_block.size() + chain.size() > kMaxRowsPerGranuleBlock ||
         output_bytes + chain_bytes > kTargetUncompressedBlockBytes)) {
      const Status flushed = flush_output(false);
      if (!flushed.ok()) return flushed;
    }
    if (output_block.empty()) {
      output_continuation_before = chain_continuation_before;
    }
    output_block.insert(output_block.end(), chain.begin(), chain.end());
    output_bytes += chain_bytes;
    chain.clear();
    chain_bytes = 0;
    chain_continuation_before = false;
    if (output_block.size() >= kMaxRowsPerGranuleBlock ||
        output_bytes >= kTargetUncompressedBlockBytes) {
      return flush_output(false);
    }
    return Status::OK();
  };

  std::optional<LogicalKey> chain_key;
  while (!heap.empty()) {
    const HeapEntry top = heap.top();
    heap.pop();
    const TemporalEvent event = cursors[top.cursor]->current();
    if (chain_key.has_value() && event.logical_key() != *chain_key) {
      status = finish_chain();
      if (!status.ok()) return status;
      chain_key = event.logical_key();
    } else if (!chain_key.has_value()) {
      chain_key = event.logical_key();
    }
    const uint64_t event_bytes = EstimateEventBytes(event);
    if (!chain.empty() &&
        (chain.size() == kMaxRowsPerGranuleBlock ||
         chain_bytes + event_bytes > kHardMaxBlockBytes)) {
      if (!output_block.empty()) {
        status = flush_output(false);
        if (!status.ok()) return status;
      }
      output_block.swap(chain);
      output_bytes = chain_bytes;
      output_continuation_before = chain_continuation_before;
      chain_bytes = 0;
      status = flush_output(true);
      if (!status.ok()) return status;
      chain_continuation_before = true;
    }
    chain.push_back(event);
    chain_bytes += event_bytes;

    bool loaded_block = false;
    status = cursors[top.cursor]->Advance(&loaded_block);
    if (!status.ok()) return status;
    if (loaded_block) {
      ++local_stats.input_blocks_read;
      if (cancellation != nullptr) {
        status = cancellation->Checkpoint("sst streaming merge");
        if (!status.ok()) return status;
      }
    }
    if (cursors[top.cursor]->valid()) heap.push(top);
    update_peak();
  }
  status = finish_chain();
  if (!status.ok()) return status;
  status = flush_output(false);
  if (!status.ok()) return status;
  if (cancellation != nullptr) {
    status = cancellation->Checkpoint("sst streaming merge");
    if (!status.ok()) return status;
  }
  auto metadata = writer.Finish(std::move(blob_refs));
  if (!metadata.ok()) return metadata.status();
  for (const auto& cursor : cursors) {
    local_stats.input_bytes_read += cursor->physical_bytes_read();
  }
  local_stats.output_bytes_written = writer.bytes_written();
  local_stats.compression = writer.compression();
  if (stats != nullptr) *stats = local_stats;
  return metadata;
}

Status VisitSstEvents(
    const std::string& path,
    const std::function<Status(const TemporalEvent&)>& visitor,
    SstCursorStats* stats, IoGovernor* io_governor,
    bool prefetch_next_block) {
  if (!visitor) {
    return Status::InvalidArgument("sst cursor", "event visitor is required");
  }
  const auto metadata = ReadSstFileMetadata(path);
  if (!metadata.ok()) return metadata.status();
  StreamingInputCursor cursor(
      path, nullptr, nullptr, io_governor, prefetch_next_block);
  Status status = cursor.Open(metadata.ValueOrDie().partition);
  if (!status.ok()) return status;
  SstCursorStats local_stats;
  if (cursor.valid()) {
    local_stats.blocks_read = 1;
    local_stats.peak_buffered_events = cursor.buffered_events();
    local_stats.peak_buffered_bytes = cursor.buffered_bytes();
  }
  while (cursor.valid()) {
    status = visitor(cursor.current());
    if (!status.ok()) {
      if (stats != nullptr) *stats = local_stats;
      return status;
    }
    ++local_stats.events_visited;
    bool loaded_block = false;
    status = cursor.Advance(&loaded_block);
    if (!status.ok()) {
      if (stats != nullptr) *stats = local_stats;
      return status;
    }
    if (loaded_block) {
      ++local_stats.blocks_read;
      local_stats.peak_buffered_events = std::max(
          local_stats.peak_buffered_events, cursor.buffered_events());
      local_stats.peak_buffered_bytes = std::max(
          local_stats.peak_buffered_bytes, cursor.buffered_bytes());
    }
  }
  if (stats != nullptr) *stats = local_stats;
  return Status::OK();
}

Status VisitSstEventsForKey(
    const std::string& path, const LogicalKey& key,
    const std::function<Status(const TemporalEvent&)>& visitor,
    SstCursorStats* stats, CacheManager* cache_manager,
    IoGovernor* io_governor) {
  if (!visitor) {
    return Status::InvalidArgument("sst key cursor", "event visitor is required");
  }
  SstCursorStats local_stats;
  const auto visited = ReadSstFileInternal(
      path, &key, cache_manager, nullptr, false, nullptr, &visitor,
      &local_stats, io_governor);
  if (stats != nullptr) *stats = local_stats;
  if (!visited.ok()) return visited.status();
  return Status::OK();
}

StatusOr<std::vector<TemporalEvent>> ReadSstFile(const std::string& path) {
  return ReadSstFileInternal(path, nullptr, nullptr, nullptr, false, nullptr);
}

StatusOr<std::vector<TemporalEvent>> ReadSstFile(
    const std::string& path, SstMetadata* metadata) {
  if (metadata == nullptr) {
    return Status::InvalidArgument("sst", "missing metadata output");
  }
  return ReadSstFileInternal(path, nullptr, nullptr, metadata, false, nullptr);
}

StatusOr<SstMetadata> ReadSstFileMetadata(const std::string& path) {
  SstMetadata metadata{};
  const auto status = ReadSstFileInternal(
      path, nullptr, nullptr, &metadata, true, nullptr);
  if (!status.ok()) return status.status();
  return metadata;
}

StatusOr<uint32_t> ReadSstBlockCount(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError(path, std::strerror(errno));
  std::string header;
  const Status read = ReadRange(fd, 0, kHeaderSize, path, &header);
  const int close_result = ::close(fd);
  if (!read.ok()) return read;
  if (close_result != 0) return Status::IOError(path, std::strerror(errno));
  const auto decoded = DecodeSstHeader(header);
  if (!decoded.ok()) return decoded.status();
  return decoded.ValueOrDie().block_count;
}

StatusOr<std::vector<TemporalEvent>> ReadSstCandidatesForKey(
    const std::string& path, const LogicalKey& key, CacheManager* cache_manager,
    SstReadStats* stats, IoGovernor* io_governor) {
  return ReadSstFileInternal(
      path, &key, cache_manager, nullptr, false, stats, nullptr, nullptr,
      io_governor);
}

StatusOr<SstOrdinalReadResult> ReadSstEventsAtOrdinals(
    const std::string& path, const BlockPartition& expected_partition,
    const std::vector<uint64_t>& ordinals,
    std::shared_ptr<QueryCancellation> cancellation,
    std::shared_ptr<QueryMemoryAccount> memory_account,
    SstReadStats* stats, IoGovernor* io_governor) {
  if (!std::is_sorted(ordinals.begin(), ordinals.end()) ||
      std::adjacent_find(ordinals.begin(), ordinals.end()) != ordinals.end()) {
    return Status::InvalidArgument("sst ordinal read",
                                   "ordinals must be sorted and unique");
  }
  if (cancellation && cancellation->IsCancelled()) {
    return Status::QueryCancelled("sst ordinal read", "query cancelled before metadata I/O");
  }
  const auto metadata = ReadSstFileMetadata(path);
  if (!metadata.ok()) return metadata.status();
  if (!SameBlockPartition(metadata.ValueOrDie().partition, expected_partition)) {
    return Status::Corruption("sst ordinal read", "partition differs from pinned metadata");
  }
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError(path, std::strerror(errno));
  const auto close_fd = [&]() { ::close(fd); };
  struct stat file_stat {};
  if (::fstat(fd, &file_stat) != 0) {
    const Status status = Status::IOError(path, std::strerror(errno));
    close_fd();
    return status;
  }
  const uint64_t file_size = static_cast<uint64_t>(file_stat.st_size);
  if (file_size < kHeaderSize + kFooterSize) {
    close_fd();
    return Status::Corruption("sst ordinal read", "truncated file");
  }
  std::string footer_bytes;
  Status status = ReadRange(
      fd, file_size - kFooterSize, kFooterSize, path, &footer_bytes);
  if (!status.ok()) {
    close_fd();
    return status;
  }
  if (stats != nullptr) stats->bytes_read += kFooterSize;
  const auto decoded_footer = DecodeFooterBytes(footer_bytes, file_size);
  if (!decoded_footer.ok()) {
    close_fd();
    return decoded_footer.status();
  }
  const Footer footer = decoded_footer.ValueOrDie();
  if (footer.block_count != metadata.ValueOrDie().block_count ||
      footer.identity != metadata.ValueOrDie().identity) {
    close_fd();
    return Status::Corruption("sst ordinal read", "metadata ownership mismatch");
  }
  std::string encoded_index;
  status = ReadRange(fd, footer.index_offset, footer.index_length,
                     path, &encoded_index);
  if (!status.ok()) {
    close_fd();
    return status;
  }
  if (stats != nullptr) stats->bytes_read += footer.index_length;
  if (crc32c::Value(encoded_index.data(), encoded_index.size()) !=
      footer.index_crc) {
    close_fd();
    return Status::Corruption("sst ordinal read", "block index checksum mismatch");
  }
  const auto decoded_index = DecodeBlockIndex(
      encoded_index, footer.block_count);
  if (!decoded_index.ok()) {
    close_fd();
    return decoded_index.status();
  }

  struct SelectedBlock {
    const BlockIndexEntry* block = nullptr;
    uint64_t first_row = 0;
    uint32_t row_count = 0;
    std::vector<uint64_t> ordinals;
  };
  std::vector<SelectedBlock> selected_blocks;
  uint64_t total_rows = 0;
  size_t requested = 0;
  for (const BlockIndexEntry& block : decoded_index.ValueOrDie()) {
    if (cancellation && cancellation->IsCancelled()) {
      close_fd();
      return Status::QueryCancelled("sst ordinal read",
                                    "query cancelled before block metadata I/O");
    }
    if (block.offset < kHeaderSize || block.offset > footer.blob_refs_offset ||
        block.length < kGranuleHeaderSize ||
        block.length > footer.blob_refs_offset - block.offset) {
      close_fd();
      return Status::Corruption("sst ordinal read", "invalid block location");
    }
    const uint32_t rows = block.row_count;
    if (total_rows > std::numeric_limits<uint64_t>::max() - rows) {
      close_fd();
      return Status::Corruption("sst ordinal read", "row count overflow");
    }
    SelectedBlock selected{&block, total_rows, rows, {}};
    while (requested < ordinals.size() &&
           ordinals[requested] < total_rows + rows) {
      if (ordinals[requested] < total_rows) {
        close_fd();
        return Status::Corruption("sst ordinal read", "ordinal mapping regressed");
      }
      selected.ordinals.push_back(ordinals[requested++]);
    }
    if (!selected.ordinals.empty()) selected_blocks.push_back(std::move(selected));
    total_rows += rows;
  }
  if (requested != ordinals.size()) {
    close_fd();
    return Status::Corruption("sst ordinal read", "posting ordinal exceeds source rows");
  }

  struct OrdinalMemoryLease {
    explicit OrdinalMemoryLease(std::shared_ptr<QueryMemoryAccount> owner)
        : account(std::move(owner)) {}
    ~OrdinalMemoryLease() {
      if (account && bytes != 0) account->Release(bytes);
    }
    Status Reserve(uint64_t amount) {
      if (amount > std::numeric_limits<uint64_t>::max() - bytes) {
        return Status::QueryMemoryLimit("sst ordinal read", "memory charge overflow");
      }
      if (account) {
        const Status reserved = account->Reserve(amount);
        if (!reserved.ok()) return reserved;
      }
      bytes += amount;
      return Status::OK();
    }
    std::shared_ptr<QueryMemoryAccount> account;
    uint64_t bytes = 0;
  };
  auto lease = std::make_shared<OrdinalMemoryLease>(std::move(memory_account));
  SstOrdinalReadResult result;
  result.total_row_count = total_rows;
  result.events.reserve(ordinals.size());
  for (const SelectedBlock& selected : selected_blocks) {
    if (cancellation && cancellation->IsCancelled()) {
      close_fd();
      return Status::QueryCancelled("sst ordinal read",
                                    "query cancelled before block I/O");
    }
    if (io_governor != nullptr) {
      const uint64_t now_ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now().time_since_epoch()).count());
      const Status acquired = io_governor->TryAcquire(
          IoTokenRequest{0, 1, 0, 0, false}, now_ns);
      if (!acquired.ok()) {
        close_fd();
        return acquired;
      }
    }
    status = lease->Reserve(selected.block->length);
    if (!status.ok()) {
      close_fd();
      return status;
    }
    uint64_t page_count = 0;
    uint64_t page_bytes_decoded = 0;
    uint64_t charge_bytes_read = 0;
    const auto block_charge = GranuleBlockMemoryCharge(
        fd, path, *selected.block, "sst ordinal read", &page_count,
        &page_bytes_decoded, &charge_bytes_read);
    if (!block_charge.ok()) {
      close_fd();
      return block_charge.status();
    }
    if (block_charge.ValueOrDie() < selected.block->length) {
      close_fd();
      return Status::Corruption("sst ordinal read",
                                "block memory charge is incomplete");
    }
    status = lease->Reserve(
        block_charge.ValueOrDie() - selected.block->length);
    if (!status.ok()) {
      close_fd();
      return status;
    }
    std::string block_bytes;
    status = ReadRange(fd, selected.block->offset, selected.block->length,
                       path, &block_bytes);
    if (!status.ok()) {
      close_fd();
      return status;
    }
    if (stats != nullptr) {
      stats->bytes_read += selected.block->length + charge_bytes_read;
      ++stats->blocks_read;
      stats->page_bytes_decoded += page_bytes_decoded;
      stats->page_decode_count += page_count;
    }
    const auto decode_started = std::chrono::steady_clock::now();
    const Status block_identity = VerifyGranuleBlockIdentity(
        block_bytes, selected.block->content_hash);
    if (!block_identity.ok()) {
      close_fd();
      return block_identity;
    }
    const auto decoded = DecodeGranuleBlock(block_bytes, expected_partition);
    if (stats != nullptr) {
      stats->page_decode_latency_ns += ElapsedSteadyNs(decode_started);
    }
    if (!decoded.ok()) {
      close_fd();
      return decoded.status();
    }
    if (decoded.ValueOrDie().size() != selected.row_count) {
      close_fd();
      return Status::Corruption("sst ordinal read", "block row count changed");
    }
    for (uint64_t ordinal : selected.ordinals) {
      result.events.emplace_back(
          ordinal, decoded.ValueOrDie()[ordinal - selected.first_row]);
    }
  }
  if (::close(fd) != 0) return Status::IOError(path, std::strerror(errno));
  result.memory_retention = std::move(lease);
  return result;
}

}  // namespace cedar
