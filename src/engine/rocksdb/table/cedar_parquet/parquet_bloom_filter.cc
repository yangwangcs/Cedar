// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "table/cedar_parquet/parquet_bloom_filter.h"

#include "table/cedar_parquet/compact_protocol.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {
namespace {

uint32_t ReadU32LE(const char* data) {
  return static_cast<uint32_t>(static_cast<unsigned char>(data[0])) |
         (static_cast<uint32_t>(static_cast<unsigned char>(data[1])) << 8U) |
         (static_cast<uint32_t>(static_cast<unsigned char>(data[2])) << 16U) |
         (static_cast<uint32_t>(static_cast<unsigned char>(data[3])) << 24U);
}

void WriteEmptyUnionMember(CompactWriter* writer, int16_t field_id) {
  writer->WriteStructFieldBegin(field_id);
  writer->WriteStructBegin();
  writer->WriteFieldStop();
  writer->WriteStructEnd();
}

void WriteBloomHeader(CompactWriter* writer, int32_t bitset_bytes) {
  writer->WriteStructBegin();
  writer->WriteI32Field(1, bitset_bytes);
  writer->WriteStructFieldBegin(2);
  writer->WriteStructBegin();
  WriteEmptyUnionMember(writer, 1);
  writer->WriteFieldStop();
  writer->WriteStructEnd();
  writer->WriteStructFieldBegin(3);
  writer->WriteStructBegin();
  WriteEmptyUnionMember(writer, 1);
  writer->WriteFieldStop();
  writer->WriteStructEnd();
  writer->WriteStructFieldBegin(4);
  writer->WriteStructBegin();
  WriteEmptyUnionMember(writer, 1);
  writer->WriteFieldStop();
  writer->WriteStructEnd();
  writer->WriteFieldStop();
  writer->WriteStructEnd();
}

Status ReadEmptyUnionMember(CompactReader* reader, int16_t expected_field,
                            const char* name) {
  int16_t field_id = 0;
  uint8_t type = 0;
  Status status = reader->ReadFieldBegin(&field_id, &type);
  if (!status.ok()) return status;
  if (field_id != expected_field || type != kCompactStruct) {
    return Status::Corruption(std::string("invalid Parquet Bloom ") + name);
  }
  reader->ReadStructBegin();
  status = reader->ReadFieldBegin(&field_id, &type);
  if (!status.ok()) return status;
  if (type != kCompactStop) {
    return Status::Corruption(std::string("non-empty Parquet Bloom ") + name);
  }
  reader->ReadStructEnd();
  status = reader->ReadFieldBegin(&field_id, &type);
  if (!status.ok()) return status;
  if (type != kCompactStop) {
    return Status::Corruption(std::string("invalid Parquet Bloom ") + name);
  }
  return Status::OK();
}

Status ReadBloomHeader(CompactReader* reader, int32_t* bitset_bytes) {
  reader->ReadStructBegin();
  bool has_size = false;
  bool has_algorithm = false;
  bool has_hash = false;
  bool has_compression = false;
  for (;;) {
    int16_t field_id = 0;
    uint8_t type = 0;
    Status status = reader->ReadFieldBegin(&field_id, &type);
    if (!status.ok()) return status;
    if (type == kCompactStop) break;
    if (field_id == 1) {
      if (type != kCompactI32) return Status::Corruption("invalid Parquet Bloom size");
      status = reader->ReadI32(bitset_bytes);
      if (!status.ok()) return status;
      has_size = true;
    } else if (field_id == 2 || field_id == 3 || field_id == 4) {
      if (type != kCompactStruct) return Status::Corruption("invalid Parquet Bloom union");
      reader->ReadStructBegin();
      status = ReadEmptyUnionMember(reader, 1,
                                    field_id == 2 ? "algorithm" :
                                    field_id == 3 ? "hash" : "compression");
      if (!status.ok()) return status;
      reader->ReadStructEnd();
      if (field_id == 2) has_algorithm = true;
      if (field_id == 3) has_hash = true;
      if (field_id == 4) has_compression = true;
    } else {
      return Status::NotSupported("unsupported Parquet Bloom header field");
    }
  }
  reader->ReadStructEnd();
  if (!has_size || !has_algorithm || !has_hash || !has_compression) {
    return Status::Corruption("incomplete Parquet Bloom header");
  }
  return Status::OK();
}

}  // namespace

uint64_t CedarSplitBlockBloomFilter::Hash(std::string_view value) {
  return XXH64(value.data(), value.size(), 0);
}

void CedarSplitBlockBloomFilter::InsertHash(uint64_t hash) {
  const uint32_t blocks = bitset_bytes_ / kBytesPerBlock;
  const uint32_t block = static_cast<uint32_t>(((hash >> 32) * blocks) >> 32);
  const uint32_t key = static_cast<uint32_t>(hash);
  for (size_t index = 0; index < 8; ++index) {
    const uint32_t mask = 1U << ((key * kS[index]) >> 27U);
    const size_t offset = (static_cast<size_t>(block) * 8U + index) * 4U;
    uint32_t word = ReadU32LE(bitset_.data() + offset);
    word |= mask;
    for (size_t byte = 0; byte < 4; ++byte) {
      bitset_[offset + byte] = static_cast<char>(word >> (byte * 8));
    }
  }
}

bool CedarSplitBlockBloomFilter::FindHash(uint64_t hash) const {
  const uint32_t blocks = bitset_bytes_ / kBytesPerBlock;
  const uint32_t block = static_cast<uint32_t>(((hash >> 32) * blocks) >> 32);
  const uint32_t key = static_cast<uint32_t>(hash);
  for (size_t index = 0; index < 8; ++index) {
    const uint32_t mask = 1U << ((key * kS[index]) >> 27U);
    const size_t offset = (static_cast<size_t>(block) * 8U + index) * 4U;
    if ((ReadU32LE(bitset_.data() + offset) & mask) == 0) return false;
  }
  return true;
}

bool CedarSplitBlockBloomFilter::MayContain(std::string_view value) const {
  return bitset_bytes_ != 0 && FindHash(Hash(value));
}

Status CedarSplitBlockBloomFilter::Build(const std::string* const* values,
                                         size_t count, std::string* encoded) {
  if (encoded == nullptr || (count != 0 && values == nullptr)) {
    return Status::InvalidArgument("missing Parquet Bloom input");
  }
  uint64_t required = std::max<uint64_t>(kBytesPerBlock,
                                         static_cast<uint64_t>(count) * 10U / 8U);
  uint32_t bytes = kBytesPerBlock;
  while (bytes < required) {
    if (bytes > kMaxBytes / 2U) return Status::MemoryLimit("Parquet Bloom is too large");
    bytes *= 2U;
  }
  CedarSplitBlockBloomFilter filter;
  filter.bitset_bytes_ = bytes;
  filter.bitset_.assign(bytes, '\0');
  for (size_t index = 0; index < count; ++index) {
    if (values[index] == nullptr || values[index]->size() != 32) {
      return Status::Corruption("Cedar Parquet Bloom requires 32-byte user keys");
    }
    filter.InsertHash(Hash(*values[index]));
  }
  CompactWriter header;
  WriteBloomHeader(&header, static_cast<int32_t>(bytes));
  *encoded = header.data();
  encoded->append(filter.bitset_);
  return Status::OK();
}

Status CedarSplitBlockBloomFilter::Decode(std::string_view encoded,
                                          CedarSplitBlockBloomFilter* filter) {
  if (filter == nullptr || encoded.empty()) {
    return Status::InvalidArgument("missing Parquet Bloom output");
  }
  std::string owned(encoded);
  CompactReader reader(owned, kMaxBytes);
  int32_t bytes = 0;
  Status status = ReadBloomHeader(&reader, &bytes);
  if (!status.ok()) return status;
  if (bytes < static_cast<int32_t>(kBytesPerBlock) ||
      bytes > static_cast<int32_t>(kMaxBytes) ||
      (static_cast<uint32_t>(bytes) & (static_cast<uint32_t>(bytes) - 1U)) != 0U ||
      static_cast<size_t>(bytes) != owned.size() - reader.position()) {
    return Status::Corruption("invalid Parquet Bloom bitset");
  }
  filter->bitset_bytes_ = static_cast<uint32_t>(bytes);
  filter->bitset_.assign(owned.data() + reader.position(), static_cast<size_t>(bytes));
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
