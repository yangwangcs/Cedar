// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "table/cedar_parquet/parquet_plain_page.h"

#include <limits>

#ifdef LZ4
#include <lz4.h>
#endif
#ifdef ZSTD
#include <zstd.h>
#endif

#include "table/cedar_parquet/compact_protocol.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {
namespace {

constexpr int32_t kDataPage = 0;
constexpr int32_t kPlain = 0;
constexpr int32_t kRle = 3;
constexpr int32_t kBooleanPhysical = 0;
constexpr int32_t kByteArrayPhysical = 6;
constexpr size_t kMaxUncompressedPageSize = 64U * 1024U * 1024U;

Status ReadDataPageHeader(CompactReader* reader, int32_t* values);
void WriteDataPageHeader(CompactWriter* writer, int32_t values);

void AppendFixed32(std::string* destination, uint32_t value) {
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    destination->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

void AppendFixed64(std::string* destination, uint64_t value) {
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    destination->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

uint32_t DecodeFixed32(const char* source) {
  uint32_t value = 0;
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    value |= static_cast<uint32_t>(static_cast<unsigned char>(source[shift / 8])) << shift;
  }
  return value;
}

uint64_t DecodeFixed64(const char* source) {
  uint64_t value = 0;
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    value |= static_cast<uint64_t>(static_cast<unsigned char>(source[shift / 8]))
             << shift;
  }
  return value;
}

void AppendVarint(std::string* destination, uint32_t value) {
  while (value >= 0x80U) {
    destination->push_back(static_cast<char>(value | 0x80U));
    value >>= 7;
  }
  destination->push_back(static_cast<char>(value));
}

Status ReadVarint(const char* data, size_t size, size_t* position, uint32_t* value) {
  uint32_t decoded = 0;
  for (uint32_t shift = 0; shift < 32; shift += 7) {
    if (*position == size) return Status::Corruption("truncated Parquet RLE run");
    const uint8_t byte = static_cast<uint8_t>(data[(*position)++]);
    decoded |= static_cast<uint32_t>(byte & 0x7fU) << shift;
    if ((byte & 0x80U) == 0) {
      *value = decoded;
      return Status::OK();
    }
  }
  return Status::Corruption("overlong Parquet RLE run");
}

bool IsSupportedPhysicalType(int32_t physical_type) {
  return physical_type == 0 || physical_type == 1 || physical_type == 2 ||
         physical_type == 4 || physical_type == 5 || physical_type == 6 ||
         physical_type == 7;
}

Status PrimitiveWidth(int32_t physical_type, int32_t fixed_length, size_t* width) {
  switch (physical_type) {
    case 1:
    case 4:
      *width = 4;
      return Status::OK();
    case 2:
    case 5:
      *width = 8;
      return Status::OK();
    case 7:
      if (fixed_length <= 0) return Status::InvalidArgument("invalid fixed Parquet length");
      *width = static_cast<size_t>(fixed_length);
      return Status::OK();
    default:
      return Status::NotSupported("Parquet physical type has no fixed width");
  }
}

Status CompressPageBody(const std::string& body,
                        CedarParquetCompressionCodec codec,
                        std::string* stored_body) {
  if (codec == CedarParquetCompressionCodec::kUncompressed) {
    *stored_body = body;
    return Status::OK();
  }
  if (codec != CedarParquetCompressionCodec::kLz4Raw &&
      codec != CedarParquetCompressionCodec::kZstd) {
    return Status::NotSupported("unsupported Parquet page codec");
  }
  if (codec == CedarParquetCompressionCodec::kZstd) {
#ifdef ZSTD
    const size_t bound = ZSTD_compressBound(body.size());
    if (ZSTD_isError(bound)) {
      return Status::MemoryLimit("Parquet Zstd page exceeds size limit");
    }
    stored_body->resize(bound);
    const size_t compressed_size = ZSTD_compress(
        stored_body->data(), stored_body->size(), body.data(), body.size(), 3);
    if (ZSTD_isError(compressed_size)) {
      return Status::IOError("unable to compress Parquet Zstd page");
    }
    stored_body->resize(compressed_size);
    return Status::OK();
#else
    return Status::NotSupported("Zstd is unavailable in this build");
#endif
  }
#ifdef LZ4
  if (body.empty()) {
    stored_body->clear();
    return Status::OK();
  }
  const int input_size = static_cast<int>(body.size());
  const int bound = LZ4_compressBound(input_size);
  if (bound <= 0) return Status::MemoryLimit("Parquet LZ4 page exceeds size limit");
  stored_body->resize(static_cast<size_t>(bound));
  const int compressed_size = LZ4_compress_default(
      body.data(), stored_body->data(), input_size, bound);
  if (compressed_size <= 0) return Status::IOError("unable to compress Parquet LZ4 page");
  stored_body->resize(static_cast<size_t>(compressed_size));
  return Status::OK();
#else
  return Status::NotSupported("LZ4 is unavailable in this build");
#endif
}

Status DecompressPageBody(const char* stored_body, size_t stored_size,
                          size_t uncompressed_size,
                          CedarParquetCompressionCodec codec,
                          std::string* body) {
  if (codec == CedarParquetCompressionCodec::kUncompressed) {
    if (stored_size != uncompressed_size) {
      return Status::Corruption("uncompressed Parquet page size mismatch");
    }
    body->assign(stored_body, stored_size);
    return Status::OK();
  }
  if (codec != CedarParquetCompressionCodec::kLz4Raw &&
      codec != CedarParquetCompressionCodec::kZstd) {
    return Status::NotSupported("unsupported Parquet page codec");
  }
  if (codec == CedarParquetCompressionCodec::kZstd) {
#ifdef ZSTD
    body->resize(uncompressed_size);
    const size_t decoded_size = ZSTD_decompress(
        body->data(), body->size(), stored_body, stored_size);
    if (ZSTD_isError(decoded_size) || decoded_size != uncompressed_size) {
      return Status::Corruption("invalid Parquet Zstd page");
    }
    return Status::OK();
#else
    return Status::NotSupported("Zstd is unavailable in this build");
#endif
  }
  if (uncompressed_size == 0) {
    return stored_size == 0 ? Status::OK()
                            : Status::Corruption("invalid empty Parquet LZ4 page");
  }
#ifdef LZ4
  if (stored_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Corruption("Parquet LZ4 page exceeds size limit");
  }
  body->resize(uncompressed_size);
  const int decoded_size = LZ4_decompress_safe(
      stored_body, body->data(), static_cast<int>(stored_size),
      static_cast<int>(uncompressed_size));
  if (decoded_size != static_cast<int>(uncompressed_size)) {
    return Status::Corruption("invalid Parquet LZ4 page");
  }
  return Status::OK();
#else
  return Status::NotSupported("LZ4 is unavailable in this build");
#endif
}

Status EncodeDataPageEnvelope(const std::string& body, int32_t num_values,
                              CedarParquetCompressionCodec codec,
                              std::string* page,
                              CedarParquetDataPageSize* page_size) {
  if (body.size() > kMaxUncompressedPageSize) {
    return Status::MemoryLimit("Parquet page exceeds decoded size limit");
  }
  std::string stored_body;
  Status status = CompressPageBody(body, codec, &stored_body);
  if (!status.ok()) return status;
  if (stored_body.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
    return Status::MemoryLimit("Parquet compressed page exceeds i32 size");
  }
  CompactWriter writer;
  writer.WriteStructBegin();
  writer.WriteI32Field(1, kDataPage);
  writer.WriteI32Field(2, static_cast<int32_t>(body.size()));
  writer.WriteI32Field(3, static_cast<int32_t>(stored_body.size()));
  writer.WriteI32Field(4, static_cast<int32_t>(
                              crc32c::Value(stored_body.data(), stored_body.size())));
  writer.WriteStructFieldBegin(5);
  WriteDataPageHeader(&writer, num_values);
  writer.WriteFieldStop();
  writer.WriteStructEnd();
  *page = writer.data();
  if (page_size != nullptr) {
    page_size->compressed_size = page->size() + stored_body.size();
    page_size->uncompressed_size = page->size() + body.size();
  }
  page->append(stored_body);
  return Status::OK();
}

Status ParseDataPageEnvelope(const std::string& page,
                             CedarParquetCompressionCodec codec,
                             int32_t* num_values, std::string* body,
                             size_t* consumed,
                             size_t max_uncompressed_body_bytes) {
  CompactReader reader(page);
  int32_t page_type = -1;
  int32_t uncompressed_size = -1;
  int32_t compressed_size = -1;
  int32_t stored_crc = 0;
  bool has_header = false;
  bool has_crc = false;
  reader.ReadStructBegin();
  while (true) {
    int16_t field_id = 0;
    uint8_t type = 0;
    Status status = reader.ReadFieldBegin(&field_id, &type);
    if (!status.ok()) return status;
    if (type == kCompactStop) break;
    if (field_id == 5) {
      if (type != kCompactStruct) return Status::Corruption("invalid data-page struct");
      status = ReadDataPageHeader(&reader, num_values);
      if (!status.ok()) return status;
      has_header = true;
      continue;
    }
    if (type != kCompactI32) return Status::Corruption("invalid page header field");
    int32_t value = 0;
    status = reader.ReadI32(&value);
    if (!status.ok()) return status;
    if (field_id == 1) page_type = value;
    else if (field_id == 2) uncompressed_size = value;
    else if (field_id == 3) compressed_size = value;
    else if (field_id == 4) {
      stored_crc = value;
      has_crc = true;
    } else {
      return Status::NotSupported("unsupported Parquet page header field");
    }
  }
  reader.ReadStructEnd();
  if (page_type != kDataPage || !has_header || !has_crc || uncompressed_size < 0 ||
      compressed_size < 0 || *num_values < 0 ||
      static_cast<size_t>(uncompressed_size) > kMaxUncompressedPageSize ||
      static_cast<size_t>(uncompressed_size) > max_uncompressed_body_bytes) {
    return Status::Corruption("unsupported Parquet data page");
  }
  const size_t body_offset = reader.position();
  const size_t stored_size = static_cast<size_t>(compressed_size);
  if (body_offset > page.size() || stored_size > page.size() - body_offset) {
    return Status::Corruption("truncated Parquet data-page body");
  }
  const char* stored_body = page.data() + body_offset;
  if (crc32c::Value(stored_body, stored_size) !=
          static_cast<uint32_t>(stored_crc)) {
    return Status::Corruption("Parquet data-page CRC32C mismatch");
  }
  Status status = DecompressPageBody(stored_body, stored_size,
                                     static_cast<size_t>(uncompressed_size), codec,
                                     body);
  if (!status.ok()) return status;
  if (consumed != nullptr) *consumed = body_offset + stored_size;
  return Status::OK();
}

void WriteDataPageHeader(CompactWriter* writer, int32_t values) {
  writer->WriteStructBegin();
  writer->WriteI32Field(1, values);
  writer->WriteI32Field(2, kPlain);
  writer->WriteI32Field(3, kRle);
  writer->WriteI32Field(4, kRle);
  writer->WriteFieldStop();
  writer->WriteStructEnd();
}

Status ReadDataPageHeader(CompactReader* reader, int32_t* values) {
  reader->ReadStructBegin();
  bool has_values = false;
  bool has_encoding = false;
  bool has_definition_level_encoding = false;
  bool has_repetition_level_encoding = false;
  while (true) {
    int16_t field_id = 0;
    uint8_t type = 0;
    Status status = reader->ReadFieldBegin(&field_id, &type);
    if (!status.ok()) return status;
    if (type == kCompactStop) break;
    if (type != kCompactI32) return Status::Corruption("invalid data-page header");
    int32_t value = 0;
    status = reader->ReadI32(&value);
    if (!status.ok()) return status;
    if (field_id == 1) {
      *values = value;
      has_values = true;
    } else if (field_id == 2) {
      if (value != kPlain) return Status::NotSupported("unsupported Parquet page encoding");
      has_encoding = true;
    } else if (field_id == 3) {
      if (value != kRle) {
        return Status::NotSupported("unsupported Parquet definition-level encoding");
      }
      has_definition_level_encoding = true;
    } else if (field_id == 4) {
      if (value != kRle) {
        return Status::NotSupported("unsupported Parquet repetition-level encoding");
      }
      has_repetition_level_encoding = true;
    } else {
      return Status::NotSupported("unsupported data-page field");
    }
  }
  reader->ReadStructEnd();
  if (!has_values || !has_encoding || !has_definition_level_encoding ||
      !has_repetition_level_encoding || *values < 0) {
    return Status::Corruption("invalid data-page values");
  }
  return Status::OK();
}

}  // namespace

Status EncodePlainByteArrayDataPage(const std::vector<Slice>& values,
                                    std::string* page,
                                    CedarParquetCompressionCodec codec,
                                    CedarParquetDataPageSize* page_size) {
  if (values.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
    return Status::MemoryLimit("too many Parquet page values");
  }
  std::string body;
  for (const Slice& value : values) {
    if (value.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::MemoryLimit("Parquet BYTE_ARRAY value exceeds 4 GiB");
    }
    AppendFixed32(&body, static_cast<uint32_t>(value.size()));
    body.append(value.data(), value.size());
  }
  if (body.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
    return Status::MemoryLimit("Parquet page exceeds i32 size");
  }
  return EncodeDataPageEnvelope(body, static_cast<int32_t>(values.size()), codec,
                                 page, page_size);
}

Status DecodePlainByteArrayDataPage(const std::string& page,
                                    std::vector<std::string>* values,
                                    size_t* consumed,
                                    CedarParquetCompressionCodec codec,
                                    size_t max_uncompressed_body_bytes) {
  return DecodePlainByteArrayDataPage(
      page, values, consumed, codec, max_uncompressed_body_bytes,
      max_uncompressed_body_bytes, std::numeric_limits<uint32_t>::max());
}

Status DecodePlainByteArrayDataPage(const std::string& page,
                                    std::vector<std::string>* values,
                                    size_t* consumed,
                                    CedarParquetCompressionCodec codec,
                                    size_t max_uncompressed_body_bytes,
                                    size_t max_value_bytes,
                                    size_t max_values) {
  int32_t num_values = -1;
  std::string decoded_body;
  size_t page_bytes = 0;
  Status status = ParseDataPageEnvelope(page, codec, &num_values, &decoded_body,
                                        &page_bytes, max_uncompressed_body_bytes);
  if (!status.ok()) return status;
  if (static_cast<size_t>(num_values) > max_values) {
    return Status::Corruption("Parquet page exceeds configured value count");
  }
  const char* body = decoded_body.data();
  const size_t body_size = decoded_body.size();
  size_t position = 0;
  values->clear();
  values->reserve(static_cast<size_t>(num_values));
  for (int32_t index = 0; index < num_values; ++index) {
    if (body_size - position < 4) return Status::Corruption("truncated BYTE_ARRAY length");
    const uint32_t value_size = DecodeFixed32(body + position);
    position += 4;
    if (value_size > body_size - position) return Status::Corruption("truncated BYTE_ARRAY value");
    if (value_size > max_value_bytes) {
      return Status::Corruption("Parquet BYTE_ARRAY exceeds configured row bound");
    }
    values->emplace_back(body + position, value_size);
    position += value_size;
  }
  if (position != body_size) return Status::Corruption("trailing BYTE_ARRAY page data");
  if (consumed != nullptr) *consumed = page_bytes;
  return Status::OK();
}

Status EncodePlainPrimitiveDataPage(
    const std::vector<std::optional<std::string>>& values, int32_t physical_type,
    bool optional, std::string* page, int32_t fixed_length,
    CedarParquetCompressionCodec codec, CedarParquetDataPageSize* page_size) {
  if (!IsSupportedPhysicalType(physical_type) ||
      values.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
    return Status::InvalidArgument("unsupported Parquet primitive page");
  }
  std::string body;
  if (optional) {
    std::string levels;
    size_t begin = 0;
    while (begin < values.size()) {
      const bool defined = values[begin].has_value();
      size_t end = begin + 1;
      while (end < values.size() && values[end].has_value() == defined) ++end;
      const size_t count = end - begin;
      if (count > (std::numeric_limits<uint32_t>::max() >> 1)) {
        return Status::MemoryLimit("too many Parquet definition levels");
      }
      AppendVarint(&levels, static_cast<uint32_t>(count << 1));
      levels.push_back(defined ? '\1' : '\0');
      begin = end;
    }
    if (levels.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::MemoryLimit("Parquet definition levels exceed 4 GiB");
    }
    AppendFixed32(&body, static_cast<uint32_t>(levels.size()));
    body.append(levels);
  }
  size_t width = 0;
  if (physical_type != kBooleanPhysical && physical_type != kByteArrayPhysical) {
    Status status = PrimitiveWidth(physical_type, fixed_length, &width);
    if (!status.ok()) return status;
  }
  uint8_t bool_byte = 0;
  uint8_t bool_bits = 0;
  for (const auto& value : values) {
    if (!value.has_value()) {
      if (!optional) return Status::InvalidArgument("required Parquet value is null");
      continue;
    }
    if (physical_type == kBooleanPhysical) {
      if (value->size() != 1 || ((*value)[0] != '\0' && (*value)[0] != '\1')) {
        return Status::InvalidArgument("invalid Parquet BOOLEAN value");
      }
      if ((*value)[0] == '\1') bool_byte |= static_cast<uint8_t>(1U << bool_bits);
      if (++bool_bits == 8) {
        body.push_back(static_cast<char>(bool_byte));
        bool_byte = 0;
        bool_bits = 0;
      }
    } else if (physical_type == kByteArrayPhysical) {
      if (value->size() > std::numeric_limits<uint32_t>::max()) {
        return Status::MemoryLimit("Parquet BYTE_ARRAY value exceeds 4 GiB");
      }
      AppendFixed32(&body, static_cast<uint32_t>(value->size()));
      body.append(*value);
    } else {
      if (value->size() != width) return Status::InvalidArgument("invalid fixed Parquet value");
      body.append(*value);
    }
  }
  if (bool_bits != 0) body.push_back(static_cast<char>(bool_byte));
  if (body.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
    return Status::MemoryLimit("Parquet page exceeds i32 size");
  }
  return EncodeDataPageEnvelope(body, static_cast<int32_t>(values.size()), codec,
                                 page, page_size);
}

Status DecodePlainPrimitiveDataPage(
    const std::string& page, int32_t physical_type, bool optional,
    std::vector<std::optional<std::string>>* values, size_t* consumed,
    int32_t fixed_length, CedarParquetCompressionCodec codec,
    size_t max_uncompressed_body_bytes) {
  return DecodePlainPrimitiveDataPage(
      page, physical_type, optional, values, consumed, fixed_length, codec,
      max_uncompressed_body_bytes, max_uncompressed_body_bytes,
      std::numeric_limits<uint32_t>::max());
}

Status DecodePlainPrimitiveDataPage(
    const std::string& page, int32_t physical_type, bool optional,
    std::vector<std::optional<std::string>>* values, size_t* consumed,
    int32_t fixed_length, CedarParquetCompressionCodec codec,
    size_t max_uncompressed_body_bytes, size_t max_value_bytes,
    size_t max_values) {
  if (!IsSupportedPhysicalType(physical_type)) {
    return Status::NotSupported("unsupported Parquet primitive physical type");
  }
  int32_t num_values = 0;
  std::string decoded_body;
  size_t page_bytes = 0;
  Status status = ParseDataPageEnvelope(page, codec, &num_values, &decoded_body,
                                        &page_bytes, max_uncompressed_body_bytes);
  if (!status.ok()) return status;
  if (static_cast<size_t>(num_values) > max_values) {
    return Status::Corruption("Parquet page exceeds configured value count");
  }
  const char* body = decoded_body.data();
  const size_t body_size = decoded_body.size();
  size_t position = 0;
  std::vector<bool> defined(static_cast<size_t>(num_values), true);
  if (optional) {
    if (body_size < 4) return Status::Corruption("missing Parquet definition levels");
    const uint32_t levels_size = DecodeFixed32(body);
    position = 4;
    if (levels_size > body_size - position) return Status::Corruption("truncated Parquet definition levels");
    const size_t levels_end = position + levels_size;
    size_t level_position = position;
    size_t output = 0;
    while (level_position < levels_end) {
      uint32_t header = 0;
      status = ReadVarint(body, levels_end, &level_position, &header);
      if (!status.ok() || (header & 1U) != 0 || header == 0 || level_position == levels_end) {
        return Status::Corruption("invalid Parquet definition-level RLE run");
      }
      const uint32_t count = header >> 1;
      const uint8_t level = static_cast<uint8_t>(body[level_position++]);
      if (level > 1 || count > defined.size() - output) {
        return Status::Corruption("invalid Parquet definition-level value");
      }
      for (uint32_t index = 0; index < count; ++index) defined[output++] = level == 1;
    }
    if (output != defined.size()) return Status::Corruption("incomplete Parquet definition levels");
    position = levels_end;
  }
  size_t width = 0;
  if (physical_type != kBooleanPhysical && physical_type != kByteArrayPhysical) {
    status = PrimitiveWidth(physical_type, fixed_length, &width);
    if (!status.ok()) return status;
    if (width > max_value_bytes) {
      return Status::Corruption("Parquet fixed value exceeds configured row bound");
    }
  }
  values->clear();
  values->reserve(defined.size());
  size_t bool_index = 0;
  const size_t bool_data_offset = position;
  for (bool is_defined : defined) {
    if (!is_defined) {
      values->emplace_back(std::nullopt);
      continue;
    }
    if (physical_type == kBooleanPhysical) {
      if (bool_data_offset + bool_index / 8 >= body_size) return Status::Corruption("truncated Parquet BOOLEAN");
      const bool value = (static_cast<uint8_t>(body[bool_data_offset + bool_index / 8]) >>
                          (bool_index % 8)) & 1U;
      values->emplace_back(std::string(1, value ? '\1' : '\0'));
      ++bool_index;
    } else if (physical_type == kByteArrayPhysical) {
      if (body_size - position < 4) return Status::Corruption("truncated Parquet BYTE_ARRAY length");
      const uint32_t size = DecodeFixed32(body + position);
      position += 4;
      if (size > body_size - position) return Status::Corruption("truncated Parquet BYTE_ARRAY");
      if (size > max_value_bytes) {
        return Status::Corruption("Parquet BYTE_ARRAY exceeds configured row bound");
      }
      values->emplace_back(std::string(body + position, size));
      position += size;
    } else {
      if (width > body_size - position) return Status::Corruption("truncated fixed Parquet value");
      values->emplace_back(std::string(body + position, width));
      position += width;
    }
  }
  if (physical_type == kBooleanPhysical) {
    position = bool_data_offset + (bool_index + 7) / 8;
  }
  if (position != body_size) return Status::Corruption("trailing primitive Parquet page data");
  if (consumed != nullptr) *consumed = page_bytes;
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
