// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/columnar/granule_block.h"

#include <cstring>
#include <map>

namespace cedar {
namespace {

constexpr uint32_t kMagic = 0x354b4247U;  // GBK5
constexpr uint16_t kVersion = 5;
constexpr uint16_t kHeaderSize = 28;
constexpr size_t kBlobReferenceBytes = 60;

struct PagePayload {
  PageType type;
  PhysicalType physical_type;
  std::string bytes;
  uint32_t value_count;
  uint64_t first_row;
  uint32_t row_count;
};

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

void PutU8(std::string* output, uint8_t value) {
  output->push_back(static_cast<char>(value));
}
void PutU16(std::string* output, uint16_t value) {
  PutU8(output, value);
  PutU8(output, value >> 8);
}
void PutU32(std::string* output, uint32_t value) {
  for (uint32_t shift = 0; shift < 32; shift += 8) PutU8(output, value >> shift);
}
void PutU64(std::string* output, uint64_t value) {
  for (uint32_t shift = 0; shift < 64; shift += 8) PutU8(output, value >> shift);
}
bool GetU8(const std::string& input, size_t* offset, uint8_t* value) {
  if (*offset >= input.size()) return false;
  *value = static_cast<uint8_t>(input[(*offset)++]);
  return true;
}
bool GetU16(const std::string& input, size_t* offset, uint16_t* value) {
  uint8_t low;
  uint8_t high;
  if (!GetU8(input, offset, &low) || !GetU8(input, offset, &high)) return false;
  *value = low | (static_cast<uint16_t>(high) << 8);
  return true;
}
bool GetU32(const std::string& input, size_t* offset, uint32_t* value) {
  *value = 0;
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    uint8_t byte;
    if (!GetU8(input, offset, &byte)) return false;
    *value |= static_cast<uint32_t>(byte) << shift;
  }
  return true;
}
bool GetU64(const std::string& input, size_t* offset, uint64_t* value) {
  *value = 0;
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    uint8_t byte;
    if (!GetU8(input, offset, &byte)) return false;
    *value |= static_cast<uint64_t>(byte) << shift;
  }
  return true;
}

size_t RankPresence(const std::string& bitmap, size_t row) {
  return static_cast<size_t>(std::count(bitmap.begin(), bitmap.begin() + row,
                                         static_cast<char>(1)));
}

void PutBlobReference(std::string* output, const BlobRef& reference) {
  output->append(reinterpret_cast<const char*>(reference.content_hash.bytes.data()),
                 reference.content_hash.bytes.size());
  PutU64(output, reference.raw_length);
  PutU32(output, reference.hint.shard_id);
  PutU64(output, reference.hint.segment_id);
  PutU64(output, reference.hint.offset);
}

bool GetBlobReference(const std::string& input, size_t* offset, BlobRef* reference) {
  if (input.size() - *offset < kBlobReferenceBytes) return false;
  std::memcpy(reference->content_hash.bytes.data(), input.data() + *offset,
              reference->content_hash.bytes.size());
  *offset += reference->content_hash.bytes.size();
  return GetU64(input, offset, &reference->raw_length) &&
      GetU32(input, offset, &reference->hint.shard_id) &&
      GetU64(input, offset, &reference->hint.segment_id) &&
      GetU64(input, offset, &reference->hint.offset);
}

bool ReadU64s(const std::string& input, uint32_t rows, std::vector<uint64_t>* output) {
  if (input.size() != rows * sizeof(uint64_t)) return false;
  size_t offset = 0;
  output->clear();
  output->reserve(rows);
  for (uint32_t row = 0; row < rows; ++row) {
    uint64_t value;
    if (!GetU64(input, &offset, &value)) return false;
    output->push_back(value);
  }
  return true;
}

LogicalKey MakeKey(const BlockPartition& partition, uint64_t entity_id,
                   uint64_t target_id, uint64_t edge_id) {
  if (partition.entity_type == EntityType::Vertex) {
    return partition.key_kind == LogicalKeyKind::kExistence
        ? LogicalKey::VertexExistence(entity_id)
        : LogicalKey::VertexProperty(entity_id, partition.column_id);
  }
  return partition.key_kind == LogicalKeyKind::kExistence
      ? LogicalKey::EdgeExistence(entity_id, target_id, partition.edge_type, edge_id,
                                  partition.entity_type)
      : LogicalKey::EdgeProperty(entity_id, target_id, partition.edge_type, edge_id,
                                 partition.column_id, partition.entity_type);
}

}  // namespace

BlobHash ComputeGranuleBlockIdentity(const std::string& encoded_header,
                                     const std::string& encoded_directory) {
  std::string material;
  PutU64(&material, encoded_header.size());
  material.append(encoded_header);
  PutU64(&material, encoded_directory.size());
  material.append(encoded_directory);
  return Blake3Hash(material);
}

Status VerifyGranuleBlockIdentity(const std::string& bytes,
                                  const BlobHash& expected_identity) {
  if (bytes.size() < kHeaderSize) {
    return Status::Corruption("granule", "truncated identity header");
  }
  const std::string header = bytes.substr(0, kHeaderSize);
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t rows = 0;
  uint16_t version = 0;
  uint16_t header_size = 0;
  uint64_t directory_offset = 0;
  uint64_t directory_length = 0;
  if (!GetU32(header, &offset, &magic) ||
      !GetU16(header, &offset, &version) ||
      !GetU16(header, &offset, &header_size) ||
      !GetU32(header, &offset, &rows) ||
      !GetU64(header, &offset, &directory_offset) ||
      !GetU64(header, &offset, &directory_length) ||
      magic != kMagic || version != kVersion || header_size != kHeaderSize ||
      rows == 0 || directory_offset > bytes.size() ||
      directory_length > bytes.size() - directory_offset) {
    return Status::Corruption("granule", "invalid identity envelope");
  }
  const std::string directory =
      bytes.substr(directory_offset, directory_length);
  if (ComputeGranuleBlockIdentity(header, directory) != expected_identity) {
    return Status::Corruption("granule", "block identity mismatch");
  }
  return Status::OK();
}

Status AppendGranuleInlineValue(const Value& value, std::string* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("granule", "missing inline value output");
  }
  if (value.type() == PhysicalType::kFloat32) {
    uint32_t bits = 0;
    const float number = std::get<float>(value.data());
    std::memcpy(&bits, &number, sizeof(bits));
    PutU32(output, bits);
    return Status::OK();
  }
  if (value.type() == PhysicalType::kFloat64) {
    uint64_t bits = 0;
    const double number = std::get<double>(value.data());
    std::memcpy(&bits, &number, sizeof(bits));
    PutU64(output, bits);
    return Status::OK();
  }
  const std::string encoded = value.Encode();
  if (encoded.size() > UINT32_MAX) {
    return Status::InvalidArgument("granule", "inline value exceeds UInt32");
  }
  PutU32(output, static_cast<uint32_t>(encoded.size()));
  output->append(encoded);
  return Status::OK();
}

bool DecodeGranuleInlineValue(const std::string& input, size_t* offset,
                              PhysicalType physical_type,
                              std::optional<Value>* value) {
  if (offset == nullptr || value == nullptr) return false;
  if (physical_type == PhysicalType::kFloat32) {
    uint32_t bits = 0;
    if (!GetU32(input, offset, &bits)) return false;
    float number = 0;
    std::memcpy(&number, &bits, sizeof(number));
    *value = Value::Float32(number);
    return true;
  }
  if (physical_type == PhysicalType::kFloat64) {
    uint64_t bits = 0;
    if (!GetU64(input, offset, &bits)) return false;
    double number = 0;
    std::memcpy(&number, &bits, sizeof(number));
    *value = Value::Float64(number);
    return true;
  }
  uint32_t length = 0;
  if (!GetU32(input, offset, &length) ||
      length > input.size() - *offset) {
    return false;
  }
  *value = Value::Decode(input.substr(*offset, length));
  *offset += length;
  return value->has_value() && value->value().type() == physical_type;
}

StatusOr<GranuleBlock> BuildGranuleBlock(
    const BlockPartition& partition, const std::vector<TemporalEvent>& events) {
  if (events.empty()) return Status::InvalidArgument("granule", "empty block");
  if (events.size() > 8192) return Status::InvalidArgument("granule", "row limit exceeded");

  std::string entity_ids;
  std::string target_ids;
  std::string edge_ids;
  std::string valid_from;
  std::string commit_seq;
  std::string operations;
  std::string value_classes;
  std::string inline_presence;
  std::string blob_presence;
  std::vector<std::pair<uint32_t, std::string>> inline_values;
  std::vector<std::pair<uint32_t, std::string>> blob_references;

  for (const TemporalEvent& event : events) {
    const LogicalKey& key = event.logical_key();
    if (key.entity_type() != partition.entity_type || key.kind() != partition.key_kind ||
        key.schema_column_id() != partition.column_id || key.edge_type() != partition.edge_type ||
        event.schema_epoch() != partition.schema_epoch) {
      return Status::SchemaMismatch("granule", "event partition mismatch");
    }
    PutU64(&entity_ids, key.entity_id());
    PutU64(&target_ids, key.target_id());
    PutU64(&edge_ids, key.edge_id());
    PutU64(&valid_from, event.valid_from());
    PutU64(&commit_seq, event.commit_seq());
    PutU8(&operations, event.is_delete() ? 1 : 0);
    if (event.is_delete()) {
      PutU8(&value_classes, 0);
    } else if (event.is_blob_reference()) {
      PutU8(&value_classes, 2);
    } else {
      if (event.value().type() != partition.physical_type) {
        return Status::SchemaMismatch("granule", "value physical type mismatch");
      }
      PutU8(&value_classes, 1);
    }
    inline_presence.push_back(static_cast<char>(!event.is_delete() &&
                                                 !event.is_blob_reference()));
    blob_presence.push_back(static_cast<char>(!event.is_delete() &&
                                               event.is_blob_reference()));
  }

  // Rebuild variable payload rows from event positions. Their dense values do
  // not share the system-page row count, but each fragment still identifies a
  // contiguous source-row range in the directory.
  inline_values.clear();
  blob_references.clear();
  for (uint32_t row = 0; row < events.size(); ++row) {
    const TemporalEvent& event = events[row];
    if (event.is_delete()) continue;
    if (event.is_blob_reference()) {
      std::string encoded;
      PutBlobReference(&encoded, *event.blob_ref());
      blob_references.emplace_back(row, std::move(encoded));
    } else {
      std::string encoded;
      const Status encoded_value =
          AppendGranuleInlineValue(event.value(), &encoded);
      if (!encoded_value.ok()) return encoded_value;
      inline_values.emplace_back(row, std::move(encoded));
    }
  }

  std::vector<PagePayload> pages = {
      {PageType::kEntityId, PhysicalType::kInt64, std::move(entity_ids), static_cast<uint32_t>(events.size()), 0, static_cast<uint32_t>(events.size())},
      {PageType::kTargetId, PhysicalType::kInt64, std::move(target_ids), static_cast<uint32_t>(events.size()), 0, static_cast<uint32_t>(events.size())},
      {PageType::kEdgeId, PhysicalType::kInt64, std::move(edge_ids), static_cast<uint32_t>(events.size()), 0, static_cast<uint32_t>(events.size())},
      {PageType::kValidFrom, PhysicalType::kTimestamp64, std::move(valid_from), static_cast<uint32_t>(events.size()), 0, static_cast<uint32_t>(events.size())},
      {PageType::kCommitSeq, PhysicalType::kInt64, std::move(commit_seq), static_cast<uint32_t>(events.size()), 0, static_cast<uint32_t>(events.size())},
      {PageType::kOperation, PhysicalType::kBool, std::move(operations), static_cast<uint32_t>(events.size()), 0, static_cast<uint32_t>(events.size())},
      {PageType::kValueClass, PhysicalType::kInt32, std::move(value_classes), static_cast<uint32_t>(events.size()), 0, static_cast<uint32_t>(events.size())},
      {PageType::kInlinePresence, PhysicalType::kBool, std::move(inline_presence), static_cast<uint32_t>(events.size()), 0, static_cast<uint32_t>(events.size())},
      {PageType::kBlobPresence, PhysicalType::kBool, std::move(blob_presence), static_cast<uint32_t>(events.size()), 0, static_cast<uint32_t>(events.size())},
  };
  const auto append_variable_fragments = [&pages](PageType type,
                                                   PhysicalType physical_type,
                                                   const std::vector<std::pair<uint32_t, std::string>>& values) -> Status {
    if (values.empty()) {
      pages.push_back(PagePayload{type, physical_type, {}, 0, 0, 0});
      return Status::OK();
    }
    std::string fragment;
    uint32_t first_row = values.front().first;
    uint32_t last_row = first_row;
    uint32_t count = 0;
    for (const auto& value : values) {
      if (value.second.size() > kHardMaxPageBytes) {
        return Status::InvalidArgument("granule", "single value exceeds maximum page size");
      }
      if (!fragment.empty() && fragment.size() + value.second.size() > kHardMaxPageBytes) {
        pages.push_back(PagePayload{type, physical_type, std::move(fragment), count, first_row,
                                    last_row - first_row + 1});
        fragment.clear();
        first_row = value.first;
        count = 0;
      }
      fragment.append(value.second);
      last_row = value.first;
      ++count;
    }
    pages.push_back(PagePayload{type, physical_type, std::move(fragment), count, first_row,
                                last_row - first_row + 1});
    return Status::OK();
  };
  Status fragments = append_variable_fragments(
      PageType::kTypedValue, partition.physical_type, inline_values);
  if (!fragments.ok()) return fragments;
  fragments = append_variable_fragments(
      PageType::kBlobRef, PhysicalType::kBinary, blob_references);
  if (!fragments.ok()) return fragments;
  std::string bytes(kHeaderSize, '\0');
  std::vector<PageDirectoryEntry> directory;
  std::map<PageType, uint32_t> ordinals;
  PageCompressionStats compression;
  for (const PagePayload& page : pages) {
    EncodingId encoding = EncodingId::kPlain;
    if (page.type == PageType::kTypedValue && page.value_count != 0) {
      encoding = page.physical_type == PhysicalType::kFloat32 ||
                         page.physical_type == PhysicalType::kFloat64
                     ? EncodingId::kXor
                     : EncodingId::kDictionary;
    } else if (page.type == PageType::kInlinePresence ||
               page.type == PageType::kBlobPresence) {
      encoding = EncodingId::kBitmap;
    } else if (page.type == PageType::kOperation || page.type == PageType::kValueClass) {
      encoding = EncodingId::kRle;
    } else if (page.type == PageType::kEntityId || page.type == PageType::kTargetId ||
               page.type == PageType::kEdgeId || page.type == PageType::kValidFrom ||
               page.type == PageType::kCommitSeq) {
      encoding = EncodingId::kDelta;
    }
    const PageHeader candidate_header{
        page.type, page.physical_type, encoding, partition.compression_id,
        page.first_row, page.row_count, page.value_count, 0};
    if (page.type == PageType::kOperation && page.value_count != 0) {
      const auto rle = EncodePageChecked(candidate_header, page.bytes);
      PageHeader bitmap_header = candidate_header;
      bitmap_header.encoding_id = EncodingId::kBitmap;
      const auto bitmap = EncodePageChecked(bitmap_header, page.bytes);
      if (bitmap.ok() &&
          (!rle.ok() || bitmap.ValueOrDie().size() < rle.ValueOrDie().size())) {
        encoding = EncodingId::kBitmap;
      }
    }
    const bool integer_page =
        page.type == PageType::kEntityId || page.type == PageType::kTargetId ||
        page.type == PageType::kEdgeId || page.type == PageType::kValidFrom ||
        page.type == PageType::kCommitSeq;
    if (integer_page && page.bytes.size() >= 2 * sizeof(uint64_t)) {
      const auto delta = EncodePageChecked(candidate_header, page.bytes);
      uint64_t best_size = delta.ok() ? delta.ValueOrDie().size() : UINT64_MAX;
      PageHeader frame_header = candidate_header;
      frame_header.encoding_id = EncodingId::kFrameOfReference;
      const auto frame = EncodePageChecked(frame_header, page.bytes);
      if (frame.ok() && frame.ValueOrDie().size() < best_size) {
        encoding = EncodingId::kFrameOfReference;
        best_size = frame.ValueOrDie().size();
      }
      PageHeader bit_header = candidate_header;
      bit_header.encoding_id = EncodingId::kBitPacking;
      const auto bit = EncodePageChecked(bit_header, page.bytes);
      // Bit-packing has a small fixed header and can win by only a few bytes
      // on low-cardinality clustered values. Prefer FOR in that near-tie
      // case: its minimum-base representation is more stable across page
      // boundaries, while bit-packing is reserved for a material reduction.
      if (bit.ok() && bit.ValueOrDie().size() * 8ULL <= best_size * 7ULL) {
        encoding = EncodingId::kBitPacking;
        best_size = bit.ValueOrDie().size();
      }
      PageHeader delta_of_delta_header = candidate_header;
      delta_of_delta_header.encoding_id = EncodingId::kDeltaOfDelta;
      const auto delta_of_delta = EncodePageChecked(
          delta_of_delta_header, page.bytes);
      if (delta_of_delta.ok() && delta_of_delta.ValueOrDie().size() < best_size) {
        encoding = EncodingId::kDeltaOfDelta;
      }
    }
    const auto encoded_page = EncodePageChecked(
        PageHeader{page.type, page.physical_type, encoding,
                   partition.compression_id, page.first_row, page.row_count,
                   page.value_count, 0}, page.bytes);
    if (!encoded_page.ok()) return encoded_page.status();
    const std::string& encoded = encoded_page.ValueOrDie();
    const auto encoded_header = DecodePageHeader(
        encoded.substr(0, kPageFormatHeaderSize));
    if (!encoded_header.ok()) return encoded_header.status();
    const size_t metric_slot = static_cast<size_t>(page.type);
    if (metric_slot >= kPageTypeMetricSlots) {
      return Status::Corruption("granule", "page type has no metric slot");
    }
    const auto add = [](uint64_t value, uint64_t* total) {
      *total = value > UINT64_MAX - *total ? UINT64_MAX : *total + value;
    };
    add(encoded_header.ValueOrDie().uncompressed_size,
        &compression.uncompressed_bytes[metric_slot]);
    add(encoded_header.ValueOrDie().compressed_size,
        &compression.stored_bytes[metric_slot]);
    directory.push_back(PageDirectoryEntry{
        page.type, ordinals[page.type]++,
        static_cast<uint64_t>(bytes.size()),
        static_cast<uint64_t>(encoded.size()), Blake3Hash(encoded).bytes});
    bytes.append(encoded);
  }
  const uint64_t directory_offset = bytes.size();
  const std::string encoded_directory = EncodePageDirectory(directory);
  bytes.append(encoded_directory);
  std::string header;
  PutU32(&header, kMagic);
  PutU16(&header, kVersion);
  PutU16(&header, kHeaderSize);
  PutU32(&header, static_cast<uint32_t>(events.size()));
  PutU64(&header, directory_offset);
  PutU64(&header, encoded_directory.size());
  bytes.replace(0, kHeaderSize, header);
  const BlobHash identity =
      ComputeGranuleBlockIdentity(header, encoded_directory);
  return GranuleBlock{std::move(bytes), static_cast<uint32_t>(events.size()),
                      compression, identity};
}

StatusOr<std::vector<TemporalEvent>> DecodeGranuleBlock(
    const std::string& bytes, const BlockPartition& partition) {
  if (bytes.size() < kHeaderSize) return Status::Corruption("granule", "truncated header");
  size_t offset = 0;
  uint32_t magic;
  uint32_t rows;
  uint16_t version;
  uint16_t header_size;
  uint64_t directory_offset;
  uint64_t directory_length;
  if (!GetU32(bytes, &offset, &magic) || !GetU16(bytes, &offset, &version) ||
      !GetU16(bytes, &offset, &header_size) || !GetU32(bytes, &offset, &rows) ||
      !GetU64(bytes, &offset, &directory_offset) || !GetU64(bytes, &offset, &directory_length) ||
      magic != kMagic || version != kVersion || header_size != kHeaderSize ||
      directory_offset > bytes.size() || directory_length > bytes.size() - directory_offset) {
    return Status::Corruption("granule", "invalid header");
  }
  const auto directory = DecodePageDirectory(bytes.substr(directory_offset, directory_length));
  if (!directory.ok()) return directory.status();
  std::map<PageType, std::vector<std::pair<uint32_t, Page>>> page_fragments;
  for (const PageDirectoryEntry& entry : directory.ValueOrDie()) {
    if (entry.offset < kHeaderSize || entry.offset > directory_offset ||
        entry.length > directory_offset - entry.offset) {
      return Status::Corruption("granule", "invalid page location");
    }
    const std::string encoded_page = bytes.substr(entry.offset, entry.length);
    if (Blake3Hash(encoded_page).bytes != entry.content_hash) {
      return Status::Corruption("granule", "page content hash mismatch");
    }
    const auto page = DecodePage(encoded_page);
    if (!page.ok()) return page.status();
    if (page.ValueOrDie().header.page_type != entry.page_type ||
        page.ValueOrDie().header.physical_type !=
            ExpectedPagePhysicalType(entry.page_type, partition) ||
        page.ValueOrDie().header.first_row + page.ValueOrDie().header.row_count > rows) {
      return Status::Corruption("granule", "page metadata mismatch");
    }
    page_fragments[entry.page_type].emplace_back(entry.ordinal, std::move(page.ValueOrDie()));
  }
  std::map<PageType, std::string> payloads;
  for (PageType type : {PageType::kEntityId, PageType::kTargetId, PageType::kEdgeId,
                        PageType::kValidFrom, PageType::kCommitSeq, PageType::kOperation,
                        PageType::kValueClass, PageType::kInlinePresence,
                        PageType::kBlobPresence, PageType::kTypedValue,
                        PageType::kBlobRef}) {
    const auto found = page_fragments.find(type);
    if (found == page_fragments.end() || found->second.empty()) {
      return Status::Corruption("granule", "required page missing");
    }
    std::vector<std::pair<uint32_t, Page>> fragments = found->second;
    std::sort(fragments.begin(), fragments.end(), [](const auto& left, const auto& right) {
      return left.first < right.first;
    });
    for (uint32_t ordinal = 0; ordinal < fragments.size(); ++ordinal) {
      if (fragments[ordinal].first != ordinal) {
        return Status::Corruption("granule", "non-contiguous page ordinals");
      }
      if (type != PageType::kTypedValue && type != PageType::kBlobRef &&
          (fragments.size() != 1 || fragments[ordinal].second.header.first_row != 0 ||
           fragments[ordinal].second.header.row_count != rows)) {
        return Status::Corruption("granule", "fragmented system page");
      }
      payloads[type].append(fragments[ordinal].second.payload);
    }
  }
  std::vector<uint64_t> entity_ids;
  std::vector<uint64_t> target_ids;
  std::vector<uint64_t> edge_ids;
  std::vector<uint64_t> valid_from;
  std::vector<uint64_t> commit_seq;
  if (!ReadU64s(payloads[PageType::kEntityId], rows, &entity_ids) ||
      !ReadU64s(payloads[PageType::kTargetId], rows, &target_ids) ||
      !ReadU64s(payloads[PageType::kEdgeId], rows, &edge_ids) ||
      !ReadU64s(payloads[PageType::kValidFrom], rows, &valid_from) ||
      !ReadU64s(payloads[PageType::kCommitSeq], rows, &commit_seq) ||
      payloads[PageType::kOperation].size() != rows ||
      payloads[PageType::kValueClass].size() != rows ||
      payloads[PageType::kInlinePresence].size() != rows ||
      payloads[PageType::kBlobPresence].size() != rows) {
    return Status::Corruption("granule", "invalid system page");
  }
  size_t inline_offset = 0;
  size_t blob_offset = 0;
  size_t inline_rank = 0;
  size_t blob_rank = 0;
  std::vector<TemporalEvent> events;
  events.reserve(rows);
  for (uint32_t row = 0; row < rows; ++row) {
    const uint8_t operation = static_cast<uint8_t>(payloads[PageType::kOperation][row]);
    const uint8_t value_class = static_cast<uint8_t>(payloads[PageType::kValueClass][row]);
    if (operation > 1 || (operation == 1 && value_class != 0) ||
        (operation == 0 && value_class != 1 && value_class != 2)) {
      return Status::Corruption("granule", "invalid value class");
    }
    const char inline_bit = payloads[PageType::kInlinePresence][row];
    const char blob_bit = payloads[PageType::kBlobPresence][row];
    if ((inline_bit != 0 && inline_bit != 1) ||
        (blob_bit != 0 && blob_bit != 1) ||
        inline_bit != static_cast<char>(value_class == 1) ||
        blob_bit != static_cast<char>(value_class == 2) ||
        (inline_bit != 0 && blob_bit != 0)) {
      return Status::Corruption("granule", "presence bitmap disagrees with value class");
    }
    const LogicalKey key = MakeKey(partition, entity_ids[row], target_ids[row], edge_ids[row]);
    if (operation == 1) {
      events.push_back(TemporalEvent::Delete(key, valid_from[row], commit_seq[row],
                                              partition.schema_epoch));
    } else if (value_class == 1) {
      if (RankPresence(payloads[PageType::kInlinePresence], row) != inline_rank) {
        return Status::Corruption("granule", "inline presence rank mismatch");
      }
      ++inline_rank;
      std::optional<Value> value;
      if (!DecodeGranuleInlineValue(payloads[PageType::kTypedValue],
                                    &inline_offset, partition.physical_type,
                                    &value)) {
        return Status::Corruption("granule", "invalid inline value");
      }
      events.push_back(TemporalEvent::Put(key, valid_from[row], commit_seq[row],
                                           partition.schema_epoch, *value));
    } else {
      if (RankPresence(payloads[PageType::kBlobPresence], row) != blob_rank) {
        return Status::Corruption("granule", "blob presence rank mismatch");
      }
      ++blob_rank;
      BlobRef reference;
      if (!GetBlobReference(payloads[PageType::kBlobRef], &blob_offset, &reference)) {
        return Status::Corruption("granule", "invalid blob reference");
      }
      events.push_back(TemporalEvent::PutBlob(key, valid_from[row], commit_seq[row],
                                               partition.schema_epoch, std::move(reference)));
    }
  }
  if (inline_offset != payloads[PageType::kTypedValue].size() ||
      blob_offset != payloads[PageType::kBlobRef].size()) {
    return Status::Corruption("granule", "trailing value data");
  }
  return events;
}

}  // namespace cedar
