// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/storage/version_set.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#include <map>
#include <tuple>

#include "cedar/core/crc32c.h"
#include "cedar/index/canonical_value.h"
#include "cedar/storage/storage_layout.h"

namespace cedar {
namespace {

constexpr uint32_t kManifestMagic = 0x3143534dU;  // MSC1
constexpr uint32_t kMaxManifestEntries = 1000000;
constexpr uint64_t kManifestFrameBytes = 8;
constexpr uint64_t kMaxManifestBodyBytes =
    VersionSet::kMaxManifestBytes - kManifestFrameBytes;
constexpr uint64_t kMinSchemaRecordBytes = 23;
constexpr uint64_t kMinFileRecordBytes = 240;
constexpr uint64_t kMinBlobSegmentRecordBytes = 17;
constexpr uint64_t kIndexDefinitionRecordBytes = 64;
constexpr uint64_t kMinIndexFragmentRecordBytes = 89;

Status EnsureManifestBodyCapacity(const std::string& output,
                                  uint64_t additional_bytes,
                                  const char* field) {
  if (output.size() > kMaxManifestBodyBytes ||
      additional_bytes > kMaxManifestBodyBytes - output.size()) {
    return Status::ResourceExhausted(
        "manifest", std::string(field) + " exceeds Manifest byte limit");
  }
  return Status::OK();
}

bool DeclaredCountFits(const std::string& input, size_t offset,
                       uint32_t count, uint64_t minimum_record_bytes) {
  return count <= kMaxManifestEntries && offset <= input.size() &&
      static_cast<uint64_t>(count) <=
          static_cast<uint64_t>(input.size() - offset) /
              minimum_record_bytes;
}

void PutU8(std::string* output, uint8_t value) {
  output->push_back(static_cast<char>(value));
}
void PutU16(std::string* output, uint16_t value) {
  PutU8(output, static_cast<uint8_t>(value));
  PutU8(output, static_cast<uint8_t>(value >> 8));
}
void PutU32(std::string* output, uint32_t value) {
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    PutU8(output, static_cast<uint8_t>(value >> shift));
  }
}
void PutU64(std::string* output, uint64_t value) {
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    PutU8(output, static_cast<uint8_t>(value >> shift));
  }
}
bool GetU8(const std::string& input, size_t* offset, uint8_t* value) {
  if (*offset >= input.size()) return false;
  *value = static_cast<uint8_t>(input[(*offset)++]);
  return true;
}
bool GetU16(const std::string& input, size_t* offset, uint16_t* value) {
  uint8_t low = 0;
  uint8_t high = 0;
  if (!GetU8(input, offset, &low) || !GetU8(input, offset, &high)) return false;
  *value = static_cast<uint16_t>(low) | (static_cast<uint16_t>(high) << 8);
  return true;
}
bool GetU32(const std::string& input, size_t* offset, uint32_t* value) {
  *value = 0;
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    uint8_t byte = 0;
    if (!GetU8(input, offset, &byte)) return false;
    *value |= static_cast<uint32_t>(byte) << shift;
  }
  return true;
}
bool GetU64(const std::string& input, size_t* offset, uint64_t* value) {
  *value = 0;
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    uint8_t byte = 0;
    if (!GetU8(input, offset, &byte)) return false;
    *value |= static_cast<uint64_t>(byte) << shift;
  }
  return true;
}

bool ValidEntity(uint8_t value) {
  return value <= static_cast<uint8_t>(EntityType::EdgeIn);
}
bool ValidPhysical(uint8_t value) {
  return value >= static_cast<uint8_t>(PhysicalType::kBool) &&
         value <= static_cast<uint8_t>(PhysicalType::kBinary);
}
bool ValidKeyKind(uint8_t value) {
  return value <= static_cast<uint8_t>(LogicalKeyKind::kProperty);
}
bool ValidCompression(uint8_t value) {
  return value >= static_cast<uint8_t>(CompressionId::kNone) &&
         value <= static_cast<uint8_t>(CompressionId::kZstd);
}

bool SchemaIdentityLess(const ColumnSchema& left, const ColumnSchema& right) {
  return std::tie(left.entity_type, left.column_id, left.schema_epoch) <
         std::tie(right.entity_type, right.column_id, right.schema_epoch);
}

const ColumnSchema* FindSchema(const std::vector<ColumnSchema>& schemas,
                               EntityType entity_type, uint16_t column_id,
                               uint32_t schema_epoch) {
  const auto found = std::lower_bound(
      schemas.begin(), schemas.end(),
      ColumnSchema{entity_type, column_id, schema_epoch, "",
                   PhysicalType::kBool, 0, EncodingPolicy::kAdaptive,
                   CompressionPolicy::kNone},
      SchemaIdentityLess);
  if (found == schemas.end() || found->entity_type != entity_type ||
      found->column_id != column_id || found->schema_epoch != schema_epoch) {
    return nullptr;
  }
  return &*found;
}

Status ValidateSchemaCatalog(const std::vector<ColumnSchema>& schemas) {
  if (schemas.size() > kMaxManifestEntries) {
    return Status::InvalidArgument("manifest", "schema entry limit exceeded");
  }
  std::map<std::pair<uint8_t, uint16_t>, uint32_t> next_epochs;
  const ColumnSchema* previous = nullptr;
  for (const ColumnSchema& schema : schemas) {
    const Status valid = ValidateColumnSchema(schema, true);
    if (!valid.ok()) return valid;
    if (previous != nullptr && !SchemaIdentityLess(*previous, schema)) {
      return Status::InvalidArgument(
          "manifest", "schema identities are duplicate or unsorted");
    }
    uint32_t& next = next_epochs[
        {static_cast<uint8_t>(schema.entity_type), schema.column_id}];
    ++next;
    if (schema.schema_epoch != next) {
      return Status::InvalidArgument("manifest",
                                     "schema epochs are not contiguous");
    }
    previous = &schema;
  }
  return Status::OK();
}

Status ValidateFinalSnapshot(const VersionSnapshot& snapshot) {
  if (snapshot.next_index_id == 0) {
    return Status::InvalidArgument("manifest", "invalid next index id");
  }
  const Status schemas = ValidateSchemaCatalog(snapshot.schemas);
  if (!schemas.ok()) return schemas;
  for (const SstFileMeta& file : snapshot.files) {
    const ColumnSchema* schema = FindSchema(
        snapshot.schemas, file.partition.entity_type, file.partition.column_id,
        file.partition.schema_epoch);
    if (schema == nullptr || schema->physical_type != file.partition.physical_type) {
      return Status::SchemaMismatch(
          "manifest", "live SST references an unknown or mismatched schema");
    }
  }
  for (const IndexDefinition& definition : snapshot.index_definitions) {
    const uint8_t entity = static_cast<uint8_t>(definition.entity_type);
    const uint8_t state = static_cast<uint8_t>(definition.state);
    if (!ValidEntity(entity) ||
        state > static_cast<uint8_t>(IndexState::kFailed) ||
        definition.index_id == 0 || definition.schema_epoch == 0 ||
        definition.index_id >= snapshot.next_index_id ||
        definition.capabilities == 0 ||
        !IsSupportedIndexCanonicalEncoding(
            definition.canonical_encoding_id)) {
      return Status::InvalidArgument(
          "manifest", "index definition has invalid structure");
    }
    if (FindSchema(snapshot.schemas, definition.entity_type,
                   definition.column_id, definition.schema_epoch) == nullptr) {
      return Status::SchemaMismatch(
          "manifest", "index definition references an unknown schema identity");
    }
  }
  for (const IndexFragment& fragment : snapshot.index_fragments) {
    const auto definition = std::find_if(
        snapshot.index_definitions.begin(), snapshot.index_definitions.end(),
        [&fragment](const IndexDefinition& candidate) {
          return candidate.index_id == fragment.index_id;
        });
    const auto source = std::find_if(
        snapshot.files.begin(), snapshot.files.end(),
        [&fragment](const SstFileMeta& candidate) {
          return candidate.file_number == fragment.source_sst_id;
        });
    if (definition == snapshot.index_definitions.end() ||
        source == snapshot.files.end() ||
        source->partition.key_kind != LogicalKeyKind::kProperty ||
        definition->entity_type != source->partition.entity_type ||
        definition->column_id != source->partition.column_id ||
        definition->schema_epoch != source->partition.schema_epoch) {
      return Status::InvalidArgument(
          "manifest", "index fragment is bound to an incompatible source SST");
    }
  }
  return Status::OK();
}

bool ValidFormat(const SstFormatDescriptor& format) {
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

void PutLogicalKey(std::string* output, const LogicalKey& key) {
  PutU8(output, static_cast<uint8_t>(key.entity_type()));
  PutU8(output, static_cast<uint8_t>(key.kind()));
  PutU64(output, key.entity_id());
  PutU64(output, key.target_id());
  PutU16(output, key.column_id());
  PutU16(output, key.edge_type());
  PutU64(output, key.edge_id());
}

bool GetLogicalKey(const std::string& input, size_t* offset,
                   LogicalKey* key) {
  uint8_t entity = 0;
  uint8_t kind = 0;
  uint64_t entity_id = 0;
  uint64_t target_id = 0;
  uint16_t column = 0;
  uint16_t edge_type = 0;
  uint64_t edge_id = 0;
  if (!GetU8(input, offset, &entity) || !GetU8(input, offset, &kind) ||
      !GetU64(input, offset, &entity_id) ||
      !GetU64(input, offset, &target_id) ||
      !GetU16(input, offset, &column) ||
      !GetU16(input, offset, &edge_type) ||
      !GetU64(input, offset, &edge_id) || !ValidEntity(entity) ||
      !ValidKeyKind(kind)) {
    return false;
  }
  const EntityType entity_type = static_cast<EntityType>(entity);
  if (kind == static_cast<uint8_t>(LogicalKeyKind::kExistence)) {
    *key = entity_type == EntityType::Vertex
        ? LogicalKey::VertexExistence(entity_id)
        : LogicalKey::EdgeExistence(entity_id, target_id, edge_type, edge_id,
                                    entity_type);
  } else {
    *key = entity_type == EntityType::Vertex
        ? LogicalKey::VertexProperty(entity_id, column)
        : LogicalKey::EdgeProperty(entity_id, target_id, edge_type, edge_id,
                                   column, entity_type);
  }
  return true;
}

void PutOptionalValue(std::string* output,
                      const std::optional<Value>& value) {
  if (!value.has_value()) {
    PutU32(output, 0);
    return;
  }
  const std::string encoded = value->Encode();
  PutU32(output, static_cast<uint32_t>(encoded.size()));
  output->append(encoded);
}

bool GetOptionalValue(const std::string& input, size_t* offset,
                      std::optional<Value>* value) {
  uint32_t length = 0;
  if (!GetU32(input, offset, &length) || length > 1024 ||
      length > input.size() - *offset) {
    return false;
  }
  if (length == 0) {
    value->reset();
    return true;
  }
  *value = Value::Decode(input.substr(*offset, length));
  *offset += length;
  return value->has_value();
}

bool ValidStatistics(const SstFileStatistics& statistics,
                     const BlockPartition& partition) {
  if (statistics.row_count == 0 ||
      statistics.last_key < statistics.first_key ||
      statistics.max_valid_from < statistics.min_valid_from ||
      statistics.max_commit_seq < statistics.min_commit_seq ||
      statistics.put_count > statistics.row_count ||
      statistics.delete_count !=
          statistics.row_count - statistics.put_count ||
      statistics.inline_value_count > statistics.put_count ||
      statistics.blob_reference_count !=
          statistics.put_count - statistics.inline_value_count ||
      statistics.typed_value_count != statistics.inline_value_count ||
      statistics.nan_count > statistics.typed_value_count ||
      statistics.typed_min.has_value() != statistics.typed_max.has_value() ||
      (!statistics.typed_min_max_complete &&
       statistics.typed_min.has_value())) {
    return false;
  }
  if (statistics.typed_min.has_value() &&
      (statistics.typed_min->type() != partition.physical_type ||
       statistics.typed_max->type() != partition.physical_type)) {
    return false;
  }
  if (statistics.typed_min.has_value()) {
    const std::string encoded_min = statistics.typed_min->Encode();
    const std::string encoded_max = statistics.typed_max->Encode();
    if (encoded_min.size() > 1024 || encoded_max.size() > 1024) return false;
    const auto canonical_min =
        EncodeIndexCanonicalValue(*statistics.typed_min);
    const auto canonical_max =
        EncodeIndexCanonicalValue(*statistics.typed_max);
    if (!canonical_min.ok() || !canonical_max.ok() ||
        CompareIndexCanonicalValues(canonical_min.ValueOrDie(),
                                    canonical_max.ValueOrDie()) > 0) {
      return false;
    }
  }
  const uint64_t comparable =
      statistics.typed_value_count - statistics.nan_count;
  return !statistics.typed_min_max_complete ||
      ((comparable == 0) == !statistics.typed_min.has_value());
}

bool ValidBlobSegment(const BlobSegmentMeta& segment) {
  return segment.segment_id != 0 && !segment.relative_path.empty() &&
      !std::filesystem::path(segment.relative_path).is_absolute() &&
      std::filesystem::path(segment.relative_path).lexically_normal() ==
          std::filesystem::path(segment.relative_path) &&
      segment.relative_path.find("..") == std::string::npos;
}

bool ValidRelativePath(const std::string& relative_path) {
  if (relative_path.empty()) return false;
  const std::filesystem::path path(relative_path);
  if (path.is_absolute() || path.lexically_normal() != path) return false;
  return std::none_of(path.begin(), path.end(), [](const auto& component) {
    return component == "." || component == "..";
  });
}

bool IsDecimalPathComponent(const std::filesystem::path& component) {
  const std::string value = component.string();
  return !value.empty() && std::all_of(value.begin(), value.end(), [](unsigned char character) {
    return character >= '0' && character <= '9';
  });
}

bool ValidSstRelativePath(const std::string& relative_path) {
  if (!ValidRelativePath(relative_path)) return false;
  const std::filesystem::path path(relative_path);
  auto component = path.begin();
  if (component == path.end() || *component++ != "shards" || component == path.end() ||
      !IsDecimalPathComponent(*component++) || component == path.end() ||
      *component++ != "sst" || component == path.end() ||
      component->extension() != storage_layout::kSstExtension) {
    return false;
  }
  ++component;
  return component == path.end();
}

std::optional<uint32_t> SstShardIdFromPath(
    const std::string& relative_path) {
  if (!ValidSstRelativePath(relative_path)) return std::nullopt;
  const std::filesystem::path path(relative_path);
  const auto component = std::next(path.begin());
  try {
    const unsigned long value = std::stoul(component->string());
    if (value > std::numeric_limits<uint32_t>::max()) return std::nullopt;
    return static_cast<uint32_t>(value);
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

bool ValidIndexFragmentPath(const IndexFragment& fragment) {
  if (!ValidRelativePath(fragment.relative_path) || fragment.index_id == 0) return false;
  const std::filesystem::path path(fragment.relative_path);
  auto component = path.begin();
  if (component == path.end() || *component++ != "indexes" || component == path.end() ||
      component++->string() != std::to_string(fragment.index_id) || component == path.end() ||
      component->extension() != storage_layout::kIndexExtension) {
    return false;
  }
  ++component;
  return component == path.end();
}

Status AppendLengthPrefixed(std::string* output, const std::string& value,
                            const char* field) {
  if (value.size() > UINT32_MAX) {
    return Status::InvalidArgument("manifest", std::string(field) + " exceeds UInt32");
  }
  const Status capacity = EnsureManifestBodyCapacity(
      *output, sizeof(uint32_t) + static_cast<uint64_t>(value.size()), field);
  if (!capacity.ok()) return capacity;
  PutU32(output, static_cast<uint32_t>(value.size()));
  output->append(value);
  return Status::OK();
}

StatusOr<std::string> ReadLengthPrefixed(const std::string& input, size_t* offset,
                                          const char* field) {
  uint32_t length = 0;
  if (!GetU32(input, offset, &length) || length > input.size() - *offset) {
    return Status::Corruption("manifest", std::string("invalid ") + field);
  }
  std::string value = input.substr(*offset, length);
  *offset += length;
  return value;
}

StatusOr<std::string> EncodeSnapshot(const VersionSnapshot& snapshot) {
  const Status valid_snapshot = ValidateFinalSnapshot(snapshot);
  if (!valid_snapshot.ok()) return valid_snapshot;
  std::string output;
  const Status header_capacity =
      EnsureManifestBodyCapacity(output, 24, "snapshot header");
  if (!header_capacity.ok()) return header_capacity;
  PutU32(&output, kManifestMagic);
  PutU64(&output, snapshot.generation);
  PutU64(&output, snapshot.next_index_id);
  if (snapshot.files.size() > kMaxManifestEntries ||
      snapshot.index_definitions.size() > kMaxManifestEntries ||
      snapshot.index_fragments.size() > kMaxManifestEntries) {
    return Status::InvalidArgument("manifest", "manifest entry limit exceeded");
  }
  PutU32(&output, static_cast<uint32_t>(snapshot.schemas.size()));
  for (const ColumnSchema& schema : snapshot.schemas) {
    const Status capacity = EnsureManifestBodyCapacity(
        output, 22 + static_cast<uint64_t>(schema.logical_type.size()),
        "schema record");
    if (!capacity.ok()) return capacity;
    PutU8(&output, static_cast<uint8_t>(schema.entity_type));
    PutU16(&output, schema.column_id);
    PutU32(&output, schema.schema_epoch);
    const Status logical_type = AppendLengthPrefixed(
        &output, schema.logical_type, "schema logical type");
    if (!logical_type.ok()) return logical_type;
    PutU8(&output, static_cast<uint8_t>(schema.physical_type));
    PutU64(&output, schema.blob_threshold);
    PutU8(&output, static_cast<uint8_t>(schema.encoding_policy));
    PutU8(&output, static_cast<uint8_t>(schema.compression_policy));
  }
  const Status file_count_capacity =
      EnsureManifestBodyCapacity(output, 4, "file count");
  if (!file_count_capacity.ok()) return file_count_capacity;
  PutU32(&output, static_cast<uint32_t>(snapshot.files.size()));
  for (const SstFileMeta& file : snapshot.files) {
    const auto path_shard = SstShardIdFromPath(file.relative_path);
    if (file.file_number == 0 || !path_shard.has_value() ||
        *path_shard != file.partition.storage_shard_id ||
        !ValidEntity(static_cast<uint8_t>(file.partition.entity_type)) ||
        !ValidKeyKind(static_cast<uint8_t>(file.partition.key_kind)) ||
        !ValidPhysical(static_cast<uint8_t>(file.partition.physical_type)) ||
        !ValidCompression(static_cast<uint8_t>(file.partition.compression_id)) ||
        file.partition.logical_type_id == 0 || !ValidFormat(file.format) ||
        IsZeroIdentity(file.identity) ||
        !ValidStatistics(file.statistics, file.partition) ||
        file.statistics_crc32c == 0 ||
        file.blob_refs.size() > kMaxManifestEntries) {
      return Status::InvalidArgument("manifest", "invalid SST metadata");
    }
    uint64_t record_bytes = kMinFileRecordBytes;
    const auto add_record_bytes = [&record_bytes](uint64_t bytes) {
      if (bytes > std::numeric_limits<uint64_t>::max() - record_bytes) {
        return false;
      }
      record_bytes += bytes;
      return true;
    };
    const uint64_t blob_bytes =
        static_cast<uint64_t>(file.blob_refs.size()) *
        BlobHash{}.bytes.size();
    const uint64_t typed_min_bytes = file.statistics.typed_min.has_value()
        ? file.statistics.typed_min->Encode().size()
        : 0;
    const uint64_t typed_max_bytes = file.statistics.typed_max.has_value()
        ? file.statistics.typed_max->Encode().size()
        : 0;
    if (!add_record_bytes(file.relative_path.size()) ||
        !add_record_bytes(blob_bytes) ||
        !add_record_bytes(typed_min_bytes) ||
        !add_record_bytes(typed_max_bytes)) {
      return Status::ResourceExhausted("manifest", "SST record size overflow");
    }
    const Status capacity =
        EnsureManifestBodyCapacity(output, record_bytes, "SST record");
    if (!capacity.ok()) return capacity;
    PutU64(&output, file.file_number);
    const Status path = AppendLengthPrefixed(&output, file.relative_path, "file path");
    if (!path.ok()) return path;
    PutU8(&output, static_cast<uint8_t>(file.partition.entity_type));
    PutU8(&output, static_cast<uint8_t>(file.partition.key_kind));
    PutU16(&output, file.partition.column_id);
    PutU32(&output, file.partition.schema_epoch);
    PutU8(&output, static_cast<uint8_t>(file.partition.physical_type));
    PutU16(&output, file.partition.edge_type);
    PutU8(&output, static_cast<uint8_t>(file.partition.compression_id));
    PutU32(&output, file.partition.storage_shard_id);
    PutU16(&output, file.partition.logical_type_id);
    PutU64(&output, file.file_size);
    PutU32(&output, static_cast<uint32_t>(file.blob_refs.size()));
    for (const BlobHash& hash : file.blob_refs) {
      output.append(reinterpret_cast<const char*>(hash.bytes.data()), hash.bytes.size());
    }
    PutU8(&output, static_cast<uint8_t>(file.format.sort_order_id));
    PutU8(&output, static_cast<uint8_t>(file.format.hash_algorithm_id));
    PutU8(&output, static_cast<uint8_t>(file.format.encoding_registry_id));
    PutU8(&output, static_cast<uint8_t>(file.format.compression_registry_id));
    PutU8(&output, static_cast<uint8_t>(file.format.checksum_algorithm_id));
    output.append(reinterpret_cast<const char*>(file.identity.bytes.data()),
                  file.identity.bytes.size());
    PutLogicalKey(&output, file.statistics.first_key);
    PutLogicalKey(&output, file.statistics.last_key);
    PutU64(&output, file.statistics.min_valid_from);
    PutU64(&output, file.statistics.max_valid_from);
    PutU64(&output, file.statistics.min_commit_seq);
    PutU64(&output, file.statistics.max_commit_seq);
    PutU64(&output, file.statistics.row_count);
    PutU64(&output, file.statistics.put_count);
    PutU64(&output, file.statistics.delete_count);
    PutU64(&output, file.statistics.inline_value_count);
    PutU64(&output, file.statistics.blob_reference_count);
    PutU64(&output, file.statistics.typed_value_count);
    PutU64(&output, file.statistics.nan_count);
    PutU8(&output, file.statistics.typed_min_max_complete ? 1 : 0);
    PutOptionalValue(&output, file.statistics.typed_min);
    PutOptionalValue(&output, file.statistics.typed_max);
    PutU32(&output, file.statistics_crc32c);
  }
  if (snapshot.blob_segments.size() > kMaxManifestEntries) {
    return Status::InvalidArgument("manifest", "blob segment limit exceeded");
  }
  const Status blob_count_capacity =
      EnsureManifestBodyCapacity(output, 4, "blob segment count");
  if (!blob_count_capacity.ok()) return blob_count_capacity;
  PutU32(&output, static_cast<uint32_t>(snapshot.blob_segments.size()));
  for (const BlobSegmentMeta& segment : snapshot.blob_segments) {
    if (!ValidBlobSegment(segment)) {
      return Status::InvalidArgument("manifest", "invalid blob segment");
    }
    const Status capacity = EnsureManifestBodyCapacity(
        output,
        kMinBlobSegmentRecordBytes +
            static_cast<uint64_t>(segment.relative_path.size()),
        "blob segment record");
    if (!capacity.ok()) return capacity;
    PutU32(&output, segment.shard_id);
    PutU64(&output, segment.segment_id);
    const Status path = AppendLengthPrefixed(&output, segment.relative_path, "blob segment path");
    if (!path.ok()) return path;
    PutU8(&output, segment.active ? 1 : 0);
  }
  const Status definition_count_capacity =
      EnsureManifestBodyCapacity(output, 4, "index definition count");
  if (!definition_count_capacity.ok()) return definition_count_capacity;
  PutU32(&output, static_cast<uint32_t>(snapshot.index_definitions.size()));
  for (const IndexDefinition& index : snapshot.index_definitions) {
    const Status capacity = EnsureManifestBodyCapacity(
        output, kIndexDefinitionRecordBytes, "index definition record");
    if (!capacity.ok()) return capacity;
    PutU64(&output, index.index_id);
    PutU8(&output, static_cast<uint8_t>(index.entity_type));
    PutU16(&output, index.column_id);
    PutU32(&output, index.schema_epoch);
    PutU32(&output, index.capabilities);
    PutU32(&output, index.canonical_encoding_id);
    PutU8(&output, static_cast<uint8_t>(index.state));
    PutU64(&output, index.generation);
    output.append(reinterpret_cast<const char*>(index.definition_checksum.data()),
                  index.definition_checksum.size());
  }
  const Status fragment_count_capacity =
      EnsureManifestBodyCapacity(output, 4, "index fragment count");
  if (!fragment_count_capacity.ok()) return fragment_count_capacity;
  PutU32(&output, static_cast<uint32_t>(snapshot.index_fragments.size()));
  for (const IndexFragment& fragment : snapshot.index_fragments) {
    if (!ValidIndexFragmentPath(fragment)) {
      return Status::InvalidArgument("manifest", "invalid index fragment path");
    }
    const Status capacity = EnsureManifestBodyCapacity(
        output,
        kMinIndexFragmentRecordBytes +
            static_cast<uint64_t>(fragment.relative_path.size()),
        "index fragment record");
    if (!capacity.ok()) return capacity;
    PutU64(&output, fragment.index_id);
    PutU64(&output, fragment.source_sst_id);
    const Status path = AppendLengthPrefixed(&output, fragment.relative_path, "index fragment path");
    if (!path.ok()) return path;
    PutU64(&output, fragment.source_row_count);
    PutU64(&output, fragment.indexed_put_count);
    PutU64(&output, fragment.catalog_generation);
    PutU32(&output, fragment.format_version);
    PutU8(&output, fragment.usable ? 1 : 0);
    output.append(reinterpret_cast<const char*>(fragment.identity_checksum.data()),
                  fragment.identity_checksum.size());
  }
  const DurableCheckpoint& checkpoint = snapshot.checkpoint;
  if (checkpoint.wal_safe_lsns.size() > kMaxManifestEntries) {
    return Status::InvalidArgument("manifest", "WAL checkpoint limit exceeded");
  }
  const uint64_t wal_bytes =
      static_cast<uint64_t>(checkpoint.wal_safe_lsns.size()) *
      sizeof(uint64_t);
  const Status checkpoint_capacity = EnsureManifestBodyCapacity(
      output,
      64 + static_cast<uint64_t>(
               checkpoint.outcome_index_relative_path.size()) +
          wal_bytes,
      "checkpoint record");
  if (!checkpoint_capacity.ok()) return checkpoint_capacity;
  PutU64(&output, checkpoint.checkpoint_seq);
  PutU64(&output, checkpoint.decision_safe_seq);
  PutU64(&output, checkpoint.manifest_generation);
  const Status checkpoint_path = AppendLengthPrefixed(
      &output, checkpoint.outcome_index_relative_path, "outcome index path");
  if (!checkpoint_path.ok()) return checkpoint_path;
  output.append(reinterpret_cast<const char*>(checkpoint.outcome_index_checksum.data()),
                checkpoint.outcome_index_checksum.size());
  PutU32(&output, static_cast<uint32_t>(checkpoint.wal_safe_lsns.size()));
  for (uint64_t lsn : checkpoint.wal_safe_lsns) PutU64(&output, lsn);
  if (output.size() > kMaxManifestBodyBytes) {
    return Status::ResourceExhausted(
        "manifest", "encoded snapshot exceeds byte limit");
  }
  return output;
}

StatusOr<std::string> EncodeFramedSnapshot(const VersionSnapshot& snapshot) {
  const auto body = EncodeSnapshot(snapshot);
  if (!body.ok()) return body.status();
  if (body.ValueOrDie().size() > kMaxManifestBodyBytes) {
    return Status::ResourceExhausted(
        "manifest", "encoded snapshot exceeds byte limit");
  }
  std::string framed;
  PutU32(&framed, static_cast<uint32_t>(body.ValueOrDie().size()));
  PutU32(&framed,
         crc32c::Value(body.ValueOrDie().data(), body.ValueOrDie().size()));
  framed.append(body.ValueOrDie());
  return framed;
}

StatusOr<VersionSnapshot> DecodeSnapshot(const std::string& input) {
  if (input.size() > kMaxManifestBodyBytes) {
    return Status::Corruption("manifest", "snapshot exceeds byte limit");
  }
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t schema_count = 0;
  uint32_t file_count = 0;
  VersionSnapshot snapshot{};
  if (!GetU32(input, &offset, &magic) ||
      !GetU64(input, &offset, &snapshot.generation) ||
      !GetU64(input, &offset, &snapshot.next_index_id) ||
      !GetU32(input, &offset, &schema_count) || magic != kManifestMagic ||
      snapshot.next_index_id == 0 ||
      !DeclaredCountFits(input, offset, schema_count,
                         kMinSchemaRecordBytes)) {
    return Status::Corruption("manifest", "invalid snapshot header");
  }
  snapshot.schemas.reserve(schema_count);
  for (uint32_t index = 0; index < schema_count; ++index) {
    ColumnSchema schema{};
    uint8_t entity = 0;
    uint8_t physical = 0;
    uint8_t encoding = 0;
    uint8_t compression = 0;
    if (!GetU8(input, &offset, &entity) ||
        !GetU16(input, &offset, &schema.column_id) ||
        !GetU32(input, &offset, &schema.schema_epoch)) {
      return Status::Corruption("manifest", "invalid schema identity");
    }
    const auto logical_type =
        ReadLengthPrefixed(input, &offset, "schema logical type");
    if (!logical_type.ok() || logical_type.ValueOrDie().empty() ||
        logical_type.ValueOrDie().size() > kMaxSchemaLogicalTypeBytes ||
        !GetU8(input, &offset, &physical) ||
        !GetU64(input, &offset, &schema.blob_threshold) ||
        !GetU8(input, &offset, &encoding) ||
        !GetU8(input, &offset, &compression)) {
      return Status::Corruption("manifest", "invalid schema metadata");
    }
    schema.entity_type = static_cast<EntityType>(entity);
    schema.logical_type = logical_type.ValueOrDie();
    schema.physical_type = static_cast<PhysicalType>(physical);
    schema.encoding_policy = static_cast<EncodingPolicy>(encoding);
    schema.compression_policy = static_cast<CompressionPolicy>(compression);
    const Status valid = ValidateColumnSchema(schema, true);
    if (valid.IsNotSupportedError()) return valid;
    if (!valid.ok() || (!snapshot.schemas.empty() &&
                        !SchemaIdentityLess(snapshot.schemas.back(), schema))) {
      return Status::Corruption(
          "manifest", "invalid, duplicate, or unsorted schema catalog");
    }
    snapshot.schemas.push_back(std::move(schema));
  }
  if (!GetU32(input, &offset, &file_count) ||
      !DeclaredCountFits(input, offset, file_count,
                         kMinFileRecordBytes)) {
    return Status::Corruption("manifest", "invalid file count");
  }
  for (uint32_t index = 0; index < file_count; ++index) {
    SstFileMeta file{};
    uint8_t entity = 0;
    uint8_t key_kind = 0;
    uint8_t physical = 0;
    uint8_t compression = 0;
    uint8_t sort_order = 0;
    uint8_t hash_algorithm = 0;
    uint8_t encoding_registry = 0;
    uint8_t compression_registry = 0;
    uint8_t checksum_algorithm = 0;
    uint8_t typed_min_max_complete = 0;
    uint32_t blob_count = 0;
    if (!GetU64(input, &offset, &file.file_number)) {
      return Status::Corruption("manifest", "invalid file number");
    }
    const auto path = ReadLengthPrefixed(input, &offset, "file path");
    if (!path.ok() || !GetU8(input, &offset, &entity) ||
        !GetU8(input, &offset, &key_kind) ||
        !GetU16(input, &offset, &file.partition.column_id) ||
        !GetU32(input, &offset, &file.partition.schema_epoch) ||
        !GetU8(input, &offset, &physical) || !GetU16(input, &offset, &file.partition.edge_type) ||
        !GetU8(input, &offset, &compression) ||
        !GetU32(input, &offset, &file.partition.storage_shard_id) ||
        !GetU16(input, &offset, &file.partition.logical_type_id) ||
        !GetU64(input, &offset, &file.file_size) || !GetU32(input, &offset, &blob_count) ||
        !ValidEntity(entity) || !ValidKeyKind(key_kind) || !ValidPhysical(physical) ||
        !ValidCompression(compression) ||
        file.partition.logical_type_id == 0 ||
        file.file_number == 0 || !ValidSstRelativePath(path.ValueOrDie()) ||
        blob_count > kMaxManifestEntries ||
        static_cast<uint64_t>(blob_count) > (input.size() - offset) / 32) {
      return Status::Corruption("manifest", "invalid file metadata");
    }
    file.relative_path = path.ValueOrDie();
    file.partition.entity_type = static_cast<EntityType>(entity);
    file.partition.key_kind = static_cast<LogicalKeyKind>(key_kind);
    file.partition.physical_type = static_cast<PhysicalType>(physical);
    file.partition.compression_id = static_cast<CompressionId>(compression);
    const auto path_shard = SstShardIdFromPath(file.relative_path);
    if (!path_shard.has_value() ||
        *path_shard != file.partition.storage_shard_id) {
      return Status::Corruption("manifest", "SST shard ownership mismatch");
    }
    for (uint32_t blob = 0; blob < blob_count; ++blob) {
      BlobHash hash{};
      std::memcpy(hash.bytes.data(), input.data() + offset, hash.bytes.size());
      offset += hash.bytes.size();
      if (!file.blob_refs.empty() && !(file.blob_refs.back().bytes < hash.bytes)) {
        return Status::Corruption("manifest", "unsorted blob references");
      }
      file.blob_refs.push_back(hash);
    }
    if (!GetU8(input, &offset, &sort_order) ||
        !GetU8(input, &offset, &hash_algorithm) ||
        !GetU8(input, &offset, &encoding_registry) ||
        !GetU8(input, &offset, &compression_registry) ||
        !GetU8(input, &offset, &checksum_algorithm) ||
        offset + file.identity.bytes.size() > input.size()) {
      return Status::Corruption("manifest", "invalid SST ownership metadata");
    }
    std::memcpy(file.identity.bytes.data(), input.data() + offset,
                file.identity.bytes.size());
    offset += file.identity.bytes.size();
    file.format = SstFormatDescriptor{
        static_cast<SstSortOrderId>(sort_order),
        static_cast<SstHashAlgorithmId>(hash_algorithm),
        static_cast<SstEncodingRegistryId>(encoding_registry),
        static_cast<SstCompressionRegistryId>(compression_registry),
        static_cast<SstChecksumAlgorithmId>(checksum_algorithm)};
    if (!GetLogicalKey(input, &offset, &file.statistics.first_key) ||
        !GetLogicalKey(input, &offset, &file.statistics.last_key) ||
        !GetU64(input, &offset, &file.statistics.min_valid_from) ||
        !GetU64(input, &offset, &file.statistics.max_valid_from) ||
        !GetU64(input, &offset, &file.statistics.min_commit_seq) ||
        !GetU64(input, &offset, &file.statistics.max_commit_seq) ||
        !GetU64(input, &offset, &file.statistics.row_count) ||
        !GetU64(input, &offset, &file.statistics.put_count) ||
        !GetU64(input, &offset, &file.statistics.delete_count) ||
        !GetU64(input, &offset, &file.statistics.inline_value_count) ||
        !GetU64(input, &offset, &file.statistics.blob_reference_count) ||
        !GetU64(input, &offset, &file.statistics.typed_value_count) ||
        !GetU64(input, &offset, &file.statistics.nan_count) ||
        !GetU8(input, &offset, &typed_min_max_complete) ||
        typed_min_max_complete > 1 ||
        !GetOptionalValue(input, &offset, &file.statistics.typed_min) ||
        !GetOptionalValue(input, &offset, &file.statistics.typed_max) ||
        !GetU32(input, &offset, &file.statistics_crc32c)) {
      return Status::Corruption("manifest", "invalid SST statistics metadata");
    }
    file.statistics.typed_min_max_complete = typed_min_max_complete == 1;
    if (!ValidFormat(file.format)) {
      return Status::NotSupported("manifest",
                                  "unknown SST format algorithm ID");
    }
    if (IsZeroIdentity(file.identity) ||
        !ValidStatistics(file.statistics, file.partition) ||
        file.statistics_crc32c == 0) {
      return Status::Corruption("manifest", "invalid SST ownership metadata");
    }
    if (std::any_of(snapshot.files.begin(), snapshot.files.end(), [&file](const auto& existing) {
          return existing.file_number == file.file_number;
        })) {
      return Status::Corruption("manifest", "duplicate file number");
    }
    snapshot.files.push_back(std::move(file));
  }
  uint32_t blob_segment_count = 0;
  if (!GetU32(input, &offset, &blob_segment_count) ||
      !DeclaredCountFits(input, offset, blob_segment_count,
                         kMinBlobSegmentRecordBytes)) {
    return Status::Corruption("manifest", "invalid blob segment count");
  }
  std::map<uint32_t, uint64_t> active_segments;
  for (uint32_t index = 0; index < blob_segment_count; ++index) {
    BlobSegmentMeta segment;
    uint8_t active = 0;
    if (!GetU32(input, &offset, &segment.shard_id) || !GetU64(input, &offset, &segment.segment_id)) {
      return Status::Corruption("manifest", "invalid blob segment identity");
    }
    const auto path = ReadLengthPrefixed(input, &offset, "blob segment path");
    if (!path.ok() || !GetU8(input, &offset, &active) || active > 1) {
      return Status::Corruption("manifest", "invalid blob segment metadata");
    }
    segment.relative_path = path.ValueOrDie();
    segment.active = active == 1;
    if (!ValidBlobSegment(segment) ||
        std::any_of(snapshot.blob_segments.begin(), snapshot.blob_segments.end(), [&segment](const auto& existing) {
          return existing.shard_id == segment.shard_id && existing.segment_id == segment.segment_id;
        }) ||
        (segment.active && !active_segments.emplace(segment.shard_id, segment.segment_id).second)) {
      return Status::Corruption("manifest", "duplicate or invalid blob segment");
    }
    snapshot.blob_segments.push_back(std::move(segment));
  }
  uint32_t definition_count = 0;
  if (!GetU32(input, &offset, &definition_count) ||
      !DeclaredCountFits(input, offset, definition_count,
                         kIndexDefinitionRecordBytes)) {
    return Status::Corruption("manifest", "invalid index definition count");
  }
  for (uint32_t position = 0; position < definition_count; ++position) {
    IndexDefinition definition{};
    uint8_t entity = 0;
    uint8_t state = 0;
    if (!GetU64(input, &offset, &definition.index_id) || !GetU8(input, &offset, &entity) ||
        !GetU16(input, &offset, &definition.column_id) ||
        !GetU32(input, &offset, &definition.schema_epoch) ||
        !GetU32(input, &offset, &definition.capabilities) ||
        !GetU32(input, &offset, &definition.canonical_encoding_id) ||
        !GetU8(input, &offset, &state) || !GetU64(input, &offset, &definition.generation) ||
        input.size() - offset < definition.definition_checksum.size() || definition.index_id == 0 ||
        !ValidEntity(entity) || state > static_cast<uint8_t>(IndexState::kFailed) ||
        definition.capabilities == 0 ||
        !IsSupportedIndexCanonicalEncoding(definition.canonical_encoding_id)) {
      return Status::Corruption("manifest", "invalid index definition");
    }
    std::memcpy(definition.definition_checksum.data(), input.data() + offset,
                definition.definition_checksum.size());
    offset += definition.definition_checksum.size();
    if (std::any_of(snapshot.index_definitions.begin(), snapshot.index_definitions.end(),
                    [&definition](const auto& existing) {
                      return existing.index_id == definition.index_id;
                    })) {
      return Status::Corruption("manifest", "duplicate index id");
    }
    definition.entity_type = static_cast<EntityType>(entity);
    definition.state = static_cast<IndexState>(state);
    snapshot.index_definitions.push_back(std::move(definition));
  }
  uint32_t fragment_count = 0;
  if (!GetU32(input, &offset, &fragment_count) ||
      !DeclaredCountFits(input, offset, fragment_count,
                         kMinIndexFragmentRecordBytes)) {
    return Status::Corruption("manifest", "invalid index fragment count");
  }
  for (uint32_t position = 0; position < fragment_count; ++position) {
    IndexFragment fragment{};
    uint8_t usable = 0;
    if (!GetU64(input, &offset, &fragment.index_id) ||
        !GetU64(input, &offset, &fragment.source_sst_id)) {
      return Status::Corruption("manifest", "invalid index fragment identity");
    }
    const auto path = ReadLengthPrefixed(input, &offset, "index fragment path");
    if (!path.ok() || !GetU64(input, &offset, &fragment.source_row_count) ||
        !GetU64(input, &offset, &fragment.indexed_put_count) ||
        !GetU64(input, &offset, &fragment.catalog_generation) ||
        !GetU32(input, &offset, &fragment.format_version) || !GetU8(input, &offset, &usable) ||
        input.size() - offset < fragment.identity_checksum.size() || fragment.index_id == 0 ||
        fragment.source_sst_id == 0 || usable > 1) {
      return Status::Corruption("manifest", "invalid index fragment");
    }
    if (!std::any_of(snapshot.index_definitions.begin(), snapshot.index_definitions.end(),
                     [&fragment](const auto& definition) {
                       return definition.index_id == fragment.index_id;
                     }) ||
        !std::any_of(snapshot.files.begin(), snapshot.files.end(), [&fragment](const auto& file) {
          return file.file_number == fragment.source_sst_id;
        }) ||
        std::any_of(snapshot.index_fragments.begin(), snapshot.index_fragments.end(),
                    [&fragment](const auto& existing) {
                      return existing.index_id == fragment.index_id &&
                             existing.source_sst_id == fragment.source_sst_id;
                    })) {
      return Status::Corruption("manifest", "unknown or duplicate index fragment");
    }
    fragment.relative_path = path.ValueOrDie();
    fragment.usable = usable == 1;
    if (!ValidIndexFragmentPath(fragment)) {
      return Status::Corruption("manifest", "invalid index fragment path");
    }
    std::memcpy(fragment.identity_checksum.data(), input.data() + offset,
                fragment.identity_checksum.size());
    offset += fragment.identity_checksum.size();
    snapshot.index_fragments.push_back(std::move(fragment));
  }
  DurableCheckpoint checkpoint;
  if (!GetU64(input, &offset, &checkpoint.checkpoint_seq) ||
      !GetU64(input, &offset, &checkpoint.decision_safe_seq) ||
      !GetU64(input, &offset, &checkpoint.manifest_generation)) {
    return Status::Corruption("manifest", "truncated checkpoint metadata");
  }
  const auto checkpoint_path = ReadLengthPrefixed(input, &offset, "outcome index path");
  uint32_t wal_count = 0;
  if (!checkpoint_path.ok() ||
      input.size() - offset < checkpoint.outcome_index_checksum.size() ||
      checkpoint.decision_safe_seq != checkpoint.checkpoint_seq ||
      (checkpoint.checkpoint_seq == 0 &&
       (!checkpoint_path.ValueOrDie().empty() || checkpoint.manifest_generation != 0)) ||
      (checkpoint.checkpoint_seq != 0 &&
       (checkpoint_path.ValueOrDie().empty() || checkpoint.manifest_generation == 0 ||
        checkpoint.manifest_generation > snapshot.generation))) {
    return Status::Corruption("manifest", "invalid checkpoint metadata");
  }
  checkpoint.outcome_index_relative_path = checkpoint_path.ValueOrDie();
  std::memcpy(checkpoint.outcome_index_checksum.data(), input.data() + offset,
              checkpoint.outcome_index_checksum.size());
  offset += checkpoint.outcome_index_checksum.size();
  if (!GetU32(input, &offset, &wal_count) || wal_count > kMaxManifestEntries ||
      static_cast<uint64_t>(wal_count) > (input.size() - offset) / sizeof(uint64_t)) {
    return Status::Corruption("manifest", "invalid WAL safe LSN count");
  }
  checkpoint.wal_safe_lsns.reserve(wal_count);
  for (uint32_t index = 0; index < wal_count; ++index) {
    uint64_t lsn = 0;
    if (!GetU64(input, &offset, &lsn)) {
      return Status::Corruption("manifest", "invalid WAL safe LSN");
    }
    checkpoint.wal_safe_lsns.push_back(lsn);
  }
  snapshot.checkpoint = std::move(checkpoint);
  if (offset != input.size()) return Status::Corruption("manifest", "trailing snapshot bytes");
  const Status valid_snapshot = ValidateFinalSnapshot(snapshot);
  if (valid_snapshot.IsNotSupportedError()) return valid_snapshot;
  if (!valid_snapshot.ok()) {
    return Status::Corruption("manifest", valid_snapshot.ToString());
  }
  return snapshot;
}

Status WriteAll(int fd, const std::string& bytes, const std::string& path) {
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

}  // namespace

VersionSet::VersionSet(std::string manifest_path)
    : manifest_path_(std::move(manifest_path)),
      current_(std::make_shared<VersionSnapshot>(VersionSnapshot{0, {}, {}, {}, {}, {}})) {}

Status VersionSet::Open() {
  std::lock_guard<std::mutex> lock(mutex_);
  requires_reopen_.store(false, std::memory_order_release);
  if (!std::filesystem::exists(manifest_path_)) return Status::OK();
  const int fd = ::open(manifest_path_.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError(manifest_path_, std::strerror(errno));
  struct stat file_status {};
  if (::fstat(fd, &file_status) != 0) {
    const Status status =
        Status::IOError(manifest_path_, std::strerror(errno));
    ::close(fd);
    return status;
  }
  if (file_status.st_size < 0 ||
      static_cast<uint64_t>(file_status.st_size) > kMaxManifestBytes) {
    ::close(fd);
    return Status::Corruption("manifest", "Manifest exceeds byte limit");
  }
  std::string data;
  data.reserve(static_cast<size_t>(file_status.st_size));
  char buffer[8192];
  for (;;) {
    const ssize_t count = ::read(fd, buffer, sizeof(buffer));
    if (count == 0) break;
    if (count < 0) {
      const Status status = Status::IOError(manifest_path_, std::strerror(errno));
      ::close(fd);
      return status;
    }
    if (static_cast<uint64_t>(count) >
        kMaxManifestBytes - static_cast<uint64_t>(data.size())) {
      ::close(fd);
      return Status::Corruption(
          "manifest", "Manifest grew beyond byte limit");
    }
    data.append(buffer, static_cast<size_t>(count));
  }
  if (::close(fd) != 0) return Status::IOError(manifest_path_, std::strerror(errno));
  size_t offset = 0;
  uint32_t length = 0;
  uint32_t checksum = 0;
  if (!GetU32(data, &offset, &length) || !GetU32(data, &offset, &checksum) ||
      length != data.size() - offset || crc32c::Value(data.data() + offset, length) != checksum) {
    return Status::Corruption("manifest", "invalid manifest checksum");
  }
  const auto decoded = DecodeSnapshot(data.substr(offset));
  if (!decoded.ok()) return decoded.status();
  current_ = std::make_shared<VersionSnapshot>(decoded.ValueOrDie());
  return Status::OK();
}

Status VersionSet::Persist(const VersionSnapshot& snapshot) const {
  const auto data = EncodeFramedSnapshot(snapshot);
  if (!data.ok()) return data.status();
  const std::filesystem::path target(manifest_path_);
  std::error_code error;
  if (!target.parent_path().empty()) {
    std::filesystem::create_directories(target.parent_path(), error);
    if (error) return Status::IOError(manifest_path_, error.message());
  }
  const std::string temporary = manifest_path_ + ".tmp";
  const int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  Status status = WriteAll(fd, data.ValueOrDie(), temporary);
  if (status.ok() && ::fsync(fd) != 0) status = Status::IOError(temporary, std::strerror(errno));
  if (::close(fd) != 0 && status.ok()) status = Status::IOError(temporary, std::strerror(errno));
  if (!status.ok()) return status;
  if (::rename(temporary.c_str(), manifest_path_.c_str()) != 0) {
    return Status::IOError(manifest_path_, std::strerror(errno));
  }
  if (fault_injector_) {
    const Status injected = fault_injector_(VersionSetFaultPoint::kAfterManifestRename);
    if (!injected.ok()) {
      return Status::Indeterminate("manifest", injected.ToString());
    }
  }
  if (!target.parent_path().empty()) {
    const int directory_fd = ::open(target.parent_path().c_str(), O_RDONLY);
    if (directory_fd < 0) {
      return Status::Indeterminate("manifest", std::strerror(errno));
    }
    if (::fsync(directory_fd) != 0) {
      const Status fsync_status = Status::Indeterminate(
          "manifest", std::strerror(errno));
      ::close(directory_fd);
      return fsync_status;
    }
    if (::close(directory_fd) != 0) {
      return Status::Indeterminate("manifest", std::strerror(errno));
    }
  }
  const uint64_t written = data.ValueOrDie().size();
  uint64_t current = durable_bytes_written_.load(std::memory_order_relaxed);
  while (!durable_bytes_written_.compare_exchange_weak(
      current,
      written > std::numeric_limits<uint64_t>::max() - current
          ? std::numeric_limits<uint64_t>::max()
          : current + written,
      std::memory_order_relaxed, std::memory_order_relaxed)) {
  }
  return Status::OK();
}

Status VersionSet::ApplyEdit(const VersionEdit& edit) {
  return ApplyEditWithAdmission(edit, {});
}

Status VersionSet::ApplyEditWithAdmission(
    const VersionEdit& edit,
    const std::function<Status(uint64_t)>& admit_projected_rewrite) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (requires_reopen_.load(std::memory_order_acquire)) {
    return Status::RecoveryRequired("manifest", "reopen after indeterminate publication");
  }
  if (edit.expected_generation.has_value() &&
      *edit.expected_generation != current_->generation) {
    return Status::Conflict("manifest", "VersionSet generation changed");
  }
  VersionSnapshot next = *current_;
  if (next.generation == std::numeric_limits<uint64_t>::max()) {
    return Status::ResourceExhausted(
        "manifest", "VersionSet generation space exhausted");
  }
  ++next.generation;
  for (const ColumnSchema& schema : edit.schema_adds) {
    const Status valid = ValidateColumnSchema(schema, true);
    if (!valid.ok()) return valid;
    next.schemas.push_back(schema);
  }
  std::sort(next.schemas.begin(), next.schemas.end(), SchemaIdentityLess);
  for (uint64_t file_number : edit.deletes) {
    next.files.erase(std::remove_if(next.files.begin(), next.files.end(), [file_number](const auto& file) {
      return file.file_number == file_number;
    }), next.files.end());
    next.index_fragments.erase(
        std::remove_if(next.index_fragments.begin(), next.index_fragments.end(),
                       [file_number](const auto& fragment) {
                         return fragment.source_sst_id == file_number;
                       }), next.index_fragments.end());
  }
  for (const BlobSegmentKey& key : edit.blob_segment_deletes) {
    next.blob_segments.erase(
        std::remove_if(next.blob_segments.begin(), next.blob_segments.end(), [&key](const auto& segment) {
          return segment.shard_id == key.shard_id && segment.segment_id == key.segment_id;
        }), next.blob_segments.end());
  }
  for (const BlobSegmentMeta& update : edit.blob_segment_updates) {
    if (!ValidBlobSegment(update)) {
      return Status::InvalidArgument("manifest", "invalid blob segment update");
    }
    const auto existing = std::find_if(next.blob_segments.begin(), next.blob_segments.end(), [&update](const auto& segment) {
      return segment.shard_id == update.shard_id && segment.segment_id == update.segment_id;
    });
    if (existing == next.blob_segments.end()) {
      return Status::InvalidArgument("manifest", "unknown blob segment update");
    }
    *existing = update;
  }
  for (const BlobSegmentMeta& add : edit.blob_segment_adds) {
    if (!ValidBlobSegment(add) ||
        std::any_of(next.blob_segments.begin(), next.blob_segments.end(), [&add](const auto& segment) {
          return segment.shard_id == add.shard_id && segment.segment_id == add.segment_id;
        })) {
      return Status::InvalidArgument("manifest", "duplicate or invalid blob segment");
    }
    next.blob_segments.push_back(add);
  }
  std::map<uint32_t, uint64_t> active_segments;
  for (const BlobSegmentMeta& segment : next.blob_segments) {
    if (segment.active && !active_segments.emplace(segment.shard_id, segment.segment_id).second) {
      return Status::InvalidArgument("manifest", "multiple active blob segments per shard");
    }
  }
  for (SstFileMeta file : edit.adds) {
    const auto path_shard = SstShardIdFromPath(file.relative_path);
    if (file.file_number == 0 || !path_shard.has_value() ||
        *path_shard != file.partition.storage_shard_id ||
        std::any_of(next.files.begin(), next.files.end(), [&file](const auto& existing) {
          return existing.file_number == file.file_number;
        })) {
      return Status::InvalidArgument("manifest", "duplicate or invalid file number");
    }
    std::sort(file.blob_refs.begin(), file.blob_refs.end(), [](const BlobHash& left, const BlobHash& right) {
      return left.bytes < right.bytes;
    });
    file.blob_refs.erase(std::unique(file.blob_refs.begin(), file.blob_refs.end()),
                         file.blob_refs.end());
    next.files.push_back(std::move(file));
  }
  const uint64_t minimum_new_index_id = next.next_index_id;
  for (uint64_t index_id : edit.index_deletes) {
    next.index_definitions.erase(
        std::remove_if(next.index_definitions.begin(), next.index_definitions.end(),
                       [index_id](const auto& definition) { return definition.index_id == index_id; }),
        next.index_definitions.end());
    next.index_fragments.erase(
        std::remove_if(next.index_fragments.begin(), next.index_fragments.end(),
                       [index_id](const auto& fragment) { return fragment.index_id == index_id; }),
        next.index_fragments.end());
  }
  for (const IndexDefinition& definition : edit.index_adds) {
    if (definition.index_id < minimum_new_index_id ||
        definition.capabilities == 0 ||
        definition.index_id == std::numeric_limits<uint64_t>::max() ||
        !IsSupportedIndexCanonicalEncoding(definition.canonical_encoding_id) ||
        std::any_of(next.index_definitions.begin(), next.index_definitions.end(),
                    [&definition](const auto& existing) {
                      return existing.index_id == definition.index_id;
                    })) {
      return Status::InvalidArgument("manifest", "duplicate or invalid index definition");
    }
    next.index_definitions.push_back(definition);
    next.next_index_id =
        std::max(next.next_index_id, definition.index_id + 1);
  }
  for (const IndexDefinition& definition : edit.index_updates) {
    if (definition.index_id == 0 || definition.capabilities == 0 ||
        !IsSupportedIndexCanonicalEncoding(definition.canonical_encoding_id)) {
      return Status::InvalidArgument("manifest", "invalid index update");
    }
    const auto found = std::find_if(next.index_definitions.begin(), next.index_definitions.end(),
                                    [&definition](const auto& existing) {
                                      return existing.index_id == definition.index_id;
                                    });
    if (found == next.index_definitions.end()) {
      return Status::InvalidArgument("manifest", "unknown index update");
    }
    *found = definition;
  }
  for (const IndexFragmentKey& key : edit.index_fragment_deletes) {
    next.index_fragments.erase(
        std::remove_if(next.index_fragments.begin(), next.index_fragments.end(), [&key](const auto& fragment) {
          return fragment.index_id == key.index_id && fragment.source_sst_id == key.source_sst_id;
        }), next.index_fragments.end());
  }
  for (const IndexFragment& fragment : edit.index_fragment_adds) {
    if (fragment.index_id == 0 || fragment.source_sst_id == 0 ||
        !ValidIndexFragmentPath(fragment) ||
        fragment.format_version == 0 ||
        !std::any_of(next.index_definitions.begin(), next.index_definitions.end(), [&fragment](const auto& index) {
          return index.index_id == fragment.index_id;
        }) ||
        !std::any_of(next.files.begin(), next.files.end(), [&fragment](const auto& file) {
          return file.file_number == fragment.source_sst_id;
        })) {
      return Status::InvalidArgument("manifest", "invalid index fragment");
    }
    const auto existing = std::find_if(next.index_fragments.begin(), next.index_fragments.end(),
                                       [&fragment](const auto& candidate) {
                                         return candidate.index_id == fragment.index_id &&
                                                candidate.source_sst_id == fragment.source_sst_id;
                                       });
    if (existing == next.index_fragments.end()) {
      next.index_fragments.push_back(fragment);
    } else {
      *existing = fragment;
    }
  }
  if (edit.checkpoint.has_value()) {
    DurableCheckpoint checkpoint = *edit.checkpoint;
    if (checkpoint.checkpoint_seq < current_->checkpoint.checkpoint_seq ||
        checkpoint.decision_safe_seq != checkpoint.checkpoint_seq ||
        (checkpoint.checkpoint_seq == 0 && !checkpoint.outcome_index_relative_path.empty()) ||
        (checkpoint.checkpoint_seq != 0 && checkpoint.outcome_index_relative_path.empty()) ||
        (!current_->checkpoint.wal_safe_lsns.empty() &&
         checkpoint.wal_safe_lsns.size() != current_->checkpoint.wal_safe_lsns.size())) {
      return Status::InvalidArgument("manifest", "invalid durable checkpoint edit");
    }
    for (size_t index = 0; index < checkpoint.wal_safe_lsns.size(); ++index) {
      if (index < current_->checkpoint.wal_safe_lsns.size() &&
          checkpoint.wal_safe_lsns[index] < current_->checkpoint.wal_safe_lsns[index]) {
        return Status::InvalidArgument("manifest", "WAL safe LSN regressed");
      }
    }
    checkpoint.manifest_generation = next.generation;
    next.checkpoint = std::move(checkpoint);
  }
  const Status valid_snapshot = ValidateFinalSnapshot(next);
  if (!valid_snapshot.ok()) return valid_snapshot;
  if (admit_projected_rewrite) {
    const auto encoded = EncodeFramedSnapshot(next);
    if (!encoded.ok()) return encoded.status();
    const Status admitted =
        admit_projected_rewrite(encoded.ValueOrDie().size());
    if (!admitted.ok()) return admitted;
  }
  const Status persisted = Persist(next);
  if (!persisted.ok()) {
    if (persisted.IsIndeterminate()) {
      requires_reopen_.store(true, std::memory_order_release);
    }
    return persisted;
  }
  current_ = std::make_shared<VersionSnapshot>(std::move(next));
  return Status::OK();
}

StatusOr<uint64_t> VersionSet::EstimateSchemaEditRewriteBytes(
    const ColumnSchema& schema_add) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (current_ == nullptr) {
    return Status::InvalidArgument(
        "manifest estimate", "VersionSet is not open");
  }
  if (current_->generation == std::numeric_limits<uint64_t>::max()) {
    return Status::ResourceExhausted(
        "manifest estimate", "VersionSet generation space exhausted");
  }
  const Status valid = ValidateColumnSchema(schema_add, true);
  if (!valid.ok()) return valid;
  VersionSnapshot projected = *current_;
  ++projected.generation;
  projected.schemas.push_back(schema_add);
  std::sort(projected.schemas.begin(), projected.schemas.end(),
            SchemaIdentityLess);
  const auto encoded = EncodeFramedSnapshot(projected);
  if (!encoded.ok()) return encoded.status();
  return static_cast<uint64_t>(encoded.ValueOrDie().size());
}

StatusOr<uint64_t> VersionSet::EstimateManifestEditRewriteBytes(
    const VersionEdit& edit) {
  uint64_t projected_bytes = 0;
  bool measured = false;
  const Status status = ApplyEditWithAdmission(
      edit, [&](uint64_t bytes) {
        projected_bytes = bytes;
        measured = true;
        return Status::MaintenanceBackoff(
            "manifest estimate", "projection complete");
      });
  if (measured && status.IsMaintenanceBackoff()) return projected_bytes;
  return status;
}

StatusOr<uint64_t> VersionSet::EstimateManifestRewriteBytes(
    uint64_t rewrite_count, uint64_t additional_blob_segments) const {
  if (rewrite_count == 0) return uint64_t{0};
  std::lock_guard<std::mutex> lock(mutex_);
  if (current_ == nullptr) {
    return Status::InvalidArgument("manifest estimate", "VersionSet is not open");
  }
  VersionSnapshot projected = *current_;
  for (uint64_t index = 0; index < additional_blob_segments; ++index) {
    if (projected.blob_segments.size() >= kMaxManifestEntries) {
      return Status::ResourceExhausted("manifest estimate", "blob segment limit exceeded");
    }
    const uint32_t shard_id = std::numeric_limits<uint32_t>::max();
    uint64_t segment_id = std::numeric_limits<uint64_t>::max() - index;
    while (std::any_of(projected.blob_segments.begin(), projected.blob_segments.end(),
                       [shard_id, segment_id](const BlobSegmentMeta& segment) {
                         return segment.shard_id == shard_id &&
                                segment.segment_id == segment_id;
                       })) {
      if (segment_id == 1) {
        return Status::ResourceExhausted("manifest estimate",
                                         "synthetic segment identity exhausted");
      }
      --segment_id;
    }
    projected.blob_segments.push_back(BlobSegmentMeta{
        shard_id, segment_id,
        "blobs/shard-4294967295/segment-18446744073709551615.blob", false});
  }
  const auto encoded = EncodeFramedSnapshot(projected);
  if (!encoded.ok()) return encoded.status();
  const uint64_t bytes = encoded.ValueOrDie().size();
  if (rewrite_count > std::numeric_limits<uint64_t>::max() / bytes) {
    return Status::ResourceExhausted("manifest estimate", "write estimate overflow");
  }
  return rewrite_count * bytes;
}

std::shared_ptr<const VersionSnapshot> VersionSet::Snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_;
}

}  // namespace cedar
