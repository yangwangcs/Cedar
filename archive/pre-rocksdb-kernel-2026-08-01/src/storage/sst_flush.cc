// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/storage/sst_flush.h"

#include <filesystem>
#include <map>
#include <tuple>

#include "cedar/columnar/sst.h"
#include "cedar/storage/storage_layout.h"

namespace cedar {
namespace {

CompressionId PageCompressionFor(const ColumnSchema& schema) {
  if (schema.compression_policy == CompressionPolicy::kLz4) {
    return CompressionId::kLz4;
  }
  if (schema.compression_policy == CompressionPolicy::kZstd) {
    return CompressionId::kZstd;
  }
  return CompressionId::kNone;
}

}  // namespace

StatusOr<FlushResult> FlushEventsToSst(const std::string& db_path,
                                            uint32_t shard_id,
                                            const std::vector<TemporalEvent>& events,
                                            uint64_t first_file_number,
                                            const SchemaRegistry& schemas,
                                            VersionSet* version_set,
                                            std::function<Status(
                                                SstPublicationFaultPoint)>
                                                fault_injector) {
  if (version_set == nullptr) return Status::InvalidArgument("flush", "missing version set");
  using PartitionKey =
      std::tuple<uint8_t, uint8_t, uint16_t, uint32_t, uint8_t, uint16_t, uint8_t>;
  std::map<PartitionKey, std::vector<TemporalEvent>> partitions;
  for (const TemporalEvent& event : events) {
    const LogicalKey& key = event.logical_key();
    const auto schema = schemas.Lookup(key.entity_type(), key.schema_column_id(), event.schema_epoch());
    if (!schema.has_value()) return Status::SchemaMismatch("flush", "missing event schema");
    const PhysicalType physical = schema->physical_type;
    if (!event.is_delete() && !event.is_blob_reference() && event.value().type() != physical)
      return Status::SchemaMismatch("flush", "event value type mismatch");
    partitions[{static_cast<uint8_t>(key.entity_type()), static_cast<uint8_t>(key.kind()),
                key.schema_column_id(), event.schema_epoch(),
                static_cast<uint8_t>(physical), key.edge_type(),
                static_cast<uint8_t>(PageCompressionFor(*schema))}].push_back(event);
  }
  std::vector<SstFileMeta> files;
  PageCompressionStats compression_stats;
  uint64_t file_number = first_file_number;
  for (const auto& entry : partitions) {
    const auto [entity, kind, column, epoch, physical, edge_type, compression] = entry.first;
    const BlockPartition partition{static_cast<EntityType>(entity), column, epoch,
                                   static_cast<PhysicalType>(physical), edge_type,
                                   static_cast<CompressionId>(compression),
                                   static_cast<LogicalKeyKind>(kind), shard_id,
                                   kCedarLogicalTypeRegistryId};
    const std::string relative = "shards/" + std::to_string(shard_id) +
                                 "/sst/" + std::to_string(file_number) +
                                     storage_layout::kSstExtension;
    const std::string path = db_path + "/" + relative;
    SstFile written;
    Status status = WriteSstFile(
        path, partition, entry.second, &written, fault_injector);
    if (!status.ok()) return status;
    std::error_code error;
    const uint64_t size = std::filesystem::file_size(path, error);
    if (error) return Status::IOError(path, error.message());
    files.push_back(SstFileMeta{
        file_number++, relative, partition, size, std::move(written.blob_refs),
        written.metadata.format, written.metadata.identity,
        written.metadata.statistics, written.metadata.statistics_crc32c});
    for (size_t slot = 0; slot < kPageTypeMetricSlots; ++slot) {
      const auto add = [](uint64_t value, uint64_t* total) {
        *total = value > UINT64_MAX - *total ? UINT64_MAX : *total + value;
      };
      add(written.compression.uncompressed_bytes[slot],
          &compression_stats.uncompressed_bytes[slot]);
      add(written.compression.stored_bytes[slot],
          &compression_stats.stored_bytes[slot]);
    }
  }
  Status status = version_set->ApplyEdit(VersionEdit{files, {}});
  if (!status.ok()) return status;
  return FlushResult{file_number, std::move(files), compression_stats};
}

StatusOr<FlushResult> FlushShardToSst(const std::string& db_path,
                                          const StorageShard& shard,
                                          uint64_t first_file_number,
                                          const SchemaRegistry& schemas,
                                          VersionSet* version_set) {
  return FlushEventsToSst(db_path, shard.shard_id(), shard.SnapshotEvents(), first_file_number,
                            schemas, version_set);
}
}  // namespace cedar
