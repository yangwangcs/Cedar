// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/storage_files.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/metadata.h>
#include <rocksdb/options.h>

#include "storage/facts/fact_store.h"
#include "storage/rocks/rocksdb_config.h"
#include "query/observability/query_metrics.h"
#include "query/resource/query_scratch.h"
#include "query/projection/projection_format.h"
#include "query/projection/projection_manifest.h"

namespace cedar {
namespace {

Status FromRocksDb(const rocksdb::Status& status, const char* context) {
  if (status.ok()) return Status::OK();
  const std::string message = status.ToString();
  if (status.IsNotFound()) return Status::NotFound(context, message);
  if (status.IsCorruption()) return Status::Corruption(context, message);
  if (status.IsInvalidArgument()) return Status::InvalidArgument(context, message);
  if (status.IsNotSupported()) return Status::NotSupported(context, message);
  return Status::IOError(context, message);
}

std::string HexEncode(const std::string& bytes) {
  constexpr char kHex[] = "0123456789abcdef";
  std::string encoded;
  encoded.reserve(bytes.size() * 2);
  for (unsigned char byte : bytes) {
    encoded.push_back(kHex[byte >> 4]);
    encoded.push_back(kHex[byte & 0x0f]);
  }
  return encoded;
}

StatusOr<std::pair<StorageFileRole, StorageTableFormat>> ClassifyFile(
    const std::string& column_family_name) {
  if (column_family_name == "facts") {
    return std::make_pair(StorageFileRole::kAuthoritativeFacts,
                          StorageTableFormat::kCedarParquet);
  }
  if (column_family_name == "meta") {
    return std::make_pair(StorageFileRole::kTransactionMetadata,
                          StorageTableFormat::kBlockBased);
  }
  if (column_family_name == rocksdb::kDefaultColumnFamilyName) {
    return std::make_pair(StorageFileRole::kEngineInternal,
                          StorageTableFormat::kBlockBased);
  }
  return Status::Corruption("storage file inspection",
                            "unknown column family: " + column_family_name);
}

void CloseReadOnlyDatabase(std::unique_ptr<rocksdb::DB>* db,
                           std::vector<rocksdb::ColumnFamilyHandle*>* handles) {
  for (rocksdb::ColumnFamilyHandle* handle : *handles) {
    (*db)->DestroyColumnFamilyHandle(handle).PermitUncheckedError();
  }
  handles->clear();
  db->reset();
}

std::optional<std::pair<StorageFileRole, StorageTableFormat>> ClassifyCedarPath(
    const std::filesystem::path& path) {
  const std::string ext = path.extension().string();
  if (ext == ".cmanifest") return std::make_pair(StorageFileRole::kQueryProjection, StorageTableFormat::kCedarManifest);
  if (ext == ".cstate" || ext == ".csegment") return std::make_pair(StorageFileRole::kQueryProjection, StorageTableFormat::kCedarState);
  if (ext == ".cadj") return std::make_pair(StorageFileRole::kQueryProjection, StorageTableFormat::kCedarAdjacency);
  if (ext == ".cprop") return std::make_pair(StorageFileRole::kQueryProjection, StorageTableFormat::kCedarProperty);
  if (ext == ".cstats") return std::make_pair(StorageFileRole::kQueryStatistics, StorageTableFormat::kCedarStatistics);
  if (ext == ".cscratch") return std::make_pair(StorageFileRole::kQueryScratch, StorageTableFormat::kCedarScratch);
  return std::nullopt;
}

void AppendCedarFiles(const std::string& root, std::vector<StorageFileInfo>* files) {
  std::error_code ec;
  for (std::filesystem::recursive_directory_iterator it(root, ec), end; it != end && !ec; it.increment(ec)) {
    if (!it->is_regular_file(ec)) continue;
    const auto classified = ClassifyCedarPath(it->path());
    if (!classified) continue;
    const auto [role, format] = *classified;
    StorageFileInfo info;
    info.relative_filename = std::filesystem::relative(it->path(), root, ec).generic_string();
    info.column_family_name = "cedar-query";
    info.role = role;
    info.table_format = format;
    info.level = -1;
    info.size_bytes = it->file_size(ec);
    QueryFileMetadata metadata;
    metadata.authority = role == StorageFileRole::kQueryScratch ? StorageFileAuthority::kTemporary : StorageFileAuthority::kDerived;
    std::ifstream input(it->path(), std::ios::binary);
    const std::string bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    if (format == StorageTableFormat::kCedarStatistics) {
      const auto decoded = internal::DecodeQueryStatistics(bytes);
      metadata.checksum_valid = decoded.ok();
      metadata.available = decoded.ok();
      if (decoded.ok()) {
        metadata.generation_id = decoded.ValueOrDie().generation_id;
        metadata.base_seq = decoded.ValueOrDie().base_seq.value;
        metadata.coverage = decoded.ValueOrDie().coverage;
      }
    } else if (format == StorageTableFormat::kCedarManifest) {
      const auto decoded = internal::DecodeProjectionManifest(bytes, root);
      metadata.checksum_valid = decoded.ok();
      metadata.available = decoded.ok();
      if (decoded.ok()) {
        const auto& manifest = decoded.ValueOrDie();
        metadata.generation_id = manifest.generation_id;
        metadata.base_seq = manifest.base_seq.value;
        for (size_t i = 0; i < manifest.regions.size(); ++i) {
          if (i != 0) metadata.coverage.push_back('|');
          const auto& region = manifest.regions[i];
          metadata.coverage += "part=" + std::to_string(region.part_id.value) +
              ",entity=[" + std::to_string(region.entity_min) + "," +
              std::to_string(region.entity_max_exclusive) + "),valid=[" +
              std::to_string(region.valid_time.from.value) + "," +
              (region.valid_time.to ? std::to_string(region.valid_time.to->value) : "inf") + ")";
        }
      }
    } else if (format == StorageTableFormat::kCedarScratch) {
      const auto decoded = internal::DecodeScratchFile(bytes);
      metadata.checksum_valid = decoded.ok();
      metadata.available = decoded.ok();
      // Scratch files are query-instance artifacts.  Their framing carries
      // a query id and payload length for integrity/budget accounting, but
      // neither is canonical coverage and must not become an inspection
      // label.  Leave coverage empty until a manifest-derived range exists.
    } else {
      const auto decoded = internal::DecodeProjectionPage(bytes);
      metadata.checksum_valid = decoded.ok();
      metadata.available = decoded.ok();
      if (decoded.ok()) {
        const auto& header = decoded.ValueOrDie().header;
        metadata.generation_id = header.generation_id;
        metadata.base_seq = header.base_seq.value;
        metadata.coverage = "part=" + std::to_string(header.part_id.value) +
            ",entity=[" + std::to_string(header.entity_min) + "," +
            std::to_string(header.entity_max_exclusive) + "),valid=[" +
            std::to_string(header.valid_from_min.value) + "," +
            (header.valid_to_max ? std::to_string(header.valid_to_max->value) : "inf") + ")";
      }
    }
    info.query_file = std::move(metadata);
    files->push_back(std::move(info));
  }
}

}  // namespace

StatusOr<std::vector<StorageFileInfo>> InspectStorageFiles(
    StorageFileInspectionOptions options) {
  if (options.path.empty()) {
    return Status::InvalidArgument("storage file inspection", "missing database path");
  }
  std::error_code filesystem_error;
  if (!std::filesystem::exists(std::filesystem::path(options.path) / "CURRENT",
                               filesystem_error)) {
    if (filesystem_error) {
      return Status::IOError("storage file inspection", filesystem_error.message());
    }
    return Status::NotFound("storage file inspection", "missing RocksDB CURRENT file");
  }

  FactStoreOptions store_options;
  store_options.path = std::move(options.path);
  store_options.storage_profile = options.storage_profile;
  store_options.production = std::move(options.production);
  const auto resolved_profile = internal::ResolveStorageProfile(store_options);
  if (!resolved_profile.ok()) return resolved_profile.status();
  const internal::ResolvedStorageProfile* resolved_ptr =
      UsesCedarKernelProfile(store_options.storage_profile)
          ? &resolved_profile.ValueOrDie()
          : nullptr;
  rocksdb::Options rocks_options = internal::MakeRocksDbOptions(
      store_options, false, resolved_ptr);
  rocks_options.create_if_missing = false;
  rocks_options.create_missing_column_families = false;

  std::vector<std::string> column_families;
  const rocksdb::Status listed = rocksdb::DB::ListColumnFamilies(
      rocksdb::DBOptions(rocks_options), store_options.path, &column_families);
  if (!listed.ok()) return FromRocksDb(listed, "list storage column families");
  const std::vector<std::string> expected = {rocksdb::kDefaultColumnFamilyName,
                                             "facts", "meta"};
  std::sort(column_families.begin(), column_families.end());
  std::vector<std::string> sorted_expected = expected;
  std::sort(sorted_expected.begin(), sorted_expected.end());
  if (column_families != sorted_expected) {
    return Status::NotSupported("storage file inspection",
                                "database has an incompatible column family layout");
  }

  auto descriptors = internal::MakeRocksDbColumnFamilyDescriptors(
      store_options, rocks_options, resolved_ptr);
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> db;
  const rocksdb::Status opened = rocksdb::DB::OpenForReadOnly(
      rocksdb::DBOptions(rocks_options), store_options.path, descriptors, &handles,
      &db, false /* error_if_wal_file_exists */);
  if (!opened.ok()) return FromRocksDb(opened, "open storage read-only");
  if (handles.size() != descriptors.size()) {
    CloseReadOnlyDatabase(&db, &handles);
    return Status::Corruption("storage file inspection",
                              "missing required column family handle");
  }

  std::vector<rocksdb::LiveFileMetaData> live_files;
  db->GetLiveFilesMetaData(&live_files);
  std::vector<StorageFileInfo> files;
  files.reserve(live_files.size());
  for (const rocksdb::LiveFileMetaData& live_file : live_files) {
    const auto classification = ClassifyFile(live_file.column_family_name);
    if (!classification.ok()) {
      const Status status = classification.status();
      CloseReadOnlyDatabase(&db, &handles);
      return status;
    }
    const auto [role, table_format] = classification.ValueOrDie();
    files.push_back(StorageFileInfo{live_file.relative_filename,
                                    live_file.column_family_name,
                                    role,
                                    table_format,
                                    live_file.level,
                                    live_file.size,
                                    live_file.smallest_seqno,
                                    live_file.largest_seqno,
                                    HexEncode(live_file.smallestkey),
                                    HexEncode(live_file.largestkey)});
  }
  // Cedar query files are a separate namespace from RocksDB live-file
  // metadata. In particular, an engine .sst is never classified as a Cedar
  // projection merely because it lives below the database directory.
  AppendCedarFiles(store_options.path, &files);
  CloseReadOnlyDatabase(&db, &handles);
  std::sort(files.begin(), files.end(), [](const StorageFileInfo& left,
                                           const StorageFileInfo& right) {
    return std::tie(left.column_family_name, left.level, left.relative_filename) <
           std::tie(right.column_family_name, right.level, right.relative_filename);
  });
  return files;
}

}  // namespace cedar
