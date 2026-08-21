// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/storage_files.h"

#include <algorithm>
#include <filesystem>
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
  CloseReadOnlyDatabase(&db, &handles);
  std::sort(files.begin(), files.end(), [](const StorageFileInfo& left,
                                           const StorageFileInfo& right) {
    return std::tie(left.column_family_name, left.level, left.relative_filename) <
           std::tie(right.column_family_name, right.level, right.relative_filename);
  });
  return files;
}

}  // namespace cedar
