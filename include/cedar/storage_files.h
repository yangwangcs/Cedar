// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STORAGE_FILES_H_
#define CEDAR_STORAGE_FILES_H_

#include <cstdint>
#include <string>
#include <vector>
#include <optional>

#include "cedar/core/status.h"
#include "cedar/storage_options.h"

namespace cedar {

enum class StorageFileRole : uint8_t {
  kAuthoritativeFacts,
  kTransactionMetadata,
  kEngineInternal,
  kQueryProjection,
  kQueryStatistics,
  kQueryScratch,
};

enum class StorageTableFormat : uint8_t {
  kCedarParquet,
  kBlockBased,
  kCedarManifest,
  kCedarState,
  kCedarAdjacency,
  kCedarProperty,
  kCedarStatistics,
  kCedarScratch,
};

enum class StorageFileAuthority : uint8_t {
  kAuthoritative,
  kDerived,
  kTemporary,
  kEngineInternal,
};

struct QueryFileMetadata {
  StorageFileAuthority authority = StorageFileAuthority::kEngineInternal;
  std::optional<uint64_t> generation_id;
  std::optional<uint64_t> base_seq;
  std::string coverage;
  bool checksum_valid = false;
};

struct StorageFileInfo {
  std::string relative_filename;
  std::string column_family_name;
  StorageFileRole role = StorageFileRole::kEngineInternal;
  StorageTableFormat table_format = StorageTableFormat::kBlockBased;
  int level = 0;
  uint64_t size_bytes = 0;
  uint64_t smallest_seqno = 0;
  uint64_t largest_seqno = 0;
  std::string smallest_key_hex;
  std::string largest_key_hex;
  std::optional<QueryFileMetadata> query_file;
};

struct StorageFileInspectionOptions {
  std::string path;
  StorageProfile storage_profile = StorageProfile::kDeveloper;
  ProductionStorageOptions production;
};

StatusOr<std::vector<StorageFileInfo>> InspectStorageFiles(
    StorageFileInspectionOptions options);

}  // namespace cedar

#endif  // CEDAR_STORAGE_FILES_H_
