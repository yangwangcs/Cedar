// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef CEDAR_TRANSACTION_DATABASE_FORMAT_H_
#define CEDAR_TRANSACTION_DATABASE_FORMAT_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/status.h"

namespace cedar {

constexpr uint32_t kCedarDatabaseFormatVersion = 1;

enum class CedarHashAlgorithm : uint32_t {
  kFnv1a64 = 1,
};

struct DatabaseFormat {
  uint32_t format_version = kCedarDatabaseFormatVersion;
  uint32_t shard_count = 0;
  CedarHashAlgorithm hash_algorithm = CedarHashAlgorithm::kFnv1a64;
  uint64_t hash_seed = 0;
  std::string manifest_location;
  std::string decision_log_location;
  std::vector<std::string> shard_wal_locations;
};

DatabaseFormat MakeDatabaseFormat(uint32_t shard_count, uint64_t hash_seed);

// Writes a structurally valid, checksummed format record. This low-level API
// permits a future format_version so recovery tests and upgrade tooling can
// distinguish an unsupported version from physical corruption.
Status WriteDatabaseFormat(const std::string& path, const DatabaseFormat& format);
StatusOr<DatabaseFormat> ReadDatabaseFormat(const std::string& path);

// Creates FORMAT only for a new empty directory. Existing databases are
// accepted only when their durable identity exactly matches expected.
Status CreateOrValidateDatabaseFormat(const std::string& db_path,
                                      const DatabaseFormat& expected);

}  // namespace cedar

#endif  // CEDAR_TRANSACTION_DATABASE_FORMAT_H_
