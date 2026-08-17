// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STORAGE_SST_FLUSH_H_
#define CEDAR_STORAGE_SST_FLUSH_H_

#include "cedar/storage/storage_shard.h"
#include "cedar/storage/version_set.h"
#include "cedar/schema/schema_registry.h"

namespace cedar {
struct FlushResult {
  uint64_t next_file_number;
  std::vector<SstFileMeta> files;
  PageCompressionStats compression;
};
StatusOr<FlushResult> FlushEventsToSst(const std::string& db_path,
                                           uint32_t shard_id,
                                           const std::vector<TemporalEvent>& events,
                                           uint64_t first_file_number,
                                           const SchemaRegistry& schemas,
                                           VersionSet* version_set,
                                           std::function<Status(
                                               SstPublicationFaultPoint)>
                                               fault_injector = {});
StatusOr<FlushResult> FlushShardToSst(const std::string& db_path,
                                          const StorageShard& shard,
                                          uint64_t first_file_number,
                                          const SchemaRegistry& schemas,
                                          VersionSet* version_set);
}  // namespace cedar
#endif  // CEDAR_STORAGE_SST_FLUSH_H_
