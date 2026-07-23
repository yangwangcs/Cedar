// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STORAGE_SST_COMPACTION_H_
#define CEDAR_STORAGE_SST_COMPACTION_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/runtime/work_cancellation.h"
#include "cedar/storage/version_set.h"

namespace cedar {

struct CompactionResult {
  uint64_t next_file_number = 0;
  SstFileMeta output;
  std::vector<SstFileMeta> inputs;
  uint64_t peak_buffered_events = 0;
  uint64_t input_bytes_read = 0;
  uint64_t output_bytes_written = 0;
  uint64_t blob_payload_bytes_read = 0;
  PageCompressionStats compression;
};

// Compacts one complete same-shard, same-partition closure. The input list is
// validated before writing and publication is one Manifest Add+Delete edit.
// Blob payloads are never read: the output reconstructs only TemporalEvents
// and their durable BlobRef values from the input SSTs.
StatusOr<CompactionResult> CompactSstPartition(
    const std::string& db_path, const std::vector<SstFileMeta>& inputs,
    uint64_t output_file_number, VersionSet* version_set,
    std::shared_ptr<WorkCancellation> cancellation = nullptr);

}  // namespace cedar

#endif  // CEDAR_STORAGE_SST_COMPACTION_H_
