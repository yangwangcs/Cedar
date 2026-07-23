// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STORAGE_VERSION_SET_H_
#define CEDAR_STORAGE_VERSION_SET_H_

#include <array>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cedar/blob/blob_store.h"
#include "cedar/columnar/sst.h"
#include "cedar/core/status.h"
#include "cedar/index/index_definition.h"
#include "cedar/schema/schema_registry.h"

namespace cedar {
struct SstFileMeta {
  uint64_t file_number;
  std::string relative_path;
  BlockPartition partition;
  uint64_t file_size;
  std::vector<BlobHash> blob_refs;
  SstFormatDescriptor format;
  SstFileIdentity identity;
  SstFileStatistics statistics;
  uint32_t statistics_crc32c = 0;
};
struct BlobSegmentMeta {
  uint32_t shard_id = 0;
  uint64_t segment_id = 0;
  std::string relative_path;
  bool active = false;
};
struct BlobSegmentKey {
  uint32_t shard_id = 0;
  uint64_t segment_id = 0;
};
// Immutable, source-SST-bound secondary-index artifact. The sidecar itself is
// optional for correctness, but its coverage state is snapshot-owned by the
// same Manifest edit as the source VersionSet.
struct IndexFragment {
  uint64_t index_id = 0;
  uint64_t source_sst_id = 0;
  std::string relative_path;
  uint64_t source_row_count = 0;
  uint64_t indexed_put_count = 0;
  uint64_t catalog_generation = 0;
  uint32_t format_version = 0;
  bool usable = false;
  std::array<uint8_t, 32> identity_checksum{};
};
struct IndexFragmentKey {
  uint64_t index_id = 0;
  uint64_t source_sst_id = 0;
};
// A checkpoint is published by the Manifest only after its immutable outcome
// index has been fsynced.  It is the recovery boundary for the retained WAL
// and DecisionLog suffixes.
struct DurableCheckpoint {
  uint64_t checkpoint_seq = 0;
  uint64_t decision_safe_seq = 0;
  uint64_t manifest_generation = 0;
  std::string outcome_index_relative_path;
  std::array<uint8_t, 32> outcome_index_checksum{};
  std::vector<uint64_t> wal_safe_lsns;
};
struct VersionEdit {
  std::vector<SstFileMeta> adds;
  std::vector<uint64_t> deletes;
  std::vector<ColumnSchema> schema_adds;
  std::vector<BlobSegmentMeta> blob_segment_adds;
  std::vector<BlobSegmentMeta> blob_segment_updates;
  std::vector<BlobSegmentKey> blob_segment_deletes;
  std::vector<IndexDefinition> index_adds;
  std::vector<IndexDefinition> index_updates;
  std::vector<uint64_t> index_deletes;
  std::vector<IndexFragment> index_fragment_adds;
  std::vector<IndexFragmentKey> index_fragment_deletes;
  std::optional<DurableCheckpoint> checkpoint;
  std::optional<uint64_t> expected_generation;
};

enum class VersionSetFaultPoint : uint8_t {
  kAfterManifestRename,
};
struct VersionSnapshot {
  uint64_t generation;
  std::vector<SstFileMeta> files;
  std::vector<ColumnSchema> schemas;
  std::vector<BlobSegmentMeta> blob_segments;
  std::vector<IndexDefinition> index_definitions;
  std::vector<IndexFragment> index_fragments;
  DurableCheckpoint checkpoint;
  // Durable high-water mark. IDs below this value are never reused, including
  // after their definitions have been dropped from the live catalog.
  uint64_t next_index_id = 1;
};

class VersionSet {
 public:
  static constexpr uint64_t kMaxManifestBytes = 256ULL * 1024U * 1024U;

  explicit VersionSet(std::string manifest_path);
  Status Open();
  Status ApplyEdit(const VersionEdit& edit);
  Status ApplyEditWithAdmission(
      const VersionEdit& edit,
      const std::function<Status(uint64_t)>& admit_projected_rewrite);
  StatusOr<uint64_t> EstimateSchemaEditRewriteBytes(
      const ColumnSchema& schema_add) const;
  StatusOr<uint64_t> EstimateManifestEditRewriteBytes(
      const VersionEdit& edit);
  StatusOr<uint64_t> EstimateManifestRewriteBytes(
      uint64_t rewrite_count, uint64_t additional_blob_segments) const;
  std::shared_ptr<const VersionSnapshot> Snapshot() const;
  bool requires_reopen() const {
    return requires_reopen_.load(std::memory_order_acquire);
  }
  uint64_t durable_bytes_written() const {
    return durable_bytes_written_.load(std::memory_order_relaxed);
  }
  void SetFaultInjectorForTesting(
      std::function<Status(VersionSetFaultPoint)> injector) {
    fault_injector_ = std::move(injector);
  }
 private:
  Status Persist(const VersionSnapshot& snapshot) const;
  std::string manifest_path_;
  mutable std::mutex mutex_;
  std::shared_ptr<const VersionSnapshot> current_;
  std::atomic<bool> requires_reopen_{false};
  mutable std::atomic<uint64_t> durable_bytes_written_{0};
  std::function<Status(VersionSetFaultPoint)> fault_injector_;
};
}  // namespace cedar
#endif  // CEDAR_STORAGE_VERSION_SET_H_
