// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BLOB_BLOB_STORE_H_
#define CEDAR_BLOB_BLOB_STORE_H_

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/runtime/work_cancellation.h"

namespace cedar {

struct BlobHash {
  std::array<uint8_t, 32> bytes{};
  bool operator==(const BlobHash& other) const { return bytes == other.bytes; }
  bool operator!=(const BlobHash& other) const { return !(*this == other); }
};

struct BlobHashHasher {
  size_t operator()(const BlobHash& hash) const;
};

BlobHash Blake3Hash(const std::string& raw_bytes);
std::string BlobHashHex(const BlobHash& hash);

struct BlobLocation {
  uint32_t shard_id = 0;
  uint64_t segment_id = 0;
  uint64_t offset = 0;
};

struct BlobRef {
  BlobHash content_hash;
  uint64_t raw_length = 0;
  BlobLocation hint;
};

struct BlobSegmentId {
  uint32_t shard_id = 0;
  uint64_t segment_id = 0;
  bool active = false;
};

struct BlobProtectedWriteEstimate {
  uint64_t segment_bytes = 0;
  uint64_t index_bytes = 0;
  uint64_t active_metadata_bytes = 0;
  uint64_t total_bytes = 0;
  uint64_t descriptors = 0;
  uint64_t metadata_ops = 0;
  uint64_t manifest_rewrites = 0;
  uint64_t additional_manifest_segments = 0;
};

struct BlobGarbageCollectionWriteEstimate {
  uint64_t segment_bytes = 0;
  uint64_t index_bytes = 0;
  uint64_t total_bytes = 0;
  uint64_t descriptors = 0;
  uint64_t metadata_ops = 0;
  uint64_t manifest_rewrites = 0;
};

struct BlobStoreStats {
  uint64_t payload_bytes_read = 0;
  uint64_t payload_bytes_written = 0;
  uint64_t payload_bytes_deduplicated = 0;
  uint64_t lookup_count = 0;
  uint64_t lookup_latency_ns = 0;
  uint64_t gc_live_bytes = 0;
  uint64_t gc_rewritten_bytes = 0;
};

enum class BlobStoreFaultPoint : uint8_t {
  kAfterPartialIndexWrite = 0,
  kAfterIndexFsync = 1,
  kBeforeRecoveryDirectoryFsync = 2,
  kAfterPartialRecordWrite = 3,
  kAfterRecordFsync = 4,
};

class BlobStore {
 public:
  BlobStore(std::string root_path, uint32_t shard_count);
  ~BlobStore();

  BlobStore(const BlobStore&) = delete;
  BlobStore& operator=(const BlobStore&) = delete;

  Status Open();
  void SetFaultInjectorForTesting(
      std::function<Status(BlobStoreFaultPoint)> injector) {
    fault_injector_ = std::move(injector);
  }
  void SetIndexFaultInjectorForTesting(
      std::function<Status(BlobStoreFaultPoint)> injector) {
    SetFaultInjectorForTesting(std::move(injector));
  }
  bool requires_reopen() const {
    return requires_reopen_.load(std::memory_order_acquire);
  }
  StatusOr<BlobProtectedWriteEstimate> EstimateProtectedPutWrites(
      const std::vector<std::string>& raw_blobs,
      uint64_t rotation_target_bytes) const;
  Status EnsureActiveSegments();
  StatusOr<BlobRef> Put(const std::string& raw_bytes);
  StatusOr<std::vector<BlobRef>> PutBatch(
      const std::vector<std::string>& raw_blobs);
  StatusOr<std::string> Get(const BlobRef& reference) const;
  Status RotateActiveSegments();
  // Returns true when a Manifest-owned active segment should be sealed before
  // the next transaction prepares a BlobRef against it.
  StatusOr<bool> ActiveSegmentsNeedRotation(uint64_t target_bytes) const;
  Status CheckpointIndex();
  StatusOr<BlobGarbageCollectionWriteEstimate>
  EstimateGarbageCollectionWrites(
      const std::vector<BlobHash>& live_hashes) const;
  StatusOr<uint64_t> EstimateRelocationBytes(
      const std::vector<BlobHash>& hashes) const;
  Status RelocateLiveHashes(
      const std::vector<BlobHash>& hashes,
      std::shared_ptr<WorkCancellation> cancellation = nullptr);
  Status RelocateLiveHash(const BlobHash& hash);
  std::vector<BlobSegmentId> SegmentIds() const;
  // Writes index tombstones and selects sealed segments that no longer have a
  // mapping.  The caller must publish their Manifest retirement before using
  // DeleteRetiredSegments.
  StatusOr<std::vector<BlobSegmentId>> RetireUnreferencedSealedSegments(
      const std::vector<BlobHash>& live_hashes);
  Status DeleteRetiredSegments(const std::vector<BlobSegmentId>& segments);
  size_t stored_blob_count() const;
  BlobStoreStats stats() const;

 private:
  struct ShardState;
  struct BlobWriteRequest {
    BlobHash hash;
    const std::string* raw_bytes = nullptr;
  };

  uint32_t ShardFor(const BlobHash& hash) const;
  std::string SegmentPath(uint32_t shard_id, uint64_t segment_id) const;
  std::string IndexPath(uint32_t shard_id) const;
  std::string IndexCheckpointPath(uint32_t shard_id) const;
  std::string ActivePath(uint32_t shard_id) const;
  Status OpenShard(uint32_t shard_id);
  Status ValidateShardLocations(uint32_t shard_id) const;
  Status PersistActiveSegment(uint32_t shard_id, uint64_t segment_id) const;
  Status EnsureSegment(uint32_t shard_id, uint64_t segment_id) const;
  Status AppendIndexRecord(uint32_t shard_id, const std::string& record);
  StatusOr<std::vector<BlobRef>> AppendBlobBlocksLocked(
      uint32_t shard_id, const std::vector<BlobWriteRequest>& requests,
      uint64_t* stored_payload_bytes = nullptr);
  Status CheckMutationAllowed() const;
  StatusOr<std::string> ReadAt(const BlobLocation& location,
                               const BlobHash& expected_hash) const;

  std::string root_path_;
  uint32_t shard_count_;
  std::vector<std::unique_ptr<ShardState>> shards_;
  std::atomic<bool> requires_reopen_{false};
  mutable std::atomic<uint64_t> payload_bytes_read_{0};
  std::atomic<uint64_t> payload_bytes_written_{0};
  std::atomic<uint64_t> payload_bytes_deduplicated_{0};
  mutable std::atomic<uint64_t> lookup_count_{0};
  mutable std::atomic<uint64_t> lookup_latency_ns_{0};
  std::atomic<uint64_t> gc_live_bytes_{0};
  std::atomic<uint64_t> gc_rewritten_bytes_{0};
  std::function<Status(BlobStoreFaultPoint)> fault_injector_;
};

}  // namespace cedar

#endif  // CEDAR_BLOB_BLOB_STORE_H_
