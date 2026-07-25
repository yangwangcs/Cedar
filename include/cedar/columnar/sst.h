// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_COLUMNAR_SST_H_
#define CEDAR_COLUMNAR_SST_H_

#include <functional>
#include <memory>
#include <optional>

#include "cedar/columnar/granule_block.h"
#include "cedar/runtime/resource_profile.h"
#include "cedar/runtime/work_cancellation.h"
#include "cedar/schema/schema_registry.h"
#include "cedar/tcypher/runtime/cancellation.h"
#include "cedar/tcypher/runtime/query_memory.h"

namespace cedar {

class IoGovernor;

class CacheManager;

enum class SstSortOrderId : uint8_t {
  kLogicalKeyValidFromCommitSeq = 1,
};
enum class SstHashAlgorithmId : uint8_t { kBlake3_256 = 1 };
enum class SstEncodingRegistryId : uint8_t { kCedarPageCodecs = 1 };
enum class SstCompressionRegistryId : uint8_t {
  kCedarPageCompression = 1,
};
enum class SstChecksumAlgorithmId : uint8_t { kCrc32c = 1 };

struct SstFormatDescriptor {
  SstSortOrderId sort_order_id =
      SstSortOrderId::kLogicalKeyValidFromCommitSeq;
  SstHashAlgorithmId hash_algorithm_id =
      SstHashAlgorithmId::kBlake3_256;
  SstEncodingRegistryId encoding_registry_id =
      SstEncodingRegistryId::kCedarPageCodecs;
  SstCompressionRegistryId compression_registry_id =
      SstCompressionRegistryId::kCedarPageCompression;
  SstChecksumAlgorithmId checksum_algorithm_id =
      SstChecksumAlgorithmId::kCrc32c;
  bool operator==(const SstFormatDescriptor& other) const {
    return sort_order_id == other.sort_order_id &&
        hash_algorithm_id == other.hash_algorithm_id &&
        encoding_registry_id == other.encoding_registry_id &&
        compression_registry_id == other.compression_registry_id &&
        checksum_algorithm_id == other.checksum_algorithm_id;
  }
  bool operator!=(const SstFormatDescriptor& other) const {
    return !(*this == other);
  }
};

struct SstFileIdentity {
  std::array<uint8_t, 32> bytes{};
  bool operator==(const SstFileIdentity& other) const {
    return bytes == other.bytes;
  }
  bool operator!=(const SstFileIdentity& other) const {
    return !(*this == other);
  }
};

struct SstFileStatistics {
  LogicalKey first_key = LogicalKey::VertexExistence(0);
  LogicalKey last_key = LogicalKey::VertexExistence(0);
  uint64_t min_valid_from = 0;
  uint64_t max_valid_from = 0;
  uint64_t min_commit_seq = 0;
  uint64_t max_commit_seq = 0;
  uint64_t row_count = 0;
  uint64_t put_count = 0;
  uint64_t delete_count = 0;
  uint64_t inline_value_count = 0;
  uint64_t blob_reference_count = 0;
  uint64_t typed_value_count = 0;
  uint64_t nan_count = 0;
  bool typed_min_max_complete = true;
  std::optional<Value> typed_min;
  std::optional<Value> typed_max;
  bool operator==(const SstFileStatistics& other) const {
    return first_key == other.first_key && last_key == other.last_key &&
        min_valid_from == other.min_valid_from &&
        max_valid_from == other.max_valid_from &&
        min_commit_seq == other.min_commit_seq &&
        max_commit_seq == other.max_commit_seq &&
        row_count == other.row_count && put_count == other.put_count &&
        delete_count == other.delete_count &&
        inline_value_count == other.inline_value_count &&
        blob_reference_count == other.blob_reference_count &&
        typed_value_count == other.typed_value_count &&
        nan_count == other.nan_count &&
        typed_min_max_complete == other.typed_min_max_complete &&
        typed_min == other.typed_min && typed_max == other.typed_max;
  }
  bool operator!=(const SstFileStatistics& other) const {
    return !(*this == other);
  }
};

struct SstMetadata {
  BlockPartition partition;
  uint32_t block_count = 0;
  uint64_t max_commit_seq = 0;
  std::vector<BlobHash> blob_refs;
  SstFormatDescriptor format;
  SstFileIdentity identity;
  SstFileStatistics statistics;
  uint32_t statistics_crc32c = 0;
};

struct SstFile {
  std::string bytes;
  BlockPartition partition;
  uint32_t block_count;
  std::vector<BlobHash> blob_refs;
  PageCompressionStats compression;
  SstMetadata metadata;
};
struct SstReadStats {
  uint64_t bytes_read = 0;
  uint64_t blocks_read = 0;
  uint64_t system_pages_read = 0;
  uint64_t value_pages_read = 0;
  uint64_t page_bytes_decoded = 0;
  uint64_t page_bytes_skipped = 0;
  uint64_t page_decode_count = 0;
  uint64_t page_decode_latency_ns = 0;
};
struct SstStreamingWriteStats {
  uint64_t input_blocks_read = 0;
  uint64_t output_blocks_written = 0;
  uint64_t peak_buffered_events = 0;
  uint64_t peak_buffered_bytes = 0;
  uint64_t input_bytes_read = 0;
  uint64_t output_bytes_written = 0;
  uint64_t blob_payload_bytes_read = 0;
  PageCompressionStats compression;
};
struct SstCursorStats {
  uint64_t blocks_read = 0;
  uint64_t pages_read = 0;
  uint64_t events_visited = 0;
  uint64_t peak_buffered_events = 0;
  uint64_t peak_buffered_bytes = 0;
  uint64_t bytes_read = 0;
  uint64_t page_bytes_decoded = 0;
  uint64_t page_bytes_skipped = 0;
  uint64_t page_decode_count = 0;
  uint64_t page_decode_latency_ns = 0;
  uint64_t coalesced_read_ops = 0;
  uint64_t prefetched_blocks = 0;
  uint64_t prefetched_bytes = 0;
};
struct SstOrdinalReadResult {
  uint64_t total_row_count = 0;
  std::vector<std::pair<uint64_t, TemporalEvent>> events;
  std::shared_ptr<void> memory_retention;
};
enum class SstPublicationFaultPoint : uint8_t {
  kAfterFileFsync,
  kAfterRename,
  kAfterDirectoryFsync,
};
struct SstCursorOptions {
  BlockPartition expected_partition;
  std::optional<LogicalKey> exact_key;
  std::shared_ptr<QueryCancellation> cancellation;
  std::shared_ptr<QueryMemoryAccount> memory_account;
  IoGovernor* io_governor = nullptr;
  bool prefetch_next_block = false;
};

class SstEventCursor {
 public:
  SstEventCursor() = default;
  SstEventCursor(SstEventCursor&&) noexcept;
  SstEventCursor& operator=(SstEventCursor&&) noexcept;
  ~SstEventCursor();

  SstEventCursor(const SstEventCursor&) = delete;
  SstEventCursor& operator=(const SstEventCursor&) = delete;

  bool valid() const;
  const TemporalEvent& current() const;
  Status Advance();
  const SstCursorStats& stats() const;
  const Status& terminal_status() const;

 private:
  struct Impl;
  explicit SstEventCursor(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
  friend StatusOr<SstEventCursor> OpenSstEventCursor(
      const std::string&, SstCursorOptions);
};

StatusOr<SstEventCursor> OpenSstEventCursor(
    const std::string& path, SstCursorOptions options);
StatusOr<SstFile> BuildSst(const BlockPartition& partition,
                               const std::vector<TemporalEvent>& events);
StatusOr<std::vector<TemporalEvent>> ReadSst(const std::string& bytes);
StatusOr<std::vector<BlobHash>> ReadSstBlobRefs(const std::string& bytes);
Status WriteSstFile(const std::string& path, const BlockPartition& partition,
                      const std::vector<TemporalEvent>& events,
                      SstFile* written = nullptr,
                      std::function<Status(SstPublicationFaultPoint)>
                          fault_injector = {});
StatusOr<std::vector<TemporalEvent>> ReadSstFile(const std::string& path);
StatusOr<ResourceProfile> EstimateSstDecodeResources(
    const SstFileStatistics& statistics, uint64_t source_file_bytes,
    const ColumnSchema& schema);
// Decodes all events while returning the independently checksummed SST
// metadata needed to validate Manifest ownership during recovery.
StatusOr<std::vector<TemporalEvent>> ReadSstFile(
    const std::string& path, SstMetadata* metadata);
// Verifies header/footer metadata and returns the durable partition and
// BlobRefSet without reading any GranuleBlock payload.
StatusOr<SstMetadata> ReadSstFileMetadata(const std::string& path);
// Reads and verifies only the fixed SST header. Advisory users can reject
// sources whose metadata walk would exceed their callback block quantum.
StatusOr<uint32_t> ReadSstBlockCount(const std::string& path);
// Point reads use the persisted BlockIndex to decode only blocks whose
// complete logical-key range contains `key`.
StatusOr<std::vector<TemporalEvent>> ReadSstCandidatesForKey(
    const std::string& path, const LogicalKey& key,
    CacheManager* cache_manager = nullptr, SstReadStats* stats = nullptr,
    IoGovernor* io_governor = nullptr);
// Uses global source row ordinals from an immutable sidecar. The block index
// and GranuleBlock headers are read first; only blocks containing requested
// ordinals are decoded.
StatusOr<SstOrdinalReadResult> ReadSstEventsAtOrdinals(
    const std::string& path, const BlockPartition& expected_partition,
    const std::vector<uint64_t>& ordinals,
    std::shared_ptr<QueryCancellation> cancellation = nullptr,
    std::shared_ptr<QueryMemoryAccount> memory_account = nullptr,
    SstReadStats* stats = nullptr, IoGovernor* io_governor = nullptr);
// Performs a bounded k-way merge over sorted input SST blocks and writes the
// output incrementally. At most one decoded block per input plus the current
// output block is retained.
StatusOr<SstMetadata> MergeSstFilesStreaming(
    const std::string& output_path, const BlockPartition& partition,
    const std::vector<std::string>& input_paths,
    SstStreamingWriteStats* stats = nullptr,
    std::shared_ptr<WorkCancellation> cancellation = nullptr);
Status VisitSstEvents(
    const std::string& path,
    const std::function<Status(const TemporalEvent&)>& visitor,
    SstCursorStats* stats = nullptr, IoGovernor* io_governor = nullptr,
    bool prefetch_next_block = false);
Status VisitSstEventsForKey(
    const std::string& path, const LogicalKey& key,
    const std::function<Status(const TemporalEvent&)>& visitor,
    SstCursorStats* stats = nullptr,
    CacheManager* cache_manager = nullptr, IoGovernor* io_governor = nullptr);

}  // namespace cedar

#endif  // CEDAR_COLUMNAR_SST_H_
