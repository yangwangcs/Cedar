// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_STORAGE_TEMPORAL_SCAN_H_
#define CEDAR_TCYPHER_STORAGE_TEMPORAL_SCAN_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "cedar/columnar/sst.h"
#include "cedar/core/status.h"
#include "cedar/storage/temporal_event.h"
#include "cedar/storage/temporal_memtable.h"
#include "cedar/storage/version_set.h"
#include "cedar/tcypher/runtime/column_batch.h"

namespace cedar {

// Stable system-column order for scan operators. The value column is emitted
// only after temporal resolution, so a DELETE never appears as a null value.
enum TemporalFactColumn : uint32_t {
  kEntityType = 0,
  kEntityId = 1,
  kTargetId = 2,
  kEdgeId = 3,
  kEdgeType = 4,
  kColumnId = 5,
  kValidFrom = 6,
  kCommitSeq = 7,
  kOperation = 8,
  kValue = 9,
};

enum class RawTemporalOrder : uint8_t {
  kValidTime,
  kCommitSequence,
};

struct BlobPredicateProbe {
  BlobHash content_hash;
  uint64_t raw_length = 0;
  Value literal = Value::Binary("");
  std::shared_ptr<void> memory_lease;
};

struct TemporalScanSpec {
  uint64_t valid_time = 0;
  uint64_t snapshot_seq = 0;
  uint32_t batch_capacity = kTcypherStandardBatchCapacity;
  std::optional<EntityType> entity_type;
  std::optional<LogicalKeyKind> key_kind;
  std::optional<uint16_t> edge_type;
  std::optional<uint16_t> column_id;
  std::optional<uint32_t> schema_epoch;
  std::optional<LogicalKey> exact_key;
  std::shared_ptr<const std::set<uint64_t>> allowed_candidate_entity_ids;
  std::shared_ptr<QueryCancellation> cancellation;
  std::shared_ptr<QueryMemoryAccount> memory_account;
  std::function<void()> open_observer;
  std::function<void(uint64_t, uint64_t, uint64_t)> stats_observer;
  std::function<void(uint64_t)> page_read_observer;
  std::function<void(uint64_t, uint64_t, uint64_t, uint64_t, uint64_t)>
      storage_stats_observer;
  std::function<StatusOr<std::optional<Value>>(const TemporalEvent&)>
      blob_materializer;
  std::shared_ptr<const std::vector<BlobPredicateProbe>>
      blob_predicate_probes;
  std::function<void()> blob_ref_observer;
  std::function<void()> blob_read_observer;
  std::function<StatusOr<bool>(const TemporalEvent&)> event_filter;
  std::optional<uint64_t> valid_time_start;
  std::optional<uint64_t> valid_time_end;
  bool raw_events = false;
  RawTemporalOrder raw_order = RawTemporalOrder::kValidTime;
  // Point gathers normally hide DELETE facts. Interval alignment needs the
  // selected tombstone's valid-time boundary while still treating its value as
  // absent, so it opts in explicitly.
  bool retain_selected_tombstone = false;
};

struct PinnedSstSource {
  SstFileMeta metadata;
  std::string path;
};

struct PinnedTemporalScanSources {
  std::vector<std::shared_ptr<const TemporalMemTable>> memtables;
  std::vector<PinnedSstSource> ssts;
  std::shared_ptr<const std::vector<TemporalEvent>> session_overlay;
  std::optional<uint64_t> base_snapshot_seq;
  std::optional<uint64_t> overlay_snapshot_seq;
  IoGovernor* io_governor = nullptr;
  bool prefetch_sst_blocks = false;
};

struct TemporalScanCursorStats {
  uint64_t source_count = 0;
  uint64_t events_visited = 0;
  uint64_t duplicate_events_suppressed = 0;
  uint64_t sst_blocks_read = 0;
  uint64_t sst_pages_read = 0;
  uint64_t source_peak_buffered_events = 0;
  uint64_t source_peak_buffered_bytes = 0;
  uint64_t max_sst_cursor_buffered_events = 0;
  uint64_t peak_retained_events = 0;
  uint64_t sst_bytes_read = 0;
  uint64_t page_bytes_decoded = 0;
  uint64_t page_bytes_skipped = 0;
  uint64_t page_decode_count = 0;
  uint64_t page_decode_latency_ns = 0;
};

// A raw temporal fact preserves the storage identity and version boundary
// without materializing an application value. Range/path runtimes consume
// this stream to derive intervals one logical timeline at a time.
struct RawTemporalFact {
  EntityType entity_type = EntityType::Vertex;
  LogicalKeyKind key_kind = LogicalKeyKind::kExistence;
  uint64_t entity_id = 0;
  uint64_t target_id = 0;
  uint64_t edge_id = 0;
  uint16_t edge_type = 0;
  uint16_t column_id = 0;
  uint64_t valid_from = 0;
  uint64_t commit_seq = 0;
  TemporalOperation operation = TemporalOperation::kPut;
};

class TemporalScanCursor {
 public:
  TemporalScanCursor() = default;
  TemporalScanCursor(TemporalScanCursor&&) noexcept;
  TemporalScanCursor& operator=(TemporalScanCursor&&) noexcept;
  ~TemporalScanCursor();

  TemporalScanCursor(const TemporalScanCursor&) = delete;
  TemporalScanCursor& operator=(const TemporalScanCursor&) = delete;

  Status NextMorsel(ColumnBatch* batch);
  const TemporalScanCursorStats& stats() const;
  const Status& terminal_status() const;

 private:
  struct Impl;
  explicit TemporalScanCursor(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
  friend StatusOr<TemporalScanCursor> OpenPinnedTemporalScan(
      PinnedTemporalScanSources, const TemporalScanSpec&);
};

StatusOr<TemporalScanCursor> OpenPinnedTemporalScan(
    PinnedTemporalScanSources sources, const TemporalScanSpec& spec);
Status VisitPinnedRawTemporalFacts(
    PinnedTemporalScanSources sources, TemporalScanSpec spec,
    const std::function<Status(const RawTemporalFact&)>& visitor);
StatusOr<std::optional<uint64_t>> FindNextPinnedValidBoundary(
    const PinnedTemporalScanSources& sources, TemporalScanSpec spec,
    const LogicalKey& key, uint64_t after_valid_from);

// The source can be an all-source merger over MemTables and SSTs. It exposes
// logical facts only, not SST row locations or legacy key representations.
StatusOr<TemporalScanCursor> OpenTemporalScan(
    const std::vector<TemporalEvent>& candidates, const TemporalScanSpec& spec);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_STORAGE_TEMPORAL_SCAN_H_
