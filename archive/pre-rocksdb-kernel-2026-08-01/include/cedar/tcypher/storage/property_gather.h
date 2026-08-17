// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_STORAGE_PROPERTY_GATHER_H_
#define CEDAR_TCYPHER_STORAGE_PROPERTY_GATHER_H_

#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/storage/temporal_event.h"
#include "cedar/tcypher/runtime/column_batch.h"
#include "cedar/tcypher/storage/temporal_scan.h"

namespace cedar {

struct PropertyGatherSpec {
  std::vector<uint32_t> column_ids;
  uint64_t valid_time = 0;
  uint64_t snapshot_seq = 0;
  std::vector<uint32_t> schema_epochs;
  std::vector<std::shared_ptr<const std::vector<BlobPredicateProbe>>>
      blob_predicate_probes;
  std::optional<uint32_t> valid_time_column;
  std::function<void(uint64_t)> payload_copy_observer;
};

class PinnedPropertyGatherCursor {
 public:
  PinnedPropertyGatherCursor() = default;
  PinnedPropertyGatherCursor(PinnedPropertyGatherCursor&&) noexcept;
  PinnedPropertyGatherCursor& operator=(PinnedPropertyGatherCursor&&) noexcept;
  ~PinnedPropertyGatherCursor();

  PinnedPropertyGatherCursor(const PinnedPropertyGatherCursor&) = delete;
  PinnedPropertyGatherCursor& operator=(const PinnedPropertyGatherCursor&) = delete;

  Status Advance(uint32_t max_lookups, uint32_t* completed_lookups);
  bool done() const;
  Status Finish(ColumnBatch* gathered);

 private:
  struct Impl;
  explicit PinnedPropertyGatherCursor(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
  friend StatusOr<PinnedPropertyGatherCursor> OpenPinnedPropertyGather(
      ColumnBatch, PinnedTemporalScanSources, TemporalScanSpec,
      PropertyGatherSpec);
};

StatusOr<PinnedPropertyGatherCursor> OpenPinnedPropertyGather(
    ColumnBatch entities, PinnedTemporalScanSources property_sources,
    TemporalScanSpec scan_spec, PropertyGatherSpec spec);

Status BatchGatherProperties(const ColumnBatch& entities,
                             const std::vector<TemporalEvent>& property_candidates,
                             const PropertyGatherSpec& spec, ColumnBatch* gathered);
Status BatchGatherProperties(
    const ColumnBatch& entities,
    const PinnedTemporalScanSources& property_sources,
    TemporalScanSpec scan_spec, const PropertyGatherSpec& spec,
    ColumnBatch* gathered);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_STORAGE_PROPERTY_GATHER_H_
