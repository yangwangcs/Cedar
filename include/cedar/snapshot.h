// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_SNAPSHOT_H_
#define CEDAR_SNAPSHOT_H_

#include <functional>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"
#include "cedar/fact/fact_scan.h"

namespace cedar {

class Database;
class Snapshot;
class PreparedQuery;

using SnapshotFactVisitor = std::function<Status(const FactEvent&)>;

struct FactScanSpec {
  PartId part_id;
  FactFamily family = FactFamily::kVertexState;
  PropertyId property_id;
  ValidTime valid_time;
  uint32_t batch_row_limit = 1024;
  std::optional<uint64_t> entity_id_min;
  std::optional<uint64_t> entity_id_max;
  std::optional<ValidTime> event_valid_from_min;
  std::optional<ValidTime> event_valid_from_max;
  std::optional<CommitSeq> event_commit_seq_min;
  std::optional<CommitSeq> event_commit_seq_max;
};

struct FactEventBatch {
  std::vector<FactEvent> events;
};

using FactEventBatchVisitor = std::function<Status(const FactEventBatch&)>;

class Snapshot {
 public:
  ~Snapshot();
  Snapshot(Snapshot&&) noexcept;
  Snapshot& operator=(Snapshot&&) noexcept;

  Snapshot(const Snapshot&) = delete;
  Snapshot& operator=(const Snapshot&) = delete;

  CommitSeq commit_seq() const;
  CommitSeq oldest_readable_seq() const;
  StatusOr<bool> Exists(EntityFact entity, ValidTime valid_time) const;
  // Evaluates a batch against one pinned Cedar snapshot and preserves caller
  // order. The current adapter is correctness-first; Parquet page-grouped
  // MultiGet remains an internal table-reader optimization.
  StatusOr<std::vector<bool>> MultiExists(
      const std::vector<EntityFact>& entities, ValidTime valid_time) const;
  StatusOr<std::optional<Value>> Get(PropertyFact property,
                                     ValidTime valid_time) const;
  Status Scan(FactFamily family, PropertyId property_id,
              const SnapshotFactVisitor& visitor) const;
  Status ScanFamily(FactFamily family, const SnapshotFactVisitor& visitor) const;
  Status EventScan(const FactScanSpec& spec,
                   const FactEventBatchVisitor& visitor) const;
  Status StateScan(const FactScanSpec& spec,
                   const FactEventBatchVisitor& visitor) const;
  Status EventColumnarScan(const FactScanSpec& spec,
                           const std::vector<FactColumnId>& projection,
                           const FactColumnarBatchVisitor& visitor) const;
  Status StateColumnarScan(const FactScanSpec& spec,
                           const std::vector<FactColumnId>& projection,
                           const FactColumnarBatchVisitor& visitor) const;

 private:
  class State;
  explicit Snapshot(std::unique_ptr<State> state);
  bool BelongsToDatabase(const void* database_identity) const;

  std::unique_ptr<State> state_;

  friend class Database;
  friend class PreparedQuery;
};

}  // namespace cedar

#endif  // CEDAR_SNAPSHOT_H_
