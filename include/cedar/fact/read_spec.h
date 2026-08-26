// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_READ_SPEC_H_
#define CEDAR_FACT_READ_SPEC_H_

#include <algorithm>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"
#include "cedar/fact/fact_scan.h"
#include "cedar/query/types.h"

namespace cedar {

struct FactEventBatch {
  std::vector<FactEvent> events;
};

using FactEventBatchVisitor = std::function<Status(const FactEventBatch&)>;
using CanonicalFactBatchVisitor = FactEventBatchVisitor;
using CanonicalColumnarBatchVisitor = FactColumnarBatchVisitor;

enum class PartScopeKind : uint8_t { kExact, kSet, kAll };

struct PartScope {
  PartScopeKind kind = PartScopeKind::kExact;
  std::vector<PartId> parts;

  static PartScope Exact(PartId part) { return {PartScopeKind::kExact, {part}}; }

  static PartScope Set(std::vector<PartId> parts) {
    std::sort(parts.begin(), parts.end(),
              [](PartId lhs, PartId rhs) { return lhs.value < rhs.value; });
    parts.erase(std::unique(parts.begin(), parts.end()), parts.end());
    return {PartScopeKind::kSet, std::move(parts)};
  }

  static PartScope All() { return {PartScopeKind::kAll, {}}; }

  Status Validate() const {
    if (kind == PartScopeKind::kAll) {
      return parts.empty() ? Status::OK()
                           : Status::InvalidArgument("part scope", "wildcard cannot carry parts");
    }
    if (parts.empty()) {
      return Status::InvalidArgument("part scope", "empty part set");
    }
    if (kind == PartScopeKind::kExact && parts.size() != 1) {
      return Status::InvalidArgument("part scope", "exact scope requires one part");
    }
    if (!std::is_sorted(parts.begin(), parts.end(),
                        [](PartId lhs, PartId rhs) { return lhs.value < rhs.value; })) {
      return Status::InvalidArgument("part scope", "parts must be sorted");
    }
    if (std::adjacent_find(parts.begin(), parts.end()) != parts.end()) {
      return Status::InvalidArgument("part scope", "parts must be unique");
    }
    return Status::OK();
  }

  bool Contains(PartId part) const {
    if (kind == PartScopeKind::kAll) return true;
    return std::binary_search(parts.begin(), parts.end(), part,
                              [](PartId lhs, PartId rhs) {
                                return lhs.value < rhs.value;
                              });
  }
};

struct EntityRange {
  std::optional<uint64_t> min;
  std::optional<uint64_t> max_exclusive;

  Status Validate() const {
    if (min.has_value() && max_exclusive.has_value() && *min >= *max_exclusive) {
      return Status::InvalidArgument("entity range", "min must be less than max_exclusive");
    }
    return Status::OK();
  }
};

struct CommitSeqRange {
  CommitSeq from;
  CommitSeq to;

  Status Validate() const {
    if (from.value > to.value) {
      return Status::InvalidArgument("system time range",
                                     "from must not exceed to");
    }
    return Status::OK();
  }

  bool Contains(CommitSeq sequence) const {
    return sequence.value >= from.value && sequence.value <= to.value;
  }
};

struct FactReadSpec {
  PartScope part_scope = PartScope::Exact(PartId{0});
  FactFamily family = FactFamily::kVertexState;
  PropertyId property_id;
  EntityRange entity_range;
  std::optional<ValidTime> valid_from_min;
  std::optional<ValidTime> valid_from_max;
  std::optional<CommitSeq> commit_seq_min;
  std::optional<CommitSeq> commit_seq_max;
  bool preserve_predecessor_context = false;
  std::vector<FactColumnId> projection;
  uint32_t batch_row_limit = 1024;
  // Total output cap. Unlike batch_row_limit this is not a callback batch
  // size and remains unset unless a planner proves LIMIT pushdown safe.
  std::optional<uint64_t> max_rows;

  Status Validate() const {
    Status status = part_scope.Validate();
    if (!status.ok()) return status;
    status = entity_range.Validate();
    if (!status.ok()) return status;
    if (batch_row_limit == 0) {
      return Status::InvalidArgument("fact read spec", "zero batch row limit");
    }
    if (valid_from_min.has_value() && valid_from_max.has_value() &&
        valid_from_min->value > valid_from_max->value) {
      return Status::InvalidArgument("fact read spec", "reversed valid-time range");
    }
    if (commit_seq_min.has_value() && commit_seq_max.has_value() &&
        commit_seq_min->value > commit_seq_max->value) {
      return Status::InvalidArgument("fact read spec", "reversed commit range");
    }
    std::unordered_set<uint8_t> seen;
    for (FactColumnId column : projection) {
      if (!seen.insert(static_cast<uint8_t>(column)).second) {
        return Status::InvalidArgument("fact read spec", "duplicate projection column");
      }
    }
    return Status::OK();
  }
};

// A state-row read is distinct from an event read: max_rows counts visible
// entities after version-chain reduction, not raw canonical events.
struct CanonicalStateReadSpec {
  FactReadSpec facts;
  ValidTime valid_time;
  CommitSeq snapshot_seq;
  std::optional<uint64_t> max_rows;
};

struct CanonicalStateRow {
  FactRef ref;
  ValidTimeInterval effective;
  CommitSeq commit_seq;
  std::optional<Value> value;
};

using CanonicalStateBatchVisitor =
    std::function<Status(const std::vector<CanonicalStateRow>&)>;

struct ExecutionScope {
  PartScope part_scope = PartScope::All();
  std::optional<CommitSeq> system_time_as_of;
  std::optional<CommitSeqRange> system_time_range;
  std::optional<ValidTimeInterval> valid_time;
  std::string graph;
  CommitSeq snapshot_seq;

  Status Validate() const {
    Status status = part_scope.Validate();
    if (!status.ok()) return status;
    if (valid_time.has_value()) {
      status = valid_time->Validate();
      if (!status.ok()) return status;
    }
    if (system_time_as_of.has_value() && system_time_range.has_value()) {
      return Status::InvalidArgument("execution scope",
                                     "system-time as-of and range are exclusive");
    }
    if (system_time_as_of.has_value() && snapshot_seq.value != 0 &&
        system_time_as_of->value > snapshot_seq.value) {
      return Status::InvalidArgument("execution scope", "system time exceeds snapshot");
    }
    if (system_time_range.has_value()) {
      status = system_time_range->Validate();
      if (!status.ok()) return status;
      if (snapshot_seq.value != 0 &&
          system_time_range->from.value > snapshot_seq.value) {
        return Status::InvalidArgument("execution scope",
                                       "system-time range starts after snapshot");
      }
    }
    return Status::OK();
  }
};

}  // namespace cedar

#endif  // CEDAR_FACT_READ_SPEC_H_
