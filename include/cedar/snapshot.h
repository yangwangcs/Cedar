// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_SNAPSHOT_H_
#define CEDAR_SNAPSHOT_H_

#include <functional>
#include <memory>
#include <optional>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"

namespace cedar {

class Database;

using SnapshotFactVisitor = std::function<Status(const FactEvent&)>;

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
  StatusOr<std::optional<Value>> Get(PropertyFact property,
                                     ValidTime valid_time) const;
  Status Scan(FactFamily family, PropertyId property_id,
              const SnapshotFactVisitor& visitor) const;

 private:
  class State;
  explicit Snapshot(std::unique_ptr<State> state);

  std::unique_ptr<State> state_;

  friend class Database;
};

}  // namespace cedar

#endif  // CEDAR_SNAPSHOT_H_
