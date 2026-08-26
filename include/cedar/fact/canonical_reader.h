// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_CANONICAL_READER_H_
#define CEDAR_FACT_CANONICAL_READER_H_

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/fact/read_spec.h"

namespace cedar {

// Snapshot-bound read seam. Implementations may be an LSM adapter, Cedar Parquet, or
// an in-memory test adapter, but no storage handle crosses this interface.
class CanonicalFactReader {
 public:
  virtual ~CanonicalFactReader() = default;

  virtual StatusOr<std::optional<FactEvent>> ReadStateAt(
      const FactReadSpec& spec, ValidTime valid_time,
      CommitSeq snapshot_seq) const = 0;
  virtual Status ReadStateRows(
      const CanonicalStateReadSpec& spec,
      const CanonicalStateBatchVisitor& visitor) const = 0;
  virtual Status ReadEvents(const FactReadSpec& spec,
                            const CanonicalFactBatchVisitor& visitor) const = 0;
  virtual Status ReadColumnar(
      const FactReadSpec& spec,
      const CanonicalColumnarBatchVisitor& visitor) const = 0;
  virtual StatusOr<std::vector<FactEvent>> ReadExact(
      const std::vector<std::string>& encoded_keys) const = 0;
};

}  // namespace cedar

#endif  // CEDAR_FACT_CANONICAL_READER_H_
