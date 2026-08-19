// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_FACT_SCAN_H_
#define CEDAR_FACT_FACT_SCAN_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <variant>
#include <vector>

#include "cedar/core/status.h"

namespace cedar {

enum class FactColumnId : uint8_t {
  kPartId = 3,
  kFactFamily = 4,
  kPropertyId = 5,
  kEntityId = 6,
  kValidFrom = 7,
  kCedarCommitSeq = 8,
  kStorageSequence = 9,
  kOperation = 10,
  kSchemaEpoch = 11,
  kPhysicalType = 12,
  kBoolValue = 13,
  kInt32Value = 14,
  kInt64Value = 15,
  kFloat32Value = 16,
  kFloat64Value = 17,
  kTimestamp64Value = 18,
  kBytesValue = 19,
  kSourcePartId = 20,
  kSourceVertexId = 21,
  kTargetPartId = 22,
  kTargetVertexId = 23,
  kEdgeType = 24,
};

using FactColumnVector = std::variant<
    std::vector<uint32_t>, std::vector<uint64_t>, std::vector<int32_t>,
    std::vector<int64_t>, std::vector<float>, std::vector<double>,
    std::vector<uint8_t>, std::vector<std::string>>;

struct FactColumn {
  FactColumnId id;
  FactColumnVector values;
  std::vector<uint8_t> present;
};

struct FactColumnarBatch {
  std::vector<FactColumn> columns;

  size_t row_count() const {
    return columns.empty() ? 0 : columns.front().present.size();
  }
};

using FactColumnarBatchVisitor = std::function<Status(const FactColumnarBatch&)>;

}  // namespace cedar

#endif  // CEDAR_FACT_FACT_SCAN_H_
