// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_SCHEMA_H_
#define CEDAR_SCHEMA_H_

#include <cstdint>
#include <string>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"

namespace cedar {

enum class PropertyEntityKind : uint8_t { kVertex = 1, kEdge = 2 };

struct PropertyDefinition {
  PropertyId property_id;
  uint32_t schema_epoch = 0;
  std::string name;
  PropertyEntityKind entity_kind = PropertyEntityKind::kVertex;
  PhysicalType physical_type = PhysicalType::kBinary;
  uint64_t blob_threshold_bytes = 0;

  Status Validate() const;
  bool operator==(const PropertyDefinition&) const = default;
};

}  // namespace cedar

#endif  // CEDAR_SCHEMA_H_
