// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TYPES_ENTITY_TYPE_H_
#define CEDAR_TYPES_ENTITY_TYPE_H_

#include <cstdint>

namespace cedar {

enum class EntityType : uint8_t {
  Vertex = 0,
  EdgeOut = 1,
  EdgeIn = 2,
};

}  // namespace cedar

#endif  // CEDAR_TYPES_ENTITY_TYPE_H_
