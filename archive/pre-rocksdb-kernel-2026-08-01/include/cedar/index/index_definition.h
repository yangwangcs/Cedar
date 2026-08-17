// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_INDEX_INDEX_DEFINITION_H_
#define CEDAR_INDEX_INDEX_DEFINITION_H_

#include <array>
#include <cstdint>

#include "cedar/types/entity_type.h"

namespace cedar {

constexpr uint32_t kIndexCanonicalEncoding = 1;
// Physical sidecar encodings share the same canonical value ordering.  The
// identifier is persisted in the index definition and selects the posting
// representation, not a different comparison semantics.
constexpr uint32_t kIndexCanonicalEncodingDictionary = 2;
constexpr uint32_t kIndexCanonicalEncodingBitmap = 3;
constexpr uint32_t kIndexCanonicalEncodingSortedDelta = 4;

inline bool IsSupportedIndexCanonicalEncoding(uint32_t encoding) {
  return encoding == kIndexCanonicalEncoding ||
         encoding == kIndexCanonicalEncodingDictionary ||
         encoding == kIndexCanonicalEncodingBitmap ||
         encoding == kIndexCanonicalEncodingSortedDelta;
}

enum IndexCapability : uint32_t {
  kIndexEquality = 1U << 0,
  kIndexOrderedRange = 1U << 1,
  kIndexPrefix = 1U << 2,
};

enum class IndexState : uint8_t {
  kDeclared = 0,
  kBuilding = 1,
  kActive = 2,
  kFailed = 3,
};

struct IndexDefinition {
  uint64_t index_id = 0;
  EntityType entity_type = EntityType::Vertex;
  uint16_t column_id = 0;
  uint32_t schema_epoch = 0;
  uint32_t capabilities = 0;
  uint32_t canonical_encoding_id = 0;
  IndexState state = IndexState::kDeclared;
  uint64_t generation = 0;
  std::array<uint8_t, 32> definition_checksum{};
};

}  // namespace cedar

#endif  // CEDAR_INDEX_INDEX_DEFINITION_H_
