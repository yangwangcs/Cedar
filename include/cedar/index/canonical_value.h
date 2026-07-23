// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_INDEX_CANONICAL_VALUE_H_
#define CEDAR_INDEX_CANONICAL_VALUE_H_

#include <string>

#include "cedar/core/status.h"
#include "cedar/types/value.h"

namespace cedar {

struct BlobRef;

enum class IndexCanonicalKind : uint8_t {
  kInline = 1,
  kBlobHash = 2,
};

struct IndexCanonicalValue {
  PhysicalType type;
  std::string bytes;
  IndexCanonicalKind kind = IndexCanonicalKind::kInline;
};

StatusOr<IndexCanonicalValue> EncodeIndexCanonicalValue(const Value& value);
StatusOr<IndexCanonicalValue> EncodeIndexBlobHash(const Value& value);
IndexCanonicalValue EncodeIndexBlobHash(const BlobRef& reference);
int CompareIndexCanonicalValues(const IndexCanonicalValue& left,
                                const IndexCanonicalValue& right);

}  // namespace cedar

#endif  // CEDAR_INDEX_CANONICAL_VALUE_H_
