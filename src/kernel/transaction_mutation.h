// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_KERNEL_TRANSACTION_MUTATION_H_
#define CEDAR_KERNEL_TRANSACTION_MUTATION_H_

#include <tuple>

#include "cedar/fact/fact.h"

namespace cedar {

inline bool CanonicalMutationLess(const PendingFactMutation& left,
                                  const PendingFactMutation& right) {
  const auto left_prefix =
      std::tuple{static_cast<uint8_t>(left.ref.family()),
                 left.ref.property_id().value, left.ref.entity_id()};
  const auto right_prefix =
      std::tuple{static_cast<uint8_t>(right.ref.family()),
                 right.ref.property_id().value, right.ref.entity_id()};
  if (left_prefix != right_prefix) return left_prefix < right_prefix;
  return left.valid_from.value > right.valid_from.value;
}

}  // namespace cedar

#endif  // CEDAR_KERNEL_TRANSACTION_MUTATION_H_
