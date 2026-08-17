// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TRANSACTION_SYSTEM_HLC_H_
#define CEDAR_TRANSACTION_SYSTEM_HLC_H_

#include <cstdint>

namespace cedar {

struct SystemHlc {
  uint64_t physical_us;
  uint32_t logical_counter;

  friend bool operator==(const SystemHlc& left, const SystemHlc& right) {
    return left.physical_us == right.physical_us &&
           left.logical_counter == right.logical_counter;
  }
  friend bool operator<(const SystemHlc& left, const SystemHlc& right) {
    return left.physical_us < right.physical_us ||
           (left.physical_us == right.physical_us &&
            left.logical_counter < right.logical_counter);
  }
};

inline bool operator>(const SystemHlc& left, const SystemHlc& right) {
  return right < left;
}

}  // namespace cedar

#endif  // CEDAR_TRANSACTION_SYSTEM_HLC_H_
