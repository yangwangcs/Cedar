// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FORMAT_H_
#define CEDAR_FORMAT_H_

#include <cstdint>

namespace cedar {

// Stored under meta/format/current. Older or future values are never upgraded
// in place and must be opened by a matching Cedar binary.
inline constexpr uint32_t kCedarFormatVersion = 2;
inline constexpr uint32_t kCedarProtocolCapabilitySingleWalAppendCommit = 1;

}  // namespace cedar

#endif  // CEDAR_FORMAT_H_
