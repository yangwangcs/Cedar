// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_FACT_CODEC_H_
#define CEDAR_FACT_FACT_CODEC_H_

#include <cstddef>
#include <string>
#include <string_view>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"

namespace cedar {

inline constexpr size_t kEncodedFactKeyBytes = 32;
inline constexpr size_t kMaxFactValuePayloadBytes = 16U * 1024U * 1024U;
// Format version of the 32-byte fact key (the first byte of every key). The
// compact meta sequence record v2a stores this byte so a reader can reject a key
// layout it cannot rebuild.
inline constexpr uint8_t kFactKeyVersion = 2;

struct DecodedFactKey {
  FactRef ref;
  ValidTime valid_from;
  CommitSeq commit_seq;
};

std::string EncodeFactKey(const FactRef& ref, ValidTime valid_from,
                          CommitSeq commit_seq);
// The `std::string_view` overloads decode in place: they never materialize an
// intermediate copy of the encoded bytes, so callers that already own a
// contiguous record (a borrowed key/value slice, a page buffer, a payload
// region) can validate and decode it without an allocation. Behavior and error
// statuses are identical to the owning `std::string` overloads, which remain
// for callers that build a buffer first.
StatusOr<DecodedFactKey> DecodeFactKey(std::string_view encoded);
StatusOr<DecodedFactKey> DecodeFactKey(const std::string& encoded);

StatusOr<std::string> EncodeFactValue(const FactEvent& event);
StatusOr<FactEvent> DecodeFactValue(const FactRef& ref, ValidTime valid_from,
                                    CommitSeq commit_seq,
                                    std::string_view encoded);
StatusOr<FactEvent> DecodeFactValue(const FactRef& ref, ValidTime valid_from,
                                    CommitSeq commit_seq,
                                    const std::string& encoded);

}  // namespace cedar

#endif  // CEDAR_FACT_FACT_CODEC_H_
