// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_FACT_CODEC_H_
#define CEDAR_FACT_FACT_CODEC_H_

#include <cstddef>
#include <string>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"

namespace cedar {

inline constexpr size_t kEncodedFactKeyBytes = 28;
inline constexpr size_t kMaxFactValuePayloadBytes = 16U * 1024U * 1024U;

struct DecodedFactKey {
  FactRef ref;
  ValidTime valid_from;
  CommitSeq commit_seq;
};

std::string EncodeFactKey(const FactRef& ref, ValidTime valid_from,
                          CommitSeq commit_seq);
StatusOr<DecodedFactKey> DecodeFactKey(const std::string& encoded);

StatusOr<std::string> EncodeFactValue(const FactEvent& event);
StatusOr<FactEvent> DecodeFactValue(const FactRef& ref, ValidTime valid_from,
                                    CommitSeq commit_seq,
                                    const std::string& encoded);

}  // namespace cedar

#endif  // CEDAR_FACT_FACT_CODEC_H_
