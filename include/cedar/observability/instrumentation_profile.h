// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_OBSERVABILITY_INSTRUMENTATION_PROFILE_H_
#define CEDAR_OBSERVABILITY_INSTRUMENTATION_PROFILE_H_

namespace cedar {

inline constexpr char kInstrumentationProfileTier0Tier1[] = "tier0-tier1";
inline constexpr char kInstrumentationProfileTier0Minimal[] = "tier0-minimal";

#ifndef CEDAR_MINIMAL_INSTRUMENTATION
#define CEDAR_MINIMAL_INSTRUMENTATION 0
#endif

inline constexpr bool kCedarMinimalInstrumentation =
    CEDAR_MINIMAL_INSTRUMENTATION != 0;

inline constexpr const char* CedarInstrumentationProfileId() {
  return kCedarMinimalInstrumentation ? kInstrumentationProfileTier0Minimal
                                      : kInstrumentationProfileTier0Tier1;
}

}  // namespace cedar

#endif  // CEDAR_OBSERVABILITY_INSTRUMENTATION_PROFILE_H_
