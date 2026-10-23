// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STORAGE_FACTS_META_SEQUENCE_COMPAT_H_
#define CEDAR_STORAGE_FACTS_META_SEQUENCE_COMPAT_H_

#include <string_view>

#include "cedar/core/status.h"

namespace cedar::internal {

// One-way gate for the meta sequence v2 format (F17). A binary built with
// CEDAR_META_SEQUENCE_V1_ONLY=ON simulates a pre-v2 release: every attempt to
// read format-2 data fails with an actionable Corruption instead of decoding
// it, and it refuses to upgrade a database to the v2 writer. The default build
// is v2-capable and this gate is disabled.
//
// The runtime flag exists so the normal test build can exercise the exact
// production gate end-to-end. Test hook: a compiled v1-only binary cannot be
// unlocked by it; only a v2-capable build can simulate the gate.
bool MetaSequenceV2ReadDisabled();

// Supported only in v2-capable builds; a compiled v1-only build ignores the
// request and keeps the gate on.
void SetMetaSequenceV2ReadDisabledForTesting(bool disabled);

// The actionable Corruption returned by every v1-only read gate. `detail`
// names the rejected artifact (marker or record); the rest of the message names
// the build switch and the recovery action so an operator can act on it.
Status V1OnlyMetaSequenceCorruption(std::string_view detail);

}  // namespace cedar::internal

#endif  // CEDAR_STORAGE_FACTS_META_SEQUENCE_COMPAT_H_
