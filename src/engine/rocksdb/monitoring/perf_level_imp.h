//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "port/port.h"
#include "rocksdb/perf_level.h"

namespace ROCKSDB_NAMESPACE {

// Inline (rather than extern) so that every translation unit sees the
// constant initializer.  With only an extern declaration, GCC emits a weak
// reference to the thread-local init function in every TU that reads the
// variable; because the defining TU constant-initializes it, no init function
// exists and the weak reference stays undefined.  When the linker relaxes the
// TLS address computation, UBSan can then mistake the stale guard condition
// for a null pointer load (see PerfStepTimer).  Making the definition visible
// avoids the external TLS init guard entirely without changing semantics.
inline thread_local PerfLevel perf_level = kEnableCount;

}  // namespace ROCKSDB_NAMESPACE
