// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STORAGE_ROCKS_ROCKS_ADAPTER_H_
#define CEDAR_STORAGE_ROCKS_ROCKS_ADAPTER_H_

// This is the private storage-engine boundary. Its implementation owns all
// direct RocksDB calls while FactStore remains the Cedar storage contract
// consumed by the kernel.
#include "storage/facts/fact_store.h"

#endif  // CEDAR_STORAGE_ROCKS_ROCKS_ADAPTER_H_
