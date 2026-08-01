// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_KERNEL_DATABASE_IMPL_H_
#define CEDAR_KERNEL_DATABASE_IMPL_H_

#include <mutex>

#include "cedar/database.h"
#include "cedar/fact/fact_store.h"

namespace cedar {

class Database::Impl {
 public:
  explicit Impl(DatabaseOptions options)
      : store(FactStoreOptions{std::move(options.path),
                               options.write_buffer_bytes,
                               options.block_cache_bytes,
                               options.blob_threshold_bytes,
                               std::move(options.commit_prewrite_fault_injector_for_testing),
                               std::move(options.commit_fault_injector_for_testing)}) {}

  mutable std::mutex mutex;
  FactStore store;
  bool closed = false;
};

}  // namespace cedar

#endif  // CEDAR_KERNEL_DATABASE_IMPL_H_
