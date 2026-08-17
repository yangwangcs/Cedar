// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "cedar/transaction/shard_directory.h"

#include <stdexcept>

namespace cedar {

ShardDirectory::ShardDirectory(uint32_t shard_count, uint64_t hash_seed)
    : shard_count_(shard_count), hash_seed_(hash_seed) {
  if (shard_count_ == 0) {
    throw std::invalid_argument("Cedar requires at least one storage shard");
  }
}

uint32_t ShardDirectory::ShardFor(const LogicalKey& key) const {
  return static_cast<uint32_t>(key.StableHash(hash_seed_) % shard_count_);
}

}  // namespace cedar
