// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef CEDAR_TRANSACTION_SHARD_DIRECTORY_H_
#define CEDAR_TRANSACTION_SHARD_DIRECTORY_H_

#include <cstdint>

#include "cedar/transaction/logical_key.h"

namespace cedar {

// The persisted shard count and seed make assignment stable across recovery.
class ShardDirectory {
 public:
  ShardDirectory(uint32_t shard_count, uint64_t hash_seed);

  uint32_t shard_count() const { return shard_count_; }
  uint64_t hash_seed() const { return hash_seed_; }
  uint32_t ShardFor(const LogicalKey& key) const;

 private:
  uint32_t shard_count_;
  uint64_t hash_seed_;
};

}  // namespace cedar

#endif  // CEDAR_TRANSACTION_SHARD_DIRECTORY_H_
