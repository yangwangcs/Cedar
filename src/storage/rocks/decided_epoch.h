// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_DECIDED_EPOCH_H_
#define CEDAR_FACT_DECIDED_EPOCH_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "storage/facts/fact_store.h"

namespace rocksdb {
class WriteBatch;
}  // namespace rocksdb

namespace cedar::internal {

// The immutable hand-off from validation/encoding to the single RocksDB
// writer. It contains no request payload references: after construction the
// sole mutable operation is claiming its already encoded batch once.
class DecidedEpoch {
 public:
  struct PublishedCommit {
    CommitSeq commit_seq;
    std::vector<PendingFactMutation> mutations;
  };

  DecidedEpoch(CommitSeq base_visible_seq, CommitSeq visible_seq_target,
               uint64_t committed_count, bool has_durable_terminal,
               std::unique_ptr<rocksdb::WriteBatch> write_batch,
               StoreCommittedGroupResult group_result,
               std::vector<PublishedCommit> publications = {});
  ~DecidedEpoch();

  DecidedEpoch(DecidedEpoch&&) noexcept = default;
  DecidedEpoch& operator=(DecidedEpoch&&) noexcept = default;
  DecidedEpoch(const DecidedEpoch&) = delete;
  DecidedEpoch& operator=(const DecidedEpoch&) = delete;

  CommitSeq base_visible_seq() const { return base_visible_seq_; }
  CommitSeq visible_seq_target() const { return visible_seq_target_; }
  uint64_t committed_count() const { return committed_count_; }
  bool requires_durable_write() const { return has_durable_terminal_; }
  const StoreCommittedGroupResult& group_result() const { return group_result_; }
  const std::vector<PublishedCommit>& publications() const {
    return publications_;
  }

  const rocksdb::WriteBatch& batch() const;
  std::unique_ptr<rocksdb::WriteBatch> ClaimBatchForWrite();
  StoreCommittedGroupResult TakeGroupResult();

 private:
  CommitSeq base_visible_seq_;
  CommitSeq visible_seq_target_;
  uint64_t committed_count_ = 0;
  bool has_durable_terminal_ = false;
  std::unique_ptr<rocksdb::WriteBatch> write_batch_;
  StoreCommittedGroupResult group_result_;
  std::vector<PublishedCommit> publications_;
};

}  // namespace cedar::internal

#endif  // CEDAR_FACT_DECIDED_EPOCH_H_
