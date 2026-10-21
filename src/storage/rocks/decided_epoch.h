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

// The immutable hand-off from validation/encoding to the single RocksDB writer.
// It contains no request payload references: after construction the sole
// mutable operation is claiming its already encoded batch once. Publication
// (recent-write index / validation cache) is intentionally not stored here:
// the writer receives the caller's live request batches alongside the epoch, so
// no per-mutation copy has to be retained across the predecision window.
// The one thing retained is an order-sensitive digest of the request
// descriptors the decision was computed from (transaction ID, persisted-abort
// flag, and per-batch counts -- no mutation payload). The writer recomputes it
// from the live batches and rejects a mismatch before the WAL write, so a
// same-size misordered/replaced request list cannot publish recent-write or
// validation entries under the wrong commit sequences.
class DecidedEpoch {
 public:
  DecidedEpoch(CommitSeq base_visible_seq, CommitSeq visible_seq_target,
               uint64_t committed_count, bool has_durable_terminal,
               std::unique_ptr<rocksdb::WriteBatch> write_batch,
               uint64_t request_alignment, StoreCommittedGroupResult group_result);
  ~DecidedEpoch();

  DecidedEpoch(DecidedEpoch&&) noexcept = default;
  DecidedEpoch& operator=(DecidedEpoch&&) noexcept = default;
  DecidedEpoch(const DecidedEpoch&) = delete;
  DecidedEpoch& operator=(const DecidedEpoch&) = delete;

  CommitSeq base_visible_seq() const { return base_visible_seq_; }
  CommitSeq visible_seq_target() const { return visible_seq_target_; }
  uint64_t committed_count() const { return committed_count_; }
  bool requires_durable_write() const { return has_durable_terminal_; }
  // Order-sensitive digest of the decided request descriptors; the writer must
  // recompute the same digest from the live batches before writing.
  uint64_t request_alignment() const { return request_alignment_; }
  const StoreCommittedGroupResult& group_result() const { return group_result_; }

  const rocksdb::WriteBatch& batch() const;
  std::unique_ptr<rocksdb::WriteBatch> ClaimBatchForWrite();
  StoreCommittedGroupResult TakeGroupResult();

 private:
  CommitSeq base_visible_seq_;
  CommitSeq visible_seq_target_;
  uint64_t committed_count_ = 0;
  bool has_durable_terminal_ = false;
  uint64_t request_alignment_ = 0;
  std::unique_ptr<rocksdb::WriteBatch> write_batch_;
  StoreCommittedGroupResult group_result_;
};

}  // namespace cedar::internal

#endif  // CEDAR_FACT_DECIDED_EPOCH_H_
