// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "storage/rocks/decided_epoch.h"

#include <cassert>
#include <utility>

#include <rocksdb/write_batch.h>

namespace cedar::internal {

DecidedEpoch::DecidedEpoch(CommitSeq base_visible_seq,
                           CommitSeq visible_seq_target,
                           uint64_t committed_count,
                           bool has_durable_terminal,
                           std::unique_ptr<rocksdb::WriteBatch> write_batch,
                           StoreCommittedGroupResult group_result,
                           std::vector<PublishedCommit> publications)
    : base_visible_seq_(base_visible_seq),
      visible_seq_target_(visible_seq_target),
      committed_count_(committed_count),
      has_durable_terminal_(has_durable_terminal),
      write_batch_(std::move(write_batch)),
      group_result_(std::move(group_result)),
      publications_(std::move(publications)) {
  assert(!has_durable_terminal_ || write_batch_ != nullptr);
  assert(has_durable_terminal_ || committed_count_ == 0);
  assert(committed_count_ == 0 ||
         visible_seq_target_.value ==
             base_visible_seq_.value + committed_count_);
  // A decision with no durable terminal result is an in-memory validation
  // outcome. It must not carry a batch that a later writer could submit.
  if (!has_durable_terminal_) write_batch_.reset();
}

DecidedEpoch::~DecidedEpoch() = default;

const rocksdb::WriteBatch& DecidedEpoch::batch() const {
  assert(write_batch_ != nullptr);
  return *write_batch_;
}

std::unique_ptr<rocksdb::WriteBatch> DecidedEpoch::ClaimBatchForWrite() {
  return std::move(write_batch_);
}

StoreCommittedGroupResult DecidedEpoch::TakeGroupResult() {
  return std::move(group_result_);
}

}  // namespace cedar::internal
