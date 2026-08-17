// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef CEDAR_TRANSACTION_DECISION_LOG_H_
#define CEDAR_TRANSACTION_DECISION_LOG_H_

#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/storage/temporal_event.h"
#include "cedar/transaction/system_hlc.h"

namespace cedar {

struct PendingEvent {
  LogicalKey logical_key;
  uint64_t valid_from;
  uint32_t schema_epoch;
  TemporalOperation operation;
  Value value;
  std::optional<BlobRef> blob_ref;

  static PendingEvent Put(LogicalKey key, uint64_t valid_from, uint32_t schema_epoch,
                          Value value) {
    return PendingEvent{std::move(key), valid_from, schema_epoch, TemporalOperation::kPut,
                        std::move(value), std::nullopt};
  }
  static PendingEvent PutBlob(LogicalKey key, uint64_t valid_from, uint32_t schema_epoch,
                              BlobRef reference) {
    return PendingEvent{std::move(key), valid_from, schema_epoch, TemporalOperation::kPut,
                        Value::Binary("", 0), std::move(reference)};
  }
};

struct PrepareRecord {
  uint64_t txn_id;
  uint64_t snapshot_seq;
  std::vector<uint32_t> participant_shards;
  std::vector<PendingEvent> events;
};

struct PrepareReference {
  uint32_t shard_id;
  uint64_t lsn;
  uint32_t checksum;
};

struct DurableCommitWriteEstimate {
  uint64_t prepare_bytes = 0;
  uint64_t decision_bytes = 0;
  uint64_t total_bytes = 0;
};

StatusOr<DurableCommitWriteEstimate> EstimateDurableCommitWriteBytes(
    const std::vector<PrepareRecord>& prepares);

enum class DecisionLogFaultPoint : uint8_t {
  kAfterPartialRecordWrite = 0,
  kAfterRecordWrite = 1,
  kAfterRecordFsync = 2,
  kBeforeRecoveryDirectoryFsync = 3,
};

struct DecisionAppendResult {
  Status status = Status::OK();
  bool may_be_durable = false;
  bool requires_reopen = false;
  uint64_t commit_seq = 0;
  bool fsync_attempted = false;
  uint64_t fsync_latency_ns = 0;
};

struct CommitDecision {
  uint64_t txn_id;
  uint64_t commit_seq;
  SystemHlc system_time_hlc;
  std::vector<PrepareReference> prepares;
};

struct TransactionOutcome {
  uint64_t txn_id = 0;
  uint64_t commit_seq = 0;
  SystemHlc system_time_hlc;
};

// Immutable, checksummed transaction-result index used after the DecisionLog
// prefix has been reclaimed.  It intentionally stores only the durable
// outcome, not prepared payloads already covered by Manifest SSTs.
Status WriteTransactionOutcomeIndex(const std::string& path,
                                    const std::vector<TransactionOutcome>& outcomes,
                                    std::array<uint8_t, 32>* checksum);
StatusOr<std::vector<TransactionOutcome>> ReadTransactionOutcomeIndex(
    const std::string& path, const std::array<uint8_t, 32>& expected_checksum,
    uint64_t expected_checkpoint_seq);

class ShardPrepareLog {
 public:
  ShardPrepareLog(std::string path, uint32_t shard_id);

  Status Open();
  Status Append(const PrepareRecord& record, PrepareReference* reference);
  void SetFaultInjectorForTesting(
      std::function<Status(DecisionLogFaultPoint)> injector) {
    std::lock_guard<std::mutex> lock(mutex_);
    fault_injector_ = std::move(injector);
  }
  bool requires_reopen() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return requires_reopen_;
  }
  Status Read(const PrepareReference& reference, PrepareRecord* record) const;
  StatusOr<uint64_t> EndLsn(const PrepareReference& reference) const;
  // Drops only whole prepare-log segments strictly older than the segment
  // containing safe_lsn.  References in the retained suffix therefore keep
  // their stable segment:offset identity.
  Status TruncateThrough(uint64_t safe_lsn);
  uint32_t shard_id() const { return shard_id_; }
  uint64_t retained_bytes() const {
    return retained_bytes_.load(std::memory_order_relaxed);
  }

 private:
  std::string path_;
  uint32_t shard_id_;
  uint32_t active_segment_id_ = 1;
  std::map<uint64_t, std::pair<uint32_t, PrepareRecord>> records_;
  std::atomic<uint64_t> retained_bytes_{0};
  mutable std::mutex mutex_;
  bool requires_reopen_ = false;
  std::function<Status(DecisionLogFaultPoint)> fault_injector_;
};

class DecisionLog {
 public:
  explicit DecisionLog(std::string path);

  Status Open(uint64_t checkpoint_seq = 0);
  Status AppendCommit(uint64_t txn_id,
                      const std::vector<PrepareReference>& prepares,
                      SystemHlc system_time_hlc,
                      uint64_t* commit_seq);
  DecisionAppendResult AppendCommitWithResult(
      uint64_t txn_id, const std::vector<PrepareReference>& prepares,
      SystemHlc system_time_hlc);
  void SetFaultInjectorForTesting(
      std::function<Status(DecisionLogFaultPoint)> injector) {
    fault_injector_ = std::move(injector);
  }
  const std::vector<CommitDecision>& commits() const { return commits_; }
  uint64_t checkpoint_seq() const { return checkpoint_seq_; }
  std::optional<TransactionOutcome> Resolve(uint64_t txn_id) const;
  // Replaces the on-disk DecisionLog with the suffix after checkpoint_seq.
  // It is invoked only after the Manifest has published the checkpoint that
  // makes the removed prefix recoverable from the outcome index.
  Status CheckpointThrough(uint64_t checkpoint_seq);

 private:
  std::string path_;
  uint64_t checkpoint_seq_ = 0;
  uint64_t next_commit_seq_ = 1;
  std::vector<CommitDecision> commits_;
  bool requires_reopen_ = false;
  std::function<Status(DecisionLogFaultPoint)> fault_injector_;
};

struct RecoveredTransaction {
  uint64_t txn_id;
  uint64_t commit_seq;
  std::vector<TemporalEvent> events;
};

Status RecoverCommittedTransactions(
    const DecisionLog& decisions, const std::vector<ShardPrepareLog*>& shards,
    std::vector<RecoveredTransaction>* recovered);

}  // namespace cedar

#endif  // CEDAR_TRANSACTION_DECISION_LOG_H_
