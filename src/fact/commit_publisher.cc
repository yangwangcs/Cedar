// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "fact/commit_publisher.h"

#include <utility>

#include <rocksdb/write_batch.h>

#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"

namespace cedar::internal {

Status AppendCandidateToWriteBatch(const CandidateCommit& candidate,
                                   rocksdb::ColumnFamilyHandle* facts_cf,
                                   rocksdb::ColumnFamilyHandle* meta_cf,
                                   rocksdb::WriteBatch* write_batch) {
  if (candidate.batch == nullptr || facts_cf == nullptr || meta_cf == nullptr ||
      write_batch == nullptr) {
    return Status::InvalidArgument("commit publisher", "missing commit output");
  }
  if (candidate.fact_keys.size() != candidate.batch->mutations.size()) {
    return Status::InvalidArgument("commit publisher",
                                   "fact key count differs from mutations");
  }
  std::vector<std::string> sequence_fact_keys = candidate.fact_keys;
  sequence_fact_keys.reserve(candidate.fact_keys.size() +
                             candidate.batch->edge_identities.size());
  for (size_t index = 0; index < candidate.batch->mutations.size(); ++index) {
    const PendingFactMutation& mutation = candidate.batch->mutations[index];
    const std::string expected_key =
        EncodeFactKey(mutation.ref, mutation.valid_from, candidate.commit_seq);
    if (expected_key.empty() || expected_key != candidate.fact_keys[index]) {
      return Status::InvalidArgument("commit publisher", "invalid fact key");
    }
    const FactEvent event{mutation.ref, mutation.valid_from, candidate.commit_seq,
                          mutation.operation, mutation.schema_epoch, mutation.value};
    const auto encoded_value = EncodeFactValue(event);
    if (!encoded_value.ok()) return encoded_value.status();
    write_batch->Put(facts_cf, candidate.fact_keys[index],
                     encoded_value.ValueOrDie());
  }
  for (const EdgeIdentity& identity : candidate.batch->edge_identities) {
    const FactRef identity_ref(identity.home_part_id, FactFamily::kEdgeIdentity,
                               PropertyId{}, identity.edge_id.value);
    const std::string identity_key =
        EncodeFactKey(identity_ref, ValidTime{0}, candidate.commit_seq);
    if (identity_key.empty()) {
      return Status::InvalidArgument("commit publisher", "invalid edge identity fact key");
    }
    sequence_fact_keys.push_back(identity_key);
    const FactEvent identity_event{
        identity_ref, ValidTime{0}, candidate.commit_seq, FactOperation::kPut,
        0, std::nullopt, identity};
    const auto encoded_identity_fact = EncodeFactValue(identity_event);
    if (!encoded_identity_fact.ok()) return encoded_identity_fact.status();
    write_batch->Put(facts_cf, identity_key,
                     encoded_identity_fact.ValueOrDie());

    const auto key = EncodeEdgeIdentityMetaKey(identity.edge_ref());
    const auto value = EncodeEdgeIdentity(identity);
    if (!key.ok()) return key.status();
    if (!value.ok()) return value.status();
    write_batch->Put(meta_cf, key.ValueOrDie(), value.ValueOrDie());
  }
  const auto transaction_key = EncodeTransactionMetaKey(candidate.batch->txn_id);
  const auto sequence_key = EncodeSequenceMetaKey(candidate.commit_seq);
  const auto outcome = EncodeTransactionOutcome(TransactionOutcomeRecord{
      candidate.batch->txn_id, candidate.commit_seq, TransactionOutcome::kCommitted});
  const auto sequence = EncodeSequenceRecord(SequenceRecord{
      candidate.commit_seq, candidate.batch->txn_id, candidate.batch->system_hlc,
      std::move(sequence_fact_keys)});
  if (!transaction_key.ok()) return transaction_key.status();
  if (!sequence_key.ok()) return sequence_key.status();
  if (!outcome.ok()) return outcome.status();
  if (!sequence.ok()) return sequence.status();
  write_batch->Put(meta_cf, transaction_key.ValueOrDie(), outcome.ValueOrDie());
  write_batch->Put(meta_cf, sequence_key.ValueOrDie(), sequence.ValueOrDie());
  return Status::OK();
}

}  // namespace cedar::internal
