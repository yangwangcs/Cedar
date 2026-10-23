// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "storage/rocks/commit_publisher.h"

#include <string_view>
#include <unordered_set>
#include <utility>

#include <rocksdb/write_batch.h>

#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"

namespace cedar::internal {

Status CheckCommittedBatchSize(const rocksdb::WriteBatch& batch,
                               uint64_t max_bytes) {
  if (batch.GetDataSize() > max_bytes) {
    return Status::ResourceExhausted("commit",
                                     "encoded batch exceeds hard byte limit");
  }
  return Status::OK();
}

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
  // Candidate keys were encoded when the candidate was assembled. Validate the
  // fixed width and committed sequence without re-encoding each key, and keep
  // duplicate detection on views instead of copying every key.
  std::unordered_set<std::string_view> mutation_keys;
  mutation_keys.reserve(candidate.fact_keys.size());
  std::unordered_set<std::string> identity_keys;
  identity_keys.reserve(candidate.batch->edge_identities.size());
  for (size_t index = 0; index < candidate.batch->mutations.size(); ++index) {
    const std::string& fact_key = candidate.fact_keys[index];
    if (fact_key.size() != kEncodedFactKeyBytes) {
      return Status::InvalidArgument("commit publisher", "invalid fact key");
    }
    const auto decoded_key = DecodeFactKey(fact_key);
    if (!decoded_key.ok() ||
        decoded_key.ValueOrDie().commit_seq != candidate.commit_seq) {
      return Status::InvalidArgument("commit publisher",
                                     "fact key commit sequence disagrees");
    }
    if (!mutation_keys.insert(fact_key).second) {
      return Status::InvalidArgument("commit publisher", "duplicate fact key");
    }
  }
  for (const EdgeIdentity& identity : candidate.batch->edge_identities) {
    const FactRef identity_ref(identity.home_part_id, FactFamily::kEdgeIdentity,
                               PropertyId{}, identity.edge_id.value);
    const std::string identity_key =
        EncodeFactKey(identity_ref, ValidTime{0}, candidate.commit_seq);
    if (identity_key.empty()) {
      return Status::InvalidArgument("commit publisher",
                                     "invalid edge identity fact key");
    }
    if (mutation_keys.contains(identity_key) ||
        !identity_keys.insert(identity_key).second) {
      return Status::InvalidArgument("commit publisher", "duplicate fact key");
    }
  }
  for (size_t index = 0; index < candidate.batch->mutations.size(); ++index) {
    const PendingFactMutation& mutation = candidate.batch->mutations[index];
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
  SequenceRecord sequence_record{candidate.commit_seq, candidate.batch->txn_id,
                                 candidate.batch->system_hlc,
                                 std::move(sequence_fact_keys)};
  // The committed sequence record is always the compact v2 encoding; every
  // other record in the batch is format-independent.
  const auto sequence = EncodeSequenceRecordV2(sequence_record);
  if (!transaction_key.ok()) return transaction_key.status();
  if (!sequence_key.ok()) return sequence_key.status();
  if (!outcome.ok()) return outcome.status();
  if (!sequence.ok()) return sequence.status();
  write_batch->Put(meta_cf, transaction_key.ValueOrDie(), outcome.ValueOrDie());
  write_batch->Put(meta_cf, sequence_key.ValueOrDie(), sequence.ValueOrDie());
  return Status::OK();
}

}  // namespace cedar::internal
