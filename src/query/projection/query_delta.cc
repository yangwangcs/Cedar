// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/projection/query_delta.h"

#include <algorithm>
#include <limits>
#include <unordered_set>

namespace cedar::internal {
namespace {

size_t HashCombine(size_t seed, uint64_t value) {
  seed ^= std::hash<uint64_t>{}(value) + static_cast<size_t>(0x9e3779b9) +
          (seed << 6) + (seed >> 2);
  return seed;
}

CorrectedBoundary BoundaryFromEvent(const FactEvent& event) {
  return CorrectedBoundary{event.valid_from, event.commit_seq, event.operation,
                           event.schema_epoch, event.value, event.edge_identity};
}

}  // namespace

Status QueryDeltaCommit::Validate() const {
  if (commit_seq.value == 0) {
    return Status::InvalidArgument("query delta", "zero commit sequence");
  }
  for (const FactEvent& event : facts) {
    const Status valid = event.Validate();
    if (!valid.ok()) return valid;
    if (event.commit_seq != commit_seq) {
      return Status::InvalidArgument("query delta", "fact commit differs from descriptor");
    }
  }
  if (!facts.empty() && !mutations.empty()) {
    return Status::InvalidArgument("query delta", "descriptor contains both facts and mutations");
  }
  for (const PendingFactMutation& mutation : mutations) {
    const Status valid = mutation.Validate();
    if (!valid.ok()) return valid;
  }
  for (const EdgeIdentity& identity : edge_identities) {
    const Status valid = identity.Validate();
    if (!valid.ok()) return valid;
  }
  return Status::OK();
}

uint64_t QueryDeltaCommit::EstimatedBytes() const {
  uint64_t bytes = sizeof(QueryDeltaCommit) +
                   static_cast<uint64_t>(facts.capacity()) * sizeof(FactEvent) +
                   static_cast<uint64_t>(mutations.capacity()) * sizeof(PendingFactMutation) +
                   static_cast<uint64_t>(edge_identities.capacity()) * sizeof(EdgeIdentity);
  for (const FactEvent& event : facts) {
    if (event.value.has_value() && event.value->type() == PhysicalType::kString) {
      bytes += std::get<std::string>(event.value->data()).size();
    } else if (event.value.has_value() && event.value->type() == PhysicalType::kBinary) {
      bytes += std::get<std::string>(event.value->data()).size();
    }
  }
  for (const PendingFactMutation& mutation : mutations) {
    if (mutation.value.has_value() &&
        (mutation.value->type() == PhysicalType::kString ||
         mutation.value->type() == PhysicalType::kBinary)) {
      bytes += std::get<std::string>(mutation.value->data()).size();
    }
  }
  // Account conservatively for vector capacity, unordered-map nodes, and
  // per-chain bookkeeping. This intentionally overestimates derived memory.
  bytes += static_cast<uint64_t>(facts.size()) * 96;
  bytes += static_cast<uint64_t>(mutations.size()) * 96;
  bytes += static_cast<uint64_t>(edge_identities.size()) * 96;
  return bytes;
}

std::vector<FactEvent> QueryDeltaView::EventsFor(const FactRef& ref) const {
  std::vector<FactEvent> result;
  for (const FactEvent& event : facts) {
    if (event.ref == ref) result.push_back(event);
  }
  std::sort(result.begin(), result.end(), [](const FactEvent& a, const FactEvent& b) {
    if (a.valid_from != b.valid_from) return a.valid_from.value < b.valid_from.value;
    return a.commit_seq.value < b.commit_seq.value;
  });
  return result;
}

std::vector<EdgeIdentity> QueryDeltaView::EdgeIdentitiesThrough(
    CommitSeq snapshot) const {
  std::vector<EdgeIdentity> result;
  if (!edge_identity_records.empty()) {
    for (const auto& record : edge_identity_records) {
      if (record.first.value <= snapshot.value) result.push_back(record.second);
    }
  } else {
    result = edge_identities;
  }
  return result;
}

QueryDelta::QueryDelta(QueryDeltaOptions options)
    : options_(std::move(options)), indexed_through_(options_.base_seq),
      visible_seq_(options_.base_seq), enqueued_through_(options_.base_seq) {
  if (options_.queue_capacity == 0) options_.queue_capacity = 1;
  if (options_.hard_memory_bytes < options_.soft_memory_bytes) {
    options_.hard_memory_bytes = options_.soft_memory_bytes;
  }
  worker_ = std::thread(&QueryDelta::WorkerMain, this);
}

QueryDelta::~QueryDelta() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
  }
  published_cv_.notify_all();
  if (worker_.joinable()) worker_.join();
}

size_t QueryDelta::FactRefHash::operator()(const FactRef& ref) const noexcept {
  size_t hash = HashCombine(static_cast<size_t>(ref.part_id().value),
                            static_cast<uint64_t>(ref.family()));
  hash = HashCombine(hash, ref.property_id().value);
  return HashCombine(hash, ref.entity_id());
}

void QueryDelta::SetMissingLocked(CommitSeq missing) {
  if (missing.value == 0) return;
  if (first_missing_.value == 0 || missing.value < first_missing_.value) {
    first_missing_ = missing;
  }
}

Status QueryDelta::IndexLocked(const QueryDeltaCommit& commit) {
  const Status valid = commit.Validate();
  if (!valid.ok()) return valid;
  const uint64_t bytes = commit.EstimatedBytes();
  if (options_.soft_lag_commits != 0 &&
      commits_.size() + 1 >= options_.soft_lag_commits) {
    soft_lag_reached_ = true;
  }
  if (options_.soft_memory_bytes != 0 &&
      (bytes >= options_.soft_memory_bytes ||
       memory_bytes_ >= options_.soft_memory_bytes - bytes)) {
    soft_memory_reached_ = true;
  }
  if (bytes > options_.hard_memory_bytes || memory_bytes_ > options_.hard_memory_bytes - bytes ||
      commits_.size() >= options_.max_lag_commits) {
    hard_limit_reached_ = true;
    SetMissingLocked(commit.commit_seq);
    return Status::ResourceExhausted("query delta", "hard memory or lag bound reached");
  }
  QueryDeltaCommit normalized = commit;
  if (!normalized.mutations.empty()) {
    normalized.facts.reserve(normalized.mutations.size());
    for (const PendingFactMutation& mutation : normalized.mutations) {
      normalized.facts.push_back(FactEvent{mutation.ref, mutation.valid_from,
                                           normalized.commit_seq,
                                           mutation.operation,
                                           mutation.schema_epoch, mutation.value,
                                           std::nullopt});
    }
    normalized.mutations.clear();
  }
  commits_.push_back(normalized);
  memory_bytes_ += bytes;
  for (const FactEvent& event : normalized.facts) {
    chains_[event.ref].push_back(event);
    if (event.edge_identity.has_value()) {
      edge_identities_.emplace_back(event.commit_seq, *event.edge_identity);
    }
  }
  for (const EdgeIdentity& identity : normalized.edge_identities) {
    const auto duplicate = std::find_if(
        edge_identities_.begin(), edge_identities_.end(),
        [&identity, &commit](const auto& candidate) {
          return candidate.first == commit.commit_seq && candidate.second == identity;
        });
    if (duplicate == edge_identities_.end()) {
      edge_identities_.emplace_back(commit.commit_seq, identity);
    }
  }
  indexed_through_ = commit.commit_seq;
  if (visible_seq_.value < commit.commit_seq.value) visible_seq_ = commit.commit_seq;
  if (first_missing_.value != 0 && indexed_through_.value >= first_missing_.value) {
    first_missing_ = CommitSeq{};
  }
  return Status::OK();
}

Status QueryDelta::ObservePublished(const QueryDeltaCommit& commit) {
  const Status valid = commit.Validate();
  if (!valid.ok()) return valid;
  std::lock_guard<std::mutex> lock(mutex_);
  if (commit.commit_seq.value <= options_.base_seq.value) {
    return Status::InvalidArgument("query delta", "commit is not after projection base");
  }
  if (visible_seq_.value < commit.commit_seq.value) visible_seq_ = commit.commit_seq;
  const uint64_t expected = indexed_through_.value + 1;
  if (commit.commit_seq.value != expected) {
    SetMissingLocked(CommitSeq{expected});
    return Status::Conflict("query delta", "published commit is not contiguous");
  }
  if (queue_size_ >= options_.queue_capacity) {
    SetMissingLocked(commit.commit_seq);
    return Status::ResourceExhausted("query delta", "descriptor queue is full");
  }
  const Status indexed = IndexLocked(commit);
  if (indexed.ok()) ++queue_size_;
  return indexed;
}

Status QueryDelta::EnqueuePublished(const QueryDeltaCommit& commit) {
  const Status valid = commit.Validate();
  if (!valid.ok()) return valid;
  std::lock_guard<std::mutex> lock(mutex_);
  if (commit.commit_seq.value <= options_.base_seq.value) {
    return Status::InvalidArgument("query delta", "commit is not after projection base");
  }
  const uint64_t expected = enqueued_through_.value + 1;
  if (commit.commit_seq.value != expected) {
    SetMissingLocked(CommitSeq{expected});
    if (visible_seq_.value < commit.commit_seq.value) visible_seq_ = commit.commit_seq;
    return Status::Conflict("query delta", "published commit is not contiguous");
  }
  if (published_queue_.size() >= options_.queue_capacity) {
    SetMissingLocked(commit.commit_seq);
    if (visible_seq_.value < commit.commit_seq.value) visible_seq_ = commit.commit_seq;
    return Status::ResourceExhausted("query delta", "descriptor queue is full");
  }
  if (options_.crash_fault_injector) {
    const Status injected = options_.crash_fault_injector("delta_enqueue");
    if (!injected.ok()) return injected;
  }
  published_queue_.push_back(commit);
  enqueued_through_ = commit.commit_seq;
  if (visible_seq_.value < commit.commit_seq.value) visible_seq_ = commit.commit_seq;
  published_cv_.notify_one();
  return Status::OK();
}

void QueryDelta::WorkerMain() {
  for (;;) {
    std::unique_lock<std::mutex> lock(mutex_);
    published_cv_.wait(lock, [this] {
      return stopping_ || !published_queue_.empty();
    });
    if (published_queue_.empty() && stopping_) return;
    QueryDeltaCommit commit = std::move(published_queue_.front());
    published_queue_.pop_front();
    if (commit.commit_seq.value != indexed_through_.value + 1) {
      SetMissingLocked(CommitSeq{indexed_through_.value + 1});
      continue;
    }
    IndexLocked(commit);
  }
}

Status QueryDelta::RepairThrough(const FactStore& store,
                                 const StoreSnapshot& snapshot,
                                 CommitSeq target,
                                 QueryDeltaRepairLimits limits) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (target.value <= indexed_through_.value) return Status::OK();
  if (target.value > snapshot.commit_seq().value) {
    return Status::InvalidArgument("query delta repair", "target exceeds snapshot");
  }
  const uint64_t first_value = indexed_through_.value + 1;
  if (target.value - indexed_through_.value > limits.max_commits) {
    return Status::ResourceExhausted("query delta repair", "commit repair budget exceeded");
  }
  const auto sequences = store.ReadSequenceRange(snapshot, CommitSeq{first_value}, target);
  if (!sequences.ok()) return sequences.status();
  std::vector<QueryDeltaCommit> repaired;
  repaired.reserve(sequences.ValueOrDie().size());
  uint64_t bytes = 0;
  for (const SequenceRecord& sequence : sequences.ValueOrDie()) {
    auto events = store.ReadExactFacts(snapshot, sequence.fact_keys);
    if (!events.ok()) return events.status();
    QueryDeltaCommit descriptor(sequence.commit_seq);
    descriptor.facts = events.ConsumeValueOrDie();
    for (const FactEvent& event : descriptor.facts) {
      if (event.edge_identity.has_value()) descriptor.edge_identities.push_back(*event.edge_identity);
    }
    bytes += descriptor.EstimatedBytes();
    if (bytes > limits.max_bytes) {
      return Status::ResourceExhausted("query delta repair", "byte repair budget exceeded");
    }
    repaired.push_back(std::move(descriptor));
  }
  for (const QueryDeltaCommit& descriptor : repaired) {
    const Status indexed = IndexLocked(descriptor);
    if (!indexed.ok()) return indexed;
  }
  queue_size_ = 0;
  published_queue_.clear();
  enqueued_through_ = indexed_through_;
  if (first_missing_.value != 0 && first_missing_.value <= indexed_through_.value) {
    first_missing_ = CommitSeq{};
  }
  return Status::OK();
}

Status QueryDelta::ConsumeThrough(CommitSeq through) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (through.value > indexed_through_.value) {
    return Status::InvalidArgument("query delta", "consume exceeds indexed coverage");
  }
  queue_size_ = 0;
  return Status::OK();
}

Status QueryDelta::RetireThrough(CommitSeq through) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (through.value < options_.base_seq.value ||
      through.value > indexed_through_.value) {
    return Status::InvalidArgument("query delta", "retirement is outside indexed coverage");
  }
  std::vector<QueryDeltaCommit> retained;
  retained.reserve(commits_.size());
  for (const QueryDeltaCommit& commit : commits_) {
    if (commit.commit_seq.value > through.value) retained.push_back(commit);
  }
  commits_ = std::move(retained);
  chains_.clear();
  edge_identities_.erase(
      std::remove_if(edge_identities_.begin(), edge_identities_.end(),
                     [through](const auto& value) {
                       return value.first.value <= through.value;
                     }),
      edge_identities_.end());
  for (const QueryDeltaCommit& commit : commits_) {
    for (const FactEvent& event : commit.facts) chains_[event.ref].push_back(event);
  }
  memory_bytes_ = 0;
  for (const QueryDeltaCommit& commit : commits_) memory_bytes_ += commit.EstimatedBytes();
  options_.base_seq = through;
  queue_size_ = 0;
  if (published_queue_.empty()) enqueued_through_ = indexed_through_;
  if (first_missing_.value != 0 && first_missing_.value <= through.value) {
    first_missing_ = CommitSeq{};
  }
  return Status::OK();
}

Status QueryDelta::ResetBase(CommitSeq base) {
  std::lock_guard<std::mutex> lock(mutex_);
  options_.base_seq = base;
  indexed_through_ = base;
  visible_seq_ = base;
  first_missing_ = CommitSeq{};
  memory_bytes_ = 0;
  queue_size_ = 0;
  hard_limit_reached_ = false;
  soft_lag_reached_ = false;
  soft_memory_reached_ = false;
  commits_.clear();
  chains_.clear();
  edge_identities_.clear();
  published_queue_.clear();
  enqueued_through_ = base;
  return Status::OK();
}

StatusOr<QueryDeltaView> QueryDelta::AcquireThrough(CommitSeq snapshot) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (snapshot.value < options_.base_seq.value) {
    return Status::InvalidArgument("query delta", "snapshot precedes projection base");
  }
  if (snapshot.value > indexed_through_.value ||
      (first_missing_.value != 0 && first_missing_.value <= snapshot.value) ||
      hard_limit_reached_) {
    return Status::ResourceExhausted("query delta", "delta is not contiguous through snapshot");
  }
  QueryDeltaView view{options_.base_seq, snapshot, {}, {}, {}, first_missing_};
  for (const QueryDeltaCommit& commit : commits_) {
    if (commit.commit_seq.value > options_.base_seq.value &&
        commit.commit_seq.value <= snapshot.value) {
      view.facts.insert(view.facts.end(), commit.facts.begin(), commit.facts.end());
      view.edge_identities.insert(view.edge_identities.end(), commit.edge_identities.begin(),
                                  commit.edge_identities.end());
      for (const EdgeIdentity& identity : commit.edge_identities) {
        view.edge_identity_records.emplace_back(commit.commit_seq, identity);
      }
    }
  }
  return view;
}

CommitSeq QueryDelta::base_seq() const { std::lock_guard<std::mutex> l(mutex_); return options_.base_seq; }
CommitSeq QueryDelta::indexed_through() const { std::lock_guard<std::mutex> l(mutex_); return indexed_through_; }
CommitSeq QueryDelta::visible_seq() const { std::lock_guard<std::mutex> l(mutex_); return visible_seq_; }
CommitSeq QueryDelta::first_missing() const { std::lock_guard<std::mutex> l(mutex_); return first_missing_; }
uint64_t QueryDelta::memory_bytes() const { std::lock_guard<std::mutex> l(mutex_); return memory_bytes_; }
bool QueryDelta::mergeable() const { std::lock_guard<std::mutex> l(mutex_); return !hard_limit_reached_ && first_missing_.value == 0; }
bool QueryDelta::hard_limit_reached() const { std::lock_guard<std::mutex> l(mutex_); return hard_limit_reached_; }
bool QueryDelta::soft_lag_reached() const { std::lock_guard<std::mutex> l(mutex_); return soft_lag_reached_; }
bool QueryDelta::soft_memory_reached() const { std::lock_guard<std::mutex> l(mutex_); return soft_memory_reached_; }
size_t QueryDelta::pending_commits() const { std::lock_guard<std::mutex> l(mutex_); return queue_size_; }

std::vector<FactEvent> QueryDelta::EventsFor(const FactRef& ref, CommitSeq snapshot) const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<FactEvent> result;
  const auto found = chains_.find(ref);
  if (found == chains_.end()) return result;
  for (const FactEvent& event : found->second) {
    if (event.commit_seq.value <= snapshot.value) result.push_back(event);
  }
  std::sort(result.begin(), result.end(), [](const FactEvent& a, const FactEvent& b) {
    if (a.valid_from != b.valid_from) return a.valid_from.value < b.valid_from.value;
    return a.commit_seq.value < b.commit_seq.value;
  });
  return result;
}

std::vector<EdgeIdentity> QueryDelta::EdgeIdentities(CommitSeq snapshot) const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<EdgeIdentity> result;
  for (const auto& candidate : edge_identities_) {
    if (candidate.first.value <= snapshot.value) result.push_back(candidate.second);
  }
  return result;
}

StatusOr<std::vector<CorrectedBoundary>> QueryDelta::MergeBoundaries(
    const std::vector<CorrectedBoundary>& base,
    const std::vector<FactEvent>& delta, CommitSeq snapshot) {
  std::vector<CorrectedBoundary> all;
  all.reserve(base.size() + delta.size());
  for (const CorrectedBoundary& boundary : base) {
    if (boundary.commit_seq.value <= snapshot.value) all.push_back(boundary);
  }
  for (const FactEvent& event : delta) {
    if (event.commit_seq.value > snapshot.value) continue;
    const Status valid = event.Validate();
    if (!valid.ok()) return valid;
    all.push_back(BoundaryFromEvent(event));
  }
  std::sort(all.begin(), all.end(), [](const CorrectedBoundary& a, const CorrectedBoundary& b) {
    if (a.valid_from != b.valid_from) return a.valid_from.value < b.valid_from.value;
    return a.commit_seq.value < b.commit_seq.value;
  });
  std::vector<CorrectedBoundary> result;
  for (size_t i = 0; i < all.size();) {
    size_t j = i + 1;
    while (j < all.size() && all[j].valid_from == all[i].valid_from) ++j;
    result.push_back(all[j - 1]);
    i = j;
  }
  return result;
}

}  // namespace cedar::internal
