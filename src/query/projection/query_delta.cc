// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/projection/query_delta.h"

#include <algorithm>
#include <limits>
#include <tuple>
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

class QueryDelta::LifecycleTransitionGuard {
 public:
  explicit LifecycleTransitionGuard(QueryDelta* owner) : owner_(owner) {}
  ~LifecycleTransitionGuard() {
    owner_->lifecycle_transition_ = false;
    owner_->published_cv_.notify_all();
  }

 private:
  QueryDelta* owner_;
};

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
  const auto begin = std::lower_bound(facts.begin(), facts.end(), ref,
      [](const FactEvent& event, const FactRef& key) {
        if (event.ref.part_id().value != key.part_id().value)
          return event.ref.part_id().value < key.part_id().value;
        if (event.ref.family() != key.family())
          return static_cast<uint8_t>(event.ref.family()) < static_cast<uint8_t>(key.family());
        if (event.ref.property_id().value != key.property_id().value)
          return event.ref.property_id().value < key.property_id().value;
        return event.ref.entity_id() < key.entity_id();
      });
  for (auto current = begin; current != facts.end() && current->ref == ref; ++current)
    result.push_back(*current);
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

std::vector<FactEvent> QueryDeltaLease::EventsFor(const FactRef& ref) const {
  std::vector<FactEvent> result;
  auto range = EventsForRange(ref);
  if (!range.ok()) return result;
  result.reserve(range.ValueOrDie().size());
  for (const FactEvent& event : range.ValueOrDie()) result.push_back(event);
  return result;
}

StatusOr<QueryDeltaFactRange> QueryDeltaLease::EventsForRange(
    const FactRef& ref) const {
  const auto less = [](const QueryDeltaFactRef& event, const FactRef& key) {
    return event.ref.part_id().value < key.part_id().value ||
           (event.ref.part_id().value == key.part_id().value &&
            (static_cast<uint8_t>(event.ref.family()) <
                 static_cast<uint8_t>(key.family()) ||
             (event.ref.family() == key.family() &&
              (event.ref.property_id().value < key.property_id().value ||
               (event.ref.property_id().value == key.property_id().value &&
                event.ref.entity_id() < key.entity_id())))));
  };
  const auto begin = std::lower_bound(
      events_->begin(), events_->end(), ref,
      [&less](const QueryDeltaFactRef& event, const FactRef& key) {
        return less(event, key);
      });
  size_t begin_index = static_cast<size_t>(begin - events_->begin());
  size_t end_index = begin_index;
  while (end_index < events_->size() && (*events_)[end_index].ref == ref) ++end_index;
  return QueryDeltaFactRange(events_, begin_index, end_index);
}

std::vector<EdgeIdentity> QueryDeltaLease::EdgeIdentitiesThrough(
    CommitSeq snapshot) const {
  std::vector<EdgeIdentity> result;
  const uint64_t cut = std::min(snapshot.value, through_.value);
  for (const auto& commit : commits_) {
    if (!commit || commit->commit_seq.value <= base_seq_.value ||
        commit->commit_seq.value > cut) {
      continue;
    }
    result.insert(result.end(), commit->edge_identities.begin(),
                  commit->edge_identities.end());
  }
  return result;
}

QueryDelta::QueryDelta(QueryDeltaOptions options)
    : options_(std::move(options)), indexed_through_(options_.base_seq),
      visible_seq_value_(options_.base_seq.value),
      first_missing_value_(0), enqueued_through_(options_.base_seq) {
  if (options_.queue_capacity == 0) options_.queue_capacity = 1;
  if (options_.hard_memory_bytes < options_.soft_memory_bytes) {
    options_.hard_memory_bytes = options_.soft_memory_bytes;
  }
  worker_ = std::thread(&QueryDelta::WorkerMain, this);
}

QueryDelta::~QueryDelta() {
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
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
  uint64_t observed = first_missing_value_.load(std::memory_order_relaxed);
  while ((observed == 0 || missing.value < observed) &&
         !first_missing_value_.compare_exchange_weak(
             observed, missing.value, std::memory_order_release,
             std::memory_order_relaxed)) {
  }
}

void AdvanceVisible(std::atomic<uint64_t>* visible, uint64_t sequence) {
  uint64_t observed = visible->load(std::memory_order_relaxed);
  while (observed < sequence &&
         !visible->compare_exchange_weak(observed, sequence,
                                         std::memory_order_release,
                                         std::memory_order_relaxed)) {
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
  uint64_t retired_bytes = 0;
  {
    std::lock_guard<std::mutex> epoch_lock(epoch_state_->mutex);
    retired_bytes = epoch_state_->retired_bytes;
  }
  bool exceeds_hard_memory = bytes > options_.hard_memory_bytes;
  if (!exceeds_hard_memory) {
    uint64_t remaining = options_.hard_memory_bytes - bytes;
    exceeds_hard_memory = memory_bytes_ > remaining;
    if (!exceeds_hard_memory) {
      remaining -= memory_bytes_;
      exceeds_hard_memory = retired_bytes > remaining;
    }
  }
  if (exceeds_hard_memory || commits_.size() >= options_.max_lag_commits) {
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
  auto immutable_commit =
      std::make_shared<const QueryDeltaCommit>(commits_.back());
  lease_commits_.push_back(immutable_commit);
  chunks_.push_back(std::make_shared<const DeltaChunk>(
      DeltaChunk{commit.commit_seq, commit.commit_seq, immutable_commit, bytes}));
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
  AdvanceVisible(&visible_seq_value_, commit.commit_seq.value);
  if (first_missing_value_.load(std::memory_order_acquire) != 0 &&
      indexed_through_.value >= first_missing_value_.load(std::memory_order_acquire)) {
    first_missing_value_.store(0, std::memory_order_release);
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
  AdvanceVisible(&visible_seq_value_, commit.commit_seq.value);
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
  std::unique_lock<std::mutex> lock(queue_mutex_);
  published_cv_.wait(lock, [this] { return !lifecycle_transition_; });
  if (commit.commit_seq.value <= options_.base_seq.value) {
    return Status::InvalidArgument("query delta", "commit is not after projection base");
  }
  const uint64_t expected = enqueued_through_.value + 1;
  if (commit.commit_seq.value != expected) {
    SetMissingLocked(CommitSeq{expected});
    AdvanceVisible(&visible_seq_value_, commit.commit_seq.value);
    return Status::Conflict("query delta", "published commit is not contiguous");
  }
  if (published_queue_.size() >= options_.queue_capacity) {
    SetMissingLocked(commit.commit_seq);
    AdvanceVisible(&visible_seq_value_, commit.commit_seq.value);
    return Status::ResourceExhausted("query delta", "descriptor queue is full");
  }
  if (options_.crash_fault_injector) {
    const Status injected = options_.crash_fault_injector("delta_enqueue");
    if (!injected.ok()) return injected;
  }
  published_queue_.push_back(commit);
  enqueued_through_ = commit.commit_seq;
  AdvanceVisible(&visible_seq_value_, commit.commit_seq.value);
  published_cv_.notify_one();
  return Status::OK();
}

void QueryDelta::WorkerMain() {
  for (;;) {
    std::unique_lock<std::mutex> queue_lock(queue_mutex_);
    published_cv_.wait(queue_lock, [this] {
      return stopping_ || !published_queue_.empty();
    });
    if (published_queue_.empty() && stopping_) return;
    QueryDeltaCommit commit = std::move(published_queue_.front());
    published_queue_.pop_front();
    worker_indexing_ = true;
    queue_lock.unlock();
    if (options_.worker_after_pop_observer_for_testing) {
      options_.worker_after_pop_observer_for_testing();
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (commit.commit_seq.value != indexed_through_.value + 1) {
        SetMissingLocked(CommitSeq{indexed_through_.value + 1});
      } else {
        if (options_.worker_before_index_observer_for_testing) {
          options_.worker_before_index_observer_for_testing();
        }
        IndexLocked(commit);
      }
    }
    queue_lock.lock();
    worker_indexing_ = false;
    queue_lock.unlock();
    published_cv_.notify_all();
  }
}

Status QueryDelta::RepairThrough(const FactStore& store,
                                 const StoreSnapshot& snapshot,
                                 CommitSeq target,
                                 QueryDeltaRepairLimits limits) {
  std::unique_lock<std::mutex> queue_lock(queue_mutex_);
  published_cv_.wait(queue_lock, [this] { return !lifecycle_transition_; });
  lifecycle_transition_ = true;
  LifecycleTransitionGuard transition_guard(this);
  published_cv_.wait(queue_lock, [this] { return !worker_indexing_; });
  std::lock_guard<std::mutex> lock(mutex_);
  if (target.value <= indexed_through_.value) {
    return Status::OK();
  }
  if (target.value > snapshot.commit_seq().value) {
    return Status::InvalidArgument("query delta repair", "target exceeds snapshot");
  }
  const uint64_t first_value = indexed_through_.value + 1;
  if (target.value - indexed_through_.value > limits.max_commits) {
    return Status::ResourceExhausted("query delta repair", "commit repair budget exceeded");
  }
  const auto sequences = store.ReadSequenceRange(snapshot, CommitSeq{first_value}, target);
  if (!sequences.ok()) {
    return sequences.status();
  }
  std::vector<QueryDeltaCommit> repaired;
  repaired.reserve(sequences.ValueOrDie().size());
  uint64_t bytes = 0;
  for (const SequenceRecord& sequence : sequences.ValueOrDie()) {
    auto events = store.ReadExactFacts(snapshot, sequence.fact_keys);
    if (!events.ok()) {
      return events.status();
    }
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
    if (!indexed.ok()) {
      return indexed;
    }
  }
  queue_size_ = 0;
  published_queue_.clear();
  enqueued_through_ = indexed_through_;
  if (first_missing_value_.load(std::memory_order_acquire) != 0 &&
      first_missing_value_.load(std::memory_order_acquire) <= indexed_through_.value) {
    first_missing_value_.store(0, std::memory_order_release);
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
  std::unique_lock<std::mutex> queue_lock(queue_mutex_);
  published_cv_.wait(queue_lock, [this] { return !lifecycle_transition_; });
  lifecycle_transition_ = true;
  LifecycleTransitionGuard transition_guard(this);
  published_cv_.wait(queue_lock, [this] { return !worker_indexing_; });
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
  while (!lease_commits_.empty() &&
         lease_commits_.front()->commit_seq.value <= through.value) {
    lease_commits_.pop_front();
  }
  while (!chunks_.empty() && chunks_.front()->last.value <= through.value) {
    {
      std::lock_guard<std::mutex> epoch_lock(epoch_state_->mutex);
      epoch_state_->retired.push_back(chunks_.front());
      epoch_state_->retired_bytes += chunks_.front()->memory_bytes;
    }
    chunks_.pop_front();
  }
  if (epoch_state_->active_leases.load(std::memory_order_acquire) == 0) {
    std::lock_guard<std::mutex> epoch_lock(epoch_state_->mutex);
    epoch_state_->retired.clear();
    epoch_state_->retired_bytes = 0;
  }
  lease_index_cache_.clear();
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
  if (first_missing_value_.load(std::memory_order_acquire) != 0 &&
      first_missing_value_.load(std::memory_order_acquire) <= through.value) {
    first_missing_value_.store(0, std::memory_order_release);
  }
  return Status::OK();
}

Status QueryDelta::ResetBase(CommitSeq base) {
  std::unique_lock<std::mutex> queue_lock(queue_mutex_);
  published_cv_.wait(queue_lock, [this] { return !lifecycle_transition_; });
  lifecycle_transition_ = true;
  LifecycleTransitionGuard transition_guard(this);
  published_cv_.wait(queue_lock, [this] { return !worker_indexing_; });
  std::lock_guard<std::mutex> lock(mutex_);
  options_.base_seq = base;
  indexed_through_ = base;
  visible_seq_value_.store(base.value, std::memory_order_release);
  first_missing_value_.store(0, std::memory_order_release);
  memory_bytes_ = 0;
  queue_size_ = 0;
  hard_limit_reached_ = false;
  soft_lag_reached_ = false;
  soft_memory_reached_ = false;
  commits_.clear();
  lease_index_cache_.clear();
  lease_commits_.clear();
  chunks_.clear();
  {
    std::lock_guard<std::mutex> epoch_lock(epoch_state_->mutex);
    epoch_state_->retired.clear();
    epoch_state_->retired_bytes = 0;
  }
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
      (first_missing_value_.load(std::memory_order_acquire) != 0 &&
       first_missing_value_.load(std::memory_order_acquire) <= snapshot.value) ||
      hard_limit_reached_) {
    return Status::ResourceExhausted("query delta", "delta is not contiguous through snapshot");
  }
  QueryDeltaView view{options_.base_seq, snapshot, {}, {}, {},
                      CommitSeq{first_missing_value_.load(std::memory_order_acquire)}};
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
  std::sort(view.facts.begin(), view.facts.end(), [](const FactEvent& left,
                                                      const FactEvent& right) {
    if (left.ref.part_id().value != right.ref.part_id().value)
      return left.ref.part_id().value < right.ref.part_id().value;
    if (left.ref.family() != right.ref.family())
      return static_cast<uint8_t>(left.ref.family()) < static_cast<uint8_t>(right.ref.family());
    if (left.ref.property_id().value != right.ref.property_id().value)
      return left.ref.property_id().value < right.ref.property_id().value;
    if (left.ref.entity_id() != right.ref.entity_id())
      return left.ref.entity_id() < right.ref.entity_id();
    return std::tie(left.valid_from.value, left.commit_seq.value) <
           std::tie(right.valid_from.value, right.commit_seq.value);
  });
  return view;
}

StatusOr<std::shared_ptr<const QueryDeltaLease>> QueryDelta::AcquireLeaseThrough(
    CommitSeq snapshot) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (snapshot.value < options_.base_seq.value) {
    return Status::InvalidArgument("query delta", "snapshot precedes projection base");
  }
  if (snapshot.value > indexed_through_.value ||
      (first_missing_value_.load(std::memory_order_acquire) != 0 &&
       first_missing_value_.load(std::memory_order_acquire) <= snapshot.value) ||
      hard_limit_reached_) {
    return Status::ResourceExhausted("query delta", "delta is not contiguous through snapshot");
  }
  std::vector<std::shared_ptr<const QueryDeltaCommit>> visible;
  visible.reserve(lease_commits_.size());
  for (const auto& commit : lease_commits_) {
    if (commit->commit_seq.value > options_.base_seq.value &&
        commit->commit_seq.value <= snapshot.value) {
      visible.push_back(commit);
    }
  }
  std::vector<std::shared_ptr<const DeltaChunk>> visible_chunks;
  visible_chunks.reserve(chunks_.size());
  for (const auto& chunk : chunks_) {
    if (chunk->first.value > options_.base_seq.value &&
        chunk->last.value <= snapshot.value) {
      visible_chunks.push_back(chunk);
    }
  }
  epoch_state_->active_leases.fetch_add(1, std::memory_order_acq_rel);
  auto epoch_token = std::shared_ptr<void>(
      epoch_state_.get(), [epoch_state = epoch_state_](void*) {
        if (epoch_state->active_leases.fetch_sub(1, std::memory_order_acq_rel) ==
            1) {
          std::lock_guard<std::mutex> epoch_lock(epoch_state->mutex);
          epoch_state->retired.clear();
          epoch_state->retired_bytes = 0;
        }
      });
  std::shared_ptr<const std::vector<QueryDeltaFactRef>> immutable_events;
  const auto cached = lease_index_cache_.find(snapshot.value);
  if (cached != lease_index_cache_.end()) immutable_events = cached->second.lock();
  if (!immutable_events) {
    auto events = std::make_shared<std::vector<QueryDeltaFactRef>>();
    size_t event_count = 0;
    for (const auto& commit : visible) event_count += commit->facts.size();
    events->reserve(event_count);
    for (const auto& commit : visible) {
      for (size_t index = 0; index < commit->facts.size(); ++index) {
        events->push_back(QueryDeltaFactRef{commit->facts[index].ref, commit, index});
      }
    }
    const auto less = [](const QueryDeltaFactRef& a,
                         const QueryDeltaFactRef& b) {
      if (a.ref.part_id().value != b.ref.part_id().value)
        return a.ref.part_id().value < b.ref.part_id().value;
      if (a.ref.family() != b.ref.family())
        return static_cast<uint8_t>(a.ref.family()) <
               static_cast<uint8_t>(b.ref.family());
      if (a.ref.property_id().value != b.ref.property_id().value)
        return a.ref.property_id().value < b.ref.property_id().value;
      if (a.ref.entity_id() != b.ref.entity_id())
        return a.ref.entity_id() < b.ref.entity_id();
      const FactEvent& ea = a.commit->facts[a.index];
      const FactEvent& eb = b.commit->facts[b.index];
      return std::tie(ea.valid_from.value, ea.commit_seq.value) <
             std::tie(eb.valid_from.value, eb.commit_seq.value);
    };
    std::sort(events->begin(), events->end(), less);
    immutable_events = std::move(events);
    lease_index_cache_[snapshot.value] = immutable_events;
    if (lease_index_cache_.size() > 8) {
      lease_index_cache_.erase(lease_index_cache_.begin());
    }
  }
  return std::shared_ptr<const QueryDeltaLease>(new QueryDeltaLease(
      options_.base_seq, snapshot,
      CommitSeq{first_missing_value_.load(std::memory_order_acquire)},
      std::move(visible), std::move(visible_chunks),
      std::move(epoch_token),
      std::move(immutable_events)));
}

CommitSeq QueryDelta::base_seq() const { std::lock_guard<std::mutex> l(mutex_); return options_.base_seq; }
CommitSeq QueryDelta::indexed_through() const { std::lock_guard<std::mutex> l(mutex_); return indexed_through_; }
CommitSeq QueryDelta::visible_seq() const {
  return CommitSeq{visible_seq_value_.load(std::memory_order_acquire)};
}
CommitSeq QueryDelta::first_missing() const {
  return CommitSeq{first_missing_value_.load(std::memory_order_acquire)};
}
uint64_t QueryDelta::memory_bytes() const {
  std::lock_guard<std::mutex> l(mutex_);
  std::lock_guard<std::mutex> epoch_lock(epoch_state_->mutex);
  return memory_bytes_ + epoch_state_->retired_bytes;
}
bool QueryDelta::mergeable() const {
  std::lock_guard<std::mutex> l(mutex_);
  return !hard_limit_reached_ &&
         first_missing_value_.load(std::memory_order_acquire) == 0;
}
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
