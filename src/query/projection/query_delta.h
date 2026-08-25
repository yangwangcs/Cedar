// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_PROJECTION_QUERY_DELTA_H_
#define CEDAR_QUERY_PROJECTION_QUERY_DELTA_H_

#include <cstddef>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <atomic>
#include <unordered_map>
#include <utility>
#include <thread>
#include <vector>

#include "storage/facts/fact_store.h"
#include "query/temporal/corrected_chain.h"

namespace cedar::internal {

// Immutable input handed off after a commit becomes visible.  It deliberately
// contains only Cedar facts and edge identities; no RocksDB handle or iterator
// may cross the query boundary.
struct QueryDeltaCommit {
  CommitSeq commit_seq;
  std::vector<FactEvent> facts;
  // Optional pre-publication form.  The append pipeline may hand off the
  // original mutations without decoding them; IndexLocked materializes them
  // with this descriptor's commit sequence.
  std::vector<PendingFactMutation> mutations;
  std::vector<EdgeIdentity> edge_identities;

  QueryDeltaCommit() = default;
  explicit QueryDeltaCommit(CommitSeq sequence) : commit_seq(sequence) {}
  explicit QueryDeltaCommit(uint64_t sequence) : commit_seq{sequence} {}
  QueryDeltaCommit(CommitSeq sequence,
                   std::vector<PendingFactMutation> pending,
                   std::vector<EdgeIdentity> identities = {})
      : commit_seq(sequence),
        mutations(std::move(pending)),
        edge_identities(std::move(identities)) {}
  Status Validate() const;
  uint64_t EstimatedBytes() const;
};
using CommitDescriptor = QueryDeltaCommit;
using DeltaCommit = QueryDeltaCommit;

struct QueryDeltaOptions {
  CommitSeq base_seq;
  size_t queue_capacity = 262144;
  uint64_t soft_memory_bytes = 256ULL << 20;
  uint64_t hard_memory_bytes = 512ULL << 20;
  uint64_t max_lag_commits = 262144;
  uint64_t target_lag_seconds = 30;
  std::function<Status(const char*)> crash_fault_injector;
  uint64_t soft_lag_commits = 0;
  std::function<void()> worker_before_index_observer_for_testing;
  std::function<void()> worker_after_pop_observer_for_testing;
};

struct QueryDeltaRepairLimits {
  uint64_t max_commits = 4096;
  uint64_t max_bytes = 32ULL << 20;
};

struct QueryDeltaView {
  CommitSeq base_seq;
  CommitSeq through;
  std::vector<FactEvent> facts;
  std::vector<EdgeIdentity> edge_identities;
  std::vector<std::pair<CommitSeq, EdgeIdentity>> edge_identity_records;
  CommitSeq first_missing;

  std::vector<FactEvent> EventsFor(const FactRef& ref) const;
  std::vector<EdgeIdentity> EdgeIdentitiesThrough(CommitSeq snapshot) const;
};

struct QueryDeltaFactRef {
  FactRef ref;
  std::shared_ptr<const QueryDeltaCommit> commit;
  size_t index = 0;
};

// Immutable commit-sequence unit retained by leases. A chunk deliberately
// owns only immutable descriptor storage; query readers never observe the
// publisher queue or mutable chain maps.
struct DeltaChunk {
  CommitSeq first;
  CommitSeq last;
  std::shared_ptr<const QueryDeltaCommit> commit;
  uint64_t memory_bytes = 0;
};

// Shared lease epoch. Retired chunks stay reachable only while at least one
// snapshot lease is alive; the final lease releases the retired queue in one
// bounded critical section.
struct QueryDeltaEpochState {
  std::atomic<uint64_t> active_leases{0};
  std::mutex mutex;
  std::deque<std::shared_ptr<const DeltaChunk>> retired;
  uint64_t retired_bytes = 0;
};

class QueryDeltaFactRange {
 public:
  class Iterator {
   public:
    const FactEvent& operator*() const {
      return (*entries_)[position_].commit->facts[(*entries_)[position_].index];
    }
    Iterator& operator++() {
      ++position_;
      return *this;
    }
    bool operator!=(const Iterator& other) const {
      return entries_ != other.entries_ || position_ != other.position_;
    }

   private:
    friend class QueryDeltaFactRange;
    Iterator(std::shared_ptr<const std::vector<QueryDeltaFactRef>> entries,
             size_t position)
        : entries_(std::move(entries)), position_(position) {}
    std::shared_ptr<const std::vector<QueryDeltaFactRef>> entries_;
    size_t position_ = 0;
  };

  Iterator begin() const { return Iterator(entries_, begin_); }
  Iterator end() const { return Iterator(entries_, end_); }
  size_t size() const { return end_ - begin_; }

 private:
  friend class QueryDeltaLease;
  QueryDeltaFactRange(std::shared_ptr<const std::vector<QueryDeltaFactRef>> entries,
                      size_t begin, size_t end)
      : entries_(std::move(entries)), begin_(begin), end_(end) {}
  std::shared_ptr<const std::vector<QueryDeltaFactRef>> entries_;
  size_t begin_ = 0;
  size_t end_ = 0;
};

// A snapshot lease over immutable published commit descriptors. The lease
// copies only descriptor pointers; fact payload vectors remain shared by all
// readers. EventsFor materializes only the requested fact chain.
class QueryDeltaLease {
 public:
  CommitSeq base_seq() const { return base_seq_; }
  CommitSeq through() const { return through_; }
  CommitSeq first_missing() const { return first_missing_; }
  size_t chunk_count() const { return chunks_.size(); }
  std::vector<FactEvent> EventsFor(const FactRef& ref) const;
  StatusOr<QueryDeltaFactRange> EventsForRange(const FactRef& ref) const;
  std::vector<EdgeIdentity> EdgeIdentitiesThrough(CommitSeq snapshot) const;

 private:
  friend class QueryDelta;
  QueryDeltaLease(CommitSeq base, CommitSeq through, CommitSeq missing,
                  std::vector<std::shared_ptr<const QueryDeltaCommit>> commits,
                  std::vector<std::shared_ptr<const DeltaChunk>> chunks,
                  std::shared_ptr<void> epoch_token,
                  std::shared_ptr<const std::vector<QueryDeltaFactRef>> events)
      : base_seq_(base),
        through_(through),
        first_missing_(missing),
        commits_(std::move(commits)),
        chunks_(std::move(chunks)),
        epoch_token_(std::move(epoch_token)),
        events_(std::move(events)) {}

  CommitSeq base_seq_;
  CommitSeq through_;
  CommitSeq first_missing_;
  std::vector<std::shared_ptr<const QueryDeltaCommit>> commits_;
  std::vector<std::shared_ptr<const DeltaChunk>> chunks_;
  std::shared_ptr<void> epoch_token_;
  // Sorted by canonical FactRef. The descriptor keeps the event storage alive,
  // so this index adds only references and avoids copying the tail payload.
  std::shared_ptr<const std::vector<QueryDeltaFactRef>> events_;
};

class QueryDelta {
 public:
  explicit QueryDelta(QueryDeltaOptions options = {});
  ~QueryDelta();
  QueryDelta(const QueryDelta&) = delete;
  QueryDelta& operator=(const QueryDelta&) = delete;

  // Publishes a descriptor without doing any storage I/O.  A rejected enqueue
  // never advances contiguous coverage; the durable sequence repair path is
  // responsible for filling that exact gap later.
  Status ObservePublished(const QueryDeltaCommit& commit);
  // Publisher-thread handoff. This operation only validates and queues an
  // immutable descriptor; chain indexing runs on the Cedar delta worker.
  Status EnqueuePublished(const QueryDeltaCommit& commit);
  Status ObservePublished(CommitSeq commit) {
    return ObservePublished(QueryDeltaCommit(commit));
  }
  Status ObservePublished(const StoreCommitBatch& batch, CommitSeq commit) {
    QueryDeltaCommit descriptor(commit);
    descriptor.mutations = batch.mutations;
    descriptor.edge_identities = batch.edge_identities;
    return ObservePublished(descriptor);
  }
  Status ObservePublished(uint64_t commit) {
    return ObservePublished(QueryDeltaCommit(commit));
  }

  // Reconstructs a contiguous tail from authoritative sequence metadata and
  // exact facts under one existing Snapshot.  No write or projection I/O is
  // performed by this method.
  Status RepairThrough(const FactStore& store, const StoreSnapshot& snapshot,
                       CommitSeq target,
                       QueryDeltaRepairLimits limits = {});

  // Releases descriptor queue permits after a worker has consumed durable
  // coverage. Indexed chunks remain available for Snapshot-correct merges;
  // this only releases bounded enqueue capacity.
  Status ConsumeThrough(CommitSeq through);
  Status ReleaseThrough(CommitSeq through) { return ConsumeThrough(through); }

  // Generation handoff retirement. It drops indexed chunks and dirty-chain
  // entries at or below the new base, while retaining newer coverage.
  Status RetireThrough(CommitSeq through);
  // Clears the derived view and starts a new projection base. This is used
  // after a hard bound schedules a new generation.
  Status ResetBase(CommitSeq base);

  // Acquires a snapshot-correct immutable view.  A view never exposes changes
  // after its requested cut, even if a publisher advances concurrently.
  StatusOr<QueryDeltaView> AcquireThrough(CommitSeq snapshot) const;
  StatusOr<std::shared_ptr<const QueryDeltaLease>> AcquireLeaseThrough(
      CommitSeq snapshot) const;

  CommitSeq base_seq() const;
  CommitSeq indexed_through() const;
  CommitSeq visible_seq() const;
  // Zero denotes that no gap has been observed.
  CommitSeq first_missing() const;
  uint64_t memory_bytes() const;
  bool mergeable() const;
  bool hard_limit_reached() const;
  bool soft_lag_reached() const;
  bool soft_memory_reached() const;
  size_t pending_commits() const;

  std::vector<FactEvent> EventsFor(const FactRef& ref,
                                   CommitSeq snapshot) const;
  std::vector<EdgeIdentity> EdgeIdentities(CommitSeq snapshot) const;

  // Merges a projection boundary stream and all delta events visible at the
  // requested cut, resolving same-valid-time corrections by greatest commit.
  static StatusOr<std::vector<CorrectedBoundary>> MergeBoundaries(
      const std::vector<CorrectedBoundary>& base,
      const std::vector<FactEvent>& delta, CommitSeq snapshot);

 private:
  class LifecycleTransitionGuard;
  struct FactRefHash {
    size_t operator()(const FactRef& ref) const noexcept;
  };
  void SetMissingLocked(CommitSeq missing);
  Status IndexLocked(const QueryDeltaCommit& commit);
  void WorkerMain();

  mutable std::mutex mutex_;
  mutable std::mutex queue_mutex_;
  QueryDeltaOptions options_;
  CommitSeq indexed_through_;
  std::atomic<uint64_t> visible_seq_value_{0};
  std::atomic<uint64_t> first_missing_value_{0};
  uint64_t memory_bytes_ = 0;
  size_t queue_size_ = 0;
  std::deque<QueryDeltaCommit> published_queue_;
  CommitSeq enqueued_through_;
  std::condition_variable published_cv_;
  bool worker_indexing_ = false;
  bool lifecycle_transition_ = false;
  std::thread worker_;
  bool stopping_ = false;
  bool hard_limit_reached_ = false;
  bool soft_lag_reached_ = false;
  bool soft_memory_reached_ = false;
  std::vector<QueryDeltaCommit> commits_;
  std::deque<std::shared_ptr<const QueryDeltaCommit>> lease_commits_;
  std::deque<std::shared_ptr<const DeltaChunk>> chunks_;
  std::shared_ptr<QueryDeltaEpochState> epoch_state_ =
      std::make_shared<QueryDeltaEpochState>();
  mutable std::unordered_map<
      uint64_t, std::weak_ptr<const std::vector<QueryDeltaFactRef>>>
      lease_index_cache_;
  std::unordered_map<FactRef, std::vector<FactEvent>, FactRefHash> chains_;
  std::vector<std::pair<CommitSeq, EdgeIdentity>> edge_identities_;
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_PROJECTION_QUERY_DELTA_H_
