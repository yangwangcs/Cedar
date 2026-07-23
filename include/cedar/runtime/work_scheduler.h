// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_RUNTIME_WORK_SCHEDULER_H_
#define CEDAR_RUNTIME_WORK_SCHEDULER_H_

#include <algorithm>
#include <array>
#include <cstdint>
#include <deque>
#include <iterator>
#include <limits>
#include <mutex>
#include <optional>
#include <unordered_set>

#include "cedar/core/status.h"

namespace cedar {

enum class WorkClass : uint8_t {
  kCommitCritical,
  kForegroundWrite,
  kPointRead,
  kInteractiveQuery,
  kAnalyticalQuery,
  kFlush,
  kCompactionUrgent,
  kCompactionNormal,
  kIndexBuild,
  kStatsMerge,
  kBlobGc,
  kRecovery,
  kShutdown,
  kForeground = kInteractiveQuery,
  kMaintenance = kCompactionNormal,
};

struct ExecutableTaskId {
  uint64_t value = 0;
  friend bool operator==(ExecutableTaskId lhs, ExecutableTaskId rhs) {
    return lhs.value == rhs.value;
  }
  friend bool operator!=(ExecutableTaskId lhs, ExecutableTaskId rhs) {
    return !(lhs == rhs);
  }
};

template <typename Id>
struct ScheduledWork {
  Id id;
  WorkClass work_class = WorkClass::kAnalyticalQuery;
  uint64_t enqueue_sequence = 0;
  uint64_t deadline_sequence = 0;
};

using ScheduledExecutableWork = ScheduledWork<ExecutableTaskId>;
// WorkScheduler selects work only. Executable callbacks and their completion
// state are owned by WorkExecutionService.
class WorkScheduler {
 public:
  explicit WorkScheduler(uint64_t aging_dispatches = 64)
      : aging_dispatches_(aging_dispatches == 0 ? 1 : aging_dispatches) {}

  StatusOr<ExecutableTaskId> AllocateExecutableTaskId() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (next_executable_id_ == std::numeric_limits<uint64_t>::max()) {
      return Status::ResourceExhausted("work scheduler",
                                       "task identifier space exhausted");
    }
    return ExecutableTaskId{next_executable_id_++};
  }

  void EnqueueExecutable(WorkClass kind, ExecutableTaskId id,
                         uint64_t deadline_sequence = 0) {
    std::lock_guard<std::mutex> lock(mutex_);
    EnqueueLocked(&executable_, kind, id, deadline_sequence);
  }

  void CancelExecutable(ExecutableTaskId id) {
    std::lock_guard<std::mutex> lock(mutex_);
    executable_.cancelled.insert(id.value);
  }

  std::optional<ScheduledExecutableWork> NextExecutableWork() {
    std::lock_guard<std::mutex> lock(mutex_);
    return NextWorkLocked(&executable_);
  }

  bool HasExecutableWork() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return HasWorkLocked(executable_);
  }

  size_t queued_executable(WorkClass kind) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return executable_.queues[QueueIndex(kind)].size();
  }

 private:
  enum class FairLane : uint8_t {
    kForeground = 0,
    kEssentialMaintenance = 1,
    kAnalytical = 2,
    kOptionalMaintenance = 3,
  };

  static constexpr size_t kQueueCount = 13;
  static constexpr size_t kFairLaneCount = 4;

  template <typename Id>
  struct QueueState {
    std::array<std::deque<ScheduledWork<Id>>, kQueueCount> queues;
    std::unordered_set<uint64_t> cancelled;
    uint64_t sequence = 0;
    uint64_t dispatch_sequence = 0;
    size_t fair_lane_cursor = 0;
    std::array<uint32_t, kFairLaneCount> deficits{};
  };

  static size_t QueueIndex(WorkClass kind) {
    switch (kind) {
      case WorkClass::kCommitCritical: return 0;
      case WorkClass::kRecovery: return 1;
      case WorkClass::kShutdown: return 2;
      case WorkClass::kForegroundWrite: return 3;
      case WorkClass::kPointRead: return 4;
      case WorkClass::kInteractiveQuery: return 5;
      case WorkClass::kFlush: return 6;
      case WorkClass::kCompactionUrgent: return 7;
      case WorkClass::kAnalyticalQuery: return 8;
      case WorkClass::kCompactionNormal: return 9;
      case WorkClass::kIndexBuild: return 10;
      case WorkClass::kStatsMerge: return 11;
      case WorkClass::kBlobGc: return 12;
    }
    return 12;
  }

  static const std::array<WorkClass, 3>& CriticalOrder() {
    static const std::array<WorkClass, 3> order = {
        WorkClass::kCommitCritical, WorkClass::kRecovery, WorkClass::kShutdown};
    return order;
  }

  static uint32_t LaneWeight(FairLane lane) {
    switch (lane) {
      case FairLane::kForeground: return 4;
      case FairLane::kEssentialMaintenance: return 2;
      case FairLane::kAnalytical: return 2;
      case FairLane::kOptionalMaintenance: return 1;
    }
    return 1;
  }

  static const std::array<WorkClass, 3>& ClassesForLane(FairLane lane) {
    static const std::array<WorkClass, 3> foreground = {
        WorkClass::kForegroundWrite, WorkClass::kPointRead,
        WorkClass::kInteractiveQuery};
    static const std::array<WorkClass, 3> essential = {
        WorkClass::kFlush, WorkClass::kCompactionUrgent, WorkClass::kFlush};
    static const std::array<WorkClass, 3> analytical = {
        WorkClass::kAnalyticalQuery, WorkClass::kAnalyticalQuery,
        WorkClass::kAnalyticalQuery};
    static const std::array<WorkClass, 3> optional = {
        WorkClass::kCompactionNormal, WorkClass::kIndexBuild,
        WorkClass::kStatsMerge};
    switch (lane) {
      case FairLane::kForeground: return foreground;
      case FairLane::kEssentialMaintenance: return essential;
      case FairLane::kAnalytical: return analytical;
      case FairLane::kOptionalMaintenance: return optional;
    }
    return optional;
  }

  template <typename Id>
  void EnqueueLocked(QueueState<Id>* state, WorkClass kind, Id id,
                     uint64_t deadline_sequence) {
    state->queues[QueueIndex(kind)].push_back(
        ScheduledWork<Id>{id, kind, ++state->sequence, deadline_sequence});
  }

  template <typename Id>
  static void DiscardCancelledLocked(QueueState<Id>* state,
                                     std::deque<ScheduledWork<Id>>* queue) {
    while (!queue->empty() &&
           state->cancelled.erase(queue->front().id.value) != 0) {
      queue->pop_front();
    }
  }

  template <typename Id>
  static std::optional<ScheduledWork<Id>> TakeFromClassLocked(
      QueueState<Id>* state, WorkClass kind) {
    auto& queue = state->queues[QueueIndex(kind)];
    DiscardCancelledLocked(state, &queue);
    if (queue.empty()) return std::nullopt;
    if (kind == WorkClass::kInteractiveQuery) {
      auto selected = queue.begin();
      for (auto candidate = std::next(queue.begin()); candidate != queue.end();
           ++candidate) {
        if (state->cancelled.count(candidate->id.value) != 0) continue;
        const bool selected_has_deadline = selected->deadline_sequence != 0;
        const bool candidate_has_deadline = candidate->deadline_sequence != 0;
        if ((candidate_has_deadline && !selected_has_deadline) ||
            (candidate_has_deadline && selected_has_deadline &&
             candidate->deadline_sequence < selected->deadline_sequence)) {
          selected = candidate;
        }
      }
      ScheduledWork<Id> work = *selected;
      queue.erase(selected);
      return work;
    }
    ScheduledWork<Id> work = queue.front();
    queue.pop_front();
    return work;
  }

  template <typename Id>
  static bool ClassHasWorkLocked(QueueState<Id>* state, WorkClass kind) {
    auto& queue = state->queues[QueueIndex(kind)];
    DiscardCancelledLocked(state, &queue);
    return !queue.empty();
  }

  template <typename Id>
  static bool LaneHasWorkLocked(QueueState<Id>* state, FairLane lane) {
    for (WorkClass kind : ClassesForLane(lane)) {
      if (ClassHasWorkLocked(state, kind)) return true;
    }
    return lane == FairLane::kOptionalMaintenance &&
           ClassHasWorkLocked(state, WorkClass::kBlobGc);
  }

  template <typename Id>
  static std::optional<ScheduledWork<Id>> TakeFromLaneLocked(
      QueueState<Id>* state, FairLane lane) {
    for (WorkClass kind : ClassesForLane(lane)) {
      if (const auto work = TakeFromClassLocked(state, kind); work.has_value()) {
        return work;
      }
    }
    if (lane == FairLane::kOptionalMaintenance) {
      return TakeFromClassLocked(state, WorkClass::kBlobGc);
    }
    return std::nullopt;
  }

  template <typename Id>
  std::optional<ScheduledWork<Id>> TakeAgedLocked(QueueState<Id>* state) const {
    WorkClass selected_kind = WorkClass::kBlobGc;
    uint64_t selected_age = 0;
    bool found = false;
    for (size_t index = 0; index < kQueueCount; ++index) {
      auto& queue = state->queues[index];
      DiscardCancelledLocked(state, &queue);
      if (queue.empty()) continue;
      const uint64_t age = state->dispatch_sequence >= queue.front().enqueue_sequence
          ? state->dispatch_sequence - queue.front().enqueue_sequence
          : 0;
      if (age >= aging_dispatches_ && (!found || age > selected_age)) {
        found = true;
        selected_age = age;
        selected_kind = queue.front().work_class;
      }
    }
    return found ? TakeFromClassLocked(state, selected_kind) : std::nullopt;
  }

  template <typename Id>
  std::optional<ScheduledWork<Id>> NextWorkLocked(QueueState<Id>* state) const {
    ++state->dispatch_sequence;
    for (WorkClass kind : CriticalOrder()) {
      if (const auto work = TakeFromClassLocked(state, kind); work.has_value()) {
        return work;
      }
    }
    if (const auto aged = TakeAgedLocked(state); aged.has_value()) return aged;
    for (size_t attempts = 0; attempts < kFairLaneCount * 2; ++attempts) {
      const FairLane lane = static_cast<FairLane>(state->fair_lane_cursor);
      if (!LaneHasWorkLocked(state, lane)) {
        state->deficits[state->fair_lane_cursor] = 0;
        state->fair_lane_cursor =
            (state->fair_lane_cursor + 1) % kFairLaneCount;
        continue;
      }
      if (state->deficits[state->fair_lane_cursor] == 0) {
        state->deficits[state->fair_lane_cursor] = LaneWeight(lane);
      }
      const auto work = TakeFromLaneLocked(state, lane);
      if (!work.has_value()) {
        state->deficits[state->fair_lane_cursor] = 0;
        state->fair_lane_cursor =
            (state->fair_lane_cursor + 1) % kFairLaneCount;
        continue;
      }
      --state->deficits[state->fair_lane_cursor];
      if (state->deficits[state->fair_lane_cursor] == 0) {
        state->fair_lane_cursor =
            (state->fair_lane_cursor + 1) % kFairLaneCount;
      }
      return work;
    }
    return std::nullopt;
  }

  template <typename Id>
  static bool HasWorkLocked(const QueueState<Id>& state) {
    for (const auto& queue : state.queues) {
      for (const auto& work : queue) {
        if (state.cancelled.count(work.id.value) == 0) return true;
      }
    }
    return false;
  }

  mutable std::mutex mutex_;
  uint64_t next_executable_id_ = 1;
  mutable QueueState<ExecutableTaskId> executable_;
  uint64_t aging_dispatches_;
};

}  // namespace cedar

#endif  // CEDAR_RUNTIME_WORK_SCHEDULER_H_
