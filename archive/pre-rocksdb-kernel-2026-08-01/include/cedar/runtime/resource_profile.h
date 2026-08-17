// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_RUNTIME_RESOURCE_PROFILE_H_
#define CEDAR_RUNTIME_RESOURCE_PROFILE_H_

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <utility>

#include "cedar/core/status.h"

namespace cedar {

// The first four fields retain the original compact construction order used by
// callers. The remaining fields express the independent resource dimensions
// needed by admission and maintenance scheduling.
struct ResourceProfile {
  uint64_t memory_bytes = 0;
  uint64_t io_tokens = 0;
  uint64_t descriptors = 0;
  uint64_t temporary_bytes = 0;
  uint64_t cpu_slots = 0;
  uint64_t sequential_read_bytes = 0;
  uint64_t random_read_ops = 0;
  uint64_t write_bytes = 0;
  uint64_t metadata_ops = 0;
};

inline ResourceProfile AddResources(const ResourceProfile& left, const ResourceProfile& right) {
  return ResourceProfile{left.memory_bytes + right.memory_bytes,
                         left.io_tokens + right.io_tokens,
                         left.descriptors + right.descriptors,
                         left.temporary_bytes + right.temporary_bytes,
                         left.cpu_slots + right.cpu_slots,
                         left.sequential_read_bytes + right.sequential_read_bytes,
                         left.random_read_ops + right.random_read_ops,
                         left.write_bytes + right.write_bytes,
                         left.metadata_ops + right.metadata_ops};
}

struct ResourceGovernorState {
  ResourceGovernorState(ResourceProfile resource_limits, ResourceProfile resource_critical_reserve)
      : limits(resource_limits), critical_reserve(resource_critical_reserve) {}

  ResourceProfile limits;
  ResourceProfile critical_reserve;
  ResourceProfile used;
  ResourceProfile noncritical_used;
  mutable std::mutex mutex;
};

class ResourceGovernor;
class ResourceGovernorExtension;

class ResourceLease {
 public:
  ResourceLease() = default;
  ResourceLease(const ResourceLease&) = delete;
  ResourceLease& operator=(const ResourceLease&) = delete;
  ResourceLease(ResourceLease&& other) noexcept { *this = std::move(other); }
  ResourceLease& operator=(ResourceLease&& other) noexcept;
  ~ResourceLease();

  Status Extend(const ResourceProfile& additional,
                bool commit_critical = false);
  void Release();
  bool active() const { return state_ != nullptr; }
  const ResourceProfile& reservation() const { return reservation_; }

 private:
  friend class ResourceGovernor;
  friend class ResourceGovernorExtension;
  ResourceLease(std::shared_ptr<ResourceGovernorState> state, ResourceProfile reservation)
      : ResourceLease(std::move(state), reservation, false) {}
  ResourceLease(std::shared_ptr<ResourceGovernorState> state,
                ResourceProfile reservation, bool commit_critical)
      : state_(std::move(state)), reservation_(reservation),
        noncritical_reservation_(commit_critical ? ResourceProfile{}
                                                 : reservation) {}

  std::shared_ptr<ResourceGovernorState> state_;
  ResourceProfile reservation_{};
  ResourceProfile noncritical_reservation_{};
};

class ResourceGovernor {
 public:
  explicit ResourceGovernor(ResourceProfile limits, ResourceProfile critical_reserve = {})
      : state_(std::make_shared<ResourceGovernorState>(limits, critical_reserve)) {}
  ResourceGovernor(const ResourceGovernor&) = delete;
  ResourceGovernor& operator=(const ResourceGovernor&) = delete;

  // Non-critical admissions cannot consume the finish reserve for WAL,
  // DecisionLog and Manifest work. Commit-critical callers pass true.
  Status Reserve(const ResourceProfile& request, bool commit_critical = false) {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (!FitsLocked(*state_, request, commit_critical)) {
      return Status::QueryMemoryLimit("resource governor", "resource reservation exceeds limit");
    }
    state_->used = AddResources(state_->used, request);
    if (!commit_critical) {
      state_->noncritical_used =
          AddResources(state_->noncritical_used, request);
    }
    return Status::OK();
  }

  StatusOr<ResourceLease> Acquire(const ResourceProfile& request,
                                  bool commit_critical = false) {
    const Status status = Reserve(request, commit_critical);
    if (!status.ok()) return status;
    return ResourceLease(state_, request, commit_critical);
  }

  std::shared_ptr<ResourceGovernorExtension> SharedExtension() const;

  void Release(const ResourceProfile& request, bool commit_critical = false) {
    std::lock_guard<std::mutex> lock(state_->mutex);
    SubtractLocked(&state_->used, request);
    if (!commit_critical) {
      SubtractLocked(&state_->noncritical_used, request);
    }
  }

  ResourceProfile used() const {
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->used;
  }
  ResourceProfile limits() const {
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->limits;
  }
  ResourceProfile available(bool commit_critical = false) const {
    std::lock_guard<std::mutex> lock(state_->mutex);
    const ResourceProfile total_available =
        Subtract(state_->limits, state_->used);
    if (commit_critical) return total_available;
    const ResourceProfile shared_cap =
        Subtract(state_->limits, state_->critical_reserve);
    return Minimum(total_available,
                   Subtract(shared_cap, state_->noncritical_used));
  }

 private:
  friend class ResourceLease;
  friend class ResourceGovernorExtension;
  static ResourceProfile Subtract(const ResourceProfile& left, const ResourceProfile& right) {
    return ResourceProfile{left.memory_bytes > right.memory_bytes ? left.memory_bytes - right.memory_bytes : 0,
                           left.io_tokens > right.io_tokens ? left.io_tokens - right.io_tokens : 0,
                           left.descriptors > right.descriptors ? left.descriptors - right.descriptors : 0,
                           left.temporary_bytes > right.temporary_bytes ? left.temporary_bytes - right.temporary_bytes : 0,
                           left.cpu_slots > right.cpu_slots ? left.cpu_slots - right.cpu_slots : 0,
                           left.sequential_read_bytes > right.sequential_read_bytes ? left.sequential_read_bytes - right.sequential_read_bytes : 0,
                           left.random_read_ops > right.random_read_ops ? left.random_read_ops - right.random_read_ops : 0,
                           left.write_bytes > right.write_bytes ? left.write_bytes - right.write_bytes : 0,
                           left.metadata_ops > right.metadata_ops ? left.metadata_ops - right.metadata_ops : 0};
  }
  static ResourceProfile Minimum(const ResourceProfile& left,
                                 const ResourceProfile& right) {
    return ResourceProfile{
        std::min(left.memory_bytes, right.memory_bytes),
        std::min(left.io_tokens, right.io_tokens),
        std::min(left.descriptors, right.descriptors),
        std::min(left.temporary_bytes, right.temporary_bytes),
        std::min(left.cpu_slots, right.cpu_slots),
        std::min(left.sequential_read_bytes, right.sequential_read_bytes),
        std::min(left.random_read_ops, right.random_read_ops),
        std::min(left.write_bytes, right.write_bytes),
        std::min(left.metadata_ops, right.metadata_ops)};
  }
  static void SubtractLocked(ResourceProfile* target, const ResourceProfile& released) {
    *target = Subtract(*target, released);
  }
  static bool FitsDimension(uint64_t used, uint64_t requested, uint64_t limit) {
    return requested <= limit && used <= limit - requested;
  }
  static bool FitsLocked(const ResourceGovernorState& state, const ResourceProfile& request,
                         bool commit_critical) {
    const auto fits = [&](const ResourceProfile& used,
                          const ResourceProfile& cap) {
      return FitsDimension(used.memory_bytes, request.memory_bytes,
                           cap.memory_bytes) &&
          FitsDimension(used.io_tokens, request.io_tokens, cap.io_tokens) &&
          FitsDimension(used.descriptors, request.descriptors,
                        cap.descriptors) &&
          FitsDimension(used.temporary_bytes, request.temporary_bytes,
                        cap.temporary_bytes) &&
          FitsDimension(used.cpu_slots, request.cpu_slots, cap.cpu_slots) &&
          FitsDimension(used.sequential_read_bytes,
                        request.sequential_read_bytes,
                        cap.sequential_read_bytes) &&
          FitsDimension(used.random_read_ops, request.random_read_ops,
                        cap.random_read_ops) &&
          FitsDimension(used.write_bytes, request.write_bytes,
                        cap.write_bytes) &&
          FitsDimension(used.metadata_ops, request.metadata_ops,
                        cap.metadata_ops);
    };
    if (!fits(state.used, state.limits)) return false;
    if (commit_critical) return true;
    return fits(state.noncritical_used,
                Subtract(state.limits, state.critical_reserve));
  }

  std::shared_ptr<ResourceGovernorState> state_;
};

class ResourceGovernorExtension {
 public:
  StatusOr<ResourceLease> Acquire(const ResourceProfile& request,
                                  bool commit_critical = false) const {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (!ResourceGovernor::FitsLocked(*state_, request, commit_critical)) {
      return Status::QueryMemoryLimit(
          "resource governor", "resource reservation exceeds limit");
    }
    state_->used = AddResources(state_->used, request);
    if (!commit_critical) {
      state_->noncritical_used =
          AddResources(state_->noncritical_used, request);
    }
    return ResourceLease(state_, request, commit_critical);
  }

 private:
  friend class ResourceGovernor;
  explicit ResourceGovernorExtension(
      std::shared_ptr<ResourceGovernorState> state)
      : state_(std::move(state)) {}

  std::shared_ptr<ResourceGovernorState> state_;
};

inline std::shared_ptr<ResourceGovernorExtension>
ResourceGovernor::SharedExtension() const {
  return std::shared_ptr<ResourceGovernorExtension>(
      new ResourceGovernorExtension(state_));
}

inline ResourceLease& ResourceLease::operator=(ResourceLease&& other) noexcept {
  if (this == &other) return *this;
  Release();
  state_ = std::move(other.state_);
  reservation_ = other.reservation_;
  noncritical_reservation_ = other.noncritical_reservation_;
  other.reservation_ = {};
  other.noncritical_reservation_ = {};
  return *this;
}
inline ResourceLease::~ResourceLease() { Release(); }
inline Status ResourceLease::Extend(const ResourceProfile& additional,
                                    bool commit_critical) {
  if (state_ == nullptr) {
    return Status::InvalidArgument("resource lease", "cannot extend an inactive lease");
  }
  std::lock_guard<std::mutex> lock(state_->mutex);
  constexpr uint64_t kMaximum = std::numeric_limits<uint64_t>::max();
  const bool reservation_fits =
      ResourceGovernor::FitsDimension(reservation_.memory_bytes,
                                      additional.memory_bytes, kMaximum) &&
      ResourceGovernor::FitsDimension(reservation_.io_tokens,
                                      additional.io_tokens, kMaximum) &&
      ResourceGovernor::FitsDimension(reservation_.descriptors,
                                      additional.descriptors, kMaximum) &&
      ResourceGovernor::FitsDimension(reservation_.temporary_bytes,
                                      additional.temporary_bytes, kMaximum) &&
      ResourceGovernor::FitsDimension(reservation_.cpu_slots,
                                      additional.cpu_slots, kMaximum) &&
      ResourceGovernor::FitsDimension(reservation_.sequential_read_bytes,
                                      additional.sequential_read_bytes,
                                      kMaximum) &&
      ResourceGovernor::FitsDimension(reservation_.random_read_ops,
                                      additional.random_read_ops, kMaximum) &&
      ResourceGovernor::FitsDimension(reservation_.write_bytes,
                                      additional.write_bytes, kMaximum) &&
      ResourceGovernor::FitsDimension(reservation_.metadata_ops,
                                      additional.metadata_ops, kMaximum);
  if (!reservation_fits ||
      !ResourceGovernor::FitsLocked(*state_, additional, commit_critical)) {
    return Status::QueryMemoryLimit(
        "resource governor", "resource reservation exceeds limit");
  }
  const ResourceProfile extended_used = AddResources(state_->used, additional);
  const ResourceProfile extended_reservation =
      AddResources(reservation_, additional);
  state_->used = extended_used;
  if (!commit_critical) {
    state_->noncritical_used =
        AddResources(state_->noncritical_used, additional);
    noncritical_reservation_ =
        AddResources(noncritical_reservation_, additional);
  }
  reservation_ = extended_reservation;
  return Status::OK();
}
inline void ResourceLease::Release() {
  if (state_ != nullptr) {
    std::lock_guard<std::mutex> lock(state_->mutex);
    ResourceGovernor::SubtractLocked(&state_->used, reservation_);
    ResourceGovernor::SubtractLocked(&state_->noncritical_used,
                                     noncritical_reservation_);
  }
  state_.reset();
  reservation_ = {};
  noncritical_reservation_ = {};
}

}  // namespace cedar

#endif  // CEDAR_RUNTIME_RESOURCE_PROFILE_H_
