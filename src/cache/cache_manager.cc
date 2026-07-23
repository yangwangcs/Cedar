// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/cache/cache_manager.h"

#include <algorithm>
#include <list>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "cedar/runtime/resource_profile.h"

namespace cedar {
namespace {

constexpr size_t kMaximumStreamingAdmissionHistory = 4096;

std::string CanonicalKey(const CacheKey& key) {
  return std::to_string(static_cast<uint32_t>(key.kind)) + ":" + key.id;
}

}  // namespace

struct CacheManager::State {
  struct Entry {
    CacheKind kind;
    std::shared_ptr<const std::string> value;
    uint64_t bytes;
    std::list<std::string>::iterator lru_position;
    std::shared_ptr<ResourceLease> reservation;
  };

  explicit State(uint64_t capacity, ResourceGovernor* resource_governor)
      : capacity_bytes(capacity), governor(resource_governor) {}

  uint64_t capacity_bytes;
  ResourceGovernor* governor;
  mutable std::mutex mutex;
  std::list<std::string> lru;
  std::unordered_map<std::string, Entry> entries;
  std::unordered_map<std::string, uint8_t> streaming_seen;
  CacheStats stats;
};

CacheManager::CacheManager(uint64_t capacity_bytes, ResourceGovernor* governor)
    : state_(std::make_unique<State>(capacity_bytes, governor)) {}

CacheManager::~CacheManager() {
  std::lock_guard<std::mutex> lock(state_->mutex);
  state_->entries.clear();
  state_->lru.clear();
  state_->stats.resident_bytes = 0;
  state_->stats.entries = 0;
}

CacheHandle CacheManager::Lookup(const CacheKey& key) {
  std::lock_guard<std::mutex> lock(state_->mutex);
  const size_t kind = static_cast<size_t>(key.kind);
  const auto found = state_->entries.find(CanonicalKey(key));
  if (found == state_->entries.end()) {
    ++state_->stats.misses;
    if (kind < kCacheKindCount) ++state_->stats.misses_by_kind[kind];
    return {};
  }
  state_->lru.splice(state_->lru.begin(), state_->lru, found->second.lru_position);
  ++state_->stats.hits;
  if (kind < kCacheKindCount) ++state_->stats.hits_by_kind[kind];
  if (found->second.value.use_count() > 1) state_->stats.pinned_bytes += found->second.bytes;
  return CacheHandle(found->second.value, found->second.bytes, found->second.reservation);
}

StatusOr<CacheInsertResult> CacheManager::Insert(CacheKey key,
                                                  std::shared_ptr<const std::string> value,
                                                  CacheAdmission admission) {
  if (!value || key.id.empty()) {
    return Status::InvalidArgument("cache manager", "cache key and value are required");
  }
  const uint64_t bytes = value->size();
  const std::string canonical = CanonicalKey(key);
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (key.kind == CacheKind::kPage) ++state_->stats.page_insert_requests;
  CacheInsertResult result;
  if (state_->capacity_bytes == 0 || bytes > state_->capacity_bytes) {
    ++state_->stats.bypasses;
    result.bypassed = true;
    return result;
  }
  if (admission == CacheAdmission::kStreamingScan) {
    const auto seen = state_->streaming_seen.find(canonical);
    if (seen == state_->streaming_seen.end()) {
      if (state_->streaming_seen.size() < kMaximumStreamingAdmissionHistory) {
        state_->streaming_seen.emplace(canonical, 1);
      }
      ++state_->stats.bypasses;
      result.bypassed = true;
      return result;
    }
    state_->streaming_seen.erase(canonical);
    ++state_->stats.promotions;
  }
  const auto existing = state_->entries.find(canonical);
  const uint64_t replaced_bytes = existing == state_->entries.end() ? 0 : existing->second.bytes;
  for (auto candidate = state_->lru.rbegin();
       state_->stats.resident_bytes - replaced_bytes + bytes > state_->capacity_bytes &&
       candidate != state_->lru.rend();) {
    const auto found = state_->entries.find(*candidate);
    if (found == state_->entries.end()) {
      candidate = std::list<std::string>::reverse_iterator(state_->lru.erase(std::next(candidate).base()));
      continue;
    }
    if (found == existing) {
      ++candidate;
      continue;
    }
    if (found->second.value.use_count() != 1) {
      ++candidate;
      continue;
    }
    const uint64_t evicted_bytes = found->second.bytes;
    candidate = std::list<std::string>::reverse_iterator(
        state_->lru.erase(std::next(candidate).base()));
    state_->entries.erase(found);
    state_->stats.resident_bytes -= evicted_bytes;
    ++state_->stats.evictions;
    ++result.evicted_entries;
  }
  if (state_->stats.resident_bytes - replaced_bytes + bytes > state_->capacity_bytes) {
    return Status::QueryMemoryLimit("cache manager", "cache capacity is pinned by active snapshots");
  }
  std::shared_ptr<ResourceLease> reservation;
  if (state_->governor != nullptr) {
    auto acquired = state_->governor->Acquire(ResourceProfile{bytes});
    if (!acquired.ok()) return acquired.status();
    reservation = std::make_shared<ResourceLease>(acquired.ConsumeValueOrDie());
  }
  if (existing != state_->entries.end()) {
    state_->lru.erase(existing->second.lru_position);
    state_->stats.resident_bytes -= existing->second.bytes;
    state_->entries.erase(existing);
  }
  state_->lru.push_front(canonical);
  state_->entries.emplace(canonical, State::Entry{key.kind, std::move(value), bytes,
                                                   state_->lru.begin(), std::move(reservation)});
  state_->stats.resident_bytes += bytes;
  state_->stats.entries = state_->entries.size();
  if (key.kind == CacheKind::kPage) ++state_->stats.page_admissions;
  result.admitted = true;
  return result;
}

void CacheManager::EvictAllUnpinned() {
  std::lock_guard<std::mutex> lock(state_->mutex);
  for (auto candidate = state_->lru.rbegin(); candidate != state_->lru.rend();) {
    const auto found = state_->entries.find(*candidate);
    if (found == state_->entries.end() || found->second.value.use_count() != 1) {
      ++candidate;
      continue;
    }
    const uint64_t bytes = found->second.bytes;
    candidate = std::list<std::string>::reverse_iterator(
        state_->lru.erase(std::next(candidate).base()));
    state_->entries.erase(found);
    state_->stats.resident_bytes -= bytes;
    ++state_->stats.evictions;
  }
  state_->stats.entries = state_->entries.size();
}

CacheStats CacheManager::stats() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  CacheStats result = state_->stats;
  result.pinned_bytes = 0;
  for (const auto& entry : state_->entries) {
    if (entry.second.value.use_count() > 1) result.pinned_bytes += entry.second.bytes;
  }
  return result;
}

}  // namespace cedar
