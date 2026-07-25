// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_CACHE_CACHE_MANAGER_H_
#define CEDAR_CACHE_CACHE_MANAGER_H_

#include <array>
#include <cstdint>
#include <memory>
#include <string>

#include "cedar/core/status.h"

namespace cedar {

class ResourceGovernor;
class ResourceLease;

enum class CacheKind : uint8_t {
  kMetadata,
  kPage,
  kBlobLocation,
  kBlobValue,
};
enum class CacheAdmission : uint8_t { kMetadata, kPointRead, kStreamingScan };
constexpr size_t kCacheKindCount = 4;

struct CacheKey {
  CacheKind kind = CacheKind::kPage;
  std::string id;
};
struct CacheStats {
  uint64_t resident_bytes = 0;
  uint64_t peak_resident_bytes = 0;
  uint64_t entries = 0;
  uint64_t hits = 0;
  uint64_t misses = 0;
  uint64_t bypasses = 0;
  uint64_t promotions = 0;
  uint64_t page_insert_requests = 0;
  uint64_t page_admissions = 0;
  uint64_t evictions = 0;
  uint64_t pinned_bytes = 0;
  std::array<uint64_t, kCacheKindCount> hits_by_kind{};
  std::array<uint64_t, kCacheKindCount> misses_by_kind{};
};
struct CacheInsertResult {
  bool admitted = false;
  bool bypassed = false;
  uint64_t evicted_entries = 0;
};

// A cache handle remains valid after an entry is evicted. This is the cache
// side of snapshot pin safety: eviction drops residency, never a reader's data.
class CacheHandle {
 public:
  CacheHandle() = default;
  const std::shared_ptr<const std::string>& value() const { return value_; }
  uint64_t bytes() const { return bytes_; }
  explicit operator bool() const { return static_cast<bool>(value_); }
 private:
  friend class CacheManager;
  CacheHandle(std::shared_ptr<const std::string> value, uint64_t bytes,
              std::shared_ptr<ResourceLease> reservation)
      : value_(std::move(value)), bytes_(bytes), reservation_(std::move(reservation)) {}
  std::shared_ptr<const std::string> value_;
  uint64_t bytes_ = 0;
  std::shared_ptr<ResourceLease> reservation_;
};

class CacheManager {
 public:
  CacheManager(uint64_t capacity_bytes, ResourceGovernor* governor = nullptr);
  CacheManager(const CacheManager&) = delete;
  CacheManager& operator=(const CacheManager&) = delete;
  ~CacheManager();

  CacheHandle Lookup(const CacheKey& key);
  StatusOr<CacheInsertResult> Insert(CacheKey key, std::shared_ptr<const std::string> value,
                                     CacheAdmission admission);
  void EvictAllUnpinned();
  CacheStats stats() const;

 private:
  struct State;
  std::unique_ptr<State> state_;
};

}  // namespace cedar

#endif  // CEDAR_CACHE_CACHE_MANAGER_H_
