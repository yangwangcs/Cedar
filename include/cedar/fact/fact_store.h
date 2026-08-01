// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_FACT_STORE_H_
#define CEDAR_FACT_FACT_STORE_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"
#include "cedar/fact/meta_codec.h"

namespace cedar {

class FactStoreImpl;

struct FactStoreOptions {
  std::string path;
  uint64_t write_buffer_bytes = 64ULL * 1024ULL * 1024ULL;
  uint64_t blob_threshold_bytes = 4096;
};

struct SnapshotOptions {
  std::optional<CommitSeq> as_of;
};

struct StoreCommitBatch {
  TxnId txn_id;
  uint64_t system_hlc = 0;
  std::vector<PendingFactMutation> mutations;
  std::vector<EdgeIdentity> edge_identities;

  Status Validate() const;
};

struct StoreCommitResult {
  CommitSeq commit_seq;
  uint64_t system_hlc = 0;

  constexpr bool operator==(const StoreCommitResult&) const = default;
};

struct IdLease {
  IdKind kind = IdKind::kVertex;
  uint64_t first_id = 0;
  uint64_t count = 0;

  constexpr bool operator==(const IdLease&) const = default;
};

class FactPrefix {
 public:
  static FactPrefix Exact(FactRef ref);
  static FactPrefix Family(FactFamily family, PropertyId property_id);

  FactFamily family() const { return family_; }
  PropertyId property_id() const { return property_id_; }
  const std::optional<uint64_t>& entity_id() const { return entity_id_; }
  Status Validate() const;

 private:
  FactPrefix(FactFamily family, PropertyId property_id,
             std::optional<uint64_t> entity_id)
      : family_(family), property_id_(property_id), entity_id_(entity_id) {}

  FactFamily family_;
  PropertyId property_id_;
  std::optional<uint64_t> entity_id_;
};

class StoreSnapshot {
 public:
  ~StoreSnapshot();
  StoreSnapshot(StoreSnapshot&&) noexcept;
  StoreSnapshot& operator=(StoreSnapshot&&) noexcept;

  StoreSnapshot(const StoreSnapshot&) = delete;
  StoreSnapshot& operator=(const StoreSnapshot&) = delete;

  CommitSeq commit_seq() const;
  CommitSeq oldest_readable_seq() const;

 private:
  class State;
  explicit StoreSnapshot(std::unique_ptr<State> state);

  std::unique_ptr<State> state_;

  friend class FactStore;
};

using FactVisitor = std::function<Status(const FactEvent&)>;

class FactStore {
 public:
  explicit FactStore(FactStoreOptions options);
  ~FactStore();

  FactStore(const FactStore&) = delete;
  FactStore& operator=(const FactStore&) = delete;

  Status Open();
  Status Close();
  StatusOr<StoreSnapshot> BeginSnapshot(SnapshotOptions options = {}) const;
  StatusOr<std::optional<FactEvent>> Read(const StoreSnapshot& snapshot,
                                          const FactRef& ref,
                                          ValidTime valid_time) const;
  Status Scan(const StoreSnapshot& snapshot, const FactPrefix& prefix,
              const FactVisitor& visitor) const;
  StatusOr<StoreCommitResult> Commit(const StoreCommitBatch& batch);
  StatusOr<IdLease> LeaseIds(IdKind kind, uint64_t count);
  StatusOr<PropertyDefinition> RegisterProperty(PropertyDefinition definition);
  StatusOr<std::optional<PropertyDefinition>> LookupProperty(
      PropertyId property_id, uint32_t schema_epoch = 0) const;
  CommitSeq visible_seq() const;
  StatusOr<std::optional<StoreCommitResult>> ResolveTransaction(
      TxnId txn_id) const;

 private:
  FactStoreOptions options_;
  mutable std::mutex lifecycle_mutex_;
  std::shared_ptr<FactStoreImpl> impl_;
};

}  // namespace cedar

#endif  // CEDAR_FACT_FACT_STORE_H_
