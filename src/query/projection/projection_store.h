#ifndef CEDAR_QUERY_PROJECTION_STORE_H_
#define CEDAR_QUERY_PROJECTION_STORE_H_

#include <functional>
#include <cstdint>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "query/projection/projection_manifest.h"

namespace cedar::internal {

struct CoverageRequest {
  ProjectionKind kind = ProjectionKind::kState;
  PartId part_id;
  std::optional<PropertyId> property_id;
  uint32_t schema_epoch = 0;
  uint64_t entity_min = 0;
  uint64_t entity_max_exclusive = UINT64_MAX;
  ValidTimeInterval valid_time;
  CommitSeq snapshot_seq;
  std::optional<uint64_t> generation_id;
  std::optional<CommitSeq> expected_base_seq;
  std::string database_identity;
};

struct ProjectionSegmentInput {
  SegmentDescriptor descriptor;
  std::string bytes;
};

struct ProjectionBuild {
  ProjectionManifest manifest;
  std::vector<ProjectionSegmentInput> segments;
};

enum class ProjectionStoreFaultPoint : uint8_t {
  kAfterSegmentSync = 1,
  kAfterManifestSync = 2,
  kCurrentTemporaryWrite = 3,
  kCurrentRename = 4,
  kDirectorySync = 5,
};

struct ProjectionStoreOptions {
  std::string path;
  std::string database_identity;
  std::function<Status(ProjectionStoreFaultPoint)> fault_injector;
};

class ProjectionGeneration {
 public:
  ProjectionGeneration() = default;
  ~ProjectionGeneration();
  ProjectionGeneration(const ProjectionGeneration&);
  ProjectionGeneration& operator=(const ProjectionGeneration&);
  ProjectionGeneration(ProjectionGeneration&&) noexcept;
  ProjectionGeneration& operator=(ProjectionGeneration&&) noexcept;
  bool exists() const;
  uint64_t generation_id() const;
  const ProjectionManifest* manifest() const;

 private:
  struct State;
  explicit ProjectionGeneration(std::shared_ptr<State> state, bool pin);
  std::shared_ptr<State> state_;
  bool pin_ = false;
  friend class QueryProjectionStore;
};

class QueryProjectionStore {
 public:
  static StatusOr<std::unique_ptr<QueryProjectionStore>> Open(
      ProjectionStoreOptions options);
  ~QueryProjectionStore();

  QueryProjectionStore(const QueryProjectionStore&) = delete;
  QueryProjectionStore& operator=(const QueryProjectionStore&) = delete;

  Status Build(const ProjectionBuild& build);
  std::optional<ProjectionGeneration> Acquire(const CoverageRequest&) const;
  Status RetireBefore(CommitSeq seq);
  Status Quarantine(const std::string& filename);
  void CollectRetired();
  bool projections_enabled() const;
  std::optional<uint64_t> current_generation_id() const;
  std::optional<CommitSeq> current_base_seq() const;
  std::optional<ProjectionManifest> current_manifest() const;
  StatusOr<std::vector<ProjectionChain>> ReadChains(
      const CoverageRequest& request) const;

 private:
  explicit QueryProjectionStore(ProjectionStoreOptions options);
  Status LoadCurrent();
  Status PublishCurrent(uint64_t generation);
  ProjectionStoreOptions options_;
  std::string projections_path_;
  std::string manifests_path_;
  mutable std::mutex mutex_;
  std::shared_ptr<ProjectionGeneration::State> current_;
  std::vector<std::shared_ptr<ProjectionGeneration::State>> retired_;
  bool enabled_ = false;
  bool closed_ = false;
};

}  // namespace cedar::internal

#endif
