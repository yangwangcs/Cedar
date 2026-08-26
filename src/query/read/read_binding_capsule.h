#ifndef CEDAR_QUERY_READ_READ_BINDING_CAPSULE_H_
#define CEDAR_QUERY_READ_READ_BINDING_CAPSULE_H_

#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "query/planner/query_planner.h"
#include "query/projection/projection_store.h"
#include "query/projection/query_delta.h"

namespace cedar::internal {

using ProjectionReader =
    std::function<StatusOr<std::vector<ProjectionChain>>(const CoverageSlice&)>;
using PropertyIndexReader =
    std::function<StatusOr<PropertyIndexSegment>(PartId, PropertyId,
                                                 uint32_t, ValidTimeInterval)>;
using DeltaReader = std::function<StatusOr<QueryDeltaView>()>;

struct PreparedPlanTemplate {
  std::shared_ptr<const PhysicalPlan> physical;
  uint64_t fingerprint = 0;
};

struct ReadBindingKey {
  uint64_t plan_fingerprint = 0;
  std::string database_identity;
  std::string schema_fingerprint;
  CommitSeq snapshot_seq;
  PartScope part_scope = PartScope::All();
  std::optional<uint64_t> projection_generation;
  std::optional<CommitSeq> projection_base;
  bool operator==(const ReadBindingKey& other) const {
    return plan_fingerprint == other.plan_fingerprint &&
           database_identity == other.database_identity &&
           schema_fingerprint == other.schema_fingerprint &&
           snapshot_seq == other.snapshot_seq &&
           part_scope.kind == other.part_scope.kind &&
           part_scope.parts == other.part_scope.parts &&
           projection_generation == other.projection_generation &&
           projection_base == other.projection_base;
  }
};

class ReadBindingCapsule {
 public:
  ReadBindingCapsule(PreparedPlanTemplate plan,
                     std::shared_ptr<const QueryDeltaView> delta_view,
                     std::shared_ptr<const QueryDeltaLease> delta_lease,
                     std::optional<ProjectionGeneration> generation,
                     std::shared_ptr<ProjectionReadStats> stats,
                     ProjectionReader projection_reader,
                     PropertyIndexReader property_index_reader,
                     DeltaReader delta_reader)
      : plan_(std::move(plan)), delta_view_(std::move(delta_view)),
        delta_lease_(std::move(delta_lease)), generation_(std::move(generation)),
        stats_(std::move(stats)), projection_reader_(std::move(projection_reader)),
        property_index_reader_(std::move(property_index_reader)),
        delta_reader_(std::move(delta_reader)) {}

  ReadBindingCapsule(PreparedPlanTemplate plan,
                     std::shared_ptr<const QueryDeltaView> delta_view,
                     std::shared_ptr<const QueryDeltaLease> delta_lease,
                     std::optional<ProjectionGeneration> generation,
                     std::shared_ptr<ProjectionReadStats> stats,
                     ProjectionReader projection_reader,
                     DeltaReader delta_reader)
      : ReadBindingCapsule(std::move(plan), std::move(delta_view),
                           std::move(delta_lease), std::move(generation),
                           std::move(stats), std::move(projection_reader),
                           PropertyIndexReader{}, std::move(delta_reader)) {}

  const PreparedPlanTemplate& plan_template() const { return plan_; }
  const std::shared_ptr<const QueryDeltaView>& delta_view() const { return delta_view_; }
  const std::shared_ptr<const QueryDeltaLease>& delta_lease() const { return delta_lease_; }
  const std::optional<ProjectionGeneration>& projection_generation() const { return generation_; }
  const std::shared_ptr<ProjectionReadStats>& projection_stats() const { return stats_; }
  const ProjectionReader& projection_reader() const { return projection_reader_; }
  const PropertyIndexReader& property_index_reader() const {
    return property_index_reader_;
  }
  const DeltaReader& delta_reader() const { return delta_reader_; }

 private:
  PreparedPlanTemplate plan_;
  std::shared_ptr<const QueryDeltaView> delta_view_;
  std::shared_ptr<const QueryDeltaLease> delta_lease_;
  std::optional<ProjectionGeneration> generation_;
  std::shared_ptr<ProjectionReadStats> stats_;
  ProjectionReader projection_reader_;
  PropertyIndexReader property_index_reader_;
  DeltaReader delta_reader_;
};

}  // namespace cedar::internal

#endif
