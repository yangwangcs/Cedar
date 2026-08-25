#ifndef CEDAR_QUERY_EXECUTION_CONTEXT_H_
#define CEDAR_QUERY_EXECUTION_CONTEXT_H_

#include <functional>
#include <memory>
#include <optional>

#include "cedar/database.h"
#include "query/planner/query_planner.h"
#include "query/projection/projection_store.h"
#include "query/read/read_binding_capsule.h"

namespace cedar::internal {

struct QueryExecutionBinding {
  PhysicalPlan physical;
  std::shared_ptr<const ReadBindingCapsule> read_binding;
  std::shared_ptr<const QueryDeltaView> delta_view;
  std::shared_ptr<const QueryDeltaLease> delta_lease;
  std::optional<ProjectionGeneration> projection_generation;
  std::shared_ptr<ProjectionReadStats> projection_stats;
  std::function<StatusOr<std::vector<ProjectionChain>>(const CoverageSlice&)>
      projection_reader;
  std::function<StatusOr<QueryDeltaView>()> delta_reader;
};

class QueryExecutionContextFactory {
 public:
  static StatusOr<QueryExecutionBinding> Bind(
      const std::shared_ptr<Database::Impl>& database,
      const std::shared_ptr<const LogicalPlanNode>& root,
      CommitSeq snapshot_seq, const QueryOptions& options,
      const PartScope& part_scope);
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_EXECUTION_CONTEXT_H_
