#include "cedar/cypher/access_profile.h"

namespace cedar::cypher {

StatusOr<AccessDecision> ChooseAccess(const BoundStatement& statement,
                                      const QueryOptions& requested) {
  AccessDecision decision;
  decision.options = requested;
  if (statement.changes) {
    decision.profile = AccessProfile::kValidTimeChanges;
    decision.options.mode = QueryExecutionMode::kAnalytical;
  } else if (statement.system_time.has_value()) {
    decision.profile = statement.system_time->as_of.has_value()
                           ? AccessProfile::kSystemTimeAsOf
                           : AccessProfile::kSystemTimeRange;
  } else if (statement.valid_time.has_value() && statement.valid_time->to.has_value()) {
    decision.profile = AccessProfile::kValidTimeRange;
    decision.options.mode = QueryExecutionMode::kAnalytical;
    decision.expects_projection = true;
  } else {
    bool graph = false;
    for (const auto& pattern : statement.patterns) {
      graph = graph || !pattern.edge.empty() || !pattern.relationship.empty() ||
              pattern.max_hops != 1;
    }
    if (graph) {
      decision.profile = AccessProfile::kGraphExpand;
      decision.options.mode = QueryExecutionMode::kInteractive;
    } else {
      bool metadata = false;
      for (const auto& projection : statement.projections) {
        metadata = metadata || !projection.function.empty();
      }
      decision.profile = metadata ? AccessProfile::kMetadataEnrichment
                                  : AccessProfile::kPointState;
      if (!metadata) decision.options.mode = QueryExecutionMode::kInteractive;
    }
  }
  return decision;
}

const char* AccessProfileName(AccessProfile profile) {
  switch (profile) {
    case AccessProfile::kPointState: return "point_state";
    case AccessProfile::kValidTimeRange: return "valid_time_range";
    case AccessProfile::kValidTimeChanges: return "valid_time_changes";
    case AccessProfile::kSystemTimeAsOf: return "system_time_as_of";
    case AccessProfile::kSystemTimeRange: return "system_time_range";
    case AccessProfile::kGraphExpand: return "graph_expand";
    case AccessProfile::kMetadataEnrichment: return "metadata_enrichment";
    case AccessProfile::kUnsupported: return "unsupported";
  }
  return "unsupported";
}

}  // namespace cedar::cypher
