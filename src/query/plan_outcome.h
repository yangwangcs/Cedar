#ifndef CEDAR_QUERY_PLAN_OUTCOME_H_
#define CEDAR_QUERY_PLAN_OUTCOME_H_

#include <memory>
#include <string>

#include "cedar/core/status.h"
#include "query/planner/query_planner.h"

namespace cedar::internal {

enum class PlanOutcomeKind : uint8_t {
  kReady,
  kCanonicalFallbackAllowed,
  kUnsupported,
  kInvalidRequest,
  kCorrupt,
};

struct PlanOutcome {
  PlanOutcomeKind kind = PlanOutcomeKind::kReady;
  std::shared_ptr<const PhysicalPlan> plan;
  std::string reason;
};

inline PlanOutcomeKind ClassifyPlanStatus(const Status& status) {
  if (status.IsNotSupportedError()) return PlanOutcomeKind::kUnsupported;
  if (status.IsInvalidArgument()) return PlanOutcomeKind::kInvalidRequest;
  if (status.IsCorruption() || status.IsIdentityConflict()) {
    return PlanOutcomeKind::kCorrupt;
  }
  return PlanOutcomeKind::kCanonicalFallbackAllowed;
}

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_PLAN_OUTCOME_H_
