#include <gtest/gtest.h>

#include "query/plan_outcome.h"

namespace cedar::internal {
namespace {

TEST(QueryPlanOutcome, ClassifiesPlannerRefusalsWithoutTextMarkers) {
  EXPECT_EQ(ClassifyPlanStatus(Status::NotSupported("planner", "range")),
            PlanOutcomeKind::kUnsupported);
  EXPECT_EQ(ClassifyPlanStatus(Status::InvalidArgument("planner", "scope")),
            PlanOutcomeKind::kInvalidRequest);
  EXPECT_EQ(ClassifyPlanStatus(Status::Corruption("planner", "overlap")),
            PlanOutcomeKind::kCorrupt);
  EXPECT_EQ(ClassifyPlanStatus(Status::Conflict("planner", "gap")),
            PlanOutcomeKind::kCanonicalFallbackAllowed);
}

}  // namespace
}  // namespace cedar::internal
