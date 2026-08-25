#include <gtest/gtest.h>

#include "query/read/read_binding_capsule.h"
#include "cedar/query.h"
#include "query/logical/logical_plan.h"
#include "query/planner/query_planner.h"

namespace cedar::internal {
namespace {

TEST(ReadBindingCapsule, SharesStaticPlanAndSeparatesDynamicStats) {
  auto plan = std::make_shared<const PhysicalPlan>();
  PreparedPlanTemplate template_state{plan, 42};
  auto stats = std::make_shared<ProjectionReadStats>();
  ReadBindingCapsule capsule(template_state, nullptr, nullptr, std::nullopt,
                             stats, {}, {});
  EXPECT_EQ(capsule.plan_template().physical, plan);
  EXPECT_EQ(capsule.plan_template().fingerprint, 42U);
  EXPECT_EQ(capsule.projection_stats(), stats);
  EXPECT_EQ(capsule.delta_view(), nullptr);
}

TEST(ReadBindingCapsule, StaticPreparationIsSnapshotIndependent) {
  auto query = Query::Vertices(Slot<VertexRef>::Named("v"),
                               At{ValidTime{1}});
  ASSERT_TRUE(query.ok());
  const auto* root = LogicalPlanInspector::Inspect(query.ValueOrDie());
  auto first = QueryPlanner::PrepareStatic(*root, "schema");
  auto second = QueryPlanner::PrepareStatic(*root, "schema");
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  EXPECT_EQ(first.ValueOrDie().fingerprint, second.ValueOrDie().fingerprint);
  EXPECT_FALSE(first.ValueOrDie().operations.empty());
}

}  // namespace
}  // namespace cedar::internal
