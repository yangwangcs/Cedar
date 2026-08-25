#include <gtest/gtest.h>

#include <algorithm>

#include "cedar/query/query.h"
#include "query/logical/logical_plan.h"
#include "query/planner/query_planner.h"

namespace cedar::internal {
namespace {

CoverageRegion Region(uint64_t from, uint64_t to) {
  CoverageRegion region;
  region.kind = ProjectionKind::kState;
  region.part_id = PartId{0};
  region.schema_epoch = 1;
  region.entity_min = 0;
  region.entity_max_exclusive = UINT64_MAX;
  region.valid_time = {ValidTime{from}, ValidTime{to}};
  region.segments.push_back(SegmentDescriptor{"s", "s.cfacts", {}, 0, 0});
  return region;
}

StatusOr<Query> Scan(TemporalScope scope) {
  const Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto query = Query::Vertices(vertex, std::move(scope));
  return query;
}

PlanningContext Context(const ProjectionCatalogView& catalog,
                        const QueryStatisticsView& stats,
                        CommitSeq snapshot = CommitSeq{25}) {
  static const QueryDeltaView delta{CommitSeq{10}, CommitSeq{25}, {}, {}, {}, {}};
  return PlanningContext{snapshot, catalog, delta, stats, QueryOptions{}, {}, 0,
                         true};
}

TEST(QueryPlannerTest, SplitsCoverageWithoutOverlapOrGap) {
  ProjectionCatalogView catalog;
  catalog.generation_id = 4;
  catalog.base_seq = CommitSeq{10};
  catalog.regions = {Region(0, 100), Region(200, 300)};
  QueryStatisticsView stats;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{300}}});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  const auto root = LogicalPlanInspector::Inspect(query.ValueOrDie());
  auto projected = query.ValueOrDie().Select(
      std::vector<cedar::Projection>{cedar::Projection{root->schema().columns()[0]}});
  ASSERT_TRUE(projected.ok()) << projected.status().ToString();
  auto plan = QueryPlanner::Bind(*LogicalPlanInspector::Inspect(projected.ValueOrDie()), Context(catalog, stats));
  ASSERT_TRUE(plan.ok()) << plan.status().ToString();
  ASSERT_EQ(plan.ValueOrDie().slices().size(), 3U);
  EXPECT_EQ(plan.ValueOrDie().slices()[0].source, CoverageSource::kDeltaMerge);
  EXPECT_EQ(plan.ValueOrDie().slices()[1].source, CoverageSource::kCanonical);
  EXPECT_EQ(plan.ValueOrDie().slices()[2].source, CoverageSource::kDeltaMerge);
  EXPECT_EQ(plan.ValueOrDie().slices()[1].interval.from, ValidTime{100});
  EXPECT_EQ(plan.ValueOrDie().slices()[1].interval.to, std::optional<ValidTime>{ValidTime{200}});
}

TEST(QueryPlannerTest, RejectsOverlappingManifestRegions) {
  ProjectionCatalogView catalog;
  catalog.generation_id = 1;
  catalog.base_seq = CommitSeq{1};
  catalog.regions = {Region(0, 100), Region(50, 120)};
  QueryStatisticsView stats;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{130}}});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto plan = QueryPlanner::Bind(*LogicalPlanInspector::Inspect(query.ValueOrDie()), Context(catalog, stats));
  EXPECT_TRUE(plan.status().IsCorruption());
}

TEST(QueryPlannerTest, ValidatesTenThousandDisjointCoverageRegionsSubquadratically) {
  ProjectionCatalogView catalog;
  catalog.generation_id = 7;
  catalog.base_seq = CommitSeq{1};
  catalog.regions.reserve(10000);
  for (uint64_t index = 0; index < 10000; ++index) {
    CoverageRegion region = Region(0, 100);
    region.part_id = PartId{1};
    region.entity_min = index * 10;
    region.entity_max_exclusive = index * 10 + 10;
    region.segments.front().segment_id = "segment-" + std::to_string(index);
    region.segments.front().filename = region.segments.front().segment_id + ".cfacts";
    catalog.regions.push_back(std::move(region));
  }
  QueryStatisticsView stats;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{100}}});
  ASSERT_TRUE(query.ok());
  auto context = Context(catalog, stats);
  context.part_scope = PartScope::Exact(PartId{1});
  auto plan = QueryPlanner::Bind(*LogicalPlanInspector::Inspect(query.ValueOrDie()),
                                 context);
  ASSERT_TRUE(plan.ok()) << plan.status().ToString();
}

TEST(QueryPlannerTest, DifferentPartOverlapFallsBackWithoutDroppingCoverage) {
  ProjectionCatalogView catalog;
  catalog.generation_id = 4;
  catalog.base_seq = CommitSeq{10};
  auto part_zero = Region(0, 100);
  auto part_one = Region(0, 100);
  part_one.part_id = PartId{1};
  catalog.regions = {part_zero, part_one};
  QueryStatisticsView stats;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{100}}});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto plan = QueryPlanner::Bind(*LogicalPlanInspector::Inspect(query.ValueOrDie()),
                                 Context(catalog, stats));
  ASSERT_TRUE(plan.ok()) << plan.status().ToString();
  ASSERT_EQ(plan.ValueOrDie().slices().size(), 1U);
  EXPECT_EQ(plan.ValueOrDie().slices().front().source,
            CoverageSource::kCanonical);
  EXPECT_NE(std::find(plan.ValueOrDie().pushdowns.begin(),
                      plan.ValueOrDie().pushdowns.end(),
                      "canonical-fallback"),
            plan.ValueOrDie().pushdowns.end());
  const std::string explain =
      QueryPlanner::ExplainPhysical(plan.ValueOrDie());
  EXPECT_NE(explain.find("canonical-fallback"), std::string::npos);
  EXPECT_NE(explain.find("generation=none"), std::string::npos);
  EXPECT_NE(explain.find("base=none"), std::string::npos);
  EXPECT_NE(explain.find("confidence=conservative"), std::string::npos);
}

TEST(QueryPlannerTest, ExactPartUsesIndependentEntityCoverageSlices) {
  ProjectionCatalogView catalog;
  catalog.generation_id = 9;
  catalog.base_seq = CommitSeq{10};
  auto lower = Region(0, 100);
  lower.entity_min = 0;
  lower.entity_max_exclusive = 10;
  auto upper = Region(0, 100);
  upper.entity_min = 10;
  upper.entity_max_exclusive = UINT64_MAX;
  catalog.regions = {lower, upper};
  QueryStatisticsView stats;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{100}}});
  ASSERT_TRUE(query.ok());
  auto context = Context(catalog, stats);
  context.part_scope = PartScope::Exact(PartId{0});
  auto plan = QueryPlanner::Bind(
      *LogicalPlanInspector::Inspect(query.ValueOrDie()), context);
  ASSERT_TRUE(plan.ok()) << plan.status().ToString();
  ASSERT_EQ(plan.ValueOrDie().slices().size(), 2U);
  EXPECT_EQ(plan.ValueOrDie().slices()[0].entity_min, 0U);
  EXPECT_EQ(plan.ValueOrDie().slices()[1].entity_min, 10U);
  EXPECT_TRUE(plan.ValueOrDie().slices()[0].part_bound);
  EXPECT_EQ(plan.ValueOrDie().slices()[0].source, CoverageSource::kDeltaMerge);
}

TEST(QueryPlannerTest, FinitePartSetUsesIndependentPartitionSlices) {
  ProjectionCatalogView catalog;
  catalog.generation_id = 10;
  catalog.base_seq = CommitSeq{10};
  auto first = Region(0, 100);
  auto second = Region(0, 100);
  second.part_id = PartId{1};
  catalog.regions = {first, second};
  QueryStatisticsView stats;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{100}}});
  ASSERT_TRUE(query.ok());
  auto context = Context(catalog, stats);
  context.part_scope = PartScope::Set({PartId{0}, PartId{1}});
  auto plan = QueryPlanner::Bind(
      *LogicalPlanInspector::Inspect(query.ValueOrDie()), context);
  ASSERT_TRUE(plan.ok()) << plan.status().ToString();
  ASSERT_EQ(plan.ValueOrDie().slices().size(), 2U);
  EXPECT_TRUE(plan.ValueOrDie().slices()[0].part_bound);
  EXPECT_TRUE(plan.ValueOrDie().slices()[1].part_bound);
}

TEST(QueryPlannerTest, EmptyProjectionRegionFallsBackAndExplainsIt) {
  ProjectionCatalogView catalog;
  catalog.generation_id = 8;
  catalog.base_seq = CommitSeq{10};
  catalog.regions.push_back(Region(0, 100));
  catalog.regions.front().segments.clear();
  QueryStatisticsView stats;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{100}}});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto plan = QueryPlanner::Bind(*LogicalPlanInspector::Inspect(query.ValueOrDie()),
                                 Context(catalog, stats));
  ASSERT_TRUE(plan.ok()) << plan.status().ToString();
  ASSERT_EQ(plan.ValueOrDie().slices().size(), 1U);
  EXPECT_EQ(plan.ValueOrDie().slices().front().source,
            CoverageSource::kCanonical);
  const std::string explain =
      QueryPlanner::ExplainPhysical(plan.ValueOrDie());
  EXPECT_NE(explain.find("canonical-fallback"), std::string::npos);
  EXPECT_NE(explain.find("generation=none"), std::string::npos);
  EXPECT_NE(explain.find("base=none"), std::string::npos);
}

TEST(QueryPlannerTest, RejectsNewerBaseBeforeKeyOverlapFallback) {
  ProjectionCatalogView catalog;
  catalog.generation_id = 9;
  catalog.base_seq = CommitSeq{26};
  auto part_zero = Region(0, 100);
  auto part_one = Region(0, 100);
  part_one.part_id = PartId{1};
  catalog.regions = {part_zero, part_one};
  QueryStatisticsView stats;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{100}}});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto plan = QueryPlanner::Bind(*LogicalPlanInspector::Inspect(query.ValueOrDie()),
                                 Context(catalog, stats, CommitSeq{25}));
  EXPECT_TRUE(plan.status().IsCorruption());
}

TEST(QueryPlannerTest, RejectsProjectionNewerThanSnapshot) {
  ProjectionCatalogView catalog;
  catalog.generation_id = 1;
  catalog.base_seq = CommitSeq{26};
  catalog.regions = {Region(0, 100)};
  QueryStatisticsView stats;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{10}}});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto plan = QueryPlanner::Bind(*LogicalPlanInspector::Inspect(query.ValueOrDie()), Context(catalog, stats, CommitSeq{25}));
  EXPECT_TRUE(plan.status().IsCorruption());
}

TEST(QueryPlannerTest, SelectsAnalyticalForBroadRowsAndExposesExplain) {
  ProjectionCatalogView catalog;
  catalog.base_seq = CommitSeq{1};
  QueryStatisticsView stats;
  stats.known = true;
  stats.candidate_rows = 100000;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{10}}});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto plan = QueryPlanner::Bind(*LogicalPlanInspector::Inspect(query.ValueOrDie()), Context(catalog, stats));
  ASSERT_TRUE(plan.ok()) << plan.status().ToString();
  EXPECT_EQ(plan.ValueOrDie().lane, QueryExecutionMode::kAnalytical);
  EXPECT_NE(QueryPlanner::ExplainPhysical(plan.ValueOrDie()).find("analytical"),
            std::string::npos);
}

TEST(QueryPlannerTest, IncompleteDeltaFallsBackToCanonical) {
  ProjectionCatalogView catalog;
  catalog.generation_id = 3;
  catalog.base_seq = CommitSeq{10};
  catalog.regions = {Region(0, 100)};
  QueryStatisticsView stats;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{10}}});
  ASSERT_TRUE(query.ok());
  QueryDeltaView delta{CommitSeq{10}, CommitSeq{11}, {}, {}, {}, CommitSeq{11}};
  const auto context = PlanningContext{CommitSeq{25}, catalog, delta, stats,
                                       QueryOptions{}};
  auto plan = QueryPlanner::Bind(*LogicalPlanInspector::Inspect(query.ValueOrDie()),
                                 context);
  ASSERT_TRUE(plan.ok()) << plan.status().ToString();
  ASSERT_EQ(plan.ValueOrDie().slices().size(), 1U);
  EXPECT_EQ(plan.ValueOrDie().slices().front().source, CoverageSource::kCanonical);
}

TEST(QueryPlannerTest, RejectsMismatchedProjectionIdentity) {
  ProjectionCatalogView catalog;
  catalog.database_identity = "db-a";
  catalog.base_seq = CommitSeq{1};
  QueryStatisticsView stats;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{10}}});
  ASSERT_TRUE(query.ok());
  PlanningContext context{CommitSeq{2}, catalog, Context(catalog, stats).delta,
                           stats, QueryOptions{}, "db-b", 0};
  auto plan = QueryPlanner::Bind(*LogicalPlanInspector::Inspect(query.ValueOrDie()),
                                 context);
  EXPECT_TRUE(plan.status().IsIdentityConflict());
}

TEST(QueryPlannerTest, PartialEntityRegionFallsBackToCanonical) {
  ProjectionCatalogView catalog;
  catalog.generation_id = 5;
  catalog.base_seq = CommitSeq{25};
  auto region = Region(0, 100);
  region.entity_min = 1;
  region.entity_max_exclusive = 9;
  catalog.regions = {region};
  QueryStatisticsView stats;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{10}}});
  ASSERT_TRUE(query.ok());
  auto plan = QueryPlanner::Bind(*LogicalPlanInspector::Inspect(query.ValueOrDie()),
                                 Context(catalog, stats));
  ASSERT_TRUE(plan.ok()) << plan.status().ToString();
  ASSERT_EQ(plan.ValueOrDie().slices().size(), 1U);
  EXPECT_EQ(plan.ValueOrDie().slices().front().source, CoverageSource::kCanonical);
}

}  // namespace
}  // namespace cedar::internal
