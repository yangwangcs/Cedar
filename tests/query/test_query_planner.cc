#include <gtest/gtest.h>

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
  static const QueryDeltaView delta{CommitSeq{0}, CommitSeq{25}, {}, {}, {}};
  return PlanningContext{snapshot, catalog, delta, stats, QueryOptions{}};
}

TEST(QueryPlannerTest, SplitsCoverageWithoutOverlapOrGap) {
  ProjectionCatalogView catalog;
  catalog.generation_id = 4;
  catalog.base_seq = CommitSeq{10};
  catalog.regions = {Region(0, 100), Region(200, 300)};
  QueryStatisticsView stats;
  auto query = Scan(Overlaps{{ValidTime{0}, ValidTime{300}}});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto plan = QueryPlanner::Bind(*LogicalPlanInspector::Inspect(query.ValueOrDie()), Context(catalog, stats));
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

}  // namespace
}  // namespace cedar::internal
