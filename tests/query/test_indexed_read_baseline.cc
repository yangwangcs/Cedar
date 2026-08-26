#include <gtest/gtest.h>

#include <filesystem>

#include "cedar/database.h"
#include "cedar/query.h"
#include "cedar/transaction.h"

namespace cedar {
namespace {

StatusOr<size_t> Drain(QueryCursor* cursor) {
  size_t rows = 0;
  while (true) {
    auto batch = cursor->Next();
    if (!batch.ok()) return batch.status();
    if (!batch.ValueOrDie().has_value()) break;
    rows += batch.ValueOrDie()->row_count();
  }
  return rows;
}

TEST(IndexedReadBaseline, FullScanPointReadAndLimitExposeBoundedEvidence) {
  char pattern[] = "/tmp/cedar_indexed_read_baseline_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  DatabaseOptions database_options;
  database_options.path = path;
  database_options.query_runtime.query_memory_bytes = 512ULL << 20;
  auto database = Database::Open(database_options);
  ASSERT_TRUE(database.ok()) << database.status().ToString();

  for (uint64_t base = 1; base <= 10000; base += 1000) {
    auto transaction = database.ValueOrDie()->BeginTransaction();
    ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
    for (uint64_t id = base; id < base + 1000; ++id) {
      ASSERT_TRUE(transaction.ValueOrDie()
                      ->Assert(EntityFact::Vertex(
                                   VertexRef{PartId{0}, VertexId{id}}),
                               ValidTime{1})
                      .ok());
    }
    auto committed = transaction.ValueOrDie()->Commit();
    ASSERT_TRUE(committed.ok()) << committed.status().ToString();
    ASSERT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted)
        << committed.ValueOrDie().status.ToString();
  }

  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto full_scan = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(full_scan.ok());
  auto projected = full_scan.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(projected.ok()) << projected.status().ToString();
  auto prepared = database.ValueOrDie()->PrepareQuery(projected.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();

  QueryOptions options;
  options.capture_profile = true;
  options.budget.memory_bytes = 128ULL << 20;
  auto full_snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(full_snapshot.ok());
  auto full_cursor = prepared.ValueOrDie().Execute(
      std::move(full_snapshot).ConsumeValueOrDie(), Bindings{}, options);
  ASSERT_TRUE(full_cursor.ok()) << full_cursor.status().ToString();
  auto full_rows = Drain(&full_cursor.ValueOrDie());
  ASSERT_TRUE(full_rows.ok()) << full_rows.status().ToString();
  EXPECT_EQ(full_rows.ValueOrDie(), 10000U);
  const QueryProfile full_profile = full_cursor.ValueOrDie().profile();
  uint64_t full_decoded_bytes = 0;
  for (const auto& operator_profile : full_profile.operators) {
    full_decoded_bytes += operator_profile.decoded_bytes;
  }
  EXPECT_GT(full_decoded_bytes, 0U);
  EXPECT_GT(full_profile.complexity.canonical_rows_decoded, 0U);
  ASSERT_TRUE(full_cursor.ValueOrDie().Close().ok());

  auto point_source = Query::VertexPoint(
      VertexRef{PartId{0}, VertexId{5000}}, vertex, At{ValidTime{1}});
  ASSERT_TRUE(point_source.ok()) << point_source.status().ToString();
  auto point_query = point_source.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(point_query.ok()) << point_query.status().ToString();
  auto point_prepared = database.ValueOrDie()->PrepareQuery(point_query.ValueOrDie());
  ASSERT_TRUE(point_prepared.ok()) << point_prepared.status().ToString();
  auto point_snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(point_snapshot.ok());
  auto point_cursor = point_prepared.ValueOrDie().Execute(
      std::move(point_snapshot).ConsumeValueOrDie(), Bindings{}, options);
  ASSERT_TRUE(point_cursor.ok()) << point_cursor.status().ToString();
  auto point_rows = Drain(&point_cursor.ValueOrDie());
  ASSERT_TRUE(point_rows.ok()) << point_rows.status().ToString();
  EXPECT_EQ(point_rows.ValueOrDie(), 1U);
  EXPECT_EQ(point_cursor.ValueOrDie().profile().complexity.point_reads, 1U);
  EXPECT_LT(point_cursor.ValueOrDie().profile().complexity.canonical_rows_decoded,
            full_profile.complexity.canonical_rows_decoded);

  auto limited = full_scan.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(limited.ok());
  limited = limited.ValueOrDie().Limit(0, 10);
  ASSERT_TRUE(limited.ok()) << limited.status().ToString();
  auto limited_prepared = database.ValueOrDie()->PrepareQuery(limited.ValueOrDie());
  ASSERT_TRUE(limited_prepared.ok()) << limited_prepared.status().ToString();
  auto limited_snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(limited_snapshot.ok());
  auto limited_cursor = limited_prepared.ValueOrDie().Execute(
      std::move(limited_snapshot).ConsumeValueOrDie(), Bindings{}, options);
  ASSERT_TRUE(limited_cursor.ok()) << limited_cursor.status().ToString();
  auto limited_rows = Drain(&limited_cursor.ValueOrDie());
  ASSERT_TRUE(limited_rows.ok()) << limited_rows.status().ToString();
  EXPECT_EQ(limited_rows.ValueOrDie(), 10U);
  EXPECT_GT(limited_cursor.ValueOrDie().profile().complexity.limit_early_stops, 0U);

  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace cedar
