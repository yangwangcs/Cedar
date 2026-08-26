#include <filesystem>

#include <gtest/gtest.h>

#include "cedar/database.h"
#include "cedar/query.h"
#include "cedar/transaction.h"

namespace cedar {
namespace {

TEST(IndexedPropertyRuntimeTest, RefreshesAndUsesCanonicalValidatedIndex) {
  char pattern[] = "/tmp/cedar_indexed_property_runtime_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto property = database.ValueOrDie()->RegisterProperty(
      PropertyDefinition{PropertyId{7}, 0, "country", PropertyEntityKind::kVertex,
                         PhysicalType::kString, 4096});
  ASSERT_TRUE(property.ok()) << property.status().ToString();
  auto numeric_property = database.ValueOrDie()->RegisterProperty(
      PropertyDefinition{PropertyId{8}, 0, "load_mw", PropertyEntityKind::kVertex,
                         PhysicalType::kInt64, 4096});
  ASSERT_TRUE(numeric_property.ok()) << numeric_property.status().ToString();
  auto tx = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(tx.ok());
  ASSERT_TRUE(tx.ValueOrDie()->Assert(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{0}).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Assert(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{0}).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Set(
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7}),
      ValidTime{0}, Value::String("CN")).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Set(
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{2}}, PropertyId{7}),
      ValidTime{0}, Value::String("US")).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Set(
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{8}),
      ValidTime{0}, Value::Int64(10)).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Set(
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{2}}, PropertyId{8}),
      ValidTime{0}, Value::Int64(20)).ok());
  auto committed = tx.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();

  auto refresh = database.ValueOrDie()->RefreshQueryIndexes(ValidTime{0});
  ASSERT_TRUE(refresh.ok()) << refresh.status().ToString();
  ASSERT_TRUE(refresh.ValueOrDie().Await().ok());

  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto source = Query::Vertices(vertex, At{ValidTime{0}});
  ASSERT_TRUE(source.ok());
  OptionalSlot<std::string> country = OptionalSlot<std::string>::Named("country");
  auto bound = source.ValueOrDie().BindVertexProperty(
      vertex, PropertyId{7}, country);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  auto filtered = bound.ValueOrDie().Where(
      Equal(ValueOf(country), Literal(std::string("CN"))));
  ASSERT_TRUE(filtered.ok()) << filtered.status().ToString();
  auto projected = filtered.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(projected.ok()) << projected.status().ToString();
  auto prepared = database.ValueOrDie()->PrepareQuery(projected.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  auto explain = prepared.ValueOrDie().ExplainPhysical(
      snapshot.ValueOrDie(), QueryOptions{});
  ASSERT_TRUE(explain.ok()) << explain.status().ToString();
  EXPECT_NE(explain.ValueOrDie().find("property-index-seek"), std::string::npos)
      << explain.ValueOrDie();
  QueryOptions query_options;
  query_options.capture_profile = true;
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, query_options);
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  size_t count = 0;
  while (true) {
    auto batch = cursor.ValueOrDie().Next();
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    if (!batch.ValueOrDie().has_value()) break;
    count += batch.ValueOrDie()->row_count();
  }
  EXPECT_EQ(count, 1U);
  EXPECT_EQ(cursor.ValueOrDie().profile().complexity.property_index_seeks, 1U);
  EXPECT_EQ(cursor.ValueOrDie().profile().complexity.property_index_candidates, 1U);

  Slot<VertexRef> numeric_vertex = Slot<VertexRef>::Named("numeric_v");
  auto numeric_source = Query::Vertices(numeric_vertex, At{ValidTime{0}});
  ASSERT_TRUE(numeric_source.ok());
  OptionalSlot<int64_t> load = OptionalSlot<int64_t>::Named("load_mw");
  auto numeric_bound = numeric_source.ValueOrDie().BindVertexProperty(
      numeric_vertex, PropertyId{8}, load);
  ASSERT_TRUE(numeric_bound.ok());
  auto numeric_filtered = numeric_bound.ValueOrDie().Where(
      GreaterThanOrEqual(ValueOf(load), Literal<int64_t>(20)));
  ASSERT_TRUE(numeric_filtered.ok());
  auto numeric_projected = numeric_filtered.ValueOrDie().Select(
      {Project(numeric_vertex)});
  ASSERT_TRUE(numeric_projected.ok());
  auto numeric_prepared = database.ValueOrDie()->PrepareQuery(
      numeric_projected.ValueOrDie());
  ASSERT_TRUE(numeric_prepared.ok());
  auto numeric_snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(numeric_snapshot.ok());
  auto numeric_cursor = numeric_prepared.ValueOrDie().Execute(
      std::move(numeric_snapshot).ConsumeValueOrDie(), Bindings{}, query_options);
  ASSERT_TRUE(numeric_cursor.ok()) << numeric_cursor.status().ToString();
  size_t numeric_count = 0;
  while (true) {
    auto batch = numeric_cursor.ValueOrDie().Next();
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    if (!batch.ValueOrDie().has_value()) break;
    numeric_count += batch.ValueOrDie()->row_count();
  }
  EXPECT_EQ(numeric_count, 1U);
  EXPECT_EQ(numeric_cursor.ValueOrDie().profile().complexity.property_index_candidates,
            1U);

  // A commit after the published generation must not be hidden by an index
  // that was built through the older snapshot. The runtime reports the miss
  // and returns the canonical result set instead.
  auto later = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(later.ok());
  ASSERT_TRUE(later.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{3}}),
                           ValidTime{0})
                  .ok());
  ASSERT_TRUE(later.ValueOrDie()
                  ->Set(PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{3}},
                                             PropertyId{7}),
                           ValidTime{0}, Value::String("CN"))
                  .ok());
  auto later_committed = later.ValueOrDie()->Commit();
  ASSERT_TRUE(later_committed.ok());
  auto later_snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(later_snapshot.ok());
  auto later_cursor = prepared.ValueOrDie().Execute(
      std::move(later_snapshot).ConsumeValueOrDie(), Bindings{}, query_options);
  ASSERT_TRUE(later_cursor.ok()) << later_cursor.status().ToString();
  size_t later_count = 0;
  while (true) {
    auto batch = later_cursor.ValueOrDie().Next();
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    if (!batch.ValueOrDie().has_value()) break;
    later_count += batch.ValueOrDie()->row_count();
  }
  EXPECT_EQ(later_count, 2U);
  EXPECT_EQ(later_cursor.ValueOrDie().profile().complexity.property_index_seeks, 0U);
  EXPECT_EQ(later_cursor.ValueOrDie().profile().complexity.property_index_candidates, 0U);
  EXPECT_GT(later_cursor.ValueOrDie().profile().complexity.canonical_fallbacks, 0U);

  // Rebuild after a durable property delete. The complete generation must
  // exclude the deleted value rather than returning a stale posting.
  auto deleted = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(deleted.ok());
  ASSERT_TRUE(deleted.ValueOrDie()
                  ->Unset(PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}},
                                               PropertyId{7}),
                          ValidTime{0})
                  .ok());
  ASSERT_TRUE(deleted.ValueOrDie()->Commit().ok());
  auto rebuilt = database.ValueOrDie()->RefreshQueryIndexes(ValidTime{0});
  ASSERT_TRUE(rebuilt.ok()) << rebuilt.status().ToString();
  ASSERT_TRUE(rebuilt.ValueOrDie().Await().ok());
  auto after_delete_snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(after_delete_snapshot.ok());
  auto after_delete = prepared.ValueOrDie().Execute(
      std::move(after_delete_snapshot).ConsumeValueOrDie(), Bindings{}, query_options);
  ASSERT_TRUE(after_delete.ok()) << after_delete.status().ToString();
  size_t after_delete_count = 0;
  while (true) {
    auto batch = after_delete.ValueOrDie().Next();
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    if (!batch.ValueOrDie().has_value()) break;
    after_delete_count += batch.ValueOrDie()->row_count();
  }
  EXPECT_EQ(after_delete_count, 1U);
  EXPECT_GT(after_delete.ValueOrDie().profile().complexity.property_index_seeks, 0U);
  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace cedar
