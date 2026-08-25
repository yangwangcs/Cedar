#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>

#include "cedar/cypher/binder.h"
#include "cedar/cypher/compiler.h"
#include "cedar/cypher/parser.h"
#include "cedar/database.h"

namespace cedar::cypher {
namespace {

StatusOr<BoundStatement> BindText(const char* source) {
  const auto parsed = Parse(source);
  if (!parsed.ok()) return parsed.status();
  return Bind(parsed.ValueOrDie(), SchemaCatalog{}, BinderOptions{});
}

TEST(TCypherSemanticMatrixTest, TypedExpansionReturnsOnlyRequestedRelationship) {
  char path[] = "/tmp/cedar_tcypher_relationship_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();

  auto tx = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(tx.ok()) << tx.status().ToString();
  auto source = database.ValueOrDie()->AllocateVertexId();
  auto target = database.ValueOrDie()->AllocateVertexId();
  auto knows = database.ValueOrDie()->AllocateEdgeId();
  auto likes = database.ValueOrDie()->AllocateEdgeId();
  ASSERT_TRUE(source.ok() && target.ok() && knows.ok() && likes.ok());
  const VertexRef source_ref{PartId{0}, source.ValueOrDie()};
  const VertexRef target_ref{PartId{0}, target.ValueOrDie()};
  ASSERT_TRUE(tx.ValueOrDie()->Assert(EntityFact::Vertex(source_ref), ValidTime{1}).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Assert(EntityFact::Vertex(target_ref), ValidTime{1}).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Assert(
      EdgeIdentity{EdgeRef{PartId{0}, knows.ValueOrDie()}, source_ref, target_ref,
                   0xd4a9c8114d237029ULL},
      ValidTime{1}).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Assert(
      EdgeIdentity{EdgeRef{PartId{0}, likes.ValueOrDie()}, source_ref, target_ref,
                   0x9550aa2d9f698039ULL},
      ValidTime{1}).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Commit().ok());

  const auto bound = BindText(
      "FOR VALID_TIME AS OF 1 MATCH (a)-[e:KNOWS]->(b) RETURN a, e, b");
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  const auto query = Compile(bound.ValueOrDie());
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database.ValueOrDie()->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  size_t rows = 0;
  while (true) {
    auto batch = cursor.ValueOrDie().Next();
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    if (!batch.ValueOrDie().has_value()) break;
    rows += batch.ValueOrDie()->row_count();
  }
  EXPECT_EQ(rows, 1U);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(TCypherSemanticMatrixTest, ExecutesConnectedMixedPathSequence) {
  char path[] = "/tmp/cedar_tcypher_sequence_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto tx = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(tx.ok()) << tx.status().ToString();
  auto a = database.ValueOrDie()->AllocateVertexId();
  auto b = database.ValueOrDie()->AllocateVertexId();
  auto c = database.ValueOrDie()->AllocateVertexId();
  auto e1 = database.ValueOrDie()->AllocateEdgeId();
  auto e2 = database.ValueOrDie()->AllocateEdgeId();
  ASSERT_TRUE(a.ok() && b.ok() && c.ok() && e1.ok() && e2.ok());
  const VertexRef ar{PartId{0}, a.ValueOrDie()};
  const VertexRef br{PartId{0}, b.ValueOrDie()};
  const VertexRef cr{PartId{0}, c.ValueOrDie()};
  ASSERT_TRUE(tx.ValueOrDie()->Assert(EntityFact::Vertex(ar), ValidTime{1}).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Assert(EntityFact::Vertex(br), ValidTime{1}).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Assert(EntityFact::Vertex(cr), ValidTime{1}).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Assert(EdgeIdentity{EdgeRef{PartId{0}, e1.ValueOrDie()}, ar, br,
                                                           0xd4a9c8114d237029ULL}, ValidTime{1}).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Assert(EdgeIdentity{EdgeRef{PartId{0}, e2.ValueOrDie()}, br, cr,
                                                           0x9550aa2d9f698039ULL}, ValidTime{1}).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Commit().ok());
  const auto bound = BindText(
      "FOR VALID_TIME AS OF 1 MATCH (a)-[e:KNOWS]->(b)-[f:LIKES]->(c) "
      "RETURN a, e, b, f, c");
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  const auto query = Compile(bound.ValueOrDie());
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database.ValueOrDie()->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  EXPECT_EQ(batch.ValueOrDie()->row_count(), 1U);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(TCypherSemanticMatrixTest, RejectsLabelsWithoutLabelFactFamily) {
  const auto bound = BindText("MATCH (a:Person) RETURN a");
  ASSERT_FALSE(bound.ok());
  EXPECT_TRUE(bound.status().IsNotSupportedError()) << bound.status().ToString();
}

TEST(TCypherSemanticMatrixTest, RejectsMatchWritesInsteadOfRecreatingEntities) {
  const auto bound = BindText("MATCH (a) RETURN a SET a.name = $name");
  ASSERT_FALSE(bound.ok());
  EXPECT_TRUE(bound.status().IsInvalidArgument()) << bound.status().ToString();
}

TEST(TCypherSemanticMatrixTest, RejectsVariableLengthCreate) {
  const auto bound = BindText("CREATE (a)-[e:KNOWS*1..3]->(b)");
  ASSERT_FALSE(bound.ok());
  EXPECT_TRUE(bound.status().IsNotSupportedError()) << bound.status().ToString();
}

TEST(TCypherSemanticMatrixTest, RequiresValidTimeRangeForChanges) {
  const auto missing_scope = BindText("CHANGES MATCH (a) RETURN a");
  ASSERT_FALSE(missing_scope.ok());
  EXPECT_TRUE(missing_scope.status().IsInvalidArgument())
      << missing_scope.status().ToString();

  const auto system_scope = BindText(
      "CHANGES FOR SYSTEM_TIME AS OF 7 MATCH (a) RETURN a");
  ASSERT_FALSE(system_scope.ok());
  EXPECT_TRUE(system_scope.status().IsInvalidArgument())
      << system_scope.status().ToString();
}

TEST(TCypherSemanticMatrixTest, RejectsDuplicatePatternVariables) {
  const auto bound = BindText("MATCH (a)-[a:KNOWS]->(b) RETURN a");
  ASSERT_FALSE(bound.ok());
  EXPECT_TRUE(bound.status().IsInvalidArgument()) << bound.status().ToString();
}

TEST(TCypherSemanticMatrixTest, AcceptsSystemTimeRange) {
  const auto bound = BindText(
      "FOR SYSTEM_TIME BETWEEN 2 AND 8 MATCH (a) RETURN a");
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  EXPECT_TRUE(bound.ValueOrDie().system_time.has_value());
  EXPECT_EQ(bound.ValueOrDie().system_time->from, 2U);
  EXPECT_EQ(bound.ValueOrDie().system_time->to, 8U);
}

}  // namespace
}  // namespace cedar::cypher
