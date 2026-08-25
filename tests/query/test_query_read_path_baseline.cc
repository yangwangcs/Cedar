#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>

#include "cedar/cypher/session.h"
#include "cedar/database.h"

namespace cedar::cypher {
namespace {

TEST(QueryReadBaseline, PartScopeAndCountersAreObservable) {
  char path[] = "/tmp/cedar_query_read_baseline_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto tx = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(tx.ok());
  for (uint64_t i = 1; i <= 10; ++i) {
    ASSERT_TRUE(tx.ValueOrDie()
                    ->Assert(EntityFact::Vertex({PartId{3}, VertexId{i}}),
                             ValidTime{1})
                    .ok());
  }
  auto commit = tx.ValueOrDie()->Commit();
  ASSERT_TRUE(commit.ok()) << commit.status().ToString();
  CypherSession session(*database.ValueOrDie(), SchemaCatalog{},
                        BinderOptions{.part_id = PartId{3}});
  auto prepared = session.Prepare(
      "FOR VALID_TIME AS OF 1 MATCH (v) RETURN v");
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  CypherRequest request;
  request.options.capture_profile = true;
  auto cursor = session.Execute(prepared.ValueOrDie(), request);
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  uint64_t rows = 0;
  while (true) {
    auto batch = cursor.ValueOrDie().Next();
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    if (!batch.ValueOrDie().has_value()) break;
    rows += batch.ValueOrDie()->row_count();
  }
  EXPECT_EQ(rows, 10U);
  const auto profile = cursor.ValueOrDie().profile();
  uint64_t decoded = 0;
  for (const auto& op : profile.operators) decoded += op.decoded_bytes;
  EXPECT_GT(decoded, 0U);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace cedar::cypher
