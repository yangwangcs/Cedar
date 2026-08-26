#include <gtest/gtest.h>

#include <cstdlib>
#include <string>
#include <vector>

#include "cedar/database.h"
#include "cedar/fact/canonical_reader.h"
#include "cedar/transaction.h"

namespace cedar {
namespace {

TEST(CanonicalFactReader, BindsPartAndEntityRangeToSnapshot) {
  char pattern[] = "/tmp/cedar_canonical_reader_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto transaction = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()->Assert(
      EntityFact::Vertex(VertexRef{PartId{1}, VertexId{7}}), ValidTime{10}).ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Assert(
      EntityFact::Vertex(VertexRef{PartId{2}, VertexId{7}}), ValidTime{10}).ok());
  auto committed = transaction.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  FactReadSpec spec;
  spec.part_scope = PartScope::Exact(PartId{1});
  spec.family = FactFamily::kVertexState;
  spec.entity_range = EntityRange{7, 8};
  std::vector<FactEvent> events;
  ASSERT_TRUE(snapshot.ValueOrDie().canonical_reader()
                  .ReadEvents(spec, [&events](const FactEventBatch& batch) {
                    events.insert(events.end(), batch.events.begin(), batch.events.end());
                    return Status::OK();
                  })
                  .ok());
  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events.front().ref.part_id(), PartId{1});

  auto state = snapshot.ValueOrDie().canonical_reader().ReadStateAt(
      spec, ValidTime{10}, snapshot.ValueOrDie().commit_seq());
  ASSERT_TRUE(state.ok()) << state.status().ToString();
  ASSERT_TRUE(state.ValueOrDie().has_value());
  EXPECT_EQ(state.ValueOrDie()->ref.part_id(), PartId{1});
}

TEST(CanonicalFactReader, MaxRowsBoundsCanonicalAndColumnarReaders) {
  char pattern[] = "/tmp/cedar_canonical_reader_limit_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto transaction = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  for (uint64_t vertex_id = 1; vertex_id <= 3; ++vertex_id) {
    ASSERT_TRUE(transaction.ValueOrDie()
                    ->Assert(EntityFact::Vertex(
                                 VertexRef{PartId{1}, VertexId{vertex_id}}),
                             ValidTime{10})
                    .ok());
  }
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  FactReadSpec spec;
  spec.part_scope = PartScope::Exact(PartId{1});
  spec.family = FactFamily::kVertexState;
  spec.entity_range = EntityRange{1, 4};
  spec.max_rows = 2;
  std::vector<FactEvent> limited;
  ASSERT_TRUE(snapshot.ValueOrDie().canonical_reader()
                  .ReadEvents(spec, [&limited](const FactEventBatch& batch) {
                    limited.insert(limited.end(), batch.events.begin(), batch.events.end());
                    return Status::OK();
                  })
                  .ok());
  EXPECT_EQ(limited.size(), 2U);

  spec.max_rows.reset();
  std::vector<FactEvent> complete;
  ASSERT_TRUE(snapshot.ValueOrDie().canonical_reader()
                  .ReadEvents(spec, [&complete](const FactEventBatch& batch) {
                    complete.insert(complete.end(), batch.events.begin(), batch.events.end());
                    return Status::OK();
                  })
                  .ok());
  EXPECT_EQ(complete.size(), 3U);

  spec.projection = {FactColumnId::kEntityId};
  spec.max_rows = 2;
  size_t limited_columnar_rows = 0;
  ASSERT_TRUE(snapshot.ValueOrDie().canonical_reader()
                  .ReadColumnar(spec, [&limited_columnar_rows](const FactColumnarBatch& batch) {
                    limited_columnar_rows += batch.row_count();
                    return Status::OK();
                  })
                  .ok());
  EXPECT_EQ(limited_columnar_rows, 2U);

  spec.max_rows.reset();
  size_t complete_columnar_rows = 0;
  ASSERT_TRUE(snapshot.ValueOrDie().canonical_reader()
                  .ReadColumnar(spec, [&complete_columnar_rows](const FactColumnarBatch& batch) {
                    complete_columnar_rows += batch.row_count();
                    return Status::OK();
                  })
                  .ok());
  EXPECT_EQ(complete_columnar_rows, 3U);
  database.ValueOrDie()->Close().IgnoreError();
}

TEST(CanonicalFactReader, StreamsVisibleStateRowsAndStopsAtStateLimit) {
  char pattern[] = "/tmp/cedar_canonical_state_rows_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  for (uint64_t vertex_id = 1; vertex_id <= 4; ++vertex_id) {
    auto transaction = database.ValueOrDie()->BeginTransaction();
    ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
    ASSERT_TRUE(transaction.ValueOrDie()
                    ->Assert(EntityFact::Vertex(
                                 VertexRef{PartId{1}, VertexId{vertex_id}}),
                             ValidTime{10})
                    .ok());
    ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());
  }
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  CanonicalStateReadSpec spec;
  spec.facts.part_scope = PartScope::Exact(PartId{1});
  spec.facts.family = FactFamily::kVertexState;
  spec.facts.entity_range = EntityRange{1, 5};
  spec.facts.batch_row_limit = 2;
  spec.valid_time = ValidTime{10};
  spec.snapshot_seq = snapshot.ValueOrDie().commit_seq();
  spec.max_rows = 2;
  std::vector<CanonicalStateRow> rows;
  ASSERT_TRUE(snapshot.ValueOrDie().canonical_reader()
                  .ReadStateRows(spec, [&rows](const auto& batch) {
                    rows.insert(rows.end(), batch.begin(), batch.end());
                    return Status::OK();
                  })
                  .ok());
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0].ref.entity_id(), 1U);
  EXPECT_EQ(rows[1].ref.entity_id(), 2U);
  database.ValueOrDie()->Close().IgnoreError();
}

TEST(CanonicalFactReader, StateLimitStopsAcrossPartScopesAndZeroDoesNoRead) {
  char pattern[] = "/tmp/cedar_canonical_state_rows_parts_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto tx = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(tx.ok());
  for (uint32_t part = 0; part < 2; ++part) {
    for (uint64_t id = 1; id <= 2; ++id) {
      ASSERT_TRUE(tx.ValueOrDie()
                      ->Assert(EntityFact::Vertex(
                                   VertexRef{PartId{part}, VertexId{id}}),
                               ValidTime{10})
                      .ok());
    }
  }
  ASSERT_TRUE(tx.ValueOrDie()->Commit().ok());
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  CanonicalStateReadSpec spec;
  spec.facts.part_scope = PartScope::Set({PartId{0}, PartId{1}});
  spec.facts.family = FactFamily::kVertexState;
  spec.valid_time = ValidTime{10};
  spec.snapshot_seq = snapshot.ValueOrDie().commit_seq();
  spec.max_rows = 3;
  size_t callbacks = 0;
  size_t rows = 0;
  ASSERT_TRUE(snapshot.ValueOrDie().canonical_reader()
                  .ReadStateRows(spec, [&callbacks, &rows](const auto& batch) {
                    ++callbacks;
                    rows += batch.size();
                    return Status::OK();
                  })
                  .ok());
  EXPECT_EQ(rows, 3U);
  EXPECT_GE(callbacks, 1U);

  spec.max_rows = 0;
  callbacks = 0;
  ASSERT_TRUE(snapshot.ValueOrDie().canonical_reader()
                  .ReadStateRows(spec, [&callbacks](const auto&) {
                    ++callbacks;
                    return Status::OK();
                  })
                  .ok());
  EXPECT_EQ(callbacks, 0U);
  database.ValueOrDie()->Close().IgnoreError();
}

}  // namespace
}  // namespace cedar
