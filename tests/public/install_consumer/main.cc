#include <chrono>
#include <filesystem>
#include <cstdint>
#include <memory>
#include <string>

#include <cedar/database.h>
#include <cedar/query.h>
#include <cedar/transaction.h>

namespace {

constexpr cedar::PropertyId kScore{7};
constexpr cedar::VertexRef kVertex{{0}, {1}};

int VerifyStateAt(cedar::Database& database,
                  const cedar::Query& query,
                  const cedar::Slot<cedar::VertexRef>& vertex,
                  const cedar::OptionalSlot<int64_t>& score) {
  auto prepared = database.PrepareQuery(query);
  if (!prepared.ok()) return 10;
  auto snapshot = database.BeginSnapshot();
  if (!snapshot.ok()) return 11;
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), cedar::Bindings{},
      cedar::QueryOptions{});
  if (!cursor.ok()) return 12;

  auto first = cursor.ValueOrDie().Next();
  if (!first.ok() || !first.ValueOrDie().has_value()) return 13;
  const cedar::QueryBatch& batch = first.ValueOrDie().value();
  if (batch.row_count() != 1) return 14;
  if (batch.Get<cedar::VertexRef>(vertex, 0) != kVertex) return 15;
  if (batch.Get<int64_t>(score, 0) != 42) return 16;

  auto end = cursor.ValueOrDie().Next();
  if (!end.ok() || end.ValueOrDie().has_value()) return 17;
  if (!cursor.ValueOrDie().Close().ok()) return 18;
  return 0;
}

}  // namespace

int main() {
  const std::filesystem::path path =
      std::filesystem::temp_directory_path() /
      ("cedar_install_consumer_" +
       std::to_string(std::chrono::steady_clock::now()
                          .time_since_epoch()
                          .count()));
  auto opened = cedar::Database::Open({.path = path.string()});
  if (!opened.ok()) return 1;
  std::unique_ptr<cedar::Database> database =
      std::move(opened).ConsumeValueOrDie();

  const auto property = database->RegisterProperty({
      kScore, 0, "score", cedar::PropertyEntityKind::kVertex,
      cedar::PhysicalType::kInt64, 4096});
  if (!property.ok()) return 2;

  {
    auto transaction = database->BeginTransaction();
    if (!transaction.ok()) return 3;
    if (!transaction.ValueOrDie()
             ->Assert(cedar::EntityFact::Vertex(kVertex),
                      cedar::ValidTime{0})
             .ok()) {
      return 4;
    }
    if (!transaction.ValueOrDie()
             ->Set(cedar::PropertyFact::Vertex(kVertex, kScore),
                   cedar::ValidTime{10}, cedar::Value::Int64(42))
             .ok()) {
      return 5;
    }
    auto commit = transaction.ValueOrDie()->Commit();
    if (!commit.ok()) return 6;
  }
  {
    auto transaction = database->BeginTransaction();
    if (!transaction.ok()) return 7;
    if (!transaction.ValueOrDie()
             ->Set(cedar::PropertyFact::Vertex(kVertex, kScore),
                   cedar::ValidTime{20}, cedar::Value::Int64(84))
             .ok()) {
      return 8;
    }
    auto commit = transaction.ValueOrDie()->Commit();
    if (!commit.ok()) return 9;
  }

  cedar::Slot<cedar::VertexRef> vertex =
      cedar::Slot<cedar::VertexRef>::Named("vertex");
  cedar::OptionalSlot<int64_t> score =
      cedar::OptionalSlot<int64_t>::Named("score");
  auto source = cedar::Query::Vertices(vertex, cedar::At{cedar::ValidTime{15}});
  if (!source.ok()) return 20;
  auto bound = source.ValueOrDie().BindVertexProperty(vertex, kScore, score);
  if (!bound.ok()) return 21;
  auto query = bound.ValueOrDie().Select(
      {cedar::Project(vertex), cedar::Project(score)});
  if (!query.ok()) return 22;
  const int first_result =
      VerifyStateAt(*database, query.ValueOrDie(), vertex, score);
  if (first_result != 0) return first_result;

  if (!database->Close().ok()) return 30;
  database.reset();

  auto reopened = cedar::Database::Open({.path = path.string()});
  if (!reopened.ok()) return 31;
  database = std::move(reopened).ConsumeValueOrDie();
  const int reopened_result =
      VerifyStateAt(*database, query.ValueOrDie(), vertex, score);
  if (reopened_result != 0) return reopened_result;
  if (!database->Close().ok()) return 32;
  database.reset();
  std::filesystem::remove_all(path);
  return 0;
}
