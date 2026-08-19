#include <chrono>
#include <filesystem>
#include <memory>
#include <string>

#include "cedar/database.h"

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

  {
    auto transaction = database->BeginTransaction();
    if (!transaction.ok()) return 2;
    auto commit = transaction.ValueOrDie()->Commit();
    if (!commit.ok()) return 3;
  }
  {
    auto snapshot = database->BeginSnapshot();
    if (!snapshot.ok()) return 4;
    const cedar::Status scan = snapshot.ValueOrDie().StateColumnarScan(
        {cedar::PartId{0}, cedar::FactFamily::kVertexState,
         cedar::PropertyId{}, cedar::ValidTime{}, 1},
        {cedar::FactColumnId::kEntityId},
        [](const cedar::FactColumnarBatch&) { return cedar::Status::OK(); });
    if (!scan.ok()) return 5;
  }
  if (!database->Close().ok()) return 6;
  std::filesystem::remove_all(path);
  return 0;
}
