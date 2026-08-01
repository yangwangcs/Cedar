#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "cedar/database.h"
#include "cedar/snapshot.h"
#include "cedar/transaction.h"

namespace cedar {
namespace {

static_assert(!std::is_copy_constructible_v<Database>);
static_assert(!std::is_copy_constructible_v<Snapshot>);
static_assert(!std::is_copy_constructible_v<Transaction>);
static_assert(std::is_move_constructible_v<Snapshot>);
static_assert(std::is_move_constructible_v<Transaction>);

template <typename T>
concept HasLegacyPut = requires(T& value) {
  value.Put(VertexId{1}, ValidTime{1});
};

template <typename T>
concept HasLegacyDelete = requires(T& value) {
  value.Delete(VertexId{1}, ValidTime{1});
};

static_assert(!HasLegacyPut<Database>);
static_assert(!HasLegacyDelete<Database>);

template <typename T>
concept HasVacuum = requires(T& value) {
  { value.Vacuum(CommitSeq{1}) } -> std::same_as<Status>;
};

static_assert(HasVacuum<Database>);

template <typename T>
concept HasTransactionResolution = requires(const T& value) {
  { value.ResolveTransaction(TxnId{1}) }
      -> std::same_as<StatusOr<std::optional<CommitResult>>>;
};

static_assert(HasTransactionResolution<Database>);

class KernelInterfaceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_kernel_interface_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
  }

  void TearDown() override { std::filesystem::remove_all(path_); }

  std::string path_;
};

TEST_F(KernelInterfaceTest, OpensExplicitKernelAndCreatesMoveOnlyHandles) {
  const auto database = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(database.ok()) << database.status().ToString();

  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  Snapshot moved_snapshot = std::move(snapshot).ConsumeValueOrDie();
  EXPECT_EQ(moved_snapshot.commit_seq(), CommitSeq{0});
  EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexId{1}),
                                           ValidTime{1})
                  .status()
                  .IsInvalidArgument());

  auto transaction = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  std::unique_ptr<Transaction> transaction_handle =
      std::move(transaction).ConsumeValueOrDie();
  Transaction moved_transaction = std::move(*transaction_handle);
  EXPECT_TRUE(moved_transaction.Rollback().ok());

  EXPECT_TRUE(database.ValueOrDie()->Close().IsSnapshotPinned());
}

TEST_F(KernelInterfaceTest, RejectsOperationsAfterCloseAndOnMovedFromHandles) {
  const auto database = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto transaction = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  std::unique_ptr<Transaction> transaction_handle =
      std::move(transaction).ConsumeValueOrDie();
  Transaction moved = std::move(*transaction_handle);
  EXPECT_TRUE(transaction_handle->Rollback().IsInvalidArgument());
  EXPECT_TRUE(moved.Rollback().ok());
  EXPECT_TRUE(moved.Rollback().IsInvalidArgument());

  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
  EXPECT_TRUE(database.ValueOrDie()->BeginTransaction().status().IsInvalidArgument());
  EXPECT_TRUE(database.ValueOrDie()->BeginSnapshot().status().IsInvalidArgument());
}

TEST_F(KernelInterfaceTest, ExposesVacuumAsAnExplicitKernelOperation) {
  const auto database = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(database.ok()) << database.status().ToString();

  EXPECT_TRUE(database.ValueOrDie()->Vacuum(CommitSeq{1}).IsNotSupportedError());
}

TEST_F(KernelInterfaceTest, RejectsLiveTransactionOperationsAfterClose) {
  const auto database = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  const auto transaction = database.ValueOrDie()->BeginTransaction(
      TransactionOptions{.isolation = IsolationLevel::kStrict});
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();

  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
  Transaction* const handle = transaction.ValueOrDie().get();
  const EntityFact entity = EntityFact::Vertex(VertexId{1});
  const PropertyFact property = PropertyFact::Vertex(VertexId{1}, PropertyId{1});

  EXPECT_TRUE(handle->Exists(entity, ValidTime{1}).status().IsInvalidArgument());
  EXPECT_TRUE(handle->Get(property, ValidTime{1}).status().IsInvalidArgument());
  EXPECT_TRUE(handle->Assert(entity, ValidTime{1}).IsInvalidArgument());
  EXPECT_TRUE(handle->Retract(entity, ValidTime{1}).IsInvalidArgument());
  EXPECT_TRUE(handle->Set(property, ValidTime{1}, Value::Int64(1)).IsInvalidArgument());
  EXPECT_TRUE(handle->Unset(property, ValidTime{1}).IsInvalidArgument());
  EXPECT_TRUE(handle->Commit().status().IsInvalidArgument());
  EXPECT_TRUE(handle->Rollback().ok());
}

TEST_F(KernelInterfaceTest, ResolvesNoCommittedTransactionAsAbsent) {
  const auto database = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(database.ok()) << database.status().ToString();

  const auto resolved = database.ValueOrDie()->ResolveTransaction(TxnId{1});
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  EXPECT_FALSE(resolved.ValueOrDie().has_value());
}

}  // namespace
}  // namespace cedar
