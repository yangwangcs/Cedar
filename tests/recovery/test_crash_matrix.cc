// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "cedar/database.h"

namespace cedar {
namespace {

#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define CEDAR_THREAD_SANITIZER_ENABLED 1
#endif
#endif
#ifndef CEDAR_THREAD_SANITIZER_ENABLED
#define CEDAR_THREAD_SANITIZER_ENABLED 0
#endif

class RecoveryCrashMatrixTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_recovery_matrix_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
  }

  void TearDown() override { std::filesystem::remove_all(path_); }

  std::unique_ptr<Database> Open(DatabaseOptions options = {}) {
    options.path = path_;
    auto opened = Database::Open(std::move(options));
    EXPECT_TRUE(opened.ok()) << opened.status().ToString();
    return opened.ok() ? std::move(opened).ConsumeValueOrDie() : nullptr;
  }

  CommitResult CommitVertex(Database& database, uint64_t vertex_id,
                            uint64_t valid_time) {
    auto begun = database.BeginTransaction();
    EXPECT_TRUE(begun.ok()) << begun.status().ToString();
    if (!begun.ok()) return {};
    std::unique_ptr<Transaction> transaction = std::move(begun).ConsumeValueOrDie();
    EXPECT_TRUE(transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}),
                                    ValidTime{valid_time})
                    .ok());
    const auto committed = transaction->Commit();
    EXPECT_TRUE(committed.ok()) << committed.status().ToString();
    return committed.ok() ? committed.ValueOrDie() : CommitResult{};
  }

  std::string path_;
};

TEST_F(RecoveryCrashMatrixTest, ReopenDistinguishesPrewriteAndPostwriteBoundaries) {
  auto prewrite = Open(DatabaseOptions{
      .commit_prewrite_fault_injector_for_testing = [] {
        return Status::Indeterminate("test", "prewrite crash");
      }});
  ASSERT_TRUE(prewrite);
  const CommitResult before = CommitVertex(*prewrite, 1, 10);
  EXPECT_EQ(before.outcome, CommitOutcome::kIndeterminate);
  const TxnId before_id = before.txn_id;
  prewrite.reset();

  auto reopened = Open();
  ASSERT_TRUE(reopened);
  const auto before_resolution = reopened->ResolveTransaction(before_id);
  ASSERT_TRUE(before_resolution.ok()) << before_resolution.status().ToString();
  EXPECT_FALSE(before_resolution.ValueOrDie().has_value());
  EXPECT_FALSE(reopened->BeginSnapshot().ValueOrDie()
                   .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                   .ValueOrDie());
  reopened.reset();

  auto postwrite = Open(DatabaseOptions{
      .commit_fault_injector_for_testing = [] {
        return Status::Indeterminate("test", "postwrite crash");
      }});
  ASSERT_TRUE(postwrite);
  const CommitResult after = CommitVertex(*postwrite, 2, 20);
  EXPECT_EQ(after.outcome, CommitOutcome::kIndeterminate);
  const TxnId after_id = after.txn_id;
  postwrite.reset();

  reopened = Open();
  ASSERT_TRUE(reopened);
  const auto after_resolution = reopened->ResolveTransaction(after_id);
  ASSERT_TRUE(after_resolution.ok()) << after_resolution.status().ToString();
  ASSERT_TRUE(after_resolution.ValueOrDie().has_value());
  EXPECT_EQ(after_resolution.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  EXPECT_TRUE(reopened->BeginSnapshot().ValueOrDie()
                  .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{20})
                  .ValueOrDie());
}

TEST_F(RecoveryCrashMatrixTest,
       KernelReopenRecoversKernelWriteAcrossPrewriteAndPostwriteBoundaries) {
  const auto kernel_options = [] {
    DatabaseOptions options;
    options.storage_profile = StorageProfile::kProductionAppend;
    options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
    options.production.kernel_mode = true;
    options.runtime_pressure_override_for_testing = [](PressureSample* sample) {
      // This recovery test exercises the write-boundary fault contract, not
      // host-dependent disk admission.
      sample->free_disk_bytes = UINT64_MAX;
      sample->free_disk_percent = 100;
    };
    return options;
  };

  DatabaseOptions prewrite_options = kernel_options();
  prewrite_options.commit_prewrite_fault_injector_for_testing = [] {
    return Status::Indeterminate("test", "kernel prewrite crash");
  };
  auto prewrite = Open(std::move(prewrite_options));
  ASSERT_TRUE(prewrite);
  const CommitResult before = CommitVertex(*prewrite, 11, 110);
  EXPECT_EQ(before.outcome, CommitOutcome::kIndeterminate);
  EXPECT_TRUE(before.status.IsIndeterminate()) << before.status.ToString();
  const TxnId before_id = before.txn_id;
  prewrite.reset();

  auto reopened = Open();
  ASSERT_TRUE(reopened);
  const auto before_resolution = reopened->ResolveTransaction(before_id);
  ASSERT_TRUE(before_resolution.ok()) << before_resolution.status().ToString();
  EXPECT_FALSE(before_resolution.ValueOrDie().has_value());
  EXPECT_FALSE(reopened->BeginSnapshot().ValueOrDie()
                   .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{11}}),
                           ValidTime{110})
                   .ValueOrDie());
  reopened.reset();

  DatabaseOptions postwrite_options = kernel_options();
  postwrite_options.commit_fault_injector_for_testing = [] {
    return Status::Indeterminate("test", "kernel postwrite crash");
  };
  auto postwrite = Open(std::move(postwrite_options));
  ASSERT_TRUE(postwrite);
  const CommitResult after = CommitVertex(*postwrite, 12, 120);
  EXPECT_EQ(after.outcome, CommitOutcome::kIndeterminate);
  EXPECT_TRUE(after.status.IsIndeterminate()) << after.status.ToString();
  const TxnId after_id = after.txn_id;
  postwrite.reset();

  reopened = Open();
  ASSERT_TRUE(reopened);
  const auto after_resolution = reopened->ResolveTransaction(after_id);
  ASSERT_TRUE(after_resolution.ok()) << after_resolution.status().ToString();
  ASSERT_TRUE(after_resolution.ValueOrDie().has_value());
  EXPECT_EQ(after_resolution.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  EXPECT_TRUE(reopened->BeginSnapshot().ValueOrDie()
                  .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{12}}),
                          ValidTime{120})
                  .ValueOrDie());
}

TEST_F(RecoveryCrashMatrixTest, ReopenRecoversActualProcessCrashesAtCommitBoundaries) {
#if CEDAR_THREAD_SANITIZER_ENABLED
  GTEST_SKIP() << "ThreadSanitizer cannot create threads after a multithreaded fork";
#endif
  const auto crash_commit = [this](uint64_t vertex_id, uint64_t valid_time,
                                   bool after_write) {
    const pid_t child = fork();
    ASSERT_NE(child, -1);
    if (child == 0) {
      DatabaseOptions options;
      options.path = path_;
      const auto crash = []() -> Status { _exit(91); };
      if (after_write) {
        options.commit_fault_injector_for_testing = crash;
      } else {
        options.commit_prewrite_fault_injector_for_testing = crash;
      }
      auto opened = Database::Open(std::move(options));
      if (!opened.ok()) _exit(2);
      std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
      auto begun = database->BeginTransaction();
      if (!begun.ok()) _exit(3);
      std::unique_ptr<Transaction> transaction = std::move(begun).ConsumeValueOrDie();
      if (!transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}),
                               ValidTime{valid_time})
               .ok()) {
        _exit(4);
      }
      const auto committed = transaction->Commit();
      static_cast<void>(committed);
      _exit(5);
    }
    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 91);
  };

  crash_commit(3, 30, false);
  auto reopened = Open();
  ASSERT_TRUE(reopened);
  const auto before = reopened->ResolveTransaction(TxnId{1});
  ASSERT_TRUE(before.ok()) << before.status().ToString();
  EXPECT_FALSE(before.ValueOrDie().has_value());
  EXPECT_FALSE(reopened->BeginSnapshot().ValueOrDie()
                   .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{3}}), ValidTime{30})
                   .ValueOrDie());
  reopened.reset();

  crash_commit(4, 40, true);
  reopened = Open();
  ASSERT_TRUE(reopened);
  const auto after = reopened->ResolveTransaction(TxnId{4097});
  ASSERT_TRUE(after.ok()) << after.status().ToString();
  ASSERT_TRUE(after.ValueOrDie().has_value());
  EXPECT_EQ(after.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  EXPECT_TRUE(reopened->BeginSnapshot().ValueOrDie()
                  .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{4}}), ValidTime{40})
                  .ValueOrDie());
}

TEST_F(RecoveryCrashMatrixTest, ReopenRecoversAsyncFinalPublishAfterDurableAcceptance) {
  const pid_t child = fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    DatabaseOptions options;
    options.path = path_;
    options.group_commit_max_batch_size = 1;
    options.group_commit_window_us = 0;
    options.commit_fault_injector_for_testing = []() -> Status { _exit(92); };
    auto opened = Database::Open(std::move(options));
    if (!opened.ok()) _exit(2);
    std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
    auto begun = database->BeginTransaction();
    if (!begun.ok()) _exit(3);
    std::unique_ptr<Transaction> transaction = std::move(begun).ConsumeValueOrDie();
    if (!transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{5}}), ValidTime{50}).ok()) {
      _exit(4);
    }
    const auto accepted = transaction->CommitAsync();
    if (!accepted.ok()) _exit(5);
    const auto completed = accepted.ValueOrDie().Wait();
    static_cast<void>(completed);
    _exit(6);
  }
  int status = 0;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 92);

  auto reopened = Open();
  ASSERT_TRUE(reopened);
  const auto resolved = reopened->ResolveTransaction(TxnId{1});
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  ASSERT_TRUE(resolved.ValueOrDie().has_value());
  EXPECT_EQ(resolved.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  EXPECT_TRUE(reopened->BeginSnapshot().ValueOrDie()
                  .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{5}}), ValidTime{50})
                  .ValueOrDie());
}

TEST_F(RecoveryCrashMatrixTest, ReopenSkipsDurableLeaseRemainders) {
  auto database = Open();
  ASSERT_TRUE(database);
  EXPECT_EQ(database->AllocateVertexId().ValueOrDie(), VertexId{1});
  EXPECT_EQ(database->AllocateEdgeId().ValueOrDie(), EdgeId{1});
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  database = Open();
  ASSERT_TRUE(database);
  EXPECT_EQ(database->AllocateVertexId().ValueOrDie(), VertexId{4097});
  EXPECT_EQ(database->AllocateEdgeId().ValueOrDie(), EdgeId{4097});
}

TEST_F(RecoveryCrashMatrixTest, ReopenResumesVacuumWithoutAnIndependentFactsStore) {
  auto database = Open();
  ASSERT_TRUE(database);
  ASSERT_EQ(CommitVertex(*database, 1, 10).outcome, CommitOutcome::kCommitted);
  ASSERT_EQ(CommitVertex(*database, 1, 20).outcome, CommitOutcome::kCommitted);
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  auto faulted = Open(DatabaseOptions{
      .vacuum_fault_injector_for_testing = [](VacuumFaultPoint point) {
        return point == VacuumFaultPoint::kAfterBoundaryWrite
                   ? Status::Indeterminate("test", "vacuum crash")
                   : Status::OK();
      }});
  ASSERT_TRUE(faulted);
  EXPECT_TRUE(faulted->Vacuum(CommitSeq{2}).IsIndeterminate());
  faulted.reset();

  database = Open();
  ASSERT_TRUE(database);
  EXPECT_TRUE(database->BeginSnapshot(SnapshotOptions{CommitSeq{1}})
                  .status()
                  .IsSnapshotExpired());
  const auto latest = database->BeginSnapshot();
  ASSERT_TRUE(latest.ok()) << latest.status().ToString();
  EXPECT_TRUE(latest.ValueOrDie()
                  .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{20})
                  .ValueOrDie());
  size_t events = 0;
  ASSERT_TRUE(latest.ValueOrDie()
                  .Scan(FactFamily::kVertexState, PropertyId{},
                        [&events](const FactEvent&) {
                          ++events;
                          return Status::OK();
                        })
                  .ok());
  EXPECT_EQ(events, 2U);
}

}  // namespace
}  // namespace cedar
