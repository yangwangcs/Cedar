// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <vector>

#include "kernel/epoch_completion.h"

namespace cedar::internal {
namespace {

TEST(EpochCompletionTest,
     PublishesOneBarrierWhilePreservingEveryRequestResult) {
  constexpr size_t kRequestCount = 128;
  EpochCompletion completion(kRequestCount);

  std::vector<CommitResult> results;
  results.reserve(kRequestCount);
  for (size_t index = 0; index < kRequestCount; ++index) {
    results.push_back(CommitResult{CommitOutcome::kCommitted,
                                   CommitSeq{1000 + index},
                                   TxnId{2000 + index}, Status::OK()});
  }

  EXPECT_EQ(completion.publication_barrier_count(), 0U);
  completion.Publish(std::move(results));
  EXPECT_EQ(completion.publication_barrier_count(), 1U);

  for (size_t index = 0; index < kRequestCount; ++index) {
    const auto result = completion.WaitForResult(index);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_EQ(result.ValueOrDie().outcome, CommitOutcome::kCommitted);
    EXPECT_EQ(result.ValueOrDie().commit_seq, CommitSeq{1000 + index});
    EXPECT_EQ(result.ValueOrDie().txn_id, TxnId{2000 + index});
  }
}

}  // namespace
}  // namespace cedar::internal
