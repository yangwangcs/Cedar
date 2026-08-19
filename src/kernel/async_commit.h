// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_KERNEL_ASYNC_COMMIT_H_
#define CEDAR_KERNEL_ASYNC_COMMIT_H_

#include <cstddef>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>

#include "cedar/transaction.h"

namespace cedar::internal {
class EpochCompletion;
}  // namespace cedar::internal

namespace cedar {

class CommitHandle::State {
 public:
  explicit State(TxnId transaction_id,
                 CommitAcceptance acceptance = CommitAcceptance::kAccepted)
      : txn_id(transaction_id), acceptance(acceptance) {}

  const TxnId txn_id;
  const CommitAcceptance acceptance;
  mutable std::mutex mutex;
  std::condition_variable completed;
  std::optional<CommitResult> result;
  std::shared_ptr<internal::EpochCompletion> epoch_completion;
  size_t epoch_result_ordinal = 0;
  bool waits_for_epoch_durability = false;
};

}  // namespace cedar

#endif  // CEDAR_KERNEL_ASYNC_COMMIT_H_
