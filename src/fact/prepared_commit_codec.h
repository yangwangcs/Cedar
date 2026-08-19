// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_PREPARED_COMMIT_CODEC_H_
#define CEDAR_FACT_PREPARED_COMMIT_CODEC_H_

#include <string>

#include "cedar/core/status.h"
#include "cedar/fact/fact_store.h"

namespace cedar::internal {

std::string PreparedCommitPrefix();
StatusOr<std::string> EncodePreparedCommitKey(TxnId txn_id);
StatusOr<std::string> EncodePreparedCommit(const StoreCommitBatch& batch);
StatusOr<StoreCommitBatch> DecodePreparedCommit(const std::string& encoded);

std::string AsyncTerminalPrefix();
StatusOr<std::string> EncodeAsyncTerminalKey(TxnId txn_id);
StatusOr<std::string> EncodeAsyncAbortTerminal(TxnId txn_id);
StatusOr<TxnId> DecodeAsyncAbortTerminal(const std::string& encoded);

}  // namespace cedar::internal

#endif  // CEDAR_FACT_PREPARED_COMMIT_CODEC_H_
