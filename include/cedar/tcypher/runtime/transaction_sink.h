// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_TRANSACTION_SINK_H_
#define CEDAR_TCYPHER_RUNTIME_TRANSACTION_SINK_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/tcypher/runtime/cancellation.h"
#include "cedar/transaction/transaction_coordinator.h"

namespace cedar {

class TcypherSession;

struct TypedMutation {
  LogicalKey logical_key;
  uint64_t valid_from;
  uint32_t schema_epoch;
  TemporalOperation operation;
  Value value;

  static TypedMutation Put(LogicalKey key, uint64_t valid_from, uint32_t schema_epoch,
                           Value value) {
    return TypedMutation{std::move(key), valid_from, schema_epoch, TemporalOperation::kPut,
                         std::move(value)};
  }
  static TypedMutation Delete(LogicalKey key, uint64_t valid_from, uint32_t schema_epoch) {
    return TypedMutation{std::move(key), valid_from, schema_epoch, TemporalOperation::kDelete,
                         Value::Binary("", 0)};
  }
};

class TransactionSink {
 public:
  TransactionSink(TransactionCoordinator* coordinator, uint64_t snapshot_seq,
                  std::shared_ptr<QueryCancellation> cancellation = nullptr,
                  TcypherSession* session = nullptr)
      : coordinator_(coordinator), snapshot_seq_(snapshot_seq),
        cancellation_(std::move(cancellation)), session_(session) {}

  Status Submit(const std::vector<TypedMutation>& mutations, uint64_t* commit_seq);

 private:
  TransactionCoordinator* coordinator_;
  uint64_t snapshot_seq_;
  std::shared_ptr<QueryCancellation> cancellation_;
  TcypherSession* session_;
};

}  // namespace cedar

#endif  // CEDAR_TCYPHER_RUNTIME_TRANSACTION_SINK_H_
