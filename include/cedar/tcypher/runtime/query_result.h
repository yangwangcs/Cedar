// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_QUERY_RESULT_H_
#define CEDAR_TCYPHER_RUNTIME_QUERY_RESULT_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/db/database_lifecycle.h"
#include "cedar/runtime/resource_profile.h"
#include "cedar/tcypher/runtime/cancellation.h"
#include "cedar/tcypher/runtime/column_batch.h"
#include "cedar/tcypher/runtime/query_memory.h"

namespace cedar {

class PartitionedSpillSet;
class QuerySpillFile;

struct ResultTemporalMetadata {
  uint64_t snapshot_seq = 0;
  bool includes_valid_time = false;
  bool includes_system_time = false;
};

// Result values retain their physical vector shape at the protocol boundary.
// They are deliberately separate from storage Value because relationships and
// paths are composite query values, not persistable scalar values.
enum class ResultValueKind : uint8_t { kScalar, kStruct, kList };

template <typename T>
class ResultCompositeValue {
 public:
  ResultCompositeValue() = default;
  ResultCompositeValue(const T& value) : value_(std::make_unique<T>(value)) {}
  ResultCompositeValue(T&& value)
      : value_(std::make_unique<T>(std::move(value))) {}
  ResultCompositeValue(const std::optional<T>& value) {
    if (value.has_value()) value_ = std::make_unique<T>(*value);
  }
  ResultCompositeValue(std::optional<T>&& value) {
    if (value.has_value()) value_ = std::make_unique<T>(std::move(*value));
  }
  ResultCompositeValue(const ResultCompositeValue& other) {
    if (other.value_) value_ = std::make_unique<T>(*other.value_);
  }
  ResultCompositeValue& operator=(const ResultCompositeValue& other) {
    if (this == &other) return *this;
    value_ = other.value_ ? std::make_unique<T>(*other.value_) : nullptr;
    return *this;
  }
  ResultCompositeValue(ResultCompositeValue&&) noexcept = default;
  ResultCompositeValue& operator=(ResultCompositeValue&&) noexcept = default;
  ResultCompositeValue& operator=(const T& value) {
    value_ = std::make_unique<T>(value);
    return *this;
  }
  ResultCompositeValue& operator=(T&& value) {
    value_ = std::make_unique<T>(std::move(value));
    return *this;
  }
  ResultCompositeValue& operator=(const std::optional<T>& value) {
    value_ = value.has_value() ? std::make_unique<T>(*value) : nullptr;
    return *this;
  }
  ResultCompositeValue& operator=(std::optional<T>&& value) {
    value_ = value.has_value()
        ? std::make_unique<T>(std::move(*value)) : nullptr;
    return *this;
  }

  bool has_value() const { return value_ != nullptr; }
  T& operator*() { return *value_; }
  const T& operator*() const { return *value_; }
  T* operator->() { return value_.get(); }
  const T* operator->() const { return value_.get(); }
  T value_or(T fallback) const {
    return value_ ? *value_ : std::move(fallback);
  }
  void reset() { value_.reset(); }

 private:
  std::unique_ptr<T> value_;
};

struct ResultValueCell {
  ResultValueKind kind = ResultValueKind::kScalar;
  std::optional<Value> scalar;
  ResultCompositeValue<StructValue> structure;
  ResultCompositeValue<ListValue> list;
};

struct QueryOperatorResourceStats {
  uint64_t memory_peak_bytes = 0;
  uint64_t spill_bytes = 0;
  bool spill_started = false;
};

class ResultBatch {
 public:
  ResultBatch() = default;
  ResultBatch(std::vector<std::string> column_names, ColumnBatch batch,
              ResultTemporalMetadata temporal_metadata = {})
      : column_names_(std::move(column_names)),
        batch_(std::move(batch)),
        temporal_metadata_(temporal_metadata) {}

  Status Validate() const;
  const std::vector<std::string>& column_names() const { return column_names_; }
  const ColumnBatch& batch() const { return batch_; }
  const ResultTemporalMetadata& temporal_metadata() const {
    return temporal_metadata_;
  }

 private:
  std::vector<std::string> column_names_;
  ColumnBatch batch_;
  ResultTemporalMetadata temporal_metadata_;
};

class QueryResultStream {
 public:
  virtual ~QueryResultStream() = default;
  virtual Status Next(ResultBatch* batch) = 0;
  virtual Status terminal_status() const = 0;
  virtual QueryOperatorResourceStats operator_resource_stats() const {
    return {};
  }
};

class LifecycleTrackedResultStream final : public QueryResultStream {
 public:
  LifecycleTrackedResultStream(
      std::unique_ptr<QueryResultStream> input,
      std::shared_ptr<DatabaseQueryRegistration> registration);
  ~LifecycleTrackedResultStream() override;

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override;
  QueryOperatorResourceStats operator_resource_stats() const override;

 private:
  struct State;
  std::shared_ptr<State> state_;
  std::shared_ptr<DatabaseQueryRegistration> registration_;
  Status terminal_status_ = Status::OK();
};

class CancellableResultStream final : public QueryResultStream {
 public:
  CancellableResultStream(std::unique_ptr<QueryResultStream> input,
                          std::shared_ptr<QueryCancellation> cancellation)
      : input_(std::move(input)), cancellation_(std::move(cancellation)) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override {
    return terminal_status_.ok() && input_ ? input_->terminal_status() : terminal_status_;
  }
  QueryOperatorResourceStats operator_resource_stats() const override {
    return input_ ? input_->operator_resource_stats()
                  : QueryOperatorResourceStats{};
  }

 private:
  std::unique_ptr<QueryResultStream> input_;
  std::shared_ptr<QueryCancellation> cancellation_;
  Status terminal_status_ = Status::OK();
};

class MemoryAccountedResultStream final : public QueryResultStream {
 public:
  MemoryAccountedResultStream(std::unique_ptr<QueryResultStream> input,
                              std::shared_ptr<QueryMemoryAccount> account,
                              uint64_t reserved_bytes)
      : input_(std::move(input)), account_(std::move(account)), reserved_bytes_(reserved_bytes) {}
  ~MemoryAccountedResultStream() override;

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override {
    return input_ ? input_->terminal_status() : Status::OK();
  }
  QueryOperatorResourceStats operator_resource_stats() const override {
    return input_ ? input_->operator_resource_stats()
                  : QueryOperatorResourceStats{};
  }

 private:
  std::unique_ptr<QueryResultStream> input_;
  std::shared_ptr<QueryMemoryAccount> account_;
  uint64_t reserved_bytes_ = 0;
};

// Keeps the database-wide query grant alive until the client releases its
// result stream. This includes streams that have produced their first batch
// but still retain a snapshot and operator state for subsequent Next calls.
class ResourceAccountedResultStream final : public QueryResultStream {
 public:
  ResourceAccountedResultStream(std::unique_ptr<QueryResultStream> input,
                                ResourceLease reservation)
      : input_(std::move(input)), reservation_(std::move(reservation)) {}

  Status Next(ResultBatch* batch) override {
    return input_ == nullptr ? Status::InvalidArgument("query result", "missing result stream")
                             : input_->Next(batch);
  }
  Status terminal_status() const override {
    return input_ == nullptr ? Status::InvalidArgument("query result", "missing result stream")
                             : input_->terminal_status();
  }
  QueryOperatorResourceStats operator_resource_stats() const override {
    return input_ ? input_->operator_resource_stats()
                  : QueryOperatorResourceStats{};
  }

 private:
  std::unique_ptr<QueryResultStream> input_;
  ResourceLease reservation_;
};

// A protocol-boundary test stream. Physical operators produce ResultBatch
// values; adapters decide how to serialize them after this boundary.
class InMemoryResultStream final : public QueryResultStream {
 public:
  InMemoryResultStream(std::vector<ResultBatch> batches, Status terminal_status)
      : batches_(std::move(batches)), terminal_status_(std::move(terminal_status)) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }

 private:
  std::vector<ResultBatch> batches_;
  Status terminal_status_;
  size_t next_batch_ = 0;
};

class ProjectColumnsResultStream final : public QueryResultStream {
 public:
  ProjectColumnsResultStream(std::unique_ptr<QueryResultStream> input,
                             std::vector<uint32_t> columns,
                             std::vector<std::string> names)
      : input_(std::move(input)),
        columns_(std::move(columns)),
        names_(std::move(names)) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override {
    return terminal_status_.ok() && input_
        ? input_->terminal_status() : terminal_status_;
  }
  QueryOperatorResourceStats operator_resource_stats() const override {
    return input_ ? input_->operator_resource_stats()
                  : QueryOperatorResourceStats{};
  }

 private:
  std::unique_ptr<QueryResultStream> input_;
  std::vector<uint32_t> columns_;
  std::vector<std::string> names_;
  Status terminal_status_ = Status::OK();
};

class LimitedResultStream final : public QueryResultStream {
 public:
  LimitedResultStream(std::unique_ptr<QueryResultStream> input, uint64_t limit)
      : input_(std::move(input)), remaining_(limit) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override {
    return input_ ? input_->terminal_status() : Status::OK();
  }

 private:
  std::unique_ptr<QueryResultStream> input_;
  uint64_t remaining_;
};

class SkipResultStream final : public QueryResultStream {
 public:
  SkipResultStream(std::unique_ptr<QueryResultStream> input, uint64_t skip)
      : input_(std::move(input)), remaining_(skip) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override {
    return input_ ? input_->terminal_status() : Status::OK();
  }

 private:
  std::unique_ptr<QueryResultStream> input_;
  uint64_t remaining_;
};

class DistinctResultStream final : public QueryResultStream {
 public:
  explicit DistinctResultStream(std::unique_ptr<QueryResultStream> input,
                                std::shared_ptr<QueryCancellation> cancellation = nullptr,
                                std::shared_ptr<QueryMemoryAccount> memory_account = nullptr,
                                std::string spill_directory = {},
                                std::shared_ptr<ResourceGovernorExtension> spill_resources = nullptr);
  ~DistinctResultStream() override;

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override {
    return terminal_status_.ok() && input_
        ? input_->terminal_status() : terminal_status_;
  }
  QueryOperatorResourceStats operator_resource_stats() const override {
    return QueryOperatorResourceStats{
        memory_peak_bytes_, spill_bytes_, spilling_};
  }

 private:
  Status BeginSpill();
  Status DrainInputToSpill();
  StatusOr<bool> OpenNextSpillPartition();
  Status NextSpilled(ResultBatch* batch);
  void ReleaseSeenRows();

  std::unique_ptr<QueryResultStream> input_;
  std::shared_ptr<QueryCancellation> cancellation_;
  std::shared_ptr<QueryMemoryAccount> memory_account_;
  std::string spill_directory_;
  std::shared_ptr<ResourceGovernorExtension> spill_resources_;
  uint64_t reserved_bytes_ = 0;
  uint64_t memory_peak_bytes_ = 0;
  uint64_t spill_bytes_ = 0;
  std::vector<std::string> seen_rows_;
  std::unique_ptr<PartitionedSpillSet> emitted_keys_spill_;
  std::unique_ptr<PartitionedSpillSet> rows_spill_;
  std::optional<ResultBatch> pending_batch_;
  uint32_t pending_row_ = 0;
  uint32_t next_spill_partition_ = 0;
  uint32_t current_spill_partition_ = 0;
  bool spilling_ = false;
  bool input_drained_ = false;
  bool spill_partition_open_ = false;
  Status terminal_status_ = Status::OK();
};

class SortResultStream final : public QueryResultStream {
 public:
  SortResultStream(std::unique_ptr<QueryResultStream> input, uint32_t column,
                   bool descending, uint32_t batch_capacity,
                   std::shared_ptr<QueryCancellation> cancellation = nullptr,
                   std::shared_ptr<QueryMemoryAccount> memory_account = nullptr,
                   std::string spill_directory = {},
                   std::shared_ptr<ResourceGovernorExtension> spill_resources = nullptr);
  ~SortResultStream() override;

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }
  QueryOperatorResourceStats operator_resource_stats() const override {
    return QueryOperatorResourceStats{
        memory_peak_bytes_, spill_bytes_, spilling_};
  }

 private:
  struct MergeCursor {
    std::unique_ptr<QuerySpillFile> spill;
    ResultBatch batch;
    uint32_t row = 0;
    uint32_t run_ordinal = 0;
    bool has_row = false;
  };

  Status Initialize();
  Status CheckCancelled() const;
  Status SpillCurrentRun();
  Status PrepareMerge();
  StatusOr<std::unique_ptr<QuerySpillFile>> MergeRunGroup(
      std::vector<std::unique_ptr<QuerySpillFile>>* runs, size_t begin, size_t end);
  Status LoadNextMergeBatch(std::vector<MergeCursor>* cursors, size_t cursor);
  Status AppendRowsToSpill(QuerySpillFile* spill,
                           const std::vector<std::vector<ResultValueCell>>& rows,
                           size_t start, size_t end);
  bool CursorPrecedes(const std::vector<MergeCursor>& cursors,
                      size_t left, size_t right) const;
  Status NextMerged(ResultBatch* batch);
  void ReleaseRows();

  std::unique_ptr<QueryResultStream> input_;
  uint32_t column_;
  bool descending_;
  uint32_t batch_capacity_;
  std::shared_ptr<QueryCancellation> cancellation_;
  std::shared_ptr<QueryMemoryAccount> memory_account_;
  std::string spill_directory_;
  std::shared_ptr<ResourceGovernorExtension> spill_resources_;
  uint64_t reserved_bytes_ = 0;
  uint64_t memory_peak_bytes_ = 0;
  uint64_t spill_bytes_ = 0;
  std::vector<std::string> column_names_;
  ResultTemporalMetadata temporal_metadata_;
  std::vector<ResultValueKind> column_kinds_;
  std::vector<std::vector<ResultValueCell>> rows_;
  std::vector<std::unique_ptr<QuerySpillFile>> spilled_runs_;
  std::vector<MergeCursor> merge_cursors_;
  std::vector<size_t> merge_heap_;
  Status terminal_status_ = Status::OK();
  size_t next_row_ = 0;
  bool spilling_ = false;
  bool merge_ready_ = false;
  bool initialized_ = false;
};

class CountResultStream final : public QueryResultStream {
 public:
  CountResultStream(std::unique_ptr<QueryResultStream> input, std::string column_name)
      : input_(std::move(input)), column_name_(std::move(column_name)) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }

 private:
  std::unique_ptr<QueryResultStream> input_;
  std::string column_name_;
  Status terminal_status_ = Status::OK();
  bool emitted_ = false;
};

struct ResultOutputSlot {
  bool aggregate = false;
  uint32_t index = 0;
};

class CollectResultStream final : public QueryResultStream {
 public:
  CollectResultStream(
      std::unique_ptr<QueryResultStream> input, std::string column_name,
      std::shared_ptr<QueryCancellation> cancellation = nullptr,
      std::shared_ptr<QueryMemoryAccount> memory_account = nullptr,
      std::optional<ResultValueKind> expected_kind = std::nullopt)
      : input_(std::move(input)), column_name_(std::move(column_name)),
        cancellation_(std::move(cancellation)),
        memory_account_(std::move(memory_account)),
        expected_kind_(expected_kind) {}
  ~CollectResultStream() override;
  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }
  QueryOperatorResourceStats operator_resource_stats() const override {
    return QueryOperatorResourceStats{memory_peak_bytes_, 0, false};
  }
 private:
  std::unique_ptr<QueryResultStream> input_;
  std::string column_name_;
  std::shared_ptr<QueryCancellation> cancellation_;
  std::shared_ptr<QueryMemoryAccount> memory_account_;
  std::optional<ResultValueKind> expected_kind_;
  uint64_t reserved_bytes_ = 0;
  uint64_t memory_peak_bytes_ = 0;
  Status terminal_status_ = Status::OK();
  bool emitted_ = false;
};

class GroupedCollectResultStream final : public QueryResultStream {
 public:
  GroupedCollectResultStream(std::unique_ptr<QueryResultStream> input,
                             std::vector<uint32_t> group_columns, uint32_t collect_column,
                             std::string collect_name, std::vector<ResultOutputSlot> output_slots,
                             uint32_t batch_capacity,
                             std::shared_ptr<QueryCancellation> cancellation = nullptr,
                             std::shared_ptr<QueryMemoryAccount> memory_account = nullptr,
                             std::string spill_directory = {},
                             std::shared_ptr<ResourceGovernorExtension> spill_resources = nullptr,
                             std::optional<ResultValueKind> expected_collect_kind = std::nullopt);
  ~GroupedCollectResultStream() override;

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }
  QueryOperatorResourceStats operator_resource_stats() const override {
    return QueryOperatorResourceStats{
        memory_peak_bytes_, spill_bytes_, spilling_};
  }

 private:
  struct Group {
    std::vector<ResultValueCell> values;
    ListValue collected;
  };

  Status Initialize();
  StatusOr<uint64_t> MeasureGroup(
      const std::string& record, uint64_t* decode_temporary_bytes) const;
  Status MeasureEncodedGroup(const Group& group, uint64_t* group_key_bytes,
                             uint64_t* encoded_bytes,
                             uint64_t* encode_temporary_bytes) const;
  StatusOr<std::string> EncodeGroup(const Group& group,
                                    uint64_t encoded_bytes) const;
  StatusOr<Group> DecodeGroup(const std::string& record) const;
  Status AppendSpillGroup(const Group& group);
  Status SpillGroups();
  StatusOr<bool> LoadNextSpillPartition();
  void ReleaseGroups();

  std::unique_ptr<QueryResultStream> input_;
  std::vector<uint32_t> group_columns_;
  uint32_t collect_column_;
  std::string collect_name_;
  std::vector<ResultOutputSlot> output_slots_;
  uint32_t batch_capacity_;
  std::shared_ptr<QueryCancellation> cancellation_;
  std::shared_ptr<QueryMemoryAccount> memory_account_;
  std::string spill_directory_;
  std::shared_ptr<ResourceGovernorExtension> spill_resources_;
  uint64_t reserved_bytes_ = 0;
  uint64_t memory_peak_bytes_ = 0;
  uint64_t spill_bytes_ = 0;
  std::vector<std::string> group_names_;
  std::vector<ResultValueKind> group_kinds_;
  std::optional<ResultValueKind> collect_kind_;
  ResultTemporalMetadata temporal_metadata_;
  std::vector<Group> groups_;
  std::unique_ptr<PartitionedSpillSet> spill_;
  Status terminal_status_ = Status::OK();
  size_t next_group_ = 0;
  uint32_t next_spill_partition_ = 0;
  bool spilling_ = false;
  bool initialized_ = false;
};

class GroupedCountResultStream final : public QueryResultStream {
 public:
  GroupedCountResultStream(std::unique_ptr<QueryResultStream> input,
                           std::vector<uint32_t> group_columns, uint32_t count_column,
                           std::string count_name, uint32_t batch_capacity)
      : input_(std::move(input)), group_columns_(std::move(group_columns)),
        count_column_(count_column), count_name_(std::move(count_name)),
        batch_capacity_(batch_capacity) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }

 private:
  struct Group {
    std::vector<std::optional<Value>> values;
    uint64_t count = 0;
  };

  Status Initialize();

  std::unique_ptr<QueryResultStream> input_;
  std::vector<uint32_t> group_columns_;
  uint32_t count_column_;
  std::string count_name_;
  uint32_t batch_capacity_;
  std::vector<std::string> group_names_;
  ResultTemporalMetadata temporal_metadata_;
  std::vector<Group> groups_;
  Status terminal_status_ = Status::OK();
  size_t next_group_ = 0;
  bool initialized_ = false;
};

class GroupedSumResultStream final : public QueryResultStream {
 public:
  GroupedSumResultStream(std::unique_ptr<QueryResultStream> input,
                         std::vector<uint32_t> group_columns, uint32_t sum_column,
                         std::string sum_name, uint32_t batch_capacity)
      : input_(std::move(input)), group_columns_(std::move(group_columns)),
        sum_column_(sum_column), sum_name_(std::move(sum_name)), batch_capacity_(batch_capacity) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }

 private:
  struct Group {
    std::vector<std::optional<Value>> values;
    int64_t sum = 0;
    bool has_value = false;
  };

  Status Initialize();

  std::unique_ptr<QueryResultStream> input_;
  std::vector<uint32_t> group_columns_;
  uint32_t sum_column_;
  std::string sum_name_;
  uint32_t batch_capacity_;
  std::vector<std::string> group_names_;
  ResultTemporalMetadata temporal_metadata_;
  std::vector<Group> groups_;
  Status terminal_status_ = Status::OK();
  size_t next_group_ = 0;
  bool initialized_ = false;
};

class GroupedAvgResultStream final : public QueryResultStream {
 public:
  GroupedAvgResultStream(std::unique_ptr<QueryResultStream> input,
                         std::vector<uint32_t> group_columns, uint32_t avg_column,
                         std::string avg_name, uint32_t batch_capacity)
      : input_(std::move(input)), group_columns_(std::move(group_columns)),
        avg_column_(avg_column), avg_name_(std::move(avg_name)), batch_capacity_(batch_capacity) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }

 private:
  struct Group {
    std::vector<std::optional<Value>> values;
    long double sum = 0;
    uint64_t count = 0;
  };

  Status Initialize();

  std::unique_ptr<QueryResultStream> input_;
  std::vector<uint32_t> group_columns_;
  uint32_t avg_column_;
  std::string avg_name_;
  uint32_t batch_capacity_;
  std::vector<std::string> group_names_;
  ResultTemporalMetadata temporal_metadata_;
  std::vector<Group> groups_;
  Status terminal_status_ = Status::OK();
  size_t next_group_ = 0;
  bool initialized_ = false;
};

class GroupedExtremaResultStream final : public QueryResultStream {
 public:
  GroupedExtremaResultStream(std::unique_ptr<QueryResultStream> input,
                             std::vector<uint32_t> group_columns, uint32_t value_column,
                             std::string value_name, bool minimum, uint32_t batch_capacity)
      : input_(std::move(input)), group_columns_(std::move(group_columns)),
        value_column_(value_column), value_name_(std::move(value_name)), minimum_(minimum),
        batch_capacity_(batch_capacity) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }

 private:
  struct Group {
    std::vector<std::optional<Value>> values;
    std::optional<Value> selected;
  };

  Status Initialize();

  std::unique_ptr<QueryResultStream> input_;
  std::vector<uint32_t> group_columns_;
  uint32_t value_column_;
  std::string value_name_;
  bool minimum_;
  uint32_t batch_capacity_;
  std::vector<std::string> group_names_;
  ResultTemporalMetadata temporal_metadata_;
  std::vector<Group> groups_;
  Status terminal_status_ = Status::OK();
  size_t next_group_ = 0;
  bool initialized_ = false;
};

class SumResultStream final : public QueryResultStream {
 public:
  SumResultStream(std::unique_ptr<QueryResultStream> input, std::string column_name)
      : input_(std::move(input)), column_name_(std::move(column_name)) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }

 private:
  std::unique_ptr<QueryResultStream> input_;
  std::string column_name_;
  Status terminal_status_ = Status::OK();
  bool emitted_ = false;
};

class AvgResultStream final : public QueryResultStream {
 public:
  AvgResultStream(std::unique_ptr<QueryResultStream> input, std::string column_name)
      : input_(std::move(input)), column_name_(std::move(column_name)) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }

 private:
  std::unique_ptr<QueryResultStream> input_;
  std::string column_name_;
  Status terminal_status_ = Status::OK();
  bool emitted_ = false;
};

class ExtremaResultStream final : public QueryResultStream {
 public:
  ExtremaResultStream(std::unique_ptr<QueryResultStream> input, std::string column_name,
                      bool minimum)
      : input_(std::move(input)), column_name_(std::move(column_name)), minimum_(minimum) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }

 private:
  std::unique_ptr<QueryResultStream> input_;
  std::string column_name_;
  bool minimum_;
  Status terminal_status_ = Status::OK();
  bool emitted_ = false;
};

enum class ResultAggregateKind : uint8_t { kCount, kSum, kAvg, kMin, kMax };

struct ResultAggregateSpec {
  ResultAggregateKind kind;
  uint32_t input_column;
  std::string column_name;
};

class MultiAggregateResultStream final : public QueryResultStream {
 public:
  MultiAggregateResultStream(std::unique_ptr<QueryResultStream> input,
                             std::vector<ResultAggregateSpec> aggregates)
      : input_(std::move(input)), aggregates_(std::move(aggregates)) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }

 private:
  struct State {
    uint64_t count = 0;
    int64_t sum = 0;
    bool has_sum = false;
    long double average_sum = 0;
    uint64_t average_count = 0;
    std::optional<Value> extrema;
  };

  Status Initialize();

  std::unique_ptr<QueryResultStream> input_;
  std::vector<ResultAggregateSpec> aggregates_;
  std::vector<State> states_;
  Status terminal_status_ = Status::OK();
  bool initialized_ = false;
  bool emitted_ = false;
};

class GroupedMultiAggregateResultStream final : public QueryResultStream {
 public:
  GroupedMultiAggregateResultStream(std::unique_ptr<QueryResultStream> input,
                                    std::vector<uint32_t> group_columns,
                                    std::vector<ResultAggregateSpec> aggregates,
                                    std::vector<ResultOutputSlot> output_slots,
                                    uint32_t batch_capacity,
                                    std::shared_ptr<QueryCancellation> cancellation = nullptr,
                                    std::shared_ptr<QueryMemoryAccount> memory_account = nullptr,
                                    std::string spill_directory = {},
                                    std::shared_ptr<ResourceGovernorExtension> spill_resources = nullptr);
  ~GroupedMultiAggregateResultStream() override;

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }
  QueryOperatorResourceStats operator_resource_stats() const override {
    return QueryOperatorResourceStats{
        memory_peak_bytes_, spill_bytes_, spilling_};
  }

 private:
  struct AggregateState {
    uint64_t count = 0;
    int64_t sum = 0;
    bool has_sum = false;
    long double average_sum = 0;
    uint64_t average_count = 0;
    std::optional<Value> extrema;
  };
  struct Group {
    std::vector<ResultValueCell> values;
    std::vector<AggregateState> aggregates;
  };

  Status Initialize();
  Status UpdateAggregate(AggregateState* state, const ResultAggregateSpec& aggregate,
                         const std::optional<Value>& value);
  StatusOr<std::optional<Value>> FinalizeAggregate(const AggregateState& state,
                                                    const ResultAggregateSpec& aggregate) const;
  Status MergeAggregateState(AggregateState* target,
                             const AggregateState& source,
                             const ResultAggregateSpec& aggregate);
  uint64_t GroupBytes(const Group& group) const;
  std::string GroupKey(const Group& group) const;
  StatusOr<std::string> EncodeGroup(const Group& group) const;
  StatusOr<Group> DecodeGroup(const std::string& record) const;
  Status AppendSpillGroup(const Group& group);
  Status SpillGroups();
  StatusOr<bool> LoadNextSpillPartition();
  void ReleaseGroups();

  std::unique_ptr<QueryResultStream> input_;
  std::vector<uint32_t> group_columns_;
  std::vector<ResultAggregateSpec> aggregates_;
  std::vector<ResultOutputSlot> output_slots_;
  uint32_t batch_capacity_;
  std::shared_ptr<QueryCancellation> cancellation_;
  std::shared_ptr<QueryMemoryAccount> memory_account_;
  std::string spill_directory_;
  std::shared_ptr<ResourceGovernorExtension> spill_resources_;
  uint64_t reserved_bytes_ = 0;
  uint64_t memory_peak_bytes_ = 0;
  uint64_t spill_bytes_ = 0;
  std::vector<std::string> group_names_;
  std::vector<ResultValueKind> group_kinds_;
  ResultTemporalMetadata temporal_metadata_;
  std::vector<Group> groups_;
  std::unique_ptr<PartitionedSpillSet> spill_;
  Status terminal_status_ = Status::OK();
  size_t next_group_ = 0;
  uint32_t next_spill_partition_ = 0;
  bool spilling_ = false;
  bool initialized_ = false;
};

}  // namespace cedar

#endif  // CEDAR_TCYPHER_RUNTIME_QUERY_RESULT_H_
