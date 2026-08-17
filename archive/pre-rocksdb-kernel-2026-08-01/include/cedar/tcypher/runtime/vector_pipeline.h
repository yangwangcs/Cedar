// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_VECTOR_PIPELINE_H_
#define CEDAR_TCYPHER_RUNTIME_VECTOR_PIPELINE_H_

#include <string>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "cedar/tcypher/runtime/cancellation.h"
#include "cedar/tcypher/runtime/query_result.h"
#include "cedar/tcypher/runtime/vector_filter.h"
#include "cedar/tcypher/runtime/vector_project.h"
#include "cedar/tcypher/storage/property_gather.h"
#include "cedar/tcypher/storage/temporal_scan.h"

namespace cedar {

using VectorBatchTransform =
    std::function<Status(ColumnBatch*)>;

class ScanFilterProjectResultStream final : public QueryResultStream {
 public:
  ScanFilterProjectResultStream(TemporalScanCursor scan, VectorPredicate predicate,
                                std::vector<VectorExpression> expressions,
                                std::vector<std::string> column_names,
                                ResultTemporalMetadata temporal_metadata,
                                std::shared_ptr<QueryCancellation> cancellation = nullptr,
                                VectorBatchTransform transform = nullptr)
      : scan_(std::move(scan)),
        predicate_(std::move(predicate)),
        expressions_(std::move(expressions)),
        column_names_(std::move(column_names)),
        temporal_metadata_(temporal_metadata),
        cancellation_(std::move(cancellation)),
        transform_(std::move(transform)) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }

 private:
  TemporalScanCursor scan_;
  VectorPredicate predicate_;
  std::vector<VectorExpression> expressions_;
  std::vector<std::string> column_names_;
  ResultTemporalMetadata temporal_metadata_;
  std::shared_ptr<QueryCancellation> cancellation_;
  VectorBatchTransform transform_;
  Status terminal_status_ = Status::OK();
  bool finished_ = false;
};

class ScanGatherProjectResultStream final : public QueryResultStream {
 public:
  ScanGatherProjectResultStream(TemporalScanCursor scan,
                                std::vector<TemporalEvent> property_candidates,
                                PropertyGatherSpec gather_spec,
                                std::vector<VectorExpression> expressions,
                                std::vector<std::string> column_names,
                                ResultTemporalMetadata temporal_metadata,
                                std::shared_ptr<QueryCancellation> cancellation = nullptr,
                                VectorPredicate predicate = nullptr,
                                VectorBatchTransform transform = nullptr)
      : scan_(std::move(scan)), property_candidates_(std::move(property_candidates)),
        gather_spec_(std::move(gather_spec)), expressions_(std::move(expressions)),
        column_names_(std::move(column_names)), temporal_metadata_(temporal_metadata),
        cancellation_(std::move(cancellation)), predicate_(std::move(predicate)),
        transform_(std::move(transform)) {}

  ScanGatherProjectResultStream(TemporalScanCursor scan,
                                PinnedTemporalScanSources property_sources,
                                TemporalScanSpec property_scan_spec,
                                PropertyGatherSpec gather_spec,
                                std::vector<VectorExpression> expressions,
                                std::vector<std::string> column_names,
                                ResultTemporalMetadata temporal_metadata,
                                std::shared_ptr<QueryCancellation> cancellation = nullptr,
                                VectorPredicate predicate = nullptr,
                                VectorBatchTransform transform = nullptr)
      : scan_(std::move(scan)), property_sources_(std::move(property_sources)),
        property_scan_spec_(std::move(property_scan_spec)),
        gather_spec_(std::move(gather_spec)), expressions_(std::move(expressions)),
        column_names_(std::move(column_names)), temporal_metadata_(temporal_metadata),
        cancellation_(std::move(cancellation)), predicate_(std::move(predicate)),
        transform_(std::move(transform)) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }

 private:
  TemporalScanCursor scan_;
  std::vector<TemporalEvent> property_candidates_;
  std::optional<PinnedTemporalScanSources> property_sources_;
  TemporalScanSpec property_scan_spec_;
  PropertyGatherSpec gather_spec_;
  std::vector<VectorExpression> expressions_;
  std::vector<std::string> column_names_;
  ResultTemporalMetadata temporal_metadata_;
  std::shared_ptr<QueryCancellation> cancellation_;
  VectorPredicate predicate_;
  VectorBatchTransform transform_;
  Status terminal_status_ = Status::OK();
  bool finished_ = false;
};

}  // namespace cedar

#endif  // CEDAR_TCYPHER_RUNTIME_VECTOR_PIPELINE_H_
