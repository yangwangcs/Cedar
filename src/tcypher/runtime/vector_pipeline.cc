// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/runtime/vector_pipeline.h"

namespace cedar {

Status ScanFilterProjectResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("vector pipeline", "missing result output");
  if (finished_) return Status::NotFound("vector pipeline", "end of stream");
  if (!terminal_status_.ok()) return terminal_status_;

  const auto check_cancelled = [this]() -> Status {
    if (cancellation_ && cancellation_->IsCancelled()) {
      terminal_status_ = Status::QueryCancelled("vector pipeline", "query cancelled");
      finished_ = true;
      return terminal_status_;
    }
    return Status::OK();
  };
  const Status before_scan = check_cancelled();
  if (!before_scan.ok()) return before_scan;

  while (true) {
    ColumnBatch input;
    const Status scanned = scan_.NextMorsel(&input);
    if (scanned.IsNotFound()) {
      finished_ = true;
      return scanned;
    }
    if (!scanned.ok()) {
      terminal_status_ = scanned;
      return scanned;
    }
    const Status after_scan = check_cancelled();
    if (!after_scan.ok()) return after_scan;

    if (transform_) {
      const Status transformed_status = transform_(&input);
      if (!transformed_status.ok()) {
        terminal_status_ = transformed_status;
        return transformed_status;
      }
    }

    ColumnBatch filtered;
    const Status filtered_status = FilterColumnBatch(input, predicate_, &filtered);
    if (!filtered_status.ok()) {
      terminal_status_ = filtered_status;
      return filtered_status;
    }
    const Status after_filter = check_cancelled();
    if (!after_filter.ok()) return after_filter;
    if (filtered.row_count() == 0) continue;

    ColumnBatch projected;
    const Status projected_status = ProjectColumnBatch(filtered, expressions_, &projected);
    if (!projected_status.ok()) {
      terminal_status_ = projected_status;
      return projected_status;
    }
    const Status after_project = check_cancelled();
    if (!after_project.ok()) return after_project;
    ResultBatch result(column_names_, std::move(projected), temporal_metadata_);
    const Status valid = result.Validate();
    if (!valid.ok()) {
      terminal_status_ = valid;
      return valid;
    }
    *batch = std::move(result);
    return Status::OK();
  }
}

Status ScanGatherProjectResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("vector pipeline", "missing result output");
  if (finished_) return Status::NotFound("vector pipeline", "end of stream");
  if (!terminal_status_.ok()) return terminal_status_;
  if (cancellation_ && cancellation_->IsCancelled()) {
    terminal_status_ = Status::QueryCancelled("vector pipeline", "query cancelled");
    finished_ = true;
    return terminal_status_;
  }
  while (true) {
    ColumnBatch input;
    const Status scanned = scan_.NextMorsel(&input);
    if (scanned.IsNotFound()) {
      finished_ = true;
      return scanned;
    }
    if (!scanned.ok()) {
      terminal_status_ = scanned;
      return scanned;
    }
    if (cancellation_ && cancellation_->IsCancelled()) {
      terminal_status_ = Status::QueryCancelled("vector pipeline", "query cancelled");
      finished_ = true;
      return terminal_status_;
    }
    if (transform_) {
      const Status transformed_status = transform_(&input);
      if (!transformed_status.ok()) {
        terminal_status_ = transformed_status;
        return transformed_status;
      }
    }
    ColumnBatch gathered;
    const Status gathered_status = property_sources_.has_value()
        ? BatchGatherProperties(input, *property_sources_, property_scan_spec_,
                                gather_spec_, &gathered)
        : BatchGatherProperties(input, property_candidates_, gather_spec_, &gathered);
    if (!gathered_status.ok()) {
      terminal_status_ = gathered_status;
      return gathered_status;
    }
    ColumnBatch filtered;
    const Status filtered_status = FilterColumnBatch(
        gathered, predicate_ ? predicate_ : [](const ColumnBatch&, uint32_t) { return true; },
        &filtered);
    if (!filtered_status.ok()) {
      terminal_status_ = filtered_status;
      return filtered_status;
    }
    if (filtered.row_count() == 0) continue;
    ColumnBatch projected;
    const Status projected_status = ProjectColumnBatch(filtered, expressions_, &projected);
    if (!projected_status.ok()) {
      terminal_status_ = projected_status;
      return projected_status;
    }
    ResultBatch result(column_names_, std::move(projected), temporal_metadata_);
    const Status valid = result.Validate();
    if (!valid.ok()) {
      terminal_status_ = valid;
      return valid;
    }
    *batch = std::move(result);
    return Status::OK();
  }
}

}  // namespace cedar
