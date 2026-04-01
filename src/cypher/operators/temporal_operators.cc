// Copyright (c) 2024 CedarGraph Project
// Licensed under the MIT License.

#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/value.h"

namespace cedar {
namespace cypher {

// ============================================================================
// TemporalNodeScan
// ============================================================================

TemporalNodeScan::TemporalNodeScan(std::string variable,
                                   std::optional<std::string> label,
                                   TemporalModifierType modifier,
                                   std::optional<TimestampExpression> start_time,
                                   std::optional<TimestampExpression> end_time,
                                   std::optional<uint64_t> version_number)
    : variable_(variable),
      label_(label),
      modifier_(modifier),
      start_time_(start_time),
      end_time_(end_time),
      version_number_(version_number),
      current_index_(0),
      query_start_(0),
      query_end_(Timestamp::Max()) {}

bool TemporalNodeScan::Init(ExecutionContext* ctx) {
  context_ = ctx;
  return true;
}

std::shared_ptr<Record> TemporalNodeScan::Next() {
  return nullptr;
}

std::string TemporalNodeScan::GetDetails() const {
  return variable_;
}

bool TemporalNodeScan::MatchesTemporalConstraint(const Node& node) const {
  return true;
}

bool TemporalNodeScan::MatchesAsOf(const Node& node) const {
  return true;
}

bool TemporalNodeScan::MatchesBetween(const Node& node) const {
  return true;
}

bool TemporalNodeScan::MatchesContainedIn(const Node& node) const {
  return true;
}

bool TemporalNodeScan::MatchesVersion(const Node& node) const {
  return true;
}

// ============================================================================
// TemporalExpand
// ============================================================================

TemporalExpand::TemporalExpand(std::string from_variable,
                               std::string rel_variable,
                               std::string to_variable,
                               Direction direction,
                               TemporalModifierType modifier,
                               std::optional<std::string> rel_type,
                               TemporalPathSemantics path_semantics)
    : from_variable_(from_variable),
      rel_variable_(rel_variable),
      to_variable_(to_variable),
      direction_(direction),
      modifier_(modifier),
      rel_type_(rel_type),
      path_semantics_(path_semantics),
      neighbor_index_(0),
      query_start_(0),
      query_end_(Timestamp::Max()) {}

bool TemporalExpand::Init(ExecutionContext* ctx) {
  context_ = ctx;
  return true;
}

std::shared_ptr<Record> TemporalExpand::Next() {
  return nullptr;
}

void TemporalExpand::Reset() {}

std::string TemporalExpand::GetDetails() const {
  return from_variable_;
}

bool TemporalExpand::MatchesTemporalConstraint(const Relationship& rel) const {
  return true;
}

bool TemporalExpand::IsPathContinuous(const std::vector<Relationship>& path) const {
  return true;
}

// ============================================================================
// SnapshotScan
// ============================================================================

SnapshotScan::SnapshotScan(std::string variable,
                           std::optional<std::string> label,
                           Timestamp snapshot_time)
    : variable_(variable),
      label_(label),
      snapshot_time_(snapshot_time),
      current_index_(0) {}

bool SnapshotScan::Init(ExecutionContext* ctx) {
  context_ = ctx;
  return true;
}

std::shared_ptr<Record> SnapshotScan::Next() {
  return nullptr;
}

std::string SnapshotScan::GetDetails() const {
  return variable_;
}

// ============================================================================
// VersionScan
// ============================================================================

VersionScan::VersionScan(std::string variable,
                         std::optional<std::string> label,
                         std::optional<uint64_t> specific_version)
    : variable_(variable),
      label_(label),
      specific_version_(specific_version),
      current_node_index_(0),
      current_version_index_(0) {}

bool VersionScan::Init(ExecutionContext* ctx) {
  context_ = ctx;
  return true;
}

std::shared_ptr<Record> VersionScan::Next() {
  return nullptr;
}

std::string VersionScan::GetDetails() const {
  return variable_;
}

}  // namespace cypher
}  // namespace cedar
