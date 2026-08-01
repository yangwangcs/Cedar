// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/fact/fact.h"

namespace cedar {
namespace {

bool IsStateFamily(FactFamily family) {
  return family == FactFamily::kVertexState ||
         family == FactFamily::kEdgeState;
}

bool IsKnownFamily(FactFamily family) {
  return IsStateFamily(family) || family == FactFamily::kVertexProperty ||
         family == FactFamily::kEdgeProperty;
}

bool IsKnownOperation(FactOperation operation) {
  return operation == FactOperation::kPut ||
         operation == FactOperation::kDelete;
}

Status ValidateMutation(const FactRef& ref, FactOperation operation,
                        uint32_t schema_epoch,
                        const std::optional<Value>& value) {
  const Status ref_status = ref.Validate();
  if (!ref_status.ok()) return ref_status;
  if (!IsKnownOperation(operation)) {
    return Status::InvalidArgument("fact mutation", "unknown operation");
  }
  if (!ref.IsProperty()) {
    if (schema_epoch != 0 || value.has_value()) {
      return Status::InvalidArgument(
          "entity fact", "state facts cannot carry schema or value");
    }
    return Status::OK();
  }
  if (schema_epoch == 0) {
    return Status::InvalidArgument("property fact", "missing schema epoch");
  }
  if (operation == FactOperation::kPut && !value.has_value()) {
    return Status::InvalidArgument("property fact", "PUT requires a value");
  }
  if (operation == FactOperation::kDelete && value.has_value()) {
    return Status::InvalidArgument("property fact", "DELETE cannot carry a value");
  }
  return Status::OK();
}

}  // namespace

bool FactRef::IsProperty() const { return !IsStateFamily(family_); }

Status FactRef::Validate() const {
  if (!IsKnownFamily(family_)) {
    return Status::InvalidArgument("fact reference", "unknown fact family");
  }
  if (entity_id_ == 0) {
    return Status::InvalidArgument("fact reference", "zero entity ID");
  }
  if (IsStateFamily(family_) && property_id_.value != 0) {
    return Status::InvalidArgument("fact reference",
                                   "state fact has property ID");
  }
  if (!IsStateFamily(family_) && !property_id_.valid()) {
    return Status::InvalidArgument("fact reference",
                                   "property fact has zero property ID");
  }
  return Status::OK();
}

EntityFact EntityFact::Vertex(VertexId vertex_id) {
  return EntityFact(
      FactRef(FactFamily::kVertexState, PropertyId{}, vertex_id.value));
}

EntityFact EntityFact::Edge(EdgeId edge_id) {
  return EntityFact(FactRef(FactFamily::kEdgeState, PropertyId{}, edge_id.value));
}

PropertyFact PropertyFact::Vertex(VertexId vertex_id, PropertyId property_id) {
  return PropertyFact(FactRef(FactFamily::kVertexProperty, property_id,
                              vertex_id.value));
}

PropertyFact PropertyFact::Edge(EdgeId edge_id, PropertyId property_id) {
  return PropertyFact(
      FactRef(FactFamily::kEdgeProperty, property_id, edge_id.value));
}

Status EdgeIdentity::Validate() const {
  if (!edge_id.valid() || !source_vertex_id.valid() ||
      !target_vertex_id.valid() || edge_type == 0) {
    return Status::InvalidArgument(
        "edge identity", "edge, endpoints, and type must be nonzero");
  }
  return Status::OK();
}

Status FactEvent::Validate() const {
  if (commit_seq.value == 0) {
    return Status::InvalidArgument("fact event", "zero commit sequence");
  }
  return ValidateMutation(ref, operation, schema_epoch, value);
}

Status PendingFactMutation::Validate() const {
  return ValidateMutation(ref, operation, schema_epoch, value);
}

}  // namespace cedar
