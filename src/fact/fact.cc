// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/fact/fact.h"

namespace cedar {
namespace {

bool IsStateFamily(FactFamily family) {
  return family == FactFamily::kVertexState ||
         family == FactFamily::kEdgeState ||
         family == FactFamily::kEdgeIdentity;
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

EntityFact EntityFact::Vertex(VertexRef vertex) {
  return EntityFact(FactRef(vertex.part_id, FactFamily::kVertexState,
                            PropertyId{}, vertex.vertex_id.value));
}

EntityFact EntityFact::Edge(EdgeRef edge) {
  return EntityFact(FactRef(edge.home_part_id, FactFamily::kEdgeState,
                            PropertyId{}, edge.edge_id.value));
}

PropertyFact PropertyFact::Vertex(VertexRef vertex, PropertyId property_id) {
  return PropertyFact(FactRef(vertex.part_id, FactFamily::kVertexProperty,
                              property_id, vertex.vertex_id.value));
}

PropertyFact PropertyFact::Edge(EdgeRef edge, PropertyId property_id) {
  return PropertyFact(FactRef(edge.home_part_id, FactFamily::kEdgeProperty,
                              property_id, edge.edge_id.value));
}

Status EdgeIdentity::Validate() const {
  if (!edge_id.valid() || !source_vertex_id.valid() ||
      !target_vertex_id.valid() || edge_type == 0 ||
      home_part_id != source_part_id) {
    return Status::InvalidArgument(
        "edge identity", "invalid edge home partition, endpoints, or type");
  }
  return Status::OK();
}

Status FactEvent::Validate() const {
  if (commit_seq.value == 0) {
    return Status::InvalidArgument("fact event", "zero commit sequence");
  }
  const Status mutation = ValidateMutation(ref, operation, schema_epoch, value);
  if (!mutation.ok()) return mutation;
  if (ref.family() == FactFamily::kEdgeIdentity) {
    if (valid_from.value != 0 || operation != FactOperation::kPut ||
        schema_epoch != 0 || value.has_value() || !edge_identity.has_value() ||
        edge_identity->edge_ref() !=
            EdgeRef{ref.part_id(), EdgeId{ref.entity_id()}}) {
      return Status::InvalidArgument(
          "edge identity fact", "identity facts require a valid_from=0 PUT payload");
    }
    return edge_identity->Validate();
  }
  if (edge_identity.has_value()) {
    return Status::InvalidArgument("fact event", "edge identity payload on non-identity fact");
  }
  return Status::OK();
}

Status PendingFactMutation::Validate() const {
  const Status status = ValidateMutation(ref, operation, schema_epoch, value);
  if (!status.ok()) return status;
  if (ref.family() == FactFamily::kEdgeIdentity) {
    return Status::InvalidArgument("fact mutation", "edge identity uses StoreCommitBatch identity payload");
  }
  return Status::OK();
}

}  // namespace cedar
