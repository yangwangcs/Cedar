#include "query/read/read_catalog.h"

#include <algorithm>
#include <cstring>
#include <type_traits>

namespace cedar::internal {

namespace {

// Values in a posting list are schema-typed. Comparing only equal physical
// types keeps an index lookup from accidentally applying numeric coercions
// that the canonical expression evaluator would reject.
int CompareValues(const Value& left, const Value& right) {
  if (left.type() != right.type()) return 0;
  return std::visit(
      [](const auto& a, const auto& b) -> int {
        using A = std::decay_t<decltype(a)>;
        using B = std::decay_t<decltype(b)>;
        if constexpr (!std::is_same_v<A, B>) {
          return 0;
        } else if constexpr (std::is_same_v<A, std::string>) {
          return a < b ? -1 : (a > b ? 1 : 0);
        } else {
          return a < b ? -1 : (a > b ? 1 : 0);
        }
      },
      left.data(), right.data());
}

bool Matches(PropertyIndexOperator op, int cmp, bool has_upper,
             int upper_cmp) {
  switch (op) {
    case PropertyIndexOperator::kEqual:
      return cmp == 0;
    case PropertyIndexOperator::kLess:
      return cmp < 0;
    case PropertyIndexOperator::kLessEqual:
      return cmp <= 0;
    case PropertyIndexOperator::kGreater:
      return cmp > 0;
    case PropertyIndexOperator::kGreaterEqual:
      return cmp >= 0;
  }
  (void)has_upper;
  (void)upper_cmp;
  return false;
}

}  // namespace

std::string ReadCatalog::Fingerprint() const {
  std::vector<PropertyDefinition> properties;
  properties.reserve(properties_by_id_.size());
  for (const auto& [id, definition] : properties_by_id_) {
    (void)id;
    properties.push_back(definition);
  }
  std::sort(properties.begin(), properties.end(),
            [](const PropertyDefinition& left, const PropertyDefinition& right) {
              return left.property_id.value < right.property_id.value;
            });
  uint64_t hash = 1469598103934665603ULL;
  auto mix = [&hash](std::string_view value) {
    for (unsigned char byte : value) {
      hash ^= byte;
      hash *= 1099511628211ULL;
    }
  };
  for (const auto& property : properties) {
    mix(std::to_string(property.property_id.value));
    mix(std::to_string(property.schema_epoch));
    mix(property.name);
    mix(std::to_string(static_cast<unsigned>(property.entity_kind)));
    mix(std::to_string(static_cast<unsigned>(property.physical_type)));
  }
  return std::to_string(hash);
}

StatusOr<std::optional<PropertyDefinition>> ReadCatalog::LookupProperty(
    PropertyId id) const {
  const auto found = properties_by_id_.find(id.value);
  if (found == properties_by_id_.end()) return std::optional<PropertyDefinition>{};
  return std::optional<PropertyDefinition>{found->second};
}

StatusOr<std::optional<PropertyDefinition>> ReadCatalog::LookupPropertyByName(
    std::string_view name, PropertyEntityKind kind) const {
  const auto found = properties_by_name_.find(NameKey{std::string(name), kind});
  if (found == properties_by_name_.end()) return std::optional<PropertyDefinition>{};
  return std::optional<PropertyDefinition>{found->second};
}

StatusOr<std::optional<PropertyDefinition>> ReadCatalog::LookupByName(
    std::string_view name, std::optional<PropertyEntityKind> kind) const {
  if (kind.has_value()) return LookupPropertyByName(name, *kind);
  auto vertex = LookupPropertyByName(name, PropertyEntityKind::kVertex);
  if (!vertex.ok() || vertex.ValueOrDie().has_value()) return vertex;
  return LookupPropertyByName(name, PropertyEntityKind::kEdge);
}

const std::vector<EdgeIdentity>* ReadCatalog::SeekAdjacency(
    const VertexRef& vertex, ExpandDirection direction,
    std::optional<uint64_t> edge_type) const {
  const auto found = adjacency_.find(AdjacencyKey{vertex, direction, edge_type});
  return found == adjacency_.end() ? nullptr : &found->second;
}

const std::vector<PropertyIndexPosting>* ReadCatalog::SeekPropertyIndex(
    PropertyId property, PropertyIndexOperator op, const Value& lower,
    const std::optional<Value>& upper) const {
  const auto found = property_indexes_.find(property.value);
  if (found == property_indexes_.end()) return nullptr;
  // The catalog owns immutable postings. Filter into a stable scratch-free
  // view only when the builder has already materialized the requested range.
  // Callers use this capability as a coverage probe; execution performs the
  // typed comparison against each posting before canonical validation.
  (void)op;
  (void)lower;
  (void)upper;
  return &found->second;
}

std::vector<PropertyIndexPosting> ReadCatalog::SeekPropertyIndexRange(
    PropertyId property, PropertyIndexOperator op, const Value& lower,
    const std::optional<Value>& upper) const {
  const auto found = property_indexes_.find(property.value);
  if (found == property_indexes_.end()) return {};
  const auto& postings = found->second;
  // The builder sorts by schema-typed value order. Use lower_bound to avoid
  // scanning values below the requested bound, then validate the typed
  // comparison for exact semantics.
  const bool lower_bounded = op == PropertyIndexOperator::kEqual ||
                             op == PropertyIndexOperator::kGreater ||
                             op == PropertyIndexOperator::kGreaterEqual;
  const auto first = lower_bounded
                         ? std::lower_bound(
                               postings.begin(), postings.end(), lower,
                               [](const PropertyIndexPosting& posting,
                                  const Value& value) {
                                 return CompareValues(posting.value, value) < 0;
                               })
                         : postings.begin();
  std::vector<PropertyIndexPosting> result;
  for (auto it = first; it != postings.end(); ++it) {
    const int cmp = CompareValues(it->value, lower);
    if (it->value.type() != lower.type()) continue;
    if (upper.has_value() && CompareValues(it->value, *upper) > 0) break;
    if (Matches(op, cmp, upper.has_value(),
                 upper.has_value() ? CompareValues(it->value, *upper) : 0)) {
      result.push_back(*it);
    }
  }
  return result;
}

const CoverageRegion* ReadCatalog::FindCoverage(
    const ReadCatalogKey& key, uint64_t entity_min, uint64_t entity_max_exclusive,
    const ValidTimeInterval& interval) const {
  const auto first = std::lower_bound(
      coverage_.begin(), coverage_.end(), key,
      [](const auto& entry, const ReadCatalogKey& target) {
        if (entry.first.kind != target.kind) return entry.first.kind < target.kind;
        if (entry.first.part_id.value != target.part_id.value)
          return entry.first.part_id.value < target.part_id.value;
        if (entry.first.property_id.has_value() != target.property_id.has_value())
          return entry.first.property_id.has_value() < target.property_id.has_value();
        if (entry.first.property_id &&
            entry.first.property_id->value != target.property_id->value)
          return entry.first.property_id->value < target.property_id->value;
        return entry.first.schema_epoch < target.schema_epoch;
      });
  for (auto current = first; current != coverage_.end() && current->first == key; ++current) {
    const auto& entry = *current;
    const CoverageRegion& region = entry.second;
    const uint64_t region_to = region.valid_time.to ? region.valid_time.to->value : UINT64_MAX;
    const uint64_t request_to = interval.to ? interval.to->value : UINT64_MAX;
    if (region.entity_min <= entity_min && region.entity_max_exclusive >= entity_max_exclusive &&
        region.valid_time.from.value <= interval.from.value && region_to >= request_to)
      return &region;
  }
  return nullptr;
}

std::vector<const CoverageRegion*> ReadCatalog::FindCoverageCandidates(
    const ReadCatalogKey& key, uint64_t entity_min,
    uint64_t entity_max_exclusive, const ValidTimeInterval& interval) const {
  std::vector<const CoverageRegion*> result;
  const auto first = std::lower_bound(
      coverage_.begin(), coverage_.end(), key,
      [](const auto& entry, const ReadCatalogKey& target) {
        if (entry.first.kind != target.kind) return entry.first.kind < target.kind;
        if (entry.first.part_id.value != target.part_id.value)
          return entry.first.part_id.value < target.part_id.value;
        if (entry.first.property_id.has_value() != target.property_id.has_value())
          return entry.first.property_id.has_value() < target.property_id.has_value();
        if (entry.first.property_id &&
            entry.first.property_id->value != target.property_id->value)
          return entry.first.property_id->value < target.property_id->value;
        return entry.first.schema_epoch < target.schema_epoch;
      });
  (void)entity_min;
  (void)entity_max_exclusive;
  (void)interval;
  for (auto current = first; current != coverage_.end() && current->first == key;
       ++current) {
    result.push_back(&current->second);
  }
  return result;
}

Status ReadCatalogBuilder::AddSchema(PropertyDefinition definition) {
  const Status valid = definition.Validate();
  if (!valid.ok()) return valid;
  const auto id = catalog_.properties_by_id_.find(definition.property_id.value);
  if (id != catalog_.properties_by_id_.end() && id->second != definition)
    return Status::SchemaMismatch("read catalog", "property ID has conflicting definition");
  const ReadCatalog::NameKey name{definition.name, definition.entity_kind};
  const auto by_name = catalog_.properties_by_name_.find(name);
  if (by_name != catalog_.properties_by_name_.end() && by_name->second != definition)
    return Status::SchemaMismatch("read catalog", "property name has conflicting definition");
  catalog_.properties_by_id_[definition.property_id.value] = definition;
  catalog_.properties_by_name_[name] = std::move(definition);
  return Status::OK();
}

Status ReadCatalogBuilder::AddAdjacency(const VertexRef& vertex, EdgeIdentity edge,
                                        ExpandDirection direction,
                                        std::optional<uint64_t> edge_type) {
  if (!vertex.valid()) return Status::InvalidArgument("read catalog", "invalid adjacency vertex");
  const Status valid = edge.Validate();
  if (!valid.ok()) return valid;
  if (edge_type.has_value() && edge.edge_type != *edge_type) {
    return Status::InvalidArgument("read catalog", "adjacency edge type mismatch");
  }
  catalog_.adjacency_[ReadCatalog::AdjacencyKey{vertex, direction, edge_type}]
      .push_back(std::move(edge));
  return Status::OK();
}

Status ReadCatalogBuilder::AddPropertyIndex(PropertyId property,
                                            PropertyIndexPosting posting) {
  if (!property.valid() || !posting.vertex.valid()) {
    return Status::InvalidArgument("read catalog", "invalid property index posting");
  }
  if (!posting.effective.Validate().ok()) {
    return Status::InvalidArgument("read catalog", "invalid property index interval");
  }
  catalog_.property_indexes_[property.value].push_back(std::move(posting));
  return Status::OK();
}

Status ReadCatalogBuilder::AddCoverage(ReadCatalogKey key, CoverageRegion region) {
  if (region.entity_max_exclusive <= region.entity_min || !region.valid_time.Validate().ok())
    return Status::InvalidArgument("read catalog", "invalid coverage range");
  catalog_.total_segment_count_ += region.segments.size();
  catalog_.coverage_.emplace_back(std::move(key), std::move(region));
  return Status::OK();
}

StatusOr<std::shared_ptr<const ReadCatalog>> ReadCatalogBuilder::Finish() && {
  for (auto& [key, edges] : catalog_.adjacency_) {
    (void)key;
    std::sort(edges.begin(), edges.end(), [](const EdgeIdentity& left, const EdgeIdentity& right) {
      return left.edge_id.value < right.edge_id.value;
    });
  }
  for (auto& [property, postings] : catalog_.property_indexes_) {
    (void)property;
    std::sort(postings.begin(), postings.end(), [](const auto& left, const auto& right) {
      const int value_order = CompareValues(left.value, right.value);
      if (value_order != 0) return value_order < 0;
      if (left.vertex.part_id.value != right.vertex.part_id.value)
        return left.vertex.part_id.value < right.vertex.part_id.value;
      return left.vertex.vertex_id.value < right.vertex.vertex_id.value;
    });
  }
  std::sort(catalog_.coverage_.begin(), catalog_.coverage_.end(),
            [](const auto& left, const auto& right) {
              if (left.first.kind != right.first.kind) return left.first.kind < right.first.kind;
              if (left.first.part_id.value != right.first.part_id.value)
                return left.first.part_id.value < right.first.part_id.value;
              if (left.first.property_id.has_value() != right.first.property_id.has_value())
                return left.first.property_id.has_value() < right.first.property_id.has_value();
              if (left.first.property_id &&
                  left.first.property_id->value != right.first.property_id->value)
                return left.first.property_id->value < right.first.property_id->value;
              if (left.first.schema_epoch != right.first.schema_epoch)
                return left.first.schema_epoch < right.first.schema_epoch;
              return left.second.entity_min < right.second.entity_min;
            });
  return std::shared_ptr<const ReadCatalog>(
      new ReadCatalog(std::move(catalog_)));
}

}  // namespace cedar::internal
