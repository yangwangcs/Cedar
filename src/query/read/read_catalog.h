#ifndef CEDAR_QUERY_READ_READ_CATALOG_H_
#define CEDAR_QUERY_READ_READ_CATALOG_H_

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/schema.h"
#include "cedar/schema_provider.h"
#include "cedar/query/query.h"
#include "cedar/query/types.h"
#include "query/projection/projection_manifest.h"

namespace cedar::internal {

struct ReadCatalogKey {
  ProjectionKind kind = ProjectionKind::kState;
  PartId part_id;
  std::optional<PropertyId> property_id;
  uint32_t schema_epoch = 0;
  bool operator==(const ReadCatalogKey&) const = default;
};

enum class PropertyIndexOperator : uint8_t {
  kEqual, kLess, kLessEqual, kGreater, kGreaterEqual
};

struct PropertyIndexPosting {
  VertexRef vertex;
  ValidTimeInterval effective;
  CommitSeq commit_seq;
  Value value;
};

class ReadCatalog final : public SchemaProvider {
 public:
  std::string Fingerprint() const override;
  StatusOr<std::optional<PropertyDefinition>> LookupProperty(PropertyId) const;
  StatusOr<std::optional<PropertyDefinition>> LookupPropertyByName(
      std::string_view, PropertyEntityKind) const;
  StatusOr<std::optional<PropertyDefinition>> Lookup(
      PropertyId property_id) const override { return LookupProperty(property_id); }
  StatusOr<std::optional<PropertyDefinition>> LookupByName(
      std::string_view name,
      std::optional<PropertyEntityKind> kind = {}) const override;
  const std::vector<EdgeIdentity>* SeekAdjacency(
      const VertexRef&, ExpandDirection direction = ExpandDirection::kOut,
      std::optional<uint64_t> edge_type = std::nullopt) const;
  const std::vector<PropertyIndexPosting>* SeekPropertyIndex(
      PropertyId property, PropertyIndexOperator op, const Value& lower,
      const std::optional<Value>& upper = std::nullopt) const;
  // Returns the immutable posting slice matching the typed predicate. The
  // result is copied so callers can safely retain it across catalog reads;
  // lookup itself uses binary-search bounds over the sorted posting vector.
  std::vector<PropertyIndexPosting> SeekPropertyIndexRange(
      PropertyId property, PropertyIndexOperator op, const Value& lower,
      const std::optional<Value>& upper = std::nullopt) const;
  const CoverageRegion* FindCoverage(const ReadCatalogKey&, uint64_t entity_min,
                                     uint64_t entity_max_exclusive,
                                     const ValidTimeInterval&) const;
  std::vector<const CoverageRegion*> FindCoverageCandidates(
      const ReadCatalogKey&, uint64_t entity_min, uint64_t entity_max_exclusive,
      const ValidTimeInterval&) const;
  uint64_t total_segment_count() const { return total_segment_count_; }

 private:
  struct NameKey {
    std::string name;
    PropertyEntityKind kind = PropertyEntityKind::kVertex;
    bool operator==(const NameKey&) const = default;
  };
  struct NameHash {
    size_t operator()(const NameKey& key) const noexcept {
      return std::hash<std::string>{}(key.name) ^
             (static_cast<size_t>(key.kind) + (key.name.size() << 6));
    }
  };
  struct VertexHash {
    size_t operator()(const VertexRef& value) const noexcept {
      return std::hash<uint64_t>{}(value.vertex_id.value) ^
             (static_cast<size_t>(value.part_id.value) << 1);
    }
  };
  struct AdjacencyKey {
    VertexRef vertex;
    ExpandDirection direction = ExpandDirection::kOut;
    std::optional<uint64_t> edge_type;
    bool operator==(const AdjacencyKey&) const = default;
  };
  struct AdjacencyHash {
    size_t operator()(const AdjacencyKey& key) const noexcept {
      size_t hash = VertexHash{}(key.vertex);
      hash ^= static_cast<size_t>(key.direction) + (hash << 6) + (hash >> 2);
      if (key.edge_type.has_value()) {
        hash ^= std::hash<uint64_t>{}(*key.edge_type) + (hash << 6) + (hash >> 2);
      }
      return hash;
    }
  };
  friend class ReadCatalogBuilder;
  std::unordered_map<uint16_t, PropertyDefinition> properties_by_id_;
  std::unordered_map<NameKey, PropertyDefinition, NameHash> properties_by_name_;
  std::unordered_map<AdjacencyKey, std::vector<EdgeIdentity>, AdjacencyHash> adjacency_;
  std::unordered_map<uint16_t, std::vector<PropertyIndexPosting>> property_indexes_;
  std::vector<std::pair<ReadCatalogKey, CoverageRegion>> coverage_;
  uint64_t total_segment_count_ = 0;
};

class ReadCatalogBuilder {
 public:
  Status AddSchema(PropertyDefinition definition);
  Status AddAdjacency(const VertexRef&, EdgeIdentity,
                      ExpandDirection direction = ExpandDirection::kOut,
                      std::optional<uint64_t> edge_type = std::nullopt);
  Status AddPropertyIndex(PropertyId property, PropertyIndexPosting posting);
  Status AddCoverage(ReadCatalogKey, CoverageRegion);
  StatusOr<std::shared_ptr<const ReadCatalog>> Finish() &&;

 private:
  ReadCatalog catalog_;
};

}  // namespace cedar::internal

#endif
