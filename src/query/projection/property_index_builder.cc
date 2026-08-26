#include "query/projection/property_index_builder.h"

#include <algorithm>
#include <map>
#include <type_traits>

#include "cedar/core/crc32c.h"
#include "query/projection/property_index.h"

namespace cedar::internal {
namespace {

bool PostingLess(const PropertyIndexPosting& left,
                 const PropertyIndexPosting& right) {
  const int value_order = std::visit(
      [](const auto& a, const auto& b) -> int {
        using A = std::decay_t<decltype(a)>;
        using B = std::decay_t<decltype(b)>;
        if constexpr (!std::is_same_v<A, B>) {
          return 0;
        } else {
          return a < b ? -1 : (a > b ? 1 : 0);
        }
      },
      left.value.data(), right.value.data());
  if (value_order != 0) return value_order < 0;
  if (left.vertex.part_id.value != right.vertex.part_id.value) {
    return left.vertex.part_id.value < right.vertex.part_id.value;
  }
  if (left.vertex.vertex_id.value != right.vertex.vertex_id.value) {
    return left.vertex.vertex_id.value < right.vertex.vertex_id.value;
  }
  return left.effective.from.value < right.effective.from.value;
}

}  // namespace

StatusOr<ProjectionBuild> BuildPropertyIndexProjection(
    const CanonicalFactReader& reader,
    const std::vector<PropertyDefinition>& definitions,
    PartScope part_scope, ValidTimeInterval valid_time,
    CommitSeq base_seq, CommitSeq built_through, uint64_t generation_id,
    std::string database_identity) {
  if (generation_id == 0 || base_seq.value > built_through.value) {
    return Status::InvalidArgument("property index builder", "invalid generation watermark");
  }
  if (!part_scope.Validate().ok() || !valid_time.Validate().ok()) {
    return Status::InvalidArgument("property index builder", "invalid scope");
  }
  ProjectionBuild build;
  build.manifest.database_identity = std::move(database_identity);
  build.manifest.generation_id = generation_id;
  build.manifest.base_seq = base_seq;
  for (const auto& definition : definitions) {
    const Status valid = definition.Validate();
    if (!valid.ok()) return valid;
    if (definition.entity_kind != PropertyEntityKind::kVertex) continue;
    FactReadSpec facts;
    facts.part_scope = part_scope;
    facts.family = FactFamily::kVertexProperty;
    facts.property_id = definition.property_id;
    facts.commit_seq_max = built_through;
    facts.batch_row_limit = 1024;
    CanonicalStateReadSpec state_spec{facts, valid_time.from, built_through,
                                      std::nullopt};
    std::map<uint32_t, PropertyIndexSegment> by_part;
    const Status read = reader.ReadStateRows(
        state_spec, [&](const std::vector<CanonicalStateRow>& rows) {
          for (const auto& row : rows) {
            if (!row.value.has_value()) continue;
            auto& segment = by_part[row.ref.part_id().value];
            segment.generation_id = generation_id;
            segment.base_seq = base_seq;
            segment.built_through = built_through;
            segment.property = definition.property_id;
            segment.part_id = row.ref.part_id();
            segment.schema_epoch = definition.schema_epoch;
            segment.postings.push_back(
                {VertexRef{row.ref.part_id(), VertexId{row.ref.entity_id()}},
                 row.effective, row.commit_seq, *row.value});
          }
          return Status::OK();
        });
    if (!read.ok()) return read;
    for (auto& [part, segment] : by_part) {
      if (segment.postings.empty()) continue;
      std::sort(segment.postings.begin(), segment.postings.end(), PostingLess);
      auto encoded = EncodePropertyIndexSegment(segment);
      if (!encoded.ok()) return encoded.status();
      CoverageRegion region;
      region.kind = ProjectionKind::kPropertyIndex;
      region.part_id = PartId{part};
      region.property_id = definition.property_id;
      region.schema_epoch = definition.schema_epoch;
      region.entity_min = segment.postings.front().vertex.vertex_id.value;
      region.entity_max_exclusive =
          segment.postings.front().vertex.vertex_id.value == UINT64_MAX
              ? UINT64_MAX
              : segment.postings.front().vertex.vertex_id.value + 1;
      for (const auto& posting : segment.postings) {
        region.entity_min = std::min(region.entity_min, posting.vertex.vertex_id.value);
        if (posting.vertex.vertex_id.value != UINT64_MAX) {
          region.entity_max_exclusive = std::max(
              region.entity_max_exclusive, posting.vertex.vertex_id.value + 1);
        } else {
          region.entity_max_exclusive = UINT64_MAX;
        }
      }
      if (region.entity_min == UINT64_MAX) region.entity_min = 0;
      region.valid_time = valid_time;
      region.built_through = built_through;
      SegmentDescriptor descriptor;
      descriptor.segment_id = "property-" + std::to_string(definition.property_id.value) +
                              "-part-" + std::to_string(part) +
                              "-generation-" + std::to_string(generation_id);
      descriptor.filename = descriptor.segment_id + ".cpi";
      descriptor.header.kind = ProjectionKind::kPropertyIndex;
      descriptor.header.generation_id = generation_id;
      descriptor.header.base_seq = base_seq;
      descriptor.header.part_id = PartId{part};
      descriptor.header.property_id = definition.property_id;
      descriptor.header.schema_epoch = definition.schema_epoch;
      descriptor.header.entity_min = region.entity_min;
      descriptor.header.entity_max_exclusive = region.entity_max_exclusive;
      descriptor.header.valid_from_min = valid_time.from;
      descriptor.file_bytes = encoded.ValueOrDie().size();
      descriptor.checksum = crc32c::Value(encoded.ValueOrDie().data(),
                                          encoded.ValueOrDie().size());
      region.segments.push_back(descriptor);
      build.segments.push_back({descriptor, encoded.ValueOrDie()});
      build.manifest.regions.push_back(std::move(region));
    }
  }
  return build;
}

}  // namespace cedar::internal
