#include "query/projection/property_index.h"

#include <algorithm>
#include <cstring>

#include "cedar/core/crc32c.h"

namespace cedar::internal {
namespace {

void Put32(std::string* out, uint32_t value) {
  for (int i = 0; i < 4; ++i) out->push_back(static_cast<char>(value >> (i * 8)));
}
void Put64(std::string* out, uint64_t value) {
  for (int i = 0; i < 8; ++i) out->push_back(static_cast<char>(value >> (i * 8)));
}
bool Get32(const std::string& in, size_t* p, uint32_t* value) {
  if (*p > in.size() || in.size() - *p < 4) return false;
  *value = 0;
  for (int i = 0; i < 4; ++i) *value |= uint32_t(uint8_t(in[*p + i])) << (i * 8);
  *p += 4;
  return true;
}
bool Get64(const std::string& in, size_t* p, uint64_t* value) {
  if (*p > in.size() || in.size() - *p < 8) return false;
  *value = 0;
  for (int i = 0; i < 8; ++i) *value |= uint64_t(uint8_t(in[*p + i])) << (i * 8);
  *p += 8;
  return true;
}
void PutBytes(std::string* out, const std::string& value) {
  Put32(out, static_cast<uint32_t>(value.size()));
  out->append(value);
}
bool GetBytes(const std::string& in, size_t* p, std::string* value) {
  uint32_t size = 0;
  if (!Get32(in, p, &size) || size > in.size() - *p) return false;
  value->assign(in.data() + *p, size);
  *p += size;
  return true;
}

}  // namespace

Status ValidatePropertyIndexSegment(const PropertyIndexSegment& segment) {
  if (segment.generation_id == 0 || segment.base_seq.value > segment.built_through.value ||
      !segment.property.valid() || segment.schema_epoch == 0) {
    return Status::InvalidArgument("property index", "invalid segment identity");
  }
  if (segment.postings.size() > 1'000'000) {
    return Status::ResourceExhausted("property index", "posting count exceeds bound");
  }
  for (size_t i = 0; i < segment.postings.size(); ++i) {
    const auto& posting = segment.postings[i];
    if (!posting.vertex.valid() || posting.vertex.part_id != segment.part_id ||
        !posting.effective.Validate().ok() || posting.commit_seq.value > segment.built_through.value) {
      return Status::InvalidArgument("property index", "invalid posting");
    }
    if (i != 0) {
      const auto& prior = segment.postings[i - 1];
      const std::string prior_key = prior.value.Encode();
      const std::string current_key = posting.value.Encode();
      if (prior_key > current_key ||
          (prior_key == current_key && prior.vertex.vertex_id.value > posting.vertex.vertex_id.value)) {
        return Status::InvalidArgument("property index", "postings are not sorted");
      }
    }
  }
  return Status::OK();
}

StatusOr<std::string> EncodePropertyIndexSegment(
    const PropertyIndexSegment& segment) {
  const Status valid = ValidatePropertyIndexSegment(segment);
  if (!valid.ok()) return valid;
  std::string out("CPI1", 4);
  Put64(&out, segment.generation_id);
  Put64(&out, segment.base_seq.value);
  Put64(&out, segment.built_through.value);
  Put32(&out, segment.property.value);
  Put32(&out, segment.part_id.value);
  Put32(&out, segment.schema_epoch);
  Put32(&out, static_cast<uint32_t>(segment.postings.size()));
  for (const auto& posting : segment.postings) {
    Put64(&out, posting.vertex.vertex_id.value);
    Put64(&out, posting.effective.from.value);
    out.push_back(static_cast<char>(posting.effective.to.has_value()));
    Put64(&out, posting.effective.to ? posting.effective.to->value : 0);
    Put64(&out, posting.commit_seq.value);
    PutBytes(&out, posting.value.Encode());
  }
  Put32(&out, crc32c::Value(out.data(), out.size()));
  return out;
}

StatusOr<PropertyIndexSegment> DecodePropertyIndexSegment(
    const std::string& bytes, size_t allocation_limit) {
  if (bytes.size() < 4 + 8 * 3 + 4 * 4 + 4 || bytes.compare(0, 4, "CPI1") != 0) {
    return Status::Corruption("property index", "invalid segment header");
  }
  if (bytes.size() > allocation_limit) {
    return Status::ResourceExhausted("property index", "segment exceeds allocation limit");
  }
  const size_t payload_end = bytes.size() - 4;
  size_t crc_at = payload_end;
  uint32_t expected = 0;
  if (!Get32(bytes, &crc_at, &expected) || expected != crc32c::Value(bytes.data(), payload_end)) {
    return Status::Corruption("property index", "segment checksum mismatch");
  }
  PropertyIndexSegment segment;
  size_t p = 4;
  uint32_t property = 0, part = 0, epoch = 0, count = 0;
  if (!Get64(bytes, &p, &segment.generation_id) || !Get64(bytes, &p, &segment.base_seq.value) ||
      !Get64(bytes, &p, &segment.built_through.value) || !Get32(bytes, &p, &property) ||
      !Get32(bytes, &p, &part) || !Get32(bytes, &p, &epoch) || !Get32(bytes, &p, &count) ||
      count > 1'000'000) {
    return Status::Corruption("property index", "truncated segment metadata");
  }
  segment.property = PropertyId{static_cast<uint16_t>(property)};
  segment.part_id = PartId{part};
  segment.schema_epoch = epoch;
  segment.postings.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    PropertyIndexPosting posting{VertexRef{}, {}, CommitSeq{}, Value::Int64(0)};
    uint64_t vertex = 0, from = 0, to = 0, commit = 0;
    if (!Get64(bytes, &p, &vertex) || !Get64(bytes, &p, &from) || p >= payload_end) {
      return Status::Corruption("property index", "truncated posting");
    }
    const uint8_t has_to = static_cast<uint8_t>(bytes[p++]);
    if (has_to > 1 || !Get64(bytes, &p, &to) || !Get64(bytes, &p, &commit)) {
      return Status::Corruption("property index", "invalid posting interval");
    }
    std::string encoded;
    if (!GetBytes(bytes, &p, &encoded)) return Status::Corruption("property index", "truncated value");
    const auto value = Value::Decode(encoded);
    if (!value.has_value()) return Status::Corruption("property index", "invalid posting value");
    posting.vertex = VertexRef{segment.part_id, VertexId{vertex}};
    posting.effective = {ValidTime{from}, has_to ? std::optional<ValidTime>(ValidTime{to}) : std::nullopt};
    posting.commit_seq = CommitSeq{commit};
    posting.value = *value;
    segment.postings.push_back(std::move(posting));
  }
  if (p != payload_end) return Status::Corruption("property index", "trailing segment payload");
  const Status valid = ValidatePropertyIndexSegment(segment);
  if (!valid.ok()) return valid;
  return segment;
}

}  // namespace cedar::internal
