#include "query/projection/property_index.h"

#include <algorithm>
#include <cstring>
#include <type_traits>

#include "cedar/core/crc32c.h"
#include "query/resource/query_read_memory.h"

namespace cedar::internal {
namespace {

constexpr uint32_t kPropertyIndexPageRows = 256;

int CompareValues(const Value& left, const Value& right) {
  if (left.type() != right.type()) return 0;
  return std::visit(
      [](const auto& a, const auto& b) -> int {
        using A = std::decay_t<decltype(a)>;
        using B = std::decay_t<decltype(b)>;
        if constexpr (!std::is_same_v<A, B>) {
          return 0;
        } else {
          return a < b ? -1 : (a > b ? 1 : 0);
        }
      },
      left.data(), right.data());
}

bool Matches(PropertyIndexOperator op, int cmp) {
  switch (op) {
    case PropertyIndexOperator::kEqual: return cmp == 0;
    case PropertyIndexOperator::kLess: return cmp < 0;
    case PropertyIndexOperator::kLessEqual: return cmp <= 0;
    case PropertyIndexOperator::kGreater: return cmp > 0;
    case PropertyIndexOperator::kGreaterEqual: return cmp >= 0;
  }
  return false;
}

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
Status ReadValue(const std::string& in, size_t* p, size_t payload_end,
                 Value* value, FactReadMemoryScope* output,
                 FactReadMemory* memory) {
  uint32_t size = 0;
  if (!Get32(in, p, &size) || *p > payload_end || size > payload_end - *p)
    return Status::Corruption("property index", "truncated value");
  FactReadMemoryScope scratch(memory);
  const uint64_t capacity = std::max<uint64_t>(16, uint64_t{size} + 1);
  if (Status status = scratch.Reserve(capacity); !status.ok()) return status;
  if (Status status = output->Reserve(capacity); !status.ok()) return status;
  std::string encoded(in.data() + *p, size);
  auto decoded = Value::Decode(encoded);
  if (!decoded) return Status::Corruption("property index", "invalid value");
  const auto* text = std::get_if<std::string>(&decoded->data());
  const uint64_t retained = text ? text->capacity() + 1 : 0;
  if (retained > capacity) {
    return Status::ResourceExhausted("property index", "decoded value exceeds reserved capacity");
  }
  output->Release(capacity - retained);
  *value = std::move(*decoded);
  *p += size;
  return Status::OK();
}

uint64_t ValueHeapBytes(const Value& value) {
  const auto* text = std::get_if<std::string>(&value.data());
  return text ? text->capacity() + 1 : 0;
}

Status ReserveEncodingScratch(const Value& value, FactReadMemoryScope* scratch) {
  const uint64_t bytes = ValueHeapBytes(value);
  if (bytes > UINT64_MAX / 8 - 128)
    return Status::ResourceExhausted("property index", "encoding scratch overflow");
  return scratch->Reserve(8 * (bytes + 128));
}

uint64_t BloomBit(const PropertyIndexPosting& posting) {
  // The page bloom is deliberately tiny: it is only a cheap negative filter
  // after the value min/max test, never a source of correctness decisions.
  uint64_t hash = posting.vertex.vertex_id.value ^
                  (uint64_t(posting.vertex.part_id.value) << 32);
  hash ^= std::hash<std::string>{}(posting.value.Encode());
  hash ^= hash >> 33;
  hash *= 0xff51afd7ed558ccdULL;
  return 1ULL << (hash & 63);
}

std::string EncodePostingForCrc(const PropertyIndexPosting& posting) {
  std::string out;
  Put64(&out, posting.vertex.vertex_id.value);
  Put64(&out, posting.effective.from.value);
  out.push_back(static_cast<char>(posting.effective.to.has_value()));
  Put64(&out, posting.effective.to ? posting.effective.to->value : 0);
  Put64(&out, posting.commit_seq.value);
  PutBytes(&out, posting.value.Encode());
  return out;
}

Status BuildPageDirectory(PropertyIndexSegment* segment,
                          FactReadMemoryScope* output = nullptr,
                          FactReadMemory* scratch_memory = nullptr) {
  segment->pages.clear();
  if (segment->postings.size() > UINT32_MAX)
    return Status::ResourceExhausted("property index", "posting count exceeds directory range");
  const uint64_t page_count = (segment->postings.size() + kPropertyIndexPageRows - 1) /
                              kPropertyIndexPageRows;
  if (output && page_count > segment->pages.capacity()) {
    if (Status status = output->ReserveElements(page_count,
            sizeof(PropertyIndexSegment::PageDirectoryEntry)); !status.ok()) return status;
  }
  segment->pages.reserve(page_count);
  for (uint64_t first = 0; first < segment->postings.size();
       first += kPropertyIndexPageRows) {
    const uint32_t count = std::min<uint32_t>(
        kPropertyIndexPageRows,
        static_cast<uint32_t>(segment->postings.size() - first));
    PropertyIndexSegment::PageDirectoryEntry page;
    page.first_posting = first;
    page.row_count = count;
    if (output) {
      if (Status status = output->Reserve(ValueHeapBytes(segment->postings[first].value));
          !status.ok()) return status;
      if (Status status = output->Reserve(ValueHeapBytes(segment->postings[first + count - 1].value));
          !status.ok()) return status;
    }
    page.min_value = segment->postings[first].value;
    page.max_value = segment->postings[first + count - 1].value;
    page.entity_min = UINT64_MAX;
    page.entity_max_exclusive = 0;
    page.valid_from_min = segment->postings[first].effective.from;
    uint32_t payload_crc = 0;
    for (uint32_t i = 0; i < count; ++i) {
      const auto& posting = segment->postings[first + i];
      page.entity_min = std::min(page.entity_min, posting.vertex.vertex_id.value);
      if (posting.vertex.vertex_id.value == UINT64_MAX) {
        page.entity_max_exclusive = UINT64_MAX;
      } else if (page.entity_max_exclusive != UINT64_MAX) {
        page.entity_max_exclusive = std::max(
            page.entity_max_exclusive, posting.vertex.vertex_id.value + 1);
      }
      page.valid_from_min = ValidTime{std::min(
          page.valid_from_min.value, posting.effective.from.value)};
      if (posting.effective.to.has_value() &&
          (!page.valid_to_max ||
           posting.effective.to->value > page.valid_to_max->value)) {
        page.valid_to_max = posting.effective.to;
      }
      FactReadMemoryScope scratch(scratch_memory);
      if (Status status = ReserveEncodingScratch(posting.value, &scratch); !status.ok()) return status;
      page.bloom_mask |= BloomBit(posting);
      const std::string encoded = EncodePostingForCrc(posting);
      payload_crc = crc32c::Extend(payload_crc, encoded.data(), encoded.size());
    }
    page.payload_crc32c = payload_crc;
    segment->pages.push_back(std::move(page));
  }
  return Status::OK();
}

void EncodePageDirectoryEntry(std::string* out,
                              const PropertyIndexSegment::PageDirectoryEntry& page) {
  Put32(out, page.first_posting);
  Put32(out, page.row_count);
  PutBytes(out, page.min_value.Encode());
  PutBytes(out, page.max_value.Encode());
  Put64(out, page.entity_min);
  Put64(out, page.entity_max_exclusive);
  Put64(out, page.valid_from_min.value);
  out->push_back(static_cast<char>(page.valid_to_max.has_value()));
  Put64(out, page.valid_to_max ? page.valid_to_max->value : 0);
  Put64(out, page.bloom_mask);
  Put32(out, page.payload_crc32c);
}

StatusOr<PropertyIndexSegment::PageDirectoryEntry> DecodePageDirectoryEntry(
    const std::string& in, size_t* p, size_t payload_end,
    FactReadMemoryScope* output, FactReadMemory* memory) {
  PropertyIndexSegment::PageDirectoryEntry page;
  uint32_t has_to = 0;
  uint64_t to = 0;
  if (!Get32(in, p, &page.first_posting) || !Get32(in, p, &page.row_count))
    return Status::Corruption("property index", "truncated page directory");
  if (Status status = ReadValue(in, p, payload_end, &page.min_value, output, memory);
      !status.ok()) return status;
  if (Status status = ReadValue(in, p, payload_end, &page.max_value, output, memory);
      !status.ok()) return status;
  if (!Get64(in, p, &page.entity_min) ||
      !Get64(in, p, &page.entity_max_exclusive) ||
      !Get64(in, p, &page.valid_from_min.value) || *p >= payload_end) {
    return Status::Corruption("property index", "truncated page directory");
  }
  has_to = static_cast<uint8_t>(in[(*p)++]);
  if (has_to > 1 || !Get64(in, p, &to) || !Get64(in, p, &page.bloom_mask) ||
      !Get32(in, p, &page.payload_crc32c) || has_to > 1) {
    return Status::Corruption("property index", "invalid page directory");
  }
  if (has_to) page.valid_to_max = ValidTime{to};
  return page;
}

}  // namespace

Status BuildPropertyIndexPages(PropertyIndexSegment* segment,
    FactReadMemoryScope* output, FactReadMemory* scratch_memory) {
  if (segment == nullptr) {
    return Status::InvalidArgument("property index", "missing segment");
  }
  return BuildPageDirectory(segment, output, scratch_memory);
}

Status ValidatePropertyIndexSegment(const PropertyIndexSegment& segment,
                                   FactReadMemory* scratch_memory) {
  if (segment.generation_id == 0 || segment.base_seq.value > segment.built_through.value ||
      !segment.property.valid() || segment.schema_epoch == 0) {
    return Status::InvalidArgument("property index", "invalid segment identity");
  }
  if (segment.postings.size() > 1'000'000) {
    return Status::ResourceExhausted("property index", "posting count exceeds bound");
  }
  std::optional<PhysicalType> value_type;
  for (size_t i = 0; i < segment.postings.size(); ++i) {
    const auto& posting = segment.postings[i];
    if (!posting.vertex.valid() || posting.vertex.part_id != segment.part_id ||
        !posting.effective.Validate().ok() || posting.commit_seq.value > segment.built_through.value) {
      return Status::InvalidArgument("property index", "invalid posting");
    }
    if (!value_type) value_type = posting.value.type();
    if (*value_type != posting.value.type()) {
      return Status::InvalidArgument("property index", "mixed posting value types");
    }
    if (i != 0) {
      const auto& prior = segment.postings[i - 1];
      const int value_order = CompareValues(prior.value, posting.value);
      if (value_order > 0 ||
          (value_order == 0 && prior.vertex.vertex_id.value > posting.vertex.vertex_id.value)) {
        return Status::InvalidArgument("property index", "postings are not sorted");
      }
    }
  }
  if (!segment.pages.empty()) {
    uint32_t expected_first = 0;
    for (const auto& page : segment.pages) {
      if (page.row_count == 0 || page.first_posting != expected_first ||
          page.first_posting > segment.postings.size() ||
          page.row_count > segment.postings.size() - page.first_posting ||
          page.entity_min > page.entity_max_exclusive ||
          page.min_value.type() != page.max_value.type()) {
        return Status::InvalidArgument("property index", "invalid page directory");
      }
      const auto& first = segment.postings[page.first_posting];
      const auto& last = segment.postings[page.first_posting + page.row_count - 1];
      if (page.min_value != first.value || page.max_value != last.value) {
        return Status::InvalidArgument("property index", "page bounds differ from postings");
      }
      uint32_t payload_crc = 0;
      for (uint32_t i = 0; i < page.row_count; ++i) {
        const auto& posting = segment.postings[page.first_posting + i];
        FactReadMemoryScope scratch(scratch_memory);
        if (Status status = ReserveEncodingScratch(posting.value, &scratch); !status.ok()) return status;
        const std::string encoded = EncodePostingForCrc(posting);
        payload_crc = crc32c::Extend(payload_crc, encoded.data(), encoded.size());
      }
      if (page.payload_crc32c != payload_crc) {
        return Status::Corruption("property index", "page payload checksum mismatch");
      }
      expected_first += page.row_count;
    }
    if (expected_first != segment.postings.size()) {
      return Status::InvalidArgument("property index", "page directory does not cover postings");
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
  PropertyIndexSegment with_pages = segment;
  if (with_pages.pages.empty()) BuildPageDirectory(&with_pages).IgnoreError();
  Put32(&out, static_cast<uint32_t>(with_pages.pages.size()));
  for (const auto& page : with_pages.pages) EncodePageDirectoryEntry(&out, page);
  Put32(&out, crc32c::Value(out.data(), out.size()));
  return out;
}

StatusOr<PropertyIndexSegment> DecodePropertyIndexSegment(
    const std::string& bytes, size_t allocation_limit) {
  QueryReservation reservation(allocation_limit);
  auto memory = std::make_shared<QueryReadMemory>(reservation);
  FactReadMemoryScope input(memory.get());
  if (Status status = input.Reserve(bytes.capacity() + 1); !status.ok()) return status;
  auto decoded = DecodeBudgetedPropertyIndexSegment(bytes, memory);
  if (!decoded.ok()) return decoded.status();
  return std::move(decoded.ValueOrDie().segment);
}

StatusOr<PropertyIndexReadResult> DecodeBudgetedPropertyIndexSegment(
    const std::string& bytes, std::shared_ptr<FactReadMemory> memory) {
  if (!memory) return Status::InvalidArgument("property index", "missing read memory");
  if (bytes.size() < 4 + 8 * 3 + 4 * 4 + 4 || bytes.compare(0, 4, "CPI1") != 0) {
    return Status::Corruption("property index", "invalid segment header");
  }
  const size_t payload_end = bytes.size() - 4;
  size_t crc_at = payload_end;
  uint32_t expected = 0;
  if (!Get32(bytes, &crc_at, &expected) || expected != crc32c::Value(bytes.data(), payload_end)) {
    return Status::Corruption("property index", "segment checksum mismatch");
  }
  FactReadMemoryScope output(memory.get());
  PropertyIndexReadResult result;
  auto& segment = result.segment;
  size_t p = 4;
  uint32_t property = 0, part = 0, epoch = 0, count = 0;
  if (!Get64(bytes, &p, &segment.generation_id) || !Get64(bytes, &p, &segment.base_seq.value) ||
      !Get64(bytes, &p, &segment.built_through.value) || !Get32(bytes, &p, &property) ||
      !Get32(bytes, &p, &part) || !Get32(bytes, &p, &epoch) || !Get32(bytes, &p, &count) ||
      count > 1'000'000 || property > UINT16_MAX) {
    return Status::Corruption("property index", "truncated segment metadata");
  }
  segment.property = PropertyId{static_cast<uint16_t>(property)};
  segment.part_id = PartId{part};
  segment.schema_epoch = epoch;
  if (Status status = output.ReserveElements(count, sizeof(PropertyIndexPosting));
      !status.ok()) return status;
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
    if (Status status = ReadValue(bytes, &p, payload_end, &posting.value, &output, memory.get());
        !status.ok()) return status;
    posting.vertex = VertexRef{segment.part_id, VertexId{vertex}};
    posting.effective = {ValidTime{from}, has_to ? std::optional<ValidTime>(ValidTime{to}) : std::nullopt};
    posting.commit_seq = CommitSeq{commit};
    segment.postings.push_back(std::move(posting));
  }
  // CPI1 files produced before page directories ended after the posting list.
  // Accept those files, while all newly written files carry a bounded page
  // directory and are validated below.
  if (p < payload_end) {
    uint32_t page_count = 0;
    if (!Get32(bytes, &p, &page_count) || page_count > 1'000'000) {
      return Status::Corruption("property index", "invalid page directory count");
    }
    if (Status status = output.ReserveElements(page_count,
            sizeof(PropertyIndexSegment::PageDirectoryEntry)); !status.ok()) return status;
    segment.pages.reserve(page_count);
    for (uint32_t i = 0; i < page_count; ++i) {
      auto page = DecodePageDirectoryEntry(bytes, &p, payload_end, &output, memory.get());
      if (!page.ok()) return page.status();
      segment.pages.push_back(std::move(page).ConsumeValueOrDie());
    }
  }
  if (p != payload_end) return Status::Corruption("property index", "trailing segment payload");
  const Status valid = ValidatePropertyIndexSegment(segment, memory.get());
  if (!valid.ok()) return valid;
  auto owner = output.Retain(memory);
  if (!owner.ok()) return owner.status();
  result.memory = owner.ConsumeValueOrDie();
  return result;
}

std::vector<PropertyIndexPosting> SeekPropertyIndexRange(
    const PropertyIndexSegment& segment, PropertyIndexOperator op,
    const Value& lower, const std::optional<Value>& upper) {
  std::vector<PropertyIndexPosting> result;
  VisitPropertyIndexRange(segment, op, lower,
      [&](const PropertyIndexPosting& posting) {
        result.push_back(posting);
        return Status::OK();
      }, upper).IgnoreError();
  return result;
}

Status VisitPropertyIndexRange(
    const PropertyIndexSegment& segment, PropertyIndexOperator op,
    const Value& lower,
    const std::function<Status(const PropertyIndexPosting&)>& visitor,
    const std::optional<Value>& upper) {
  if (!visitor) return Status::InvalidArgument("property index", "missing visitor");
  const auto& postings = segment.postings;
  if (postings.empty()) return Status::OK();
  const bool starts_at_lower = op == PropertyIndexOperator::kEqual ||
                               op == PropertyIndexOperator::kGreater ||
                               op == PropertyIndexOperator::kGreaterEqual;
  size_t first_index = 0;
  if (starts_at_lower && !segment.pages.empty()) {
    const auto page = std::lower_bound(
        segment.pages.begin(), segment.pages.end(), lower,
        [](const PropertyIndexSegment::PageDirectoryEntry& page,
           const Value& value) { return CompareValues(page.max_value, value) < 0; });
    first_index = page == segment.pages.end()
                      ? postings.size()
                      : page->first_posting;
  } else if (starts_at_lower) {
    first_index = static_cast<size_t>(std::lower_bound(
        postings.begin(), postings.end(), lower,
        [](const PropertyIndexPosting& posting, const Value& value) {
          return CompareValues(posting.value, value) < 0;
        }) - postings.begin());
  }
  for (size_t index = first_index; index < postings.size(); ++index) {
    const auto& posting = postings[index];
    const int cmp = CompareValues(posting.value, lower);
    if (posting.value.type() != lower.type()) continue;
    if (op == PropertyIndexOperator::kLess ||
        op == PropertyIndexOperator::kLessEqual) {
      if (cmp > 0 || (op == PropertyIndexOperator::kLess && cmp == 0)) break;
    } else if (op == PropertyIndexOperator::kEqual && cmp > 0) {
      break;
    }
    if (upper.has_value() && CompareValues(posting.value, *upper) > 0) break;
    if (Matches(op, cmp)) {
      if (Status status = visitor(posting); !status.ok()) return status;
    }
  }
  return Status::OK();
}

}  // namespace cedar::internal
