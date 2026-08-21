#include "query/projection/projection_format.h"

#include <algorithm>
#include <limits>
#include <unordered_map>

#include "cedar/core/crc32c.h"

namespace cedar::internal {
namespace {
constexpr size_t kHeaderBytes = 81;
constexpr size_t kDirectoryBytes = 74;

void P16(std::string* s, uint16_t v) { for (int i = 0; i < 2; ++i) s->push_back(char(v >> (8 * i))); }
void P32(std::string* s, uint32_t v) { for (int i = 0; i < 4; ++i) s->push_back(char(v >> (8 * i))); }
void P64(std::string* s, uint64_t v) { for (int i = 0; i < 8; ++i) s->push_back(char(v >> (8 * i))); }
bool G16(const std::string& s, size_t* p, uint16_t* v) {
  if (*p > s.size() || s.size() - *p < 2) return false;
  *v = uint16_t(uint8_t(s[*p])) | uint16_t(uint8_t(s[*p + 1])) << 8; *p += 2; return true;
}
bool G32(const std::string& s, size_t* p, uint32_t* v) {
  if (*p > s.size() || s.size() - *p < 4) return false; *v = 0;
  for (int i = 0; i < 4; ++i) *v |= uint32_t(uint8_t(s[*p + i])) << (8 * i); *p += 4; return true;
}
bool G64(const std::string& s, size_t* p, uint64_t* v) {
  if (*p > s.size() || s.size() - *p < 8) return false; *v = 0;
  for (int i = 0; i < 8; ++i) *v |= uint64_t(uint8_t(s[*p + i])) << (8 * i); *p += 8; return true;
}
Status ValidRange(uint64_t emin, uint64_t emax, uint64_t from,
                  const std::optional<ValidTime>& to) {
  if (emax < emin || (to && to->value < from)) return Status::Corruption("projection", "invalid range");
  return Status::OK();
}
Status ValidPage(const ProjectionPageDirectoryEntry& d) {
  auto s = ValidRange(d.entity_min, d.entity_max_exclusive, d.valid_from_min.value, d.valid_to_max);
  if (!s.ok()) return s;
  if (d.edge_type_min.has_value() != d.edge_type_max.has_value() ||
      (d.edge_type_min && d.edge_type_max && *d.edge_type_max < *d.edge_type_min))
    return Status::Corruption("projection", "invalid edge range");
  return Status::OK();
}

void EncodePayload(std::string* s, const std::vector<ProjectionInterval>& is,
                   const std::vector<ProjectionBoundary>& bs) {
  std::vector<std::string> dictionary;
  std::unordered_map<std::string, uint32_t> indexes;
  auto add_value = [&](const Value& value) {
    std::string encoded = value.Encode();
    auto [it, inserted] = indexes.emplace(encoded, uint32_t(dictionary.size()));
    if (inserted) dictionary.push_back(std::move(encoded));
    return it->second;
  };
  std::vector<uint32_t> interval_indexes, boundary_indexes;
  for (const auto& i : is) interval_indexes.push_back(add_value(i.value));
  for (const auto& b : bs) boundary_indexes.push_back(add_value(b.value));
  P32(s, uint32_t(is.size())); P32(s, uint32_t(bs.size())); P32(s, uint32_t(dictionary.size()));
  for (const auto& value : dictionary) { P32(s, uint32_t(value.size())); s->append(value); }
  uint64_t prev = 0;
  for (size_t n = 0; n < is.size(); ++n) { const auto& i = is[n]; P64(s, i.effective.from.value - prev); prev = i.effective.from.value;
    s->push_back(char(i.effective.to.has_value() ? 1 : 0)); P64(s, i.effective.to ? i.effective.to->value : 0); P32(s, interval_indexes[n]); }
  prev = 0;
  for (size_t n = 0; n < bs.size(); ++n) { const auto& b = bs[n]; P64(s, b.time.value - prev); prev = b.time.value; s->push_back(char(b.operation)); P32(s, boundary_indexes[n]); }
}
Status DecodePayload(const std::string& s, size_t row_limit, ProjectionChain* c) {
  size_t p = 0; uint32_t ni = 0, nb = 0, nd = 0;
  if (!G32(s, &p, &ni) || !G32(s, &p, &nb) || !G32(s, &p, &nd) || ni > row_limit ||
      nb > row_limit - std::min<size_t>(row_limit, ni) || nd > s.size() / 5)
    return Status::Corruption("projection", "invalid column counts");
  std::vector<Value> dictionary; dictionary.reserve(nd);
  for (uint32_t n = 0; n < nd; ++n) {
    uint32_t length = 0; if (!G32(s, &p, &length) || p > s.size() || length > s.size() - p)
      return Status::Corruption("projection", "invalid value dictionary");
    auto value = Value::Decode(s.substr(p, length)); if (!value) return Status::Corruption("projection", "invalid dictionary value");
    dictionary.push_back(*value); p += length;
  }
  uint64_t prev = 0;
  for (uint32_t n = 0; n < ni; ++n) {
    uint64_t d = 0, to = 0; uint32_t index = 0; if (!G64(s, &p, &d) || d > UINT64_MAX - prev || p >= s.size()) return Status::Corruption("projection", "invalid interval");
    prev += d; uint8_t has = uint8_t(s[p++]); if (has > 1 || !G64(s, &p, &to) || !G32(s, &p, &index) || index >= dictionary.size() || (has && to < prev)) return Status::Corruption("projection", "invalid interval range");
    Value v = dictionary[index];
    c->intervals.push_back({{ValidTime{prev}, has ? std::optional<ValidTime>(ValidTime{to}) : std::nullopt}, v});
  }
  prev = 0;
  for (uint32_t n = 0; n < nb; ++n) {
    uint64_t d = 0; uint32_t index = 0; if (!G64(s, &p, &d) || d > UINT64_MAX - prev || p >= s.size()) return Status::Corruption("projection", "invalid boundary");
    prev += d; auto op = FactOperation(uint8_t(s[p++])); if (op != FactOperation::kPut && op != FactOperation::kDelete) return Status::Corruption("projection", "invalid operation");
    if (!G32(s, &p, &index) || index >= dictionary.size()) return Status::Corruption("projection", "invalid boundary dictionary index");
    c->boundaries.push_back({ValidTime{prev}, op, dictionary[index]});
  }
  return p == s.size() ? Status::OK() : Status::Corruption("projection", "trailing payload");
}
struct Page { std::vector<ProjectionInterval> intervals; std::vector<ProjectionBoundary> boundaries; };
std::vector<Page> Pages(const ProjectionChain& c) {
  size_t count = c.page_directory.empty() ? 1 : c.page_directory.size(); std::vector<Page> out(count);
  size_t total = c.intervals.size() + c.boundaries.size(), cursor = 0;
  for (size_t page = 0; page < count; ++page) { size_t want = (total - cursor + count - page - 1) / (count - page);
    while (want && cursor < c.intervals.size()) { out[page].intervals.push_back(c.intervals[cursor++]); --want; }
    while (want && cursor < total) { out[page].boundaries.push_back(c.boundaries[cursor++ - c.intervals.size()]); --want; }
  } return out;
}
Status ValidDirectory(const std::vector<ProjectionPageDirectoryEntry>& d, size_t start, size_t size) {
  if (size < 4 || start > size - 4) return Status::Corruption("projection", "directory outside file");
  uint64_t end = start;
  for (const auto& p : d) { if (p.offset < start || p.offset < end || p.offset > size - 4) return Status::Corruption("projection", "malformed directory");
    if (p.compressed_bytes > size - 4 - size_t(p.offset)) return Status::Corruption("projection", "page exceeds file"); end = p.offset + p.compressed_bytes; }
  return end == size - 4 ? Status::OK() : Status::Corruption("projection", "page payload gap");
}
}  // namespace

StatusOr<std::string> EncodeProjectionPage(const ProjectionChain& c, CompressionCodec codec) {
  if (uint8_t(c.header.kind) < 1 || uint8_t(c.header.kind) > 4) return Status::InvalidArgument("projection", "invalid projection kind");
  if (uint8_t(codec) > uint8_t(CompressionCodec::kRle)) return Status::NotSupported("projection", "unknown compression codec");
  auto hr = ValidRange(c.header.entity_min, c.header.entity_max_exclusive, c.header.valid_from_min.value, c.header.valid_to_max); if (!hr.ok()) return hr;
  auto pages = Pages(c); if (pages.size() > UINT32_MAX) return Status::ResourceExhausted("projection", "too many pages");
  std::string out("CDRPRJ1\0", 8); P32(&out, 1); out.push_back(char(c.header.kind)); out.push_back(char(codec));
  P64(&out, c.header.generation_id); P64(&out, c.header.base_seq.value); P32(&out, c.header.part_id.value); P16(&out, c.header.property_id.value); P32(&out, c.header.schema_epoch);
  P64(&out, c.header.entity_min); P64(&out, c.header.entity_max_exclusive); P64(&out, c.header.valid_from_min.value); out.push_back(char(c.header.valid_to_max ? 1 : 0)); P64(&out, c.header.valid_to_max ? c.header.valid_to_max->value : 0); P32(&out, uint32_t(pages.size())); P32(&out, crc32c::Value(out.data(), out.size()));
  std::vector<std::string> payloads; std::vector<ProjectionPageDirectoryEntry> dirs; size_t offset = kHeaderBytes + pages.size() * kDirectoryBytes;
  for (size_t i = 0; i < pages.size(); ++i) { std::string raw; EncodePayload(&raw, pages[i].intervals, pages[i].boundaries); auto cp = CompressProjectionPayload(codec, raw); if (!cp.ok()) return cp.status();
    if (raw.size() > UINT32_MAX || cp.ValueOrDie().size() > UINT32_MAX) return Status::ResourceExhausted("projection", "page too large");
    ProjectionPageDirectoryEntry d;
    if (i < c.page_directory.size()) {
      d = c.page_directory[i];
    } else {
      d.entity_min = c.header.entity_min;
      d.entity_max_exclusive = c.header.entity_max_exclusive;
      d.valid_from_min = c.header.valid_from_min;
      d.valid_to_max = c.header.valid_to_max;
    }
    auto vd = ValidPage(d); if (!vd.ok()) return vd; d.offset = offset; d.compressed_bytes = uint32_t(cp.ValueOrDie().size()); d.uncompressed_bytes = uint32_t(raw.size()); d.row_count = uint32_t(pages[i].intervals.size() + pages[i].boundaries.size()); d.payload_crc32c = crc32c::Value(cp.ValueOrDie().data(), cp.ValueOrDie().size()); dirs.push_back(d); offset += cp.ValueOrDie().size(); payloads.push_back(cp.ConsumeValueOrDie()); }
  for (const auto& d : dirs) { P64(&out, d.offset); P32(&out, d.compressed_bytes); P32(&out, d.uncompressed_bytes); P32(&out, d.row_count); P64(&out, d.entity_min); P64(&out, d.entity_max_exclusive); P64(&out, d.valid_from_min.value); out.push_back(char(d.valid_to_max ? 1 : 0)); P64(&out, d.valid_to_max ? d.valid_to_max->value : 0); out.push_back(char(d.edge_type_min ? 1 : 0)); P64(&out, d.edge_type_min.value_or(0)); P64(&out, d.edge_type_max.value_or(0)); P32(&out, d.payload_crc32c); }
  for (const auto& p : payloads) out.append(p); P32(&out, crc32c::Value(out.data(), out.size())); return out;
}

StatusOr<ProjectionChain> DecodeProjectionPage(const std::string& b, size_t limit) {
  if (b.size() < kHeaderBytes + 4 || b.compare(0, 8, "CDRPRJ1\0", 8) != 0) return Status::Corruption("projection", "bad magic or truncated file");
  size_t p = 8; uint32_t version = 0; if (!G32(b, &p, &version)) return Status::Corruption("projection", "truncated version"); if (version != 1) return Status::NotSupported("projection", "unknown format version"); if (p + 2 > b.size()) return Status::Corruption("projection", "truncated header");
  auto kind = ProjectionKind(uint8_t(b[p++])); auto codec = CompressionCodec(uint8_t(b[p++])); if (uint8_t(kind) < 1 || uint8_t(kind) > 4) return Status::Corruption("projection", "invalid projection kind"); if (uint8_t(codec) > uint8_t(CompressionCodec::kRle)) return Status::NotSupported("projection", "unknown compression codec");
  ProjectionChain c; c.header.kind = kind; uint64_t gen = 0, base = 0, emin = 0, emax = 0, from = 0, to = 0; uint32_t part = 0, schema = 0, count = 0; uint16_t prop = 0;
  if (!G64(b, &p, &gen) || !G64(b, &p, &base) || !G32(b, &p, &part) || !G16(b, &p, &prop) || !G32(b, &p, &schema) || !G64(b, &p, &emin) || !G64(b, &p, &emax) || !G64(b, &p, &from) || p >= b.size()) return Status::Corruption("projection", "truncated header");
  uint8_t has = uint8_t(b[p++]); if (has > 1 || !G64(b, &p, &to) || !G32(b, &p, &count)) return Status::Corruption("projection", "invalid header flag"); auto hr = ValidRange(emin, emax, from, has ? std::optional<ValidTime>(ValidTime{to}) : std::nullopt); if (!hr.ok()) return hr;
  if (count > limit / kDirectoryBytes || count > (b.size() - kHeaderBytes) / kDirectoryBytes) return Status::ResourceExhausted("projection", "directory exceeds budget"); uint32_t hc = 0; if (!G32(b, &p, &hc) || p != kHeaderBytes || hc != crc32c::Value(b.data(), kHeaderBytes - 4)) return Status::Corruption("projection", "header CRC32C mismatch");
  c.header.generation_id = gen; c.header.base_seq = {base}; c.header.part_id = {part}; c.header.property_id = {prop}; c.header.schema_epoch = schema; c.header.entity_min = emin; c.header.entity_max_exclusive = emax; c.header.valid_from_min = {from}; c.header.valid_to_max = has ? std::optional<ValidTime>(ValidTime{to}) : std::nullopt; c.page_directory.reserve(count);
  for (uint32_t i = 0; i < count; ++i) { ProjectionPageDirectoryEntry d; uint8_t hp = 0, he = 0; uint64_t vf = 0, vt = 0, mn = 0, mx = 0; if (!G64(b, &p, &d.offset) || !G32(b, &p, &d.compressed_bytes) || !G32(b, &p, &d.uncompressed_bytes) || !G32(b, &p, &d.row_count) || !G64(b, &p, &d.entity_min) || !G64(b, &p, &d.entity_max_exclusive) || !G64(b, &p, &vf) || p >= b.size()) return Status::Corruption("projection", "truncated directory"); d.valid_from_min = {vf}; hp = uint8_t(b[p++]); if (hp > 1 || !G64(b, &p, &vt) || p >= b.size()) return Status::Corruption("projection", "invalid page flag"); he = uint8_t(b[p++]); if (he > 1 || !G64(b, &p, &mn) || !G64(b, &p, &mx) || !G32(b, &p, &d.payload_crc32c)) return Status::Corruption("projection", "truncated page metadata"); d.valid_to_max = hp ? std::optional<ValidTime>(ValidTime{vt}) : std::nullopt; if (he) { d.edge_type_min = mn; d.edge_type_max = mx; } auto vd = ValidPage(d); if (!vd.ok()) return vd; c.page_directory.push_back(d); }
  size_t start = kHeaderBytes + size_t(count) * kDirectoryBytes; auto valid = ValidDirectory(c.page_directory, start, b.size()); if (!valid.ok()) return valid; size_t fc_at = b.size() - 4; uint32_t fc = 0; if (!G32(b, &fc_at, &fc) || fc != crc32c::Value(b.data(), b.size() - 4)) return Status::Corruption("projection", "file CRC32C mismatch");
  size_t used_bytes = 0, rows = 0; for (const auto& d : c.page_directory) { size_t offset = size_t(d.offset); if (d.payload_crc32c != crc32c::Value(b.data() + offset, d.compressed_bytes)) return Status::Corruption("projection", "page payload CRC32C mismatch"); if (d.uncompressed_bytes > limit - std::min(limit, used_bytes)) return Status::ResourceExhausted("projection", "decoded bytes exceed remaining budget"); size_t remaining = limit - used_bytes; auto raw = DecompressProjectionPayload(codec, b.substr(offset, d.compressed_bytes), std::min<size_t>(d.uncompressed_bytes, remaining)); if (!raw.ok()) return raw.status(); if (raw.ValueOrDie().size() != d.uncompressed_bytes) return Status::Corruption("projection", "decoded length mismatch"); size_t before = rows; auto ds = DecodePayload(raw.ValueOrDie(), limit - std::min(limit, rows), &c); if (!ds.ok()) return ds; rows = c.intervals.size() + c.boundaries.size(); if (rows - before != d.row_count) return Status::Corruption("projection", "directory row count mismatch"); used_bytes += d.uncompressed_bytes; }
  return c;
}
}  // namespace cedar::internal
