#include "query/projection/projection_format.h"

#include <algorithm>
#include <limits>
#include <unordered_map>
#include <utility>

#include "cedar/core/crc32c.h"

namespace cedar::internal {
namespace {
constexpr size_t kHeaderBytes = 81;
constexpr size_t kDirectoryBytes = 91;

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
  if (d.bloom_hashes > 8 || d.bloom_bits > 64 ||
      (d.bloom_bits == 0 && (d.bloom_hashes != 0 || d.bloom_mask != 0)) ||
      (d.bloom_bits != 0 && d.bloom_hashes == 0))
    return Status::Corruption("projection", "invalid bloom metadata");
  if (d.bloom_bits < 64 && d.bloom_mask >> d.bloom_bits)
    return Status::Corruption("projection", "bloom mask exceeds bit count");
  return Status::OK();
}

constexpr uint32_t kPayloadMagic = 0x314c4f43;  // COL1
void PutBlob(std::string* out, const std::string& blob) { P32(out, uint32_t(blob.size())); out->append(blob); }
bool GetBlob(const std::string& in, size_t* at, std::string* blob) {
  uint32_t n = 0; if (!G32(in, at, &n) || *at > in.size() || n > in.size() - *at) return false;
  *blob = in.substr(*at, n); *at += n; return true;
}
std::string EncodeRle(const std::vector<uint32_t>& values) {
  std::string out;
  for (size_t i = 0; i < values.size();) { size_t j = i + 1; while (j < values.size() && values[j] == values[i]) ++j; P32(&out, values[i]); P32(&out, uint32_t(j - i)); i = j; }
  return out;
}
uint64_t EncodeSignedDelta(uint64_t current, uint64_t previous) {
  return current - previous;
}
bool DecodeSignedDelta(uint64_t encoded, uint64_t previous, uint64_t* current) {
  if (encoded > UINT64_MAX - previous) return false;
  *current = previous + encoded;
  return true;
}
bool DecodeRle(const std::string& in, size_t expected, std::vector<uint32_t>* values) {
  size_t at = 0; values->clear();
  while (values->size() < expected) { uint32_t value = 0, run = 0; if (!G32(in, &at, &value) || !G32(in, &at, &run) || run == 0 || run > expected - values->size()) return false; values->insert(values->end(), run, value); }
  return at == in.size();
}
void EncodePayload(std::string* s, const std::vector<ProjectionInterval>& is,
                   const std::vector<ProjectionBoundary>& bs) {
  std::vector<std::string> dictionary; std::unordered_map<std::string, uint32_t> indexes;
  auto add = [&](const Value& value) { std::string encoded = value.Encode(); auto [it, inserted] = indexes.emplace(encoded, uint32_t(dictionary.size())); if (inserted) dictionary.push_back(std::move(encoded)); return it->second; };
  std::vector<uint32_t> ii, bi; for (const auto& i : is) ii.push_back(add(i.value)); for (const auto& b : bs) bi.push_back(add(b.value));
  std::string presence((is.size() + 7) / 8, '\0'), operations((bs.size() + 7) / 8, '\0'), ie, it, ito, be, bt;
  uint64_t previous_entity = 0, previous_time = 0;
  for (size_t n = 0; n < is.size(); ++n) { P64(&ie, EncodeSignedDelta(is[n].entity_id, previous_entity)); previous_entity = is[n].entity_id; P64(&it, is[n].effective.from.value - previous_time); previous_time = is[n].effective.from.value; if (is[n].effective.to) presence[n / 8] |= char(1u << (n % 8)); P64(&ito, is[n].effective.to ? is[n].effective.to->value : 0); }
  previous_entity = previous_time = 0;
  for (size_t n = 0; n < bs.size(); ++n) { P64(&be, EncodeSignedDelta(bs[n].entity_id, previous_entity)); previous_entity = bs[n].entity_id; P64(&bt, bs[n].time.value - previous_time); previous_time = bs[n].time.value; if (bs[n].operation == FactOperation::kPut) operations[n / 8] |= char(1u << (n % 8)); }
  P32(s, kPayloadMagic); P32(s, uint32_t(is.size())); P32(s, uint32_t(bs.size())); P32(s, uint32_t(dictionary.size()));
  for (const auto& value : dictionary) PutBlob(s, value);
  PutBlob(s, presence); PutBlob(s, operations); PutBlob(s, ie); PutBlob(s, it); PutBlob(s, ito); PutBlob(s, EncodeRle(ii)); PutBlob(s, be); PutBlob(s, bt); PutBlob(s, EncodeRle(bi));
}
Status DecodePayload(const std::string& s, size_t row_limit, ProjectionChain* c) {
  size_t p = 0; uint32_t magic = 0, ni = 0, nb = 0, nd = 0;
  if (row_limit < sizeof(ProjectionInterval) || row_limit < sizeof(ProjectionBoundary))
    return Status::ResourceExhausted("projection", "row output budget exhausted");
  if (!G32(s, &p, &magic) || magic != kPayloadMagic || !G32(s, &p, &ni) || !G32(s, &p, &nb) || !G32(s, &p, &nd) || ni > row_limit ||
      nb > row_limit - std::min<size_t>(row_limit, ni) || nd > s.size() / 5)
    return Status::Corruption("projection", "invalid column counts");
  if (ni > row_limit / sizeof(ProjectionInterval) || nb > row_limit / sizeof(ProjectionBoundary) ||
      nd > row_limit / sizeof(Value))
    return Status::ResourceExhausted("projection", "decoded rows exceed budget");
  std::vector<Value> dictionary; dictionary.reserve(nd);
  for (uint32_t n = 0; n < nd; ++n) {
    std::string encoded; if (!GetBlob(s, &p, &encoded)) return Status::Corruption("projection", "invalid value dictionary");
    if (encoded.size() > row_limit) return Status::ResourceExhausted("projection", "dictionary exceeds budget");
    auto value = Value::Decode(encoded); if (!value) return Status::Corruption("projection", "invalid dictionary value"); dictionary.push_back(*value);
  }
  std::string presence, operations, ie, it, ito, irle, be, bt, brle;
  if (!GetBlob(s, &p, &presence) || !GetBlob(s, &p, &operations) || !GetBlob(s, &p, &ie) || !GetBlob(s, &p, &it) || !GetBlob(s, &p, &ito) || !GetBlob(s, &p, &irle) || !GetBlob(s, &p, &be) || !GetBlob(s, &p, &bt) || !GetBlob(s, &p, &brle) || presence.size() != (ni + 7) / 8 || operations.size() != (nb + 7) / 8) return Status::Corruption("projection", "invalid column stream");
  if ((ni % 8 != 0 && (uint8_t(presence.back()) & ~uint8_t((1u << (ni % 8)) - 1))) ||
      (nb % 8 != 0 && (uint8_t(operations.back()) & ~uint8_t((1u << (nb % 8)) - 1))))
    return Status::Corruption("projection", "non-zero bitset padding");
  std::vector<uint32_t> ii, bi; if (!DecodeRle(irle, ni, &ii) || !DecodeRle(brle, nb, &bi)) return Status::Corruption("projection", "invalid value run");
  if (ie.size() != ni * 8 || it.size() != ni * 8 || ito.size() != ni * 8 || be.size() != nb * 8 || bt.size() != nb * 8) return Status::Corruption("projection", "invalid delta column");
  size_t ie_at = 0, it_at = 0, ito_at = 0, be_at = 0, bt_at = 0; uint64_t prev = 0, prev_entity = 0;
  for (uint32_t n = 0; n < ni; ++n) {
    uint64_t entity_delta = 0, time_delta = 0, to = 0, entity = 0; if (!G64(ie, &ie_at, &entity_delta) || !G64(it, &it_at, &time_delta) || !G64(ito, &ito_at, &to) || !DecodeSignedDelta(entity_delta, prev_entity, &entity) || time_delta > UINT64_MAX - prev) return Status::Corruption("projection", "invalid interval delta"); prev_entity = entity; prev += time_delta; bool has = (uint8_t(presence[n / 8]) & (1u << (n % 8))) != 0; if (has && to < prev || ii[n] >= dictionary.size()) return Status::Corruption("projection", "invalid interval range"); c->intervals.push_back({{ValidTime{prev}, has ? std::optional<ValidTime>(ValidTime{to}) : std::nullopt}, dictionary[ii[n]], prev_entity});
  }
  prev = prev_entity = 0;
  for (uint32_t n = 0; n < nb; ++n) {
    uint64_t entity_delta = 0, time_delta = 0, entity = 0; if (!G64(be, &be_at, &entity_delta) || !G64(bt, &bt_at, &time_delta) || !DecodeSignedDelta(entity_delta, prev_entity, &entity) || time_delta > UINT64_MAX - prev || bi[n] >= dictionary.size()) return Status::Corruption("projection", "invalid boundary delta"); prev_entity = entity; prev += time_delta; auto op = (uint8_t(operations[n / 8]) & (1u << (n % 8))) ? FactOperation::kPut : FactOperation::kDelete; c->boundaries.push_back({ValidTime{prev}, op, dictionary[bi[n]], prev_entity});
  }
  return p == s.size() ? Status::OK() : Status::Corruption("projection", "trailing payload");
}
struct Page { std::vector<ProjectionInterval> intervals; std::vector<ProjectionBoundary> boundaries; };
std::vector<Page> Pages(const ProjectionChain& c) {
  size_t count = c.page_directory.empty() ? 1 : c.page_directory.size(); std::vector<Page> out(count);
  size_t total = c.intervals.size() + c.boundaries.size(), interval_cursor = 0, boundary_cursor = 0;
  for (size_t page = 0; page < count; ++page) { size_t consumed = interval_cursor + boundary_cursor; size_t want = (total - consumed + count - page - 1) / (count - page);
    while (want && interval_cursor < c.intervals.size()) { out[page].intervals.push_back(c.intervals[interval_cursor++]); --want; }
    while (want && boundary_cursor < c.boundaries.size()) { out[page].boundaries.push_back(c.boundaries[boundary_cursor++]); --want; }
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
  if (codec != CompressionCodec::kNone && codec != CompressionCodec::kLz4) return Status::NotSupported("projection", "column codec is not a file codec");
  auto hr = ValidRange(c.header.entity_min, c.header.entity_max_exclusive, c.header.valid_from_min.value, c.header.valid_to_max); if (!hr.ok()) return hr;
  uint64_t previous_time = 0, previous_entity = 0;
  for (const auto& interval : c.intervals) {
    if (interval.effective.from.value < previous_time || interval.entity_id < previous_entity)
      return Status::InvalidArgument("projection", "non-monotonic interval columns");
    previous_time = interval.effective.from.value; previous_entity = interval.entity_id;
  }
  previous_time = previous_entity = 0;
  for (const auto& boundary : c.boundaries) {
    if (boundary.time.value < previous_time || boundary.entity_id < previous_entity)
      return Status::InvalidArgument("projection", "non-monotonic boundary columns");
    if (boundary.operation != FactOperation::kPut && boundary.operation != FactOperation::kDelete)
      return Status::InvalidArgument("projection", "unknown fact operation");
    previous_time = boundary.time.value; previous_entity = boundary.entity_id;
  }
  for (const auto& interval : c.intervals)
    if (interval.value.Encode().size() > UINT32_MAX)
      return Status::ResourceExhausted("projection", "typed value exceeds column limit");
  for (const auto& boundary : c.boundaries)
    if (boundary.value.Encode().size() > UINT32_MAX)
      return Status::ResourceExhausted("projection", "typed value exceeds column limit");
  auto pages = Pages(c); if (pages.size() > UINT32_MAX) return Status::ResourceExhausted("projection", "too many pages");
  std::string out("CDRPRJ1\0", 8); P32(&out, 1); out.push_back(char(c.header.kind)); out.push_back(char(codec));
  P64(&out, c.header.generation_id); P64(&out, c.header.base_seq.value); P32(&out, c.header.part_id.value); P16(&out, c.header.property_id.value); P32(&out, c.header.schema_epoch);
  P64(&out, c.header.entity_min); P64(&out, c.header.entity_max_exclusive); P64(&out, c.header.valid_from_min.value); out.push_back(char(c.header.valid_to_max ? 1 : 0)); P64(&out, c.header.valid_to_max ? c.header.valid_to_max->value : 0); P32(&out, uint32_t(pages.size())); P32(&out, crc32c::Value(out.data(), out.size()));
  std::vector<std::string> payloads; std::vector<ProjectionPageDirectoryEntry> dirs; size_t offset = kHeaderBytes + pages.size() * kDirectoryBytes;
  for (size_t i = 0; i < pages.size(); ++i) { if (pages[i].intervals.size() > UINT32_MAX || pages[i].boundaries.size() > UINT32_MAX) return Status::ResourceExhausted("projection", "too many page rows"); std::string raw; EncodePayload(&raw, pages[i].intervals, pages[i].boundaries); auto cp = CompressProjectionPayload(codec, raw); if (!cp.ok()) return cp.status();
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
  for (const auto& d : dirs) { P64(&out, d.offset); P32(&out, d.compressed_bytes); P32(&out, d.uncompressed_bytes); P32(&out, d.row_count); P64(&out, d.entity_min); P64(&out, d.entity_max_exclusive); P64(&out, d.valid_from_min.value); out.push_back(char(d.valid_to_max ? 1 : 0)); P64(&out, d.valid_to_max ? d.valid_to_max->value : 0); out.push_back(char(d.edge_type_min ? 1 : 0)); P64(&out, d.edge_type_min.value_or(0)); P64(&out, d.edge_type_max.value_or(0)); P32(&out, d.payload_crc32c); P64(&out, d.bloom_bits); out.push_back(char(d.bloom_hashes)); P64(&out, d.bloom_mask); }
  for (const auto& p : payloads) out.append(p); P32(&out, crc32c::Value(out.data(), out.size())); return out;
}

StatusOr<ProjectionChain> DecodeProjectionPageImpl(const std::string& b, size_t limit,
                                                   std::optional<size_t> only_page) {
  if (b.size() < kHeaderBytes + 4 || b.compare(0, 8, "CDRPRJ1\0", 8) != 0) return Status::Corruption("projection", "bad magic or truncated file");
  size_t p = 8; uint32_t version = 0; if (!G32(b, &p, &version)) return Status::Corruption("projection", "truncated version"); if (version != 1) return Status::NotSupported("projection", "unknown format version"); if (p + 2 > b.size()) return Status::Corruption("projection", "truncated header");
  auto kind = ProjectionKind(uint8_t(b[p++])); auto codec = CompressionCodec(uint8_t(b[p++])); if (uint8_t(kind) < 1 || uint8_t(kind) > 4) return Status::Corruption("projection", "invalid projection kind"); if (codec != CompressionCodec::kNone && codec != CompressionCodec::kLz4) return Status::NotSupported("projection", "column codec is not a file codec");
  ProjectionChain c; c.header.kind = kind; uint64_t gen = 0, base = 0, emin = 0, emax = 0, from = 0, to = 0; uint32_t part = 0, schema = 0, count = 0; uint16_t prop = 0;
  if (!G64(b, &p, &gen) || !G64(b, &p, &base) || !G32(b, &p, &part) || !G16(b, &p, &prop) || !G32(b, &p, &schema) || !G64(b, &p, &emin) || !G64(b, &p, &emax) || !G64(b, &p, &from) || p >= b.size()) return Status::Corruption("projection", "truncated header");
  uint8_t has = uint8_t(b[p++]); if (has > 1 || !G64(b, &p, &to) || !G32(b, &p, &count)) return Status::Corruption("projection", "invalid header flag"); auto hr = ValidRange(emin, emax, from, has ? std::optional<ValidTime>(ValidTime{to}) : std::nullopt); if (!hr.ok()) return hr;
  if (count > limit / kDirectoryBytes || count > (b.size() - kHeaderBytes) / kDirectoryBytes) return Status::ResourceExhausted("projection", "directory exceeds budget"); uint32_t hc = 0; if (!G32(b, &p, &hc) || p != kHeaderBytes || hc != crc32c::Value(b.data(), kHeaderBytes - 4)) return Status::Corruption("projection", "header CRC32C mismatch");
  c.header.generation_id = gen; c.header.base_seq = {base}; c.header.part_id = {part}; c.header.property_id = {prop}; c.header.schema_epoch = schema; c.header.entity_min = emin; c.header.entity_max_exclusive = emax; c.header.valid_from_min = {from}; c.header.valid_to_max = has ? std::optional<ValidTime>(ValidTime{to}) : std::nullopt; c.page_directory.reserve(count);
  for (uint32_t i = 0; i < count; ++i) { ProjectionPageDirectoryEntry d; uint8_t hp = 0, he = 0; uint64_t vf = 0, vt = 0, mn = 0, mx = 0; if (!G64(b, &p, &d.offset) || !G32(b, &p, &d.compressed_bytes) || !G32(b, &p, &d.uncompressed_bytes) || !G32(b, &p, &d.row_count) || !G64(b, &p, &d.entity_min) || !G64(b, &p, &d.entity_max_exclusive) || !G64(b, &p, &vf) || p >= b.size()) return Status::Corruption("projection", "truncated directory"); d.valid_from_min = {vf}; hp = uint8_t(b[p++]); if (hp > 1 || !G64(b, &p, &vt) || p >= b.size()) return Status::Corruption("projection", "invalid page flag"); he = uint8_t(b[p++]); if (he > 1 || !G64(b, &p, &mn) || !G64(b, &p, &mx) || !G32(b, &p, &d.payload_crc32c) || !G64(b, &p, &d.bloom_bits) || p >= b.size()) return Status::Corruption("projection", "truncated page metadata"); d.bloom_hashes = uint8_t(b[p++]); if (!G64(b, &p, &d.bloom_mask)) return Status::Corruption("projection", "truncated bloom metadata"); d.valid_to_max = hp ? std::optional<ValidTime>(ValidTime{vt}) : std::nullopt; if (he) { d.edge_type_min = mn; d.edge_type_max = mx; } auto vd = ValidPage(d); if (!vd.ok()) return vd; c.page_directory.push_back(d); }
  size_t start = kHeaderBytes + size_t(count) * kDirectoryBytes; auto valid = ValidDirectory(c.page_directory, start, b.size()); if (!valid.ok()) return valid; size_t fc_at = b.size() - 4; uint32_t fc = 0; if (!G32(b, &fc_at, &fc) || fc != crc32c::Value(b.data(), b.size() - 4)) return Status::Corruption("projection", "file CRC32C mismatch");
  size_t used_bytes = 0, rows = 0; for (size_t page_index = 0; page_index < c.page_directory.size(); ++page_index) { if (only_page && page_index != *only_page) continue; const auto& d = c.page_directory[page_index]; size_t offset = size_t(d.offset); if (d.payload_crc32c != crc32c::Value(b.data() + offset, d.compressed_bytes)) return Status::Corruption("projection", "page payload CRC32C mismatch"); if (d.uncompressed_bytes > limit - std::min(limit, used_bytes)) return Status::ResourceExhausted("projection", "decoded bytes exceed remaining budget"); size_t remaining = limit - used_bytes; if (d.compressed_bytes > remaining) return Status::ResourceExhausted("projection", "compressed payload exceeds remaining budget"); auto raw = DecompressProjectionPayload(codec, b.substr(offset, d.compressed_bytes), std::min<size_t>(d.uncompressed_bytes, remaining)); if (!raw.ok()) return raw.status(); if (raw.ValueOrDie().size() != d.uncompressed_bytes) return Status::Corruption("projection", "decoded length mismatch"); size_t before = rows; auto ds = DecodePayload(raw.ValueOrDie(), limit - std::min(limit, rows), &c); if (!ds.ok()) return ds; rows = c.intervals.size() + c.boundaries.size(); if (rows - before != d.row_count) return Status::Corruption("projection", "directory row count mismatch"); used_bytes += d.uncompressed_bytes; }
  return c;
}

StatusOr<ProjectionChain> DecodeProjectionPage(const std::string& bytes, size_t limit) {
  return DecodeProjectionPageImpl(bytes, limit, std::nullopt);
}

StatusOr<ProjectionChain> ReadProjectionPage(const std::string& bytes,
                                              size_t page_index,
                                              size_t limit) {
  auto decoded = DecodeProjectionPageImpl(bytes, limit, page_index);
  if (!decoded.ok()) return decoded.status();
  ProjectionChain all = decoded.ConsumeValueOrDie();
  if (page_index >= all.page_directory.size())
    return Status::InvalidArgument("projection", "page index out of range");
  ProjectionChain result;
  result.header = all.header;
  result.page_directory.push_back(all.page_directory[page_index]);
  result.intervals = std::move(all.intervals);
  result.boundaries = std::move(all.boundaries);
  return result;
}

bool PageMayContainEntity(const ProjectionPageDirectoryEntry& page,
                          uint64_t entity_id) {
  if (page.bloom_bits == 0 || page.bloom_mask == 0) return true;
  const uint64_t bits = std::min<uint64_t>(page.bloom_bits, 64);
  uint64_t hash = entity_id * 0x9e3779b97f4a7c15ULL;
  for (uint8_t i = 0; i < std::max<uint8_t>(page.bloom_hashes, 1); ++i) {
    const uint64_t bit = (i == 0) ? (entity_id % bits) : ((hash >> (i * 7 % 63)) % bits);
    if ((page.bloom_mask & (1ULL << bit)) == 0) return false;
    hash ^= hash >> 29;
  }
  return true;
}
}  // namespace cedar::internal
