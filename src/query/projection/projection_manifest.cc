#include "query/projection/projection_manifest.h"

#include <algorithm>
#include <cstring>
#include <set>

#include "cedar/core/crc32c.h"

namespace cedar::internal {
namespace {
constexpr uint32_t kMagic = 0x31464d43;  // CMF1
void P16(std::string* s, uint16_t v) { for (int i = 0; i < 2; ++i) s->push_back(char(v >> (i * 8))); }
void P32(std::string* s, uint32_t v) { for (int i = 0; i < 4; ++i) s->push_back(char(v >> (i * 8))); }
void P64(std::string* s, uint64_t v) { for (int i = 0; i < 8; ++i) s->push_back(char(v >> (i * 8))); }
bool G16(const std::string& s, size_t* p, uint16_t* v) { if (*p > s.size() || s.size() - *p < 2) return false; *v = uint8_t(s[*p]) | uint16_t(uint8_t(s[*p + 1])) << 8; *p += 2; return true; }
bool G32(const std::string& s, size_t* p, uint32_t* v) { if (*p > s.size() || s.size() - *p < 4) return false; *v = 0; for (int i = 0; i < 4; ++i) *v |= uint32_t(uint8_t(s[*p + i])) << (i * 8); *p += 4; return true; }
bool G64(const std::string& s, size_t* p, uint64_t* v) { if (*p > s.size() || s.size() - *p < 8) return false; *v = 0; for (int i = 0; i < 8; ++i) *v |= uint64_t(uint8_t(s[*p + i])) << (i * 8); *p += 8; return true; }
void PutString(std::string* s, const std::string& v) { P32(s, uint32_t(v.size())); s->append(v); }
bool GetString(const std::string& s, size_t* p, std::string* v) { uint32_t n = 0; if (!G32(s, p, &n) || n > s.size() - *p) return false; *v = s.substr(*p, n); *p += n; return true; }
void PutHeader(std::string* s, const ProjectionHeader& h) { s->push_back(char(h.kind)); P64(s, h.generation_id); P64(s, h.base_seq.value); P32(s, h.part_id.value); P16(s, h.property_id.value); P32(s, h.schema_epoch); P64(s, h.entity_min); P64(s, h.entity_max_exclusive); P64(s, h.valid_from_min.value); s->push_back(char(h.valid_to_max.has_value())); P64(s, h.valid_to_max ? h.valid_to_max->value : 0); }
bool GetHeader(const std::string& s, size_t* p, ProjectionHeader* h) { uint8_t kind = 0; uint64_t x = 0; uint32_t p32 = 0; uint16_t p16 = 0; if (*p >= s.size()) return false; kind = uint8_t(s[(*p)++]); h->kind = ProjectionKind(kind); if (!G64(s,p,&h->generation_id) || !G64(s,p,&h->base_seq.value) || !G32(s,p,&p32) || !G16(s,p,&p16) || !G32(s,p,&h->schema_epoch) || !G64(s,p,&h->entity_min) || !G64(s,p,&h->entity_max_exclusive) || !G64(s,p,&h->valid_from_min.value) || *p >= s.size()) return false; h->part_id = PartId{p32}; h->property_id = PropertyId{p16}; uint8_t has = uint8_t(s[(*p)++]); if (has > 1 || !G64(s,p,&x)) return false; h->valid_to_max = has ? std::optional<ValidTime>(ValidTime{x}) : std::nullopt; return true; }
}  // namespace

Status ValidateProjectionManifest(const ProjectionManifest& m, const std::string& identity) {
  if (m.database_identity != identity || m.database_identity.empty()) return Status::IdentityConflict("projection manifest", "database identity mismatch");
  if (m.generation_id == 0) return Status::Corruption("projection manifest", "zero generation");
  std::set<std::string> ids;
  for (const auto& r : m.regions) {
    if (uint8_t(r.kind) < 1 || uint8_t(r.kind) > 4 || r.entity_max_exclusive <= r.entity_min || !r.valid_time.Validate().ok()) return Status::Corruption("projection manifest", "invalid coverage range");
    std::vector<std::pair<uint64_t,uint64_t>> ranges;
    for (const auto& s : r.segments) {
      if (s.segment_id.empty() || !ids.insert(s.segment_id).second || s.filename.empty() || s.filename.find("..") != std::string::npos || s.filename.front() == '/') return Status::Corruption("projection manifest", "invalid or duplicate segment");
      if (s.header.entity_max_exclusive <= s.header.entity_min) return Status::Corruption("projection manifest", "invalid segment range");
      ranges.emplace_back(s.header.entity_min, s.header.entity_max_exclusive);
    }
    std::sort(ranges.begin(), ranges.end());
    for (size_t i = 1; i < ranges.size(); ++i) if (ranges[i-1].second > ranges[i].first) return Status::Corruption("projection manifest", "overlapping segment coverage");
  }
  auto times_overlap = [](const ValidTimeInterval& a, const ValidTimeInterval& b) {
    const uint64_t a_to = a.to ? a.to->value : UINT64_MAX;
    const uint64_t b_to = b.to ? b.to->value : UINT64_MAX;
    return a.from.value < b_to && b.from.value < a_to;
  };
  for (size_t i = 0; i < m.regions.size(); ++i) {
    for (size_t j = i + 1; j < m.regions.size(); ++j) {
      const auto& a = m.regions[i];
      const auto& b = m.regions[j];
      if (a.kind != b.kind || a.part_id != b.part_id ||
          a.property_id != b.property_id || a.schema_epoch != b.schema_epoch)
        continue;
      if (a.entity_min < b.entity_max_exclusive &&
          b.entity_min < a.entity_max_exclusive && times_overlap(a.valid_time, b.valid_time))
        return Status::Corruption("projection manifest", "overlapping coverage regions");
    }
  }
  return Status::OK();
}

StatusOr<std::string> EncodeProjectionManifest(const ProjectionManifest& m) {
  if (m.database_identity.empty()) return Status::InvalidArgument("projection manifest", "missing database identity");
  std::string out; P32(&out, kMagic); P32(&out, 1); PutString(&out, m.database_identity); P64(&out, m.generation_id); P64(&out, m.base_seq.value); P32(&out, uint32_t(m.schema_fingerprints.size())); for (const auto& s : m.schema_fingerprints) PutString(&out, s); P32(&out, uint32_t(m.regions.size()));
  for (const auto& r : m.regions) { out.push_back(char(r.kind)); P32(&out, r.part_id.value); out.push_back(char(r.property_id.has_value())); P16(&out, r.property_id ? r.property_id->value : 0); P32(&out, r.schema_epoch); P64(&out, r.entity_min); P64(&out, r.entity_max_exclusive); P64(&out, r.valid_time.from.value); out.push_back(char(r.valid_time.to.has_value())); P64(&out, r.valid_time.to ? r.valid_time.to->value : 0); P32(&out, uint32_t(r.segments.size())); for (const auto& s : r.segments) { PutString(&out, s.segment_id); PutString(&out, s.filename); PutHeader(&out, s.header); P64(&out, s.file_bytes); P32(&out, s.checksum); } }
  P32(&out, crc32c::Value(out.data(), out.size())); return out;
}

StatusOr<ProjectionManifest> DecodeProjectionManifest(const std::string& in, const std::string& identity) {
  if (in.size() < 12) return Status::Corruption("projection manifest", "truncated"); size_t p = 0; uint32_t magic = 0, version = 0, count = 0; ProjectionManifest m; if (!G32(in,&p,&magic) || !G32(in,&p,&version) || magic != kMagic || version != 1 || !GetString(in,&p,&m.database_identity) || !G64(in,&p,&m.generation_id) || !G64(in,&p,&m.base_seq.value) || !G32(in,&p,&count) || count > 100000) return Status::Corruption("projection manifest", "invalid header"); for (uint32_t i=0;i<count;++i) { std::string v; if (!GetString(in,&p,&v)) return Status::Corruption("projection manifest", "invalid schema fingerprint"); m.schema_fingerprints.push_back(std::move(v)); } if (!G32(in,&p,&count) || count > 100000) return Status::Corruption("projection manifest", "invalid regions"); for (uint32_t i=0;i<count;++i) { CoverageRegion r; uint8_t kind=0, has=0; uint32_t part=0, epoch=0, n=0; uint16_t prop=0; uint64_t x=0; if (p>=in.size()) return Status::Corruption("projection manifest", "truncated region"); kind=uint8_t(in[p++]); r.kind=ProjectionKind(kind); if (!G32(in,&p,&part) || p>=in.size()) return Status::Corruption("projection manifest", "invalid region"); r.part_id=PartId{part}; has=uint8_t(in[p++]); if (has>1 || !G16(in,&p,&prop) || !G32(in,&p,&epoch) || !G64(in,&p,&r.entity_min) || !G64(in,&p,&r.entity_max_exclusive) || !G64(in,&p,&r.valid_time.from.value) || p>=in.size()) return Status::Corruption("projection manifest", "invalid region"); r.property_id=has ? std::optional<PropertyId>(PropertyId{prop}) : std::nullopt; r.schema_epoch=epoch; has=uint8_t(in[p++]); if (has>1 || !G64(in,&p,&x)) return Status::Corruption("projection manifest", "invalid region time"); r.valid_time.to=has ? std::optional<ValidTime>(ValidTime{x}) : std::nullopt; if (!G32(in,&p,&n) || n>100000) return Status::Corruption("projection manifest", "invalid segments"); for (uint32_t j=0;j<n;++j) { SegmentDescriptor s; if (!GetString(in,&p,&s.segment_id) || !GetString(in,&p,&s.filename) || !GetHeader(in,&p,&s.header) || !G64(in,&p,&s.file_bytes) || !G32(in,&p,&s.checksum)) return Status::Corruption("projection manifest", "invalid segment"); r.segments.push_back(std::move(s)); } m.regions.push_back(std::move(r)); }
  if (p > in.size() || in.size() - p != 4) return Status::Corruption("projection manifest", "trailing manifest bytes");
  const uint32_t expected = crc32c::Value(in.data(), p);
  uint32_t checksum=0; if (!G32(in,&p,&checksum) || expected != checksum) return Status::Corruption("projection manifest", "checksum mismatch"); auto valid=ValidateProjectionManifest(m,identity); if (!valid.ok()) return valid; return m;
}
}  // namespace cedar::internal
