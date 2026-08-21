#include "query/projection/projection_format.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "cedar/core/crc32c.h"

namespace cedar::internal {
namespace {
constexpr size_t kHeaderBytes = 81;
constexpr size_t kDirectoryBytes = 74;

void Put16(std::string* s, uint16_t v) { s->push_back(char(v)); s->push_back(char(v >> 8)); }
void Put32(std::string* s, uint32_t v) { for (unsigned i=0;i<4;++i) s->push_back(char(v>>(8*i))); }
void Put64(std::string* s, uint64_t v) { for (unsigned i=0;i<8;++i) s->push_back(char(v>>(8*i))); }
bool Get16(const std::string& s, size_t* p, uint16_t* v) {
  if (*p > s.size() || s.size()-*p < 2) return false;
  *v = uint16_t(uint8_t(s[*p])) | uint16_t(uint8_t(s[*p+1])) << 8; *p += 2; return true;
}
bool Get32(const std::string& s, size_t* p, uint32_t* v) {
  if (*p > s.size() || s.size()-*p < 4) return false; *v=0;
  for (unsigned i=0;i<4;++i) *v |= uint32_t(uint8_t(s[*p+i])) << (8*i); *p += 4; return true;
}
bool Get64(const std::string& s, size_t* p, uint64_t* v) {
  if (*p > s.size() || s.size()-*p < 8) return false; *v=0;
  for (unsigned i=0;i<8;++i) *v |= uint64_t(uint8_t(s[*p+i])) << (8*i); *p += 8; return true;
}
void PutValue(std::string* out, const Value& value) {
  const std::string encoded = value.Encode(); Put32(out, uint32_t(encoded.size())); out->append(encoded);
}
bool GetValue(const std::string& in, size_t* p, Value* value) {
  uint32_t n=0; if (!Get32(in,p,&n) || n > in.size()-*p) return false;
  auto decoded = Value::Decode(in.substr(*p,n)); if (!decoded) return false; *p += n; *value=*decoded; return true;
}
void EncodePayload(std::string* out, const std::vector<ProjectionInterval>& intervals,
                   const std::vector<ProjectionBoundary>& boundaries) {
  Put32(out,uint32_t(intervals.size())); uint64_t prev=0;
  for (const auto& i: intervals) { Put64(out,i.effective.from.value-prev); prev=i.effective.from.value;
    out->push_back(i.effective.to?1:0); Put64(out,i.effective.to?i.effective.to->value:0); PutValue(out,i.value); }
  Put32(out,uint32_t(boundaries.size())); prev=0;
  for (const auto& b: boundaries) { Put64(out,b.time.value-prev); prev=b.time.value; out->push_back(char(b.operation)); PutValue(out,b.value); }
}
Status DecodePayload(const std::string& in, size_t row_limit, ProjectionChain* c) {
  size_t p=0; uint32_t ni=0; if (!Get32(in,&p,&ni) || ni>row_limit) return Status::Corruption("projection","invalid interval count");
  uint64_t prev=0;
  for (uint32_t n=0;n<ni;++n) { uint64_t d=0,t=0; if(!Get64(in,&p,&d)||d>std::numeric_limits<uint64_t>::max()-prev||p>=in.size()) return Status::Corruption("projection","invalid interval"); prev+=d; bool has=in[p++]!=0; if(!Get64(in,&p,&t)|| (has&&t<prev)) return Status::Corruption("projection","invalid interval range"); Value v=Value::Int64(0); if(!GetValue(in,&p,&v)) return Status::Corruption("projection","invalid interval value"); c->intervals.push_back({{ValidTime{prev},has?std::optional<ValidTime>(ValidTime{t}):std::nullopt},v}); }
  uint32_t nb=0; if(!Get32(in,&p,&nb)||nb>row_limit-ni) return Status::Corruption("projection","invalid boundary count"); prev=0;
  for(uint32_t n=0;n<nb;++n){uint64_t d=0;if(!Get64(in,&p,&d)||d>std::numeric_limits<uint64_t>::max()-prev||p>=in.size())return Status::Corruption("projection","invalid boundary");prev+=d;auto op=FactOperation(uint8_t(in[p++]));if(op!=FactOperation::kPut&&op!=FactOperation::kDelete)return Status::Corruption("projection","invalid operation");Value v=Value::Int64(0);if(!GetValue(in,&p,&v))return Status::Corruption("projection","invalid boundary value");c->boundaries.push_back({ValidTime{prev},op,v});}
  if(p!=in.size())return Status::Corruption("projection","trailing payload"); return Status::OK();
}
struct Page { std::vector<ProjectionInterval> intervals; std::vector<ProjectionBoundary> boundaries; };
std::vector<Page> MakePages(const ProjectionChain& c) {
  size_t count=c.page_directory.empty()?1:c.page_directory.size(); std::vector<Page> pages(count);
  const size_t total=c.intervals.size()+c.boundaries.size(); size_t cursor=0;
  for(size_t page=0;page<count;++page){size_t wanted=(total-cursor+count-page-1)/(count-page); while(wanted&&cursor<c.intervals.size()){pages[page].intervals.push_back(c.intervals[cursor++]);--wanted;} while(wanted&&cursor<total){pages[page].boundaries.push_back(c.boundaries[cursor++-c.intervals.size()]);--wanted;}}
  return pages;
}
Status ValidateDirectory(const std::vector<ProjectionPageDirectoryEntry>& d,size_t start,size_t size){uint64_t end=start;for(const auto& p:d){if(p.offset<start||p.offset<end||p.offset>size-4||p.compressed_bytes>size-static_cast<size_t>(p.offset)-4)return Status::Corruption("projection","malformed page directory");end=p.offset+p.compressed_bytes;}return end>size-4?Status::Corruption("projection","page exceeds file"):Status::OK();}
}  // namespace

StatusOr<std::string> EncodeProjectionPage(const ProjectionChain& c, CompressionCodec codec) {
  if (uint8_t(c.header.kind)<1||uint8_t(c.header.kind)>4) return Status::InvalidArgument("projection","invalid projection kind");
  if (uint8_t(codec)>uint8_t(CompressionCodec::kRle)) return Status::NotSupported("projection","unknown compression codec");
  auto pages=MakePages(c); if(pages.size()>UINT32_MAX)return Status::ResourceExhausted("projection","too many pages");
  std::string out("CDRPRJ1\0",8); Put32(&out,1); out.push_back(char(c.header.kind)); out.push_back(char(codec)); Put64(&out,c.header.generation_id); Put64(&out,c.header.base_seq.value); Put32(&out,c.header.part_id.value); Put16(&out,c.header.property_id.value); Put32(&out,c.header.schema_epoch); Put64(&out,c.header.entity_min); Put64(&out,c.header.entity_max_exclusive); Put64(&out,c.header.valid_from_min.value); out.push_back(c.header.valid_to_max?1:0); Put64(&out,c.header.valid_to_max?c.header.valid_to_max->value:0); Put32(&out,uint32_t(pages.size())); Put32(&out,crc32c::Value(out.data(),out.size()));
  std::vector<std::string> payloads; std::vector<ProjectionPageDirectoryEntry> directory; size_t offset=kHeaderBytes+pages.size()*kDirectoryBytes;
  for(const auto& page:pages){std::string raw;EncodePayload(&raw,page.intervals,page.boundaries);auto cp=CompressProjectionPayload(codec,raw);if(!cp.ok())return cp.status();if(raw.size()>UINT32_MAX||cp.ValueOrDie().size()>UINT32_MAX)return Status::ResourceExhausted("projection","page too large");ProjectionPageDirectoryEntry d;d.offset=offset;d.compressed_bytes=uint32_t(cp.ValueOrDie().size());d.uncompressed_bytes=uint32_t(raw.size());d.row_count=uint32_t(page.intervals.size()+page.boundaries.size());d.entity_min=c.header.entity_min;d.entity_max_exclusive=c.header.entity_max_exclusive;d.valid_from_min=c.header.valid_from_min;d.valid_to_max=c.header.valid_to_max;d.payload_crc32c=crc32c::Value(cp.ValueOrDie().data(),cp.ValueOrDie().size());directory.push_back(d);offset+=cp.ValueOrDie().size();payloads.push_back(cp.ConsumeValueOrDie());}
  for(const auto& d:directory){Put64(&out,d.offset);Put32(&out,d.compressed_bytes);Put32(&out,d.uncompressed_bytes);Put32(&out,d.row_count);Put64(&out,d.entity_min);Put64(&out,d.entity_max_exclusive);Put64(&out,d.valid_from_min.value);out.push_back(d.valid_to_max?1:0);Put64(&out,d.valid_to_max?d.valid_to_max->value:0);out.push_back(d.edge_type_min?1:0);Put64(&out,d.edge_type_min.value_or(0));Put64(&out,d.edge_type_max.value_or(0));Put32(&out,d.payload_crc32c);}
  for(const auto& p:payloads)out.append(p);Put32(&out,crc32c::Value(out.data(),out.size()));return out;
}

StatusOr<ProjectionChain> DecodeProjectionPage(const std::string& bytes,size_t limit) {
  if(bytes.size()<kHeaderBytes+4||bytes.compare(0,8,"CDRPRJ1\0",8)!=0)return Status::Corruption("projection","bad magic or truncated file");size_t p=8;uint32_t version=0;if(!Get32(bytes,&p,&version))return Status::Corruption("projection","truncated version");if(version!=1)return Status::NotSupported("projection","unknown format version");if(p+2>bytes.size())return Status::Corruption("projection","truncated header");auto kind=ProjectionKind(uint8_t(bytes[p++]));auto codec=CompressionCodec(uint8_t(bytes[p++]));if(uint8_t(kind)<1||uint8_t(kind)>4)return Status::Corruption("projection","invalid projection kind");if(uint8_t(codec)>uint8_t(CompressionCodec::kRle))return Status::NotSupported("projection","unknown compression codec");ProjectionChain c;c.header.kind=kind;uint64_t gen=0,base=0,emin=0,emax=0,vfrom=0,vto=0;uint32_t part=0,schema=0,count=0;uint16_t prop=0;if(!Get64(bytes,&p,&gen)||!Get64(bytes,&p,&base)||!Get32(bytes,&p,&part)||!Get16(bytes,&p,&prop)||!Get32(bytes,&p,&schema)||!Get64(bytes,&p,&emin)||!Get64(bytes,&p,&emax)||!Get64(bytes,&p,&vfrom)||p>=bytes.size())return Status::Corruption("projection","truncated header");uint8_t has=uint8_t(bytes[p++]);if(has>1||!Get64(bytes,&p,&vto)||!Get32(bytes,&p,&count))return Status::Corruption("projection","invalid header ranges");if(count>limit/kDirectoryBytes||count>(bytes.size()-kHeaderBytes)/kDirectoryBytes)return Status::ResourceExhausted("projection","directory exceeds budget");uint32_t hcrc=0;if(!Get32(bytes,&p,&hcrc)||p!=kHeaderBytes||hcrc!=crc32c::Value(bytes.data(),kHeaderBytes-4))return Status::Corruption("projection","header CRC32C mismatch");c.header.generation_id=gen;c.header.base_seq={base};c.header.part_id={part};c.header.property_id={prop};c.header.schema_epoch=schema;c.header.entity_min=emin;c.header.entity_max_exclusive=emax;c.header.valid_from_min={vfrom};c.header.valid_to_max=has?std::optional<ValidTime>(ValidTime{vto}):std::nullopt;c.page_directory.reserve(count);
  for(uint32_t i=0;i<count;++i){ProjectionPageDirectoryEntry d;uint8_t ph=0,eh=0;uint64_t edge_min=0,edge_max=0;if(!Get64(bytes,&p,&d.offset)||!Get32(bytes,&p,&d.compressed_bytes)||!Get32(bytes,&p,&d.uncompressed_bytes)||!Get32(bytes,&p,&d.row_count)||!Get64(bytes,&p,&d.entity_min)||!Get64(bytes,&p,&d.entity_max_exclusive)||!Get64(bytes,&p,&d.valid_from_min.value)||p>=bytes.size())return Status::Corruption("projection","truncated directory");ph=uint8_t(bytes[p++]);if(ph>1||!Get64(bytes,&p,&vto)||p>=bytes.size())return Status::Corruption("projection","invalid page range");eh=uint8_t(bytes[p++]);if(eh>1||!Get64(bytes,&p,&edge_min)||!Get64(bytes,&p,&edge_max)||!Get32(bytes,&p,&d.payload_crc32c))return Status::Corruption("projection","truncated directory");d.valid_to_max=ph?std::optional<ValidTime>(ValidTime{vto}):std::nullopt;if(eh){d.edge_type_min=edge_min;d.edge_type_max=edge_max;}c.page_directory.push_back(d);}
  const size_t start=kHeaderBytes+size_t(count)*kDirectoryBytes;if(start>bytes.size()-4)return Status::Corruption("projection","directory outside file");size_t q=bytes.size()-4;uint32_t fcrc=0;if(!Get32(bytes,&q,&fcrc)||fcrc!=crc32c::Value(bytes.data(),bytes.size()-4))return Status::Corruption("projection","file CRC32C mismatch");auto valid=ValidateDirectory(c.page_directory,start,bytes.size());if(!valid.ok())return valid;
  size_t rows=0;for(const auto& d:c.page_directory){if(d.offset>bytes.size()-4||d.compressed_bytes>bytes.size()-size_t(d.offset)-4||d.uncompressed_bytes>limit-std::min(limit,rows)||d.payload_crc32c!=crc32c::Value(bytes.data()+d.offset,d.compressed_bytes))return Status::Corruption("projection","invalid page payload");auto raw=DecompressProjectionPayload(codec,bytes.substr(size_t(d.offset),d.compressed_bytes),d.uncompressed_bytes);if(!raw.ok())return raw.status();if(raw.ValueOrDie().size()!=d.uncompressed_bytes)return Status::Corruption("projection","decoded length mismatch");auto status=DecodePayload(raw.ValueOrDie(),limit-std::min(limit,rows),&c);if(!status.ok())return status;rows=c.intervals.size()+c.boundaries.size();if(rows>limit)return Status::ResourceExhausted("projection","row budget exceeded");}return c;
}
}  // namespace cedar::internal
