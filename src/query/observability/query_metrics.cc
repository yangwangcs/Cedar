// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/observability/query_metrics.h"

#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <cstring>
#include <cmath>
#include <sys/stat.h>
#include <unistd.h>

#include "cedar/core/crc32c.h"
#include "query/projection/projection_manifest.h"

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
  *value = 0; for (int i = 0; i < 4; ++i) *value |= uint32_t(uint8_t(in[*p + i])) << (i * 8); *p += 4; return true;
}
bool Get64(const std::string& in, size_t* p, uint64_t* value) {
  if (*p > in.size() || in.size() - *p < 8) return false;
  *value = 0; for (int i = 0; i < 8; ++i) *value |= uint64_t(uint8_t(in[*p + i])) << (i * 8); *p += 8; return true;
}
bool GetBytes(const std::string& in, size_t* p, std::string* value) {
  uint32_t n = 0; if (!Get32(in, p, &n) || n > in.size() - *p) return false;
  value->assign(in.data() + *p, n); *p += n; return true;
}
void PutBytes(std::string* out, const std::string& value) {
  Put32(out, static_cast<uint32_t>(value.size())); out->append(value);
}
Status ValidateBounds(const QueryStatisticsSnapshot& snapshot) {
  if (snapshot.database_identity.size() > 4096 || snapshot.schema_fingerprint.size() > 4096 ||
      snapshot.columns.size() > 4096) return Status::ResourceExhausted("query statistics", "metadata exceeds bound");
  for (const auto& c : snapshot.columns) {
    if (c.distinct.precision > 20 || c.distinct.registers.size() != (size_t{1} << c.distinct.precision) ||
        c.histogram.size() > 128 || c.top_values.size() > 64 || c.fanout.size() > 128 ||
        c.interval_length.size() > 128) return Status::ResourceExhausted("query statistics", "sketch exceeds bound");
    auto valid_quantiles = [](const QuantileSummary& values) {
      double prior = -1.0;
      for (const auto& point : values) {
        if (!std::isfinite(point.quantile) || point.quantile < 0.0 ||
            point.quantile > 1.0 || point.quantile < prior) return false;
        prior = point.quantile;
      }
      return true;
    };
    if (!valid_quantiles(c.fanout) || !valid_quantiles(c.interval_length))
      return Status::Corruption("query statistics", "invalid quantile summary");
  }
  return Status::OK();
}

Status SyncPath(const std::filesystem::path& path, bool directory) {
  const int flags = O_RDONLY | (directory ? O_DIRECTORY : 0);
  const int fd = ::open(path.c_str(), flags);
  if (fd < 0) return Status::IOError("query statistics", "open for sync failed");
  const int result = ::fsync(fd);
  ::close(fd);
  return result == 0 ? Status::OK() : Status::IOError("query statistics", "sync failed");
}
}

StatusOr<std::string> EncodeQueryStatistics(const QueryStatisticsSnapshot& snapshot) {
  const Status valid = ValidateBounds(snapshot); if (!valid.ok()) return valid;
  std::string out("CDRSTS1\0", 8); PutBytes(&out, snapshot.database_identity); PutBytes(&out, snapshot.schema_fingerprint);
  Put64(&out, snapshot.generation_id); Put64(&out, snapshot.base_seq.value); out.push_back(static_cast<char>(snapshot.complete)); Put32(&out, static_cast<uint32_t>(snapshot.columns.size()));
  for (const auto& c : snapshot.columns) {
    Put64(&out, c.rows); Put64(&out, c.pages); Put64(&out, c.bytes); Put64(&out, c.interval_count); Put64(&out, c.edge_count);
    out.push_back(static_cast<char>(c.entity_range.has_value())); if (c.entity_range) { Put64(&out, c.entity_range->min); Put64(&out, c.entity_range->max_exclusive); }
    out.push_back(static_cast<char>(c.valid_time_range.has_value())); if (c.valid_time_range) { Put64(&out, c.valid_time_range->from.value); out.push_back(static_cast<char>(c.valid_time_range->to.has_value())); if (c.valid_time_range->to) Put64(&out, c.valid_time_range->to->value); }
    out.push_back(static_cast<char>(c.distinct.precision)); Put32(&out, static_cast<uint32_t>(c.distinct.registers.size())); out.append(reinterpret_cast<const char*>(c.distinct.registers.data()), c.distinct.registers.size());
    Put32(&out, static_cast<uint32_t>(c.histogram.size())); for (const auto& b : c.histogram) { PutBytes(&out, b.upper_bound.Encode()); Put64(&out, b.cumulative_count); }
    Put32(&out, static_cast<uint32_t>(c.top_values.size())); for (const auto& t : c.top_values) { PutBytes(&out, t.value.Encode()); Put64(&out, t.estimated_count); }
    Put32(&out, static_cast<uint32_t>(c.fanout.size())); for (const auto& q : c.fanout) { uint64_t bits = 0; std::memcpy(&bits, &q.quantile, sizeof(bits)); Put64(&out, bits); Put64(&out, q.value); }
    Put32(&out, static_cast<uint32_t>(c.interval_length.size())); for (const auto& q : c.interval_length) { uint64_t bits = 0; std::memcpy(&bits, &q.quantile, sizeof(bits)); Put64(&out, bits); Put64(&out, q.value); }
  }
  Put32(&out, crc32c::Value(out.data(), out.size())); return out;
}

StatusOr<QueryStatisticsSnapshot> DecodeQueryStatistics(const std::string& in) {
  if (in.size() < 12 || in.compare(0, 8, "CDRSTS1\0", 8) != 0) return Status::Corruption("query statistics", "invalid header");
  uint32_t expected = 0; const size_t payload_end = in.size() - 4; size_t crc_at = payload_end; if (!Get32(in, &crc_at, &expected) || expected != crc32c::Value(in.data(), payload_end)) return Status::Corruption("query statistics", "checksum mismatch");
  size_t p = 8; QueryStatisticsSnapshot snapshot; if (!GetBytes(in, &p, &snapshot.database_identity) || !GetBytes(in, &p, &snapshot.schema_fingerprint) || !Get64(in, &p, &snapshot.generation_id) || !Get64(in, &p, &snapshot.base_seq.value)) return Status::Corruption("query statistics", "truncated identity");
  if (p >= in.size()) return Status::Corruption("query statistics", "truncated completeness flag");
  const uint8_t complete = uint8_t(in[p++]); if (complete > 1) return Status::Corruption("query statistics", "invalid completeness flag"); snapshot.complete = complete != 0;
  uint32_t count = 0; if (!Get32(in, &p, &count) || count > 4096) return Status::ResourceExhausted("query statistics", "column count exceeds bound"); snapshot.columns.resize(count);
  for (auto& c : snapshot.columns) {
    if (!Get64(in,&p,&c.rows)||!Get64(in,&p,&c.pages)||!Get64(in,&p,&c.bytes)||!Get64(in,&p,&c.interval_count)||!Get64(in,&p,&c.edge_count)||p>=in.size()) return Status::Corruption("query statistics", "truncated column");
    uint8_t has = uint8_t(in[p++]); if (has > 1) return Status::Corruption("query statistics", "invalid entity flag"); if (has) { EntityRange r; if (!Get64(in,&p,&r.min)||!Get64(in,&p,&r.max_exclusive)) return Status::Corruption("query statistics", "truncated entity range"); c.entity_range=r; }
    if (p>=in.size()) return Status::Corruption("query statistics", "truncated time flag"); has=uint8_t(in[p++]); if (has>1) return Status::Corruption("query statistics", "invalid time flag"); if(has){ ValidTimeInterval t; if(!Get64(in,&p,&t.from.value)||p>=in.size()) return Status::Corruption("query statistics", "truncated time range"); uint8_t to=uint8_t(in[p++]); if(to>1)return Status::Corruption("query statistics", "invalid time bound"); if(to){uint64_t v;if(!Get64(in,&p,&v))return Status::Corruption("query statistics", "truncated time bound");t.to=ValidTime{v};} c.valid_time_range=t; }
    if(p>=in.size())return Status::Corruption("query statistics", "truncated sketch"); c.distinct.precision=uint8_t(in[p++]); uint32_t n=0;if(!Get32(in,&p,&n)||n>(1u<<20)||n>in.size()-p)return Status::ResourceExhausted("query statistics", "sketch exceeds bound");c.distinct.registers.assign(in.begin()+p,in.begin()+p+n);p+=n;
    if(!Get32(in,&p,&n)||n>128)return Status::ResourceExhausted("query statistics", "histogram exceeds bound");for(uint32_t i=0;i<n;++i){std::string v;if(!GetBytes(in,&p,&v)||!Get64(in,&p,&c.histogram.emplace_back().cumulative_count))return Status::Corruption("query statistics", "truncated histogram");auto decoded=Value::Decode(v);if(!decoded)return Status::Corruption("query statistics", "invalid histogram value");c.histogram.back().upper_bound=*decoded;}
    if(!Get32(in,&p,&n)||n>64)return Status::ResourceExhausted("query statistics", "top values exceeds bound");for(uint32_t i=0;i<n;++i){std::string v;if(!GetBytes(in,&p,&v)||!Get64(in,&p,&c.top_values.emplace_back().estimated_count))return Status::Corruption("query statistics", "truncated top values");auto decoded=Value::Decode(v);if(!decoded)return Status::Corruption("query statistics", "invalid top value");c.top_values.back().value=*decoded;}
    auto decode_q=[&](QuantileSummary* q)->Status{uint32_t m=0;if(!Get32(in,&p,&m)||m>128)return Status::ResourceExhausted("query statistics", "quantile summary exceeds bound");for(uint32_t i=0;i<m;++i){uint64_t bits=0,v=0;if(!Get64(in,&p,&bits)||!Get64(in,&p,&v))return Status::Corruption("query statistics", "truncated quantile");QuantilePoint point;std::memcpy(&point.quantile,&bits,sizeof(bits));point.value=v;q->push_back(point);}return Status::OK();};
    if(!decode_q(&c.fanout).ok()||!decode_q(&c.interval_length).ok())return Status::Corruption("query statistics", "truncated quantile summary");
  }
  snapshot.checksum=expected; if(p!=payload_end)return Status::Corruption("query statistics", "trailing payload");
  const Status valid = ValidateBounds(snapshot); if (!valid.ok()) return valid;
  return snapshot;
}

std::string QueryStatisticsStore::FileName(uint64_t generation_id) { return "generation-" + std::to_string(generation_id) + ".cstats"; }
QueryStatisticsStore::QueryStatisticsStore(std::string directory, std::string database_identity):directory_(std::move(directory)),database_identity_(std::move(database_identity)){}
Status QueryStatisticsStore::Refresh(const ProjectionManifest& manifest, const std::string& schema_fingerprint) {
  QueryStatisticsSnapshot snapshot; snapshot.database_identity=database_identity_; snapshot.schema_fingerprint=schema_fingerprint; snapshot.generation_id=manifest.generation_id; snapshot.base_seq=manifest.base_seq;
  snapshot.columns.reserve(manifest.regions.size()); for(const auto& region:manifest.regions){QueryColumnStatistics c;c.distinct.registers.assign(size_t{1} << c.distinct.precision, 0);for(const auto& segment:region.segments){c.pages++;c.bytes+=segment.file_bytes;c.rows+=segment.header.entity_max_exclusive-segment.header.entity_min;}c.interval_count=c.rows;c.edge_count=(region.kind==ProjectionKind::kAdjacency?c.rows:0);c.entity_range=EntityRange{region.entity_min,region.entity_max_exclusive};c.valid_time_range=region.valid_time;if(c.rows!=0){c.histogram.push_back({Value::Int64(static_cast<int64_t>(c.rows)),c.rows});c.fanout.push_back({0.5,c.edge_count});c.interval_length.push_back({0.5,region.valid_time.to?region.valid_time.to->value-region.valid_time.from.value:0});c.top_values.push_back({Value::Int64(static_cast<int64_t>(region.entity_min)),c.rows});}snapshot.columns.push_back(std::move(c));} snapshot.complete=!snapshot.columns.empty()&&!schema_fingerprint.empty();
  auto encoded=EncodeQueryStatistics(snapshot);if(!encoded.ok())return encoded.status();std::error_code ec;std::filesystem::create_directories(directory_,ec);if(ec)return Status::IOError("query statistics",ec.message());const auto path=std::filesystem::path(directory_)/FileName(manifest.generation_id);const auto temp=path.string()+".tmp";{std::ofstream out(temp,std::ios::binary|std::ios::trunc);if(!out)return Status::IOError("query statistics","cannot create file");out.write(encoded.ValueOrDie().data(),encoded.ValueOrDie().size());out.flush();if(!out)return Status::IOError("query statistics","write failed");}auto synced=SyncPath(temp,false);if(!synced.ok()){std::filesystem::remove(temp);return synced;}std::filesystem::rename(temp,path,ec);if(ec){std::filesystem::remove(temp);return Status::IOError("query statistics",ec.message());}if(!SyncPath(directory_,true).ok())return Status::IOError("query statistics","statistics directory sync failed");
  std::string current("CSC1", 4); Put64(&current, manifest.generation_id); Put32(&current, crc32c::Value(current.data(), current.size()));
  const auto current_path = std::filesystem::path(directory_) / "CSTATS-CURRENT";
  const auto current_tmp = current_path.string() + ".tmp";
  { std::ofstream out(current_tmp, std::ios::binary | std::ios::trunc); if (!out) return Status::IOError("query statistics", "cannot create current link"); out.write(current.data(), current.size()); out.flush(); if (!out) return Status::IOError("query statistics", "current link write failed"); }
  if (!(SyncPath(current_tmp, false).ok())) return Status::IOError("query statistics", "current link sync failed");
  if (std::filesystem::rename(current_tmp, current_path, ec), ec) return Status::IOError("query statistics", ec.message());
  return SyncPath(directory_,true);
}
StatusOr<QueryStatisticsSnapshot> QueryStatisticsStore::Load(uint64_t generation_id, CommitSeq base_seq, const std::string& schema_fingerprint) const {const auto current_path=std::filesystem::path(directory_)/"CSTATS-CURRENT";std::ifstream current(current_path,std::ios::binary);std::string link((std::istreambuf_iterator<char>(current)),std::istreambuf_iterator<char>());if(link.size()!=16||link.compare(0,4,"CSC1",4)!=0)return Status::NotFound("query statistics","statistics linkage unavailable");size_t lp=4;uint64_t linked=0;uint32_t crc=0;if(!Get64(link,&lp,&linked)||!Get32(link,&lp,&crc)||crc!=crc32c::Value(link.data(),12))return Status::Corruption("query statistics","statistics linkage checksum mismatch");if(linked!=generation_id)return Status::NotFound("query statistics","statistics unavailable");const auto path=std::filesystem::path(directory_)/FileName(generation_id);std::ifstream in(path,std::ios::binary);if(!in)return Status::NotFound("query statistics","statistics unavailable");std::string bytes((std::istreambuf_iterator<char>(in)),std::istreambuf_iterator<char>());auto decoded=DecodeQueryStatistics(bytes);if(!decoded.ok())return decoded.status();const auto& s=decoded.ValueOrDie();if(s.database_identity!=database_identity_||s.generation_id!=generation_id||s.base_seq!=base_seq||(s.complete&&s.schema_fingerprint.empty())||(!schema_fingerprint.empty()&&s.schema_fingerprint!=schema_fingerprint))return Status::Conflict("query statistics","statistics identity is stale");return decoded;}

void QueryMetrics::AddBatch(QueryMetricOperator op,uint64_t rows,uint64_t physical_bytes,uint64_t decoded_bytes,uint64_t interval_fragments){const auto i=static_cast<size_t>(op);if(i>=operator_rows_.size())return;operator_rows_[i].fetch_add(rows);batches_.fetch_add(1);physical_bytes_.fetch_add(physical_bytes);decoded_bytes_.fetch_add(decoded_bytes);interval_fragments_.fetch_add(interval_fragments);}
void QueryMetrics::AddTerminal(QueryMetricTerminal t){const auto i=static_cast<size_t>(t);if(i<terminal_.size())terminal_[i].fetch_add(1);}
void QueryMetrics::AddFallback(QueryMetricFallback f){const auto i=static_cast<size_t>(f);if(i<fallback_.size())fallback_[i].fetch_add(1);}
void QueryMetrics::AddSpillBytes(uint64_t b){spill_bytes_.fetch_add(b);}
QueryMetricsSnapshot QueryMetrics::Snapshot() const {QueryMetricsSnapshot s;for(size_t i=0;i<s.operator_rows.size();++i)s.operator_rows[i]=operator_rows_[i].load();for(size_t i=0;i<s.terminal.size();++i)s.terminal[i]=terminal_[i].load();for(size_t i=0;i<s.fallback.size();++i)s.fallback[i]=fallback_[i].load();s.batches=batches_.load();s.physical_bytes=physical_bytes_.load();s.decoded_bytes=decoded_bytes_.load();s.interval_fragments=interval_fragments_.load();s.spill_bytes=spill_bytes_.load();return s;}
Status QueryMetrics::RegisterLabel(QueryMetricOperator op){return static_cast<size_t>(op)<operator_rows_.size()?Status::OK():Status::InvalidArgument("query metrics","operator label is out of bounds");}
Status QueryMetrics::RegisterLabel(QueryMetricTerminal t){return static_cast<size_t>(t)<terminal_.size()?Status::OK():Status::InvalidArgument("query metrics","terminal label is out of bounds");}
Status QueryMetrics::RegisterLabel(QueryMetricFallback f){return static_cast<size_t>(f)<fallback_.size()?Status::OK():Status::InvalidArgument("query metrics","fallback label is out of bounds");}
}  // namespace cedar::internal
