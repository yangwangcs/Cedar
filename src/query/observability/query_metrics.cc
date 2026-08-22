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
#include <thread>
#include <unordered_map>
#include <algorithm>

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

// The projection store owns PROJECTION-CURRENT.  Refresh only observes it so
// an asynchronous statistics job cannot publish an older generation after a
// newer projection has become current.  A missing pointer is allowed for the
// standalone statistics tests and for databases without a derived projection.
StatusOr<std::optional<ProjectionManifest>> ReadCurrentProjectionManifest(
    const std::filesystem::path& directory, const std::string& identity) {
  const auto current_path = directory / "PROJECTION-CURRENT";
  std::ifstream current(current_path, std::ios::binary);
  if (!current) return std::optional<ProjectionManifest>();
  const std::string link((std::istreambuf_iterator<char>(current)),
                         std::istreambuf_iterator<char>());
  if (link.size() != 16 || link.compare(0, 4, "CPC1", 4) != 0)
    return Status::Corruption("query statistics", "invalid PROJECTION-CURRENT");
  size_t p = 4;
  uint64_t generation = 0;
  uint32_t checksum = 0;
  if (!Get64(link, &p, &generation) || !Get32(link, &p, &checksum) ||
      checksum != crc32c::Value(link.data(), 12)) {
    return Status::Corruption("query statistics", "invalid PROJECTION-CURRENT checksum");
  }
  if (generation == 0) return std::optional<ProjectionManifest>();
  const auto manifest_path = directory / "manifests" /
      (std::to_string(generation) + ".cmanifest");
  std::ifstream manifest_in(manifest_path, std::ios::binary);
  if (!manifest_in)
    return Status::Corruption("query statistics", "current projection manifest is missing");
  const std::string bytes((std::istreambuf_iterator<char>(manifest_in)),
                          std::istreambuf_iterator<char>());
  auto manifest = DecodeProjectionManifest(bytes, identity);
  if (!manifest.ok() || manifest.ValueOrDie().generation_id != generation)
    return Status::Corruption("query statistics", "current projection manifest is invalid");
  return std::optional<ProjectionManifest>(manifest.ConsumeValueOrDie());
}
}

StatusOr<std::string> EncodeQueryStatistics(const QueryStatisticsSnapshot& snapshot) {
  // Keep the on-disk trust bit consistent with Load's identity contract even
  // for callers that construct snapshots directly (outside Refresh).
  if (snapshot.complete && snapshot.schema_fingerprint.empty()) {
    QueryStatisticsSnapshot normalized = snapshot;
    normalized.complete = false;
    return EncodeQueryStatistics(normalized);
  }
  const Status valid = ValidateBounds(snapshot); if (!valid.ok()) return valid;
  std::string out("CDRSTS1\0", 8); PutBytes(&out, snapshot.database_identity); PutBytes(&out, snapshot.schema_fingerprint); PutBytes(&out, snapshot.coverage);
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
  size_t p = 8; QueryStatisticsSnapshot snapshot; if (!GetBytes(in, &p, &snapshot.database_identity) || !GetBytes(in, &p, &snapshot.schema_fingerprint) || !GetBytes(in, &p, &snapshot.coverage) || !Get64(in, &p, &snapshot.generation_id) || !Get64(in, &p, &snapshot.base_seq.value)) return Status::Corruption("query statistics", "truncated identity");
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
  std::lock_guard<std::mutex> refresh_lock(refresh_mutex_);
  if (latest_generation_id_ && manifest.generation_id < *latest_generation_id_) {
    return Status::Conflict("query statistics", "statistics refresh generation is stale");
  }
  const auto validate_current = [&]() -> Status {
    auto current = ReadCurrentProjectionManifest(directory_, database_identity_);
    if (!current.ok()) return current.status();
    if (!current.ValueOrDie()) return Status::OK();
    const auto& current_manifest = *current.ValueOrDie();
    if (current_manifest.generation_id > manifest.generation_id) {
      return Status::Conflict("query statistics", "statistics refresh generation is stale");
    }
    if (current_manifest.generation_id == manifest.generation_id &&
        current_manifest.base_seq != manifest.base_seq) {
      return Status::Conflict("query statistics", "statistics refresh base is stale");
    }
    return Status::OK();
  };
  const Status initial_generation = validate_current();
  if (!initial_generation.ok()) return initial_generation;
  QueryStatisticsSnapshot snapshot;
  snapshot.database_identity = database_identity_;
  snapshot.schema_fingerprint = schema_fingerprint;
  snapshot.generation_id = manifest.generation_id;
  snapshot.base_seq = manifest.base_seq;
  // A complete payload without a schema identity cannot be trusted by Load.
  snapshot.complete = !schema_fingerprint.empty();
  for (const auto& region : manifest.regions) {
    if (!snapshot.coverage.empty()) snapshot.coverage.push_back('|');
    snapshot.coverage += "part=" + std::to_string(region.part_id.value) +
        ",entity=[" + std::to_string(region.entity_min) + "," +
        std::to_string(region.entity_max_exclusive) + "),valid=[" +
        std::to_string(region.valid_time.from.value) + "," +
        (region.valid_time.to ? std::to_string(region.valid_time.to->value) : "inf") + ")";
  }
  snapshot.columns.reserve(manifest.regions.size());
  const auto update_hll = [](HllSketch* sketch, uint64_t key) {
    uint64_t hash = 1469598103934665603ULL;
    for (int i = 0; i < 8; ++i) { hash ^= uint8_t(key >> (i * 8)); hash *= 1099511628211ULL; }
    const uint64_t mask = (uint64_t{1} << sketch->precision) - 1;
    const size_t index = static_cast<size_t>(hash & mask);
    uint64_t rest = hash >> sketch->precision;
    uint8_t rank = 1;
    while (rank < 64 - sketch->precision && (rest & 1) == 0) { ++rank; rest >>= 1; }
    sketch->registers[index] = std::max(sketch->registers[index], rank);
  };
  for (const auto& region : manifest.regions) {
    QueryColumnStatistics c;
    c.distinct.registers.assign(size_t{1} << c.distinct.precision, 0);
    c.entity_range = EntityRange{region.entity_min, region.entity_max_exclusive};
    c.valid_time_range = region.valid_time;
    std::unordered_map<std::string, std::pair<Value, uint64_t>> values;
    std::unordered_map<uint64_t, uint64_t> fanout;
    std::vector<uint64_t> lengths;
    std::vector<uint64_t> numeric_values;
    for (const auto& segment : region.segments) {
      const auto path = std::filesystem::path(directory_) / segment.filename;
      std::ifstream in(path, std::ios::binary);
      if (!in) { snapshot.complete = false; continue; }
      const std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
      if (bytes.size() != segment.file_bytes || crc32c::Value(bytes.data(), bytes.size()) != segment.checksum) {
        snapshot.complete = false;
        continue;
      }
      auto decoded = DecodeProjectionPage(bytes);
      if (!decoded.ok()) { snapshot.complete = false; continue; }
      const auto& chain = decoded.ValueOrDie();
      c.bytes += bytes.size();
      c.pages += chain.page_directory.size();
      for (const auto& page : chain.page_directory) c.rows += page.row_count;
      c.interval_count += chain.intervals.size();
      c.edge_count += region.kind == ProjectionKind::kAdjacency ? chain.intervals.size() : 0;
      for (const auto& row : chain.intervals) {
        update_hll(&c.distinct, row.entity_id);
        update_hll(&c.distinct, row.entity_id ^ row.effective.from.value);
        ++fanout[row.entity_id];
        if (row.effective.to && row.effective.to->value >= row.effective.from.value)
          lengths.push_back(row.effective.to->value - row.effective.from.value);
        const std::string encoded = row.value.Encode();
        auto it = values.find(encoded);
        if (it == values.end()) values.emplace(encoded, std::make_pair(row.value, 1));
        else ++it->second.second;
        if (row.value.type() == PhysicalType::kInt32) numeric_values.push_back(static_cast<uint64_t>(std::get<int32_t>(row.value.data())));
        else if (row.value.type() == PhysicalType::kInt64) numeric_values.push_back(static_cast<uint64_t>(std::get<int64_t>(row.value.data())));
        else if (row.value.type() == PhysicalType::kTimestamp64) numeric_values.push_back(std::get<uint64_t>(row.value.data()));
      }
      for (const auto& row : chain.boundaries) {
        update_hll(&c.distinct, row.entity_id);
        update_hll(&c.distinct, row.entity_id ^ row.time.value);
      }
    }
    if (!numeric_values.empty()) {
      std::sort(numeric_values.begin(), numeric_values.end());
      const size_t buckets = std::min<size_t>(128, numeric_values.size());
      for (size_t i = 0; i < buckets; ++i) {
        const size_t at = ((i + 1) * numeric_values.size() - 1) / buckets;
        c.histogram.push_back({Value::Int64(static_cast<int64_t>(numeric_values[at])), at + 1});
      }
    }
    std::vector<std::pair<Value, uint64_t>> top;
    for (const auto& entry : values) top.push_back(entry.second);
    std::sort(top.begin(), top.end(), [](const auto& a, const auto& b) {
      if (a.second != b.second) return a.second > b.second;
      return a.first.Encode() < b.first.Encode();
    });
    if (top.size() > 64) top.erase(top.begin() + 64, top.end());
    for (const auto& entry : top) c.top_values.push_back({entry.first, entry.second});
    if (!fanout.empty()) {
      std::vector<uint64_t> sample; for (const auto& entry : fanout) sample.push_back(entry.second);
      std::sort(sample.begin(), sample.end());
      c.fanout.push_back({0.5, sample[(sample.size() - 1) / 2]});
      c.fanout.push_back({0.95, sample[(sample.size() * 95) / 100 < sample.size() ? (sample.size() * 95) / 100 : sample.size() - 1]});
    }
    if (!lengths.empty()) {
      std::sort(lengths.begin(), lengths.end());
      c.interval_length.push_back({0.5, lengths[(lengths.size() - 1) / 2]});
      c.interval_length.push_back({0.95, lengths[(lengths.size() * 95) / 100 < lengths.size() ? (lengths.size() * 95) / 100 : lengths.size() - 1]});
    }
    snapshot.columns.push_back(std::move(c));
  }
  auto encoded=EncodeQueryStatistics(snapshot);if(!encoded.ok())return encoded.status();
  std::error_code ec;
  std::filesystem::create_directories(std::filesystem::path(directory_) / "manifests", ec);
  if(ec)return Status::IOError("query statistics",ec.message());
  const auto path=std::filesystem::path(directory_)/FileName(manifest.generation_id);
  const auto temp=path.string()+".tmp."+std::to_string(++refresh_counter_)+"."+std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));
  {std::ofstream out(temp,std::ios::binary|std::ios::trunc);if(!out)return Status::IOError("query statistics","cannot create file");out.write(encoded.ValueOrDie().data(),encoded.ValueOrDie().size());out.flush();if(!out)return Status::IOError("query statistics","write failed");}
  auto synced=SyncPath(temp,false);if(!synced.ok()){std::filesystem::remove(temp);return synced;}
  // Revalidate after the potentially long segment scan and before making the
  // generation artifact visible.  A newer PROJECTION-CURRENT wins over this
  // refresh, even when the refresh started first.
  const Status pre_publish_generation = validate_current();
  if (!pre_publish_generation.ok()) {
    std::filesystem::remove(temp);
    return pre_publish_generation;
  }
  std::filesystem::rename(temp,path,ec);if(ec){std::filesystem::remove(temp);return Status::IOError("query statistics",ec.message());}
  if(!SyncPath(directory_,true).ok())return Status::IOError("query statistics","statistics directory sync failed");

  // Publish the immutable generation manifest's statistics reference before
  // publishing CSTATS-CURRENT.  A crash before the final pointer replacement
  // therefore leaves either an old pointer or no loadable statistics; a
  // pointer can never name a manifest that lacks the matching reference.
  ProjectionManifest linked_manifest = manifest;
  StatisticsReference reference;
  reference.filename = FileName(manifest.generation_id);
  reference.generation_id = manifest.generation_id;
  reference.base_seq = manifest.base_seq;
  reference.checksum = crc32c::Value(encoded.ValueOrDie().data(), encoded.ValueOrDie().size());
  reference.complete = snapshot.complete;
  linked_manifest.statistics = reference;
  auto manifest_bytes = EncodeProjectionManifest(linked_manifest);
  if (!manifest_bytes.ok()) return manifest_bytes.status();
  const auto manifest_path = std::filesystem::path(directory_) / "manifests" /
      (std::to_string(manifest.generation_id) + ".cmanifest");
  const auto manifest_tmp = manifest_path.string() + ".tmp." + std::to_string(refresh_counter_);
  { std::ofstream out(manifest_tmp, std::ios::binary | std::ios::trunc); if (!out) return Status::IOError("query statistics", "cannot create generation manifest"); out.write(manifest_bytes.ValueOrDie().data(), manifest_bytes.ValueOrDie().size()); out.flush(); if (!out) return Status::IOError("query statistics", "generation manifest write failed"); }
  if (!(SyncPath(manifest_tmp, false).ok())) return Status::IOError("query statistics", "generation manifest sync failed");
  const Status pre_manifest_generation = validate_current();
  if (!pre_manifest_generation.ok()) {
    std::filesystem::remove(manifest_tmp);
    return pre_manifest_generation;
  }
  if (std::filesystem::rename(manifest_tmp, manifest_path, ec), ec) return Status::IOError("query statistics", ec.message());
  if (!SyncPath(std::filesystem::path(directory_) / "manifests", true).ok()) return Status::IOError("query statistics", "manifest directory sync failed");

  // CSTATS-CURRENT is a generation/base/checksum-validated pointer.  The
  // manifest CRC binds this pointer to the just-published reference.
  std::string current("CSC2", 4); Put64(&current, manifest.generation_id);
  Put64(&current, manifest.base_seq.value);
  Put32(&current, reference.checksum);
  Put32(&current, crc32c::Value(manifest_bytes.ValueOrDie().data(), manifest_bytes.ValueOrDie().size()));
  Put32(&current, crc32c::Value(current.data(), current.size()));
  const auto current_path = std::filesystem::path(directory_) / "CSTATS-CURRENT";
  const auto current_tmp = current_path.string() + ".tmp." + std::to_string(refresh_counter_);
  { std::ofstream out(current_tmp, std::ios::binary | std::ios::trunc); if (!out) return Status::IOError("query statistics", "cannot create current link"); out.write(current.data(), current.size()); out.flush(); if (!out) return Status::IOError("query statistics", "current link write failed"); }
  if (!(SyncPath(current_tmp, false).ok())) return Status::IOError("query statistics", "current link sync failed");
  const Status pre_current_generation = validate_current();
  if (!pre_current_generation.ok()) {
    std::filesystem::remove(current_tmp);
    return pre_current_generation;
  }
  if (std::filesystem::rename(current_tmp, current_path, ec), ec) return Status::IOError("query statistics", ec.message());
  latest_generation_id_ = manifest.generation_id;
  return SyncPath(directory_,true);
}
StatusOr<QueryStatisticsSnapshot> QueryStatisticsStore::Load(uint64_t generation_id, CommitSeq base_seq, const std::string& schema_fingerprint) const {
  const auto current_path=std::filesystem::path(directory_)/"CSTATS-CURRENT";
  std::ifstream current(current_path,std::ios::binary);
  std::string link((std::istreambuf_iterator<char>(current)),std::istreambuf_iterator<char>());
  if (link.size()!=32 || link.compare(0,4,"CSC2",4)!=0) return Status::NotFound("query statistics","statistics linkage unavailable");
  size_t lp=4; uint64_t linked=0, linked_base=0; uint32_t payload_crc=0, manifest_crc=0, link_crc=0;
  if(!Get64(link,&lp,&linked)||!Get64(link,&lp,&linked_base)||!Get32(link,&lp,&payload_crc)||!Get32(link,&lp,&manifest_crc)||!Get32(link,&lp,&link_crc)||link_crc!=crc32c::Value(link.data(),28)) return Status::Corruption("query statistics","statistics linkage checksum mismatch");
  if(linked!=generation_id||linked_base!=base_seq.value)return Status::NotFound("query statistics","statistics unavailable");
  // A CSC2 pointer is only valid when it can be bound to the immutable
  // projection manifest that describes the same generation.  Treat a
  // missing sibling as stale rather than accepting an unbound statistics
  // payload after a crash or partial cleanup.
  const auto manifest_path = std::filesystem::path(directory_) / "manifests" / (std::to_string(generation_id) + ".cmanifest");
  std::ifstream manifest_in(manifest_path, std::ios::binary);
  if (!manifest_in) return Status::NotFound("query statistics", "statistics manifest linkage is unavailable");
  std::string manifest_bytes((std::istreambuf_iterator<char>(manifest_in)), std::istreambuf_iterator<char>());
  if (crc32c::Value(manifest_bytes.data(), manifest_bytes.size()) != manifest_crc) return Status::NotFound("query statistics", "statistics manifest linkage is stale");
  auto decoded_manifest = DecodeProjectionManifest(manifest_bytes, database_identity_);
  if (!decoded_manifest.ok() || decoded_manifest.ValueOrDie().generation_id != generation_id || decoded_manifest.ValueOrDie().base_seq != base_seq || !decoded_manifest.ValueOrDie().statistics) return Status::NotFound("query statistics", "statistics manifest linkage is stale");
  const auto& reference = *decoded_manifest.ValueOrDie().statistics;
  if (reference.generation_id != linked || reference.base_seq.value != linked_base || reference.checksum != payload_crc || reference.complete != decoded_manifest.ValueOrDie().statistics->complete) return Status::NotFound("query statistics", "statistics reference is stale");
  const auto path=std::filesystem::path(directory_)/reference.filename;std::ifstream in(path,std::ios::binary);if(!in)return Status::NotFound("query statistics","statistics unavailable");std::string bytes((std::istreambuf_iterator<char>(in)),std::istreambuf_iterator<char>());
  if(crc32c::Value(bytes.data(),bytes.size())!=payload_crc)return Status::Corruption("query statistics","statistics payload checksum mismatch");
  auto decoded=DecodeQueryStatistics(bytes);if(!decoded.ok())return decoded.status();const auto& s=decoded.ValueOrDie();if(s.database_identity!=database_identity_||s.generation_id!=generation_id||s.base_seq!=base_seq||s.complete!=reference.complete||(s.complete&&s.schema_fingerprint.empty())||(!schema_fingerprint.empty()&&s.schema_fingerprint!=schema_fingerprint))return Status::Conflict("query statistics","statistics identity is stale");return decoded;
}

void QueryMetrics::AddBatch(QueryMetricOperator op,uint64_t rows,uint64_t physical_bytes,uint64_t decoded_bytes,uint64_t interval_fragments){const auto i=static_cast<size_t>(op);if(i>=operator_rows_.size())return;operator_rows_[i].fetch_add(rows);batches_.fetch_add(1);physical_bytes_.fetch_add(physical_bytes);decoded_bytes_.fetch_add(decoded_bytes);interval_fragments_.fetch_add(interval_fragments);}
void QueryMetrics::AddTerminal(QueryMetricTerminal t){const auto i=static_cast<size_t>(t);if(i<terminal_.size())terminal_[i].fetch_add(1);}
void QueryMetrics::AddFallback(QueryMetricFallback f){const auto i=static_cast<size_t>(f);if(i<fallback_.size())fallback_[i].fetch_add(1);}
void QueryMetrics::AddSpillBytes(uint64_t b){spill_bytes_.fetch_add(b);}
QueryMetricsSnapshot QueryMetrics::Snapshot() const {QueryMetricsSnapshot s;for(size_t i=0;i<s.operator_rows.size();++i)s.operator_rows[i]=operator_rows_[i].load();for(size_t i=0;i<s.terminal.size();++i)s.terminal[i]=terminal_[i].load();for(size_t i=0;i<s.fallback.size();++i)s.fallback[i]=fallback_[i].load();s.batches=batches_.load();s.physical_bytes=physical_bytes_.load();s.decoded_bytes=decoded_bytes_.load();s.interval_fragments=interval_fragments_.load();s.spill_bytes=spill_bytes_.load();return s;}
Status QueryMetrics::RegisterLabel(QueryMetricOperator op){return static_cast<size_t>(op)<operator_rows_.size()?Status::OK():Status::InvalidArgument("query metrics","operator label is out of bounds");}
Status QueryMetrics::RegisterLabel(QueryMetricTerminal t){return static_cast<size_t>(t)<terminal_.size()?Status::OK():Status::InvalidArgument("query metrics","terminal label is out of bounds");}
Status QueryMetrics::RegisterLabel(QueryMetricFallback f){return static_cast<size_t>(f)<fallback_.size()?Status::OK():Status::InvalidArgument("query metrics","fallback label is out of bounds");}
}  // namespace cedar::internal
