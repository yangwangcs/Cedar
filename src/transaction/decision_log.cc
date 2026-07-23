// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "cedar/transaction/decision_log.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <map>
#include <utility>

#include "cedar/core/crc32c.h"

namespace cedar {
namespace {

constexpr uint32_t kPrepareMagic = 0x32505250U;   // PRP2
constexpr uint32_t kDecisionMagic = 0x32444344U;  // DCD2
constexpr uint32_t kOutcomeMagic = 0x314f5854U;   // TXO1
constexpr uint32_t kOutcomeVersion = 1;
constexpr size_t kRecordHeaderBytes = 12;
constexpr uint32_t kMaximumRecordBytes = 64U * 1024U * 1024U;
constexpr uint64_t kPrepareSegmentBytes = 4ULL * 1024U * 1024U;

Status FsyncDirectory(const std::filesystem::path& directory) {
  const std::string path = directory.string();
  const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) return Status::IOError(path, std::strerror(errno));
  Status status = Status::OK();
  if (::fsync(fd) != 0) status = Status::IOError(path, std::strerror(errno));
  if (::close(fd) != 0 && status.ok()) status = Status::IOError(path, std::strerror(errno));
  return status;
}

Status EnsureDirectoryDurably(const std::filesystem::path& directory) {
  if (directory.empty()) {
    return Status::InvalidArgument("transaction log", "empty parent directory");
  }
  std::error_code error;
  std::vector<std::filesystem::path> missing;
  std::filesystem::path current = directory;
  while (!std::filesystem::exists(current, error)) {
    if (error) return Status::IOError(current.string(), error.message());
    missing.push_back(current);
    const std::filesystem::path parent = current.parent_path();
    if (parent.empty() || parent == current) {
      return Status::IOError(current.string(), "no existing parent directory");
    }
    current = parent;
  }
  if (error || !std::filesystem::is_directory(current, error)) {
    return Status::IOError(current.string(), error ? error.message() : "not a directory");
  }
  for (auto component = missing.rbegin(); component != missing.rend(); ++component) {
    const std::filesystem::path parent = component->parent_path();
    if (!std::filesystem::create_directory(*component, error) && error) {
      return Status::IOError(component->string(), error.message());
    }
    Status status = FsyncDirectory(parent);
    if (!status.ok()) return status;
    status = FsyncDirectory(*component);
    if (!status.ok()) return status;
  }
  return Status::OK();
}

void PutU8(std::string* out, uint8_t value) { out->push_back(static_cast<char>(value)); }
void PutU16(std::string* out, uint16_t value) {
  out->push_back(static_cast<char>(value));
  out->push_back(static_cast<char>(value >> 8));
}
void PutU32(std::string* out, uint32_t value) {
  for (uint32_t shift = 0; shift < 32; shift += 8) PutU8(out, value >> shift);
}
void PutU64(std::string* out, uint64_t value) {
  for (uint32_t shift = 0; shift < 64; shift += 8) PutU8(out, value >> shift);
}

bool GetU8(const std::string& in, size_t* offset, uint8_t* value) {
  if (*offset >= in.size()) return false;
  *value = static_cast<uint8_t>(in[(*offset)++]);
  return true;
}
bool GetU16(const std::string& in, size_t* offset, uint16_t* value) {
  uint8_t lo, hi;
  if (!GetU8(in, offset, &lo) || !GetU8(in, offset, &hi)) return false;
  *value = static_cast<uint16_t>(lo) | (static_cast<uint16_t>(hi) << 8);
  return true;
}
bool GetU32(const std::string& in, size_t* offset, uint32_t* value) {
  *value = 0;
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    uint8_t byte;
    if (!GetU8(in, offset, &byte)) return false;
    *value |= static_cast<uint32_t>(byte) << shift;
  }
  return true;
}
bool GetU64(const std::string& in, size_t* offset, uint64_t* value) {
  *value = 0;
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    uint8_t byte;
    if (!GetU8(in, offset, &byte)) return false;
    *value |= static_cast<uint64_t>(byte) << shift;
  }
  return true;
}

Status WriteAll(int fd, const std::string& bytes, const std::string& path) {
  const char* cursor = bytes.data();
  size_t remaining = bytes.size();
  while (remaining != 0) {
    const ssize_t written = ::write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    cursor += written;
    remaining -= static_cast<size_t>(written);
  }
  return Status::OK();
}

Status WriteAtomically(const std::string& path, const std::string& bytes) {
  const std::filesystem::path target(path);
  const std::filesystem::path directory = target.parent_path();
  std::error_code error;
  if (!directory.empty()) {
    std::filesystem::create_directories(directory, error);
    if (error) return Status::IOError(directory.string(), error.message());
  }
  const std::string temporary = path + ".tmp";
  const int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  Status status = WriteAll(fd, bytes, temporary);
  if (status.ok() && ::fsync(fd) != 0) status = Status::IOError(temporary, std::strerror(errno));
  if (::close(fd) != 0 && status.ok()) status = Status::IOError(temporary, std::strerror(errno));
  if (!status.ok()) return status;
  if (::rename(temporary.c_str(), path.c_str()) != 0) {
    return Status::IOError(path, std::strerror(errno));
  }
  if (!directory.empty()) {
    const int directory_fd = ::open(directory.c_str(), O_RDONLY);
    if (directory_fd < 0) return Status::IOError(directory.string(), std::strerror(errno));
    if (::fsync(directory_fd) != 0) {
      const Status fsync_status = Status::IOError(directory.string(), std::strerror(errno));
      ::close(directory_fd);
      return fsync_status;
    }
    if (::close(directory_fd) != 0) return Status::IOError(directory.string(), std::strerror(errno));
  }
  return Status::OK();
}

Status ReadFile(const std::string& path, std::string* contents) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError(path, std::strerror(errno));
  contents->clear();
  char buffer[8192];
  for (;;) {
    const ssize_t count = ::read(fd, buffer, sizeof(buffer));
    if (count == 0) break;
    if (count < 0) {
      const Status status = Status::IOError(path, std::strerror(errno));
      ::close(fd);
      return status;
    }
    contents->append(buffer, static_cast<size_t>(count));
  }
  if (::close(fd) != 0) return Status::IOError(path, std::strerror(errno));
  return Status::OK();
}

uint64_t EncodePrepareLsn(uint32_t segment_id, uint32_t offset) {
  return (static_cast<uint64_t>(segment_id) << 32) | offset;
}

uint32_t PrepareSegmentId(uint64_t lsn) { return static_cast<uint32_t>(lsn >> 32); }
uint32_t PrepareSegmentOffset(uint64_t lsn) { return static_cast<uint32_t>(lsn); }

std::string PrepareSegmentPath(const std::string& base_path, uint32_t segment_id) {
  return base_path + "." + std::to_string(segment_id);
}

StatusOr<std::vector<uint32_t>> ListPrepareSegments(const std::string& base_path) {
  const std::filesystem::path base(base_path);
  const std::filesystem::path directory = base.parent_path();
  const std::string prefix = base.filename().string() + ".";
  if (!std::filesystem::exists(directory)) return std::vector<uint32_t>{};
  std::vector<uint32_t> segments;
  std::error_code error;
  for (const auto& entry : std::filesystem::directory_iterator(directory, error)) {
    if (error) return Status::IOError(directory.string(), error.message());
    if (!entry.is_regular_file(error)) {
      if (error) return Status::IOError(entry.path().string(), error.message());
      continue;
    }
    const std::string name = entry.path().filename().string();
    if (name.compare(0, prefix.size(), prefix) != 0) continue;
    const std::string suffix = name.substr(prefix.size());
    try {
      size_t parsed = 0;
      const unsigned long value = std::stoul(suffix, &parsed);
      if (parsed != suffix.size() || value == 0 || value > UINT32_MAX) {
        return Status::Corruption(base_path, "invalid prepare segment identity");
      }
      segments.push_back(static_cast<uint32_t>(value));
    } catch (const std::exception&) {
      return Status::Corruption(base_path, "invalid prepare segment name");
    }
  }
  std::sort(segments.begin(), segments.end());
  if (std::adjacent_find(segments.begin(), segments.end()) != segments.end()) {
    return Status::Corruption(base_path, "duplicate prepare segment identity");
  }
  return segments;
}
void PutString(std::string* out, const std::string& value) {
  PutU32(out, static_cast<uint32_t>(value.size()));
  out->append(value);
}
bool GetString(const std::string& in, size_t* offset, std::string* value) {
  uint32_t size;
  if (!GetU32(in, offset, &size) || size > in.size() - *offset) return false;
  *value = in.substr(*offset, size);
  *offset += size;
  return true;
}

void PutKey(std::string* out, const LogicalKey& key) {
  PutU8(out, static_cast<uint8_t>(key.entity_type()));
  PutU8(out, static_cast<uint8_t>(key.kind()));
  PutU64(out, key.entity_id());
  PutU64(out, key.target_id());
  PutU16(out, key.column_id());
  PutU16(out, key.edge_type());
  PutU64(out, key.edge_id());
}
bool GetKey(const std::string& in, size_t* offset, LogicalKey* key) {
  uint8_t type, kind;
  uint64_t entity_id, target_id, edge_id;
  uint16_t column_id, edge_type;
  if (!GetU8(in, offset, &type) || !GetU8(in, offset, &kind) ||
      !GetU64(in, offset, &entity_id) || !GetU64(in, offset, &target_id) ||
      !GetU16(in, offset, &column_id) || !GetU16(in, offset, &edge_type) ||
      !GetU64(in, offset, &edge_id)) return false;
  const EntityType entity_type = static_cast<EntityType>(type);
  if (kind == static_cast<uint8_t>(LogicalKeyKind::kExistence)) {
    *key = entity_type == EntityType::Vertex
               ? LogicalKey::VertexExistence(entity_id)
               : LogicalKey::EdgeExistence(entity_id, target_id, edge_type,
                                           edge_id, entity_type);
  } else if (kind == static_cast<uint8_t>(LogicalKeyKind::kProperty)) {
    *key = entity_type == EntityType::Vertex
               ? LogicalKey::VertexProperty(entity_id, column_id)
               : LogicalKey::EdgeProperty(entity_id, target_id, edge_type,
                                         edge_id, column_id, entity_type);
  } else {
    return false;
  }
  return true;
}

std::string EncodePrepare(const PrepareRecord& record) {
  std::string out;
  PutU32(&out, kPrepareMagic);
  PutU64(&out, record.txn_id);
  PutU64(&out, record.snapshot_seq);
  PutU32(&out, static_cast<uint32_t>(record.participant_shards.size()));
  for (uint32_t shard : record.participant_shards) PutU32(&out, shard);
  PutU32(&out, static_cast<uint32_t>(record.events.size()));
  for (const PendingEvent& event : record.events) {
    PutKey(&out, event.logical_key);
    PutU64(&out, event.valid_from);
    PutU32(&out, event.schema_epoch);
    PutU8(&out, static_cast<uint8_t>(event.operation));
    PutU8(&out, event.blob_ref.has_value() ? 1 : 0);
    if (event.blob_ref.has_value()) {
      const BlobRef& reference = *event.blob_ref;
      out.append(reinterpret_cast<const char*>(reference.content_hash.bytes.data()),
                 reference.content_hash.bytes.size());
      PutU64(&out, reference.raw_length);
      PutU32(&out, reference.hint.shard_id);
      PutU64(&out, reference.hint.segment_id);
      PutU64(&out, reference.hint.offset);
    } else {
      PutString(&out, event.value.Encode());
    }
  }
  return out;
}

bool DecodePrepare(const std::string& in, PrepareRecord* record) {
  size_t offset = 0;
  uint32_t magic, participant_count, event_count;
  if (!GetU32(in, &offset, &magic) || magic != kPrepareMagic ||
      !GetU64(in, &offset, &record->txn_id) ||
      !GetU64(in, &offset, &record->snapshot_seq) ||
      !GetU32(in, &offset, &participant_count) || participant_count > 65536) return false;
  record->participant_shards.clear();
  for (uint32_t i = 0; i < participant_count; ++i) {
    uint32_t shard;
    if (!GetU32(in, &offset, &shard)) return false;
    record->participant_shards.push_back(shard);
  }
  if (!GetU32(in, &offset, &event_count) || event_count > 1000000) return false;
  record->events.clear();
  for (uint32_t i = 0; i < event_count; ++i) {
    LogicalKey key = LogicalKey::VertexExistence(0);
    uint64_t valid_from; uint32_t schema_epoch;
    uint8_t operation, is_blob;
    std::string encoded_value;
    if (!GetKey(in, &offset, &key) ||
        !GetU64(in, &offset, &valid_from) ||
        !GetU32(in, &offset, &schema_epoch) ||
        !GetU8(in, &offset, &operation) || operation > 1 ||
        !GetU8(in, &offset, &is_blob) || is_blob > 1) return false;
    if (is_blob != 0) {
      BlobRef reference;
      if (operation != static_cast<uint8_t>(TemporalOperation::kPut) ||
          in.size() - offset < reference.content_hash.bytes.size()) return false;
      std::memcpy(reference.content_hash.bytes.data(), in.data() + offset,
                  reference.content_hash.bytes.size());
      offset += reference.content_hash.bytes.size();
      if (!GetU64(in, &offset, &reference.raw_length) ||
          !GetU32(in, &offset, &reference.hint.shard_id) ||
          !GetU64(in, &offset, &reference.hint.segment_id) ||
          !GetU64(in, &offset, &reference.hint.offset)) return false;
      record->events.push_back(PendingEvent::PutBlob(
          std::move(key), valid_from, schema_epoch, std::move(reference)));
      continue;
    }
    if (!GetString(in, &offset, &encoded_value)) return false;
    const auto value = Value::Decode(encoded_value);
    if (!value.has_value()) return false;
    record->events.push_back(PendingEvent{
        std::move(key), valid_from, schema_epoch, static_cast<TemporalOperation>(operation),
        *value, std::nullopt});
  }
  return offset == in.size();
}

std::string EncodeDecision(const CommitDecision& decision) {
  std::string out;
  PutU32(&out, kDecisionMagic);
  PutU64(&out, decision.txn_id);
  PutU64(&out, decision.commit_seq);
  PutU64(&out, decision.system_time_hlc.physical_us);
  PutU32(&out, decision.system_time_hlc.logical_counter);
  PutU32(&out, static_cast<uint32_t>(decision.prepares.size()));
  for (const PrepareReference& ref : decision.prepares) {
    PutU32(&out, ref.shard_id);
    PutU64(&out, ref.lsn);
    PutU32(&out, ref.checksum);
  }
  return out;
}

bool DecodeDecision(const std::string& in, CommitDecision* decision) {
  size_t offset = 0;
  uint32_t magic, count;
  if (!GetU32(in, &offset, &magic) || magic != kDecisionMagic ||
      !GetU64(in, &offset, &decision->txn_id) ||
      !GetU64(in, &offset, &decision->commit_seq) ||
      !GetU64(in, &offset, &decision->system_time_hlc.physical_us) ||
      !GetU32(in, &offset, &decision->system_time_hlc.logical_counter) ||
      !GetU32(in, &offset, &count) || count == 0 || count > 65536) return false;
  decision->prepares.clear();
  for (uint32_t i = 0; i < count; ++i) {
    PrepareReference ref;
    if (!GetU32(in, &offset, &ref.shard_id) || !GetU64(in, &offset, &ref.lsn) ||
        !GetU32(in, &offset, &ref.checksum)) return false;
    decision->prepares.push_back(ref);
  }
  return offset == in.size();
}

bool ValidOutcomes(const std::vector<TransactionOutcome>& outcomes) {
  SystemHlc previous{0, 0};
  std::map<uint64_t, bool> txn_ids;
  for (size_t index = 0; index < outcomes.size(); ++index) {
    const TransactionOutcome& outcome = outcomes[index];
    if (outcome.txn_id == 0 || outcome.commit_seq != index + 1 ||
        !txn_ids.emplace(outcome.txn_id, true).second ||
        (index != 0 && !(outcome.system_time_hlc > previous))) {
      return false;
    }
    previous = outcome.system_time_hlc;
  }
  return true;
}

std::string EncodeOutcomeIndex(const std::vector<TransactionOutcome>& outcomes) {
  std::string body;
  PutU32(&body, kOutcomeMagic);
  PutU32(&body, kOutcomeVersion);
  PutU64(&body, static_cast<uint64_t>(outcomes.size()));
  for (const TransactionOutcome& outcome : outcomes) {
    PutU64(&body, outcome.txn_id);
    PutU64(&body, outcome.commit_seq);
    PutU64(&body, outcome.system_time_hlc.physical_us);
    PutU32(&body, outcome.system_time_hlc.logical_counter);
  }
  PutU32(&body, crc32c::Value(body.data(), body.size()));
  return body;
}

StatusOr<std::vector<TransactionOutcome>> DecodeOutcomeIndex(const std::string& encoded,
                                                              uint64_t checkpoint_seq) {
  if (encoded.size() < 20) return Status::Corruption("transaction outcomes", "truncated index");
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t version = 0;
  uint64_t count = 0;
  if (!GetU32(encoded, &offset, &magic) || !GetU32(encoded, &offset, &version) ||
      !GetU64(encoded, &offset, &count) || magic != kOutcomeMagic ||
      version != kOutcomeVersion || count > 100000000ULL ||
      count > (encoded.size() - offset - sizeof(uint32_t)) / 28) {
    return Status::Corruption("transaction outcomes", "invalid index header");
  }
  std::vector<TransactionOutcome> outcomes;
  outcomes.reserve(static_cast<size_t>(count));
  for (uint64_t index = 0; index < count; ++index) {
    TransactionOutcome outcome;
    if (!GetU64(encoded, &offset, &outcome.txn_id) ||
        !GetU64(encoded, &offset, &outcome.commit_seq) ||
        !GetU64(encoded, &offset, &outcome.system_time_hlc.physical_us) ||
        !GetU32(encoded, &offset, &outcome.system_time_hlc.logical_counter)) {
      return Status::Corruption("transaction outcomes", "truncated outcome");
    }
    outcomes.push_back(outcome);
  }
  uint32_t checksum = 0;
  if (!GetU32(encoded, &offset, &checksum) || offset != encoded.size() ||
      checksum != crc32c::Value(encoded.data(), encoded.size() - sizeof(uint32_t)) ||
      outcomes.size() != checkpoint_seq || !ValidOutcomes(outcomes)) {
    return Status::Corruption("transaction outcomes", "invalid outcome index");
  }
  return outcomes;
}

std::string EncodeFramedRecord(const std::string& payload) {
  std::string record;
  PutU32(&record, static_cast<uint32_t>(payload.size()));
  PutU32(&record, crc32c::Value(payload.data(), payload.size()));
  PutU32(&record, 1);
  record.append(payload);
  return record;
}

struct AppendRecordResult {
  Status status;
  bool may_be_durable = false;
  bool requires_reopen = false;
  bool fsync_attempted = false;
  uint64_t fsync_latency_ns = 0;
};

AppendRecordResult AppendRecord(
    const std::string& path, const std::string& payload,
    uint64_t* lsn, uint32_t* checksum,
    const std::function<Status(DecisionLogFaultPoint)>& fault_injector = {}) {
  const std::filesystem::path file(path);
  Status status = EnsureDirectoryDurably(file.parent_path());
  if (!status.ok()) return {status, false, false};
  std::error_code error;
  const bool file_existed = std::filesystem::exists(file, error);
  if (error) return {Status::IOError(path, error.message()), false, false};
  int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
  if (fd < 0) return {Status::IOError(path, std::strerror(errno)), false, false};
  const off_t offset = ::lseek(fd, 0, SEEK_END);
  if (offset < 0) {
    const Status failure = Status::IOError(path, std::strerror(errno));
    ::close(fd);
    return {failure, false, false};
  }
  *lsn = static_cast<uint64_t>(offset);
  *checksum = crc32c::Value(payload.data(), payload.size());
  const std::string record = EncodeFramedRecord(payload);
  const char* data = record.data();
  size_t remaining = record.size();
  bool wrote_any = false;
  if (fault_injector) {
    size_t partial_remaining = record.size() / 2;
    while (partial_remaining > 0) {
      const ssize_t written = ::write(fd, data, partial_remaining);
      if (written < 0) {
        if (errno == EINTR) continue;
        const Status failure = Status::IOError(path, std::strerror(errno));
        ::close(fd);
        return {failure, false, wrote_any};
      }
      wrote_any = true;
      data += written;
      remaining -= static_cast<size_t>(written);
      partial_remaining -= static_cast<size_t>(written);
    }
    status = fault_injector(DecisionLogFaultPoint::kAfterPartialRecordWrite);
    if (!status.ok()) {
      ::close(fd);
      return {status, false, true};
    }
  }
  while (remaining > 0) {
    const ssize_t written = ::write(fd, data, remaining);
    if (written < 0) {
      if (errno == EINTR) continue;
      const Status failure = Status::IOError(path, std::strerror(errno));
      ::close(fd);
      return {failure, false, wrote_any};
    }
    wrote_any = true;
    data += written;
    remaining -= static_cast<size_t>(written);
  }
  if (fault_injector) {
    status = fault_injector(DecisionLogFaultPoint::kAfterRecordWrite);
    if (!status.ok()) {
      ::close(fd);
      return {status, true, true};
    }
  }
  const auto fsync_start = std::chrono::steady_clock::now();
  const int fsync_result = ::fsync(fd);
  const auto fsync_elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - fsync_start).count();
  const uint64_t fsync_latency_ns = fsync_elapsed <= 0
      ? 0 : static_cast<uint64_t>(fsync_elapsed);
  if (fsync_result != 0) {
    const Status failure = Status::IOError(path, std::strerror(errno));
    ::close(fd);
    return {failure, true, true, true, fsync_latency_ns};
  }
  if (fault_injector) {
    status = fault_injector(DecisionLogFaultPoint::kAfterRecordFsync);
    if (!status.ok()) {
      ::close(fd);
      return {status, true, true, true, fsync_latency_ns};
    }
  }
  if (::close(fd) != 0) {
    return {Status::IOError(path, std::strerror(errno)), true, true,
            true, fsync_latency_ns};
  }
  if (!file_existed) {
    status = FsyncDirectory(file.parent_path());
    if (!status.ok()) return {status, true, true, true, fsync_latency_ns};
  }
  return {Status::OK(), false, false, true, fsync_latency_ns};
}

Status ReadRecords(const std::string& path,
                   std::vector<std::pair<uint64_t, std::pair<uint32_t, std::string>>>* records) {
  records->clear();
  if (!std::filesystem::exists(path)) return Status::OK();
  std::string contents;
  int fd = ::open(path.c_str(), O_RDWR);
  if (fd < 0) return Status::IOError(path, std::strerror(errno));
  char buffer[8192];
  for (;;) {
    const ssize_t read_count = ::read(fd, buffer, sizeof(buffer));
    if (read_count == 0) break;
    if (read_count < 0) { ::close(fd); return Status::IOError(path, std::strerror(errno)); }
    contents.append(buffer, static_cast<size_t>(read_count));
  }
  size_t offset = 0;
  std::optional<size_t> torn_offset;
  while (offset < contents.size()) {
    const uint64_t lsn = offset;
    if (contents.size() - offset < kRecordHeaderBytes) {
      torn_offset = offset;
      break;
    }
    uint32_t size, checksum, version;
    if (!GetU32(contents, &offset, &size) || !GetU32(contents, &offset, &checksum) ||
        !GetU32(contents, &offset, &version) || version != 1 || size > kMaximumRecordBytes) {
      ::close(fd);
      return Status::Corruption(path, "invalid log record header");
    }
    if (contents.size() - offset < size) {
      torn_offset = static_cast<size_t>(lsn);
      break;
    }
    std::string payload = contents.substr(offset, size);
    offset += size;
    if (crc32c::Value(payload.data(), payload.size()) != checksum) {
      ::close(fd);
      return Status::Corruption(path, "log record checksum mismatch");
    }
    records->push_back({lsn, {checksum, std::move(payload)}});
  }
  if (torn_offset.has_value()) {
    if (::ftruncate(fd, static_cast<off_t>(*torn_offset)) != 0) {
      const Status status = Status::IOError(path, std::strerror(errno));
      ::close(fd);
      return status;
    }
    if (::fsync(fd) != 0) {
      const Status status = Status::IOError(path, std::strerror(errno));
      ::close(fd);
      return status;
    }
  }
  if (::close(fd) != 0) return Status::IOError(path, std::strerror(errno));
  return Status::OK();
}

Status SyncRecoveryDirectory(
    const std::filesystem::path& directory,
    const std::function<Status(DecisionLogFaultPoint)>& fault_injector) {
  if (fault_injector) {
    const Status injected =
        fault_injector(DecisionLogFaultPoint::kBeforeRecoveryDirectoryFsync);
    if (!injected.ok()) return injected;
  }
  return FsyncDirectory(directory);
}

}  // namespace

StatusOr<DurableCommitWriteEstimate> EstimateDurableCommitWriteBytes(
    const std::vector<PrepareRecord>& prepares) {
  if (prepares.empty() || prepares.size() > UINT32_MAX) {
    return Status::InvalidArgument("transaction log estimate",
                                   "missing or excessive prepare records");
  }

  DurableCommitWriteEstimate estimate;
  std::vector<PrepareReference> references;
  references.reserve(prepares.size());
  for (const PrepareRecord& prepare : prepares) {
    if (prepare.participant_shards.size() > UINT32_MAX ||
        prepare.events.empty() || prepare.events.size() > UINT32_MAX) {
      return Status::InvalidArgument("transaction log estimate",
                                     "invalid prepare record cardinality");
    }
    const std::string payload = EncodePrepare(prepare);
    if (payload.size() > kPrepareSegmentBytes - kRecordHeaderBytes) {
      return Status::InvalidArgument("transaction log estimate",
                                     "prepare record exceeds segment size");
    }
    const uint64_t framed_bytes = EncodeFramedRecord(payload).size();
    if (estimate.prepare_bytes > UINT64_MAX - framed_bytes) {
      return Status::ResourceExhausted("transaction log estimate",
                                       "prepare byte estimate overflow");
    }
    estimate.prepare_bytes += framed_bytes;
    references.push_back(PrepareReference{0, 0, 0});
  }

  const CommitDecision decision{0, 0, SystemHlc{}, std::move(references)};
  const std::string decision_payload = EncodeDecision(decision);
  estimate.decision_bytes = EncodeFramedRecord(decision_payload).size();
  if (estimate.prepare_bytes > UINT64_MAX - estimate.decision_bytes) {
    return Status::ResourceExhausted("transaction log estimate",
                                     "durable commit byte estimate overflow");
  }
  estimate.total_bytes = estimate.prepare_bytes + estimate.decision_bytes;
  return estimate;
}

ShardPrepareLog::ShardPrepareLog(std::string path, uint32_t shard_id)
    : path_(std::move(path)), shard_id_(shard_id) {}

Status ShardPrepareLog::Open() {
  std::lock_guard<std::mutex> lock(mutex_);
  records_.clear();
  retained_bytes_.store(0, std::memory_order_relaxed);
  const auto segments = ListPrepareSegments(path_);
  if (!segments.ok()) return segments.status();
  active_segment_id_ = segments.ValueOrDie().empty() ? 1 : segments.ValueOrDie().back();
  for (uint32_t segment_id : segments.ValueOrDie()) {
    const std::string segment_path = PrepareSegmentPath(path_, segment_id);
    std::vector<std::pair<uint64_t, std::pair<uint32_t, std::string>>> entries;
    const Status status = ReadRecords(segment_path, &entries);
    if (!status.ok()) return status;
    std::error_code size_error;
    const uint64_t segment_bytes = std::filesystem::file_size(segment_path, size_error);
    if (size_error) return Status::IOError(segment_path, size_error.message());
    const uint64_t retained = retained_bytes_.load(std::memory_order_relaxed);
    retained_bytes_.store(
        retained > UINT64_MAX - segment_bytes ? UINT64_MAX
                                              : retained + segment_bytes,
        std::memory_order_relaxed);
    for (const auto& entry : entries) {
      if (entry.first > UINT32_MAX) {
        return Status::Corruption(path_, "prepare segment offset exceeds LSN encoding");
      }
      PrepareRecord record;
      if (!DecodePrepare(entry.second.second, &record)) {
        return Status::Corruption(path_, "invalid prepare payload");
      }
      const uint64_t lsn = EncodePrepareLsn(segment_id, static_cast<uint32_t>(entry.first));
      if (!records_.emplace(lsn, std::make_pair(entry.second.first, std::move(record))).second) {
        return Status::Corruption(path_, "duplicate prepare LSN");
      }
    }
  }
  if (!segments.ValueOrDie().empty()) {
    const Status synced = SyncRecoveryDirectory(
        std::filesystem::path(path_).parent_path(), fault_injector_);
    if (!synced.ok()) {
      requires_reopen_ = true;
      return synced;
    }
  }
  requires_reopen_ = false;
  return Status::OK();
}

Status ShardPrepareLog::Append(const PrepareRecord& record, PrepareReference* reference) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (requires_reopen_) {
    return Status::RecoveryRequired("prepare", "reopen after failed append");
  }
  if (reference == nullptr || record.events.empty())
    return Status::InvalidArgument("prepare", "missing output reference or events");
  const std::string payload = EncodePrepare(record);
  if (payload.size() > kPrepareSegmentBytes - kRecordHeaderBytes) {
    return Status::InvalidArgument("prepare", "record exceeds segment size");
  }
  std::string segment_path = PrepareSegmentPath(path_, active_segment_id_);
  std::error_code error;
  const uint64_t existing_size = std::filesystem::exists(segment_path, error)
      ? std::filesystem::file_size(segment_path, error) : 0;
  if (error) return Status::IOError(segment_path, error.message());
  if (existing_size != 0 && existing_size + kRecordHeaderBytes + payload.size() > kPrepareSegmentBytes) {
    if (active_segment_id_ == UINT32_MAX) {
      return Status::ResourceExhausted("prepare", "segment identifier space exhausted");
    }
    ++active_segment_id_;
    segment_path = PrepareSegmentPath(path_, active_segment_id_);
  }
  uint64_t offset;
  uint32_t checksum;
  const AppendRecordResult appended = AppendRecord(
      segment_path, payload, &offset, &checksum, fault_injector_);
  if (!appended.status.ok()) {
    requires_reopen_ = appended.requires_reopen;
    return appended.status;
  }
  if (offset > UINT32_MAX) {
    return Status::Corruption(segment_path, "prepare segment offset exceeds LSN encoding");
  }
  const uint64_t lsn = EncodePrepareLsn(active_segment_id_, static_cast<uint32_t>(offset));
  records_[lsn] = {checksum, record};
  const uint64_t appended_bytes = kRecordHeaderBytes + payload.size();
  const uint64_t retained = retained_bytes_.load(std::memory_order_relaxed);
  retained_bytes_.store(
      retained > UINT64_MAX - appended_bytes ? UINT64_MAX
                                             : retained + appended_bytes,
      std::memory_order_relaxed);
  *reference = PrepareReference{shard_id_, lsn, checksum};
  return Status::OK();
}

Status ShardPrepareLog::Read(const PrepareReference& reference, PrepareRecord* record) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (record == nullptr || reference.shard_id != shard_id_)
    return Status::Corruption(path_, "prepare reference shard mismatch");
  const auto it = records_.find(reference.lsn);
  if (it == records_.end() || it->second.first != reference.checksum)
    return Status::Corruption(path_, "missing or mismatched prepare reference");
  *record = it->second.second;
  return Status::OK();
}

StatusOr<uint64_t> ShardPrepareLog::EndLsn(const PrepareReference& reference) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (reference.shard_id != shard_id_) {
    return Status::Corruption(path_, "prepare reference shard mismatch");
  }
  const auto it = records_.find(reference.lsn);
  if (it == records_.end() || it->second.first != reference.checksum) {
    return Status::Corruption(path_, "missing or mismatched prepare reference");
  }
  const uint64_t record_size = kRecordHeaderBytes + EncodePrepare(it->second.second).size();
  const uint32_t segment_id = PrepareSegmentId(reference.lsn);
  const uint32_t offset = PrepareSegmentOffset(reference.lsn);
  if (segment_id == 0 || record_size > UINT32_MAX || offset > UINT32_MAX - record_size) {
    return Status::Corruption(path_, "prepare LSN overflow");
  }
  return EncodePrepareLsn(segment_id, static_cast<uint32_t>(offset + record_size));
}

Status ShardPrepareLog::TruncateThrough(uint64_t safe_lsn) {
  std::lock_guard<std::mutex> lock(mutex_);
  const uint32_t safe_segment = PrepareSegmentId(safe_lsn);
  if (safe_segment == 0) return Status::InvalidArgument("prepare", "invalid WAL safe LSN");
  const auto segments = ListPrepareSegments(path_);
  if (!segments.ok()) return segments.status();
  std::error_code error;
  bool removed = false;
  for (uint32_t segment_id : segments.ValueOrDie()) {
    if (segment_id >= safe_segment) break;
    const std::string segment_path = PrepareSegmentPath(path_, segment_id);
    const uint64_t segment_bytes = std::filesystem::file_size(segment_path, error);
    if (error) return Status::IOError(segment_path, error.message());
    std::filesystem::remove(segment_path, error);
    if (error) return Status::IOError(segment_path, error.message());
    const uint64_t retained = retained_bytes_.load(std::memory_order_relaxed);
    retained_bytes_.store(retained > segment_bytes ? retained - segment_bytes : 0,
                          std::memory_order_relaxed);
    removed = true;
    const uint64_t first_lsn = EncodePrepareLsn(segment_id, 0);
    const uint64_t next_lsn = EncodePrepareLsn(segment_id + 1, 0);
    records_.erase(records_.lower_bound(first_lsn), records_.lower_bound(next_lsn));
  }
  if (removed) {
    const std::filesystem::path directory = std::filesystem::path(path_).parent_path();
    const int directory_fd = ::open(directory.c_str(), O_RDONLY);
    if (directory_fd < 0) return Status::IOError(directory.string(), std::strerror(errno));
    Status status = Status::OK();
    if (::fsync(directory_fd) != 0) status = Status::IOError(directory.string(), std::strerror(errno));
    if (::close(directory_fd) != 0 && status.ok()) status = Status::IOError(directory.string(), std::strerror(errno));
    if (!status.ok()) return status;
  }
  return Status::OK();
}

DecisionLog::DecisionLog(std::string path) : path_(std::move(path)) {}

Status DecisionLog::Open(uint64_t checkpoint_seq) {
  const bool append_file_exists = std::filesystem::exists(path_);
  std::vector<std::pair<uint64_t, std::pair<uint32_t, std::string>>> entries;
  Status status = ReadRecords(path_, &entries);
  if (!status.ok()) return status;
  commits_.clear();
  checkpoint_seq_ = checkpoint_seq;
  uint64_t expected_seq = entries.empty() ? checkpoint_seq + 1 : 0;
  for (const auto& entry : entries) {
    CommitDecision decision;
    if (!DecodeDecision(entry.second.second, &decision))
      return Status::Corruption(path_, "invalid decision payload");
    if (expected_seq == 0) {
      expected_seq = decision.commit_seq == 1 ? 1 : checkpoint_seq + 1;
    }
    if (decision.commit_seq != expected_seq++)
      return Status::Corruption(path_, "invalid or non-contiguous decision sequence");
    if (decision.commit_seq > checkpoint_seq_) commits_.push_back(std::move(decision));
  }
  next_commit_seq_ = entries.empty() ? checkpoint_seq_ + 1 : expected_seq;
  if (append_file_exists) {
    const Status synced = SyncRecoveryDirectory(
        std::filesystem::path(path_).parent_path(), fault_injector_);
    if (!synced.ok()) {
      requires_reopen_ = true;
      return synced;
    }
  }
  requires_reopen_ = false;
  return Status::OK();
}

Status DecisionLog::AppendCommit(uint64_t txn_id,
                                 const std::vector<PrepareReference>& prepares,
                                 SystemHlc system_time_hlc,
                                 uint64_t* commit_seq) {
  if (commit_seq == nullptr) {
    return Status::InvalidArgument("decision", "missing commit output or prepares");
  }
  const DecisionAppendResult result =
      AppendCommitWithResult(txn_id, prepares, system_time_hlc);
  if (result.status.ok()) *commit_seq = result.commit_seq;
  return result.status;
}

DecisionAppendResult DecisionLog::AppendCommitWithResult(
    uint64_t txn_id, const std::vector<PrepareReference>& prepares,
    SystemHlc system_time_hlc) {
  if (prepares.empty()) {
    return {Status::InvalidArgument("decision", "missing prepares"),
            false, false, 0};
  }
  if (requires_reopen_) {
    return {Status::RecoveryRequired("decision", "reopen after ambiguous append"),
            false, true, 0};
  }
  CommitDecision decision{txn_id, next_commit_seq_, system_time_hlc, prepares};
  uint64_t ignored_lsn;
  uint32_t ignored_checksum;
  const AppendRecordResult appended = AppendRecord(
      path_, EncodeDecision(decision), &ignored_lsn, &ignored_checksum,
      fault_injector_);
  if (!appended.status.ok()) {
    requires_reopen_ = appended.requires_reopen;
    return {appended.status, appended.may_be_durable,
            appended.requires_reopen, decision.commit_seq,
            appended.fsync_attempted, appended.fsync_latency_ns};
  }
  commits_.push_back(std::move(decision));
  return {Status::OK(), false, false, next_commit_seq_++,
          appended.fsync_attempted, appended.fsync_latency_ns};
}

std::optional<TransactionOutcome> DecisionLog::Resolve(uint64_t txn_id) const {
  const auto found = std::find_if(commits_.begin(), commits_.end(), [txn_id](const auto& decision) {
    return decision.txn_id == txn_id;
  });
  if (found == commits_.end()) return std::nullopt;
  return TransactionOutcome{found->txn_id, found->commit_seq, found->system_time_hlc};
}

Status DecisionLog::CheckpointThrough(uint64_t checkpoint_seq) {
  if (checkpoint_seq < checkpoint_seq_) {
    return Status::InvalidArgument("decision", "checkpoint sequence regressed");
  }
  if (checkpoint_seq >= next_commit_seq_) {
    return Status::InvalidArgument("decision", "checkpoint exceeds durable decision prefix");
  }
  std::string encoded;
  for (const CommitDecision& decision : commits_) {
    if (decision.commit_seq > checkpoint_seq) {
      encoded.append(EncodeFramedRecord(EncodeDecision(decision)));
    }
  }
  const Status rewritten = WriteAtomically(path_, encoded);
  if (!rewritten.ok()) return rewritten;
  commits_.erase(std::remove_if(commits_.begin(), commits_.end(),
                                [checkpoint_seq](const auto& decision) {
                                  return decision.commit_seq <= checkpoint_seq;
                                }),
                 commits_.end());
  checkpoint_seq_ = checkpoint_seq;
  return Status::OK();
}

Status WriteTransactionOutcomeIndex(const std::string& path,
                                    const std::vector<TransactionOutcome>& outcomes,
                                    std::array<uint8_t, 32>* checksum) {
  if (checksum == nullptr || !ValidOutcomes(outcomes)) {
    return Status::InvalidArgument("transaction outcomes", "invalid outcome index input");
  }
  const std::string encoded = EncodeOutcomeIndex(outcomes);
  *checksum = Blake3Hash(encoded).bytes;
  return WriteAtomically(path, encoded);
}

StatusOr<std::vector<TransactionOutcome>> ReadTransactionOutcomeIndex(
    const std::string& path, const std::array<uint8_t, 32>& expected_checksum,
    uint64_t expected_checkpoint_seq) {
  std::string encoded;
  const Status read = ReadFile(path, &encoded);
  if (!read.ok()) return read;
  if (Blake3Hash(encoded).bytes != expected_checksum) {
    return Status::Corruption("transaction outcomes", "index checksum mismatch");
  }
  return DecodeOutcomeIndex(encoded, expected_checkpoint_seq);
}

Status RecoverCommittedTransactions(
    const DecisionLog& decisions, const std::vector<ShardPrepareLog*>& shards,
    std::vector<RecoveredTransaction>* recovered) {
  if (recovered == nullptr) return Status::InvalidArgument("recovery", "missing output");
  std::map<uint32_t, ShardPrepareLog*> by_id;
  for (ShardPrepareLog* shard : shards) {
    if (shard == nullptr || !by_id.emplace(shard->shard_id(), shard).second)
      return Status::InvalidArgument("recovery", "duplicate or null shard");
  }
  recovered->clear();
  if (decisions.checkpoint_seq() == UINT64_MAX) {
    return Status::Corruption("decision", "checkpoint sequence exhausted");
  }
  uint64_t expected_seq = decisions.checkpoint_seq() + 1;
  for (const CommitDecision& decision : decisions.commits()) {
    if (decision.commit_seq != expected_seq++)
      return Status::Corruption("decision", "non-contiguous commit sequence");
    RecoveredTransaction transaction{decision.txn_id, decision.commit_seq, {}};
    for (const PrepareReference& ref : decision.prepares) {
      const auto shard = by_id.find(ref.shard_id);
      if (shard == by_id.end()) return Status::Corruption("decision", "referenced shard missing");
      PrepareRecord prepare;
      Status status = shard->second->Read(ref, &prepare);
      if (!status.ok()) return status;
      if (prepare.txn_id != decision.txn_id)
        return Status::Corruption("decision", "prepare transaction mismatch");
      for (const PendingEvent& event : prepare.events) {
        transaction.events.push_back(event.operation == TemporalOperation::kPut
            ? (event.blob_ref.has_value()
                  ? TemporalEvent::PutBlob(event.logical_key, event.valid_from, decision.commit_seq,
                                           event.schema_epoch, *event.blob_ref)
                  : TemporalEvent::Put(event.logical_key, event.valid_from, decision.commit_seq,
                                       event.schema_epoch, event.value))
            : TemporalEvent::Delete(event.logical_key, event.valid_from, decision.commit_seq, event.schema_epoch));
      }
    }
    recovered->push_back(std::move(transaction));
  }
  return Status::OK();
}

}  // namespace cedar
