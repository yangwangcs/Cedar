// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/storage/temporal_scan.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <string>
#include <unistd.h>
#include <utility>

#include "cedar/core/crc32c.h"

namespace cedar {
namespace {

constexpr uint32_t kRawEventRunMagic = 0x31524543U;  // CER1
constexpr uint32_t kRawEventRunVersion = 1;
constexpr uint64_t kRawEventRunMaxRecordBytes = 64ULL << 20;
constexpr size_t kRawEventSortRunEvents = kTcypherStandardBatchCapacity;
constexpr size_t kRawEventMergeFanIn = 16;

void PutU8(std::string* output, uint8_t value) {
  output->push_back(static_cast<char>(value));
}

void PutU16(std::string* output, uint16_t value) {
  for (uint32_t shift = 0; shift < 16; shift += 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}

void PutU32(std::string* output, uint32_t value) {
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}

void PutU64(std::string* output, uint64_t value) {
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}

bool GetU8(const std::string& input, size_t* offset, uint8_t* value) {
  if (*offset == input.size()) return false;
  *value = static_cast<uint8_t>(input[(*offset)++]);
  return true;
}

bool GetU16(const std::string& input, size_t* offset, uint16_t* value) {
  if (input.size() - *offset < 2) return false;
  *value = static_cast<uint16_t>(static_cast<uint8_t>(input[(*offset)++])) |
      static_cast<uint16_t>(static_cast<uint8_t>(input[(*offset)++]) << 8);
  return true;
}

bool GetU32(const std::string& input, size_t* offset, uint32_t* value) {
  if (input.size() - *offset < 4) return false;
  *value = 0;
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    *value |= static_cast<uint32_t>(static_cast<uint8_t>(input[(*offset)++])) << shift;
  }
  return true;
}

bool GetU64(const std::string& input, size_t* offset, uint64_t* value) {
  if (input.size() - *offset < 8) return false;
  *value = 0;
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    *value |= static_cast<uint64_t>(static_cast<uint8_t>(input[(*offset)++])) << shift;
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

Status ReadExact(int fd, size_t bytes, std::string* output, const std::string& path,
                 bool* eof) {
  output->clear();
  output->resize(bytes);
  size_t offset = 0;
  while (offset < bytes) {
    const ssize_t read_bytes = ::read(fd, output->data() + offset, bytes - offset);
    if (read_bytes == 0) {
      if (offset == 0) {
        *eof = true;
        output->clear();
        return Status::OK();
      }
      return Status::Corruption(path, "truncated raw event run");
    }
    if (read_bytes < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    offset += static_cast<size_t>(read_bytes);
  }
  *eof = false;
  return Status::OK();
}

bool EventBefore(const TemporalEvent& left, const TemporalEvent& right) {
  if (left.logical_key() != right.logical_key()) {
    return left.logical_key() < right.logical_key();
  }
  if (left.valid_from() != right.valid_from()) {
    return left.valid_from() > right.valid_from();
  }
  return left.commit_seq() > right.commit_seq();
}

bool SameEventIdentity(const TemporalEvent& left, const TemporalEvent& right) {
  return left.logical_key() == right.logical_key() &&
         left.valid_from() == right.valid_from() &&
         left.commit_seq() == right.commit_seq();
}

bool RawEventBefore(const TemporalEvent& left, const TemporalEvent& right,
                    RawTemporalOrder order) {
  if (order == RawTemporalOrder::kCommitSequence) {
    if (left.commit_seq() != right.commit_seq()) {
      return left.commit_seq() < right.commit_seq();
    }
  } else {
    if (left.valid_from() != right.valid_from()) {
      return left.valid_from() < right.valid_from();
    }
    if (left.commit_seq() != right.commit_seq()) {
      return left.commit_seq() < right.commit_seq();
    }
  }
  return left.logical_key() < right.logical_key();
}

uint64_t RawEventRetentionBytes(const TemporalEvent& event) {
  return sizeof(TemporalEvent) +
      (event.is_blob_reference() ? sizeof(BlobRef)
                                 : event.value().Encode().size());
}

uint64_t MixRawContentHash(uint64_t hash, uint64_t value) {
  hash ^= value;
  return hash * 1099511628211ULL;
}

uint64_t RawContentHash(const TemporalEvent& event) {
  uint64_t hash = 1469598103934665603ULL;
  hash = MixRawContentHash(hash, static_cast<uint8_t>(event.operation()));
  hash = MixRawContentHash(hash, event.schema_epoch());
  hash = MixRawContentHash(hash, event.is_blob_reference());
  if (event.is_blob_reference()) {
    const BlobRef& reference = *event.blob_ref();
    for (uint8_t byte : reference.content_hash.bytes) hash = MixRawContentHash(hash, byte);
    return MixRawContentHash(hash, reference.raw_length);
  }
  const Value& value = event.value();
  hash = MixRawContentHash(hash, static_cast<uint8_t>(value.type()));
  switch (value.type()) {
    case PhysicalType::kInt32:
      return MixRawContentHash(hash, static_cast<uint32_t>(std::get<int32_t>(value.data())));
    case PhysicalType::kInt64:
      return MixRawContentHash(hash, static_cast<uint64_t>(std::get<int64_t>(value.data())));
    case PhysicalType::kTimestamp64:
      return MixRawContentHash(hash, std::get<uint64_t>(value.data()));
    case PhysicalType::kBool:
      return MixRawContentHash(hash, std::get<bool>(value.data()));
    case PhysicalType::kFloat32:
      return MixRawContentHash(hash, std::hash<float>{}(std::get<float>(value.data())));
    case PhysicalType::kFloat64:
      return MixRawContentHash(hash, std::hash<double>{}(std::get<double>(value.data())));
    case PhysicalType::kString:
    case PhysicalType::kBinary:
      for (unsigned char byte : std::get<std::string>(value.data())) {
        hash = MixRawContentHash(hash, byte);
      }
      return hash;
  }
  return hash;
}

TemporalEvent StripRawPayload(const TemporalEvent& event) {
  return event.is_delete()
      ? TemporalEvent::Delete(event.logical_key(), event.valid_from(), event.commit_seq(),
                              event.schema_epoch())
      : TemporalEvent::Put(event.logical_key(), event.valid_from(), event.commit_seq(),
                           event.schema_epoch(), Value::Binary(""));
}

StatusOr<std::string> EncodeRawEvent(const TemporalEvent& event) {
  const LogicalKey& key = event.logical_key();
  std::string output;
  PutU8(&output, static_cast<uint8_t>(key.entity_type()));
  PutU8(&output, static_cast<uint8_t>(key.kind()));
  PutU64(&output, key.entity_id());
  PutU64(&output, key.target_id());
  PutU16(&output, key.column_id());
  PutU16(&output, key.edge_type());
  PutU64(&output, key.edge_id());
  PutU64(&output, event.valid_from());
  PutU64(&output, event.commit_seq());
  PutU32(&output, event.schema_epoch());
  PutU8(&output, static_cast<uint8_t>(event.operation()));
  PutU8(&output, event.is_blob_reference() ? 1 : 0);
  if (event.is_blob_reference()) {
    const BlobRef& blob = *event.blob_ref();
    output.append(reinterpret_cast<const char*>(blob.content_hash.bytes.data()),
                  blob.content_hash.bytes.size());
    PutU64(&output, blob.raw_length);
    PutU32(&output, blob.hint.shard_id);
    PutU64(&output, blob.hint.segment_id);
    PutU64(&output, blob.hint.offset);
  } else {
    const std::string value = event.value().Encode();
    if (value.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::QueryMemoryLimit("temporal scan", "raw event value exceeds run bound");
    }
    PutU32(&output, static_cast<uint32_t>(value.size()));
    output.append(value);
  }
  if (output.size() > kRawEventRunMaxRecordBytes) {
    return Status::QueryMemoryLimit("temporal scan", "raw event run record exceeds bound");
  }
  return output;
}

StatusOr<std::optional<TemporalEvent>> DecodeRawEvent(const std::string& input) {
  size_t offset = 0;
  uint8_t entity_type = 0;
  uint8_t kind = 0;
  uint64_t entity_id = 0;
  uint64_t target_id = 0;
  uint16_t column_id = 0;
  uint16_t edge_type = 0;
  uint64_t edge_id = 0;
  uint64_t valid_from = 0;
  uint64_t commit_seq = 0;
  uint32_t schema_epoch = 0;
  uint8_t operation = 0;
  uint8_t has_blob = 0;
  if (!GetU8(input, &offset, &entity_type) || !GetU8(input, &offset, &kind) ||
      !GetU64(input, &offset, &entity_id) || !GetU64(input, &offset, &target_id) ||
      !GetU16(input, &offset, &column_id) || !GetU16(input, &offset, &edge_type) ||
      !GetU64(input, &offset, &edge_id) || !GetU64(input, &offset, &valid_from) ||
      !GetU64(input, &offset, &commit_seq) || !GetU32(input, &offset, &schema_epoch) ||
      !GetU8(input, &offset, &operation) || !GetU8(input, &offset, &has_blob) ||
      entity_type > static_cast<uint8_t>(EntityType::EdgeIn) ||
      kind > static_cast<uint8_t>(LogicalKeyKind::kProperty) ||
      operation > static_cast<uint8_t>(TemporalOperation::kDelete) || has_blob > 1) {
    return Status::Corruption("temporal scan", "invalid raw event run record");
  }
  const EntityType type = static_cast<EntityType>(entity_type);
  const LogicalKeyKind key_kind = static_cast<LogicalKeyKind>(kind);
  LogicalKey key = type == EntityType::Vertex
      ? (key_kind == LogicalKeyKind::kExistence
             ? LogicalKey::VertexExistence(entity_id)
             : LogicalKey::VertexProperty(entity_id, column_id))
      : (key_kind == LogicalKeyKind::kExistence
             ? LogicalKey::EdgeExistence(entity_id, target_id, edge_type, edge_id, type)
             : LogicalKey::EdgeProperty(entity_id, target_id, edge_type, edge_id,
                                        column_id, type));
  if (has_blob == 1) {
    BlobRef blob;
    if (input.size() - offset < blob.content_hash.bytes.size()) {
      return Status::Corruption("temporal scan", "truncated raw event blob hash");
    }
    std::copy_n(input.data() + offset, blob.content_hash.bytes.size(),
                reinterpret_cast<char*>(blob.content_hash.bytes.data()));
    offset += blob.content_hash.bytes.size();
    if (!GetU64(input, &offset, &blob.raw_length) ||
        !GetU32(input, &offset, &blob.hint.shard_id) ||
        !GetU64(input, &offset, &blob.hint.segment_id) ||
        !GetU64(input, &offset, &blob.hint.offset) || offset != input.size()) {
      return Status::Corruption("temporal scan", "invalid raw event blob reference");
    }
    if (operation != static_cast<uint8_t>(TemporalOperation::kPut)) {
      return Status::Corruption("temporal scan", "DELETE raw event carries a blob reference");
    }
    return std::optional<TemporalEvent>(TemporalEvent::PutBlob(
        std::move(key), valid_from, commit_seq, schema_epoch, std::move(blob)));
  }
  uint32_t value_size = 0;
  if (!GetU32(input, &offset, &value_size) || value_size > input.size() - offset ||
      offset + value_size != input.size()) {
    return Status::Corruption("temporal scan", "invalid raw event value");
  }
  const auto value = Value::Decode(input.substr(offset, value_size));
  if (!value.has_value()) {
    return Status::Corruption("temporal scan", "invalid raw event value encoding");
  }
  if (operation == static_cast<uint8_t>(TemporalOperation::kDelete)) {
    return std::optional<TemporalEvent>(
        TemporalEvent::Delete(std::move(key), valid_from, commit_seq, schema_epoch));
  }
  return std::optional<TemporalEvent>(TemporalEvent::Put(
      std::move(key), valid_from, commit_seq, schema_epoch, *value));
}

class RawEventRun {
 public:
  RawEventRun() = default;
  ~RawEventRun() { Close().IgnoreError(); }
  RawEventRun(const RawEventRun&) = delete;
  RawEventRun& operator=(const RawEventRun&) = delete;

  Status Create() {
    if (fd_ >= 0 || !path_.empty()) {
      return Status::InvalidArgument("temporal scan", "raw event run is already open");
    }
    std::string pattern =
        (std::filesystem::temp_directory_path() / "cedar-temporal-run-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    fd_ = ::mkstemp(writable.data());
    if (fd_ < 0) return Status::IOError(pattern, std::strerror(errno));
    path_.assign(writable.data());
    std::string header;
    PutU32(&header, kRawEventRunMagic);
    PutU32(&header, kRawEventRunVersion);
    return WriteAll(fd_, header, path_);
  }

  Status Append(const TemporalEvent& event) {
    if (fd_ < 0 || reading_) {
      return Status::InvalidArgument("temporal scan", "raw event run is not writable");
    }
    const auto encoded = EncodeRawEvent(event);
    if (!encoded.ok()) return encoded.status();
    std::string record;
    PutU32(&record, static_cast<uint32_t>(encoded.ValueOrDie().size()));
    PutU32(&record, crc32c::Value(encoded.ValueOrDie().data(), encoded.ValueOrDie().size()));
    record.append(encoded.ValueOrDie());
    return WriteAll(fd_, record, path_);
  }

  Status FinishWriting() {
    if (fd_ < 0 || reading_) {
      return Status::InvalidArgument("temporal scan", "raw event run is not writable");
    }
    if (::close(fd_) != 0) return Status::IOError(path_, std::strerror(errno));
    fd_ = -1;
    return Status::OK();
  }

  Status Rewind() {
    if (fd_ >= 0) {
      if (::close(fd_) != 0) return Status::IOError(path_, std::strerror(errno));
      fd_ = -1;
    }
    fd_ = ::open(path_.c_str(), O_RDONLY);
    if (fd_ < 0) return Status::IOError(path_, std::strerror(errno));
    std::string header;
    bool eof = false;
    const Status read = ReadExact(fd_, 8, &header, path_, &eof);
    if (!read.ok()) return read;
    size_t offset = 0;
    uint32_t magic = 0;
    uint32_t version = 0;
    if (eof || !GetU32(header, &offset, &magic) || !GetU32(header, &offset, &version) ||
        magic != kRawEventRunMagic || version != kRawEventRunVersion) {
      return Status::Corruption(path_, "invalid raw event run header");
    }
    reading_ = true;
    return Status::OK();
  }

  StatusOr<std::optional<TemporalEvent>> Next() {
    if (fd_ < 0 || !reading_) {
      return Status::InvalidArgument("temporal scan", "raw event run is not readable");
    }
    std::string header;
    bool eof = false;
    Status read = ReadExact(fd_, 8, &header, path_, &eof);
    if (!read.ok()) return read;
    if (eof) return std::optional<TemporalEvent>();
    size_t offset = 0;
    uint32_t length = 0;
    uint32_t checksum = 0;
    if (!GetU32(header, &offset, &length) || !GetU32(header, &offset, &checksum) ||
        length > kRawEventRunMaxRecordBytes) {
      return Status::Corruption(path_, "invalid raw event run record header");
    }
    std::string payload;
    read = ReadExact(fd_, length, &payload, path_, &eof);
    if (!read.ok()) return read;
    if (eof || crc32c::Value(payload.data(), payload.size()) != checksum) {
      return Status::Corruption(path_, "invalid raw event run record checksum");
    }
    return DecodeRawEvent(payload);
  }

  Status Close() {
    Status status = Status::OK();
    if (fd_ >= 0 && ::close(fd_) != 0) status = Status::IOError(path_, std::strerror(errno));
    fd_ = -1;
    reading_ = false;
    if (!path_.empty() && ::unlink(path_.c_str()) != 0 && errno != ENOENT && status.ok()) {
      status = Status::IOError(path_, std::strerror(errno));
    }
    path_.clear();
    return status;
  }

 private:
  std::string path_;
  int fd_ = -1;
  bool reading_ = false;
};

StatusOr<std::unique_ptr<RawEventRun>> WriteRawEventRun(
    std::vector<TemporalEvent>* events, RawTemporalOrder order) {
  if (events == nullptr || events->empty()) {
    return Status::InvalidArgument("temporal scan", "empty raw event run");
  }
  std::sort(events->begin(), events->end(),
            [order](const TemporalEvent& left, const TemporalEvent& right) {
              return RawEventBefore(left, right, order);
            });
  auto run = std::make_unique<RawEventRun>();
  Status status = run->Create();
  if (!status.ok()) return status;
  for (const TemporalEvent& event : *events) {
    status = run->Append(event);
    if (!status.ok()) return status;
  }
  status = run->FinishWriting();
  if (!status.ok()) return status;
  events->clear();
  return run;
}

StatusOr<std::unique_ptr<RawEventRun>> MergeRawEventRuns(
    std::vector<std::unique_ptr<RawEventRun>> runs,
    RawTemporalOrder order) {
  if (runs.empty() || runs.size() > kRawEventMergeFanIn) {
    return Status::InvalidArgument("temporal scan", "invalid raw event merge fan-in");
  }
  std::vector<std::optional<TemporalEvent>> heads(runs.size());
  std::vector<size_t> heap;
  heap.reserve(runs.size());
  const auto later = [&heads, order](size_t left, size_t right) {
    return RawEventBefore(*heads[right], *heads[left], order);
  };
  const auto push = [&heap, &later](size_t index) {
    heap.push_back(index);
    std::push_heap(heap.begin(), heap.end(), later);
  };
  for (size_t index = 0; index < runs.size(); ++index) {
    Status status = runs[index]->Rewind();
    if (!status.ok()) return status;
    const auto next = runs[index]->Next();
    if (!next.ok()) return next.status();
    heads[index] = next.ValueOrDie();
    if (heads[index].has_value()) push(index);
  }
  auto merged = std::make_unique<RawEventRun>();
  Status status = merged->Create();
  if (!status.ok()) return status;
  while (!heap.empty()) {
    std::pop_heap(heap.begin(), heap.end(), later);
    const size_t index = heap.back();
    heap.pop_back();
    status = merged->Append(*heads[index]);
    if (!status.ok()) return status;
    const auto next = runs[index]->Next();
    if (!next.ok()) return next.status();
    heads[index] = next.ValueOrDie();
    if (heads[index].has_value()) push(index);
  }
  status = merged->FinishWriting();
  if (!status.ok()) return status;
  return merged;
}

StatusOr<int64_t> ToInt64(uint64_t value, const char* field) {
  if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return Status::InvalidArgument("temporal scan", field);
  }
  return static_cast<int64_t>(value);
}

Status AddColumn(ColumnBatch* batch, std::vector<Value> values,
                 std::shared_ptr<void> retention = nullptr,
                 std::vector<bool> validity = {}) {
  return batch->AddVector(
      std::make_shared<FlatVector>(std::move(values), std::move(validity),
                                   std::move(retention)));
}

struct OutputMemoryLease {
  std::shared_ptr<QueryMemoryAccount> account;
  uint64_t bytes = 0;
  ~OutputMemoryLease() { if (account) account->Release(bytes); }
};

uint64_t ValueRetentionBytes(const TemporalEvent& event) {
  if (event.is_blob_reference()) return 64;
  if (event.value().type() == PhysicalType::kString ||
      event.value().type() == PhysicalType::kBinary) {
    return static_cast<uint64_t>(
        std::get<std::string>(event.value().data()).size());
  }
  return sizeof(uint64_t);
}

bool PartitionMayMatch(const BlockPartition& partition,
                       const TemporalScanSpec& spec) {
  return (!spec.entity_type.has_value() ||
          partition.entity_type == *spec.entity_type) &&
         (!spec.key_kind.has_value() || partition.key_kind == *spec.key_kind) &&
         (!spec.edge_type.has_value() || partition.edge_type == *spec.edge_type) &&
         (!spec.column_id.has_value() || partition.column_id == *spec.column_id) &&
         (!spec.schema_epoch.has_value() ||
          partition.schema_epoch == *spec.schema_epoch);
}

class ScanSource {
 public:
  static ScanSource Memtable(TemporalMemTableCursor cursor) {
    ScanSource source;
    source.memtable_ = std::make_unique<TemporalMemTableCursor>(std::move(cursor));
    return source;
  }
  static ScanSource Sst(SstEventCursor cursor) {
    ScanSource source;
    source.sst_ = std::make_unique<SstEventCursor>(std::move(cursor));
    return source;
  }
  static ScanSource Overlay(
      std::shared_ptr<const std::vector<TemporalEvent>> events,
      std::optional<LogicalKey> exact_key = std::nullopt) {
    ScanSource source;
    source.overlay_ = std::move(events);
    source.is_overlay_ = true;
    source.exact_key_ = std::move(exact_key);
    if (source.overlay_ && source.exact_key_.has_value()) {
      source.overlay_position_ = static_cast<size_t>(std::lower_bound(
          source.overlay_->begin(), source.overlay_->end(), *source.exact_key_,
          [](const TemporalEvent& event, const LogicalKey& key) {
            return event.logical_key() < key;
          }) - source.overlay_->begin());
    }
    return source;
  }

  bool valid() const {
    if (memtable_) return memtable_->valid();
    if (sst_) return sst_->valid();
    return overlay_ && overlay_position_ < overlay_->size() &&
        (!exact_key_.has_value() ||
         (*overlay_)[overlay_position_].logical_key() == *exact_key_);
  }
  const TemporalEvent& current() const {
    if (memtable_) return memtable_->current();
    if (sst_) return sst_->current();
    return (*overlay_)[overlay_position_];
  }
  Status Advance() {
    if (memtable_) return memtable_->Advance();
    if (sst_) return sst_->Advance();
    if (!valid()) return Status::NotFound("temporal scan source", "end of input");
    ++overlay_position_;
    return Status::OK();
  }
  const Status& terminal_status() const {
    if (memtable_) return memtable_->terminal_status();
    if (sst_) return sst_->terminal_status();
    return overlay_terminal_status_;
  }
  bool is_overlay() const { return is_overlay_; }
  const SstCursorStats* sst_stats() const {
    return sst_ ? &sst_->stats() : nullptr;
  }
  uint64_t buffered_events() const {
    if (sst_) return sst_->stats().peak_buffered_events;
    return overlay_ ? overlay_->size() : 1;
  }
  uint64_t buffered_bytes() const {
    return sst_ ? sst_->stats().peak_buffered_bytes : 0;
  }

 private:
  std::unique_ptr<TemporalMemTableCursor> memtable_;
  std::unique_ptr<SstEventCursor> sst_;
  std::shared_ptr<const std::vector<TemporalEvent>> overlay_;
  size_t overlay_position_ = 0;
  bool is_overlay_ = false;
  std::optional<LogicalKey> exact_key_;
  Status overlay_terminal_status_ = Status::OK();
};

}  // namespace

struct TemporalScanCursor::Impl {
  explicit Impl(TemporalScanSpec scan_spec,
                std::optional<uint64_t> base_snapshot,
                std::optional<uint64_t> overlay_snapshot)
      : spec(std::move(scan_spec)),
        base_snapshot_seq(std::min(base_snapshot.value_or(spec.snapshot_seq),
                                   spec.snapshot_seq)),
        overlay_snapshot_seq(overlay_snapshot.value_or(spec.snapshot_seq)) {
    last_physical_lease.account = spec.memory_account;
  }

  ~Impl() {
    if (spec.memory_account && heap_charge != 0) {
      spec.memory_account->Release(heap_charge);
    }
    if (spec.memory_account) {
      for (uint64_t bytes : raw_head_charges) {
        if (bytes != 0) spec.memory_account->Release(bytes);
      }
    }
  }

  bool Later(size_t left, size_t right) const {
    const TemporalEvent& left_event = sources[left].current();
    const TemporalEvent& right_event = sources[right].current();
    if (SameEventIdentity(left_event, right_event)) return left > right;
    return EventBefore(right_event, left_event);
  }

  void Push(size_t source) {
    heap.push_back(source);
    std::push_heap(heap.begin(), heap.end(),
                   [this](size_t left, size_t right) { return Later(left, right); });
  }

  size_t Pop() {
    std::pop_heap(heap.begin(), heap.end(),
                  [this](size_t left, size_t right) { return Later(left, right); });
    const size_t source = heap.back();
    heap.pop_back();
    detached_source = source;
    ++stats.events_visited;
    NotifyStats();
    return source;
  }

  void NotifyStats() {
    if (spec.stats_observer) {
      spec.stats_observer(stats.events_visited - notified_events,
                          stats.sst_blocks_read - notified_blocks,
                          stats.max_sst_cursor_buffered_events);
    }
    if (spec.page_read_observer) {
      spec.page_read_observer(stats.sst_pages_read - notified_pages);
    }
    if (spec.storage_stats_observer) {
      spec.storage_stats_observer(
          stats.sst_bytes_read - notified_sst_bytes,
          stats.page_bytes_decoded - notified_page_bytes_decoded,
          stats.page_bytes_skipped - notified_page_bytes_skipped,
          stats.page_decode_count - notified_page_decode_count,
          stats.page_decode_latency_ns - notified_page_decode_latency_ns);
    }
    notified_events = stats.events_visited;
    notified_blocks = stats.sst_blocks_read;
    notified_pages = stats.sst_pages_read;
    notified_sst_bytes = stats.sst_bytes_read;
    notified_page_bytes_decoded = stats.page_bytes_decoded;
    notified_page_bytes_skipped = stats.page_bytes_skipped;
    notified_page_decode_count = stats.page_decode_count;
    notified_page_decode_latency_ns = stats.page_decode_latency_ns;
  }

  Status CheckCancelled(const char* boundary) {
    if (spec.cancellation && spec.cancellation->IsCancelled()) {
      return Fail(Status::QueryCancelled("temporal scan", boundary));
    }
    return Status::OK();
  }

  void ClearLastPhysicalEvent() {
    last_physical_event.reset();
    if (last_physical_lease.account && last_physical_lease.bytes != 0) {
      last_physical_lease.account->Release(last_physical_lease.bytes);
    }
    last_physical_lease.bytes = 0;
  }

  Status RetainLastPhysicalEvent(const TemporalEvent& event) {
    const uint64_t payload_bytes = ValueRetentionBytes(event);
    if (payload_bytes > std::numeric_limits<uint64_t>::max() -
                            sizeof(TemporalEvent)) {
      return Status::QueryMemoryLimit("temporal scan",
                                      "last event charge overflow");
    }
    const uint64_t retained_bytes = sizeof(TemporalEvent) + payload_bytes;
    if (last_physical_lease.account) {
      const Status reserved =
          last_physical_lease.account->Reserve(retained_bytes);
      if (!reserved.ok()) return reserved;
    }
    TemporalEvent retained = event;
    ClearLastPhysicalEvent();
    last_physical_event = std::move(retained);
    last_physical_lease.bytes = retained_bytes;
    return Status::OK();
  }

  Status Fail(Status status) {
    terminal_status = std::move(status);
    ClearLastPhysicalEvent();
    return terminal_status;
  }

  void RefreshSourceStats() {
    stats.sst_blocks_read = 0;
    stats.sst_pages_read = 0;
    stats.source_peak_buffered_events = 0;
    stats.source_peak_buffered_bytes = 0;
    stats.max_sst_cursor_buffered_events = 0;
    stats.sst_bytes_read = 0;
    stats.page_bytes_decoded = 0;
    stats.page_bytes_skipped = 0;
    stats.page_decode_count = 0;
    stats.page_decode_latency_ns = 0;
    for (const ScanSource& source : sources) {
      stats.source_peak_buffered_events += source.buffered_events();
      stats.source_peak_buffered_bytes += source.buffered_bytes();
      if (const SstCursorStats* sst = source.sst_stats()) {
        stats.sst_blocks_read += sst->blocks_read;
        stats.sst_pages_read += sst->pages_read;
        stats.sst_bytes_read += sst->bytes_read;
        stats.page_bytes_decoded += sst->page_bytes_decoded;
        stats.page_bytes_skipped += sst->page_bytes_skipped;
        stats.page_decode_count += sst->page_decode_count;
        stats.page_decode_latency_ns += sst->page_decode_latency_ns;
        stats.max_sst_cursor_buffered_events = std::max(
            stats.max_sst_cursor_buffered_events,
            sst->peak_buffered_events);
      }
    }
    NotifyStats();
  }

  Status AdvanceDetached() {
    if (!detached_source.has_value()) return Status::OK();
    const size_t source = *detached_source;
    const Status advanced = sources[source].Advance();
    detached_source.reset();
    if (!advanced.ok()) {
      if (advanced.IsNotFound() && !sources[source].valid() &&
          sources[source].terminal_status().ok()) {
        return Status::OK();
      }
      terminal_status = advanced;
      RefreshSourceStats();
      return Fail(terminal_status);
    }
    RefreshSourceStats();
    if (sources[source].valid()) Push(source);
    return Status::OK();
  }

  bool MatchesConstraints(const TemporalEvent& event) const {
    const LogicalKey& key = event.logical_key();
    return (!spec.entity_type.has_value() || key.entity_type() == *spec.entity_type) &&
           (!spec.key_kind.has_value() || key.kind() == *spec.key_kind) &&
           (!spec.edge_type.has_value() || key.edge_type() == *spec.edge_type) &&
           (!spec.column_id.has_value() || key.column_id() == *spec.column_id) &&
           (!spec.schema_epoch.has_value() || event.schema_epoch() == *spec.schema_epoch) &&
           (!spec.exact_key.has_value() || key == *spec.exact_key) &&
           (!spec.allowed_candidate_entity_ids ||
            spec.allowed_candidate_entity_ids->count(key.entity_id()) != 0);
  }

  StatusOr<std::optional<TemporalEvent>> NextRawInputEvent() {
    while (!heap.empty()) {
      Status status = CheckCancelled("query cancelled during raw event ordering");
      if (!status.ok()) return status;
      const size_t source = Pop();
      const TemporalEvent& event = sources[source].current();
      const uint64_t source_snapshot = sources[source].is_overlay()
          ? overlay_snapshot_seq : base_snapshot_seq;
      const auto advance = [this]() { return AdvanceDetached(); };
      if (event.commit_seq() > source_snapshot) {
        status = advance();
        if (!status.ok()) return status;
        continue;
      }
      const uint64_t content_hash = RawContentHash(event);
      if (last_raw_key.has_value() && *last_raw_key == event.logical_key() &&
          last_raw_valid_from == event.valid_from() &&
          last_raw_commit_seq == event.commit_seq()) {
        if (last_raw_content_hash != content_hash) {
          return Fail(Status::Corruption(
              "temporal scan", "contradictory duplicate event identity"));
        }
        ++stats.duplicate_events_suppressed;
        status = advance();
        if (!status.ok()) return status;
        continue;
      }
      last_raw_key = event.logical_key();
      last_raw_valid_from = event.valid_from();
      last_raw_commit_seq = event.commit_seq();
      last_raw_content_hash = content_hash;
      if (spec.event_filter) {
        const auto accepted = spec.event_filter(event);
        if (!accepted.ok()) return Fail(accepted.status());
        if (!accepted.ValueOrDie()) {
          status = advance();
          if (!status.ok()) return status;
          continue;
        }
      }
      const bool outside_valid_range = spec.valid_time_start.has_value() &&
          (event.valid_from() < *spec.valid_time_start ||
           event.valid_from() >= *spec.valid_time_end);
      if (!MatchesConstraints(event) || outside_valid_range) {
        status = advance();
        if (!status.ok()) return status;
        continue;
      }
      TemporalEvent raw_event = StripRawPayload(event);
      status = advance();
      if (!status.ok()) return status;
      return std::optional<TemporalEvent>(std::move(raw_event));
    }
    last_raw_key.reset();
    return std::optional<TemporalEvent>();
  }

  Status FlushRawSortRun(std::vector<TemporalEvent>* events,
                         uint64_t* retained_bytes) {
    if (events->empty()) return Status::OK();
    auto run = WriteRawEventRun(events, spec.raw_order);
    if (!run.ok()) return Fail(run.status());
    raw_runs.push_back(std::move(run).ConsumeValueOrDie());
    if (spec.memory_account && *retained_bytes != 0) {
      spec.memory_account->Release(*retained_bytes);
    }
    *retained_bytes = 0;
    return Status::OK();
  }

  Status PrepareRawOrder() {
    if (raw_order_prepared) return Status::OK();
    std::vector<TemporalEvent> events;
    events.reserve(kRawEventSortRunEvents);
    uint64_t retained_bytes = 0;
    for (;;) {
      const auto next = NextRawInputEvent();
      if (!next.ok()) return next.status();
      if (!next.ValueOrDie().has_value()) break;
      const TemporalEvent& event = *next.ValueOrDie();
      const uint64_t bytes = RawEventRetentionBytes(event);
      if (bytes > std::numeric_limits<uint64_t>::max() - retained_bytes) {
        return Fail(Status::QueryMemoryLimit("temporal scan", "raw sort run charge overflow"));
      }
      if (spec.memory_account) {
        const Status reserved = spec.memory_account->Reserve(bytes);
        if (!reserved.ok()) return Fail(reserved);
      }
      retained_bytes += bytes;
      events.push_back(event);
      if (events.size() == kRawEventSortRunEvents) {
        const Status flushed = FlushRawSortRun(&events, &retained_bytes);
        if (!flushed.ok()) return flushed;
      }
    }
    const Status flushed = FlushRawSortRun(&events, &retained_bytes);
    if (!flushed.ok()) return flushed;
    while (raw_runs.size() > kRawEventMergeFanIn) {
      std::vector<std::unique_ptr<RawEventRun>> next_runs;
      next_runs.reserve((raw_runs.size() + kRawEventMergeFanIn - 1) /
                        kRawEventMergeFanIn);
      for (size_t begin = 0; begin < raw_runs.size(); begin += kRawEventMergeFanIn) {
        const size_t end = std::min(raw_runs.size(), begin + kRawEventMergeFanIn);
        std::vector<std::unique_ptr<RawEventRun>> group;
        group.reserve(end - begin);
        for (size_t index = begin; index < end; ++index) {
          group.push_back(std::move(raw_runs[index]));
        }
        auto merged = MergeRawEventRuns(std::move(group), spec.raw_order);
        if (!merged.ok()) return Fail(merged.status());
        next_runs.push_back(std::move(merged).ConsumeValueOrDie());
      }
      raw_runs = std::move(next_runs);
    }
    raw_heads.resize(raw_runs.size());
    raw_head_charges.assign(raw_runs.size(), 0);
    for (size_t index = 0; index < raw_runs.size(); ++index) {
      Status status = raw_runs[index]->Rewind();
      if (!status.ok()) return Fail(status);
      const auto next = raw_runs[index]->Next();
      if (!next.ok()) return Fail(next.status());
      raw_heads[index] = next.ValueOrDie();
      if (raw_heads[index].has_value()) {
        const uint64_t bytes = RawEventRetentionBytes(*raw_heads[index]);
        if (spec.memory_account) {
          status = spec.memory_account->Reserve(bytes);
          if (!status.ok()) return Fail(status);
        }
        raw_head_charges[index] = bytes;
        PushRawHead(index);
      }
    }
    raw_order_prepared = true;
    return Status::OK();
  }

  bool RawHeadLater(size_t left, size_t right) const {
    return RawEventBefore(*raw_heads[right], *raw_heads[left], spec.raw_order);
  }

  void PushRawHead(size_t index) {
    raw_heap.push_back(index);
    std::push_heap(raw_heap.begin(), raw_heap.end(),
                   [this](size_t left, size_t right) {
                     return RawHeadLater(left, right);
                   });
  }

  StatusOr<std::optional<TemporalEvent>> PopRawEvent() {
    if (raw_heap.empty()) return std::optional<TemporalEvent>();
    std::pop_heap(raw_heap.begin(), raw_heap.end(),
                  [this](size_t left, size_t right) {
                    return RawHeadLater(left, right);
                  });
    const size_t index = raw_heap.back();
    raw_heap.pop_back();
    TemporalEvent event = *raw_heads[index];
    if (spec.memory_account && raw_head_charges[index] != 0) {
      spec.memory_account->Release(raw_head_charges[index]);
    }
    raw_head_charges[index] = 0;
    raw_heads[index].reset();
    const auto next = raw_runs[index]->Next();
    if (!next.ok()) return Fail(next.status());
    raw_heads[index] = next.ValueOrDie();
    if (raw_heads[index].has_value()) {
      const uint64_t bytes = RawEventRetentionBytes(*raw_heads[index]);
      if (spec.memory_account) {
        const Status reserved = spec.memory_account->Reserve(bytes);
        if (!reserved.ok()) return Fail(reserved);
      }
      raw_head_charges[index] = bytes;
      PushRawHead(index);
    }
    return std::optional<TemporalEvent>(std::move(event));
  }

  TemporalScanSpec spec;
  uint64_t base_snapshot_seq = 0;
  uint64_t overlay_snapshot_seq = 0;
  std::vector<ScanSource> sources;
  std::vector<size_t> heap;
  uint64_t heap_charge = 0;
  uint64_t notified_events = 0;
  uint64_t notified_blocks = 0;
  uint64_t notified_pages = 0;
  uint64_t notified_sst_bytes = 0;
  uint64_t notified_page_bytes_decoded = 0;
  uint64_t notified_page_bytes_skipped = 0;
  uint64_t notified_page_decode_count = 0;
  uint64_t notified_page_decode_latency_ns = 0;
  std::optional<size_t> detached_source;
  std::optional<LogicalKey> skip_key;
  std::optional<LogicalKey> last_raw_key;
  uint64_t last_raw_valid_from = 0;
  uint64_t last_raw_commit_seq = 0;
  uint64_t last_raw_content_hash = 0;
  OutputMemoryLease last_physical_lease;
  std::optional<TemporalEvent> last_physical_event;
  bool raw_order_prepared = false;
  std::vector<std::unique_ptr<RawEventRun>> raw_runs;
  std::vector<std::optional<TemporalEvent>> raw_heads;
  std::vector<uint64_t> raw_head_charges;
  std::vector<size_t> raw_heap;
  TemporalScanCursorStats stats;
  Status terminal_status = Status::OK();
};

TemporalScanCursor::TemporalScanCursor(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
TemporalScanCursor::TemporalScanCursor(TemporalScanCursor&&) noexcept = default;
TemporalScanCursor& TemporalScanCursor::operator=(TemporalScanCursor&&) noexcept = default;
TemporalScanCursor::~TemporalScanCursor() = default;

const TemporalScanCursorStats& TemporalScanCursor::stats() const {
  return impl_->stats;
}
const Status& TemporalScanCursor::terminal_status() const {
  return impl_->terminal_status;
}

StatusOr<TemporalScanCursor> OpenPinnedTemporalScan(
    PinnedTemporalScanSources pinned, const TemporalScanSpec& spec) {
  if ((spec.snapshot_seq == 0 && !pinned.session_overlay) ||
      spec.batch_capacity == 0 ||
      spec.batch_capacity > kTcypherStandardBatchCapacity) {
    return Status::InvalidArgument("temporal scan", "invalid snapshot or batch capacity");
  }
  if (spec.raw_events &&
      (spec.valid_time_start.has_value() != spec.valid_time_end.has_value() ||
       (spec.valid_time_start.has_value() &&
        *spec.valid_time_start >= *spec.valid_time_end))) {
    return Status::InvalidArgument(
        "temporal scan", "raw event scan has an invalid half-open range");
  }
  if (spec.cancellation && spec.cancellation->IsCancelled()) {
    return Status::QueryCancelled("temporal scan", "query cancelled at open");
  }
  if (pinned.session_overlay && !pinned.overlay_snapshot_seq.has_value()) {
    uint64_t overlay_ceiling = spec.snapshot_seq;
    for (const TemporalEvent& event : *pinned.session_overlay) {
      overlay_ceiling = std::max(overlay_ceiling, event.commit_seq());
    }
    pinned.overlay_snapshot_seq = overlay_ceiling;
  }
  auto impl = std::make_unique<TemporalScanCursor::Impl>(
      spec, pinned.base_snapshot_seq, pinned.overlay_snapshot_seq);
  const size_t maximum_sources = pinned.memtables.size() + pinned.ssts.size() +
      (pinned.session_overlay ? 1 : 0);
  if (maximum_sources > std::numeric_limits<uint64_t>::max() /
                            (sizeof(ScanSource) + sizeof(size_t))) {
    return Status::QueryMemoryLimit("temporal scan", "source heap charge overflow");
  }
  impl->heap_charge = maximum_sources * (sizeof(ScanSource) + sizeof(size_t));
  if (pinned.session_overlay) {
    if (pinned.session_overlay->size() >
        (std::numeric_limits<uint64_t>::max() - impl->heap_charge) /
            sizeof(TemporalEvent)) {
      return Status::QueryMemoryLimit("temporal scan", "overlay charge overflow");
    }
    impl->heap_charge += pinned.session_overlay->size() * sizeof(TemporalEvent);
  }
  if (spec.memory_account && impl->heap_charge != 0) {
    const Status reserved = spec.memory_account->Reserve(impl->heap_charge);
    if (!reserved.ok()) {
      impl->heap_charge = 0;
      return reserved;
    }
  }
  impl->sources.reserve(maximum_sources);
  impl->heap.reserve(maximum_sources);

  TemporalMemTableCursorOptions memtable_options;
  memtable_options.exact_key = spec.exact_key;
  for (auto& memtable : pinned.memtables) {
    auto opened = OpenTemporalMemTableCursor(memtable, memtable_options);
    if (!opened.ok()) return opened.status();
    impl->sources.push_back(ScanSource::Memtable(
        std::move(opened).ConsumeValueOrDie()));
  }
  for (PinnedSstSource& source : pinned.ssts) {
    if (!PartitionMayMatch(source.metadata.partition, spec)) continue;
    std::error_code error;
    const uint64_t file_size = std::filesystem::file_size(source.path, error);
    if (error) return Status::IOError(source.path, error.message());
    if (file_size != source.metadata.file_size) {
      return Status::Corruption("temporal scan", "pinned SST file size differs from Manifest");
    }
    auto opened = OpenSstEventCursor(
        source.path,
        SstCursorOptions{source.metadata.partition, spec.exact_key,
                           spec.cancellation, spec.memory_account,
                           pinned.io_governor,
                           pinned.prefetch_sst_blocks});
    if (!opened.ok()) return opened.status();
    impl->sources.push_back(ScanSource::Sst(
        std::move(opened).ConsumeValueOrDie()));
  }
  if (pinned.session_overlay) {
    if (!std::is_sorted(pinned.session_overlay->begin(),
                        pinned.session_overlay->end(), EventBefore)) {
      return Status::InvalidArgument("temporal scan", "session overlay is not sorted");
    }
    impl->sources.push_back(ScanSource::Overlay(
        std::move(pinned.session_overlay), spec.exact_key));
  }
  impl->stats.source_count = impl->sources.size();
  for (size_t source = 0; source < impl->sources.size(); ++source) {
    if (impl->sources[source].valid()) impl->Push(source);
  }
  impl->RefreshSourceStats();
  impl->stats.peak_retained_events = impl->stats.source_peak_buffered_events;
  if (spec.open_observer) spec.open_observer();
  return TemporalScanCursor(std::move(impl));
}

StatusOr<std::optional<uint64_t>> FindNextPinnedValidBoundary(
    const PinnedTemporalScanSources& pinned, TemporalScanSpec spec,
    const LogicalKey& key, uint64_t after_valid_from) {
  if (spec.snapshot_seq == 0 && !pinned.session_overlay) {
    return Status::InvalidArgument("temporal boundary", "invalid snapshot");
  }
  if (spec.cancellation && spec.cancellation->IsCancelled()) {
    return Status::QueryCancelled("temporal boundary", "query cancelled at open");
  }
  spec.exact_key = key;
  const size_t maximum_sources = pinned.memtables.size() + pinned.ssts.size() +
      (pinned.session_overlay ? 1 : 0);
  const uint64_t merge_charge = static_cast<uint64_t>(maximum_sources) *
      (sizeof(ScanSource) + 3 * sizeof(size_t));
  if (spec.memory_account) {
    const Status reserved = spec.memory_account->Reserve(merge_charge);
    if (!reserved.ok()) return reserved;
  }
  OutputMemoryLease merge_lease{spec.memory_account, merge_charge};
  std::vector<ScanSource> sources;
  std::vector<size_t> heap;
  std::vector<size_t> duplicates;
  sources.reserve(maximum_sources);
  heap.reserve(maximum_sources);
  duplicates.reserve(maximum_sources);
  TemporalMemTableCursorOptions memtable_options;
  memtable_options.exact_key = key;
  for (const auto& memtable : pinned.memtables) {
    auto opened = OpenTemporalMemTableCursor(memtable, memtable_options);
    if (!opened.ok()) return opened.status();
    sources.push_back(ScanSource::Memtable(
        std::move(opened).ConsumeValueOrDie()));
  }
  for (const PinnedSstSource& source : pinned.ssts) {
    if (!PartitionMayMatch(source.metadata.partition, spec)) continue;
    std::error_code error;
    const uint64_t file_size = std::filesystem::file_size(source.path, error);
    if (error) return Status::IOError(source.path, error.message());
    if (file_size != source.metadata.file_size) {
      return Status::Corruption("temporal boundary",
                                "pinned SST file size differs from Manifest");
    }
    auto opened = OpenSstEventCursor(
        source.path,
        SstCursorOptions{source.metadata.partition, key,
                           spec.cancellation, spec.memory_account,
                           pinned.io_governor,
                           false});
    if (!opened.ok()) return opened.status();
    sources.push_back(ScanSource::Sst(
        std::move(opened).ConsumeValueOrDie()));
  }
  if (pinned.session_overlay) {
    if (!std::is_sorted(pinned.session_overlay->begin(),
                        pinned.session_overlay->end(), EventBefore)) {
      return Status::InvalidArgument("temporal boundary",
                                     "session overlay is not sorted");
    }
    sources.push_back(ScanSource::Overlay(pinned.session_overlay, key));
  }
  const auto later = [&sources](size_t left, size_t right) {
    const TemporalEvent& left_event = sources[left].current();
    const TemporalEvent& right_event = sources[right].current();
    if (SameEventIdentity(left_event, right_event)) return left > right;
    return EventBefore(right_event, left_event);
  };
  const auto push = [&heap, &later](size_t source) {
    heap.push_back(source);
    std::push_heap(heap.begin(), heap.end(), later);
  };
  const auto pop = [&heap, &later]() {
    std::pop_heap(heap.begin(), heap.end(), later);
    const size_t source = heap.back();
    heap.pop_back();
    return source;
  };
  for (size_t source = 0; source < sources.size(); ++source) {
    if (sources[source].valid()) push(source);
  }
  uint64_t reported_blocks = 0;
  uint64_t reported_pages = 0;
  uint64_t reported_sst_bytes = 0;
  uint64_t reported_page_bytes_decoded = 0;
  uint64_t reported_page_bytes_skipped = 0;
  uint64_t reported_page_decode_count = 0;
  uint64_t reported_page_decode_latency_ns = 0;
  const auto report = [&](uint64_t event_delta) {
    uint64_t blocks = 0;
    uint64_t pages = 0;
    uint64_t sst_bytes = 0;
    uint64_t page_bytes_decoded = 0;
    uint64_t page_bytes_skipped = 0;
    uint64_t page_decode_count = 0;
    uint64_t page_decode_latency_ns = 0;
    uint64_t max_buffered = 0;
    for (const ScanSource& source : sources) {
      if (const SstCursorStats* stats = source.sst_stats()) {
        blocks += stats->blocks_read;
        pages += stats->pages_read;
        sst_bytes += stats->bytes_read;
        page_bytes_decoded += stats->page_bytes_decoded;
        page_bytes_skipped += stats->page_bytes_skipped;
        page_decode_count += stats->page_decode_count;
        page_decode_latency_ns += stats->page_decode_latency_ns;
        max_buffered = std::max(max_buffered,
                                stats->peak_buffered_events);
      }
    }
    if (spec.stats_observer) {
      spec.stats_observer(event_delta, blocks - reported_blocks, max_buffered);
    }
    if (spec.page_read_observer) {
      spec.page_read_observer(pages - reported_pages);
    }
    if (spec.storage_stats_observer) {
      spec.storage_stats_observer(
          sst_bytes - reported_sst_bytes,
          page_bytes_decoded - reported_page_bytes_decoded,
          page_bytes_skipped - reported_page_bytes_skipped,
          page_decode_count - reported_page_decode_count,
          page_decode_latency_ns - reported_page_decode_latency_ns);
    }
    reported_blocks = blocks;
    reported_pages = pages;
    reported_sst_bytes = sst_bytes;
    reported_page_bytes_decoded = page_bytes_decoded;
    reported_page_bytes_skipped = page_bytes_skipped;
    reported_page_decode_count = page_decode_count;
    reported_page_decode_latency_ns = page_decode_latency_ns;
  };
  report(0);
  if (spec.open_observer) spec.open_observer();
  const uint64_t base_snapshot = std::min(
      pinned.base_snapshot_seq.value_or(spec.snapshot_seq), spec.snapshot_seq);
  uint64_t overlay_snapshot = spec.snapshot_seq;
  if (pinned.overlay_snapshot_seq.has_value()) {
    overlay_snapshot = *pinned.overlay_snapshot_seq;
  } else if (pinned.session_overlay) {
    for (const TemporalEvent& event : *pinned.session_overlay) {
      overlay_snapshot = std::max(overlay_snapshot, event.commit_seq());
    }
  }
  std::optional<uint64_t> boundary;
  while (!heap.empty()) {
    if (sources[heap.front()].current().valid_from() <= after_valid_from) {
      break;
    }
    const size_t source = pop();
    const TemporalEvent& event = sources[source].current();
    const uint64_t source_snapshot = sources[source].is_overlay()
        ? overlay_snapshot : base_snapshot;
    if (event.commit_seq() > source_snapshot) {
      const Status advanced = sources[source].Advance();
      report(1);
      if (!advanced.ok()) return advanced;
      if (sources[source].valid()) push(source);
      continue;
    }
    duplicates.clear();
    duplicates.push_back(source);
    while (!heap.empty() &&
           SameEventIdentity(event, sources[heap.front()].current())) {
      const size_t duplicate = pop();
      if (!SameTemporalEventContent(event, sources[duplicate].current())) {
        return Status::Corruption("temporal boundary",
                                  "contradictory duplicate event identity");
      }
      duplicates.push_back(duplicate);
    }
    if ((!spec.entity_type.has_value() ||
         event.logical_key().entity_type() == *spec.entity_type) &&
        (!spec.key_kind.has_value() ||
         event.logical_key().kind() == *spec.key_kind) &&
        (!spec.column_id.has_value() ||
         event.logical_key().column_id() == *spec.column_id) &&
        (!spec.schema_epoch.has_value() ||
         event.schema_epoch() == *spec.schema_epoch) &&
        event.valid_from() > after_valid_from) {
      boundary = !boundary.has_value()
          ? std::optional<uint64_t>(event.valid_from())
          : std::optional<uint64_t>(std::min(*boundary, event.valid_from()));
    }
    for (size_t duplicate : duplicates) {
      const Status advanced = sources[duplicate].Advance();
      report(1);
      if (!advanced.ok()) return advanced;
      if (sources[duplicate].valid()) push(duplicate);
    }
  }
  return boundary;
}

StatusOr<TemporalScanCursor> OpenTemporalScan(
    const std::vector<TemporalEvent>& candidates, const TemporalScanSpec& spec) {
  auto ordered = std::make_shared<std::vector<TemporalEvent>>(candidates);
  std::sort(ordered->begin(), ordered->end(), EventBefore);
  PinnedTemporalScanSources sources;
  sources.session_overlay = std::move(ordered);
  sources.base_snapshot_seq = spec.snapshot_seq;
  sources.overlay_snapshot_seq = spec.snapshot_seq;
  return OpenPinnedTemporalScan(std::move(sources), spec);
}

Status TemporalScanCursor::NextMorsel(ColumnBatch* batch) {
  if (batch == nullptr) {
    return Status::InvalidArgument("temporal scan", "missing batch output");
  }
  if (!impl_) return Status::InvalidArgument("temporal scan", "cursor was moved from");
  if (!impl_->terminal_status.ok()) return impl_->terminal_status;
  Status status = impl_->CheckCancelled("query cancelled at batch boundary");
  if (!status.ok()) return status;
  status = impl_->AdvanceDetached();
  if (!status.ok()) return status;

  const uint64_t staging_charge =
      static_cast<uint64_t>(impl_->spec.batch_capacity) * sizeof(TemporalEvent);
  const uint64_t output_charge =
      static_cast<uint64_t>(impl_->spec.batch_capacity) *
      10 * (sizeof(Value) + sizeof(bool));
  if (impl_->spec.memory_account) {
    status = impl_->spec.memory_account->Reserve(staging_charge);
    if (!status.ok()) {
      return impl_->Fail(status);
    }
  }
  auto staging_lease = std::make_shared<OutputMemoryLease>();
  staging_lease->account = impl_->spec.memory_account;
  staging_lease->bytes = staging_charge;
  if (impl_->spec.memory_account) {
    status = impl_->spec.memory_account->Reserve(output_charge);
    if (!status.ok()) {
      return impl_->Fail(status);
    }
  }
  auto output_lease = std::make_shared<OutputMemoryLease>();
  output_lease->account = impl_->spec.memory_account;
  output_lease->bytes = output_charge;

  std::vector<TemporalEvent> facts;
  facts.reserve(impl_->spec.batch_capacity);
  if (impl_->spec.raw_events) {
    status = impl_->PrepareRawOrder();
    if (!status.ok()) return status;
  }
  while (facts.size() < impl_->spec.batch_capacity) {
    if (impl_->spec.raw_events) {
      const auto event = impl_->PopRawEvent();
      if (!event.ok()) return event.status();
      if (!event.ValueOrDie().has_value()) break;
      const uint64_t value_charge = ValueRetentionBytes(*event.ValueOrDie());
      if (impl_->spec.memory_account && value_charge != 0) {
        status = impl_->spec.memory_account->Reserve(value_charge);
        if (!status.ok()) return impl_->Fail(status);
        staging_lease->bytes += value_charge;
      }
      facts.push_back(std::move(*event.ValueOrDie()));
      continue;
    }
    if (impl_->heap.empty()) break;
    const size_t source = impl_->Pop();
    const TemporalEvent& event = impl_->sources[source].current();

    const uint64_t source_snapshot = impl_->sources[source].is_overlay()
        ? impl_->overlay_snapshot_seq : impl_->base_snapshot_seq;
    if (event.commit_seq() > source_snapshot) {
      status = impl_->AdvanceDetached();
      if (!status.ok()) return status;
      continue;
    }

    if (impl_->last_physical_event.has_value() &&
        SameEventIdentity(*impl_->last_physical_event, event)) {
      if (!SameTemporalEventContent(*impl_->last_physical_event, event)) {
        return impl_->Fail(Status::Corruption(
            "temporal scan", "contradictory duplicate event identity"));
      }
      ++impl_->stats.duplicate_events_suppressed;
      status = impl_->AdvanceDetached();
      if (!status.ok()) return status;
      continue;
    }
    status = impl_->RetainLastPhysicalEvent(event);
    if (!status.ok()) return impl_->Fail(status);

    if (!impl_->spec.raw_events && impl_->skip_key.has_value()) {
      if (event.logical_key() == *impl_->skip_key) {
        status = impl_->AdvanceDetached();
        if (!status.ok()) return status;
        continue;
      }
      impl_->skip_key.reset();
    }

    const bool outside_valid_range = false;
    if (!impl_->MatchesConstraints(event) || outside_valid_range ||
        (!impl_->spec.raw_events && event.valid_from() > impl_->spec.valid_time) ||
        event.commit_seq() > source_snapshot) {
      status = impl_->AdvanceDetached();
      if (!status.ok()) return status;
      continue;
    }

    if (!impl_->spec.raw_events) impl_->skip_key = event.logical_key();
    if (!event.is_delete() || impl_->spec.retain_selected_tombstone) {
      const uint64_t value_charge = ValueRetentionBytes(event);
      if (impl_->spec.memory_account && value_charge != 0) {
        status = impl_->spec.memory_account->Reserve(value_charge);
        if (!status.ok()) {
          return impl_->Fail(status);
        }
        staging_lease->bytes += value_charge;
      }
      facts.push_back(event);
    }
    if (facts.size() == impl_->spec.batch_capacity) break;
    status = impl_->AdvanceDetached();
    if (!status.ok()) return status;
  }

  if (facts.empty()) {
    impl_->ClearLastPhysicalEvent();
    return Status::NotFound("temporal scan", "end of scan");
  }
  impl_->stats.peak_retained_events = std::max<uint64_t>(
      impl_->stats.peak_retained_events,
      impl_->stats.source_peak_buffered_events + facts.size());

  std::vector<Value> entity_types;
  std::vector<Value> entity_ids;
  std::vector<Value> target_ids;
  std::vector<Value> edge_ids;
  std::vector<Value> edge_types;
  std::vector<Value> column_ids;
  std::vector<Value> valid_from;
  std::vector<Value> commit_seqs;
  std::vector<Value> operations;
  std::vector<Value> values;
  std::vector<bool> value_validity;
  for (std::vector<Value>* column : {&entity_types, &entity_ids, &target_ids,
                                     &edge_ids, &edge_types, &column_ids,
                                     &valid_from, &commit_seqs, &operations,
                                     &values}) {
    column->reserve(facts.size());
  }
  for (const TemporalEvent& fact : facts) {
    Value output_value = fact.value();
    bool output_value_valid = true;
    if (fact.is_blob_reference() && impl_->spec.blob_ref_observer) {
      impl_->spec.blob_ref_observer();
    }
    if (fact.is_blob_reference() && impl_->spec.blob_predicate_probes) {
      output_value_valid = false;
      const BlobRef& reference = *fact.blob_ref();
      for (const BlobPredicateProbe& probe :
           *impl_->spec.blob_predicate_probes) {
        if (probe.content_hash == reference.content_hash &&
            probe.raw_length == reference.raw_length) {
          output_value = probe.literal;
          output_value_valid = true;
          break;
        }
      }
      if (!output_value_valid) output_value = Value::Bool(false);
    } else if (fact.is_blob_reference() && impl_->spec.blob_materializer) {
      auto materialized = impl_->spec.blob_materializer(fact);
      if (!materialized.ok()) return impl_->Fail(materialized.status());
      if (!materialized.ValueOrDie().has_value()) {
        return impl_->Fail(Status::BlobCorruption(
            "temporal scan", "BlobRef materialized no value"));
      }
      output_value = *materialized.ValueOrDie();
      if (impl_->spec.blob_read_observer) {
        impl_->spec.blob_read_observer();
      }
    }
    const uint64_t value_charge = output_value.Encode().size();
    if (impl_->spec.memory_account && value_charge != 0) {
      status = impl_->spec.memory_account->Reserve(value_charge);
      if (!status.ok()) {
        return impl_->Fail(status);
      }
      output_lease->bytes += value_charge;
    }
    const LogicalKey& key = fact.logical_key();
    const auto entity_id = ToInt64(key.entity_id(), "entity id exceeds Int64");
    const auto target_id = ToInt64(key.target_id(), "target id exceeds Int64");
    const auto edge_id = ToInt64(key.edge_id(), "edge id exceeds Int64");
    const auto commit_seq = ToInt64(fact.commit_seq(), "commit sequence exceeds Int64");
    if (!entity_id.ok()) return impl_->Fail(entity_id.status());
    if (!target_id.ok()) return impl_->Fail(target_id.status());
    if (!edge_id.ok()) return impl_->Fail(edge_id.status());
    if (!commit_seq.ok()) return impl_->Fail(commit_seq.status());
    entity_types.push_back(Value::Int32(static_cast<int32_t>(key.entity_type())));
    entity_ids.push_back(Value::Int64(entity_id.ValueOrDie()));
    target_ids.push_back(Value::Int64(target_id.ValueOrDie()));
    edge_ids.push_back(Value::Int64(edge_id.ValueOrDie()));
    edge_types.push_back(Value::Int32(static_cast<int32_t>(key.edge_type())));
    column_ids.push_back(Value::Int32(static_cast<int32_t>(key.column_id())));
    valid_from.push_back(Value::Timestamp(fact.valid_from()));
    commit_seqs.push_back(Value::Int64(commit_seq.ValueOrDie()));
    operations.push_back(Value::Int32(static_cast<int32_t>(fact.operation())));
    values.push_back(std::move(output_value));
    value_validity.push_back(output_value_valid);
  }
  ColumnBatch result(impl_->spec.batch_capacity);
  for (std::vector<Value>* column : {&entity_types, &entity_ids, &target_ids,
                                     &edge_ids, &edge_types, &column_ids,
                                     &valid_from, &commit_seqs, &operations}) {
    status = AddColumn(&result, std::move(*column), output_lease);
    if (!status.ok()) {
      return impl_->Fail(status);
    }
  }
  status = AddColumn(&result, std::move(values), output_lease,
                     std::move(value_validity));
  if (!status.ok()) return impl_->Fail(status);
  *batch = std::move(result);
  return Status::OK();
}

Status VisitPinnedRawTemporalFacts(
    PinnedTemporalScanSources sources, TemporalScanSpec spec,
    const std::function<Status(const RawTemporalFact&)>& visitor) {
  if (!visitor || spec.snapshot_seq == 0 || spec.batch_capacity == 0) {
    return Status::InvalidArgument("temporal scan", "invalid raw fact visitor request");
  }
  spec.raw_events = true;
  auto opened = OpenPinnedTemporalScan(std::move(sources), spec);
  if (!opened.ok()) return opened.status();
  TemporalScanCursor cursor = std::move(opened).ConsumeValueOrDie();
  for (;;) {
    ColumnBatch batch;
    const Status next = cursor.NextMorsel(&batch);
    if (next.IsNotFound()) return Status::OK();
    if (!next.ok()) return next;
    for (uint32_t row = 0; row < batch.row_count(); ++row) {
      const Value* entity_type = batch.ValueRefAt(kEntityType, row);
      const Value* entity_id = batch.ValueRefAt(kEntityId, row);
      const Value* target_id = batch.ValueRefAt(kTargetId, row);
      const Value* edge_id = batch.ValueRefAt(kEdgeId, row);
      const Value* edge_type = batch.ValueRefAt(kEdgeType, row);
      const Value* column_id = batch.ValueRefAt(kColumnId, row);
      const Value* valid_from = batch.ValueRefAt(kValidFrom, row);
      const Value* commit_seq = batch.ValueRefAt(kCommitSeq, row);
      const Value* operation = batch.ValueRefAt(kOperation, row);
      if (entity_type == nullptr || entity_id == nullptr || target_id == nullptr ||
          edge_id == nullptr || edge_type == nullptr || column_id == nullptr ||
          valid_from == nullptr || commit_seq == nullptr || operation == nullptr ||
          entity_type->type() != PhysicalType::kInt32 ||
          entity_id->type() != PhysicalType::kInt64 ||
          target_id->type() != PhysicalType::kInt64 ||
          edge_id->type() != PhysicalType::kInt64 ||
          edge_type->type() != PhysicalType::kInt32 ||
          column_id->type() != PhysicalType::kInt32 ||
          valid_from->type() != PhysicalType::kTimestamp64 ||
          commit_seq->type() != PhysicalType::kInt64 ||
          operation->type() != PhysicalType::kInt32 ||
          std::get<int64_t>(entity_id->data()) < 0 ||
          std::get<int64_t>(target_id->data()) < 0 ||
          std::get<int64_t>(edge_id->data()) < 0 ||
          std::get<int32_t>(edge_type->data()) < 0 ||
          std::get<int32_t>(edge_type->data()) > std::numeric_limits<uint16_t>::max() ||
          std::get<int32_t>(column_id->data()) < 0 ||
          std::get<int32_t>(column_id->data()) > std::numeric_limits<uint16_t>::max() ||
          std::get<int64_t>(commit_seq->data()) <= 0 ||
          std::get<int32_t>(operation->data()) < static_cast<int32_t>(TemporalOperation::kPut) ||
          std::get<int32_t>(operation->data()) > static_cast<int32_t>(TemporalOperation::kDelete)) {
        return Status::Corruption("temporal scan", "raw fact columns are invalid");
      }
      const int32_t type_value = std::get<int32_t>(entity_type->data());
      if (type_value < static_cast<int32_t>(EntityType::Vertex) ||
          type_value > static_cast<int32_t>(EntityType::EdgeIn)) {
        return Status::Corruption("temporal scan", "raw fact entity type is invalid");
      }
      const RawTemporalFact fact{
          static_cast<EntityType>(type_value),
          std::get<int32_t>(column_id->data()) == 0 ? LogicalKeyKind::kExistence
                                                     : LogicalKeyKind::kProperty,
          static_cast<uint64_t>(std::get<int64_t>(entity_id->data())),
          static_cast<uint64_t>(std::get<int64_t>(target_id->data())),
          static_cast<uint64_t>(std::get<int64_t>(edge_id->data())),
          static_cast<uint16_t>(std::get<int32_t>(edge_type->data())),
          static_cast<uint16_t>(std::get<int32_t>(column_id->data())),
          std::get<uint64_t>(valid_from->data()),
          static_cast<uint64_t>(std::get<int64_t>(commit_seq->data())),
          static_cast<TemporalOperation>(std::get<int32_t>(operation->data()))};
      const Status visited = visitor(fact);
      if (!visited.ok()) return visited;
    }
  }
}

}  // namespace cedar
