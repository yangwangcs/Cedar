// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/statistics/stats_snapshot.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <unistd.h>

#include "cedar/core/crc32c.h"

namespace cedar {
namespace {

constexpr char kCheckpointMagic[] = "STS2";

void Put32(std::string* output, uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}

void Put64(std::string* output, uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}

bool Get32(const std::string& input, size_t* offset, uint32_t* value) {
  if (input.size() - *offset < sizeof(uint32_t)) return false;
  *value = 0;
  for (int shift = 0; shift < 32; shift += 8) {
    *value |= static_cast<uint32_t>(static_cast<uint8_t>(input[(*offset)++])) << shift;
  }
  return true;
}

bool Get64(const std::string& input, size_t* offset, uint64_t* value) {
  if (input.size() - *offset < sizeof(uint64_t)) return false;
  *value = 0;
  for (int shift = 0; shift < 64; shift += 8) {
    *value |= static_cast<uint64_t>(static_cast<uint8_t>(input[(*offset)++])) << shift;
  }
  return true;
}

bool IsValidFragment(const StatsFragment& fragment) {
  if (fragment.source_identity == 0 || fragment.format_version != kStatsFragmentFormatV1 ||
      fragment.put_count > fragment.row_count || fragment.delete_count > fragment.row_count ||
      fragment.put_count + fragment.delete_count != fragment.row_count ||
      fragment.distinct_value_count > fragment.put_count) {
    return false;
  }
  return fragment.row_count == 0 ||
         (fragment.min_valid_from <= fragment.max_valid_from &&
          fragment.min_commit_seq <= fragment.max_commit_seq);
}

uint64_t SaturatingAdd(uint64_t left, uint64_t right) {
  return left > std::numeric_limits<uint64_t>::max() - right
             ? std::numeric_limits<uint64_t>::max()
             : left + right;
}

void Merge(StatsFragment* aggregate, const StatsFragment& fragment) {
  if (aggregate->row_count == 0) {
    aggregate->min_valid_from = fragment.min_valid_from;
    aggregate->max_valid_from = fragment.max_valid_from;
    aggregate->min_commit_seq = fragment.min_commit_seq;
    aggregate->max_commit_seq = fragment.max_commit_seq;
  } else {
    aggregate->min_valid_from = std::min(aggregate->min_valid_from, fragment.min_valid_from);
    aggregate->max_valid_from = std::max(aggregate->max_valid_from, fragment.max_valid_from);
    aggregate->min_commit_seq = std::min(aggregate->min_commit_seq, fragment.min_commit_seq);
    aggregate->max_commit_seq = std::max(aggregate->max_commit_seq, fragment.max_commit_seq);
  }
  aggregate->row_count = SaturatingAdd(aggregate->row_count, fragment.row_count);
  aggregate->put_count = SaturatingAdd(aggregate->put_count, fragment.put_count);
  aggregate->delete_count = SaturatingAdd(aggregate->delete_count, fragment.delete_count);
  aggregate->distinct_value_count =
      SaturatingAdd(aggregate->distinct_value_count, fragment.distinct_value_count);
}

void EncodeFragment(std::string* output, const StatsFragment& fragment) {
  Put64(output, fragment.source_identity);
  output->push_back(static_cast<char>(fragment.entity_type));
  output->push_back(static_cast<char>(fragment.column_id));
  output->push_back(static_cast<char>(fragment.column_id >> 8));
  Put64(output, fragment.row_count);
  Put64(output, fragment.put_count);
  Put64(output, fragment.delete_count);
  Put64(output, fragment.distinct_value_count);
  Put64(output, fragment.min_valid_from);
  Put64(output, fragment.max_valid_from);
  Put64(output, fragment.min_commit_seq);
  Put64(output, fragment.max_commit_seq);
  Put32(output, fragment.format_version);
}

bool DecodeFragment(const std::string& input, size_t* offset, StatsFragment* fragment) {
  uint8_t entity_type = 0;
  if (!Get64(input, offset, &fragment->source_identity) || *offset >= input.size()) return false;
  entity_type = static_cast<uint8_t>(input[(*offset)++]);
  if (input.size() - *offset < sizeof(uint16_t)) return false;
  fragment->column_id = static_cast<uint8_t>(input[(*offset)++]);
  fragment->column_id |= static_cast<uint16_t>(static_cast<uint8_t>(input[(*offset)++])) << 8;
  if (!Get64(input, offset, &fragment->row_count) ||
      !Get64(input, offset, &fragment->put_count) ||
      !Get64(input, offset, &fragment->delete_count) ||
      !Get64(input, offset, &fragment->distinct_value_count) ||
      !Get64(input, offset, &fragment->min_valid_from) ||
      !Get64(input, offset, &fragment->max_valid_from) ||
      !Get64(input, offset, &fragment->min_commit_seq) ||
      !Get64(input, offset, &fragment->max_commit_seq) ||
      !Get32(input, offset, &fragment->format_version) ||
      entity_type > static_cast<uint8_t>(EntityType::EdgeIn)) {
    return false;
  }
  fragment->entity_type = static_cast<EntityType>(entity_type);
  return IsValidFragment(*fragment);
}

bool SameFragment(const StatsFragment& left, const StatsFragment& right) {
  return left.source_identity == right.source_identity &&
         left.entity_type == right.entity_type && left.column_id == right.column_id &&
         left.row_count == right.row_count && left.put_count == right.put_count &&
         left.delete_count == right.delete_count &&
         left.distinct_value_count == right.distinct_value_count &&
         left.min_valid_from == right.min_valid_from &&
         left.max_valid_from == right.max_valid_from &&
         left.min_commit_seq == right.min_commit_seq &&
         left.max_commit_seq == right.max_commit_seq &&
         left.format_version == right.format_version;
}

struct DecodedCheckpoint {
  uint64_t generation = 0;
  std::map<uint64_t, StatsFragment> fragments;
};

StatusOr<std::string> EncodeCheckpoint(
    uint64_t generation,
    const std::map<uint64_t, StatsFragment>& fragments) {
  if (fragments.size() > 1000000) {
    return Status::ResourceExhausted(
        "stats checkpoint", "fragment count exceeds bound");
  }
  std::string body(kCheckpointMagic, sizeof(kCheckpointMagic) - 1);
  Put64(&body, generation);
  Put32(&body, static_cast<uint32_t>(fragments.size()));
  for (const auto& entry : fragments) EncodeFragment(&body, entry.second);
  if (body.size() > std::numeric_limits<uint32_t>::max()) {
    return Status::ResourceExhausted(
        "stats checkpoint", "encoded checkpoint exceeds bound");
  }
  std::string data;
  Put32(&data, static_cast<uint32_t>(body.size()));
  Put32(&data, crc32c::Value(body.data(), body.size()));
  data += body;
  return data;
}

StatsSnapshot BuildSnapshotFor(
    const VersionSnapshot& version_set, EntityType entity_type,
    uint16_t column_id, uint64_t statistics_snapshot_id,
    bool checkpoint_corrupt,
    const std::map<uint64_t, StatsFragment>& fragments) {
  StatsSnapshot snapshot;
  snapshot.version_set_generation = version_set.generation;
  snapshot.statistics_snapshot_id = statistics_snapshot_id;
  snapshot.aggregate.entity_type = entity_type;
  snapshot.aggregate.column_id = column_id;
  snapshot.aggregate.format_version = kStatsFragmentFormatV1;
  for (const SstFileMeta& file : version_set.files) {
    if (file.partition.entity_type == entity_type &&
        file.partition.key_kind == LogicalKeyKind::kProperty &&
        file.partition.column_id == column_id) {
      snapshot.source_identities.push_back(file.file_number);
    }
  }
  std::sort(snapshot.source_identities.begin(), snapshot.source_identities.end());
  snapshot.source_identities.erase(
      std::unique(snapshot.source_identities.begin(), snapshot.source_identities.end()),
      snapshot.source_identities.end());
  if (checkpoint_corrupt) return snapshot;

  for (uint64_t source_identity : snapshot.source_identities) {
    const auto found = fragments.find(source_identity);
    if (found == fragments.end() || found->second.entity_type != entity_type ||
        found->second.column_id != column_id || !IsValidFragment(found->second)) {
      return snapshot;
    }
    Merge(&snapshot.aggregate, found->second);
  }
  snapshot.complete = true;
  snapshot.conservative = false;
  return snapshot;
}

StatusOr<DecodedCheckpoint> DecodeCheckpoint(const std::string& data) {
  size_t offset = 0;
  uint32_t body_size = 0;
  uint32_t stored_crc = 0;
  if (!Get32(data, &offset, &body_size) || !Get32(data, &offset, &stored_crc) ||
      body_size != data.size() - offset ||
      crc32c::Value(data.data() + offset, body_size) != stored_crc) {
    return Status::Corruption("stats checkpoint", "checksum");
  }
  const std::string body = data.substr(offset);
  if (body.size() < sizeof(kCheckpointMagic) - 1 ||
      body.compare(0, sizeof(kCheckpointMagic) - 1, kCheckpointMagic) != 0) {
    return Status::Corruption("stats checkpoint", "magic");
  }
  offset = sizeof(kCheckpointMagic) - 1;
  uint64_t generation = 0;
  uint32_t count = 0;
  if (!Get64(body, &offset, &generation) || !Get32(body, &offset, &count) ||
      count > 1000000) {
    return Status::Corruption("stats checkpoint", "fragment count");
  }
  std::map<uint64_t, StatsFragment> fragments;
  for (uint32_t index = 0; index < count; ++index) {
    StatsFragment fragment{};
    if (!DecodeFragment(body, &offset, &fragment) ||
        !fragments.emplace(fragment.source_identity, fragment).second) {
      return Status::Corruption("stats checkpoint", "fragment");
    }
  }
  if (offset != body.size()) return Status::Corruption("stats checkpoint", "trailing bytes");
  return DecodedCheckpoint{generation, std::move(fragments)};
}

}  // namespace

StatusOr<StatsSnapshot> PinnedStatsSnapshot::SnapshotFor(
    const VersionSnapshot& version_set, EntityType entity_type,
    uint16_t column_id) const {
  return BuildSnapshotFor(version_set, entity_type, column_id,
                          statistics_snapshot_id_, checkpoint_corrupt_, fragments_);
}

Status StatsSnapshotStore::Open() {
  std::lock_guard<std::mutex> lock(mutex_);
  fragments_.clear();
  generation_ = 0;
  checkpoint_corrupt_ = false;
  if (!std::filesystem::exists(checkpoint_path_)) return Status::OK();

  const int fd = ::open(checkpoint_path_.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError(checkpoint_path_, std::strerror(errno));
  std::string data;
  char buffer[4096];
  for (;;) {
    const ssize_t read_count = ::read(fd, buffer, sizeof(buffer));
    if (read_count == 0) break;
    if (read_count < 0) {
      const int error = errno;
      ::close(fd);
      return Status::IOError(checkpoint_path_, std::strerror(error));
    }
    data.append(buffer, static_cast<size_t>(read_count));
  }
  ::close(fd);
  const auto decoded = DecodeCheckpoint(data);
  if (!decoded.ok()) {
    checkpoint_corrupt_ = true;
    return Status::OK();
  }
  generation_ = decoded.ValueOrDie().generation;
  fragments_ = std::move(decoded.ValueOrDie().fragments);
  return Status::OK();
}

Status StatsSnapshotStore::Upsert(const StatsFragment& fragment) {
  if (!IsValidFragment(fragment)) {
    return Status::InvalidArgument("stats checkpoint", "invalid statistics fragment");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  return UpsertLocked(fragment, generation_);
}

Status StatsSnapshotStore::UpsertExpected(
    const StatsFragment& fragment, uint64_t expected_generation) {
  if (!IsValidFragment(fragment)) {
    return Status::InvalidArgument(
        "stats checkpoint", "invalid statistics fragment");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  return UpsertLocked(fragment, expected_generation);
}

Status StatsSnapshotStore::UpsertLocked(
    const StatsFragment& fragment, uint64_t expected_generation) {
  if (expected_generation != generation_) {
    return Status::Conflict(
        "stats checkpoint", "statistics generation changed");
  }
  const auto existing = fragments_.find(fragment.source_identity);
  if (existing != fragments_.end() &&
      SameFragment(existing->second, fragment)) {
    return Status::OK();
  }
  if (generation_ == std::numeric_limits<uint64_t>::max()) {
    return Status::ResourceExhausted(
        "stats checkpoint", "statistics generation space exhausted");
  }
  std::map<uint64_t, StatsFragment> projected = fragments_;
  projected[fragment.source_identity] = fragment;
  const uint64_t projected_generation = generation_ + 1;
  const Status persisted =
      PersistProjected(projected_generation, projected);
  if (!persisted.ok()) return persisted;
  fragments_.swap(projected);
  generation_ = projected_generation;
  checkpoint_corrupt_ = false;
  return Status::OK();
}

StatusOr<ResourceProfile> StatsSnapshotStore::EstimateUpsertResources(
    const StatsFragment& fragment,
    uint64_t expected_generation) const {
  if (!IsValidFragment(fragment)) {
    return Status::InvalidArgument(
        "stats checkpoint", "invalid statistics fragment");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (expected_generation != generation_) {
    return Status::Conflict(
        "stats checkpoint", "statistics generation changed");
  }
  if (generation_ == std::numeric_limits<uint64_t>::max()) {
    return Status::ResourceExhausted(
        "stats checkpoint", "statistics generation space exhausted");
  }
  std::map<uint64_t, StatsFragment> projected = fragments_;
  projected[fragment.source_identity] = fragment;
  const auto encoded = EncodeCheckpoint(generation_ + 1, projected);
  if (!encoded.ok()) return encoded.status();
  const uint64_t bytes = encoded.ValueOrDie().size();
  return ResourceProfile{bytes, 0, 2, bytes, 1, 0, 0, bytes, 4};
}

uint64_t StatsSnapshotStore::generation() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return generation_;
}

std::shared_ptr<const PinnedStatsSnapshot> StatsSnapshotStore::Pin() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::shared_ptr<PinnedStatsSnapshot> snapshot(new PinnedStatsSnapshot());
  snapshot->statistics_snapshot_id_ = generation_;
  snapshot->fragments_ = fragments_;
  snapshot->checkpoint_corrupt_ = checkpoint_corrupt_;
  return snapshot;
}

StatusOr<StatsSnapshot> StatsSnapshotStore::SnapshotFor(const VersionSnapshot& version_set,
                                                         EntityType entity_type,
                                                         uint16_t column_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return BuildSnapshotFor(version_set, entity_type, column_id, generation_,
                          checkpoint_corrupt_, fragments_);
}

Status StatsSnapshotStore::PersistProjected(
    uint64_t generation,
    const std::map<uint64_t, StatsFragment>& fragments) const {
  std::error_code error;
  std::filesystem::create_directories(
      std::filesystem::path(checkpoint_path_).parent_path(), error);
  if (error) return Status::IOError(checkpoint_path_, error.message());

  const auto encoded = EncodeCheckpoint(generation, fragments);
  if (!encoded.ok()) return encoded.status();
  const std::string& data = encoded.ValueOrDie();

  const std::string temporary = checkpoint_path_ + ".tmp";
  const int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  const char* cursor = data.data();
  size_t remaining = data.size();
  while (remaining != 0) {
    const ssize_t written = ::write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) continue;
      const int write_error = errno;
      ::close(fd);
      std::filesystem::remove(temporary);
      return Status::IOError(temporary, std::strerror(write_error));
    }
    cursor += written;
    remaining -= static_cast<size_t>(written);
  }
  if (::fsync(fd) != 0) {
    const int sync_error = errno;
    ::close(fd);
    std::filesystem::remove(temporary);
    return Status::IOError(temporary, std::strerror(sync_error));
  }
  if (::close(fd) != 0) {
    const int close_error = errno;
    std::filesystem::remove(temporary);
    return Status::IOError(temporary, std::strerror(close_error));
  }
  if (fault_injector_) {
    const Status injected = fault_injector_(
        StatsSnapshotFaultPoint::kBeforeCheckpointRename);
    if (!injected.ok()) {
      std::filesystem::remove(temporary);
      return injected;
    }
  }
  if (::rename(temporary.c_str(), checkpoint_path_.c_str()) != 0) {
    std::filesystem::remove(temporary);
    return Status::IOError(checkpoint_path_, std::strerror(errno));
  }
  if (fault_injector_) {
    const Status injected = fault_injector_(
        StatsSnapshotFaultPoint::kAfterCheckpointRename);
    if (!injected.ok()) return injected;
  }
  const std::filesystem::path parent =
      std::filesystem::path(checkpoint_path_).parent_path();
  if (!parent.empty()) {
    const int directory_fd = ::open(parent.c_str(), O_RDONLY);
    if (directory_fd < 0) {
      return Status::IOError(parent.string(), std::strerror(errno));
    }
    if (::fsync(directory_fd) != 0) {
      const int sync_error = errno;
      ::close(directory_fd);
      return Status::IOError(parent.string(), std::strerror(sync_error));
    }
    if (::close(directory_fd) != 0) {
      return Status::IOError(parent.string(), std::strerror(errno));
    }
  }
  return Status::OK();
}

}  // namespace cedar
