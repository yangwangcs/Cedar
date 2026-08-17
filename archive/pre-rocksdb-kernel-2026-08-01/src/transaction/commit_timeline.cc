// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/transaction/commit_timeline.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>

#include "cedar/core/crc32c.h"

namespace cedar {
namespace {

constexpr uint32_t kTimelineMagic = 0x314c5443U;  // CTL1
constexpr uint32_t kTimelineVersion = 1;
constexpr size_t kTimelineHeaderBytes = 12;

void PutU32(std::string* out, uint32_t value) {
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    out->push_back(static_cast<char>(value >> shift));
  }
}

void PutU64(std::string* out, uint64_t value) {
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    out->push_back(static_cast<char>(value >> shift));
  }
}

bool GetU32(const std::string& input, size_t* offset, uint32_t* value) {
  if (input.size() - *offset < sizeof(uint32_t)) return false;
  *value = 0;
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    *value |= static_cast<uint32_t>(
        static_cast<uint8_t>(input[(*offset)++])) << shift;
  }
  return true;
}

bool GetU64(const std::string& input, size_t* offset, uint64_t* value) {
  if (input.size() - *offset < sizeof(uint64_t)) return false;
  *value = 0;
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    *value |= static_cast<uint64_t>(
        static_cast<uint8_t>(input[(*offset)++])) << shift;
  }
  return true;
}

std::string Encode(const std::vector<CommitTimelineEntry>& entries) {
  std::string body;
  PutU32(&body, kTimelineMagic);
  PutU32(&body, kTimelineVersion);
  PutU32(&body, static_cast<uint32_t>(entries.size()));
  for (const CommitTimelineEntry& entry : entries) {
    PutU64(&body, entry.commit_seq);
    PutU64(&body, entry.system_time_hlc.physical_us);
    PutU32(&body, entry.system_time_hlc.logical_counter);
  }
  std::string encoded;
  PutU32(&encoded, static_cast<uint32_t>(body.size()));
  PutU32(&encoded, crc32c::Value(body.data(), body.size()));
  encoded.append(body);
  return encoded;
}

Status Decode(const std::string& encoded,
              std::vector<CommitTimelineEntry>* entries) {
  if (entries == nullptr || encoded.size() < 8) {
    return Status::Corruption("commit timeline", "truncated checkpoint");
  }
  size_t offset = 0;
  uint32_t body_size;
  uint32_t checksum;
  if (!GetU32(encoded, &offset, &body_size) || !GetU32(encoded, &offset, &checksum) ||
      body_size != encoded.size() - offset ||
      crc32c::Value(encoded.data() + offset, body_size) != checksum) {
    return Status::Corruption("commit timeline", "invalid checkpoint checksum");
  }
  const std::string body = encoded.substr(offset);
  offset = 0;
  uint32_t magic;
  uint32_t version;
  uint32_t count;
  if (!GetU32(body, &offset, &magic) || !GetU32(body, &offset, &version) ||
      !GetU32(body, &offset, &count) || magic != kTimelineMagic ||
      version != kTimelineVersion || count > 100000000U ||
      body.size() - offset != static_cast<size_t>(count) * 20) {
    return Status::Corruption("commit timeline", "invalid checkpoint header");
  }
  entries->clear();
  SystemHlc previous{0, 0};
  for (uint32_t index = 0; index < count; ++index) {
    CommitTimelineEntry entry;
    if (!GetU64(body, &offset, &entry.commit_seq) ||
        !GetU64(body, &offset, &entry.system_time_hlc.physical_us) ||
        !GetU32(body, &offset, &entry.system_time_hlc.logical_counter) ||
        entry.commit_seq != static_cast<uint64_t>(index) + 1 ||
        (index > 0 && !(entry.system_time_hlc > previous))) {
      return Status::Corruption("commit timeline", "invalid checkpoint ordering");
    }
    previous = entry.system_time_hlc;
    entries->push_back(entry);
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

Status WriteAll(int fd, const std::string& data, const std::string& path) {
  const char* cursor = data.data();
  size_t remaining = data.size();
  while (remaining > 0) {
    const ssize_t count = ::write(fd, cursor, remaining);
    if (count < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    cursor += count;
    remaining -= static_cast<size_t>(count);
  }
  return Status::OK();
}

}  // namespace

CommitTimeline::CommitTimeline(std::string checkpoint_path)
    : checkpoint_path_(std::move(checkpoint_path)) {}

Status CommitTimeline::Open() {
  entries_.clear();
  if (!std::filesystem::exists(checkpoint_path_)) return Status::OK();
  std::string contents;
  Status status = ReadFile(checkpoint_path_, &contents);
  if (!status.ok()) return status;
  return Decode(contents, &entries_);
}

Status CommitTimeline::RestoreFromOutcomes(
    const std::vector<TransactionOutcome>& outcomes) {
  SystemHlc previous{0, 0};
  for (size_t index = 0; index < outcomes.size(); ++index) {
    const TransactionOutcome& outcome = outcomes[index];
    if (outcome.commit_seq != index + 1 || outcome.txn_id == 0 ||
        (index != 0 && !(outcome.system_time_hlc > previous))) {
      return Status::Corruption("commit timeline", "invalid transaction outcome ordering");
    }
    previous = outcome.system_time_hlc;
    const CommitTimelineEntry expected{outcome.commit_seq, outcome.system_time_hlc};
    if (index < entries_.size()) {
      if (entries_[index].commit_seq != expected.commit_seq ||
          !(entries_[index].system_time_hlc == expected.system_time_hlc)) {
        return Status::Corruption("commit timeline", "outcome index mismatch");
      }
    } else {
      entries_.push_back(expected);
    }
  }
  return Status::OK();
}

Status CommitTimeline::RestoreFromDecisions(
    const std::vector<CommitDecision>& decisions) {
  for (const CommitDecision& decision : decisions) {
    if (decision.commit_seq == 0 || decision.commit_seq > entries_.size() + 1 ||
        (decision.commit_seq > 1 &&
         (entries_.empty() || decision.commit_seq - 2 >= entries_.size() ||
          !(decision.system_time_hlc > entries_[decision.commit_seq - 2].system_time_hlc)))) {
      return Status::Corruption("commit timeline", "invalid decision ordering");
    }
    const CommitTimelineEntry expected{decision.commit_seq, decision.system_time_hlc};
    const size_t index = static_cast<size_t>(decision.commit_seq - 1);
    if (index < entries_.size()) {
      if (entries_[index].commit_seq != expected.commit_seq ||
          !(entries_[index].system_time_hlc == expected.system_time_hlc)) {
        return Status::Corruption("commit timeline", "checkpoint decision mismatch");
      }
    } else {
      entries_.push_back(expected);
    }
  }
  return Status::OK();
}

Status CommitTimeline::Allocate(uint64_t wall_clock_us, SystemHlc* result) const {
  if (result == nullptr) return Status::InvalidArgument("commit timeline", "missing HLC output");
  if (entries_.empty() || wall_clock_us > entries_.back().system_time_hlc.physical_us) {
    *result = SystemHlc{wall_clock_us, 0};
    return Status::OK();
  }
  const SystemHlc& previous = entries_.back().system_time_hlc;
  if (previous.logical_counter == std::numeric_limits<uint32_t>::max()) {
    if (previous.physical_us == std::numeric_limits<uint64_t>::max()) {
      return Status::InvalidArgument("commit timeline", "HLC physical component overflow");
    }
    *result = SystemHlc{previous.physical_us + 1, 0};
    return Status::OK();
  }
  *result = SystemHlc{previous.physical_us, previous.logical_counter + 1};
  return Status::OK();
}

Status CommitTimeline::AddDurableCommit(uint64_t commit_seq, SystemHlc system_time_hlc) {
  if (commit_seq != entries_.size() + 1 ||
      (!entries_.empty() && !(system_time_hlc > entries_.back().system_time_hlc))) {
    return Status::Corruption("commit timeline", "non-monotonic durable mapping");
  }
  entries_.push_back(CommitTimelineEntry{commit_seq, system_time_hlc});
  return Status::OK();
}

Status CommitTimeline::Checkpoint() const {
  std::error_code error;
  const std::filesystem::path destination(checkpoint_path_);
  const std::filesystem::path directory = destination.parent_path();
  std::filesystem::create_directories(directory, error);
  if (error) return Status::IOError(directory.string(), error.message());
  const std::string temporary = checkpoint_path_ + ".tmp";
  const std::string encoded = Encode(entries_);
  int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  Status status = WriteAll(fd, encoded, temporary);
  if (status.ok() && ::fsync(fd) != 0) status = Status::IOError(temporary, std::strerror(errno));
  if (::close(fd) != 0 && status.ok()) status = Status::IOError(temporary, std::strerror(errno));
  if (!status.ok()) return status;
  if (::rename(temporary.c_str(), checkpoint_path_.c_str()) != 0) {
    return Status::IOError(checkpoint_path_, std::strerror(errno));
  }
  int directory_fd = ::open(directory.c_str(), O_RDONLY);
  if (directory_fd < 0) return Status::IOError(directory.string(), std::strerror(errno));
  if (::fsync(directory_fd) != 0) {
    const Status fsync_status = Status::IOError(directory.string(), std::strerror(errno));
    ::close(directory_fd);
    return fsync_status;
  }
  if (::close(directory_fd) != 0) return Status::IOError(directory.string(), std::strerror(errno));
  return Status::OK();
}

StatusOr<uint64_t> CommitTimeline::ResolveAsOf(
    uint64_t timestamp_us, uint64_t visible_seq_ceiling) const {
  uint64_t result = 0;
  for (const CommitTimelineEntry& entry : entries_) {
    if (entry.commit_seq > visible_seq_ceiling || entry.system_time_hlc.physical_us > timestamp_us) {
      break;
    }
    result = entry.commit_seq;
  }
  return result;
}

}  // namespace cedar
