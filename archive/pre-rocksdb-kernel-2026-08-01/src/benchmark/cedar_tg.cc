// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/cedar_tg.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <unistd.h>

#include "cedar/blob/blob_store.h"

namespace cedar {
namespace {

class SplitMix64 {
 public:
  explicit SplitMix64(uint64_t seed) : state_(seed) {}
  uint64_t Next() {
    uint64_t value = (state_ += 0x9e3779b97f4a7c15ULL);
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
  }
 private:
  uint64_t state_;
};

void AppendU16(std::string* output, uint16_t value) {
  for (uint32_t byte = 0; byte < 2; ++byte) {
    output->push_back(static_cast<char>(value >> (byte * 8)));
  }
}
void AppendU32(std::string* output, uint32_t value) {
  for (uint32_t byte = 0; byte < 4; ++byte) {
    output->push_back(static_cast<char>(value >> (byte * 8)));
  }
}
void AppendU64(std::string* output, uint64_t value) {
  for (uint32_t byte = 0; byte < 8; ++byte) {
    output->push_back(static_cast<char>(value >> (byte * 8)));
  }
}
void AppendKey(std::string* output, const LogicalKey& key) {
  output->push_back(static_cast<char>(key.entity_type()));
  output->push_back(static_cast<char>(key.kind()));
  AppendU64(output, key.entity_id());
  AppendU64(output, key.target_id());
  AppendU16(output, key.column_id());
  AppendU16(output, key.edge_type());
  AppendU64(output, key.edge_id());
}
std::string CanonicalBytes(const CedarTgDataset& dataset) {
  std::string output("CEDAR_TG_V1", 11);
  AppendU64(&output, dataset.config.seed);
  AppendU64(&output, dataset.config.vertex_count);
  AppendU64(&output, dataset.config.edge_count);
  AppendU32(&output, dataset.config.property_events_per_vertex);
  AppendU64(&output, dataset.config.valid_time_span);
  AppendU64(&output, dataset.events.size());
  for (const TemporalEvent& event : dataset.events) {
    AppendKey(&output, event.logical_key());
    AppendU64(&output, event.valid_from());
    AppendU64(&output, event.commit_seq());
    AppendU32(&output, event.schema_epoch());
    output.push_back(static_cast<char>(event.operation()));
    const std::string value = event.value().Encode();
    AppendU64(&output, value.size());
    output.append(value);
  }
  return output;
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

}  // namespace

StatusOr<CedarTgDataset> GenerateCedarTg(const CedarTgConfig& config) {
  if (config.vertex_count == 0 || config.valid_time_span == 0) {
    return Status::InvalidArgument("Cedar-TG", "vertex count and valid-time span must be positive");
  }
  if (config.edge_count != 0 && config.vertex_count < 2) {
    return Status::InvalidArgument("Cedar-TG", "edges require at least two vertices");
  }
  const uint64_t per_vertex = static_cast<uint64_t>(config.property_events_per_vertex);
  if (per_vertex != 0 && config.vertex_count >
      (std::numeric_limits<uint64_t>::max() - config.edge_count) / per_vertex) {
    return Status::InvalidArgument("Cedar-TG", "event count overflows");
  }
  CedarTgDataset dataset;
  dataset.config = config;
  dataset.events.reserve(config.vertex_count + config.edge_count +
                         config.vertex_count * per_vertex);
  SplitMix64 random(config.seed);
  uint64_t commit_seq = 1;
  for (uint64_t vertex_id = 1; vertex_id <= config.vertex_count; ++vertex_id) {
    dataset.events.push_back(TemporalEvent::Put(LogicalKey::VertexExistence(vertex_id), 0,
                                                 commit_seq++, 1, Value::Binary("")));
    ++dataset.vertex_events;
    for (uint32_t version = 0; version < config.property_events_per_vertex; ++version) {
      const uint64_t valid_from = random.Next() % config.valid_time_span;
      const std::string value = "v" + std::to_string(vertex_id) + "-" +
          std::to_string(version) + "-" + std::to_string(random.Next());
      dataset.events.push_back(TemporalEvent::Put(LogicalKey::VertexProperty(vertex_id, 1),
                                                   valid_from, commit_seq++, 1,
                                                   Value::String(value)));
      ++dataset.property_events;
    }
  }
  for (uint64_t edge_id = 1; edge_id <= config.edge_count; ++edge_id) {
    const uint64_t source_id = (random.Next() % config.vertex_count) + 1;
    uint64_t target_id = (random.Next() % config.vertex_count) + 1;
    if (target_id == source_id) target_id = (target_id % config.vertex_count) + 1;
    dataset.events.push_back(TemporalEvent::Put(
        LogicalKey::EdgeExistence(source_id, target_id, 1, edge_id, EntityType::EdgeOut),
        random.Next() % config.valid_time_span, commit_seq++, 1, Value::Binary("")));
    ++dataset.edge_events;
  }
  dataset.dataset_hash = HashCedarTgDataset(dataset);
  return dataset;
}

std::string HashCedarTgDataset(const CedarTgDataset& dataset) {
  return BlobHashHex(Blake3Hash(CanonicalBytes(dataset)));
}

Status WriteCedarTgCanonicalFile(const std::string& path, const CedarTgDataset& dataset) {
  if (path.empty() || dataset.dataset_hash.empty()) {
    return Status::InvalidArgument("Cedar-TG", "missing output path or dataset hash");
  }
  const std::string bytes = CanonicalBytes(dataset);
  if (BlobHashHex(Blake3Hash(bytes)) != dataset.dataset_hash) {
    return Status::Corruption("Cedar-TG", "dataset hash does not match event stream");
  }
  const std::filesystem::path target(path);
  if (target.parent_path().empty()) {
    return Status::InvalidArgument("Cedar-TG", "canonical output path requires a parent directory");
  }
  std::error_code error;
  std::filesystem::create_directories(target.parent_path(), error);
  if (error) return Status::IOError(path, error.message());
  const std::string temporary = path + ".tmp";
  const int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  Status status = WriteAll(fd, bytes, temporary);
  if (status.ok() && ::fsync(fd) != 0) status = Status::IOError(temporary, std::strerror(errno));
  if (::close(fd) != 0 && status.ok()) status = Status::IOError(temporary, std::strerror(errno));
  if (!status.ok()) {
    ::unlink(temporary.c_str());
    return status;
  }
  if (::rename(temporary.c_str(), path.c_str()) != 0) {
    const Status rename_status = Status::IOError(path, std::strerror(errno));
    ::unlink(temporary.c_str());
    return rename_status;
  }
  const std::string directory = target.parent_path().string();
  const int directory_fd = ::open(directory.c_str(), O_RDONLY);
  if (directory_fd < 0) return Status::IOError(directory, std::strerror(errno));
  if (::fsync(directory_fd) != 0) {
    const Status fsync_status = Status::IOError(directory, std::strerror(errno));
    ::close(directory_fd);
    return fsync_status;
  }
  if (::close(directory_fd) != 0) return Status::IOError(directory, std::strerror(errno));
  return Status::OK();
}

}  // namespace cedar
