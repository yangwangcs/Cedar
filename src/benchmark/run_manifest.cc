// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/run_manifest.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <sstream>
#include <unistd.h>

#include "cedar/blob/blob_store.h"

namespace cedar {
namespace {

std::string EscapeJson(const std::string& input) {
  std::string output;
  output.reserve(input.size());
  for (const unsigned char character : input) {
    switch (character) {
      case '"': output.append("\\\""); break;
      case '\\': output.append("\\\\"); break;
      case '\b': output.append("\\b"); break;
      case '\f': output.append("\\f"); break;
      case '\n': output.append("\\n"); break;
      case '\r': output.append("\\r"); break;
      case '\t': output.append("\\t"); break;
      default:
        if (character < 0x20) {
          constexpr char kHex[] = "0123456789abcdef";
          output.append("\\u00");
          output.push_back(kHex[character >> 4]);
          output.push_back(kHex[character & 0x0f]);
        } else {
          output.push_back(static_cast<char>(character));
        }
    }
  }
  return output;
}
void StringField(std::ostringstream* json, const char* name, const std::string& value,
                 bool* first) {
  if (!*first) *json << ',';
  *first = false;
  *json << '"' << name << "\":\"" << EscapeJson(value) << '"';
}
void NumberField(std::ostringstream* json, const char* name, uint64_t value, bool* first) {
  if (!*first) *json << ',';
  *first = false;
  *json << '"' << name << "\":" << value;
}
void BoolField(std::ostringstream* json, const char* name, bool value, bool* first) {
  if (!*first) *json << ',';
  *first = false;
  *json << '"' << name << "\":" << (value ? "true" : "false");
}
Status WriteAll(int fd, const std::string& content, const std::string& path) {
  const char* cursor = content.data();
  size_t remaining = content.size();
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

std::string CanonicalBenchmarkRunManifestPayloadImpl(
    const BenchmarkRunManifest& manifest,
    bool include_instrumentation_profile) {
  std::ostringstream json;
  bool first = true;
  json << '{';
  NumberField(&json, "benchmark_protocol_version", manifest.protocol_version, &first);
  StringField(&json, "source_commit", manifest.source_commit, &first);
  BoolField(&json, "source_dirty_state", manifest.source_dirty, &first);
  StringField(&json, "binary_hash", manifest.binary_hash, &first);
  StringField(&json, "compiler_and_flags", manifest.compiler_and_flags, &first);
  StringField(&json, "os_kernel", manifest.os_kernel, &first);
  StringField(&json, "cpu_model_and_count", manifest.cpu_model_and_count, &first);
  NumberField(&json, "memory_limit_bytes", manifest.memory_limit_bytes, &first);
  StringField(&json, "storage_device_and_filesystem", manifest.storage_device_and_filesystem,
              &first);
  StringField(&json, "resource_profile_id", manifest.resource_profile_id, &first);
  if (include_instrumentation_profile) {
    StringField(&json, "instrumentation_profile_id",
                manifest.instrumentation_profile_id, &first);
  }
  NumberField(&json, "database_format_version", manifest.database_format_version, &first);
  StringField(&json, "language_version", manifest.language_version, &first);
  StringField(&json, "schema_hash", manifest.schema_hash, &first);
  StringField(&json, "dataset_id", manifest.dataset_id, &first);
  StringField(&json, "dataset_hash", manifest.dataset_hash, &first);
  StringField(&json, "dataset_profile_id", manifest.dataset_profile_id, &first);
  NumberField(&json, "dataset_vertex_count", manifest.dataset_vertex_count,
              &first);
  NumberField(&json, "dataset_edge_count", manifest.dataset_edge_count, &first);
  NumberField(&json, "dataset_property_events_per_vertex",
              manifest.dataset_property_events_per_vertex, &first);
  NumberField(&json, "dataset_valid_time_span",
              manifest.dataset_valid_time_span, &first);
  StringField(&json, "source_dataset_kind", manifest.source_dataset_kind, &first);
  StringField(&json, "source_dataset_license", manifest.source_dataset_license, &first);
  StringField(&json, "source_transform_policy", manifest.source_transform_policy, &first);
  NumberField(&json, "generator_seed", manifest.generator_seed, &first);
  StringField(&json, "workload_id", manifest.workload_id, &first);
  StringField(&json, "workload_hash", manifest.workload_hash, &first);
  StringField(&json, "durability_mode", manifest.durability_mode, &first);
  StringField(&json, "cache_mode", manifest.cache_mode, &first);
  NumberField(&json, "worker_limit", manifest.worker_limit, &first);
  StringField(&json, "execution_nonce", manifest.execution_nonce, &first);
  json << '}';
  return json.str();
}

}  // namespace

std::string CanonicalBenchmarkRunManifestPayload(
    const BenchmarkRunManifest& manifest) {
  return CanonicalBenchmarkRunManifestPayloadImpl(manifest, true);
}

std::string SerializeBenchmarkRunManifest(const BenchmarkRunManifest& manifest) {
  std::string payload = CanonicalBenchmarkRunManifestPayload(manifest);
  payload.pop_back();
  if (payload.size() > 1) payload.push_back(',');
  payload.append("\"run_id\":\"");
  payload.append(BenchmarkRunId(manifest));
  payload.append("\"}");
  return payload;
}

std::string BenchmarkRunId(const BenchmarkRunManifest& manifest) {
  return BlobHashHex(Blake3Hash(CanonicalBenchmarkRunManifestPayload(manifest)));
}

std::string BenchmarkRunIdWithImplicitTier0Tier1(
    const BenchmarkRunManifest& manifest) {
  return BlobHashHex(Blake3Hash(
      CanonicalBenchmarkRunManifestPayloadImpl(manifest, false)));
}

Status WriteBenchmarkRunManifest(const std::string& path,
                                 const BenchmarkRunManifest& manifest) {
  if (path.empty() || manifest.dataset_hash.empty() || manifest.workload_hash.empty() ||
      manifest.execution_nonce.empty()) {
    return Status::InvalidArgument("benchmark manifest", "missing required provenance field");
  }
  const std::filesystem::path target(path);
  if (target.parent_path().empty()) {
    return Status::InvalidArgument("benchmark manifest", "manifest path requires a parent directory");
  }
  std::error_code error;
  std::filesystem::create_directories(target.parent_path(), error);
  if (error) return Status::IOError(path, error.message());
  const std::string temporary = path + ".tmp";
  const int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  Status status = WriteAll(fd, SerializeBenchmarkRunManifest(manifest), temporary);
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
