// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "cedar/transaction/database_format.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>

#include "cedar/core/crc32c.h"
#include "cedar/storage/storage_layout.h"

namespace cedar {
namespace {

constexpr uint32_t kFormatMagic = 0x4d464443U;  // CDFM
constexpr uint32_t kOldFormatMagic = 0x32544d46U;  // FMT2
constexpr uint32_t kFormatEncodingVersion = 1;
constexpr uint32_t kMaximumLocationBytes = 4096;
constexpr uint32_t kMaximumShards = 65536;

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

void PutString(std::string* output, const std::string& value) {
  PutU32(output, static_cast<uint32_t>(value.size()));
  output->append(value);
}

bool GetString(const std::string& input, size_t* offset, std::string* value) {
  uint32_t length = 0;
  if (!GetU32(input, offset, &length) || length > kMaximumLocationBytes ||
      length > input.size() - *offset) {
    return false;
  }
  *value = input.substr(*offset, length);
  *offset += length;
  return true;
}

Status ValidateLocation(const std::string& location) {
  const std::filesystem::path path(location);
  if (location.empty() || location.size() > kMaximumLocationBytes || path.is_absolute()) {
    return Status::InvalidArgument("FORMAT", "storage location must be a relative path");
  }
  for (const auto& component : path) {
    if (component == ".." || component == ".") {
      return Status::InvalidArgument("FORMAT", "unsafe storage location");
    }
  }
  return Status::OK();
}

Status ValidateStructure(const DatabaseFormat& format) {
  if (format.shard_count == 0 || format.shard_count > kMaximumShards ||
      format.shard_wal_locations.size() != format.shard_count) {
    return Status::InvalidArgument("FORMAT", "invalid shard configuration");
  }
  if (format.hash_algorithm != CedarHashAlgorithm::kFnv1a64) {
    return Status::InvalidArgument("FORMAT", "unknown hash algorithm identity");
  }
  Status status = ValidateLocation(format.manifest_location);
  if (!status.ok()) return status;
  status = ValidateLocation(format.decision_log_location);
  if (!status.ok()) return status;
  for (const std::string& location : format.shard_wal_locations) {
    status = ValidateLocation(location);
    if (!status.ok()) return status;
  }
  return Status::OK();
}

Status FsyncDirectory(const std::filesystem::path& directory) {
  const std::string path = directory.string();
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError(path, std::strerror(errno));
  Status status = Status::OK();
  if (::fsync(fd) != 0) status = Status::IOError(path, std::strerror(errno));
  if (::close(fd) != 0 && status.ok()) status = Status::IOError(path, std::strerror(errno));
  return status;
}

Status EnsureDirectory(const std::filesystem::path& directory) {
  if (directory.empty()) return Status::InvalidArgument("FORMAT", "missing database directory");
  std::error_code error;
  const bool existed = std::filesystem::exists(directory, error);
  if (error) return Status::IOError(directory.string(), error.message());
  if (!existed) {
    if (!std::filesystem::create_directories(directory, error) || error) {
      return Status::IOError(directory.string(), error.message());
    }
    const std::filesystem::path parent = directory.parent_path();
    if (!parent.empty()) {
      const Status synced = FsyncDirectory(parent);
      if (!synced.ok()) return synced;
    }
  }
  if (!std::filesystem::is_directory(directory, error) || error) {
    return Status::IOError(directory.string(),
                           error ? error.message() : "database path is not a directory");
  }
  return Status::OK();
}

Status WriteAll(int fd, const std::string& bytes, const std::string& path) {
  const char* cursor = bytes.data();
  size_t remaining = bytes.size();
  while (remaining != 0) {
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

Status ReadAll(const std::string& path, std::string* bytes) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError(path, std::strerror(errno));
  bytes->clear();
  char buffer[4096];
  for (;;) {
    const ssize_t count = ::read(fd, buffer, sizeof(buffer));
    if (count == 0) break;
    if (count < 0) {
      if (errno == EINTR) continue;
      const Status status = Status::IOError(path, std::strerror(errno));
      ::close(fd);
      return status;
    }
    bytes->append(buffer, static_cast<size_t>(count));
  }
  if (::close(fd) != 0) return Status::IOError(path, std::strerror(errno));
  return Status::OK();
}

std::string Encode(const DatabaseFormat& format) {
  std::string payload;
  PutU32(&payload, format.format_version);
  PutU32(&payload, format.shard_count);
  PutU32(&payload, static_cast<uint32_t>(format.hash_algorithm));
  PutU64(&payload, format.hash_seed);
  PutString(&payload, format.manifest_location);
  PutString(&payload, format.decision_log_location);
  PutU32(&payload, static_cast<uint32_t>(format.shard_wal_locations.size()));
  for (const std::string& location : format.shard_wal_locations) {
    PutString(&payload, location);
  }

  std::string encoded;
  PutU32(&encoded, kFormatMagic);
  PutU32(&encoded, kFormatEncodingVersion);
  PutU32(&encoded, static_cast<uint32_t>(payload.size()));
  encoded.append(payload);
  PutU32(&encoded, crc32c::Value(encoded.data(), encoded.size()));
  return encoded;
}

StatusOr<DatabaseFormat> Decode(const std::string& encoded) {
  constexpr size_t kEnvelopeBytes = 4 * sizeof(uint32_t);
  if (encoded.size() < kEnvelopeBytes) {
    size_t magic_offset = 0;
    uint32_t magic = 0;
    if (GetU32(encoded, &magic_offset, &magic) && magic == kOldFormatMagic) {
      return Status::NotSupported("FORMAT", "old database format magic is not supported");
    }
    return Status::Corruption("FORMAT", "truncated metadata");
  }
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t encoding_version = 0;
  uint32_t payload_size = 0;
  if (!GetU32(encoded, &offset, &magic) ||
      !GetU32(encoded, &offset, &encoding_version) ||
      !GetU32(encoded, &offset, &payload_size) || magic != kFormatMagic ||
      encoding_version != kFormatEncodingVersion ||
      payload_size != encoded.size() - offset - sizeof(uint32_t)) {
    if (magic == kOldFormatMagic) {
      return Status::NotSupported("FORMAT", "old database format magic is not supported");
    }
    return Status::Corruption("FORMAT", "invalid metadata envelope");
  }
  uint32_t stored_checksum = 0;
  size_t checksum_offset = encoded.size() - sizeof(uint32_t);
  if (!GetU32(encoded, &checksum_offset, &stored_checksum) ||
      stored_checksum != crc32c::Value(encoded.data(), encoded.size() - sizeof(uint32_t))) {
    return Status::Corruption("FORMAT", "checksum mismatch");
  }

  DatabaseFormat format;
  uint32_t algorithm = 0;
  uint32_t wal_count = 0;
  if (!GetU32(encoded, &offset, &format.format_version) ||
      !GetU32(encoded, &offset, &format.shard_count) ||
      !GetU32(encoded, &offset, &algorithm) ||
      !GetU64(encoded, &offset, &format.hash_seed) ||
      !GetString(encoded, &offset, &format.manifest_location) ||
      !GetString(encoded, &offset, &format.decision_log_location) ||
      !GetU32(encoded, &offset, &wal_count) || wal_count > kMaximumShards) {
    return Status::Corruption("FORMAT", "invalid metadata payload");
  }
  format.hash_algorithm = static_cast<CedarHashAlgorithm>(algorithm);
  format.shard_wal_locations.reserve(wal_count);
  for (uint32_t index = 0; index < wal_count; ++index) {
    std::string location;
    if (!GetString(encoded, &offset, &location)) {
      return Status::Corruption("FORMAT", "invalid shard WAL location");
    }
    format.shard_wal_locations.push_back(std::move(location));
  }
  if (offset != encoded.size() - sizeof(uint32_t)) {
    return Status::Corruption("FORMAT", "trailing metadata bytes");
  }
  const Status structure = ValidateStructure(format);
  if (!structure.ok()) return Status::Corruption("FORMAT", structure.ToString());
  return format;
}

Status ValidateIdentity(const DatabaseFormat& actual,
                        const DatabaseFormat& expected) {
  if (actual.format_version != kCedarDatabaseFormatVersion) {
    return Status::NotSupported("FORMAT", "unsupported database format version " +
                                             std::to_string(actual.format_version));
  }
  if (actual.shard_count != expected.shard_count) {
    return Status::InvalidArgument("FORMAT", "configured shard count does not match database");
  }
  if (actual.hash_algorithm != expected.hash_algorithm ||
      actual.hash_seed != expected.hash_seed) {
    return Status::InvalidArgument("FORMAT", "configured shard hash does not match database");
  }
  if (actual.manifest_location != expected.manifest_location ||
      actual.decision_log_location != expected.decision_log_location ||
      actual.shard_wal_locations != expected.shard_wal_locations) {
    if (actual.manifest_location == storage_layout::kOldManifestRelativePath) {
      return Status::NotSupported("FORMAT", "old Manifest layout is not supported");
    }
    return Status::Corruption("FORMAT", "persistent storage locations do not match current layout");
  }
  return Status::OK();
}

}  // namespace

DatabaseFormat MakeDatabaseFormat(uint32_t shard_count, uint64_t hash_seed) {
  DatabaseFormat format;
  format.shard_count = shard_count;
  format.hash_seed = hash_seed;
  format.manifest_location = storage_layout::kManifestRelativePath;
  format.decision_log_location = "decision/DECISION";
  format.shard_wal_locations.reserve(shard_count);
  for (uint32_t shard = 0; shard < shard_count; ++shard) {
    format.shard_wal_locations.push_back(
        "shards/" + std::to_string(shard) + "/wal/PREPARE");
  }
  return format;
}

Status WriteDatabaseFormat(const std::string& path, const DatabaseFormat& format) {
  const Status valid = ValidateStructure(format);
  if (!valid.ok()) return valid;
  const std::filesystem::path target(path);
  const Status directory = EnsureDirectory(target.parent_path());
  if (!directory.ok()) return directory;
  const std::string temporary = path + ".tmp";
  const int fd = ::open(temporary.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  const std::string encoded = Encode(format);
  Status status = WriteAll(fd, encoded, temporary);
  if (status.ok() && ::fsync(fd) != 0) {
    status = Status::IOError(temporary, std::strerror(errno));
  }
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
  return FsyncDirectory(target.parent_path());
}

StatusOr<DatabaseFormat> ReadDatabaseFormat(const std::string& path) {
  std::string encoded;
  const Status read = ReadAll(path, &encoded);
  if (!read.ok()) return read;
  return Decode(encoded);
}

Status CreateOrValidateDatabaseFormat(const std::string& db_path,
                                      const DatabaseFormat& expected) {
  const Status valid = ValidateStructure(expected);
  if (!valid.ok()) return valid;
  const std::filesystem::path root(db_path);
  const Status directory = EnsureDirectory(root);
  if (!directory.ok()) return directory;
  const std::filesystem::path format_path = root / "FORMAT";
  std::error_code error;
  if (!std::filesystem::exists(format_path, error)) {
    if (error) return Status::IOError(format_path.string(), error.message());
    if (!std::filesystem::is_empty(root, error) || error) {
      return error ? Status::IOError(root.string(), error.message())
                   : Status::NotSupported("FORMAT", "nonempty legacy database has no format metadata");
    }
    return WriteDatabaseFormat(format_path.string(), expected);
  }
  const auto actual = ReadDatabaseFormat(format_path.string());
  if (!actual.ok()) return actual.status();
  return ValidateIdentity(actual.ValueOrDie(), expected);
}

}  // namespace cedar
