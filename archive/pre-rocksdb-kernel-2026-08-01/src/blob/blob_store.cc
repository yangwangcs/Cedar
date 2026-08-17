// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/blob/blob_store.h"

#include <algorithm>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

#include <lz4.h>

extern "C" {
#include "blake3.h"
}

#include "cedar/core/crc32c.h"

namespace cedar {
namespace {

constexpr uint32_t kBlobBlockMagic = 0x31424243U;   // CBB1
constexpr uint32_t kBlobRecordMagic = 0x31524243U;  // CBR1
constexpr uint32_t kBlobIndexMagic = 0x31494243U;   // CBI1
constexpr uint32_t kBlobIndexTombstoneMagic = 0x31544243U;  // CBT1
constexpr uint32_t kBlobIndexCheckpointMagic = 0x31434243U;  // CBC1
constexpr uint32_t kBlobIndexCheckpointVersion = 1;
constexpr uint32_t kActiveMagic = 0x31414243U;      // CBA1
constexpr uint64_t kActiveSegmentId = 1;
constexpr uint32_t kBlobBlockVersion = 1;
constexpr size_t kBlobBlockHeaderBytes = 88;
constexpr size_t kBlobBlockDirectoryEntryBytes = 72;
constexpr uint64_t kBlobBlockTargetBytes = 1ULL << 20;
constexpr uint32_t kBlobBlockFlagOversized = 1U;
constexpr uint32_t kBlobBlockSupportedFlags = kBlobBlockFlagOversized;
constexpr uint64_t kMaximumRecordsPerBlobBlock = 8192;
constexpr size_t kBlobRecordHeaderBytes = 57;
constexpr uint64_t kMaximumBlobBytes = 1ULL << 34;
constexpr uint8_t kBlobCodecNone = 0;
constexpr uint8_t kBlobCodecLz4 = 1;
constexpr uint64_t kMinimumCompressionSavingsNumerator = 7;
constexpr uint64_t kMinimumCompressionSavingsDenominator = 8;

void AtomicSaturatingAdd(std::atomic<uint64_t>* value, uint64_t delta) {
  uint64_t current = value->load(std::memory_order_relaxed);
  for (;;) {
    const uint64_t next = delta > UINT64_MAX - current
        ? UINT64_MAX : current + delta;
    if (value->compare_exchange_weak(current, next, std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
      return;
    }
  }
}

uint64_t ElapsedNs(std::chrono::steady_clock::time_point start) {
  return std::max<uint64_t>(
      1, static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now() - start).count()));
}

void PutU32(std::string* output, uint32_t value) {
  for (uint32_t shift = 0; shift < 32; shift += 8) output->push_back(static_cast<char>(value >> shift));
}
void PutU64(std::string* output, uint64_t value) {
  for (uint32_t shift = 0; shift < 64; shift += 8) output->push_back(static_cast<char>(value >> shift));
}
void PutU8(std::string* output, uint8_t value) {
  output->push_back(static_cast<char>(value));
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
bool GetU8(const std::string& input, size_t* offset, uint8_t* value) {
  if (*offset >= input.size()) return false;
  *value = static_cast<uint8_t>(input[(*offset)++]);
  return true;
}

Status WriteAll(int fd, const std::string& bytes, const std::string& path) {
  const char* data = bytes.data();
  size_t remaining = bytes.size();
  while (remaining > 0) {
    const ssize_t written = ::write(fd, data, remaining);
    if (written < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    data += written;
    remaining -= static_cast<size_t>(written);
  }
  return Status::OK();
}

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
  if (directory.empty()) return Status::InvalidArgument("blob", "empty parent directory");
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
  if (error) return Status::IOError(current.string(), error.message());
  if (!std::filesystem::is_directory(current, error) || error) {
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

struct DurableAppendResult {
  Status status;
  bool requires_reopen = false;
};

DurableAppendResult AppendDurablyWithFault(
    const std::string& path, const std::string& bytes, uint64_t* offset,
    const std::function<Status(BlobStoreFaultPoint)>& fault_injector = {},
    BlobStoreFaultPoint partial_write_point =
        BlobStoreFaultPoint::kAfterPartialIndexWrite,
    BlobStoreFaultPoint fsync_point = BlobStoreFaultPoint::kAfterIndexFsync) {
  const std::filesystem::path file(path);
  const std::filesystem::path parent = file.parent_path();
  Status status = EnsureDirectoryDurably(parent);
  if (!status.ok()) return {status, false};
  std::error_code error;
  const bool existed = std::filesystem::exists(file, error);
  if (error) return {Status::IOError(path, error.message()), false};
  const int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
  if (fd < 0) return {Status::IOError(path, std::strerror(errno)), false};
  const off_t position = ::lseek(fd, 0, SEEK_END);
  if (position < 0) {
    const Status status = Status::IOError(path, std::strerror(errno));
    ::close(fd);
    return {status, false};
  }
  const char* data = bytes.data();
  size_t remaining = bytes.size();
  bool wrote_any = false;
  const auto write_bytes = [&](size_t count) -> Status {
    size_t pending = count;
    while (pending > 0) {
      const ssize_t written = ::write(fd, data, pending);
      if (written < 0) {
        if (errno == EINTR) continue;
        return Status::IOError(path, std::strerror(errno));
      }
      wrote_any = true;
      data += written;
      remaining -= static_cast<size_t>(written);
      pending -= static_cast<size_t>(written);
    }
    return Status::OK();
  };
  if (fault_injector) {
    status = write_bytes(bytes.size() / 2);
    if (status.ok()) {
      status = fault_injector(partial_write_point);
    }
    if (!status.ok()) {
      ::close(fd);
      return {status, wrote_any};
    }
  }
  status = write_bytes(remaining);
  if (!status.ok()) {
    ::close(fd);
    return {status, wrote_any};
  }
  if (::fsync(fd) != 0) {
    const Status failure = Status::IOError(path, std::strerror(errno));
    ::close(fd);
    return {failure, wrote_any};
  }
  if (fault_injector) {
    status = fault_injector(fsync_point);
    if (!status.ok()) {
      ::close(fd);
      return {status, wrote_any};
    }
  }
  if (::close(fd) != 0) {
    return {Status::IOError(path, std::strerror(errno)), wrote_any};
  }
  if (!existed) {
    status = FsyncDirectory(parent);
    if (!status.ok()) return {status, wrote_any};
  }
  if (offset != nullptr) *offset = static_cast<uint64_t>(position);
  return {Status::OK(), false};
}

Status ReadFile(const std::string& path, std::string* bytes) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError(path, std::strerror(errno));
  bytes->clear();
  char buffer[8192];
  for (;;) {
    const ssize_t count = ::read(fd, buffer, sizeof(buffer));
    if (count == 0) break;
    if (count < 0) {
      const Status status = Status::IOError(path, std::strerror(errno));
      ::close(fd);
      return status;
    }
    bytes->append(buffer, static_cast<size_t>(count));
  }
  if (::close(fd) != 0) return Status::IOError(path, std::strerror(errno));
  return Status::OK();
}

Status ReadAtFully(int fd, uint64_t file_offset, char* destination, size_t length,
                   const std::string& path) {
  size_t copied = 0;
  while (copied < length) {
    const ssize_t read = ::pread(fd, destination + copied, length - copied,
                                 static_cast<off_t>(file_offset + copied));
    if (read < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    if (read == 0) return Status::BlobCorruption("blob", "truncated blob record");
    copied += static_cast<size_t>(read);
  }
  return Status::OK();
}

Status WriteAtomically(const std::string& path, const std::string& bytes) {
  const std::filesystem::path destination(path);
  const Status directory = EnsureDirectoryDurably(destination.parent_path());
  if (!directory.ok()) return directory;
  const std::string temporary = path + ".tmp";
  const int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  Status status = WriteAll(fd, bytes, temporary);
  if (status.ok() && ::fsync(fd) != 0) status = Status::IOError(temporary, std::strerror(errno));
  if (::close(fd) != 0 && status.ok()) status = Status::IOError(temporary, std::strerror(errno));
  if (!status.ok()) return status;
  if (::rename(temporary.c_str(), path.c_str()) != 0) return Status::IOError(path, std::strerror(errno));
  return FsyncDirectory(destination.parent_path());
}

bool CheckedAdd(uint64_t value, uint64_t* total);

struct EncodedBlobRecord {
  BlobHash hash;
  uint64_t raw_length = 0;
  uint64_t stored_length = 0;
  uint8_t codec = kBlobCodecNone;
  uint32_t payload_crc32c = 0;
  std::string bytes;
};

struct EncodedBlobBlock {
  std::string bytes;
  std::vector<size_t> request_indexes;
  uint64_t stored_payload_bytes = 0;
};

EncodedBlobRecord EncodeRecord(const BlobHash& hash,
                               const std::string& raw_bytes) {
  uint8_t codec = kBlobCodecNone;
  std::string stored_payload = raw_bytes;
  if (!raw_bytes.empty() && raw_bytes.size() <= static_cast<size_t>(std::numeric_limits<int>::max())) {
    const int bound = LZ4_compressBound(static_cast<int>(raw_bytes.size()));
    if (bound > 0) {
      std::string compressed(static_cast<size_t>(bound), '\0');
      const int compressed_size = LZ4_compress_default(
          raw_bytes.data(), compressed.data(),
          static_cast<int>(raw_bytes.size()), bound);
      if (compressed_size > 0 &&
          static_cast<uint64_t>(compressed_size) *
                  kMinimumCompressionSavingsDenominator <=
              raw_bytes.size() * kMinimumCompressionSavingsNumerator) {
        compressed.resize(static_cast<size_t>(compressed_size));
        stored_payload = std::move(compressed);
        codec = kBlobCodecLz4;
      }
    }
  }
  EncodedBlobRecord encoded;
  encoded.hash = hash;
  encoded.raw_length = raw_bytes.size();
  encoded.stored_length = stored_payload.size();
  encoded.codec = codec;
  encoded.payload_crc32c =
      crc32c::Value(stored_payload.data(), stored_payload.size());
  encoded.bytes.reserve(kBlobRecordHeaderBytes + stored_payload.size());
  PutU32(&encoded.bytes, kBlobRecordMagic);
  encoded.bytes.append(reinterpret_cast<const char*>(hash.bytes.data()),
                       hash.bytes.size());
  PutU64(&encoded.bytes, encoded.raw_length);
  PutU64(&encoded.bytes, encoded.stored_length);
  PutU8(&encoded.bytes, codec);
  PutU32(&encoded.bytes, encoded.payload_crc32c);
  encoded.bytes.append(stored_payload);
  return encoded;
}

StatusOr<EncodedBlobBlock> EncodeBlobBlock(
    const std::vector<EncodedBlobRecord>& records,
    const std::vector<size_t>& request_indexes, bool oversized) {
  if (request_indexes.empty() ||
      request_indexes.size() > kMaximumRecordsPerBlobBlock ||
      (oversized && request_indexes.size() != 1)) {
    return Status::InvalidArgument("blob block", "invalid block record set");
  }
  uint64_t records_bytes = 0;
  uint64_t stored_payload_bytes = 0;
  for (size_t request_index : request_indexes) {
    if (request_index >= records.size()) {
      return Status::InvalidArgument("blob block", "invalid record index");
    }
    const EncodedBlobRecord& record = records[request_index];
    if (!CheckedAdd(record.bytes.size(), &records_bytes) ||
        !CheckedAdd(record.stored_length, &stored_payload_bytes)) {
      return Status::ResourceExhausted("blob block", "block size overflow");
    }
  }
  if (request_indexes.size() >
      std::numeric_limits<uint64_t>::max() /
          kBlobBlockDirectoryEntryBytes) {
    return Status::ResourceExhausted("blob block", "directory size overflow");
  }
  const uint64_t directory_length =
      request_indexes.size() * kBlobBlockDirectoryEntryBytes;
  uint64_t directory_offset = kBlobBlockHeaderBytes;
  if (!CheckedAdd(records_bytes, &directory_offset)) {
    return Status::ResourceExhausted("blob block", "directory offset overflow");
  }
  uint64_t block_length = directory_offset;
  if (!CheckedAdd(directory_length, &block_length) ||
      block_length > kMaximumBlobBytes + kBlobBlockHeaderBytes +
                         kBlobRecordHeaderBytes +
                         kBlobBlockDirectoryEntryBytes) {
    return Status::ResourceExhausted("blob block", "block length overflow");
  }

  std::string directory;
  directory.reserve(static_cast<size_t>(directory_length));
  uint64_t record_offset = kBlobBlockHeaderBytes;
  for (size_t request_index : request_indexes) {
    const EncodedBlobRecord& record = records[request_index];
    directory.append(reinterpret_cast<const char*>(record.hash.bytes.data()),
                     record.hash.bytes.size());
    PutU64(&directory, record_offset);
    PutU64(&directory, record.bytes.size());
    PutU64(&directory, record.raw_length);
    PutU64(&directory, record.stored_length);
    PutU8(&directory, record.codec);
    PutU8(&directory, 0);
    PutU8(&directory, 0);
    PutU8(&directory, 0);
    PutU32(&directory, record.payload_crc32c);
    if (!CheckedAdd(record.bytes.size(), &record_offset)) {
      return Status::ResourceExhausted("blob block", "record offset overflow");
    }
  }
  if (directory.size() != directory_length ||
      record_offset != directory_offset) {
    return Status::Corruption("blob block", "internal directory mismatch");
  }

  std::string header_prefix;
  header_prefix.reserve(52);
  PutU32(&header_prefix, kBlobBlockMagic);
  PutU32(&header_prefix, kBlobBlockVersion);
  PutU32(&header_prefix, kBlobBlockHeaderBytes);
  PutU32(&header_prefix, oversized ? kBlobBlockFlagOversized : 0);
  PutU64(&header_prefix, block_length);
  PutU64(&header_prefix, request_indexes.size());
  PutU64(&header_prefix, directory_offset);
  PutU64(&header_prefix, directory_length);
  PutU32(&header_prefix,
         crc32c::Value(directory.data(), directory.size()));
  if (header_prefix.size() != 52) {
    return Status::Corruption("blob block", "internal header mismatch");
  }
  const BlobHash identity = Blake3Hash(header_prefix + directory);
  std::string header = header_prefix;
  header.append(reinterpret_cast<const char*>(identity.bytes.data()),
                identity.bytes.size());
  PutU32(&header, crc32c::Value(header.data(), header.size()));
  if (header.size() != kBlobBlockHeaderBytes) {
    return Status::Corruption("blob block", "internal header size mismatch");
  }

  EncodedBlobBlock block;
  block.bytes.reserve(static_cast<size_t>(block_length));
  block.bytes.append(header);
  for (size_t request_index : request_indexes) {
    block.bytes.append(records[request_index].bytes);
  }
  block.bytes.append(directory);
  if (block.bytes.size() != block_length) {
    return Status::Corruption("blob block", "internal block size mismatch");
  }
  block.request_indexes = request_indexes;
  block.stored_payload_bytes = stored_payload_bytes;
  return block;
}

StatusOr<std::vector<EncodedBlobBlock>> PackBlobBlocks(
    const std::vector<EncodedBlobRecord>& records) {
  std::vector<EncodedBlobBlock> blocks;
  std::vector<size_t> current_indexes;
  uint64_t current_record_bytes = 0;
  const auto flush = [&]() -> Status {
    if (current_indexes.empty()) return Status::OK();
    const auto encoded = EncodeBlobBlock(records, current_indexes, false);
    if (!encoded.ok()) return encoded.status();
    blocks.push_back(encoded.ValueOrDie());
    current_indexes.clear();
    current_record_bytes = 0;
    return Status::OK();
  };
  for (size_t index = 0; index < records.size(); ++index) {
    const EncodedBlobRecord& record = records[index];
    const bool intrinsically_oversized =
        record.raw_length > kBlobBlockTargetBytes ||
        record.bytes.size() + kBlobBlockHeaderBytes +
                kBlobBlockDirectoryEntryBytes >
            kBlobBlockTargetBytes;
    if (intrinsically_oversized) {
      const Status flushed = flush();
      if (!flushed.ok()) return flushed;
      const auto encoded = EncodeBlobBlock(records, {index}, true);
      if (!encoded.ok()) return encoded.status();
      blocks.push_back(encoded.ValueOrDie());
      continue;
    }
    const uint64_t next_count = current_indexes.size() + 1;
    uint64_t projected = kBlobBlockHeaderBytes;
    if (!CheckedAdd(current_record_bytes, &projected) ||
        !CheckedAdd(record.bytes.size(), &projected) ||
        next_count > std::numeric_limits<uint64_t>::max() /
                         kBlobBlockDirectoryEntryBytes ||
        !CheckedAdd(next_count * kBlobBlockDirectoryEntryBytes, &projected)) {
      return Status::ResourceExhausted("blob block", "packing size overflow");
    }
    if (!current_indexes.empty() && projected > kBlobBlockTargetBytes) {
      const Status flushed = flush();
      if (!flushed.ok()) return flushed;
    }
    current_indexes.push_back(index);
    if (!CheckedAdd(record.bytes.size(), &current_record_bytes)) {
      return Status::ResourceExhausted("blob block", "packing offset overflow");
    }
  }
  const Status flushed = flush();
  if (!flushed.ok()) return flushed;
  return blocks;
}

std::string EncodeIndexRecord(const BlobHash& hash, uint64_t raw_length,
                              const BlobLocation& location) {
  std::string payload;
  PutU32(&payload, kBlobIndexMagic);
  payload.append(reinterpret_cast<const char*>(hash.bytes.data()), hash.bytes.size());
  PutU64(&payload, raw_length);
  PutU32(&payload, location.shard_id);
  PutU64(&payload, location.segment_id);
  PutU64(&payload, location.offset);
  std::string record;
  PutU32(&record, static_cast<uint32_t>(payload.size()));
  PutU32(&record, crc32c::Value(payload.data(), payload.size()));
  record.append(payload);
  return record;
}

std::string EncodeActiveSegment(uint64_t segment_id) {
  std::string bytes;
  PutU32(&bytes, kActiveMagic);
  PutU64(&bytes, segment_id);
  PutU32(&bytes, crc32c::Value(bytes.data(), bytes.size()));
  return bytes;
}

bool CheckedAdd(uint64_t value, uint64_t* total) {
  if (*total > std::numeric_limits<uint64_t>::max() - value) return false;
  *total += value;
  return true;
}

std::string EncodeIndexTombstone(const BlobHash& hash) {
  std::string payload;
  PutU32(&payload, kBlobIndexTombstoneMagic);
  payload.append(reinterpret_cast<const char*>(hash.bytes.data()), hash.bytes.size());
  std::string record;
  PutU32(&record, static_cast<uint32_t>(payload.size()));
  PutU32(&record, crc32c::Value(payload.data(), payload.size()));
  record.append(payload);
  return record;
}

bool IsSegmentFile(const std::filesystem::path& path, uint64_t* segment_id) {
  const std::string name = path.filename().string();
  constexpr char kPrefix[] = "segment-";
  constexpr char kSuffix[] = ".blob";
  if (name.compare(0, sizeof(kPrefix) - 1, kPrefix) != 0 ||
      name.size() <= sizeof(kPrefix) - 1 + sizeof(kSuffix) - 1 ||
      name.compare(name.size() - (sizeof(kSuffix) - 1), sizeof(kSuffix) - 1, kSuffix) != 0) {
    return false;
  }
  try {
    const std::string number = name.substr(sizeof(kPrefix) - 1,
        name.size() - (sizeof(kPrefix) - 1) - (sizeof(kSuffix) - 1));
    size_t parsed = 0;
    const uint64_t value = std::stoull(number, &parsed);
    if (parsed != number.size() || value == 0) return false;
    *segment_id = value;
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

std::string EncodeIndexCheckpoint(
    const std::unordered_map<BlobHash, BlobLocation, BlobHashHasher>& locations,
    const std::unordered_map<BlobHash, uint64_t, BlobHashHasher>& raw_lengths) {
  std::vector<BlobHash> hashes;
  hashes.reserve(locations.size());
  for (const auto& entry : locations) hashes.push_back(entry.first);
  std::sort(hashes.begin(), hashes.end(), [](const auto& left, const auto& right) {
    return left.bytes < right.bytes;
  });
  std::string encoded;
  PutU32(&encoded, kBlobIndexCheckpointMagic);
  PutU32(&encoded, kBlobIndexCheckpointVersion);
  PutU32(&encoded, static_cast<uint32_t>(hashes.size()));
  for (const BlobHash& hash : hashes) {
    const BlobLocation& location = locations.at(hash);
    encoded.append(reinterpret_cast<const char*>(hash.bytes.data()), hash.bytes.size());
    PutU64(&encoded, raw_lengths.at(hash));
    PutU32(&encoded, location.shard_id);
    PutU64(&encoded, location.segment_id);
    PutU64(&encoded, location.offset);
  }
  PutU32(&encoded, crc32c::Value(encoded.data(), encoded.size()));
  return encoded;
}

Status DecodeIndexCheckpoint(
    const std::string& path, uint32_t expected_shard_id,
    std::unordered_map<BlobHash, BlobLocation, BlobHashHasher>* locations,
    std::unordered_map<BlobHash, uint64_t, BlobHashHasher>* raw_lengths) {
  std::string encoded;
  const Status read = ReadFile(path, &encoded);
  if (!read.ok()) return read;
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t version = 0;
  uint32_t count = 0;
  const uint32_t stored_checksum = encoded.size() < sizeof(uint32_t) ? 0 :
      static_cast<uint32_t>(static_cast<uint8_t>(encoded[encoded.size() - 4])) |
      (static_cast<uint32_t>(static_cast<uint8_t>(encoded[encoded.size() - 3])) << 8) |
      (static_cast<uint32_t>(static_cast<uint8_t>(encoded[encoded.size() - 2])) << 16) |
      (static_cast<uint32_t>(static_cast<uint8_t>(encoded[encoded.size() - 1])) << 24);
  if (encoded.size() < 16 || !GetU32(encoded, &offset, &magic) ||
      !GetU32(encoded, &offset, &version) || !GetU32(encoded, &offset, &count) ||
      magic != kBlobIndexCheckpointMagic || version != kBlobIndexCheckpointVersion ||
      count > 100000000U || encoded.size() != 16ULL + static_cast<uint64_t>(count) * 60 ||
      crc32c::Value(encoded.data(), encoded.size() - sizeof(uint32_t)) != stored_checksum) {
    return Status::Corruption(path, "invalid blob index checkpoint");
  }
  locations->clear();
  raw_lengths->clear();
  for (uint32_t index = 0; index < count; ++index) {
    BlobHash hash;
    BlobLocation location;
    uint64_t raw_length = 0;
    if (encoded.size() - offset < hash.bytes.size()) return Status::Corruption(path, "truncated checkpoint hash");
    std::memcpy(hash.bytes.data(), encoded.data() + offset, hash.bytes.size());
    offset += hash.bytes.size();
    if (!GetU64(encoded, &offset, &raw_length) || !GetU32(encoded, &offset, &location.shard_id) ||
        !GetU64(encoded, &offset, &location.segment_id) || !GetU64(encoded, &offset, &location.offset) ||
        location.shard_id != expected_shard_id || location.segment_id == 0 ||
        locations->count(hash) != 0) {
      return Status::Corruption(path, "invalid checkpoint entry");
    }
    locations->emplace(hash, location);
    raw_lengths->emplace(hash, raw_length);
  }
  return offset == encoded.size() - sizeof(uint32_t)
      ? Status::OK() : Status::Corruption(path, "trailing checkpoint bytes");
}

struct BlobBlockEntry {
  uint64_t block_length = 0;
  uint64_t record_offset = 0;
  uint64_t record_length = 0;
  uint64_t raw_length = 0;
  uint64_t stored_length = 0;
  uint8_t codec = kBlobCodecNone;
  uint32_t payload_crc32c = 0;
};

StatusOr<BlobBlockEntry> ReadBlobBlockEntry(
    int fd, const std::string& path, uint64_t block_offset,
    const BlobHash& expected_hash,
    std::unordered_set<BlobHash, BlobHashHasher>* block_hashes = nullptr) {
  if (block_offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    return Status::BlobCorruption("blob", "block offset exceeds platform bound");
  }
  std::string header(kBlobBlockHeaderBytes, '\0');
  const Status header_read = ReadAtFully(
      fd, block_offset, header.data(), header.size(), path);
  if (!header_read.ok()) return header_read;
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t version = 0;
  uint32_t header_size = 0;
  uint32_t flags = 0;
  uint64_t block_length = 0;
  uint64_t record_count = 0;
  uint64_t directory_offset = 0;
  uint64_t directory_length = 0;
  uint32_t directory_crc32c = 0;
  if (!GetU32(header, &offset, &magic) ||
      !GetU32(header, &offset, &version) ||
      !GetU32(header, &offset, &header_size) ||
      !GetU32(header, &offset, &flags) ||
      !GetU64(header, &offset, &block_length) ||
      !GetU64(header, &offset, &record_count) ||
      !GetU64(header, &offset, &directory_offset) ||
      !GetU64(header, &offset, &directory_length) ||
      !GetU32(header, &offset, &directory_crc32c) ||
      magic != kBlobBlockMagic || version != kBlobBlockVersion ||
      header_size != kBlobBlockHeaderBytes ||
      (flags & ~kBlobBlockSupportedFlags) != 0 || record_count == 0 ||
      record_count > kMaximumRecordsPerBlobBlock ||
      ((flags & kBlobBlockFlagOversized) != 0 && record_count != 1) ||
      record_count > std::numeric_limits<uint64_t>::max() /
                         kBlobBlockDirectoryEntryBytes ||
      directory_length !=
          record_count * kBlobBlockDirectoryEntryBytes ||
      directory_offset < kBlobBlockHeaderBytes ||
      directory_offset > block_length ||
      directory_length != block_length - directory_offset ||
      block_length > kMaximumBlobBytes + kBlobBlockHeaderBytes +
                         kBlobRecordHeaderBytes +
                         kBlobBlockDirectoryEntryBytes) {
    return Status::BlobCorruption("blob", "invalid blob block header");
  }
  BlobHash stored_identity;
  if (header.size() - offset < stored_identity.bytes.size()) {
    return Status::BlobCorruption("blob", "truncated blob block identity");
  }
  std::memcpy(stored_identity.bytes.data(), header.data() + offset,
              stored_identity.bytes.size());
  offset += stored_identity.bytes.size();
  uint32_t header_crc32c = 0;
  if (!GetU32(header, &offset, &header_crc32c) || offset != header.size() ||
      crc32c::Value(header.data(), header.size() - sizeof(uint32_t)) !=
          header_crc32c) {
    return Status::BlobCorruption("blob", "blob block header checksum mismatch");
  }
  if ((flags & kBlobBlockFlagOversized) == 0 &&
      block_length > kBlobBlockTargetBytes) {
    return Status::BlobCorruption("blob", "ordinary blob block exceeds target");
  }
  if (block_offset > std::numeric_limits<uint64_t>::max() - directory_offset ||
      block_offset + directory_offset >
          static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    return Status::BlobCorruption("blob", "blob directory offset overflow");
  }
  std::string directory(static_cast<size_t>(directory_length), '\0');
  const Status directory_read = ReadAtFully(
      fd, block_offset + directory_offset, directory.data(), directory.size(),
      path);
  if (!directory_read.ok()) return directory_read;
  if (crc32c::Value(directory.data(), directory.size()) != directory_crc32c) {
    return Status::BlobCorruption("blob", "blob block directory checksum mismatch");
  }
  const BlobHash actual_identity =
      Blake3Hash(header.substr(0, 52) + directory);
  if (actual_identity != stored_identity) {
    return Status::BlobCorruption("blob", "blob block identity mismatch");
  }

  std::optional<BlobBlockEntry> selected;
  std::unordered_set<BlobHash, BlobHashHasher> hashes;
  hashes.reserve(static_cast<size_t>(record_count));
  uint64_t expected_record_offset = kBlobBlockHeaderBytes;
  size_t directory_cursor = 0;
  for (uint64_t record_index = 0; record_index < record_count;
       ++record_index) {
    BlobHash hash;
    if (directory.size() - directory_cursor < hash.bytes.size()) {
      return Status::BlobCorruption("blob", "truncated blob block directory");
    }
    std::memcpy(hash.bytes.data(), directory.data() + directory_cursor,
                hash.bytes.size());
    directory_cursor += hash.bytes.size();
    BlobBlockEntry entry;
    uint8_t reserved0 = 0;
    uint8_t reserved1 = 0;
    uint8_t reserved2 = 0;
    if (!GetU64(directory, &directory_cursor, &entry.record_offset) ||
        !GetU64(directory, &directory_cursor, &entry.record_length) ||
        !GetU64(directory, &directory_cursor, &entry.raw_length) ||
        !GetU64(directory, &directory_cursor, &entry.stored_length) ||
        !GetU8(directory, &directory_cursor, &entry.codec) ||
        !GetU8(directory, &directory_cursor, &reserved0) ||
        !GetU8(directory, &directory_cursor, &reserved1) ||
        !GetU8(directory, &directory_cursor, &reserved2) ||
        !GetU32(directory, &directory_cursor, &entry.payload_crc32c) ||
        reserved0 != 0 || reserved1 != 0 || reserved2 != 0 ||
        !hashes.insert(hash).second ||
        entry.record_offset != expected_record_offset ||
        entry.record_offset > directory_offset ||
        entry.raw_length > kMaximumBlobBytes ||
        entry.stored_length > kMaximumBlobBytes ||
        entry.record_length != kBlobRecordHeaderBytes + entry.stored_length ||
        entry.record_length > directory_offset - entry.record_offset ||
        (entry.codec != kBlobCodecNone && entry.codec != kBlobCodecLz4) ||
        (entry.codec == kBlobCodecNone &&
         entry.raw_length != entry.stored_length) ||
        (entry.codec == kBlobCodecLz4 &&
         (entry.raw_length >
              static_cast<uint64_t>(std::numeric_limits<int>::max()) ||
          entry.stored_length >
              static_cast<uint64_t>(std::numeric_limits<int>::max())))) {
      return Status::BlobCorruption("blob", "invalid blob block directory entry");
    }
    entry.block_length = block_length;
    expected_record_offset += entry.record_length;
    if (hash == expected_hash) selected = entry;
  }
  if (directory_cursor != directory.size() ||
      expected_record_offset != directory_offset) {
    return Status::BlobCorruption("blob", "invalid blob block boundaries");
  }
  if (!selected.has_value()) {
    return Status::BlobCorruption("blob", "hash is absent from hinted block");
  }
  if (block_hashes != nullptr) *block_hashes = std::move(hashes);
  return *selected;
}

}  // namespace

struct BlobStore::ShardState {
  mutable std::mutex mutex;
  uint64_t active_segment_id = kActiveSegmentId;
  bool active_segment_persisted = false;
  std::unordered_map<BlobHash, BlobLocation, BlobHashHasher> locations;
  std::unordered_map<BlobHash, uint64_t, BlobHashHasher> raw_lengths;
};

size_t BlobHashHasher::operator()(const BlobHash& hash) const {
  size_t value = 1469598103934665603ULL;
  for (uint8_t byte : hash.bytes) {
    value ^= byte;
    value *= 1099511628211ULL;
  }
  return value;
}

BlobHash Blake3Hash(const std::string& raw_bytes) {
  BlobHash hash;
  blake3_hasher hasher;
  blake3_hasher_init(&hasher);
  blake3_hasher_update(&hasher, raw_bytes.data(), raw_bytes.size());
  blake3_hasher_finalize(&hasher, hash.bytes.data(), hash.bytes.size());
  return hash;
}

std::string BlobHashHex(const BlobHash& hash) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string encoded;
  encoded.reserve(hash.bytes.size() * 2);
  for (uint8_t byte : hash.bytes) {
    encoded.push_back(kHex[byte >> 4]);
    encoded.push_back(kHex[byte & 0x0f]);
  }
  return encoded;
}

BlobStore::BlobStore(std::string root_path, uint32_t shard_count)
    : root_path_(std::move(root_path)), shard_count_(shard_count) {}

BlobStore::~BlobStore() = default;

uint32_t BlobStore::ShardFor(const BlobHash& hash) const {
  return static_cast<uint32_t>(hash.bytes[0] % shard_count_);
}

std::string BlobStore::SegmentPath(uint32_t shard_id, uint64_t segment_id) const {
  return root_path_ + "/shard-" + std::to_string(shard_id) +
      "/segment-" + std::to_string(segment_id) + ".blob";
}

std::string BlobStore::IndexPath(uint32_t shard_id) const {
  return root_path_ + "/shard-" + std::to_string(shard_id) + "/INDEX";
}

std::string BlobStore::IndexCheckpointPath(uint32_t shard_id) const {
  return root_path_ + "/shard-" + std::to_string(shard_id) + "/INDEX-CHECKPOINT";
}

std::string BlobStore::ActivePath(uint32_t shard_id) const {
  return root_path_ + "/shard-" + std::to_string(shard_id) + "/ACTIVE";
}

Status BlobStore::EnsureSegment(uint32_t shard_id, uint64_t segment_id) const {
  const std::string path = SegmentPath(shard_id, segment_id);
  const std::filesystem::path file(path);
  const std::filesystem::path parent = file.parent_path();
  Status status = EnsureDirectoryDurably(parent);
  if (!status.ok()) return status;
  std::error_code error;
  const bool existed = std::filesystem::exists(file, error);
  if (error) return Status::IOError(path, error.message());
  const int fd = ::open(path.c_str(), O_CREAT | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(path, std::strerror(errno));
  if (::fsync(fd) != 0) {
    const Status sync_status = Status::IOError(path, std::strerror(errno));
    ::close(fd);
    return sync_status;
  }
  if (::close(fd) != 0) return Status::IOError(path, std::strerror(errno));
  if (!existed) return FsyncDirectory(parent);
  return Status::OK();
}

Status BlobStore::PersistActiveSegment(uint32_t shard_id, uint64_t segment_id) const {
  const std::string path = ActivePath(shard_id);
  const std::filesystem::path destination(path);
  const Status directory_status = EnsureDirectoryDurably(destination.parent_path());
  if (!directory_status.ok()) return directory_status;
  const std::string bytes = EncodeActiveSegment(segment_id);
  const std::string temporary = path + ".tmp";
  const int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  Status status = WriteAll(fd, bytes, temporary);
  if (status.ok() && ::fsync(fd) != 0) status = Status::IOError(temporary, std::strerror(errno));
  if (::close(fd) != 0 && status.ok()) status = Status::IOError(temporary, std::strerror(errno));
  if (!status.ok()) return status;
  if (::rename(temporary.c_str(), path.c_str()) != 0) return Status::IOError(path, std::strerror(errno));
  const std::string directory = destination.parent_path().string();
  const int directory_fd = ::open(directory.c_str(), O_RDONLY);
  if (directory_fd < 0) return Status::IOError(directory, std::strerror(errno));
  if (::fsync(directory_fd) != 0) {
    const Status directory_status = Status::IOError(directory, std::strerror(errno));
    ::close(directory_fd);
    return directory_status;
  }
  if (::close(directory_fd) != 0) return Status::IOError(directory, std::strerror(errno));
  return Status::OK();
}

Status BlobStore::Open() {
  if (shard_count_ == 0) return Status::InvalidArgument("blob store", "zero shard count");
  shards_.clear();
  shards_.reserve(shard_count_);
  for (uint32_t shard = 0; shard < shard_count_; ++shard) {
    shards_.push_back(std::make_unique<ShardState>());
    Status status = OpenShard(shard);
    if (!status.ok()) {
      requires_reopen_.store(true, std::memory_order_release);
      return status;
    }
  }
  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    const std::string index_path = IndexPath(shard_id);
    std::error_code error;
    if (!std::filesystem::exists(index_path, error)) {
      if (error) {
        requires_reopen_.store(true, std::memory_order_release);
        return Status::IOError(index_path, error.message());
      }
      continue;
    }
    const std::filesystem::path directory =
        std::filesystem::path(index_path).parent_path();
    if (fault_injector_) {
      const Status injected = fault_injector_(
          BlobStoreFaultPoint::kBeforeRecoveryDirectoryFsync);
      if (!injected.ok()) {
        requires_reopen_.store(true, std::memory_order_release);
        return injected;
      }
    }
    const Status synced = FsyncDirectory(directory);
    if (!synced.ok()) {
      requires_reopen_.store(true, std::memory_order_release);
      return synced;
    }
  }
  requires_reopen_.store(false, std::memory_order_release);
  return Status::OK();
}

Status BlobStore::CheckMutationAllowed() const {
  return requires_reopen()
      ? Status::RecoveryRequired("blob store", "reopen after failed INDEX append")
      : Status::OK();
}

Status BlobStore::AppendIndexRecord(uint32_t shard_id,
                                    const std::string& record) {
  const Status allowed = CheckMutationAllowed();
  if (!allowed.ok()) return allowed;
  const DurableAppendResult appended = AppendDurablyWithFault(
      IndexPath(shard_id), record, nullptr, fault_injector_);
  if (!appended.status.ok() && appended.requires_reopen) {
    requires_reopen_.store(true, std::memory_order_release);
  }
  return appended.status;
}

StatusOr<std::vector<BlobRef>> BlobStore::AppendBlobBlocksLocked(
    uint32_t shard_id, const std::vector<BlobWriteRequest>& requests,
    uint64_t* stored_payload_bytes_out) {
  if (shard_id >= shards_.size()) {
    return Status::InvalidArgument("blob block", "invalid shard");
  }
  if (requests.empty()) return std::vector<BlobRef>{};
  std::unordered_set<BlobHash, BlobHashHasher> unique_hashes;
  unique_hashes.reserve(requests.size());
  std::vector<EncodedBlobRecord> records;
  records.reserve(requests.size());
  for (const BlobWriteRequest& request : requests) {
    if (request.raw_bytes == nullptr ||
        request.raw_bytes->size() > kMaximumBlobBytes ||
        ShardFor(request.hash) != shard_id ||
        Blake3Hash(*request.raw_bytes) != request.hash ||
        !unique_hashes.insert(request.hash).second) {
      return Status::InvalidArgument("blob block", "invalid write request");
    }
    records.push_back(EncodeRecord(request.hash, *request.raw_bytes));
  }
  const auto packed = PackBlobBlocks(records);
  if (!packed.ok()) return packed.status();

  std::string segment_append;
  uint64_t segment_append_bytes = 0;
  for (const EncodedBlobBlock& block : packed.ValueOrDie()) {
    if (!CheckedAdd(block.bytes.size(), &segment_append_bytes) ||
        segment_append_bytes > std::numeric_limits<size_t>::max()) {
      return Status::ResourceExhausted("blob block", "segment append overflow");
    }
  }
  segment_append.reserve(static_cast<size_t>(segment_append_bytes));
  for (const EncodedBlobBlock& block : packed.ValueOrDie()) {
    segment_append.append(block.bytes);
  }

  const std::string segment_path =
      SegmentPath(shard_id, shards_[shard_id]->active_segment_id);
  Status status = EnsureSegment(shard_id, shards_[shard_id]->active_segment_id);
  if (!status.ok()) return status;
  uint64_t append_offset = 0;
  const DurableAppendResult appended = AppendDurablyWithFault(
      segment_path, segment_append, &append_offset, fault_injector_,
      BlobStoreFaultPoint::kAfterPartialRecordWrite,
      BlobStoreFaultPoint::kAfterRecordFsync);
  if (!appended.status.ok()) return appended.status;

  std::vector<BlobLocation> locations(requests.size());
  uint64_t relative_block_offset = 0;
  uint64_t stored_payload_bytes = 0;
  for (const EncodedBlobBlock& block : packed.ValueOrDie()) {
    if (append_offset > std::numeric_limits<uint64_t>::max() -
                            relative_block_offset) {
      return Status::ResourceExhausted("blob block", "location overflow");
    }
    const BlobLocation location{
        shard_id, shards_[shard_id]->active_segment_id,
        append_offset + relative_block_offset};
    for (size_t request_index : block.request_indexes) {
      locations[request_index] = location;
    }
    if (!CheckedAdd(block.bytes.size(), &relative_block_offset) ||
        !CheckedAdd(block.stored_payload_bytes, &stored_payload_bytes)) {
      return Status::ResourceExhausted("blob block", "metric overflow");
    }
  }

  std::string index_append;
  for (size_t index = 0; index < requests.size(); ++index) {
    index_append.append(EncodeIndexRecord(
        requests[index].hash, requests[index].raw_bytes->size(),
        locations[index]));
  }
  status = AppendIndexRecord(shard_id, index_append);
  if (!status.ok()) return status;

  std::vector<BlobRef> references;
  references.reserve(requests.size());
  ShardState& shard = *shards_[shard_id];
  for (size_t index = 0; index < requests.size(); ++index) {
    const uint64_t raw_length = requests[index].raw_bytes->size();
    shard.locations[requests[index].hash] = locations[index];
    shard.raw_lengths[requests[index].hash] = raw_length;
    references.push_back(
        BlobRef{requests[index].hash, raw_length, locations[index]});
  }
  AtomicSaturatingAdd(&payload_bytes_written_, stored_payload_bytes);
  if (stored_payload_bytes_out != nullptr) {
    *stored_payload_bytes_out = stored_payload_bytes;
  }
  return references;
}

StatusOr<BlobProtectedWriteEstimate> BlobStore::EstimateProtectedPutWrites(
    const std::vector<std::string>& raw_blobs,
    uint64_t rotation_target_bytes) const {
  if (shards_.size() != shard_count_ || rotation_target_bytes == 0) {
    return Status::InvalidArgument("blob estimate",
                                   "store must be open and rotation target nonzero");
  }
  BlobProtectedWriteEstimate estimate;
  if (raw_blobs.empty()) return estimate;

  std::vector<std::unique_lock<std::mutex>> locks;
  locks.reserve(shards_.size());
  for (const auto& shard : shards_) locks.emplace_back(shard->mutex);

  std::vector<uint64_t> projected_segment_bytes(shard_count_, 0);
  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    const ShardState& shard = *shards_[shard_id];
    const std::string segment_path = SegmentPath(shard_id, shard.active_segment_id);
    std::error_code error;
    if (std::filesystem::exists(segment_path, error)) {
      projected_segment_bytes[shard_id] =
          std::filesystem::file_size(segment_path, error);
    }
    if (error) return Status::IOError(segment_path, error.message());
    if (!shard.active_segment_persisted) {
      if (!CheckedAdd(EncodeActiveSegment(shard.active_segment_id).size(),
                      &estimate.active_metadata_bytes) ||
          !CheckedAdd(2, &estimate.descriptors) ||
          !CheckedAdd(2, &estimate.metadata_ops)) {
        return Status::ResourceExhausted("blob estimate", "ACTIVE estimate overflow");
      }
    }
  }

  std::unordered_set<BlobHash, BlobHashHasher> projected_hashes;
  std::vector<std::vector<EncodedBlobRecord>> records_by_shard(shard_count_);
  for (const std::string& raw_blob : raw_blobs) {
    if (raw_blob.size() > kMaximumBlobBytes) {
      return Status::InvalidArgument("blob estimate", "blob exceeds maximum size");
    }
    const BlobHash hash = Blake3Hash(raw_blob);
    const uint32_t shard_id = ShardFor(hash);
    const ShardState& shard = *shards_[shard_id];
    if (shard.locations.count(hash) != 0 || !projected_hashes.insert(hash).second) {
      continue;
    }
    records_by_shard[shard_id].push_back(EncodeRecord(hash, raw_blob));
  }
  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    if (records_by_shard[shard_id].empty()) continue;
    const auto blocks = PackBlobBlocks(records_by_shard[shard_id]);
    if (!blocks.ok()) return blocks.status();
    for (const EncodedBlobBlock& block : blocks.ValueOrDie()) {
      const uint64_t block_offset = projected_segment_bytes[shard_id];
      for (size_t record_index : block.request_indexes) {
        const EncodedBlobRecord& record =
            records_by_shard[shard_id][record_index];
        const std::string index_record = EncodeIndexRecord(
            record.hash, record.raw_length,
            BlobLocation{shard_id,
                         shards_[shard_id]->active_segment_id,
                         block_offset});
        if (!CheckedAdd(index_record.size(), &estimate.index_bytes)) {
          return Status::ResourceExhausted("blob estimate",
                                           "index estimate overflow");
        }
      }
      if (!CheckedAdd(block.bytes.size(), &estimate.segment_bytes) ||
          !CheckedAdd(block.bytes.size(),
                      &projected_segment_bytes[shard_id])) {
        return Status::ResourceExhausted("blob estimate",
                                         "block estimate overflow");
      }
    }
    if (!CheckedAdd(3, &estimate.descriptors) ||
        !CheckedAdd(3, &estimate.metadata_ops)) {
      return Status::ResourceExhausted("blob estimate", "put estimate overflow");
    }
  }

  const bool rotates = std::any_of(
      projected_segment_bytes.begin(), projected_segment_bytes.end(),
      [rotation_target_bytes](uint64_t bytes) {
        return bytes >= rotation_target_bytes;
      });
  if (rotates) {
    for (const auto& shard : shards_) {
      if (shard->active_segment_id == std::numeric_limits<uint64_t>::max() ||
          !CheckedAdd(EncodeActiveSegment(shard->active_segment_id + 1).size(),
                      &estimate.active_metadata_bytes) ||
          !CheckedAdd(2, &estimate.descriptors) ||
          !CheckedAdd(2, &estimate.metadata_ops)) {
        return Status::ResourceExhausted("blob estimate", "rotation estimate overflow");
      }
    }
  }
  estimate.manifest_rewrites = rotates ? 2 : 1;
  const uint64_t segment_generations = rotates ? 2 : 1;
  if (shard_count_ != 0 &&
      segment_generations > std::numeric_limits<uint64_t>::max() / shard_count_) {
    return Status::ResourceExhausted("blob estimate", "Manifest segment estimate overflow");
  }
  estimate.additional_manifest_segments = segment_generations * shard_count_;
  if (!CheckedAdd(estimate.segment_bytes, &estimate.total_bytes) ||
      !CheckedAdd(estimate.index_bytes, &estimate.total_bytes) ||
      !CheckedAdd(estimate.active_metadata_bytes, &estimate.total_bytes)) {
    return Status::ResourceExhausted("blob estimate", "write estimate overflow");
  }
  return estimate;
}

Status BlobStore::OpenShard(uint32_t shard_id) {
  const std::string active_path = ActivePath(shard_id);
  if (std::filesystem::exists(active_path)) {
    std::string active;
    Status status = ReadFile(active_path, &active);
    if (!status.ok()) return status;
    size_t offset = 0;
    uint32_t magic;
    uint64_t active_segment_id;
    uint32_t checksum;
    if (!GetU32(active, &offset, &magic) || !GetU64(active, &offset, &active_segment_id) ||
        !GetU32(active, &offset, &checksum) || offset != active.size() ||
        magic != kActiveMagic || active_segment_id == 0 ||
        crc32c::Value(active.data(), active.size() - sizeof(uint32_t)) != checksum) {
      return Status::Corruption(active_path, "invalid active segment metadata");
    }
    shards_[shard_id]->active_segment_id = active_segment_id;
    shards_[shard_id]->active_segment_persisted = true;
    if (!std::filesystem::exists(SegmentPath(shard_id, active_segment_id))) {
      return Status::BlobCorruption("blob", "active segment is missing");
    }
  }
  const std::string path = IndexPath(shard_id);
  const std::string checkpoint_path = IndexCheckpointPath(shard_id);
  if (std::filesystem::exists(checkpoint_path)) {
    Status status = DecodeIndexCheckpoint(
        checkpoint_path, shard_id, &shards_[shard_id]->locations, &shards_[shard_id]->raw_lengths);
    if (!status.ok()) return status;
  }
  if (!std::filesystem::exists(path)) return ValidateShardLocations(shard_id);
  const int index_fd = ::open(path.c_str(), O_RDWR);
  if (index_fd < 0) return Status::IOError(path, std::strerror(errno));
  std::string bytes;
  char buffer[8192];
  for (;;) {
    const ssize_t count = ::read(index_fd, buffer, sizeof(buffer));
    if (count == 0) break;
    if (count < 0) {
      const Status failure = Status::IOError(path, std::strerror(errno));
      ::close(index_fd);
      return failure;
    }
    bytes.append(buffer, static_cast<size_t>(count));
  }
  size_t offset = 0;
  std::optional<size_t> torn_offset;
  while (offset < bytes.size()) {
    const size_t record_start = offset;
    if (bytes.size() - offset < 8) {
      torn_offset = record_start;
      break;
    }
    uint32_t length;
    uint32_t checksum;
    if (!GetU32(bytes, &offset, &length) || !GetU32(bytes, &offset, &checksum) ||
        (length != 64 && length != 36)) {
      ::close(index_fd);
      return Status::Corruption(path, "invalid blob index record");
    }
    if (bytes.size() - offset < length) {
      torn_offset = record_start;
      break;
    }
    const std::string record = bytes.substr(offset, length);
    offset += length;
    if (crc32c::Value(record.data(), record.size()) != checksum) {
      ::close(index_fd);
      return Status::Corruption(path, "blob index checksum mismatch");
    }
    size_t record_offset = 0;
    uint32_t magic;
    BlobHash hash;
    if (!GetU32(record, &record_offset, &magic) ||
        record.size() - record_offset < hash.bytes.size()) {
      ::close(index_fd);
      return Status::Corruption(path, "invalid blob index payload");
    }
    std::memcpy(hash.bytes.data(), record.data() + record_offset, hash.bytes.size());
    record_offset += hash.bytes.size();
    if (magic == kBlobIndexTombstoneMagic) {
      if (record_offset != record.size()) {
        ::close(index_fd);
        return Status::Corruption(path, "invalid blob index tombstone");
      }
      shards_[shard_id]->locations.erase(hash);
      shards_[shard_id]->raw_lengths.erase(hash);
      continue;
    }
    uint64_t raw_length;
    BlobLocation location;
    if (magic != kBlobIndexMagic || !GetU64(record, &record_offset, &raw_length) ||
        !GetU32(record, &record_offset, &location.shard_id) ||
        !GetU64(record, &record_offset, &location.segment_id) ||
        !GetU64(record, &record_offset, &location.offset) ||
        record_offset != record.size() || location.shard_id != shard_id ||
        ShardFor(hash) != shard_id) {
      ::close(index_fd);
      return Status::Corruption(path, "invalid blob index location");
    }
    shards_[shard_id]->locations[hash] = location;
    shards_[shard_id]->raw_lengths[hash] = raw_length;
  }
  if (torn_offset.has_value()) {
    if (::ftruncate(index_fd, static_cast<off_t>(*torn_offset)) != 0) {
      const Status failure = Status::IOError(path, std::strerror(errno));
      ::close(index_fd);
      return failure;
    }
    if (::fsync(index_fd) != 0) {
      const Status failure = Status::IOError(path, std::strerror(errno));
      ::close(index_fd);
      return failure;
    }
  }
  if (::close(index_fd) != 0) {
    return Status::IOError(path, std::strerror(errno));
  }
  return ValidateShardLocations(shard_id);
}

Status BlobStore::ValidateShardLocations(uint32_t shard_id) const {
  if (shard_id >= shards_.size()) {
    return Status::InvalidArgument("blob recovery", "invalid shard");
  }
  using BlockKey = std::pair<uint64_t, uint64_t>;
  std::map<BlockKey, std::vector<BlobHash>> hashes_by_block;
  for (const auto& mapping : shards_[shard_id]->locations) {
    const BlobLocation& location = mapping.second;
    if (location.shard_id != shard_id || location.segment_id == 0) {
      return Status::BlobCorruption("blob recovery",
                                    "invalid indexed block location");
    }
    hashes_by_block[{location.segment_id, location.offset}].push_back(
        mapping.first);
  }
  for (const auto& block : hashes_by_block) {
    const std::string path = SegmentPath(shard_id, block.first.first);
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
      return Status::BlobCorruption("blob recovery",
                                    "indexed segment is missing");
    }
    std::unordered_set<BlobHash, BlobHashHasher> block_hashes;
    const auto validated = ReadBlobBlockEntry(
        fd, path, block.first.second, block.second.front(), &block_hashes);
    const int close_result = ::close(fd);
    if (!validated.ok()) return validated.status();
    if (close_result != 0) {
      return Status::IOError(path, std::strerror(errno));
    }
    for (const BlobHash& hash : block.second) {
      if (block_hashes.count(hash) == 0) {
        return Status::BlobCorruption(
            "blob recovery", "index hash is absent from referenced block");
      }
    }
  }
  return Status::OK();
}

StatusOr<BlobRef> BlobStore::Put(const std::string& raw_bytes) {
  const auto references = PutBatch({raw_bytes});
  if (!references.ok()) return references.status();
  if (references.ValueOrDie().size() != 1) {
    return Status::Corruption("blob store", "single put result mismatch");
  }
  return references.ValueOrDie().front();
}

StatusOr<std::vector<BlobRef>> BlobStore::PutBatch(
    const std::vector<std::string>& raw_blobs) {
  const Status allowed = CheckMutationAllowed();
  if (!allowed.ok()) return allowed;
  if (shards_.size() != shard_count_) {
    return Status::InvalidArgument("blob store", "store is not open");
  }
  std::vector<BlobRef> results(raw_blobs.size());
  std::vector<BlobHash> hashes;
  hashes.reserve(raw_blobs.size());
  std::vector<std::vector<size_t>> indexes_by_shard(shard_count_);
  for (size_t index = 0; index < raw_blobs.size(); ++index) {
    if (raw_blobs[index].size() > kMaximumBlobBytes) {
      return Status::InvalidArgument("blob store", "blob exceeds maximum size");
    }
    hashes.push_back(Blake3Hash(raw_blobs[index]));
    indexes_by_shard[ShardFor(hashes.back())].push_back(index);
  }

  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    if (indexes_by_shard[shard_id].empty()) continue;
    ShardState& shard = *shards_[shard_id];
    std::lock_guard<std::mutex> lock(shard.mutex);
    if (!shard.active_segment_persisted) {
      Status status = EnsureSegment(shard_id, shard.active_segment_id);
      if (!status.ok()) return status;
      status = PersistActiveSegment(shard_id, shard.active_segment_id);
      if (!status.ok()) return status;
      shard.active_segment_persisted = true;
    }

    std::unordered_map<BlobHash, size_t, BlobHashHasher> new_request_by_hash;
    std::vector<BlobWriteRequest> requests;
    std::vector<std::vector<size_t>> result_indexes;
    for (size_t input_index : indexes_by_shard[shard_id]) {
      const BlobHash& hash = hashes[input_index];
      const auto existing = shard.locations.find(hash);
      if (existing != shard.locations.end()) {
        const uint64_t raw_length = shard.raw_lengths.at(hash);
        if (raw_length != raw_blobs[input_index].size()) {
          return Status::BlobCorruption("blob store", "hash length mismatch");
        }
        AtomicSaturatingAdd(&payload_bytes_deduplicated_, raw_length);
        results[input_index] = BlobRef{hash, raw_length, existing->second};
        continue;
      }
      const auto duplicate = new_request_by_hash.find(hash);
      if (duplicate != new_request_by_hash.end()) {
        result_indexes[duplicate->second].push_back(input_index);
        AtomicSaturatingAdd(&payload_bytes_deduplicated_,
                            raw_blobs[input_index].size());
        continue;
      }
      const size_t request_index = requests.size();
      new_request_by_hash.emplace(hash, request_index);
      requests.push_back(BlobWriteRequest{hash, &raw_blobs[input_index]});
      result_indexes.push_back({input_index});
    }
    const auto written = AppendBlobBlocksLocked(shard_id, requests);
    if (!written.ok()) return written.status();
    for (size_t request_index = 0;
         request_index < written.ValueOrDie().size(); ++request_index) {
      for (size_t input_index : result_indexes[request_index]) {
        results[input_index] = written.ValueOrDie()[request_index];
      }
    }
  }
  return results;
}

StatusOr<std::string> BlobStore::ReadAt(const BlobLocation& location,
                                         const BlobHash& expected_hash) const {
  if (location.shard_id >= shard_count_ || location.segment_id == 0) {
    return Status::BlobCorruption("blob", "invalid location hint");
  }
  const std::string path = SegmentPath(location.shard_id, location.segment_id);
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Status::BlobCorruption("blob", "referenced segment is missing");
  const auto block_entry =
      ReadBlobBlockEntry(fd, path, location.offset, expected_hash);
  if (!block_entry.ok()) {
    ::close(fd);
    return block_entry.status();
  }
  const BlobBlockEntry& entry = block_entry.ValueOrDie();
  if (location.offset > std::numeric_limits<uint64_t>::max() -
                            entry.record_offset) {
    ::close(fd);
    return Status::BlobCorruption("blob", "record offset overflow");
  }
  const uint64_t absolute_record_offset =
      location.offset + entry.record_offset;
  std::string header(kBlobRecordHeaderBytes, '\0');
  const Status header_read = ReadAtFully(
      fd, absolute_record_offset, header.data(), header.size(), path);
  if (!header_read.ok()) {
    ::close(fd);
    return header_read;
  }
  size_t offset = 0;
  uint32_t magic;
  BlobHash actual_hash;
  uint64_t raw_length;
  uint64_t stored_length;
  uint8_t codec;
  uint32_t checksum;
  if (!GetU32(header, &offset, &magic) || magic != kBlobRecordMagic ||
      header.size() - offset < actual_hash.bytes.size()) {
    ::close(fd);
    return Status::BlobCorruption("blob", "invalid blob record header");
  }
  std::memcpy(actual_hash.bytes.data(), header.data() + offset, actual_hash.bytes.size());
  offset += actual_hash.bytes.size();
  if (!GetU64(header, &offset, &raw_length) ||
      !GetU64(header, &offset, &stored_length) ||
      !GetU8(header, &offset, &codec)) {
    ::close(fd);
    return Status::BlobCorruption("blob", "invalid blob record metadata");
  }
  if (!GetU32(header, &offset, &checksum) || offset != header.size() ||
      (codec != kBlobCodecNone && codec != kBlobCodecLz4) || raw_length > kMaximumBlobBytes ||
      stored_length > kMaximumBlobBytes ||
      (codec == kBlobCodecNone && raw_length != stored_length) ||
      (codec == kBlobCodecLz4 && raw_length > static_cast<uint64_t>(std::numeric_limits<int>::max())) ||
      actual_hash != expected_hash || raw_length != entry.raw_length ||
      stored_length != entry.stored_length || codec != entry.codec ||
      checksum != entry.payload_crc32c ||
      entry.record_length != kBlobRecordHeaderBytes + stored_length) {
    ::close(fd);
    return Status::BlobCorruption("blob", "invalid blob record metadata");
  }
  std::string stored_payload(static_cast<size_t>(stored_length), '\0');
  Status payload_read = Status::OK();
  if (!stored_payload.empty()) {
    payload_read = ReadAtFully(fd, absolute_record_offset + kBlobRecordHeaderBytes,
                               stored_payload.data(), stored_payload.size(), path);
  }
  if (payload_read.ok()) {
    AtomicSaturatingAdd(&payload_bytes_read_, stored_payload.size());
  }
  const int close_result = ::close(fd);
  if (!payload_read.ok()) return payload_read;
  if (close_result != 0 ||
      crc32c::Value(stored_payload.data(), stored_payload.size()) != checksum) {
    return Status::BlobCorruption("blob", "blob payload verification failed");
  }
  std::string raw_payload;
  if (codec == kBlobCodecNone) {
    raw_payload = std::move(stored_payload);
  } else {
    raw_payload.resize(static_cast<size_t>(raw_length));
    const int decoded = LZ4_decompress_safe(
        stored_payload.data(), raw_payload.data(), static_cast<int>(stored_payload.size()),
        static_cast<int>(raw_payload.size()));
    if (decoded < 0 || static_cast<uint64_t>(decoded) != raw_length) {
      return Status::BlobCorruption("blob", "invalid LZ4 blob payload");
    }
  }
  if (Blake3Hash(raw_payload) != expected_hash) {
    return Status::BlobCorruption("blob", "blob payload hash mismatch");
  }
  return raw_payload;
}

StatusOr<std::string> BlobStore::Get(const BlobRef& reference) const {
  const auto started = std::chrono::steady_clock::now();
  auto lookup = [&]() -> StatusOr<std::string> {
    if (shards_.size() != shard_count_ ||
        ShardFor(reference.content_hash) >= shards_.size()) {
      return Status::InvalidArgument("blob store", "store is not open");
    }
    if (reference.hint.segment_id != 0) {
      const auto hinted = ReadAt(reference.hint, reference.content_hash);
      if (hinted.ok()) {
        if (hinted.ValueOrDie().size() != reference.raw_length) {
          return Status::BlobCorruption("blob", "reference raw length mismatch");
        }
        return hinted;
      }
    }
    const uint32_t shard_id = ShardFor(reference.content_hash);
    const ShardState& shard = *shards_[shard_id];
    std::lock_guard<std::mutex> lock(shard.mutex);
    const auto location = shard.locations.find(reference.content_hash);
    if (location == shard.locations.end()) {
      return Status::BlobCorruption("blob", "hash is not indexed");
    }
    const auto resolved = ReadAt(location->second, reference.content_hash);
    if (!resolved.ok()) return resolved.status();
    if (resolved.ValueOrDie().size() != reference.raw_length) {
      return Status::BlobCorruption("blob", "reference raw length mismatch");
    }
    return resolved;
  };
  auto result = lookup();
  AtomicSaturatingAdd(&lookup_count_, 1);
  AtomicSaturatingAdd(&lookup_latency_ns_, ElapsedNs(started));
  return result;
}

Status BlobStore::RotateActiveSegments() {
  const Status allowed = CheckMutationAllowed();
  if (!allowed.ok()) return allowed;
  if (shards_.size() != shard_count_) {
    return Status::InvalidArgument("blob store", "store is not open");
  }
  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    ShardState& shard = *shards_[shard_id];
    std::lock_guard<std::mutex> lock(shard.mutex);
    if (shard.active_segment_id == std::numeric_limits<uint64_t>::max()) {
      return Status::InvalidArgument("blob store", "segment id overflow");
    }
    const uint64_t next_segment_id = shard.active_segment_id + 1;
    Status status = EnsureSegment(shard_id, next_segment_id);
    if (!status.ok()) return status;
    status = PersistActiveSegment(shard_id, next_segment_id);
    if (!status.ok()) return status;
    shard.active_segment_id = next_segment_id;
    shard.active_segment_persisted = true;
  }
  return Status::OK();
}

Status BlobStore::EnsureActiveSegments() {
  const Status allowed = CheckMutationAllowed();
  if (!allowed.ok()) return allowed;
  if (shards_.size() != shard_count_) {
    return Status::InvalidArgument("blob store", "store is not open");
  }
  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    ShardState& shard = *shards_[shard_id];
    std::lock_guard<std::mutex> lock(shard.mutex);
    if (shard.active_segment_persisted) continue;
    Status status = EnsureSegment(shard_id, shard.active_segment_id);
    if (!status.ok()) return status;
    status = PersistActiveSegment(shard_id, shard.active_segment_id);
    if (!status.ok()) return status;
    shard.active_segment_persisted = true;
  }
  return Status::OK();
}

StatusOr<bool> BlobStore::ActiveSegmentsNeedRotation(uint64_t target_bytes) const {
  if (shards_.size() != shard_count_ || target_bytes == 0) {
    return Status::InvalidArgument("blob store", "store must be open and target must be nonzero");
  }
  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    const ShardState& shard = *shards_[shard_id];
    std::lock_guard<std::mutex> lock(shard.mutex);
    std::error_code error;
    const uint64_t size = std::filesystem::file_size(
        SegmentPath(shard_id, shard.active_segment_id), error);
    if (error) return Status::IOError("blob store", error.message());
    if (size >= target_bytes) return true;
  }
  return false;
}

Status BlobStore::CheckpointIndex() {
  const Status allowed = CheckMutationAllowed();
  if (!allowed.ok()) return allowed;
  if (shards_.size() != shard_count_) {
    return Status::InvalidArgument("blob index", "store is not open");
  }
  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    ShardState& shard = *shards_[shard_id];
    std::lock_guard<std::mutex> lock(shard.mutex);
    const std::string checkpoint = EncodeIndexCheckpoint(shard.locations, shard.raw_lengths);
    const Status published = WriteAtomically(IndexCheckpointPath(shard_id), checkpoint);
    if (!published.ok()) return published;
    const int fd = ::open(IndexPath(shard_id).c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) return Status::IOError(IndexPath(shard_id), std::strerror(errno));
    Status status = Status::OK();
    if (::fsync(fd) != 0) status = Status::IOError(IndexPath(shard_id), std::strerror(errno));
    if (::close(fd) != 0 && status.ok()) status = Status::IOError(IndexPath(shard_id), std::strerror(errno));
    if (!status.ok()) return status;
  }
  return Status::OK();
}

StatusOr<uint64_t> BlobStore::EstimateRelocationBytes(
    const std::vector<BlobHash>& hashes) const {
  const auto estimate = EstimateGarbageCollectionWrites(hashes);
  if (!estimate.ok()) return estimate.status();
  return estimate.ValueOrDie().segment_bytes;
}

StatusOr<BlobGarbageCollectionWriteEstimate>
BlobStore::EstimateGarbageCollectionWrites(
    const std::vector<BlobHash>& live_hashes) const {
  if (shards_.size() != shard_count_) {
    return Status::InvalidArgument("blob store", "store is not open");
  }

  std::vector<std::unique_lock<std::mutex>> locks;
  locks.reserve(shards_.size());
  for (const auto& shard : shards_) locks.emplace_back(shard->mutex);

  BlobGarbageCollectionWriteEstimate estimate;
  std::unordered_set<BlobHash, BlobHashHasher> live;
  live.reserve(live_hashes.size());
  std::vector<uint64_t> projected_offsets(shard_count_, 0);
  std::vector<std::vector<EncodedBlobRecord>> records_by_shard(shard_count_);
  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    const ShardState& shard = *shards_[shard_id];
    const std::string active_path =
        SegmentPath(shard_id, shard.active_segment_id);
    std::error_code error;
    if (std::filesystem::exists(active_path, error)) {
      projected_offsets[shard_id] = std::filesystem::file_size(
          active_path, error);
    }
    if (error) return Status::IOError(active_path, error.message());
  }

  for (const BlobHash& hash : live_hashes) {
    if (!live.insert(hash).second) continue;
    const uint32_t shard_id = ShardFor(hash);
    const ShardState& shard = *shards_[shard_id];
    const auto location = shard.locations.find(hash);
    if (location == shard.locations.end()) {
      return Status::BlobCorruption("blob gc", "live hash is not indexed");
    }
    if (location->second.segment_id == shard.active_segment_id) continue;
    const auto payload = ReadAt(location->second, hash);
    if (!payload.ok()) return payload.status();
    records_by_shard[shard_id].push_back(
        EncodeRecord(hash, payload.ValueOrDie()));
  }
  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    if (records_by_shard[shard_id].empty()) continue;
    const auto blocks = PackBlobBlocks(records_by_shard[shard_id]);
    if (!blocks.ok()) return blocks.status();
    for (const EncodedBlobBlock& block : blocks.ValueOrDie()) {
      const uint64_t block_offset = projected_offsets[shard_id];
      for (size_t record_index : block.request_indexes) {
        const EncodedBlobRecord& record =
            records_by_shard[shard_id][record_index];
        const std::string index_record = EncodeIndexRecord(
            record.hash, record.raw_length,
            BlobLocation{shard_id,
                         shards_[shard_id]->active_segment_id,
                         block_offset});
        if (!CheckedAdd(index_record.size(), &estimate.index_bytes)) {
          return Status::ResourceExhausted(
              "blob gc estimate", "index estimate overflow");
        }
      }
      if (!CheckedAdd(block.bytes.size(), &estimate.segment_bytes) ||
          !CheckedAdd(block.bytes.size(), &projected_offsets[shard_id])) {
        return Status::ResourceExhausted(
            "blob gc estimate", "relocation estimate overflow");
      }
    }
    if (!CheckedAdd(3, &estimate.metadata_ops)) {
      return Status::ResourceExhausted(
          "blob gc estimate", "metadata estimate overflow");
    }
    estimate.descriptors = std::max<uint64_t>(estimate.descriptors, 3);
  }

  bool has_sealed_segment = false;
  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    const ShardState& shard = *shards_[shard_id];
    for (const auto& location : shard.locations) {
      if (live.count(location.first) == 0 &&
          location.second.segment_id != shard.active_segment_id) {
        const uint64_t tombstone_bytes =
            EncodeIndexTombstone(location.first).size();
        if (!CheckedAdd(tombstone_bytes, &estimate.index_bytes) ||
            !CheckedAdd(1, &estimate.metadata_ops)) {
          return Status::ResourceExhausted(
              "blob gc estimate", "tombstone estimate overflow");
        }
        estimate.descriptors = std::max<uint64_t>(estimate.descriptors, 1);
      }
    }
    const std::filesystem::path directory =
        std::filesystem::path(
            SegmentPath(shard_id, shard.active_segment_id)).parent_path();
    std::error_code error;
    if (!std::filesystem::exists(directory, error)) {
      if (error) return Status::IOError(directory.string(), error.message());
      continue;
    }
    for (const auto& entry :
         std::filesystem::directory_iterator(directory, error)) {
      if (error) return Status::IOError(directory.string(), error.message());
      uint64_t segment_id = 0;
      if (!entry.is_regular_file(error) || error) {
        if (error) return Status::IOError(entry.path().string(), error.message());
        continue;
      }
      if (IsSegmentFile(entry.path(), &segment_id) &&
          segment_id != shard.active_segment_id) {
        has_sealed_segment = true;
      }
    }
  }
  estimate.manifest_rewrites = has_sealed_segment ? 1 : 0;
  if (!CheckedAdd(estimate.segment_bytes, &estimate.total_bytes) ||
      !CheckedAdd(estimate.index_bytes, &estimate.total_bytes)) {
    return Status::ResourceExhausted(
        "blob gc estimate", "physical write estimate overflow");
  }
  return estimate;
}

Status BlobStore::RelocateLiveHashes(
    const std::vector<BlobHash>& hashes,
    std::shared_ptr<WorkCancellation> cancellation) {
  const Status allowed = CheckMutationAllowed();
  if (!allowed.ok()) return allowed;
  if (shards_.size() != shard_count_) {
    return Status::InvalidArgument("blob store", "store is not open");
  }
  if (cancellation != nullptr) {
    const Status checkpoint = cancellation->Checkpoint("blob gc relocation");
    if (!checkpoint.ok()) return checkpoint;
  }
  std::unordered_set<BlobHash, BlobHashHasher> observed;
  observed.reserve(hashes.size());
  std::vector<std::vector<BlobHash>> hashes_by_shard(shard_count_);
  for (const BlobHash& hash : hashes) {
    if (observed.insert(hash).second) {
      hashes_by_shard[ShardFor(hash)].push_back(hash);
    }
  }
  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    if (hashes_by_shard[shard_id].empty()) continue;
    if (cancellation != nullptr) {
      const Status checkpoint = cancellation->Checkpoint("blob gc relocation");
      if (!checkpoint.ok()) return checkpoint;
    }
    ShardState& shard = *shards_[shard_id];
    std::lock_guard<std::mutex> lock(shard.mutex);
    constexpr size_t kRelocationBatchSize = 64;
    for (size_t begin = 0; begin < hashes_by_shard[shard_id].size();
         begin += kRelocationBatchSize) {
      if (cancellation != nullptr) {
        const Status checkpoint =
            cancellation->Checkpoint("blob gc relocation");
        if (!checkpoint.ok()) return checkpoint;
      }
      const size_t end = std::min(
          hashes_by_shard[shard_id].size(), begin + kRelocationBatchSize);
      std::vector<std::string> payloads;
      payloads.reserve(end - begin);
      std::vector<BlobWriteRequest> requests;
      requests.reserve(end - begin);
      for (size_t index = begin; index < end; ++index) {
        const BlobHash& hash = hashes_by_shard[shard_id][index];
        const auto existing = shard.locations.find(hash);
        if (existing == shard.locations.end()) continue;
        if (existing->second.segment_id == shard.active_segment_id) {
          AtomicSaturatingAdd(&gc_live_bytes_, shard.raw_lengths.at(hash));
          continue;
        }
        AtomicSaturatingAdd(&gc_live_bytes_, shard.raw_lengths.at(hash));
        const auto payload = ReadAt(existing->second, hash);
        if (!payload.ok()) return payload.status();
        payloads.push_back(payload.ValueOrDie());
        requests.push_back(BlobWriteRequest{hash, nullptr});
      }
      for (size_t index = 0; index < requests.size(); ++index) {
        requests[index].raw_bytes = &payloads[index];
      }
      uint64_t stored_payload_bytes = 0;
      const auto relocated = AppendBlobBlocksLocked(
          shard_id, requests, &stored_payload_bytes);
      if (!relocated.ok()) return relocated.status();
      if (!requests.empty()) {
        AtomicSaturatingAdd(&gc_rewritten_bytes_, stored_payload_bytes);
      }
    }
  }
  return Status::OK();
}

Status BlobStore::RelocateLiveHash(const BlobHash& hash) {
  const Status allowed = CheckMutationAllowed();
  if (!allowed.ok()) return allowed;
  if (shards_.size() != shard_count_) {
    return Status::InvalidArgument("blob store", "store is not open");
  }
  const uint32_t shard_id = ShardFor(hash);
  ShardState& shard = *shards_[shard_id];
  std::lock_guard<std::mutex> lock(shard.mutex);
  const auto current = shard.locations.find(hash);
  if (current == shard.locations.end()) {
    return Status::BlobCorruption("blob gc", "live hash is not indexed");
  }
  AtomicSaturatingAdd(&gc_live_bytes_, shard.raw_lengths.at(hash));
  if (current->second.segment_id == shard.active_segment_id) return Status::OK();
  const auto payload = ReadAt(current->second, hash);
  if (!payload.ok()) return payload.status();
  const std::string raw_payload = payload.ValueOrDie();
  uint64_t stored_payload_bytes = 0;
  const auto relocated = AppendBlobBlocksLocked(
      shard_id, {BlobWriteRequest{hash, &raw_payload}},
      &stored_payload_bytes);
  if (!relocated.ok()) return relocated.status();
  AtomicSaturatingAdd(&gc_rewritten_bytes_, stored_payload_bytes);
  return Status::OK();
}

std::vector<BlobSegmentId> BlobStore::SegmentIds() const {
  std::vector<BlobSegmentId> segments;
  if (shards_.size() != shard_count_) return segments;
  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    const ShardState& shard = *shards_[shard_id];
    std::lock_guard<std::mutex> lock(shard.mutex);
    const std::filesystem::path directory =
        std::filesystem::path(SegmentPath(shard_id, shard.active_segment_id)).parent_path();
    std::error_code error;
    if (!std::filesystem::exists(directory, error) || error) continue;
    for (const auto& entry : std::filesystem::directory_iterator(directory, error)) {
      if (error || !entry.is_regular_file(error) || error) break;
      uint64_t segment_id = 0;
      if (IsSegmentFile(entry.path(), &segment_id)) {
        segments.push_back(BlobSegmentId{shard_id, segment_id, segment_id == shard.active_segment_id});
      }
    }
  }
  std::sort(segments.begin(), segments.end(), [](const auto& left, const auto& right) {
    return left.shard_id != right.shard_id ? left.shard_id < right.shard_id
                                           : left.segment_id < right.segment_id;
  });
  return segments;
}

StatusOr<std::vector<BlobSegmentId>> BlobStore::RetireUnreferencedSealedSegments(
    const std::vector<BlobHash>& live_hashes) {
  const Status allowed = CheckMutationAllowed();
  if (!allowed.ok()) return allowed;
  if (shards_.size() != shard_count_) {
    return Status::InvalidArgument("blob gc", "store is not open");
  }
  std::unordered_set<BlobHash, BlobHashHasher> live(live_hashes.begin(), live_hashes.end());
  std::vector<BlobSegmentId> retired;
  for (uint32_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
    ShardState& shard = *shards_[shard_id];
    std::lock_guard<std::mutex> lock(shard.mutex);
    for (const BlobHash& hash : live) {
      if (ShardFor(hash) != shard_id) continue;
      const auto location = shard.locations.find(hash);
      if (location == shard.locations.end()) {
        return Status::BlobCorruption("blob gc", "live hash is not indexed");
      }
      if (location->second.segment_id != shard.active_segment_id) {
        return Status::Corruption("blob gc", "live hash remains in sealed segment");
      }
    }
    std::vector<BlobHash> stale_hashes;
    for (const auto& location : shard.locations) {
      if (live.count(location.first) == 0 &&
          location.second.segment_id != shard.active_segment_id) {
        stale_hashes.push_back(location.first);
      }
    }
    for (const BlobHash& hash : stale_hashes) {
      const Status tombstone =
          AppendIndexRecord(shard_id, EncodeIndexTombstone(hash));
      if (!tombstone.ok()) return tombstone;
      shard.locations.erase(hash);
      shard.raw_lengths.erase(hash);
    }
    const std::filesystem::path directory =
        std::filesystem::path(SegmentPath(shard_id, shard.active_segment_id)).parent_path();
    std::error_code error;
    if (!std::filesystem::exists(directory, error)) {
      if (error) return Status::IOError(directory.string(), error.message());
      continue;
    }
    for (const auto& entry : std::filesystem::directory_iterator(directory, error)) {
      if (error) return Status::IOError(directory.string(), error.message());
      uint64_t segment_id = 0;
      if (!entry.is_regular_file(error) || error) {
        if (error) return Status::IOError(entry.path().string(), error.message());
        continue;
      }
      if (!IsSegmentFile(entry.path(), &segment_id) || segment_id == shard.active_segment_id) continue;
      const bool still_referenced = std::any_of(
          shard.locations.begin(), shard.locations.end(), [segment_id](const auto& location) {
            return location.second.segment_id == segment_id;
          });
      if (!still_referenced) retired.push_back(BlobSegmentId{shard_id, segment_id, false});
    }
  }
  return retired;
}

Status BlobStore::DeleteRetiredSegments(const std::vector<BlobSegmentId>& segments) {
  const Status allowed = CheckMutationAllowed();
  if (!allowed.ok()) return allowed;
  if (shards_.size() != shard_count_) {
    return Status::InvalidArgument("blob gc", "store is not open");
  }
  std::unordered_set<uint32_t> dirty_directories;
  for (const BlobSegmentId& segment : segments) {
    if (segment.shard_id >= shard_count_ || segment.segment_id == 0 || segment.active) {
      return Status::InvalidArgument("blob gc", "invalid retired segment identity");
    }
    ShardState& shard = *shards_[segment.shard_id];
    std::lock_guard<std::mutex> lock(shard.mutex);
    if (segment.segment_id == shard.active_segment_id) {
      return Status::Corruption("blob gc", "attempted to delete active segment");
    }
    const bool still_referenced = std::any_of(
        shard.locations.begin(), shard.locations.end(), [&segment](const auto& location) {
          return location.second.segment_id == segment.segment_id;
        });
    if (still_referenced) {
      return Status::Corruption("blob gc", "attempted to delete referenced segment");
    }
    std::error_code error;
    std::filesystem::remove(SegmentPath(segment.shard_id, segment.segment_id), error);
    if (error) return Status::IOError("blob gc", error.message());
    dirty_directories.insert(segment.shard_id);
  }
  for (uint32_t shard_id : dirty_directories) {
    const std::filesystem::path directory =
        std::filesystem::path(SegmentPath(shard_id, 1)).parent_path();
    const Status synced = FsyncDirectory(directory);
    if (!synced.ok()) return synced;
  }
  return Status::OK();
}

size_t BlobStore::stored_blob_count() const {
  size_t count = 0;
  for (const auto& shard : shards_) {
    std::lock_guard<std::mutex> lock(shard->mutex);
    count += shard->locations.size();
  }
  return count;
}

BlobStoreStats BlobStore::stats() const {
  return BlobStoreStats{
      payload_bytes_read_.load(std::memory_order_relaxed),
      payload_bytes_written_.load(std::memory_order_relaxed),
      payload_bytes_deduplicated_.load(std::memory_order_relaxed),
      lookup_count_.load(std::memory_order_relaxed),
      lookup_latency_ns_.load(std::memory_order_relaxed),
      gc_live_bytes_.load(std::memory_order_relaxed),
      gc_rewritten_bytes_.load(std::memory_order_relaxed)};
}

}  // namespace cedar
