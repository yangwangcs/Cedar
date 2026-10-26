#include "query/projection/projection_page_reader.h"

#include <algorithm>
#include <array>
#include <fstream>
#include <limits>
#include "query/runtime/relational.h"
#include "query/projection/projection_page_cache.h"

namespace cedar::internal {
namespace {

bool Intersects(const ProjectionPageDirectoryEntry& page,
                const CoverageRequest& request) {
  if (page.entity_max_exclusive <= request.entity_min ||
      request.entity_max_exclusive <= page.entity_min) return false;
  const uint64_t page_to = page.valid_to_max ? page.valid_to_max->value : UINT64_MAX;
  const uint64_t request_to = request.valid_time.to ? request.valid_time.to->value : UINT64_MAX;
  return page.valid_from_min.value < request_to && request.valid_time.from.value < page_to;
}

}  // namespace

StatusOr<ProjectionChain> ProjectionPageReader::ReadDirectory(
    const std::string& filename, size_t allocation_limit,
    const std::function<bool(size_t)>& reserve) const {
  std::ifstream input(filename, std::ios::binary);
  if (!input) return Status::NotFound("projection page reader", "page file is unavailable");
  input.seekg(0, std::ios::end);
  const std::streamoff end = input.tellg();
  if (end < 0) return Status::IOError("projection page reader", "cannot stat page file");
  const uint64_t file_size = static_cast<uint64_t>(end);
  input.seekg(0, std::ios::beg);
  constexpr size_t kHeaderBytes = 81;
  constexpr size_t kDirectoryBytes = 91;
  if (allocation_limit < kHeaderBytes || (reserve && !reserve(kHeaderBytes)))
    return Status::ResourceExhausted("projection page reader", "header exceeds budget");
  std::string header(kHeaderBytes, '\0');
  input.read(header.data(), static_cast<std::streamsize>(header.size()));
  if (input.gcount() != static_cast<std::streamsize>(header.size())) {
    return Status::Corruption("projection page reader", "truncated page header");
  }
  uint32_t page_count = uint32_t(uint8_t(header[73])) |
                        (uint32_t(uint8_t(header[74])) << 8) |
                        (uint32_t(uint8_t(header[75])) << 16) |
                        (uint32_t(uint8_t(header[76])) << 24);
  if (page_count > (allocation_limit - std::min(allocation_limit, kHeaderBytes)) /
                       kDirectoryBytes) {
    return Status::ResourceExhausted("projection page reader", "directory exceeds budget");
  }
  const size_t directory_bytes = kHeaderBytes + size_t(page_count) * kDirectoryBytes;
  const uint64_t working_bytes = uint64_t(directory_bytes) + kHeaderBytes +
      uint64_t(page_count) * sizeof(ProjectionPageDirectoryEntry);
  if (working_bytes > allocation_limit ||
      (reserve && !reserve(static_cast<size_t>(working_bytes) - kHeaderBytes)))
    return Status::ResourceExhausted("projection page reader", "directory scratch exceeds budget");
  std::string prefix(directory_bytes, '\0');
  std::copy(header.begin(), header.end(), prefix.begin());
  if (directory_bytes > kHeaderBytes) {
    input.read(prefix.data() + kHeaderBytes,
               static_cast<std::streamsize>(directory_bytes - kHeaderBytes));
    if (input.gcount() != static_cast<std::streamsize>(directory_bytes - kHeaderBytes)) {
      return Status::Corruption("projection page reader", "truncated page directory");
    }
  }
  CompressionCodec codec = CompressionCodec::kNone;
  return DecodeProjectionDirectory(prefix, file_size, &codec);
}

StatusOr<ProjectionPageSelection> ProjectionPageReader::Select(
  const ProjectionChain& directory, const CoverageRequest& request,
  const std::function<bool(size_t)>& reserve) const {
  if (directory.header.entity_max_exclusive < directory.header.entity_min ||
      (directory.header.valid_to_max.has_value() &&
       directory.header.valid_to_max->value < directory.header.valid_from_min.value) ||
      !request.valid_time.Validate().ok()) {
    return Status::Corruption("projection page reader", "invalid page directory header");
  }
  ProjectionPageSelection selection;
  if (reserve && !reserve(directory.page_directory.size() * sizeof(size_t)))
    return Status::ResourceExhausted("projection page reader", "selection exceeds budget");
  selection.page_indexes.reserve(directory.page_directory.size());
  selection.pages_skipped = directory.page_directory.size();
  for (size_t index = 0; index < directory.page_directory.size(); ++index) {
    if (!Intersects(directory.page_directory[index], request)) continue;
    selection.page_indexes.push_back(index);
    --selection.pages_skipped;
  }
  return selection;
}

Status ProjectionPageReader::VisitSelected(
    const std::string& filename, const ProjectionChain& directory,
    const ProjectionPageSelection& selection, QueryReservation& reservation,
    const ProjectionChainVisitor& visitor,
    const ProjectionPageReadCache* cache) const {
  if (!visitor) return Status::InvalidArgument("projection page reader", "missing visitor");
  if (selection.page_indexes.empty()) return Status::OK();
  struct WorkingReservation {
    QueryReservation& query;
    uint64_t bytes = 0;
    bool Reserve(size_t count) {
      if (!query.ReserveMemory(count).ok()) return false;
      bytes += count;
      return true;
    }
    ~WorkingReservation() { query.ReleaseMemory(bytes); }
  };
  WorkingReservation metadata{reservation};
  const uint64_t directory_bytes = sizeof(ProjectionChain) +
      uint64_t(directory.page_directory.capacity()) * sizeof(ProjectionPageDirectoryEntry) +
      uint64_t(selection.page_indexes.capacity()) * sizeof(size_t);
  if (!metadata.Reserve(directory_bytes))
    return Status::ResourceExhausted("projection page reader", "directory reservation failed");
  if (cache && !metadata.Reserve(2 * cache->namespace_key.size() + 512))
    return Status::ResourceExhausted("projection page reader", "cache key exceeds budget");
  std::ifstream input;
  CompressionCodec codec = CompressionCodec::kNone;
  for (size_t page_index : selection.page_indexes) {
    if (page_index >= directory.page_directory.size())
      return Status::Corruption("projection page reader", "page index outside directory");
    const auto& page = directory.page_directory[page_index];
    std::string key;
    if (cache && cache->pages) {
      key = cache->namespace_key;
      for (uint64_t field : {uint64_t{1}, directory.header.generation_id,
                             directory.header.base_seq.value,
                             uint64_t(directory.header.schema_epoch),
                             uint64_t(page_index), page.offset,
                             uint64_t(page.payload_crc32c)}) {
        for (unsigned shift = 0; shift < 64; shift += 8)
          key.push_back(static_cast<char>(field >> shift));
      }
      auto found = cache->pages->Lookup(key, reservation, cache->generation);
      if (!found.ok()) return found.status();
      if (found.ValueOrDie()) {
        const Status status = visitor(*found.ValueOrDie());
        if (!status.ok()) return status;
        continue;
      }
    }
    if (!input.is_open()) {
      input.open(filename, std::ios::binary);
      if (!input) return Status::NotFound("projection page reader", "page file is unavailable");
      std::array<char, 14> header{};
      input.read(header.data(), static_cast<std::streamsize>(header.size()));
      if (input.gcount() != static_cast<std::streamsize>(header.size()))
        return Status::Corruption("projection page reader", "truncated page header");
      codec = CompressionCodec(uint8_t(header[13]));
    }
    const size_t available = reservation.limit_bytes() -
        std::min(reservation.limit_bytes(), reservation.used_bytes());
    WorkingReservation working{reservation};
    if (!working.Reserve(std::max<size_t>(page.compressed_bytes, 15)))
      return Status::ResourceExhausted("projection page reader", "page input exceeds budget");
    input.seekg(static_cast<std::streamoff>(page.offset), std::ios::beg);
    std::string payload(page.compressed_bytes, '\0');
    input.read(payload.data(), static_cast<std::streamsize>(payload.size()));
    if (cache && cache->stats) {
      ++cache->stats->pages_read;
      cache->stats->physical_bytes += payload.size();
    }
    if (input.gcount() != static_cast<std::streamsize>(payload.size()))
      return Status::Corruption("projection page reader", "truncated page payload");
    auto decoded = ReadProjectionPagePayload(
        directory.header, codec, page, payload, available,
        [&working](size_t bytes) { return working.Reserve(bytes); });
    if (!decoded.ok()) return decoded.status();
    if (cache && cache->stats)
      cache->stats->decoded_bytes += ProjectionChainMemoryBytes(decoded.ValueOrDie());
    const Status status = visitor(decoded.ValueOrDie());
    if (!status.ok()) return status;
    if (cache && cache->pages && cache->admit) {
      // Input, decoded data and the new cache-entry wrapper coexist here.
      // Failure to budget the wrapper simply leaves this page transient.
      if (working.Reserve(512 + 2 * key.size()))
        cache->pages->Admit(key, &decoded.ValueOrDie());
    }
  }
  return Status::OK();
}

StatusOr<std::vector<ProjectionChain>> ProjectionPageReader::ReadSelected(
    const std::string& filename, const ProjectionChain& directory,
    const ProjectionPageSelection& selection, size_t allocation_limit) const {
  QueryReservation reservation(allocation_limit);
  constexpr size_t per_page = sizeof(ProjectionChain) +
                             sizeof(std::shared_ptr<QueryReservationLease>);
  if (selection.page_indexes.size() > allocation_limit / per_page)
    return Status::ResourceExhausted("projection page reader", "result capacity exceeds budget");
  auto containers = reservation.TryRetain(selection.page_indexes.size() * per_page);
  if (!containers)
    return Status::ResourceExhausted("projection page reader", "result reservation failed");
  std::vector<ProjectionChain> result;
  std::vector<std::shared_ptr<QueryReservationLease>> retained;
  result.reserve(selection.page_indexes.size());
  retained.reserve(selection.page_indexes.size());
  const Status status = VisitSelected(
      filename, directory, selection, reservation,
      [&](const ProjectionChain& chain) {
        auto lease = reservation.TryRetain(ProjectionChainMemoryBytes(chain));
        if (!lease)
          return Status::ResourceExhausted("projection page reader", "selected pages exceed budget");
        retained.push_back(std::move(lease));
        result.push_back(chain);
        return Status::OK();
      });
  if (!status.ok()) return status;
  return result;
}

}  // namespace cedar::internal
