#include "query/projection/projection_page_reader.h"

#include <algorithm>
#include <fstream>
#include <limits>

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
    const std::string& filename, size_t allocation_limit) const {
  std::ifstream input(filename, std::ios::binary);
  if (!input) return Status::NotFound("projection page reader", "page file is unavailable");
  input.seekg(0, std::ios::end);
  const std::streamoff end = input.tellg();
  if (end < 0) return Status::IOError("projection page reader", "cannot stat page file");
  const uint64_t file_size = static_cast<uint64_t>(end);
  input.seekg(0, std::ios::beg);
  constexpr size_t kHeaderBytes = 81;
  constexpr size_t kDirectoryBytes = 91;
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
  const ProjectionChain& directory, const CoverageRequest& request) const {
  if (directory.header.entity_max_exclusive < directory.header.entity_min ||
      (directory.header.valid_to_max.has_value() &&
       directory.header.valid_to_max->value < directory.header.valid_from_min.value) ||
      !request.valid_time.Validate().ok()) {
    return Status::Corruption("projection page reader", "invalid page directory header");
  }
  ProjectionPageSelection selection;
  selection.pages_skipped = directory.page_directory.size();
  for (size_t index = 0; index < directory.page_directory.size(); ++index) {
    if (!Intersects(directory.page_directory[index], request)) continue;
    selection.page_indexes.push_back(index);
    --selection.pages_skipped;
  }
  return selection;
}

StatusOr<std::vector<ProjectionChain>> ProjectionPageReader::ReadSelected(
    const std::string& filename, const ProjectionChain& directory,
    const ProjectionPageSelection& selection, size_t allocation_limit) const {
  std::ifstream input(filename, std::ios::binary);
  if (!input) return Status::NotFound("projection page reader", "page file is unavailable");
  std::string codec_bytes(14, '\0');
  input.read(codec_bytes.data(), static_cast<std::streamsize>(codec_bytes.size()));
  if (input.gcount() != static_cast<std::streamsize>(codec_bytes.size()))
    return Status::Corruption("projection page reader", "truncated page header");
  const auto codec = CompressionCodec(uint8_t(codec_bytes[13]));
  std::vector<ProjectionChain> result;
  result.reserve(selection.page_indexes.size());
  for (size_t page_index : selection.page_indexes) {
    if (page_index >= directory.page_directory.size())
      return Status::Corruption("projection page reader", "page index outside directory");
    const auto& page = directory.page_directory[page_index];
    input.seekg(static_cast<std::streamoff>(page.offset), std::ios::beg);
    std::string payload(page.compressed_bytes, '\0');
    input.read(payload.data(), static_cast<std::streamsize>(payload.size()));
    if (input.gcount() != static_cast<std::streamsize>(payload.size()))
      return Status::Corruption("projection page reader", "truncated page payload");
    auto decoded = ReadProjectionPagePayload(directory.header, codec, page,
                                              payload, allocation_limit);
    if (!decoded.ok()) return decoded.status();
    result.push_back(std::move(decoded).ConsumeValueOrDie());
  }
  return result;
}

}  // namespace cedar::internal
