#include "query/projection/projection_compression.h"

#include <algorithm>
#include <array>
#include <limits>
#include <unordered_map>
#include <vector>

#include "lz4.h"

namespace cedar::internal {
namespace {
void PutVarint(std::string* out, uint64_t value) {
  while (value >= 0x80) {
    out->push_back(static_cast<char>(value | 0x80));
    value >>= 7;
  }
  out->push_back(static_cast<char>(value));
}
bool GetVarint(const std::string& in, size_t* at, uint64_t* value) {
  uint64_t result = 0;
  for (unsigned shift = 0; shift < 64; shift += 7) {
    if (*at >= in.size()) return false;
    const uint8_t byte = static_cast<uint8_t>(in[(*at)++]);
    if (shift == 63 && byte > 1) return false;
    result |= uint64_t(byte & 0x7f) << shift;
    if ((byte & 0x80) == 0) {
      *value = result;
      return true;
    }
  }
  return false;
}
StatusOr<std::string> TransformEncode(CompressionCodec codec,
                                       const std::string& input) {
  std::string out;
  if (codec == CompressionCodec::kDelta) {
    out.reserve(input.size());
    PutVarint(&out, input.size());
    uint8_t previous = 0;
    for (unsigned char byte : input) {
      out.push_back(static_cast<char>(byte - previous));
      previous = byte;
    }
    return out;
  }
  if (codec == CompressionCodec::kRle) {
    PutVarint(&out, input.size());
    for (size_t i = 0; i < input.size();) {
      const uint8_t byte = static_cast<uint8_t>(input[i]);
      size_t j = i + 1;
      while (j < input.size() && input[j] == input[i]) ++j;
      out.push_back(static_cast<char>(byte));
      PutVarint(&out, j - i);
      i = j;
    }
    return out;
  }
  if (codec == CompressionCodec::kBitPacked) {
    uint8_t width = 0;
    for (unsigned char byte : input) {
      uint8_t bits = 0;
      for (unsigned char value = byte; value != 0; value >>= 1) ++bits;
      width = std::max(width, bits);
    }
    PutVarint(&out, input.size());
    out.push_back(static_cast<char>(width));
    uint32_t bits = 0;
    unsigned used = 0;
    for (unsigned char byte : input) {
      bits |= uint32_t(byte) << used;
      used += width;
      while (used >= 8) {
        out.push_back(static_cast<char>(bits & 0xff));
        bits >>= 8;
        used -= 8;
      }
    }
    if (used) out.push_back(static_cast<char>(bits & 0xff));
    return out;
  }
  if (codec == CompressionCodec::kDictionary) {
    std::vector<uint8_t> dictionary;
    std::array<int, 256> index;
    index.fill(-1);
    for (unsigned char byte : input) {
      if (index[byte] < 0) {
        index[byte] = static_cast<int>(dictionary.size());
        dictionary.push_back(byte);
      }
    }
    PutVarint(&out, input.size());
    PutVarint(&out, dictionary.size());
    out.append(reinterpret_cast<const char*>(dictionary.data()), dictionary.size());
    for (unsigned char byte : input) PutVarint(&out, index[byte]);
    return out;
  }
  return Status::NotSupported("projection", "unknown transform codec");
}
StatusOr<std::string> TransformDecode(CompressionCodec codec,
                                       const std::string& input, size_t limit) {
  size_t at = 0;
  uint64_t count = 0;
  if (!GetVarint(input, &at, &count))
    return Status::Corruption("projection", "invalid length varint");
  if (count > limit)
    return Status::ResourceExhausted("projection", "decoded bytes exceed budget");
  std::string out;
  out.reserve(static_cast<size_t>(count));
  if (codec == CompressionCodec::kDelta) {
    if (count > input.size() - at) return Status::Corruption("projection", "delta payload truncated");
    uint8_t previous = 0;
    for (size_t i = 0; i < count; ++i) {
      previous = static_cast<uint8_t>(previous + static_cast<uint8_t>(input[at++]));
      out.push_back(static_cast<char>(previous));
    }
    if (at != input.size()) return Status::Corruption("projection", "delta payload trailing bytes");
    return out;
  }
  if (codec == CompressionCodec::kRle) {
    while (out.size() < count) {
      if (at >= input.size()) return Status::Corruption("projection", "RLE payload truncated");
      const char byte = input[at++];
      uint64_t run = 0;
      if (!GetVarint(input, &at, &run) || run == 0 || run > count - out.size())
        return Status::Corruption("projection", "invalid RLE run");
      out.append(static_cast<size_t>(run), byte);
    }
    if (at != input.size()) return Status::Corruption("projection", "RLE payload trailing bytes");
    return out;
  }
  if (codec == CompressionCodec::kBitPacked) {
    if (at >= input.size()) return Status::Corruption("projection", "bit-packed payload truncated");
    const uint8_t width = static_cast<uint8_t>(input[at++]);
    if (width > 8) return Status::Corruption("projection", "invalid bit width");
    if (width != 0 && count > (std::numeric_limits<size_t>::max() - 7) / width)
      return Status::ResourceExhausted("projection", "bit-packed size overflow");
    const size_t bytes = (static_cast<size_t>(count) * width + 7) / 8;
    if (bytes != input.size() - at) return Status::Corruption("projection", "bit-packed length mismatch");
    uint32_t bits = 0;
    unsigned used = 0;
    for (size_t i = 0; i < count; ++i) {
      while (used < width) {
        if (at >= input.size())
          return Status::Corruption("projection", "bit-packed payload truncated");
        bits |= uint32_t(static_cast<uint8_t>(input[at++])) << used;
        used += 8;
      }
      out.push_back(static_cast<char>(width == 0 ? 0 : bits & ((1u << width) - 1u)));
      bits >>= width;
      used -= width;
    }
    return out;
  }
  if (codec == CompressionCodec::kDictionary) {
    uint64_t dictionary_size = 0;
    if (!GetVarint(input, &at, &dictionary_size) || dictionary_size > 256 ||
        dictionary_size > input.size() - at)
      return Status::Corruption("projection", "invalid dictionary");
    const size_t dict_at = at;
    at += static_cast<size_t>(dictionary_size);
    for (size_t i = 0; i < count; ++i) {
      uint64_t index = 0;
      if (!GetVarint(input, &at, &index) || index >= dictionary_size)
        return Status::Corruption("projection", "invalid dictionary index");
      out.push_back(input[dict_at + static_cast<size_t>(index)]);
    }
    if (at != input.size()) return Status::Corruption("projection", "dictionary trailing bytes");
    return out;
  }
  return Status::NotSupported("projection", "unknown transform codec");
}
}  // namespace
StatusOr<std::string> CompressProjectionPayload(CompressionCodec codec,
                                                const std::string& input) {
  if (codec == CompressionCodec::kNone) return input;
  if (codec >= CompressionCodec::kDelta) return TransformEncode(codec, input);
  if (input.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::ResourceExhausted("projection", "payload exceeds LZ4 limit");
  }
  const int bound = LZ4_compressBound(static_cast<int>(input.size()));
  std::string output(static_cast<size_t>(bound), '\0');
  const int encoded = LZ4_compress_default(
      input.data(), output.data(), static_cast<int>(input.size()), bound);
  if (encoded <= 0)
    return Status::Corruption("projection", "LZ4 compression failed");
  output.resize(static_cast<size_t>(encoded));
  return output;
}
StatusOr<std::string> DecompressProjectionPayload(CompressionCodec codec,
                                                  const std::string& input,
                                                  size_t limit) {
  if (codec == CompressionCodec::kNone) {
    if (input.size() > limit)
      return Status::ResourceExhausted("projection",
                                       "decoded bytes exceed budget");
    return input;
  }
  if (codec >= CompressionCodec::kDelta) return TransformDecode(codec, input, limit);
  if (limit > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::InvalidArgument("projection",
                                   "decode budget exceeds LZ4 limit");
  }
  if (input.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::ResourceExhausted("projection",
                                     "compressed payload exceeds LZ4 limit");
  }
  std::string output(limit, '\0');
  const int decoded = LZ4_decompress_safe(input.data(), output.data(),
                                          static_cast<int>(input.size()),
                                          static_cast<int>(limit));
  if (decoded < 0)
    return Status::Corruption("projection", "LZ4 payload corrupt");
  output.resize(static_cast<size_t>(decoded));
  return output;
}
}  // namespace cedar::internal
