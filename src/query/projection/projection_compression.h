#ifndef CEDAR_QUERY_PROJECTION_COMPRESSION_H_
#define CEDAR_QUERY_PROJECTION_COMPRESSION_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "cedar/core/status.h"

namespace cedar::internal {
// The first two values are the file-level codecs. The remaining codecs are
// small, deterministic column transforms used by page writers and tests.
enum class CompressionCodec : uint8_t {
  kNone = 0,
  kLz4 = 1,
  kDelta = 2,
  kBitPacked = 3,
  kDictionary = 4,
  kRle = 5,
};
StatusOr<std::string> CompressProjectionPayload(CompressionCodec codec,
                                                const std::string& input);
StatusOr<std::string> DecompressProjectionPayload(CompressionCodec codec,
                                                  const std::string& input,
                                                  size_t limit);
}  // namespace cedar::internal
#endif
