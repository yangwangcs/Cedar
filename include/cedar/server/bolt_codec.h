#ifndef CEDAR_SERVER_BOLT_CODEC_H_
#define CEDAR_SERVER_BOLT_CODEC_H_

#include <cstdint>
#include <string>

#include "cedar/core/status.h"

namespace cedar::server {

StatusOr<std::string> EncodeBoltChunk(const std::string& payload,
                                      uint32_t max_chunk_bytes = 1U * 1024U * 1024U);
StatusOr<std::string> DecodeBoltChunk(const std::string& frame,
                                      uint32_t max_chunk_bytes = 1U * 1024U * 1024U);
StatusOr<std::string> NegotiateBoltHandshake(const std::string& handshake);

enum class BoltMessageKind : uint8_t {
  kHello,
  kRun,
  kPull,
  kBegin,
  kCommit,
  kRollback,
  kReset,
  kGoodbye,
};

StatusOr<BoltMessageKind> DecodeBoltMessageKind(const std::string& payload);
StatusOr<std::string> EncodeBoltSuccess(uint32_t max_chunk_bytes = 1U * 1024U * 1024U);
StatusOr<std::string> EncodeBoltIgnored(uint32_t max_chunk_bytes = 1U * 1024U * 1024U);

}  // namespace cedar::server

#endif  // CEDAR_SERVER_BOLT_CODEC_H_
