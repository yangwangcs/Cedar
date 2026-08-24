#include <gtest/gtest.h>

#include "cedar/server/bolt_codec.h"

namespace cedar::server {
namespace {

TEST(BoltProtocolTest, RoundTripsBoundedChunk) {
  const auto encoded = EncodeBoltChunk("hello", 16);
  ASSERT_TRUE(encoded.ok());
  const auto decoded = DecodeBoltChunk(encoded.ValueOrDie(), 16);
  ASSERT_TRUE(decoded.ok());
  EXPECT_EQ(decoded.ValueOrDie(), "hello");
}

TEST(BoltProtocolTest, RejectsTruncatedAndOversizedChunks) {
  EXPECT_TRUE(DecodeBoltChunk("\x00", 16).status().IsParseError());
  EXPECT_TRUE(EncodeBoltChunk("0123456789", 4).status().IsResourceExhausted());
  EXPECT_TRUE(DecodeBoltChunk("\x00\x05x", 16).status().IsParseError());
}

TEST(BoltProtocolTest, NegotiatesSupportedHandshakeOnly) {
  const std::string handshake("\x60\x60\xB0\x17\x00\x00\x04\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", 20);
  const auto response = NegotiateBoltHandshake(handshake);
  ASSERT_TRUE(response.ok());
  EXPECT_EQ(response.ValueOrDie(), std::string("\x00\x00\x04\x05", 4));
}

}  // namespace
}  // namespace cedar::server
