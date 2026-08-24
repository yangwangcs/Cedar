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

TEST(BoltProtocolTest, DecodesBoundedRequestSignatures) {
  EXPECT_EQ(DecodeBoltMessageKind("\xB1\x01\xA0").ValueOrDie(),
            BoltMessageKind::kHello);
  EXPECT_EQ(DecodeBoltMessageKind("\xB3\x10\x80\xA0\xA0").ValueOrDie(),
            BoltMessageKind::kRun);
  EXPECT_EQ(DecodeBoltMessageKind("\xB1\x3F\xA0").ValueOrDie(),
            BoltMessageKind::kPull);
  EXPECT_TRUE(DecodeBoltMessageKind("\xB1\x55\xA0").status().IsParseError());
}

TEST(BoltProtocolTest, EncodesBoundedSuccessAndIgnoredMessages) {
  const auto success = EncodeBoltSuccess(16);
  ASSERT_TRUE(success.ok());
  EXPECT_EQ(success.ValueOrDie(), std::string("\x00\x03\xB1\x70\xA0", 5));
  const auto ignored = EncodeBoltIgnored(16);
  ASSERT_TRUE(ignored.ok());
  EXPECT_EQ(ignored.ValueOrDie(), std::string("\x00\x03\xB1\x7E\xA0", 5));
}

}  // namespace
}  // namespace cedar::server
