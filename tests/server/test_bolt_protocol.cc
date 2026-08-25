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

TEST(BoltProtocolTest, AuthenticatesHelloCredentialsWithoutLoggingToken) {
  const std::string hello = {static_cast<char>(0xB1), static_cast<char>(0x01),
                             static_cast<char>(0xA1), static_cast<char>(0x8B),
                             'c', 'r', 'e', 'd', 'e', 'n', 't', 'i', 'a', 'l', 's',
                             static_cast<char>(0x86), 's', 'e', 'c', 'r', 'e', 't'};
  ASSERT_EQ(hello.size(), 22U);
  EXPECT_EQ(hello.substr(4, 11), "credentials");
  EXPECT_TRUE(AuthenticateBoltHello(hello, "secret").ok())
      << AuthenticateBoltHello(hello, "secret").ToString();
  EXPECT_TRUE(AuthenticateBoltHello(hello, "wrong").IsInvalidArgument());
  EXPECT_TRUE(AuthenticateBoltHello("\xB1\x01\xA0", "secret").IsInvalidArgument());
  EXPECT_TRUE(AuthenticateBoltHello(hello, "").ok());
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

TEST(BoltProtocolTest, EncodesFailureWithStableSignatureAndMessage) {
  const auto failure = EncodeBoltFailure(
      Status::InvalidArgument("bolt", "bad request"), 256);
  ASSERT_TRUE(failure.ok()) << failure.status().ToString();
  const auto payload = DecodeBoltChunk(failure.ValueOrDie(), 256);
  ASSERT_TRUE(payload.ok());
  ASSERT_GE(payload.ValueOrDie().size(), 3U);
  EXPECT_EQ(static_cast<uint8_t>(payload.ValueOrDie()[1]), 0x7F);
  EXPECT_NE(payload.ValueOrDie().find("bad request"), std::string::npos);
}

}  // namespace
}  // namespace cedar::server
