#include <gtest/gtest.h>

#include <string>

#include "query/projection/projection_format.h"

namespace cedar::internal {
namespace {

ProjectionChain Fixture() {
  ProjectionChain chain;
  chain.header.kind = ProjectionKind::kState;
  chain.header.generation_id = 7;
  chain.header.base_seq = CommitSeq{11};
  chain.header.part_id = PartId{3};
  chain.header.schema_epoch = 5;
  chain.header.entity_min = 4;
  chain.header.entity_max_exclusive = 9;
  chain.header.valid_from_min = ValidTime{0};
  chain.header.valid_to_max = ValidTime{30};
  chain.intervals = {{{ValidTime{0}, ValidTime{30}}, Value::Int64(7)}};
  chain.boundaries = {{ValidTime{0}, FactOperation::kPut, Value::Int64(7)},
                      {ValidTime{10}, FactOperation::kPut, Value::Int64(7)}};
  chain.page_directory = {{0, 0, 0, 1, 4, 9, ValidTime{0}, ValidTime{30}, 0}};
  return chain;
}

TEST(ProjectionFormatTest, RoundTripsIntervalsAndLatentBoundaries) {
  const ProjectionChain chain = Fixture();
  auto encoded = EncodeProjectionPage(chain, CompressionCodec::kLz4);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto decoded = DecodeProjectionPage(encoded.ValueOrDie(), 1 << 20);
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  EXPECT_EQ(decoded.ValueOrDie().header, chain.header);
  EXPECT_EQ(decoded.ValueOrDie().intervals, chain.intervals);
  EXPECT_EQ(decoded.ValueOrDie().boundaries, chain.boundaries);
  ASSERT_EQ(decoded.ValueOrDie().page_directory.size(), 1U);
  EXPECT_EQ(decoded.ValueOrDie().page_directory.front().row_count, 3U);
  EXPECT_GT(decoded.ValueOrDie().page_directory.front().compressed_bytes, 0U);
}

TEST(ProjectionFormatTest, RejectsBitFlippedPayload) {
  auto encoded = EncodeProjectionPage(Fixture(), CompressionCodec::kLz4);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  std::string bytes = encoded.ValueOrDie();
  bytes[bytes.size() / 2] ^= 0x40;
  auto decoded = DecodeProjectionPage(bytes, 1 << 20);
  ASSERT_FALSE(decoded.ok());
  EXPECT_TRUE(decoded.status().IsCorruption());
}

TEST(ProjectionFormatTest, RefusesToAllocatePastCallerBudget) {
  auto encoded = EncodeProjectionPage(Fixture(), CompressionCodec::kLz4);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto decoded = DecodeProjectionPage(encoded.ValueOrDie(), 1);
  ASSERT_FALSE(decoded.ok());
  EXPECT_TRUE(decoded.status().IsResourceExhausted());
}

TEST(ProjectionFormatTest, RejectsUnknownFormatVersion) {
  auto encoded = EncodeProjectionPage(Fixture(), CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  std::string bytes = encoded.ValueOrDie();
  bytes[8] = 2;
  auto decoded = DecodeProjectionPage(bytes, 1 << 20);
  ASSERT_FALSE(decoded.ok());
  EXPECT_TRUE(decoded.status().IsNotSupportedError());
}

TEST(ProjectionFormatTest, RejectsUnknownCompressionCodec) {
  auto encoded = EncodeProjectionPage(Fixture(), CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  std::string bytes = encoded.ValueOrDie();
  bytes[13] = 9;
  auto decoded = DecodeProjectionPage(bytes, 1 << 20);
  ASSERT_FALSE(decoded.ok());
  EXPECT_TRUE(decoded.status().IsNotSupportedError());
}

TEST(ProjectionFormatTest, EncodesFixedWidthIntegersLittleEndian) {
  auto encoded = EncodeProjectionPage(Fixture(), CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  const std::string& bytes = encoded.ValueOrDie();
  ASSERT_GE(bytes.size(), 34U);
  EXPECT_EQ(static_cast<unsigned char>(bytes[8]), 1U);
  EXPECT_EQ(static_cast<unsigned char>(bytes[14]), 7U);
  EXPECT_EQ(static_cast<unsigned char>(bytes[22]), 11U);
  EXPECT_EQ(static_cast<unsigned char>(bytes[30]), 3U);
}

TEST(ProjectionFormatTest, RoundTripsAllBoundedPageCodecs) {
  for (CompressionCodec codec : {CompressionCodec::kNone,
                                 CompressionCodec::kLz4,
                                 CompressionCodec::kDelta,
                                 CompressionCodec::kBitPacked,
                                 CompressionCodec::kDictionary,
                                 CompressionCodec::kRle}) {
    auto encoded = EncodeProjectionPage(Fixture(), codec);
    ASSERT_TRUE(encoded.ok()) << static_cast<int>(codec) << ": "
                              << encoded.status().ToString();
    auto decoded = DecodeProjectionPage(encoded.ValueOrDie(), 1 << 20);
    ASSERT_TRUE(decoded.ok()) << static_cast<int>(codec) << ": "
                              << decoded.status().ToString();
    EXPECT_EQ(decoded.ValueOrDie().intervals, Fixture().intervals);
    EXPECT_EQ(decoded.ValueOrDie().boundaries, Fixture().boundaries);
  }
}

TEST(ProjectionFormatTest, EncodesMultiplePagesAndRejectsTruncatedDirectory) {
  ProjectionChain chain = Fixture();
  chain.page_directory.resize(2);
  chain.intervals.push_back(
      {{ValidTime{30}, ValidTime{40}}, Value::Int64(8)});
  auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto decoded = DecodeProjectionPage(encoded.ValueOrDie(), 1 << 20);
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  ASSERT_EQ(decoded.ValueOrDie().page_directory.size(), 2U);
  EXPECT_EQ(decoded.ValueOrDie().intervals.size(), 2U);
  EXPECT_EQ(decoded.ValueOrDie().boundaries.size(), 2U);

  std::string truncated = encoded.ValueOrDie();
  truncated.resize(80);
  auto malformed = DecodeProjectionPage(truncated, 1 << 20);
  ASSERT_FALSE(malformed.ok());
  EXPECT_TRUE(malformed.status().IsCorruption() ||
              malformed.status().IsResourceExhausted());
}

}  // namespace
}  // namespace cedar::internal
