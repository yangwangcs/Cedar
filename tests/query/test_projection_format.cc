#include <gtest/gtest.h>

#include <string>

#include "cedar/core/crc32c.h"
#include "query/projection/projection_format.h"
#include "query/projection/property_index.h"

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
  ProjectionPageDirectoryEntry page;
  page.entity_min = 40;
  page.entity_max_exclusive = 48;
  page.valid_from_min = ValidTime{3};
  page.valid_to_max = ValidTime{27};
  page.edge_type_min = 100;
  page.edge_type_max = 200;
  page.bloom_bits = 64;
  page.bloom_hashes = 1;
  page.bloom_mask = (1ULL << (42 % 64)) | (1ULL << (43 % 64));
  chain.page_directory = {page};
  chain.intervals[0].entity_id = 42;
  chain.boundaries[0].entity_id = 42;
  chain.boundaries[1].entity_id = 43;
  return chain;
}

void Put32At(std::string* bytes, size_t offset, uint32_t value) {
  for (unsigned i = 0; i < 4; ++i) (*bytes)[offset + i] = static_cast<char>(value >> (8 * i));
}

void RefreshFileCrc(std::string* bytes) {
  Put32At(bytes, bytes->size() - 4,
          crc32c::Value(bytes->data(), bytes->size() - 4));
}

void RefreshHeaderCrc(std::string* bytes) {
  Put32At(bytes, 77, crc32c::Value(bytes->data(), 77));
  RefreshFileCrc(bytes);
}

std::string Bytes(std::initializer_list<unsigned char> values) {
  std::string out;
  for (unsigned char value : values) out.push_back(static_cast<char>(value));
  return out;
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
  EXPECT_EQ(decoded.ValueOrDie().page_directory.front().entity_min, 40U);
  EXPECT_EQ(decoded.ValueOrDie().page_directory.front().entity_max_exclusive, 48U);
  EXPECT_EQ(decoded.ValueOrDie().page_directory.front().valid_from_min.value, 3U);
  EXPECT_EQ(decoded.ValueOrDie().page_directory.front().valid_to_max->value, 27U);
  EXPECT_EQ(decoded.ValueOrDie().page_directory.front().edge_type_min, 100U);
  EXPECT_EQ(decoded.ValueOrDie().page_directory.front().edge_type_max, 200U);
  EXPECT_EQ(decoded.ValueOrDie().page_directory.front().bloom_bits, 64U);
  EXPECT_EQ(decoded.ValueOrDie().page_directory.front().bloom_hashes, 1U);
  EXPECT_EQ(decoded.ValueOrDie().page_directory.front().bloom_mask,
            (1ULL << (42 % 64)) | (1ULL << (43 % 64)));
  EXPECT_TRUE(PageMayContainEntity(decoded.ValueOrDie().page_directory.front(), 42));
  EXPECT_FALSE(PageMayContainEntity(decoded.ValueOrDie().page_directory.front(), 7));
  EXPECT_EQ(decoded.ValueOrDie().intervals.front().entity_id, 42U);
  EXPECT_EQ(decoded.ValueOrDie().boundaries.back().entity_id, 43U);
}

TEST(ProjectionFormatTest, RoundTripsBoundedPropertyIndexSegment) {
  PropertyIndexSegment segment;
  segment.generation_id = 4;
  segment.base_seq = CommitSeq{10};
  segment.built_through = CommitSeq{12};
  segment.property = PropertyId{7};
  segment.part_id = PartId{0};
  segment.schema_epoch = 2;
  segment.postings = {
      {VertexRef{PartId{0}, VertexId{1}}, {ValidTime{0}, ValidTime{20}},
       CommitSeq{11}, Value::String("CN")},
      {VertexRef{PartId{0}, VertexId{2}}, {ValidTime{0}, std::nullopt},
       CommitSeq{12}, Value::String("US")}};
  auto encoded = EncodePropertyIndexSegment(segment);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto decoded = DecodePropertyIndexSegment(encoded.ValueOrDie());
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  EXPECT_EQ(decoded.ValueOrDie().generation_id, 4U);
  EXPECT_EQ(decoded.ValueOrDie().part_id, PartId{0});
  EXPECT_EQ(decoded.ValueOrDie().postings, segment.postings);
  ASSERT_EQ(decoded.ValueOrDie().pages.size(), 1U);
  EXPECT_EQ(decoded.ValueOrDie().pages.front().first_posting, 0U);
  EXPECT_EQ(decoded.ValueOrDie().pages.front().row_count, 2U);
  EXPECT_EQ(decoded.ValueOrDie().pages.front().min_value, Value::String("CN"));
  EXPECT_EQ(decoded.ValueOrDie().pages.front().max_value, Value::String("US"));
  std::string corrupt = encoded.ValueOrDie();
  corrupt[corrupt.size() / 2] ^= 1;
  EXPECT_TRUE(DecodePropertyIndexSegment(corrupt).status().IsCorruption());
}

TEST(ProjectionFormatTest, PropertyIndexPageDirectoryBoundsLargeSeek) {
  PropertyIndexSegment segment;
  segment.generation_id = 3;
  segment.base_seq = CommitSeq{1};
  segment.built_through = CommitSeq{8};
  segment.property = PropertyId{8};
  segment.part_id = PartId{0};
  segment.schema_epoch = 1;
  for (uint64_t i = 0; i < 600; ++i) {
    segment.postings.push_back({
        VertexRef{PartId{0}, VertexId{i + 1}},
        {ValidTime{0}, std::nullopt}, CommitSeq{2}, Value::Int64(static_cast<int64_t>(i))});
  }
  auto encoded = EncodePropertyIndexSegment(segment);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto decoded = DecodePropertyIndexSegment(encoded.ValueOrDie());
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  ASSERT_EQ(decoded.ValueOrDie().pages.size(), 3U);
  EXPECT_EQ(decoded.ValueOrDie().pages[1].first_posting, 256U);
  EXPECT_EQ(decoded.ValueOrDie().pages[1].row_count, 256U);
  const auto matches = SeekPropertyIndexRange(
      decoded.ValueOrDie(), PropertyIndexOperator::kGreaterEqual, Value::Int64(513));
  ASSERT_EQ(matches.size(), 87U);
  EXPECT_EQ(matches.front().value, Value::Int64(513));
}

TEST(ProjectionFormatTest, PropertyIndexSeekUsesTypedNumericOrdering) {
  PropertyIndexSegment segment;
  segment.generation_id = 9;
  segment.base_seq = CommitSeq{1};
  segment.built_through = CommitSeq{3};
  segment.property = PropertyId{8};
  segment.part_id = PartId{0};
  segment.schema_epoch = 1;
  segment.postings = {
      {VertexRef{PartId{0}, VertexId{1}}, {ValidTime{0}, std::nullopt},
       CommitSeq{1}, Value::Int64(-10)},
      {VertexRef{PartId{0}, VertexId{2}}, {ValidTime{0}, std::nullopt},
       CommitSeq{2}, Value::Int64(0)},
      {VertexRef{PartId{0}, VertexId{3}}, {ValidTime{0}, std::nullopt},
       CommitSeq{3}, Value::Int64(10)}};
  ASSERT_TRUE(ValidatePropertyIndexSegment(segment).ok());
  const auto matches = SeekPropertyIndexRange(
      segment, PropertyIndexOperator::kGreaterEqual, Value::Int64(0));
  ASSERT_EQ(matches.size(), 2U);
  EXPECT_EQ(matches[0].value, Value::Int64(0));
  EXPECT_EQ(matches[1].value, Value::Int64(10));
  const auto less = SeekPropertyIndexRange(
      segment, PropertyIndexOperator::kLess, Value::Int64(0));
  ASSERT_EQ(less.size(), 1U);
  EXPECT_EQ(less.front().value, Value::Int64(-10));
}

TEST(ProjectionFormatTest, PropertyIndexSeekHonorsAllTypedBounds) {
  PropertyIndexSegment segment;
  segment.generation_id = 2;
  segment.base_seq = CommitSeq{1};
  segment.built_through = CommitSeq{4};
  segment.property = PropertyId{8};
  segment.part_id = PartId{0};
  segment.schema_epoch = 1;
  for (int64_t value = -2; value <= 2; ++value) {
    segment.postings.push_back({
        VertexRef{PartId{0}, VertexId{static_cast<uint64_t>(value + 3)}},
        {ValidTime{0}, std::nullopt}, CommitSeq{2}, Value::Int64(value)});
  }
  ASSERT_TRUE(ValidatePropertyIndexSegment(segment).ok());
  EXPECT_EQ(SeekPropertyIndexRange(segment, PropertyIndexOperator::kEqual,
                                   Value::Int64(0)).size(), 1U);
  EXPECT_EQ(SeekPropertyIndexRange(segment, PropertyIndexOperator::kLess,
                                   Value::Int64(0)).size(), 2U);
  EXPECT_EQ(SeekPropertyIndexRange(segment, PropertyIndexOperator::kLessEqual,
                                   Value::Int64(0)).size(), 3U);
  EXPECT_EQ(SeekPropertyIndexRange(segment, PropertyIndexOperator::kGreater,
                                   Value::Int64(0)).size(), 2U);
  EXPECT_EQ(SeekPropertyIndexRange(segment, PropertyIndexOperator::kGreaterEqual,
                                   Value::Int64(0)).size(), 3U);
  EXPECT_EQ(SeekPropertyIndexRange(segment, PropertyIndexOperator::kGreaterEqual,
                                   Value::Int64(-1), Value::Int64(1)).size(), 3U);
  EXPECT_TRUE(SeekPropertyIndexRange(segment, PropertyIndexOperator::kEqual,
                                     Value::String("wrong-type"))
                  .empty());
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

TEST(ProjectionFormatTest, UncompressedBytesPastRemainingBudgetIsResourceExhausted) {
  auto encoded = EncodeProjectionPage(Fixture(), CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto decoded = DecodeProjectionPage(encoded.ValueOrDie(), 10);
  ASSERT_FALSE(decoded.ok());
  EXPECT_TRUE(decoded.status().IsResourceExhausted()) << decoded.status().ToString();
}

TEST(ProjectionFormatTest, RejectsDirectoryRowCountMismatch) {
  auto encoded = EncodeProjectionPage(Fixture(), CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok());
  std::string bytes = encoded.ValueOrDie();
  Put32At(&bytes, 81 + 16, 99);
  RefreshFileCrc(&bytes);
  auto decoded = DecodeProjectionPage(bytes);
  ASSERT_FALSE(decoded.ok());
  EXPECT_TRUE(decoded.status().IsCorruption()) << decoded.status().ToString();
}

TEST(ProjectionFormatTest, RejectsNonBooleanFlagsAndReversedRanges) {
  auto encoded = EncodeProjectionPage(Fixture(), CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok());
  std::string bad_header_flag = encoded.ValueOrDie();
  bad_header_flag[64] = 2;
  RefreshHeaderCrc(&bad_header_flag);
  auto header_result = DecodeProjectionPage(bad_header_flag);
  ASSERT_FALSE(header_result.ok());
  EXPECT_TRUE(header_result.status().IsCorruption());

  std::string bad_page_range = encoded.ValueOrDie();
  // First directory entry: valid_to flag at 81 + 44, value starts at +45.
  Put32At(&bad_page_range, 81 + 45, 1);
  bad_page_range[81 + 44] = 1;
  RefreshFileCrc(&bad_page_range);
  auto page_result = DecodeProjectionPage(bad_page_range);
  ASSERT_FALSE(page_result.ok());
  EXPECT_TRUE(page_result.status().IsCorruption());
}

TEST(ProjectionFormatTest, CodecVectorsAndMalformedInputs) {
  const std::string input("\0\x01\xff", 3);
  auto delta = CompressProjectionPayload(CompressionCodec::kDelta, input);
  ASSERT_TRUE(delta.ok());
  EXPECT_EQ(delta.ValueOrDie(), std::string("\x03\0\x01\xfe", 4));
  EXPECT_EQ(DecompressProjectionPayload(CompressionCodec::kDelta,
                                        delta.ValueOrDie(), 3).ValueOrDie(), input);

  auto packed = CompressProjectionPayload(CompressionCodec::kBitPacked,
                                           std::string("\0\x01\x02\x03", 4));
  ASSERT_TRUE(packed.ok());
  EXPECT_EQ(packed.ValueOrDie(), std::string("\x04\x02\xe4", 3));
  EXPECT_EQ(DecompressProjectionPayload(CompressionCodec::kBitPacked,
                                        packed.ValueOrDie(), 4).ValueOrDie(),
            std::string("\0\x01\x02\x03", 4));

  auto rle = CompressProjectionPayload(CompressionCodec::kRle, "aaabb");
  ASSERT_TRUE(rle.ok());
  EXPECT_EQ(rle.ValueOrDie(), Bytes({5, 'a', 3, 'b', 2}));
  auto dict = CompressProjectionPayload(CompressionCodec::kDictionary, "abca");
  ASSERT_TRUE(dict.ok());
  EXPECT_EQ(dict.ValueOrDie(), Bytes({4, 3, 'a', 'b', 'c', 0, 1, 2, 0}));

  EXPECT_TRUE(DecompressProjectionPayload(CompressionCodec::kDelta, "\x80", 64)
                  .status().IsCorruption());
  EXPECT_TRUE(DecompressProjectionPayload(CompressionCodec::kRle, "\x01a\0", 64)
                  .status().IsCorruption());
  EXPECT_TRUE(DecompressProjectionPayload(CompressionCodec::kDictionary,
                                           "\x01\x01a\x01", 64)
                  .status().IsCorruption());
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
  for (CompressionCodec codec : {CompressionCodec::kNone, CompressionCodec::kLz4}) {
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

TEST(ProjectionFormatTest, FileCodecRejectsColumnTransforms) {
  for (CompressionCodec codec : {CompressionCodec::kDelta, CompressionCodec::kBitPacked,
                                 CompressionCodec::kDictionary, CompressionCodec::kRle}) {
    auto encoded = EncodeProjectionPage(Fixture(), codec);
    ASSERT_FALSE(encoded.ok());
    EXPECT_TRUE(encoded.status().IsNotSupportedError());
  }
}

TEST(ProjectionFormatTest, ReadsOnePageWithoutDecodingOtherPages) {
  ProjectionChain chain = Fixture();
  chain.page_directory.resize(2);
  chain.intervals.push_back({{ValidTime{30}, ValidTime{40}}, Value::Int64(8), 99});
  auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok());
  auto page = ReadProjectionPage(encoded.ValueOrDie(), 0);
  ASSERT_TRUE(page.ok()) << page.status().ToString();
  ASSERT_EQ(page.ValueOrDie().intervals.size(), 2U);
  EXPECT_EQ(page.ValueOrDie().intervals.front().entity_id, 42U);
}

TEST(ProjectionFormatTest, PageReaderSkipsCorruptNonTargetPayload) {
  ProjectionChain chain = Fixture();
  chain.page_directory.resize(2);
  auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok());
  std::string bytes = encoded.ValueOrDie();
  uint64_t second_offset = 0;
  for (unsigned i = 0; i < 8; ++i) second_offset |= uint64_t(uint8_t(bytes[81 + 91 + i])) << (8 * i);
  bytes[size_t(second_offset)] ^= 0x40;
  RefreshFileCrc(&bytes);
  auto page = ReadProjectionPage(bytes, 0);
  ASSERT_TRUE(page.ok()) << page.status().ToString();
}

TEST(ProjectionFormatTest, RejectsNonMonotonicRowsAndUnknownOperation) {
  ProjectionChain non_monotonic = Fixture();
  non_monotonic.intervals.push_back({{ValidTime{29}, ValidTime{40}}, Value::Int64(8), 1});
  auto interval_result = EncodeProjectionPage(non_monotonic, CompressionCodec::kNone);
  ASSERT_FALSE(interval_result.ok());
  EXPECT_TRUE(interval_result.status().IsInvalidArgument());

  ProjectionChain unknown_operation = Fixture();
  unknown_operation.boundaries.front().operation = FactOperation(9);
  auto operation_result = EncodeProjectionPage(unknown_operation, CompressionCodec::kNone);
  ASSERT_FALSE(operation_result.ok());
  EXPECT_TRUE(operation_result.status().IsInvalidArgument());
}

TEST(ProjectionFormatTest, RoundTripsMaximumEntityDelta) {
  ProjectionChain chain = Fixture();
  chain.intervals.front().entity_id = UINT64_MAX;
  chain.intervals.back().entity_id = UINT64_MAX;
  chain.boundaries.front().entity_id = UINT64_MAX;
  chain.boundaries.back().entity_id = UINT64_MAX;
  auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto decoded = DecodeProjectionPage(encoded.ValueOrDie());
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  EXPECT_EQ(decoded.ValueOrDie().intervals.front().entity_id, UINT64_MAX);
}

TEST(ProjectionFormatTest, RejectsNonZeroColumnPaddingBits) {
  auto encoded = EncodeProjectionPage(Fixture(), CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok());
  std::string bytes = encoded.ValueOrDie();
  const size_t directory_start = 81;
  uint64_t payload_offset = 0;
  for (unsigned i = 0; i < 8; ++i) payload_offset |= uint64_t(uint8_t(bytes[directory_start + i])) << (8 * i);
  bytes[size_t(payload_offset) + 33] |= static_cast<char>(0x80);
  Put32At(&bytes, directory_start + 70,
          crc32c::Value(bytes.data() + payload_offset,
                        bytes.size() - 4 - payload_offset));
  RefreshFileCrc(&bytes);
  auto decoded = DecodeProjectionPage(bytes);
  ASSERT_FALSE(decoded.ok());
  EXPECT_TRUE(decoded.status().IsCorruption());
}

TEST(ProjectionFormatTest, AdjacencyBuildsAndUsesBloomMask) {
  ProjectionChain chain = Fixture();
  chain.header.kind = ProjectionKind::kAdjacency;
  chain.page_directory.front().bloom_bits = 0;
  chain.page_directory.front().bloom_hashes = 0;
  chain.page_directory.front().bloom_mask = 0;
  auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto decoded = DecodeProjectionPage(encoded.ValueOrDie());
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  const auto& page = decoded.ValueOrDie().page_directory.front();
  EXPECT_EQ(page.bloom_bits, 64U);
  EXPECT_EQ(page.bloom_hashes, 1U);
  EXPECT_TRUE(PageMayContainEntity(page, 42));
  EXPECT_FALSE(PageMayContainEntity(page, 7));
}

TEST(ProjectionFormatTest, RejectsColumnCopiesPastAllocationBudget) {
  ProjectionChain chain = Fixture();
  for (size_t i = 0; i < 32; ++i)
    chain.intervals.push_back({{ValidTime{31 + i}, ValidTime{32 + i}}, Value::String(std::string(128, 'x')), 42});
  auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok());
  auto decoded = DecodeProjectionPage(encoded.ValueOrDie(), 512);
  ASSERT_FALSE(decoded.ok());
  EXPECT_TRUE(decoded.status().IsResourceExhausted());
}

TEST(ProjectionFormatTest, EncodesMultiplePagesAndRejectsTruncatedDirectory) {
  ProjectionChain chain = Fixture();
  chain.page_directory.resize(2);
  chain.intervals.push_back(
      {{ValidTime{30}, ValidTime{40}}, Value::Int64(8), 44});
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
