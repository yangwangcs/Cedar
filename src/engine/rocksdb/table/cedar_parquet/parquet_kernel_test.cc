#include <gtest/gtest.h>

#include <array>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "table/cedar_parquet/compact_protocol.h"
#include "table/cedar_parquet/parquet_metadata.h"
#include "table/cedar_parquet/parquet_plain_page.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {
namespace {

void AssertOk(const Status& status) {
  ASSERT_TRUE(status.ok()) << status.ToString();
}

CedarParquetFooter::RowGroup MakeValidRowGroup(
    CedarParquetCompressionCodec codec) {
  CedarParquetFooter::RowGroup row_group;
  row_group.total_byte_size = 25;
  row_group.num_rows = 1;
  row_group.file_offset = 4;
  CedarParquetFooter schema = MakeRequiredFactsFooter(0, {});
  for (size_t index = 1; index < schema.schema.size(); ++index) {
    CedarParquetFooter::ColumnChunk column;
    column.path = schema.schema[index].name;
    column.physical_type = schema.schema[index].physical_type;
    column.type_length = schema.schema[index].type_length;
    column.compression_codec = codec;
    column.data_page_offset = 4;
    column.total_compressed_size = codec == CedarParquetCompressionCodec::kUncompressed
                                       ? 1
                                       : 0;
    column.total_uncompressed_size = 1;
    column.num_values = 1;
    column.offset_index_offset = 4;
    column.offset_index_length = 1;
    row_group.columns.push_back(std::move(column));
  }
  return row_group;
}

TEST(CedarParquetKernelTest, WritesAndReadsParquetMagicAndFooter) {
  CedarParquetFooter footer = MakeRequiredFactsFooter(0, {});
  std::string file = "PAR1payload";
  AssertOk(AppendParquetFooter(&file, footer));

  ASSERT_GE(file.size(), 12U);
  EXPECT_EQ(file.substr(0, 4), "PAR1");
  EXPECT_EQ(file.substr(file.size() - 4), "PAR1");

  CedarParquetFooter decoded;
  size_t footer_offset = 0;
  AssertOk(ParseParquetFooter(file, &decoded, &footer_offset));
  EXPECT_EQ(footer_offset, 11U);
  EXPECT_EQ(decoded.version, 2);
  EXPECT_EQ(decoded.num_rows, 0);
  ASSERT_EQ(decoded.schema.size(), 26U);
  EXPECT_EQ(decoded.schema[0].name, "schema");
  EXPECT_EQ(decoded.schema[1].name, "sort_key");
  EXPECT_EQ(decoded.schema[2].name, "internal_key");
  EXPECT_EQ(decoded.schema[3].name, "encoded_value");
  EXPECT_EQ(decoded.schema[4].name, "part_id");
  EXPECT_EQ(decoded.schema.back().name, "edge_type");
}

TEST(CedarParquetKernelTest, EncodesAndDecodesPlainByteArrayPage) {
  const std::array<std::string, 3> values = {"alpha", std::string("b\0eta", 5), "gamma"};
  std::vector<Slice> slices;
  for (const auto& value : values) {
    slices.emplace_back(value);
  }

  std::string page;
  AssertOk(EncodePlainByteArrayDataPage(slices, &page));
  std::vector<std::string> decoded;
  size_t consumed = 0;
  AssertOk(DecodePlainByteArrayDataPage(page, &decoded, &consumed));
  EXPECT_EQ(consumed, page.size());
  EXPECT_EQ(decoded, std::vector<std::string>(values.begin(), values.end()));
}

TEST(CedarParquetKernelTest, RejectsPagePayloadWithInvalidCrc) {
  std::vector<Slice> values = {Slice("alpha"), Slice("beta")};
  std::string page;
  AssertOk(EncodePlainByteArrayDataPage(values, &page));
  page.back() ^= 1;
  std::vector<std::string> decoded;
  EXPECT_TRUE(DecodePlainByteArrayDataPage(page, &decoded, nullptr).IsCorruption());
}

TEST(CedarParquetKernelTest, RejectsPageWhoseDecodedBodyExceedsCallerLimit) {
  std::vector<Slice> values = {Slice("alpha"), Slice("beta")};
  std::string page;
  AssertOk(EncodePlainByteArrayDataPage(values, &page));

  std::vector<std::string> decoded;
  EXPECT_TRUE(DecodePlainByteArrayDataPage(
                  page, &decoded, nullptr,
                  CedarParquetCompressionCodec::kUncompressed, 1)
                  .IsCorruption());
}

TEST(CedarParquetKernelTest, RoundTripsLz4RawPageAndRejectsMalformedStoredBody) {
  const std::string repeated(4096, 'x');
  std::vector<Slice> values = {Slice(repeated), Slice(repeated)};
  std::string page;
  AssertOk(EncodePlainByteArrayDataPage(
      values, &page, CedarParquetCompressionCodec::kLz4Raw));

  std::vector<std::string> decoded;
  size_t consumed = 0;
  AssertOk(DecodePlainByteArrayDataPage(
      page, &decoded, &consumed, CedarParquetCompressionCodec::kLz4Raw));
  EXPECT_EQ(consumed, page.size());
  EXPECT_EQ(decoded, std::vector<std::string>({repeated, repeated}));

  std::string bad_crc = page;
  bad_crc.back() ^= 1;
  EXPECT_TRUE(DecodePlainByteArrayDataPage(
                  bad_crc, &decoded, nullptr,
                  CedarParquetCompressionCodec::kLz4Raw)
                  .IsCorruption());

  std::string truncated = page;
  truncated.pop_back();
  EXPECT_TRUE(DecodePlainByteArrayDataPage(
                  truncated, &decoded, nullptr,
                  CedarParquetCompressionCodec::kLz4Raw)
                  .IsCorruption());
}

TEST(CedarParquetKernelTest, RoundTripsZstdPageAtPinnedLevelThree) {
  const std::string repeated(4096, 'z');
  std::vector<Slice> values = {Slice(repeated), Slice(repeated)};
  std::string page;
  AssertOk(EncodePlainByteArrayDataPage(
      values, &page, CedarParquetCompressionCodec::kZstd));

  std::vector<std::string> decoded;
  size_t consumed = 0;
  AssertOk(DecodePlainByteArrayDataPage(
      page, &decoded, &consumed, CedarParquetCompressionCodec::kZstd));
  EXPECT_EQ(consumed, page.size());
  EXPECT_EQ(decoded, std::vector<std::string>({repeated, repeated}));
}

TEST(CedarParquetKernelTest, SerializesSupportedParquetCompressionCodecs) {
  for (const CedarParquetCompressionCodec codec : {
           CedarParquetCompressionCodec::kUncompressed,
           CedarParquetCompressionCodec::kLz4Raw,
           CedarParquetCompressionCodec::kZstd}) {
    CedarParquetFooter footer = MakeRequiredFactsFooter(0, {});
    AssertOk(AddCedarParquetRowGroup(&footer, MakeValidRowGroup(codec)));
    std::string encoded;
    AssertOk(EncodeCompactFooter(footer, &encoded));

    CedarParquetFooter decoded;
    AssertOk(DecodeCompactFooter(encoded, &decoded));
    ASSERT_EQ(decoded.row_groups.size(), 1U);
    EXPECT_EQ(decoded.row_groups[0].columns[0].compression_codec, codec);
  }

  CedarParquetFooter invalid = MakeRequiredFactsFooter(0, {});
  AssertOk(AddCedarParquetRowGroup(
      &invalid, MakeValidRowGroup(static_cast<CedarParquetCompressionCodec>(1))));
  std::string encoded;
  EXPECT_TRUE(EncodeCompactFooter(invalid, &encoded).IsCorruption());
}

TEST(CedarParquetKernelTest, RoundTripsAscendingColumnIndex) {
  std::vector<CedarParquetFooter::ColumnChunk::PageIndex> pages = {
      {"alpha", "bravo"}, {"charlie", "delta"}};
  std::string encoded;
  AssertOk(EncodeColumnIndex(pages, &encoded));

  std::vector<CedarParquetFooter::ColumnChunk::PageIndex> decoded;
  AssertOk(DecodeColumnIndex(encoded, &decoded));
  ASSERT_EQ(decoded.size(), pages.size());
  EXPECT_EQ(decoded[0].min_value, "alpha");
  EXPECT_EQ(decoded[0].max_value, "bravo");
  EXPECT_EQ(decoded[1].min_value, "charlie");
  EXPECT_EQ(decoded[1].max_value, "delta");
}

TEST(CedarParquetKernelTest, RoundTripsUnorderedColumnIndexWithNullPage) {
  std::vector<CedarParquetFooter::ColumnChunk::PageIndex> pages = {
      {"", "", true}, {"bravo", "delta"}, {"alpha", "charlie"}};
  std::string encoded;
  AssertOk(EncodeColumnIndex(pages, &encoded));

  std::vector<CedarParquetFooter::ColumnChunk::PageIndex> decoded;
  AssertOk(DecodeColumnIndex(encoded, &decoded));
  ASSERT_EQ(decoded.size(), pages.size());
  EXPECT_TRUE(decoded[0].all_null);
  EXPECT_EQ(decoded[1].min_value, "bravo");
  EXPECT_EQ(decoded[2].max_value, "charlie");
}

TEST(CedarParquetKernelTest, RejectsDataPageWithoutRequiredRepetitionEncoding) {
  CompactWriter writer;
  writer.WriteStructBegin();
  writer.WriteI32Field(1, 0);
  writer.WriteI32Field(2, 5);
  writer.WriteI32Field(3, 5);
  writer.WriteStructFieldBegin(5);
  writer.WriteStructBegin();
  writer.WriteI32Field(1, 1);
  writer.WriteI32Field(2, 0);
  writer.WriteI32Field(3, 3);
  writer.WriteFieldStop();
  writer.WriteStructEnd();
  writer.WriteFieldStop();
  writer.WriteStructEnd();

  std::string page = writer.data();
  page.append("\x01\x00\x00\x00x", 5);
  std::vector<std::string> decoded;
  EXPECT_TRUE(DecodePlainByteArrayDataPage(page, &decoded, nullptr).IsCorruption());
}

TEST(CedarParquetKernelTest, RejectsCorruptAndUnsupportedInput) {
  CedarParquetFooter footer = MakeRequiredFactsFooter(0, {});
  std::string file;
  AssertOk(AppendParquetFooter(&file, footer));
  file.back() = 'X';
  CedarParquetFooter decoded;
  EXPECT_TRUE(ParseParquetFooter(file, &decoded, nullptr).IsCorruption());

  const std::string unsupported = "\x1d\x00";
  EXPECT_FALSE(DecodeCompactFooter(unsupported, &decoded).ok());
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
