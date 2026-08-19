#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <string>
#include <cstring>
#include <vector>

#include "db/dbformat.h"
#include "table/cedar_parquet/cedar_parquet_format.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {
namespace {

void StoreBigEndian64(std::string* destination, size_t offset, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    (*destination)[offset++] = static_cast<char>(value >> shift);
  }
}

std::string V2UserKey(uint64_t entity_id) {
  std::string user_key(32, '\0');
  user_key[0] = 2;
  user_key[5] = 1;
  StoreBigEndian64(&user_key, 8, entity_id);
  return user_key;
}

std::string InternalKeyFor(uint64_t entity_id, SequenceNumber sequence,
                           ValueType type) {
  std::string internal_key;
  AppendInternalKey(&internal_key,
                    ParsedInternalKey(V2UserKey(entity_id), sequence, type));
  return internal_key;
}

void StoreBigEndian32(std::string* destination, size_t offset, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    (*destination)[offset++] = static_cast<char>(value >> shift);
  }
}

void AppendBigEndian32(std::string* destination, uint32_t value) {
  destination->push_back(static_cast<char>(value >> 24));
  destination->push_back(static_cast<char>(value >> 16));
  destination->push_back(static_cast<char>(value >> 8));
  destination->push_back(static_cast<char>(value));
}

void AppendBigEndian64(std::string* destination, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    destination->push_back(static_cast<char>(value >> shift));
  }
}

std::string PropertyUserKey() {
  std::string key(32, '\0');
  key[0] = 2;
  StoreBigEndian32(&key, 1, 7);
  key[5] = 2;
  key[7] = 9;
  StoreBigEndian64(&key, 8, 42);
  StoreBigEndian64(&key, 16, ~uint64_t{11});
  StoreBigEndian64(&key, 24, ~uint64_t{3});
  return key;
}

std::string Int32FactValue() {
  std::string value;
  value.push_back(1);
  value.push_back(1);
  AppendBigEndian32(&value, 5);
  value.push_back(2);
  AppendBigEndian32(&value, 4);
  AppendBigEndian32(&value, 1234);
  AppendBigEndian32(&value, crc32c::Value(value.data(), value.size()));
  return value;
}

std::string EdgeIdentityUserKey() {
  std::string key(32, '\0');
  key[0] = 2;
  StoreBigEndian32(&key, 1, 7);
  key[5] = 3;
  StoreBigEndian64(&key, 8, 17);
  StoreBigEndian64(&key, 16, ~uint64_t{0});
  StoreBigEndian64(&key, 24, ~uint64_t{3});
  return key;
}

std::string EdgeIdentityFactValue() {
  std::string value;
  value.push_back(1);
  value.push_back(1);
  AppendBigEndian32(&value, 0);
  value.push_back(9);
  AppendBigEndian32(&value, 44);
  AppendBigEndian32(&value, 7);
  AppendBigEndian64(&value, 17);
  AppendBigEndian32(&value, 7);
  AppendBigEndian64(&value, 101);
  AppendBigEndian32(&value, 9);
  AppendBigEndian64(&value, 202);
  AppendBigEndian64(&value, 3);
  AppendBigEndian32(&value, crc32c::Value(value.data(), value.size()));
  return value;
}

TEST(CedarParquetFormatTest, RoundTripsBinaryCanonicalRows) {
  const std::string internal_key = InternalKeyFor(3, 99, kTypeValue);
  const std::string encoded_value("v\0alue", 7);
  CedarParquetRow row;
  ASSERT_TRUE(EncodeCedarParquetRow(internal_key, encoded_value, &row).ok());
  EXPECT_EQ(row.sort_key.size(), kCedarParquetV2SortKeyBytes);

  std::string decoded_key;
  std::string decoded_value;
  ASSERT_TRUE(DecodeCedarParquetRow(row, &decoded_key, &decoded_value).ok());
  EXPECT_EQ(decoded_key, internal_key);
  EXPECT_EQ(decoded_value, encoded_value);
}

TEST(CedarParquetFormatTest, SortKeysMatchBytewiseInternalComparator) {
  const std::array<std::string, 5> keys = {
      InternalKeyFor(2, 1, kTypeValue),
      InternalKeyFor(1, 3, kTypeDeletion),
      InternalKeyFor(1, 9, kTypeValue),
      InternalKeyFor(1, 7, kTypeDeletion),
      InternalKeyFor(1, 7, kTypeValue),
  };
  InternalKeyComparator comparator(BytewiseComparator());
  std::vector<std::string> by_internal(keys.begin(), keys.end());
  std::sort(by_internal.begin(), by_internal.end(), [&comparator](
      const std::string& left, const std::string& right) {
    return comparator.Compare(left, right) < 0;
  });

  std::vector<std::pair<std::string, std::string>> by_sort_key;
  for (const std::string& key : keys) {
    std::string sort_key;
    ASSERT_TRUE(EncodeCedarParquetSortKey(key, &sort_key).ok());
    by_sort_key.emplace_back(std::move(sort_key), key);
  }
  std::sort(by_sort_key.begin(), by_sort_key.end());
  for (size_t index = 0; index < by_internal.size(); ++index) {
    EXPECT_EQ(by_sort_key[index].second, by_internal[index]);
  }
}

TEST(CedarParquetFormatTest, RejectsMalformedInternalKeysAndSortKeys) {
  std::string sort_key;
  EXPECT_TRUE(EncodeCedarParquetSortKey("too-short", &sort_key).IsCorruption());
  EXPECT_TRUE(DecodeCedarParquetSortKey("\0\0", &sort_key).IsCorruption());
}

TEST(CedarParquetFormatTest, MaterializesCanonicalFactIntoTypedLanes) {
  std::string internal_key = InternalKeyFor(42, 17, kTypeValue);
  std::string user_key = PropertyUserKey();
  internal_key.replace(0, user_key.size(), user_key);
  CedarParquetMaterializedFact fact;
  ASSERT_TRUE(DecodeCedarParquetMaterializedFact(
                  internal_key, Int32FactValue(), &fact)
                  .ok());
  EXPECT_EQ(fact.part_id, 7U);
  EXPECT_EQ(fact.fact_family, 2U);
  EXPECT_EQ(fact.property_id, 9U);
  EXPECT_EQ(fact.entity_id, 42U);
  EXPECT_EQ(fact.valid_from, 11U);
  EXPECT_EQ(fact.cedar_commit_seq, 3U);
  EXPECT_EQ(fact.rocksdb_sequence, 17U);
  EXPECT_EQ(fact.operation, 1U);
  EXPECT_EQ(fact.schema_epoch, 5U);
  EXPECT_EQ(fact.physical_type, 2U);
  ASSERT_TRUE(fact.int32_value.has_value());
  EXPECT_EQ(*fact.int32_value, 1234);
  EXPECT_FALSE(fact.bytes_value.has_value());
}

TEST(CedarParquetFormatTest, MaterializesAuthoritativeEdgeIdentityLanes) {
  std::string internal_key = InternalKeyFor(17, 19, kTypeValue);
  internal_key.replace(0, 32, EdgeIdentityUserKey());
  CedarParquetMaterializedFact fact;
  ASSERT_TRUE(DecodeCedarParquetMaterializedFact(
                  internal_key, EdgeIdentityFactValue(), &fact)
                  .ok());
  EXPECT_EQ(fact.part_id, 7U);
  EXPECT_EQ(fact.fact_family, 3U);
  EXPECT_EQ(fact.entity_id, 17U);
  EXPECT_EQ(fact.valid_from, 0U);
  EXPECT_EQ(fact.cedar_commit_seq, 3U);
  ASSERT_TRUE(fact.source_part_id.has_value());
  EXPECT_EQ(*fact.source_part_id, 7U);
  ASSERT_TRUE(fact.source_vertex_id.has_value());
  EXPECT_EQ(*fact.source_vertex_id, 101U);
  ASSERT_TRUE(fact.target_part_id.has_value());
  EXPECT_EQ(*fact.target_part_id, 9U);
  ASSERT_TRUE(fact.target_vertex_id.has_value());
  EXPECT_EQ(*fact.target_vertex_id, 202U);
  ASSERT_TRUE(fact.edge_type.has_value());
  EXPECT_EQ(*fact.edge_type, 3U);
}

TEST(CedarParquetFormatTest, BoundsRowGroupMemoryAndPreservesInputOrder) {
  CedarParquetTableOptions options;
  options.row_group_max_rows = 2;
  options.row_group_max_bytes = 1024;
  options.max_row_bytes = 128;
  CedarParquetRowGroupBuilder builder(options);

  CedarParquetRow first;
  CedarParquetRow second;
  CedarParquetRow third;
  ASSERT_TRUE(EncodeCedarParquetRow(InternalKeyFor(1, 3, kTypeValue), "1", &first).ok());
  ASSERT_TRUE(EncodeCedarParquetRow(InternalKeyFor(2, 2, kTypeDeletion), "2", &second).ok());
  ASSERT_TRUE(EncodeCedarParquetRow(InternalKeyFor(3, 1, kTypeValue), "3", &third).ok());
  ASSERT_TRUE(builder.Add(std::move(first)).ok());
  ASSERT_TRUE(builder.Add(std::move(second)).ok());
  EXPECT_TRUE(builder.Add(std::move(third)).IsIncomplete());
  ASSERT_EQ(builder.rows().size(), 2U);
  EXPECT_EQ(builder.rows()[0].internal_key, InternalKeyFor(1, 3, kTypeValue));
  EXPECT_EQ(builder.rows()[1].internal_key, InternalKeyFor(2, 2, kTypeDeletion));
  EXPECT_LE(builder.resident_bytes(), options.row_group_max_bytes);

  builder.Reset();
  EXPECT_TRUE(builder.rows().empty());
  EXPECT_EQ(builder.resident_bytes(), 0U);
}

TEST(CedarParquetFormatTest, RejectsRowsOverTheConfiguredBudget) {
  CedarParquetTableOptions options;
  options.row_group_max_rows = 4;
  options.row_group_max_bytes = 1024;
  options.max_row_bytes = 1024;
  CedarParquetRowGroupBuilder builder(options);
  CedarParquetRow row;
  ASSERT_TRUE(EncodeCedarParquetRow(InternalKeyFor(1, 1, kTypeValue),
                                    std::string(1024, 'x'), &row).ok());
  EXPECT_TRUE(builder.Add(std::move(row)).IsMemoryLimit());
}

TEST(CedarParquetFormatTest, ReservesPageHeadersInsideTheRowGroupBudget) {
  CedarParquetTableOptions options;
  options.row_group_max_bytes = 256;
  options.max_row_bytes = 128;
  EXPECT_TRUE(options.Validate().IsInvalidArgument());
}

TEST(CedarParquetFormatTest, RejectsZeroFooterBudget) {
  CedarParquetTableOptions options;
  options.max_footer_bytes = 0;
  EXPECT_TRUE(options.Validate().IsInvalidArgument());
}

TEST(CedarParquetFormatTest, RejectsUnsupportedPageCompressionCodec) {
  CedarParquetTableOptions options;
  options.page_compression = static_cast<CedarParquetCompressionCodec>(1);
  EXPECT_TRUE(options.Validate().IsInvalidArgument());
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
