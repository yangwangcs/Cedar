#include <gtest/gtest.h>
#include "cedar/types/descriptor.h"

using namespace cedar;

TEST(DescriptorTest, DefaultConstructionIsTombstone) {
  Descriptor d;
  EXPECT_EQ(d.kind(), EntryKind::Tombstone);
  EXPECT_TRUE(d.IsTombstone());
  EXPECT_FALSE(d.IsInline());
  EXPECT_FALSE(d.IsExternal());
}

TEST(DescriptorTest, InlineInt) {
  Descriptor d = Descriptor::InlineInt(5, -12345);
  EXPECT_EQ(d.kind(), EntryKind::InlineInt);
  EXPECT_EQ(d.GetColumnId(), 5);
  EXPECT_TRUE(d.IsInline());
  EXPECT_FALSE(d.IsTombstone());

  auto val = d.AsInlineInt();
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, -12345);
}

TEST(DescriptorTest, InlineFloat) {
  Descriptor d = Descriptor::InlineFloat(3, 3.14f);
  EXPECT_EQ(d.kind(), EntryKind::InlineFloat);
  EXPECT_EQ(d.GetColumnId(), 3);

  auto val = d.AsInlineFloat();
  ASSERT_TRUE(val.has_value());
  EXPECT_FLOAT_EQ(*val, 3.14f);
}

TEST(DescriptorTest, InlineShortStrWithinLimit) {
  auto opt = Descriptor::InlineShortStr(2, Slice("hi"));
  ASSERT_TRUE(opt.has_value());
  Descriptor d = *opt;
  EXPECT_EQ(d.kind(), EntryKind::InlineShortStr);
  EXPECT_EQ(d.GetLength(), 2);
  EXPECT_EQ(d.AsInlineShortStr(), "hi");
}

TEST(DescriptorTest, InlineShortStrExactLimit) {
  auto opt = Descriptor::InlineShortStr(2, Slice("abcd"));
  ASSERT_TRUE(opt.has_value());
  EXPECT_EQ(opt->GetLength(), 4);
  EXPECT_EQ(opt->AsInlineShortStr(), "abcd");
}

TEST(DescriptorTest, InlineShortStrOverLimitReturnsNullopt) {
  auto opt = Descriptor::InlineShortStr(2, Slice("hello"));
  EXPECT_FALSE(opt.has_value());
}

TEST(DescriptorTest, ExternalRef) {
  Descriptor d = Descriptor::ExternalRef(7, 0x12345678, 64);
  EXPECT_EQ(d.kind(), EntryKind::ExternalRef);
  EXPECT_EQ(d.GetColumnId(), 7);
  EXPECT_TRUE(d.IsExternal());

  auto info = d.AsExternalRef();
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->offset, 0x12345678U);
  EXPECT_EQ(info->length, 64);
  EXPECT_EQ(info->compression, CompressionType::None);
}

TEST(DescriptorTest, TombstoneFactory) {
  Descriptor d = Descriptor::Tombstone(9);
  EXPECT_EQ(d.kind(), EntryKind::Tombstone);
  EXPECT_EQ(d.GetColumnId(), 9);
  EXPECT_TRUE(d.IsTombstone());
}

TEST(DescriptorTest, PayloadAndLength) {
  Descriptor d(EntryKind::InlineInt, 1, 0xDEADBEEF, 4);
  EXPECT_EQ(d.GetPayload(), 0xDEADBEEFU);
  EXPECT_EQ(d.GetLength(), 4);

  d.SetPayload(0xCAFEBABE);
  EXPECT_EQ(d.GetPayload(), 0xCAFEBABEU);

  d.SetLength(8);
  EXPECT_EQ(d.GetLength(), 8);
}

TEST(DescriptorTest, CompressionType) {
  Descriptor d;
  d.SetCompression(CompressionType::Lz4);
  EXPECT_EQ(d.GetCompression(), CompressionType::Lz4);

  d.SetCompression(CompressionType::Zstd);
  EXPECT_EQ(d.GetCompression(), CompressionType::Zstd);

  d.SetCompression(CompressionType::None);
  EXPECT_EQ(d.GetCompression(), CompressionType::None);
}

TEST(DescriptorTest, EncodeDecode) {
  Descriptor original = Descriptor::InlineInt(42, 0x7FFFFFFF);
  std::string encoded = original.Encode();
  EXPECT_EQ(encoded.size(), 8ULL);

  auto decoded = Descriptor::Decode(Slice(encoded));
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->kind(), EntryKind::InlineInt);
  EXPECT_EQ(decoded->GetColumnId(), 42);

  auto val = decoded->AsInlineInt();
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, 0x7FFFFFFF);
}

TEST(DescriptorTest, DecodeTooShortReturnsNullopt) {
  std::string buf(7, '\x00');
  auto decoded = Descriptor::Decode(Slice(buf));
  EXPECT_FALSE(decoded.has_value());
}

TEST(DescriptorTest, RawValueRoundTrip) {
  Descriptor original(EntryKind::InlineFloat, 5, 0x12345678, 4);
  uint64_t raw = original.AsRaw();
  Descriptor reconstructed(raw);
  EXPECT_EQ(reconstructed.AsRaw(), raw);
  EXPECT_EQ(reconstructed.kind(), EntryKind::InlineFloat);
  EXPECT_EQ(reconstructed.GetColumnId(), 5);
  EXPECT_EQ(reconstructed.GetPayload(), 0x12345678U);
}

TEST(DescriptorTest, BlobRefOperations) {
  Descriptor d = Descriptor::MakeBlobRef(0x00001000, 64, 0xAB);
  EXPECT_TRUE(d.IsBlobRef());

  auto ref = d.GetBlobRef();
  EXPECT_EQ(ref.offset, 0x00001000U);
  EXPECT_EQ(ref.size_kb, 64);
  EXPECT_EQ(ref.checksum, 0xAB);

  EXPECT_EQ(d.GetBlobOffset(), 0x00001000U);
  EXPECT_EQ(d.GetBlobSizeKb(), 64);
  EXPECT_EQ(d.GetBlobChecksum(), 0xAB);
}

TEST(DescriptorTest, DebugStringDoesNotCrash) {
  Descriptor d = Descriptor::InlineInt(1, 100);
  std::string s = d.DebugString();
  EXPECT_FALSE(s.empty());
}
