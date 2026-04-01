#include <gtest/gtest.h>
#include "cedar/types/cedar_key.h"

using namespace cedar;

// ==================== Timestamp Tests ====================

TEST(TimestampTest, DefaultConstruction) {
  Timestamp ts;
  EXPECT_EQ(ts.value(), 0ULL);
}

TEST(TimestampTest, ValueConstruction) {
  Timestamp ts(12345);
  EXPECT_EQ(ts.value(), 12345ULL);
}

TEST(TimestampTest, EncodeDecodeForStorage) {
  Timestamp ts(1000);
  uint64_t encoded = ts.EncodeForStorage();
  Timestamp decoded = Timestamp::DecodeFromStorage(encoded);
  EXPECT_EQ(decoded.value(), 1000ULL);
}

TEST(TimestampTest, MaxMinStatic) {
  EXPECT_EQ(Timestamp::Max().value(), std::numeric_limits<uint64_t>::max());
  EXPECT_EQ(Timestamp::Min().value(), 0ULL);
  EXPECT_EQ(Timestamp::Static().value(), 1ULL);
  EXPECT_TRUE(Timestamp::Static().IsStatic());
  EXPECT_FALSE(Timestamp::Min().IsStatic());
}

TEST(TimestampTest, ComparisonOperators) {
  Timestamp a(100);
  Timestamp b(200);
  EXPECT_TRUE(a < b);
  EXPECT_TRUE(b > a);
  EXPECT_TRUE(a == a);
  EXPECT_TRUE(a != b);
  EXPECT_TRUE(a <= b);
  EXPECT_TRUE(b >= a);
}

// ==================== CedarKey Tests ====================

TEST(CedarKeyTest, DefaultConstruction) {
  CedarKey key;
  EXPECT_EQ(key.entity_id(), 0ULL);
  EXPECT_EQ(key.column_id(), 0);
  EXPECT_EQ(key.sequence(), 0);
  EXPECT_EQ(key.entity_type(), EntityType::Vertex);
  EXPECT_FALSE(key.IsDeleted());
}

TEST(CedarKeyTest, VertexConstruction) {
  CedarKey key = CedarKey::Vertex(42, 1_vcol, Timestamp(1000));
  EXPECT_EQ(key.entity_id(), 42ULL);
  EXPECT_EQ(key.column_id(), 1);
  EXPECT_EQ(key.entity_type(), EntityType::Vertex);
  EXPECT_TRUE(key.IsVertex());
  EXPECT_FALSE(key.IsEdge());
  EXPECT_EQ(key.timestamp().value(), 1000ULL);
}

TEST(CedarKeyTest, EdgeOutConstruction) {
  CedarKey key = CedarKey::EdgeOut(1, 2, 3_etype, Timestamp(2000));
  EXPECT_EQ(key.entity_id(), 1ULL);
  EXPECT_EQ(key.target_id(), 2ULL);
  EXPECT_EQ(key.column_id(), 3);
  EXPECT_EQ(key.entity_type(), EntityType::EdgeOut);
  EXPECT_TRUE(key.IsEdgeOut());
  EXPECT_TRUE(key.IsEdge());
  EXPECT_EQ(key.GetOutEdgeDstId(), 2ULL);
}

TEST(CedarKeyTest, EdgeInConstruction) {
  CedarKey key = CedarKey::EdgeIn(10, 20, 5_etype, Timestamp(3000));
  EXPECT_EQ(key.entity_id(), 10ULL);
  EXPECT_EQ(key.target_id(), 20ULL);
  EXPECT_EQ(key.column_id(), 5);
  EXPECT_EQ(key.entity_type(), EntityType::EdgeIn);
  EXPECT_TRUE(key.IsEdgeIn());
  EXPECT_EQ(key.GetInEdgeSrcId(), 20ULL);
}

TEST(CedarKeyTest, MakeEdge) {
  auto [out, in] = CedarKey::MakeEdge(100, 200, 1_etype, Timestamp(4000));
  EXPECT_EQ(out.entity_id(), 100ULL);
  EXPECT_EQ(out.target_id(), 200ULL);
  EXPECT_EQ(out.entity_type(), EntityType::EdgeOut);

  EXPECT_EQ(in.entity_id(), 200ULL);
  EXPECT_EQ(in.target_id(), 100ULL);
  EXPECT_EQ(in.entity_type(), EntityType::EdgeIn);
}

TEST(CedarKeyTest, EncodeDecode) {
  CedarKey original = CedarKey::Vertex(99, 7_vcol, Timestamp(5555), 3, 1234, key_flags::kCompressed);
  std::string encoded = original.Encode();
  EXPECT_EQ(encoded.size(), CedarKey::kKeySize);

  auto decoded = CedarKey::Decode(encoded);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->entity_id(), 99ULL);
  EXPECT_EQ(decoded->column_id(), 7);
  EXPECT_EQ(decoded->sequence(), 3);
  EXPECT_EQ(decoded->timestamp().value(), 5555ULL);
  EXPECT_TRUE(decoded->IsCompressed());
}

TEST(CedarKeyTest, Comparison) {
  CedarKey a = CedarKey::Vertex(1, 1_vcol, Timestamp(100));
  CedarKey b = CedarKey::Vertex(2, 1_vcol, Timestamp(100));
  CedarKey c = CedarKey::Vertex(1, 2_vcol, Timestamp(100));
  CedarKey d = CedarKey::Vertex(1, 1_vcol, Timestamp(200));

  EXPECT_TRUE(a < b);
  EXPECT_TRUE(a < c);
  // Same entity_id, same type, same column_id, same target_id => compare timestamp
  EXPECT_TRUE(d < a);  // timestamp is descending in storage, but Compare uses logical order
}

TEST(CedarKeyTest, SameUserKey) {
  CedarKey a = CedarKey::Vertex(1, 1_vcol, Timestamp(100), 1);
  CedarKey b = CedarKey::Vertex(1, 1_vcol, Timestamp(200), 2);
  EXPECT_TRUE(a.SameUserKey(b));

  CedarKey c = CedarKey::Vertex(1, 2_vcol, Timestamp(100), 1);
  EXPECT_FALSE(a.SameUserKey(c));
}

TEST(CedarKeyTest, FlagsManipulation) {
  CedarKey key;
  key.AddFlags(key_flags::kDeleted);
  EXPECT_TRUE(key.IsDeleted());
  key.ClearFlags(key_flags::kDeleted);
  EXPECT_FALSE(key.IsDeleted());
}

TEST(CedarKeyTest, SizeAndAlignment) {
  EXPECT_EQ(sizeof(CedarKey), 32ULL);
  EXPECT_EQ(alignof(CedarKey), 8ULL);
}

// ==================== InternalKey Tests ====================

TEST(InternalKeyTest, ConstructionAndComparison) {
  InternalKey a(1, EntityType::Vertex, 10);
  InternalKey b(2, EntityType::Vertex, 10);
  InternalKey c(1, EntityType::EdgeOut, 10);

  EXPECT_TRUE(a < b);
  EXPECT_TRUE(a < c);
  EXPECT_TRUE(a == a);
  EXPECT_FALSE(a == b);
}

TEST(InternalKeyTest, FromCedarKey) {
  CedarKey key = CedarKey::EdgeOut(5, 6, 2_etype, Timestamp(100));
  InternalKey ik(key);
  EXPECT_EQ(ik.entity_id, 5ULL);
  EXPECT_EQ(ik.entity_type, EntityType::EdgeOut);
  EXPECT_EQ(ik.column_id, 2);
  EXPECT_EQ(ik.target_id, 6ULL);
}

TEST(InternalKeyTest, Hash) {
  InternalKey a(1, EntityType::Vertex, 10);
  InternalKey b(1, EntityType::Vertex, 10);
  InternalKey c(2, EntityType::Vertex, 10);

  std::hash<InternalKey> hasher;
  EXPECT_EQ(hasher(a), hasher(b));
  EXPECT_NE(hasher(a), hasher(c));
}

// ==================== CedarKeyComparator Tests ====================

TEST(CedarKeyComparatorTest, CompareEncodedKeys) {
  CedarKeyComparator cmp;
  CedarKey a = CedarKey::Vertex(1, 1_vcol, Timestamp(100));
  CedarKey b = CedarKey::Vertex(2, 1_vcol, Timestamp(100));

  std::string sa = a.Encode();
  std::string sb = b.Encode();

  EXPECT_LT(cmp.Compare(sa, sb), 0);
  EXPECT_GT(cmp.Compare(sb, sa), 0);
  EXPECT_EQ(cmp.Compare(sa, sa), 0);
}

TEST(CedarKeyComparatorTest, CompareUserKey) {
  CedarKeyComparator cmp;
  std::string ua = CedarKey::Vertex(1, 1_vcol, Timestamp(100)).Encode();
  std::string ub = CedarKey::Vertex(1, 1_vcol, Timestamp(200)).Encode();

  EXPECT_EQ(cmp.CompareUserKey(ua, ub), 0);
}
