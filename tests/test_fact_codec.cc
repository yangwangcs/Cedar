#include <gtest/gtest.h>

#include <array>
#include <string>
#include <vector>

#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"

namespace cedar {
namespace {

std::string Hex(std::string_view bytes) {
  static constexpr char kDigits[] = "0123456789abcdef";
  std::string hex;
  hex.reserve(bytes.size() * 2);
  for (unsigned char byte : bytes) {
    hex.push_back(kDigits[byte >> 4]);
    hex.push_back(kDigits[byte & 0x0f]);
  }
  return hex;
}

TEST(FactCodecTest, EncodesFixedWidthKeyInCanonicalOrder) {
  const FactRef ref = EntityFact::Vertex(VertexRef{PartId{7}, VertexId{7}}).ref();
  const std::string key = EncodeFactKey(ref, ValidTime{20}, CommitSeq{9});

  EXPECT_EQ(key.size(), kEncodedFactKeyBytes);
  EXPECT_EQ(Hex(key),
            "02000000070100000000000000000007ffffffffffffffebfffffffffffffff6");
  EXPECT_LT(EncodeFactKey(ref, ValidTime{20}, CommitSeq{9}),
            EncodeFactKey(ref, ValidTime{10}, CommitSeq{99}));
  EXPECT_LT(EncodeFactKey(ref, ValidTime{20}, CommitSeq{9}),
            EncodeFactKey(ref, ValidTime{20}, CommitSeq{8}));

  const auto decoded = DecodeFactKey(key);
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  EXPECT_EQ(decoded.ValueOrDie().ref, ref);
  EXPECT_EQ(decoded.ValueOrDie().valid_from, ValidTime{20});
  EXPECT_EQ(decoded.ValueOrDie().commit_seq, CommitSeq{9});
}

TEST(FactCodecTest, PreservesPartFamilyAndStorageOrderInV2SortKey) {
  const FactRef vertex =
      EntityFact::Vertex(VertexRef{PartId{7}, VertexId{42}}).ref();
  const FactRef edge =
      EntityFact::Edge(EdgeRef{PartId{8}, EdgeId{42}}).ref();
  const std::string vertex_key =
      EncodeFactKey(vertex, ValidTime{11}, CommitSeq{3});
  const std::string edge_key = EncodeFactKey(edge, ValidTime{11}, CommitSeq{3});

  EXPECT_EQ(vertex_key.size(), 32U);
  EXPECT_LT(vertex_key, edge_key);
  EXPECT_EQ(DecodeFactKey(vertex_key).ValueOrDie().ref.part_id(), PartId{7});
  EXPECT_EQ(DecodeFactKey(edge_key).ValueOrDie().ref.family(),
            FactFamily::kEdgeState);
}

TEST(FactCodecTest, RejectsMalformedKeys) {
  const FactRef ref = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}).ref();
  std::string key = EncodeFactKey(ref, ValidTime{20}, CommitSeq{9});
  key.pop_back();
  EXPECT_TRUE(DecodeFactKey(key).status().IsCorruption());

  key = EncodeFactKey(ref, ValidTime{20}, CommitSeq{9});
  key[0] = 1;
  EXPECT_TRUE(DecodeFactKey(key).status().IsCorruption());

  key = EncodeFactKey(ref, ValidTime{20}, CommitSeq{9});
  key[5] = static_cast<char>(99);
  EXPECT_TRUE(DecodeFactKey(key).status().IsCorruption());
}

TEST(FactCodecTest, RoundTripsEveryValueType) {
  const FactRef ref = PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{7}}, PropertyId{9}).ref();
  const std::array<Value, 8> values = {
      Value::Bool(true), Value::Int32(-7), Value::Int64(-9),
      Value::Float32(1.5F), Value::Float64(-2.25), Value::Timestamp(42),
      Value::String("cedar"), Value::Binary(std::string("a\\0b", 3))};

  for (const Value& value : values) {
    FactEvent event{ref, ValidTime{10}, CommitSeq{3}, FactOperation::kPut, 2,
                    value};
    const auto encoded = EncodeFactValue(event);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    const auto decoded = DecodeFactValue(ref, ValidTime{10}, CommitSeq{3},
                                          encoded.ValueOrDie());
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(decoded.ValueOrDie().value, event.value);
    EXPECT_EQ(decoded.ValueOrDie().operation, FactOperation::kPut);
    EXPECT_EQ(decoded.ValueOrDie().schema_epoch, 2U);
  }
}

TEST(FactCodecTest, RejectsCorruptAndOversizedValues) {
  const FactRef ref = PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{7}}, PropertyId{9}).ref();
  const FactEvent event{ref, ValidTime{10}, CommitSeq{3}, FactOperation::kPut,
                        2, Value::String("cedar")};
  auto encoded = EncodeFactValue(event).ConsumeValueOrDie();
  encoded.back() ^= 0x01;
  EXPECT_TRUE(DecodeFactValue(ref, ValidTime{10}, CommitSeq{3}, encoded)
                  .status()
                  .IsCorruption());

  std::string oversized(11, '\0');
  oversized[0] = 1;
  oversized[1] = static_cast<char>(FactOperation::kDelete);
  oversized[10] = static_cast<char>(0x04);
  EXPECT_TRUE(DecodeFactValue(ref, ValidTime{10}, CommitSeq{3}, oversized)
                  .status()
                  .IsCorruption());
}

TEST(FactCodecTest, RoundTripsDeleteWithoutValue) {
  const FactRef ref = PropertyFact::Edge(EdgeRef{PartId{0}, EdgeId{7}}, PropertyId{9}).ref();
  const FactEvent event{ref, ValidTime{10}, CommitSeq{3},
                        FactOperation::kDelete, 2, std::nullopt};

  const auto encoded = EncodeFactValue(event);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  const auto decoded =
      DecodeFactValue(ref, ValidTime{10}, CommitSeq{3}, encoded.ValueOrDie());
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  EXPECT_EQ(decoded.ValueOrDie().operation, FactOperation::kDelete);
  EXPECT_FALSE(decoded.ValueOrDie().value.has_value());
}

TEST(FactCodecTest, RoundTripsAuthoritativeEdgeIdentityFact) {
  const EdgeIdentity identity{
      EdgeRef{PartId{7}, EdgeId{17}},
      VertexRef{PartId{7}, VertexId{101}},
      VertexRef{PartId{9}, VertexId{202}}, 3};
  const FactRef ref(PartId{7}, FactFamily::kEdgeIdentity, PropertyId{},
                    identity.edge_id.value);
  const FactEvent event{ref, ValidTime{0}, CommitSeq{11}, FactOperation::kPut,
                        0, std::nullopt, identity};

  const auto encoded = EncodeFactValue(event);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  const auto decoded = DecodeFactValue(ref, event.valid_from, event.commit_seq,
                                       encoded.ValueOrDie());
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  EXPECT_EQ(decoded.ValueOrDie().edge_identity,
            std::optional<EdgeIdentity>{identity});
}

TEST(MetaCodecTest, UsesStableKeysAndRoundTripsTypedMetadata) {
  EXPECT_EQ(EncodeCurrentFormatKey(), "format/current");
  ASSERT_TRUE(EncodeSchemaMetaKey(PropertyId{9}, 2).ok());
  EXPECT_EQ(Hex(EncodeSchemaMetaKey(PropertyId{9}, 2).ValueOrDie()),
            "736368656d612f00092f00000002");

  const PropertyDefinition definition{PropertyId{9}, 2, "name",
                                      PropertyEntityKind::kVertex,
                                      PhysicalType::kString, 4096};
  const auto encoded = EncodePropertyDefinition(definition);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  const auto decoded = DecodePropertyDefinition(encoded.ValueOrDie());
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  EXPECT_EQ(decoded.ValueOrDie(), definition);

  const EdgeIdentity identity{EdgeId{17}, VertexId{7}, VertexId{11}, 3};
  const auto edge = EncodeEdgeIdentity(identity);
  ASSERT_TRUE(edge.ok()) << edge.status().ToString();
  EXPECT_EQ(DecodeEdgeIdentity(edge.ValueOrDie()).ValueOrDie(), identity);

  const IdAllocatorState allocator{IdKind::kVertex, 4097};
  const auto allocator_bytes = EncodeIdAllocatorState(allocator);
  ASSERT_TRUE(allocator_bytes.ok()) << allocator_bytes.status().ToString();
  EXPECT_EQ(DecodeIdAllocatorState(allocator_bytes.ValueOrDie()).ValueOrDie(),
            allocator);

  const TransactionOutcomeRecord outcome{TxnId{4}, CommitSeq{3},
                                         TransactionOutcome::kCommitted};
  const auto outcome_bytes = EncodeTransactionOutcome(outcome);
  ASSERT_TRUE(outcome_bytes.ok()) << outcome_bytes.status().ToString();
  EXPECT_EQ(DecodeTransactionOutcome(outcome_bytes.ValueOrDie()).ValueOrDie(),
            outcome);

  const SequenceRecord sequence{
      CommitSeq{3}, TxnId{4}, 99,
      {EncodeFactKey(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}).ref(), ValidTime{10},
                     CommitSeq{3})}};
  const auto sequence_bytes = EncodeSequenceRecord(sequence);
  ASSERT_TRUE(sequence_bytes.ok()) << sequence_bytes.status().ToString();
  EXPECT_EQ(DecodeSequenceRecord(sequence_bytes.ValueOrDie()).ValueOrDie(),
            sequence);
}

}  // namespace
}  // namespace cedar
