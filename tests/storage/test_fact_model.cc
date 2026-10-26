#include <gtest/gtest.h>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"

namespace cedar {
namespace {

template <typename T>
concept HasBareVertexEntityFact = requires {
  EntityFact::Vertex(T{7});
};

template <typename T>
concept HasBareVertexPropertyFact = requires {
  PropertyFact::Vertex(T{7}, PropertyId{9});
};

static_assert(!HasBareVertexEntityFact<VertexId>);
static_assert(!HasBareVertexPropertyFact<VertexId>);

struct NonDefaultConstructible {
  explicit NonDefaultConstructible(uint64_t value) : value(value) {}

  uint64_t value;
};

TEST(StatusOrTest, CarriesErrorWithoutConstructingValue) {
  StatusOr<NonDefaultConstructible> result(
      Status::InvalidArgument("test", "expected failure"));

  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalidArgument());
}

TEST(FactModelTest, SeparatesEntityStateFromProperties) {
  const EntityFact vertex = EntityFact::Vertex(VertexRef{PartId{3}, VertexId{7}});
  const PropertyFact property =
      PropertyFact::Vertex(VertexRef{PartId{3}, VertexId{7}}, PropertyId{9});

  EXPECT_EQ(vertex.ref().family(), FactFamily::kVertexState);
  EXPECT_EQ(vertex.ref().property_id(), PropertyId{0});
  EXPECT_EQ(property.ref().family(), FactFamily::kVertexProperty);
  EXPECT_EQ(property.ref().property_id(), PropertyId{9});
  EXPECT_EQ(property.ref().entity_id(), 7U);
  EXPECT_EQ(property.ref().part_id(), PartId{3});
}

TEST(FactModelTest, ReleasesReadReservationAfterDestroyingOwnedPayload) {
  bool released = false;
  {
    FactEvent event{EntityFact::Vertex({PartId{0}, VertexId{1}}).ref(),
                    ValidTime{1}, CommitSeq{1}};
    event.value = Value::String(std::string(4096, 'x'));
    event.read_memory_owner = std::shared_ptr<const void>(
        new int(1), [&](const void* token) {
          // The owner is the final field, so value is still a live optional
          // here. Its payload must already be cleared before releasing budget.
          EXPECT_FALSE(event.value.has_value());
          released = true;
          delete static_cast<const int*>(token);
        });
  }
  EXPECT_TRUE(released);
}

TEST(FactModelTest, MovesPayloadAndReservationWithoutCopyingStringStorage) {
  FactEvent event{EntityFact::Vertex({PartId{0}, VertexId{1}}).ref(),
                  ValidTime{1}, CommitSeq{1}};
  event.value = Value::String(std::string(4096, 'x'));
  auto owner = std::make_shared<int>(1);
  event.read_memory_owner = owner;
  const auto* bytes = std::get<std::string>(event.value->data()).data();
  FactEvent moved(std::move(event));
  EXPECT_EQ(std::get<std::string>(moved.value->data()).data(), bytes);
  EXPECT_FALSE(event.read_memory_owner);
  EXPECT_EQ(moved.read_memory_owner, owner);
  FactEvent assigned{EntityFact::Vertex({PartId{0}, VertexId{2}}).ref(),
                     ValidTime{1}, CommitSeq{1}};
  assigned = std::move(moved);
  EXPECT_EQ(std::get<std::string>(assigned.value->data()).data(), bytes);
  EXPECT_FALSE(moved.read_memory_owner);
  EXPECT_EQ(assigned.read_memory_owner, owner);
}

TEST(FactModelTest, ValidatesDistinctEdgeIdentity) {
  const EdgeIdentity identity{EdgeRef{PartId{7}, EdgeId{17}},
                              VertexRef{PartId{7}, VertexId{7}},
                              VertexRef{PartId{12}, VertexId{11}}, 3};
  EXPECT_TRUE(identity.Validate().ok());
  EXPECT_FALSE((EdgeIdentity{EdgeRef{PartId{7}, EdgeId{17}},
                              VertexRef{PartId{7}, VertexId{7}},
                              VertexRef{PartId{12}, VertexId{11}}, 0}
                    .Validate()
                    .ok()));
}

TEST(FactModelTest, RequiresSchemaAndValueOnlyForProperties) {
  PendingFactMutation existence{EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}).ref(),
                                ValidTime{10}, FactOperation::kPut,
                                0, std::nullopt};
  EXPECT_TRUE(existence.Validate().ok());

  PendingFactMutation property{
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{7}}, PropertyId{9}).ref(), ValidTime{10},
      FactOperation::kPut, 2, Value::String("cedar")};
  EXPECT_TRUE(property.Validate().ok());

  property.schema_epoch = 0;
  EXPECT_FALSE(property.Validate().ok());
}

TEST(StatusTest, SnapshotExpiredIsTypedAndStable) {
  const Status status = Status::SnapshotExpired(
      "snapshot", "sequence is below retention boundary");

  EXPECT_TRUE(status.IsSnapshotExpired());
  EXPECT_EQ(status.ToString(),
            "SnapshotExpired: snapshot: sequence is below retention boundary");
}

TEST(StatusTest, DistinguishesKernelValidationFailures) {
  EXPECT_TRUE(Status::IdentityConflict("edge", "binding changed")
                  .IsIdentityConflict());
  EXPECT_TRUE(Status::SnapshotPinned("vacuum", "active reader")
                  .IsSnapshotPinned());
  EXPECT_TRUE(Status::UnsupportedSerializablePredicate("strict", "range")
                  .IsUnsupportedSerializablePredicate());
}

}  // namespace
}  // namespace cedar
