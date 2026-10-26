#include <gtest/gtest.h>

#include "query/projection/projection_page_cache.h"
#include "query/runtime/relational.h"

namespace cedar::internal {
namespace {

ProjectionChain Page(char value = 'a') {
  ProjectionChain page;
  page.intervals.push_back({{ValidTime{0}, ValidTime{1}},
                            Value::String(std::string(4096, value)), 7});
  return page;
}

TEST(ProjectionPageCache, DisabledProjectionCacheDoesNotAdmit) {
  ProjectionPageCache cache(0);
  auto page = Page();
  EXPECT_FALSE(cache.Admit("page", &page));
  EXPECT_EQ(page.intervals.size(), 1U);
  EXPECT_EQ(cache.resident_bytes(), 0U);
}

TEST(ProjectionPageCache, ProjectionCacheRespectsByteCapacity) {
  ProjectionPageCache cache(16384);
  QueryReservation query(1 << 20);
  auto first = Page();
  ASSERT_TRUE(cache.Admit("first", &first));
  auto pinned = cache.Lookup("first", query);
  ASSERT_TRUE(pinned.ok());
  ASSERT_TRUE(pinned.ValueOrDie());
  for (int i = 0; i < 20; ++i) {
    auto page = Page('b');
    cache.Admit(std::to_string(i), &page);
    EXPECT_LE(cache.resident_bytes(), 16384U);
  }
  EXPECT_EQ(pinned.ValueOrDie()->intervals[0].value, Value::String(std::string(4096, 'a')));
  EXPECT_GT(query.used_bytes(), 0U);
  pinned = std::shared_ptr<const ProjectionChain>{};
  EXPECT_EQ(query.used_bytes(), 0U);
}

TEST(ProjectionPageCache, PinRejectionAndOwnerLifetime) {
  QueryReservation query(1 << 20);
  std::shared_ptr<const ProjectionChain> owner;
  auto generation = std::make_shared<int>(7);
  std::weak_ptr<int> weak = generation;
  {
    ProjectionPageCache cache(16384);
    auto page = Page();
    ASSERT_TRUE(cache.Admit("db/generation/page", &page));
    QueryReservation tiny(1);
    auto rejected = cache.Lookup("db/generation/page", tiny);
    EXPECT_FALSE(rejected.ok());
    EXPECT_EQ(tiny.used_bytes(), 0U);
    auto found = cache.Lookup("db/generation/page", query, generation);
    ASSERT_TRUE(found.ok());
    owner = found.ConsumeValueOrDie();
    auto missing = cache.Lookup("other-db/generation/page", query);
    ASSERT_TRUE(missing.ok());
    EXPECT_FALSE(missing.ValueOrDie());
  }
  generation.reset();
  EXPECT_FALSE(weak.expired());
  ASSERT_EQ(owner->intervals.size(), 1U);
  owner.reset();
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(query.used_bytes(), 0U);
}

TEST(ProjectionPageCache, SameQueryPinsPayloadOnceAndChargesEachHandle) {
  ProjectionPageCache cache(16384);
  auto page = Page();
  ASSERT_TRUE(cache.Admit("page", &page));
  QueryReservation query(1 << 20);
  auto first = cache.Lookup("page", query);
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(first.ValueOrDie());
  const auto charged = query.used_bytes();
  auto second = cache.Lookup("page", query);
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(second.ValueOrDie());
  EXPECT_GT(query.used_bytes(), charged);
  EXPECT_LT(query.used_bytes() - charged, 512U);
  first = std::shared_ptr<const ProjectionChain>{};
  EXPECT_EQ(query.used_bytes(), charged);
  second = std::shared_ptr<const ProjectionChain>{};
  EXPECT_EQ(query.used_bytes(), 0U);
}

TEST(ProjectionPageCache, IndependentStoresDoNotShareIdenticalPageKeys) {
  ProjectionPageCache first_store(16384);
  ProjectionPageCache second_store(16384);
  auto first = Page('a');
  auto second = Page('b');
  ASSERT_TRUE(first_store.Admit("generation/segment/page", &first));
  ASSERT_TRUE(second_store.Admit("generation/segment/page", &second));
  QueryReservation query(1 << 20);
  {
    auto a = first_store.Lookup("generation/segment/page", query);
    auto b = second_store.Lookup("generation/segment/page", query);
    ASSERT_TRUE(a.ok());
    ASSERT_TRUE(b.ok());
    ASSERT_TRUE(a.ValueOrDie());
    ASSERT_TRUE(b.ValueOrDie());
    EXPECT_EQ(a.ValueOrDie()->intervals[0].value, Value::String(std::string(4096, 'a')));
    EXPECT_EQ(b.ValueOrDie()->intervals[0].value, Value::String(std::string(4096, 'b')));
  }
  EXPECT_EQ(query.used_bytes(), 0U);
}

}  // namespace
}  // namespace cedar::internal
