#include <gtest/gtest.h>
#include <mutex>

#include "query/runtime/fact_chain_cursor.h"
#include "query/runtime/read_context.h"

namespace cedar::internal {
namespace {

FactEvent Event(uint64_t valid, uint64_t commit, FactOperation operation) {
  return FactEvent{FactRef{PartId{1}, FactFamily::kVertexState, PropertyId{}, 7},
                   ValidTime{valid}, CommitSeq{commit}, operation, 0,
                   std::nullopt, std::nullopt};
}

TEST(FactChainCursor, OrderedInputAvoidsSortFallback) {
  FactChainCursor cursor(FactBatchOrder::kIdentityValidDescCommitDesc);
  ASSERT_TRUE(cursor.Consume(Event(10, 1, FactOperation::kPut)).ok());
  ASSERT_TRUE(cursor.Consume(Event(20, 2, FactOperation::kDelete)).ok());
  ASSERT_TRUE(cursor.Finish(CommitSeq{2}).ok());
  ASSERT_EQ(cursor.sort_fallbacks(), 0U);
  ASSERT_EQ(cursor.chains().size(), 1U);
  ASSERT_EQ(cursor.chains().front().boundaries.size(), 2U);
}

TEST(FactChainCursor, UnorderedInputSortsOnceAndMatchesOutput) {
  FactChainCursor cursor(FactBatchOrder::kUnknown);
  ASSERT_TRUE(cursor.Consume(std::vector<FactEvent>{
                               Event(20, 2, FactOperation::kDelete),
                               Event(10, 1, FactOperation::kPut)})
                  .ok());
  ASSERT_TRUE(cursor.Finish(CommitSeq{2}).ok());
  EXPECT_EQ(cursor.sort_fallbacks(), 1U);
  ASSERT_EQ(cursor.chains().size(), 1U);
  ASSERT_EQ(cursor.chains().front().boundaries.size(), 2U);
  EXPECT_EQ(cursor.chains().front().boundaries.front().valid_from, ValidTime{10});
}

TEST(FactChainCursor, QueryCacheStoresReducedChainsNotRawEvents) {
  TemporalChainCache cache;
  auto chains = std::make_shared<const std::vector<FactChainView>>(
      std::vector<FactChainView>{FactChainView{
          Event(10, 1, FactOperation::kPut).ref, {}, {}}});
  {
    std::lock_guard<std::mutex> lock(cache.mutex);
    cache.chains.emplace("key", chains);
  }
  std::lock_guard<std::mutex> lock(cache.mutex);
  ASSERT_EQ(cache.chains.size(), 1U);
  EXPECT_EQ(cache.chains.begin()->second->front().ref.entity_id(), 7U);
}

}  // namespace
}  // namespace cedar::internal
