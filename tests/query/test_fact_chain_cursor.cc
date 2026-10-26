#include <gtest/gtest.h>
#include <mutex>

#include "query/runtime/fact_chain_cursor.h"
#include "query/runtime/read_context.h"
#include "query/resource/query_read_memory.h"

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

TEST(FactChainCursor, OwnedHistoryAndCorrectionWorkspaceRespectBudget) {
  FactEvent event{PropertyFact::Vertex({PartId{1}, VertexId{7}}, PropertyId{2}).ref(),
                  ValidTime{10}, CommitSeq{1}, FactOperation::kPut, 1,
                  Value::String(std::string(4096, 'x'))};
  QueryReservation tiny(1);
  {
    FactChainCursor cursor(FactBatchOrder::kIdentityValidDescCommitDesc,
                           CommitSeq{1}, std::nullopt,
                           std::make_shared<QueryReadMemory>(tiny));
    EXPECT_TRUE(cursor.Consume(event).IsResourceExhausted());
  }
  EXPECT_EQ(tiny.used_bytes(), 0U);
  QueryReservation workspace(12 * 1024);
  {
    FactChainCursor cursor(FactBatchOrder::kIdentityValidDescCommitDesc,
                           CommitSeq{1}, std::nullopt,
                           std::make_shared<QueryReadMemory>(workspace));
    ASSERT_TRUE(cursor.Consume(event).ok());
    EXPECT_GE(workspace.used_bytes(), 4096U);
    EXPECT_TRUE(cursor.Finish(CommitSeq{1}).IsResourceExhausted());
  }
  EXPECT_EQ(workspace.used_bytes(), 0U);
  QueryReservation adequate(1 << 20);
  {
    FactChainCursor cursor(FactBatchOrder::kIdentityValidDescCommitDesc,
                           CommitSeq{1}, std::nullopt,
                           std::make_shared<QueryReadMemory>(adequate));
    ASSERT_TRUE(cursor.Consume(event).ok());
    ASSERT_TRUE(cursor.Finish(CommitSeq{1}).ok());
    ASSERT_EQ(cursor.chains().size(), 1U);
    ASSERT_EQ(cursor.chains().front().present.size(), 1U);
    EXPECT_EQ(cursor.chains().front().present.front().value, event.value);
    EXPECT_GE(adequate.used_bytes(), 8192U);
  }
  EXPECT_EQ(adequate.used_bytes(), 0U);
}

TEST(FactChainCursor, TransferredChainsRetainBudgetAfterCursorDestruction) {
  QueryReservation budget(1 << 20);
  std::shared_ptr<const std::vector<FactChainView>> retained;
  {
    FactChainCursor cursor(FactBatchOrder::kIdentityValidDescCommitDesc,
                           CommitSeq{1}, std::nullopt,
                           std::make_shared<QueryReadMemory>(budget));
    auto event = Event(10, 1, FactOperation::kPut);
    ASSERT_TRUE(cursor.Consume(event).ok());
    ASSERT_TRUE(cursor.Finish(CommitSeq{1}).ok());
    auto transferred = cursor.TakeChains(4096);
    ASSERT_TRUE(transferred.ok());
    retained = transferred.ConsumeValueOrDie();
    EXPECT_TRUE(cursor.chains().empty());
  }
  ASSERT_EQ(retained->size(), 1U);
  EXPECT_EQ(retained->front().ref.entity_id(), 7U);
  EXPECT_GE(budget.used_bytes(), 4096U + sizeof(FactChainView));
  retained.reset();
  EXPECT_EQ(budget.used_bytes(), 0U);
}

}  // namespace
}  // namespace cedar::internal
