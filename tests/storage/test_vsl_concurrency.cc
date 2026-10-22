// Cedar 0.1 并发插入回归测试（Task 0.2 / 审计 1.1、1.9）
//
// 红测试：8 线程各写 10k 个不同 entity（共 80k 行），
//   1. 共享 VSLMemTable 并发 Put 后，Traverse 必须数到 80000 行；
//   2. 走 LsmEngine::Put -> ForceFlush 后，落盘行数必须为 80000。
// 现状失败原因：LockFreeVSL::Insert 用普通 atomic store 覆盖 preds->next，
// 没有 CAS 重试（审计 1.1），并发写会丢节点；VSLMemTable::Put 也不串行化。
//
// T5（Task 2.1-2.4）补充：
//   - 跳表排序必须与完整 key 契约一致（entity → type → column → target →
//     timestamp DESC → sequence → flags），否则版本链断裂（审计 1.3）；
//   - 并发/乱序插入后版本链必须完整且严格按 timestamp 降序（审计 1.2、2.1）；
//   - 节点必须来自 MemTableArena，插入路径 0 次 operator new（Task 2.2）；
//   - VSLMemTable::PutBatch 与逐条 Put 语义一致（Task 2.4）。

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/vsl_memtable.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "sst_test_utils.h"

namespace {

std::atomic<bool> g_count_alloc{false};
std::atomic<uint64_t> g_alloc_count{0};

}  // namespace

// 统计 operator new 次数（Task 2.2 要求插入路径 0 per-node malloc）。
// arena 的 slab 走 malloc，不计入；因此测试窗口内计数必须为 0。
void* operator new(std::size_t size) {
  if (g_count_alloc.load(std::memory_order_relaxed)) {
    g_alloc_count.fetch_add(1, std::memory_order_relaxed);
  }
  void* p = std::malloc(size ? size : 1);
  if (p == nullptr) {
    throw std::bad_alloc();
  }
  return p;
}

void* operator new[](std::size_t size) { return ::operator new(size); }

void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

namespace {

using namespace cedar;

constexpr int kThreads = 8;
constexpr int kKeysPerThread = 10000;
constexpr size_t kExpectedRows =
    static_cast<size_t>(kThreads) * static_cast<size_t>(kKeysPerThread);  // 80000

// 交叉分布 key（i*kThreads+t），确保 8 个线程持续竞争同一段跳表
inline uint64_t EntityIdFor(int thread, int i) {
  return static_cast<uint64_t>(i) * kThreads + static_cast<uint64_t>(thread) + 1;
}

// 排序契约（Task 2.1）：entity 升序 → entity_type 升序 → column 升序 →
// target 升序 → timestamp 降序 → sequence 升序 → flags 升序。
int ExpectedCompare(const CedarKey& a, const CedarKey& b) {
  if (a.entity_id() != b.entity_id()) return a.entity_id() < b.entity_id() ? -1 : 1;
  if (a.entity_type() != b.entity_type()) {
    return static_cast<uint8_t>(a.entity_type()) < static_cast<uint8_t>(b.entity_type()) ? -1 : 1;
  }
  if (a.column_id() != b.column_id()) return a.column_id() < b.column_id() ? -1 : 1;
  if (a.target_id() != b.target_id()) return a.target_id() < b.target_id() ? -1 : 1;
  if (a.timestamp().value() != b.timestamp().value()) {
    return a.timestamp().value() > b.timestamp().value() ? -1 : 1;  // DESC
  }
  if (a.sequence() != b.sequence()) return a.sequence() < b.sequence() ? -1 : 1;
  if (a.flags() != b.flags()) return a.flags() < b.flags() ? -1 : 1;
  return 0;
}

}  // namespace

TEST(VslConcurrencyTest, EightThreadsInsert80k) {
  // 第一段：共享 memtable 并发 Put 后按链表实际遍历计数（不信任原子计数器）
  {
    VSLMemTable mem;
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
      workers.emplace_back([&mem, t]() {
        for (int i = 0; i < kKeysPerThread; ++i) {
          mem.Put(CedarKey::Vertex(EntityIdFor(t, i), 1, Timestamp(1000 + i)),
                  Descriptor::InlineInt(1, i), Timestamp(1));
        }
      });
    }
    for (auto& worker : workers) {
      worker.join();
    }

    size_t traversed = 0;
    mem.Traverse([&traversed](const CedarKey&, const Descriptor&) {
      ++traversed;
      return true;
    });
    EXPECT_EQ(traversed, kExpectedRows)
        << "并发插入丢节点（审计 1.1：Insert 用非 CAS 的 SetNext 覆盖前驱指针）。"
        << " 实际遍历到 " << traversed << " / " << kExpectedRows;
  }

  // 第二段：同样并发写入 LsmEngine，flush 后统计真实落盘行数
  {
    cedar_test::ScopedTempDir temp_dir("vsl_80k");
    ASSERT_TRUE(temp_dir.ok());
    const std::string& dir = temp_dir.path();

    {
      CedarOptions options;
      options.create_if_missing = true;
      options.env = Env::Default();

      LsmEngine engine(dir, options, Env::Default());
      ASSERT_TRUE(engine.Open().ok());

      std::vector<std::thread> workers;
      workers.reserve(kThreads);
      for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&engine, t]() {
          for (int i = 0; i < kKeysPerThread; ++i) {
            engine.Put(CedarKey::Vertex(EntityIdFor(t, i), 1, Timestamp(1000 + i)),
                       Descriptor::InlineInt(1, i), Timestamp(1));
          }
        });
      }
      for (auto& worker : workers) {
        worker.join();
      }

      ASSERT_TRUE(engine.ForceFlush().ok()) << "flush 失败";

      uint64_t flushed_rows = 0;
      for (int level = 0; level < 7; ++level) {
        for (const auto& meta : engine.GetFrondFiles(level)) {
          flushed_rows += meta.num_entries;
        }
      }
      EXPECT_EQ(flushed_rows, static_cast<uint64_t>(kExpectedRows))
          << "flush 后行数不足（审计 1.1/1.9）：实际 " << flushed_rows << " / "
          << kExpectedRows << "，并发写入丢失的节点不会落盘";

      engine.Close();
    }
  }
}

// Task 2.1：跳表排序必须与完整 key 契约一致，且与 traversal 顺序一致。
TEST(VslConcurrencyTest, InsertOrderMatchesKeyContract) {
  constexpr uint64_t kEntity = 100;
  std::vector<CedarKey> keys;
  for (uint64_t ts : {50ULL, 10ULL, 30ULL, 20ULL, 40ULL}) {
    for (uint16_t col : {2, 1}) {
      for (uint64_t target : {7ULL, 3ULL}) {
        keys.push_back(CedarKey::Vertex(kEntity, col, Timestamp(ts), 0, target));
        keys.push_back(CedarKey::EdgeOut(kEntity, target, EdgeTypeId(col), Timestamp(ts), 0));
        keys.push_back(CedarKey::EdgeIn(kEntity, target, EdgeTypeId(col), Timestamp(ts), 0));
      }
    }
  }
  // 同 timestamp、不同 sequence 的版本也必须可区分且按 sequence 升序。
  for (uint16_t seq : {1, 0, 2}) {
    keys.push_back(CedarKey::Vertex(kEntity, 9, Timestamp(10), seq));
  }
  ASSERT_GE(keys.size(), 30u);

  std::vector<CedarKey> insertion = keys;
  std::shuffle(insertion.begin(), insertion.end(), std::mt19937(12345));

  VSLMemTable mem;
  for (const auto& key : insertion) {
    ASSERT_TRUE(mem.Put(key, Descriptor::InlineInt(1, 7), Timestamp(1)).ok());
  }

  std::vector<CedarKey> traversed;
  mem.Traverse([&traversed](const CedarKey& key, const Descriptor&) {
    traversed.push_back(key);
    return true;
  });

  ASSERT_EQ(traversed.size(), keys.size());
  for (size_t i = 1; i < traversed.size(); ++i) {
    EXPECT_LT(ExpectedCompare(traversed[i - 1], traversed[i]), 0)
        << "Traverse 顺序违反 key 契约：index " << i << " (ts="
        << traversed[i - 1].timestamp().value() << " -> " << traversed[i].timestamp().value()
        << ", type=" << static_cast<int>(traversed[i - 1].entity_type()) << "->"
        << static_cast<int>(traversed[i].entity_type()) << ", seq="
        << traversed[i - 1].sequence() << "->" << traversed[i].sequence() << ")";
  }
}

// Task 2.1：8 线程并发插入同一 user key 的 4000 个版本，版本链必须完整、
// 严格按 timestamp 降序，且 GetAtTime 能命中链上任意精确版本（审计 1.2/2.1）。
TEST(VslConcurrencyTest, ConcurrentVersionChainComplete) {
  constexpr uint64_t kEntity = 7;
  constexpr uint16_t kColumn = 1;
  constexpr int kThreadsLocal = 8;
  constexpr int kVersionsPerThread = 500;
  constexpr uint64_t kTotal =
      static_cast<uint64_t>(kThreadsLocal) * static_cast<uint64_t>(kVersionsPerThread);

  VSLMemTable mem;
  std::vector<std::thread> workers;
  workers.reserve(kThreadsLocal);
  for (int t = 0; t < kThreadsLocal; ++t) {
    workers.emplace_back([&mem, t]() {
      for (int i = 0; i < kVersionsPerThread; ++i) {
        const uint64_t ts =
            static_cast<uint64_t>(t) * kVersionsPerThread + static_cast<uint64_t>(i) + 1;
        mem.Put(CedarKey::Vertex(kEntity, kColumn, Timestamp(ts)),
                Descriptor::InlineInt(kColumn, static_cast<int32_t>(ts)), Timestamp(1));
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }

  auto chain = mem.GetVersionChain(kEntity, EntityType::Vertex, kColumn);
  ASSERT_EQ(chain.size(), kTotal) << "版本链丢版本（审计 1.2/2.1）";

  std::vector<uint64_t> got;
  got.reserve(chain.size());
  for (const auto& entry : chain) {
    got.push_back(entry.timestamp.value());
  }
  std::vector<uint64_t> want;
  want.reserve(kTotal);
  for (uint64_t ts = kTotal; ts >= 1; --ts) want.push_back(ts);
  EXPECT_EQ(got, want) << "版本链顺序或完整性错误：必须严格 timestamp 降序且无缺漏";

  // GetAtTime 沿链回溯必须命中精确版本。
  for (uint64_t ts : {1ULL, 499ULL, 500ULL, 1001ULL, 2500ULL, 3999ULL}) {
    auto got_desc = mem.GetAtTime(kEntity, EntityType::Vertex, kColumn, Timestamp(ts));
    ASSERT_TRUE(got_desc.has_value()) << "ts=" << ts << " 未命中";
    auto value = got_desc->AsInlineInt();
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(static_cast<uint64_t>(*value), ts) << "ts=" << ts << " 命中错误版本";
  }
}

// Task 2.1/2.3：单线程乱序插入 1000 个版本，链也必须完整（older 回链修复）。
TEST(VslConcurrencyTest, ShuffledInsertVersionChainComplete) {
  constexpr uint64_t kEntity = 9;
  constexpr uint16_t kColumn = 2;
  constexpr uint64_t kVersions = 1000;

  std::vector<uint64_t> order(kVersions);
  for (uint64_t i = 0; i < kVersions; ++i) order[i] = i + 1;
  std::shuffle(order.begin(), order.end(), std::mt19937(777));

  VSLMemTable mem;
  for (uint64_t ts : order) {
    mem.Put(CedarKey::Vertex(kEntity, kColumn, Timestamp(ts)),
            Descriptor::InlineInt(kColumn, static_cast<int32_t>(ts)), Timestamp(1));
  }

  auto chain = mem.GetVersionChain(kEntity, EntityType::Vertex, kColumn);
  ASSERT_EQ(chain.size(), kVersions);
  for (uint64_t i = 0; i < kVersions; ++i) {
    EXPECT_EQ(chain[i].timestamp.value(), kVersions - i)
        << "版本链第 " << i << " 个版本不是预期的 timestamp=" << (kVersions - i);
  }

  auto range = mem.GetRange(kEntity, EntityType::Vertex, kColumn, Timestamp(200),
                            Timestamp(800));
  ASSERT_EQ(range.size(), 601u);
  EXPECT_EQ(range.front().timestamp.value(), 800u);
  EXPECT_EQ(range.back().timestamp.value(), 200u);
  for (size_t i = 1; i < range.size(); ++i) {
    EXPECT_GT(range[i - 1].timestamp.value(), range[i].timestamp.value());
  }
}

// Task 2.2：插入 10 万行期间不得有 per-node operator new（节点来自 arena slab）。
TEST(VslMemTablePerfTest, ArenaAvoidsPerNodeAllocation) {
  constexpr uint64_t kRows = 100000;
  VSLMemTable mem;

  g_alloc_count.store(0, std::memory_order_relaxed);
  g_count_alloc.store(true, std::memory_order_relaxed);
  for (uint64_t i = 0; i < kRows; ++i) {
    mem.Put(CedarKey::Vertex(i + 1, 1, Timestamp(1000 + i)),
            Descriptor::InlineInt(1, static_cast<int32_t>(i)), Timestamp(1));
  }
  g_count_alloc.store(false, std::memory_order_relaxed);

  EXPECT_EQ(g_alloc_count.load(std::memory_order_relaxed), 0u)
      << "插入路径出现 per-node operator new（Task 2.2 要求 0，节点应由 MemTableArena 分配）";

  size_t traversed = 0;
  mem.Traverse([&traversed](const CedarKey&, const Descriptor&) {
    ++traversed;
    return true;
  });
  EXPECT_EQ(traversed, kRows);
  EXPECT_GT(mem.ApproximateMemoryUsage(), kRows * sizeof(uint64_t));
}
// Task 2.4：PutBatch 与逐条 Put 语义一致（乱序输入、多版本、同 key 冲突）。
TEST(VslMemTableBatchTest, PutBatchMatchesIndividualPuts) {
  constexpr uint64_t kEntities = 50;
  constexpr int kVersionsPerEntity = 100;

  std::vector<VSLMemTable::BatchEntry> entries;
  entries.reserve(kEntities * kVersionsPerEntity);
  for (uint64_t entity = 1; entity <= kEntities; ++entity) {
    for (int v = 0; v < kVersionsPerEntity; ++v) {
      const uint64_t ts = 100 + static_cast<uint64_t>(v);
      entries.emplace_back(CedarKey::Vertex(entity, 1, Timestamp(ts)),
                           Descriptor::InlineInt(1, static_cast<int32_t>(ts)), Timestamp(1));
    }
  }
  // 乱序输入：PutBatch 内部必须自行排序。
  std::shuffle(entries.begin(), entries.end(), std::mt19937(2024));

  VSLMemTable batch;
  ASSERT_TRUE(batch.PutBatch(entries).ok());
  VSLMemTable single;
  for (const auto& entry : entries) {
    ASSERT_TRUE(single.Put(entry.key, entry.descriptor, entry.txn_version).ok());
  }

  ASSERT_EQ(batch.size(), entries.size());
  EXPECT_EQ(batch.size(), single.size());

  for (uint64_t entity = 1; entity <= kEntities; ++entity) {
    const auto batch_chain = batch.GetVersionChain(entity, EntityType::Vertex, 1);
    const auto single_chain = single.GetVersionChain(entity, EntityType::Vertex, 1);
    ASSERT_EQ(batch_chain.size(), single_chain.size())
        << "entity=" << entity << " 版本链长度不一致";
    for (size_t i = 0; i < batch_chain.size(); ++i) {
      EXPECT_EQ(batch_chain[i].timestamp.value(), single_chain[i].timestamp.value());
      EXPECT_EQ(batch_chain[i].descriptor.AsRaw(), single_chain[i].descriptor.AsRaw());
      EXPECT_EQ(batch_chain[i].txn_version.value(), single_chain[i].txn_version.value());
    }
  }

  // 指针 + count 重载与 vector 重载等价。
  VSLMemTable batch_ptr;
  ASSERT_TRUE(batch_ptr.PutBatch(entries.data(), entries.size()).ok());
  EXPECT_EQ(batch_ptr.size(), batch.size());

  // GetAtTime 抽样：两条路径命中同一个版本。
  for (uint64_t entity : {1ULL, 17ULL, 50ULL}) {
    for (uint64_t ts : {100ULL, 137ULL, 199ULL}) {
      auto a = batch.GetAtTime(entity, EntityType::Vertex, 1, Timestamp(ts));
      auto b = single.GetAtTime(entity, EntityType::Vertex, 1, Timestamp(ts));
      ASSERT_EQ(a.has_value(), b.has_value());
      if (a.has_value()) {
        EXPECT_EQ(a->AsRaw(), b->AsRaw());
      }
    }
  }
}
// Task 1.5 新增 API（供 t3 使用）：按 target 的版本链遍历 + 同列所有 target 遍历。
TEST(VslConcurrencyTest, TargetVersionTraversal) {
  constexpr uint64_t kSrc = 1;
  constexpr uint16_t kEdgeType = 2;
  VSLMemTable mem;
  // 3 个 dst × 3 个版本，交叉顺序插入。
  for (uint64_t ts : {20ULL, 10ULL, 30ULL}) {
    for (uint64_t dst : {7ULL, 3ULL, 5ULL}) {
      mem.Put(CedarKey::EdgeOut(kSrc, dst, EdgeTypeId(kEdgeType), Timestamp(ts)),
              Descriptor::InlineInt(kEdgeType, static_cast<int32_t>(dst * 100 + ts)),
              Timestamp(1));
    }
  }

  // 1) 指定 target=3：只返回该 target 的 3 个版本，timestamp 降序。
  std::vector<std::pair<uint64_t, uint64_t>> got;
  mem.skiplist().ForEachVersionForTarget(
      kSrc, EntityType::EdgeOut, kEdgeType, /*target_id=*/3, Timestamp(0), Timestamp::Max(),
      [&got](Timestamp ts, Timestamp, const Descriptor&, uint64_t target_id) {
        got.emplace_back(ts.value(), target_id);
      });
  ASSERT_EQ(got.size(), 3u);
  EXPECT_EQ(got[0], std::make_pair(30ULL, 3ULL));
  EXPECT_EQ(got[1], std::make_pair(20ULL, 3ULL));
  EXPECT_EQ(got[2], std::make_pair(10ULL, 3ULL));

  // 2) 所有 target：按 target 升序分组，组内 timestamp 降序，共 9 条。
  got.clear();
  mem.skiplist().ForEachVersionAllTargets(
      kSrc, EntityType::EdgeOut, kEdgeType, Timestamp(0), Timestamp::Max(),
      [&got](Timestamp ts, Timestamp, const Descriptor&, uint64_t target_id) {
        got.emplace_back(ts.value(), target_id);
      });
  ASSERT_EQ(got.size(), 9u);
  const std::vector<std::pair<uint64_t, uint64_t>> expected = {
      {30, 3}, {20, 3}, {10, 3}, {30, 5}, {20, 5}, {10, 5}, {30, 7}, {20, 7}, {10, 7}};
  EXPECT_EQ(got, expected);

  // 3) 区间过滤：[20, 30] → 每个 target 2 条。
  got.clear();
  mem.skiplist().ForEachVersionAllTargets(
      kSrc, EntityType::EdgeOut, kEdgeType, Timestamp(20), Timestamp(30),
      [&got](Timestamp ts, Timestamp, const Descriptor&, uint64_t target_id) {
        got.emplace_back(ts.value(), target_id);
      });
  ASSERT_EQ(got.size(), 6u);
  for (const auto& [ts, target] : got) {
    EXPECT_GE(ts, 20u);
    EXPECT_LE(ts, 30u);
  }
}
