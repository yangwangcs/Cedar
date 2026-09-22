// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <new>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "memory/arena.h"
#include "memory/concurrent_arena.h"
#include "memtable/cedar_pure_radix_index.h"
#include "rocksdb/memtablerep.h"
#include "util/coding.h"

namespace {

std::atomic<size_t> g_heap_allocations{0};

}  // namespace

void* operator new(std::size_t size) {
  g_heap_allocations.fetch_add(1, std::memory_order_relaxed);
  if (void* result = std::malloc(size == 0 ? 1 : size)) return result;
  throw std::bad_alloc();
}

void operator delete(void* pointer) noexcept { std::free(pointer); }

void operator delete(void* pointer, std::size_t) noexcept { std::free(pointer); }

namespace ROCKSDB_NAMESPACE {
namespace {

void StoreBigEndian64(std::string* destination, size_t offset, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    (*destination)[offset++] = static_cast<char>(value >> shift);
  }
}

std::string V2UserKey(uint64_t entity_id) {
  std::string key(32, '\0');
  key[0] = 2;
  key[5] = 1;
  StoreBigEndian64(&key, 8, entity_id);
  return key;
}

std::string InternalKeyFor(uint64_t entity_id, SequenceNumber sequence,
                           ValueType type) {
  std::string internal_key;
  AppendInternalKey(&internal_key,
                    ParsedInternalKey(V2UserKey(entity_id), sequence, type));
  return internal_key;
}

class TestKeyComparator final : public MemTableRep::KeyComparator {
 public:
  TestKeyComparator() : comparator_(BytewiseComparator()) {}

  int operator()(const char* left, const char* right) const override {
    return comparator_.Compare(decode_key(left), decode_key(right));
  }

  int operator()(const char* left, const Slice& right) const override {
    return comparator_.Compare(decode_key(left), right);
  }

 private:
  InternalKeyComparator comparator_;
};

bool Insert(MemTableRep* table, const std::string& internal_key,
            const std::string& value) {
  std::string entry;
  PutVarint32(&entry, static_cast<uint32_t>(internal_key.size()));
  entry.append(internal_key);
  entry.append(value);
  char* storage = nullptr;
  const KeyHandle handle = table->Allocate(entry.size(), &storage);
  std::memcpy(storage, entry.data(), entry.size());
  return table->InsertKey(handle);
}

bool InsertConcurrently(MemTableRep* table, const std::string& internal_key,
                        const std::string& value) {
  std::string entry;
  PutVarint32(&entry, static_cast<uint32_t>(internal_key.size()));
  entry.append(internal_key);
  entry.append(value);
  char* storage = nullptr;
  const KeyHandle handle = table->Allocate(entry.size(), &storage);
  std::memcpy(storage, entry.data(), entry.size());
  return table->InsertKeyConcurrently(handle);
}

std::vector<std::string> Collect(MemTableRep* table) {
  std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
  std::vector<std::string> keys;
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    keys.emplace_back(GetLengthPrefixedSlice(iterator->key()).ToString());
  }
  return keys;
}

uint64_t OrderedKeyHash(const std::vector<std::string>& keys) {
  // Keep a stable result digest so the concurrent differential checks do not
  // depend on the implementation-defined std::hash specialization.
  uint64_t hash = 1469598103934665603ULL;
  for (const std::string& key : keys) {
    for (unsigned char byte : key) {
      hash ^= byte;
      hash *= 1099511628211ULL;
    }
    hash ^= 0xffU;
    hash *= 1099511628211ULL;
  }
  return hash;
}

struct GetCandidates {
  Slice user_key;
  std::vector<std::string> entries;
};

bool CollectMatchingUserKey(void* argument, const char* entry) {
  auto* candidates = static_cast<GetCandidates*>(argument);
  const Slice internal_key = GetLengthPrefixedSlice(entry);
  if (ExtractUserKey(internal_key) != candidates->user_key) return false;
  candidates->entries.push_back(internal_key.ToString());
  return true;
}

std::vector<std::string> CollectGetCandidates(MemTableRep* table,
                                              const std::string& user_key,
                                              SequenceNumber snapshot) {
  LookupKey lookup(user_key, snapshot);
  GetCandidates candidates{lookup.user_key(), {}};
  table->Get(lookup, &candidates, CollectMatchingUserKey);
  return candidates.entries;
}

bool InternalKeyLess(const std::string& left, const std::string& right) {
  static const InternalKeyComparator comparator(BytewiseComparator());
  return comparator.Compare(left, right) < 0;
}

std::vector<std::string> CollectReverse(MemTableRep* table) {
  std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
  std::vector<std::string> keys;
  for (iterator->SeekToLast(); iterator->Valid(); iterator->Prev()) {
    keys.emplace_back(GetLengthPrefixedSlice(iterator->key()).ToString());
  }
  return keys;
}

std::vector<std::string> VisibleCandidatesFromModel(
    const std::vector<std::string>& keys, const std::string& user_key,
    SequenceNumber snapshot) {
  std::vector<std::string> visible;
  for (const std::string& key : keys) {
    ParsedInternalKey parsed;
    EXPECT_TRUE(ParseInternalKey(key, &parsed, false).ok());
    if (parsed.user_key == user_key && parsed.sequence <= snapshot) {
      visible.push_back(key);
    }
  }
  std::sort(visible.begin(), visible.end(), InternalKeyLess);
  return visible;
}

struct LookupRequest {
  std::string user_key;
  SequenceNumber snapshot;
};

struct MultiGetCandidateResults {
  Status status;
  std::vector<std::vector<std::string>> entries;
};

std::vector<size_t> SortedLookupOrder(
    const std::vector<LookupRequest>& requests) {
  std::vector<std::unique_ptr<LookupKey>> lookups;
  lookups.reserve(requests.size());
  for (const LookupRequest& request : requests) {
    lookups.push_back(
        std::make_unique<LookupKey>(request.user_key, request.snapshot));
  }
  std::vector<size_t> order(requests.size());
  std::iota(order.begin(), order.end(), 0);
  TestKeyComparator comparator;
  std::stable_sort(order.begin(), order.end(), [&](size_t left, size_t right) {
    return comparator(lookups[left]->memtable_key().data(),
                      lookups[right]->memtable_key().data()) < 0;
  });
  return order;
}

MultiGetCandidateResults CollectMultiGetInOrder(
    MemTableRep* table, const std::vector<LookupRequest>& requests,
    const std::vector<size_t>& order) {
  std::vector<std::unique_ptr<LookupKey>> lookups;
  lookups.reserve(requests.size());
  for (const LookupRequest& request : requests) {
    lookups.push_back(
        std::make_unique<LookupKey>(request.user_key, request.snapshot));
  }
  std::vector<GetCandidates> candidates;
  candidates.reserve(requests.size());
  for (const std::unique_ptr<LookupKey>& lookup : lookups) {
    candidates.push_back(GetCandidates{lookup->user_key(), {}});
  }
  std::vector<const char*> keys;
  std::vector<void*> callback_args;
  keys.reserve(order.size());
  callback_args.reserve(order.size());
  for (size_t index : order) {
    keys.push_back(lookups[index]->memtable_key().data());
    callback_args.push_back(&candidates[index]);
  }
  const auto accept = [](const char*, bool) { return Status::OK(); };
  Status status = table->MultiGet(keys.size(), keys.data(), callback_args.data(),
                                  CollectMatchingUserKey, false, true, accept);
  MultiGetCandidateResults result{std::move(status), {}};
  result.entries.reserve(candidates.size());
  for (GetCandidates& candidate : candidates) {
    result.entries.push_back(std::move(candidate.entries));
  }
  return result;
}

MultiGetCandidateResults CollectMultiGetInCallerOrder(
    MemTableRep* table, const std::vector<LookupRequest>& requests) {
  return CollectMultiGetInOrder(table, requests, SortedLookupOrder(requests));
}

TEST(PartitionedVersionRadixMemTableTest, MatchesInternalOrderingForVersionsAndDeletes) {
  TestKeyComparator comparator;
  Arena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));
  const std::vector<std::string> inserted = {
      InternalKeyFor(7, 1, kTypeValue),
      InternalKeyFor(3, 9, kTypeDeletion),
      InternalKeyFor(3, 11, kTypeValue),
      InternalKeyFor(3, 10, kTypeValue),
      InternalKeyFor(9, 5, kTypeDeletion),
  };
  for (const std::string& key : inserted) EXPECT_TRUE(Insert(table.get(), key, "value"));
  EXPECT_FALSE(Insert(table.get(), inserted[0], "duplicate"));

  std::vector<std::string> expected = inserted;
  std::sort(expected.begin(), expected.end(), [&comparator](const std::string& left,
                                                             const std::string& right) {
    std::string left_entry;
    std::string right_entry;
    PutVarint32(&left_entry, static_cast<uint32_t>(left.size()));
    PutVarint32(&right_entry, static_cast<uint32_t>(right.size()));
    left_entry.append(left);
    right_entry.append(right);
    return comparator(left_entry.data(), right_entry.data()) < 0;
  });
  EXPECT_EQ(Collect(table.get()), expected);
  // Node and entry storage are allocated by the MemTable allocator, which is
  // excluded from MemTableRep::ApproximateMemoryUsage().
  EXPECT_EQ(table->ApproximateMemoryUsage(), 0U);
}

TEST(PartitionedVersionRadixMemTableTest,
     BoundaryCandidateObserverCountsInsertionWork) {
  TestKeyComparator comparator;
  Arena arena;
  std::atomic<size_t> boundary_candidates{0};
  PartitionedVersionRadixFactory::Options options;
  options.boundary_candidate_observer_for_testing = [&] {
    boundary_candidates.fetch_add(1, std::memory_order_relaxed);
  };
  PartitionedVersionRadixFactory factory(options);
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  for (uint64_t entity_id = 1; entity_id <= 256; ++entity_id) {
    ASSERT_TRUE(Insert(table.get(), InternalKeyFor(entity_id, 1, kTypeValue),
                       "value"));
  }

  EXPECT_GT(boundary_candidates.load(std::memory_order_relaxed), 0U);
}

TEST(PartitionedVersionRadixMemTableTest,
     StandaloneIndexKeepsEntryPointerAcrossInsert) {
  Arena arena;
  CedarPureRadixIndex index(&arena);
  const CedarPureRadixIndex::Key key = [] {
    CedarPureRadixIndex::Key value{};
    value[0] = 0x21;
    value[39] = 0x0f;
    return value;
  }();
  char* entry = nullptr;
  void* handle = index.Allocate(3, &entry);
  ASSERT_NE(handle, nullptr);
  ASSERT_NE(entry, nullptr);
  entry[0] = 'c';
  entry[1] = 'e';
  entry[2] = 'd';

  ASSERT_TRUE(index.Insert(handle, key));
  EXPECT_EQ(index.EntryForHandle(handle), entry);
  CedarPureRadixIndex::Cursor cursor(&index);
  cursor.SeekToFirst();
  ASSERT_TRUE(cursor.Valid());
  EXPECT_EQ(cursor.entry(), entry);
  EXPECT_EQ(std::string(cursor.entry(), 3), "ced");
}

TEST(PartitionedVersionRadixMemTableTest,
     BoundaryPublicationChecksBothCachesForSparsePathAncestors) {
  Arena arena;
  std::atomic<size_t> boundary_candidates{0};
  CedarPureRadixIndex::TestHooks hooks;
  hooks.boundary_candidate_for_testing = [&] {
    boundary_candidates.fetch_add(1, std::memory_order_relaxed);
  };
  CedarPureRadixIndex index(&arena, hooks);

  const auto insert = [&](const CedarPureRadixIndex::Key& key) {
    char* entry = nullptr;
    void* handle = index.Allocate(1, &entry);
    ASSERT_NE(handle, nullptr);
    ASSERT_NE(entry, nullptr);
    *entry = 'v';
    ASSERT_TRUE(index.Insert(handle, key));
  };

  CedarPureRadixIndex::Key zero{};
  insert(zero);
  for (size_t bit = 1; bit < 16; ++bit) {
    CedarPureRadixIndex::Key key{};
    key[bit / 8] = static_cast<unsigned char>(
        1U << (7U - (bit % 8)));
    boundary_candidates.store(0, std::memory_order_relaxed);
    insert(key);
    const size_t candidates =
        boundary_candidates.load(std::memory_order_relaxed);
    // Sparse child tables no longer have a binary all-zero/all-one direction
    // rule. Publication visits both boundary caches on every retained frame.
    EXPECT_EQ(candidates % 2, 0U) << "first differing bit " << bit;
    if (bit > 1) EXPECT_GE(candidates, 2U);
  }
}

TEST(PartitionedVersionRadixMemTableTest,
     FirstDifferingBitPositionsRemainVisibleAcrossFullKeyWidth) {
  ConcurrentArena arena;
  CedarPureRadixIndex index(&arena);
  CedarPureRadixIndex::Key base{};
  std::vector<CedarPureRadixIndex::Key> probes;
  probes.reserve(CedarPureRadixIndex::kMaxBits);

  auto insert_key = [&](const CedarPureRadixIndex::Key& key, char value) {
    char* storage = nullptr;
    void* handle = index.Allocate(1, &storage);
    ASSERT_NE(handle, nullptr);
    ASSERT_NE(storage, nullptr);
    *storage = value;
    ASSERT_TRUE(index.Insert(handle, key));
  };

  insert_key(base, 'b');
  // This exercises every possible first-differing bit. It includes all bits
  // in the version-2 key prefix and the inverted RocksDB tag suffix.
  for (size_t bit = 0; bit < CedarPureRadixIndex::kMaxBits; ++bit) {
    CedarPureRadixIndex::Key probe = base;
    probe[bit / 8] = static_cast<unsigned char>(
        probe[bit / 8] | (1U << (7U - (bit % 8))));
    probes.push_back(probe);
    insert_key(probe, 'p');
  }

  EXPECT_TRUE(index.Contains(base));
  EXPECT_FALSE(index.Insert(nullptr, base));
  for (const auto& probe : probes) EXPECT_TRUE(index.Contains(probe));
  CedarPureRadixIndex::Key absent{};
  absent.fill(0xff);
  EXPECT_FALSE(index.Contains(absent));
}

TEST(PartitionedVersionRadixMemTableTest,
     FinalBytePrefixValuesRemainOrdered) {
  ConcurrentArena arena;
  CedarPureRadixIndex index(&arena);
  std::array<CedarPureRadixIndex::Key, 16> keys{};
  std::array<void*, 16> handles{};

  for (uint8_t value = 0; value != keys.size(); ++value) {
    keys[value][39] = value;
    char* entry = nullptr;
    handles[value] = index.Allocate(1, &entry);
    ASSERT_NE(handles[value], nullptr);
    ASSERT_NE(entry, nullptr);
    *entry = static_cast<char>(value);
  }

  for (size_t position : {15U, 0U, 9U, 3U, 12U, 6U, 1U, 14U,
                          7U, 4U, 10U, 2U, 13U, 5U, 11U, 8U}) {
    ASSERT_TRUE(index.Insert(handles[position], keys[position]));
  }

  for (const CedarPureRadixIndex::Key& key : keys) EXPECT_TRUE(index.Contains(key));

  CedarPureRadixIndex::Cursor cursor(&index);
  cursor.SeekToFirst();
  for (uint8_t value = 0; value != keys.size(); ++value) {
    ASSERT_TRUE(cursor.Valid());
    EXPECT_EQ(static_cast<unsigned char>(*cursor.entry()), value);
    cursor.Next();
  }
  EXPECT_FALSE(cursor.Valid());

  const auto stats = index.GetStructureStatsForTesting();
  EXPECT_EQ(stats.branches, 1U);
  EXPECT_EQ(stats.child_tables, 2U);
  EXPECT_EQ(stats.child_blocks, 2U);
  EXPECT_EQ(stats.max_depth, 1U);
}

TEST(PartitionedVersionRadixMemTableTest,
     BytePatriciaFinalByteValuesUseThirtyTwoEightValueSegments) {
  ConcurrentArena arena;
  CedarPureRadixIndex index(&arena);
  std::array<CedarPureRadixIndex::Key, 256> keys{};
  std::array<void*, 256> handles{};

  for (size_t value = 0; value < keys.size(); ++value) {
    keys[value][39] = static_cast<unsigned char>(value);
    char* entry = nullptr;
    handles[value] = index.Allocate(1, &entry);
    ASSERT_NE(handles[value], nullptr);
    ASSERT_NE(entry, nullptr);
    *entry = static_cast<char>(value);
  }
  for (size_t index_in_order = 0; index_in_order < keys.size(); ++index_in_order) {
    const size_t value = (index_in_order * 73 + 19) & 0xff;
    ASSERT_TRUE(index.Insert(handles[value], keys[value]));
  }

  for (const auto& key : keys) EXPECT_TRUE(index.Contains(key));
  EXPECT_EQ(CedarPureRadixIndex::kByteSegmentCount, 32U);
  for (uint16_t value = 0; value < 256; ++value) {
    EXPECT_EQ(CedarPureRadixIndex::SegmentForByte(
                  static_cast<uint8_t>(value)),
              value >> 3);
  }

  CedarPureRadixIndex::Cursor cursor(&index);
  cursor.SeekToFirst();
  for (uint16_t value = 0; value < 256; ++value) {
    ASSERT_TRUE(cursor.Valid());
    EXPECT_EQ(static_cast<unsigned char>(*cursor.entry()), value);
    cursor.Next();
  }
  EXPECT_FALSE(cursor.Valid());

  const auto stats = index.GetStructureStatsForTesting();
  EXPECT_EQ(stats.byte_branches, 1U);
  EXPECT_EQ(stats.child_blocks, 32U);
  for (uint8_t segment = 0;
       segment < CedarPureRadixIndex::kByteSegmentCount; ++segment) {
    EXPECT_EQ(stats.byte_segment_occupied[segment], 0xffU);
    EXPECT_EQ(stats.byte_segment_child_counts[segment], 8U);
  }

  for (uint16_t boundary = 8; boundary < 256; boundary += 8) {
    CedarPureRadixIndex::Key below{};
    below[39] = static_cast<uint8_t>(boundary - 1);
    cursor.Seek(below);
    ASSERT_TRUE(cursor.Valid());
    EXPECT_EQ(static_cast<unsigned char>(*cursor.entry()), boundary - 1);
    cursor.Next();
    ASSERT_TRUE(cursor.Valid());
    EXPECT_EQ(static_cast<unsigned char>(*cursor.entry()), boundary);

    CedarPureRadixIndex::Key above{};
    above[39] = static_cast<uint8_t>(boundary);
    cursor.SeekForPrev(above);
    ASSERT_TRUE(cursor.Valid());
    EXPECT_EQ(static_cast<unsigned char>(*cursor.entry()), boundary);
    cursor.Prev();
    ASSERT_TRUE(cursor.Valid());
    EXPECT_EQ(static_cast<unsigned char>(*cursor.entry()), boundary - 1);
  }

  index.MarkReadOnly();
  index.PrepareForFlush();
  for (size_t scan = 0; scan < 2; ++scan) {
    cursor.SeekToFirst();
    for (uint16_t value = 0; value < 256; ++value) {
      ASSERT_TRUE(cursor.Valid());
      EXPECT_EQ(static_cast<unsigned char>(*cursor.entry()), value);
      cursor.Next();
    }
    EXPECT_FALSE(cursor.Valid());
  }
}

TEST(PartitionedVersionRadixMemTableTest,
     SegmentedByteBlocksPackLocalRanksInGlobalOrder) {
  ConcurrentArena arena;
  CedarPureRadixIndex index(&arena);
  std::array<CedarPureRadixIndex::Key, 16> keys{};
  std::array<void*, 16> handles{};

  for (uint8_t value = 0; value != keys.size(); ++value) {
    keys[value][0] = static_cast<unsigned char>(value << 4);
    char* entry = nullptr;
    handles[value] = index.Allocate(1, &entry);
    ASSERT_NE(handles[value], nullptr);
    ASSERT_NE(entry, nullptr);
    *entry = static_cast<char>(value);
  }
  for (size_t position : {13U, 0U, 7U, 3U, 15U, 4U, 10U, 1U,
                          8U, 14U, 6U, 11U, 2U, 12U, 5U, 9U}) {
    ASSERT_TRUE(index.Insert(handles[position], keys[position]));
  }

  const auto stats = index.GetStructureStatsForTesting();
  ASSERT_EQ(stats.child_blocks, 16U);
  for (uint8_t segment = 0;
       segment < CedarPureRadixIndex::kByteSegmentCount; ++segment) {
    const bool occupied = (segment & 1U) == 0;
    EXPECT_EQ(stats.byte_segment_occupied[segment], occupied ? 1U : 0U);
    EXPECT_EQ(stats.byte_segment_child_counts[segment], occupied ? 1U : 0U);
  }
}

TEST(PartitionedVersionRadixMemTableTest,
     SegmentedNibbleSeekCrossesEveryBlockBoundary) {
  ConcurrentArena arena;
  CedarPureRadixIndex index(&arena);
  std::array<CedarPureRadixIndex::Key, 16> keys{};
  std::array<void*, 16> handles{};
  for (uint8_t nibble = 0; nibble != keys.size(); ++nibble) {
    keys[nibble][0] = static_cast<unsigned char>(nibble << 4);
    char* entry = nullptr;
    handles[nibble] = index.Allocate(1, &entry);
    ASSERT_NE(handles[nibble], nullptr);
    ASSERT_NE(entry, nullptr);
    *entry = static_cast<char>(nibble);
  }
  for (size_t position : {12U, 3U, 8U, 7U, 4U, 11U, 0U, 15U,
                          1U, 14U, 5U, 10U, 2U, 13U, 6U, 9U}) {
    ASSERT_TRUE(index.Insert(handles[position], keys[position]));
  }

  CedarPureRadixIndex::Cursor cursor(&index);
  for (uint8_t boundary : {4U, 8U, 12U}) {
    CedarPureRadixIndex::Key lower{};
    lower[0] = static_cast<unsigned char>((boundary - 1) << 4 | 0x0f);
    cursor.Seek(lower);
    ASSERT_TRUE(cursor.Valid());
    EXPECT_EQ(static_cast<unsigned char>(*cursor.entry()), boundary);

    CedarPureRadixIndex::Key upper{};
    upper[0] = static_cast<unsigned char>(boundary << 4);
    cursor.SeekForPrev(upper);
    ASSERT_TRUE(cursor.Valid());
    EXPECT_EQ(static_cast<unsigned char>(*cursor.entry()), boundary);

    upper[0] = static_cast<unsigned char>(boundary << 4 | 0x0f);
    cursor.SeekForPrev(upper);
    ASSERT_TRUE(cursor.Valid());
    EXPECT_EQ(static_cast<unsigned char>(*cursor.entry()), boundary);
  }
}

TEST(PartitionedVersionRadixMemTableTest,
     SharesIdentityPrefixMemoryAcrossVersionHeavyChains) {
  TestKeyComparator comparator;
  Arena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  for (SequenceNumber sequence = 1; sequence <= 64; ++sequence) {
    EXPECT_TRUE(Insert(table.get(), InternalKeyFor(42, sequence, kTypeValue),
                       "value"));
  }

  EXPECT_EQ(Collect(table.get()).size(), 64U);
  EXPECT_LT(table->ApproximateMemoryUsage(), 64U * 1024U);
}

TEST(PartitionedVersionRadixMemTableTest,
     CanonicalEntriesBackRadixLeafKeysWithoutDuplicateKeyStorage) {
  TestKeyComparator comparator;
  ConcurrentArena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  constexpr size_t kEntries = 1024;
  for (size_t entity = 1; entity <= kEntries; ++entity) {
    EXPECT_TRUE(Insert(table.get(), InternalKeyFor(entity, entity, kTypeValue),
                       ""));
  }

  EXPECT_EQ(Collect(table.get()).size(), kEntries);
  // Immutable 64-value block snapshots are arena-owned through MemTable
  // destruction. This leaves room below 448 bytes/entry without reserving a
  // second normalized-key copy per entry.
  EXPECT_LT(arena.ApproximateMemoryUsage(), kEntries * 448U);
}

TEST(PartitionedVersionRadixMemTableTest,
     CanonicalEntriesDoNotReserveTailPaddingForSubmissionState) {
  TestKeyComparator comparator;
  ConcurrentArena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  constexpr size_t kEntries = 1024;
  for (size_t entity = 1; entity <= kEntries; ++entity) {
    EXPECT_TRUE(Insert(table.get(), InternalKeyFor(entity, entity, kTypeValue),
                       ""));
  }

  EXPECT_EQ(Collect(table.get()).size(), kEntries);
  // A private branch in every handle would add at least 48 KiB here and
  // exceed this byte-block representation-specific bound.
  EXPECT_LT(arena.ApproximateMemoryUsage(), 448U * 1024U);
}

TEST(PartitionedVersionRadixMemTableTest,
     SeeksFromTheFactFamilyPrefix) {
  TestKeyComparator comparator;
  Arena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));
  const std::string first = InternalKeyFor(7, 3, kTypeValue);
  const std::string second = InternalKeyFor(8, 2, kTypeValue);
  ASSERT_TRUE(Insert(table.get(), first, "one"));
  ASSERT_TRUE(Insert(table.get(), second, "two"));

  std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
  std::string prefix_seek;
  AppendInternalKey(&prefix_seek,
                    ParsedInternalKey(Slice(first.data(), 8),
                                      kMaxSequenceNumber, kValueTypeForSeek));
  iterator->Seek(prefix_seek, nullptr);
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(GetLengthPrefixedSlice(iterator->key()), Slice(first));
}

TEST(PartitionedVersionRadixMemTableTest,
     IteratorAllocatedInArenaDoesNotMaterializeAllEntriesOnTheHeap) {
  TestKeyComparator comparator;
  Arena table_arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &table_arena, nullptr, nullptr));
  for (uint64_t entity_id = 1; entity_id <= 256; ++entity_id) {
    ASSERT_TRUE(Insert(table.get(), InternalKeyFor(entity_id, 1, kTypeValue),
                       "value"));
  }

  Arena iterator_arena;
  // Fill the 2 KiB inline block and then make Arena install a normal 4 KiB
  // active block. A large pre-allocation would be an irregular block and is
  // deliberately not reused by Arena's next allocation.
  iterator_arena.AllocateAligned(Arena::kInlineSize);
  iterator_arena.AllocateAligned(1);
  const size_t allocations_before =
      g_heap_allocations.load(std::memory_order_relaxed);
  MemTableRep::Iterator* iterator = table->GetIterator(&iterator_arena);
  const size_t allocations_after_construction =
      g_heap_allocations.load(std::memory_order_relaxed);
  size_t entries_seen = 0;
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    ++entries_seen;
  }
  const size_t allocations_after_scan =
      g_heap_allocations.load(std::memory_order_relaxed);
  iterator->~Iterator();

  EXPECT_EQ(entries_seen, 256U);
  // The fixed cursor fits in the prepared caller-owned arena block. The scan
  // must not materialize facts or allocate once per fact.
  EXPECT_EQ(allocations_after_construction, allocations_before);
  EXPECT_EQ(allocations_after_scan, allocations_after_construction);
}

TEST(PartitionedVersionRadixMemTableTest,
     DirectIteratorSupportsBidirectionalAndBoundedTraversal) {
  TestKeyComparator comparator;
  Arena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));
  ASSERT_TRUE(Insert(table.get(), InternalKeyFor(1, 1, kTypeValue), "one"));
  ASSERT_TRUE(Insert(table.get(), InternalKeyFor(3, 2, kTypeValue), "three-2"));
  ASSERT_TRUE(Insert(table.get(), InternalKeyFor(3, 1, kTypeValue), "three-1"));
  ASSERT_TRUE(Insert(table.get(), InternalKeyFor(5, 1, kTypeValue), "five"));
  const std::vector<std::string> expected = Collect(table.get());

  std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
  iterator->SeekToLast();
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(GetLengthPrefixedSlice(iterator->key()), Slice(expected.back()));
  iterator->Prev();
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(GetLengthPrefixedSlice(iterator->key()),
            Slice(expected[expected.size() - 2]));

  iterator->Seek(InternalKeyFor(4, 1, kTypeValue), nullptr);
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(GetLengthPrefixedSlice(iterator->key()), Slice(expected.back()));
  iterator->SeekForPrev(InternalKeyFor(4, 1, kTypeValue), nullptr);
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(GetLengthPrefixedSlice(iterator->key()),
            Slice(expected[expected.size() - 2]));
  iterator->SeekToFirst();
  iterator->Prev();
  EXPECT_FALSE(iterator->Valid());
}

TEST(PartitionedVersionRadixMemTableTest,
     DeepPathSeekAndSeekForPrevFindAdjacentBounds) {
  TestKeyComparator comparator;
  Arena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  std::vector<std::string> expected;
  for (uint64_t bit = 1; bit < 64; ++bit) {
    const std::string key = InternalKeyFor((uint64_t{1} << bit) | 1U, 1,
                                           kTypeValue);
    ASSERT_TRUE(Insert(table.get(), key, "value"));
    expected.push_back(key);
  }
  std::sort(expected.begin(), expected.end(), InternalKeyLess);

  std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
  for (size_t index = 1; index < expected.size(); ++index) {
    const std::string& target = expected[index];
    iterator->Seek(target, nullptr);
    ASSERT_TRUE(iterator->Valid());
    EXPECT_EQ(GetLengthPrefixedSlice(iterator->key()), Slice(target));
    iterator->SeekForPrev(target, nullptr);
    ASSERT_TRUE(iterator->Valid());
    EXPECT_EQ(GetLengthPrefixedSlice(iterator->key()), Slice(target));

    // The generated keys are entity IDs 2^bit + 1.  Use an entity ID
    // midpoint so the probe remains a valid canonical/internal key rather
    // than mutating the sequence/type tag at the end of the key.
    const uint64_t lower_entity = (uint64_t{1} << index) | 1U;
    const uint64_t upper_entity = (uint64_t{1} << (index + 1)) | 1U;
    const uint64_t midpoint = lower_entity + (upper_entity - lower_entity) / 2;
    const std::string between = InternalKeyFor(midpoint, 1, kTypeValue);
    iterator->Seek(between, nullptr);
    ASSERT_TRUE(iterator->Valid());
    EXPECT_EQ(GetLengthPrefixedSlice(iterator->key()), Slice(target));
    iterator->SeekForPrev(between, nullptr);
    ASSERT_TRUE(iterator->Valid());
    EXPECT_EQ(GetLengthPrefixedSlice(iterator->key()), Slice(expected[index - 1]));
  }
}

TEST(PartitionedVersionRadixMemTableTest,
     SparseNibbleSeekChoosesAdjacentOccupiedChildren) {
  ConcurrentArena arena;
  CedarPureRadixIndex index(&arena);

  const auto insert = [&](uint8_t nibble) {
    CedarPureRadixIndex::Key key{};
    key[39] = nibble;
    char* entry = nullptr;
    void* handle = index.Allocate(1, &entry);
    ASSERT_NE(handle, nullptr);
    ASSERT_NE(entry, nullptr);
    *entry = static_cast<char>(nibble);
    ASSERT_TRUE(index.Insert(handle, key));
  };
  for (uint8_t nibble : {uint8_t{2}, uint8_t{7}, uint8_t{13}}) insert(nibble);

  const auto seek = [&](uint8_t probe, bool previous,
                        std::optional<uint8_t> expected) {
    CedarPureRadixIndex::Key key{};
    key[39] = probe;
    CedarPureRadixIndex::Cursor cursor(&index);
    if (previous) cursor.SeekForPrev(key);
    else cursor.Seek(key);
    ASSERT_EQ(cursor.Valid(), expected.has_value());
    if (expected) EXPECT_EQ(static_cast<uint8_t>(*cursor.entry()), *expected);
  };

  seek(0, false, 2);
  seek(3, false, 7);
  seek(8, false, 13);
  seek(14, false, std::nullopt);
  seek(0, true, std::nullopt);
  seek(3, true, 2);
  seek(8, true, 7);
  seek(14, true, 13);

  ConcurrentArena boundary_arena;
  CedarPureRadixIndex boundary_index(&boundary_arena);
  const auto insert_boundary = [&](uint8_t nibble) {
    CedarPureRadixIndex::Key key{};
    key[39] = nibble;
    char* entry = nullptr;
    void* handle = boundary_index.Allocate(1, &entry);
    ASSERT_NE(handle, nullptr);
    ASSERT_NE(entry, nullptr);
    *entry = static_cast<char>(nibble);
    ASSERT_TRUE(boundary_index.Insert(handle, key));
  };
  insert_boundary(0);
  insert_boundary(15);
  CedarPureRadixIndex::Cursor boundary_cursor(&boundary_index);
  CedarPureRadixIndex::Key zero{};
  boundary_cursor.Seek(zero);
  ASSERT_TRUE(boundary_cursor.Valid());
  EXPECT_EQ(static_cast<uint8_t>(*boundary_cursor.entry()), 0U);
  CedarPureRadixIndex::Key fifteen{};
  fifteen[39] = 15;
  boundary_cursor.SeekForPrev(fifteen);
  ASSERT_TRUE(boundary_cursor.Valid());
  EXPECT_EQ(static_cast<uint8_t>(*boundary_cursor.entry()), 15U);
}

TEST(PartitionedVersionRadixMemTableTest,
     SparseByteSegmentsSeekAcrossEvery64ValueBoundary) {
  ConcurrentArena arena;
  CedarPureRadixIndex index(&arena);
  const auto insert = [&](uint8_t value) {
    CedarPureRadixIndex::Key key{};
    key[39] = value;
    char* entry = nullptr;
    void* handle = index.Allocate(1, &entry);
    ASSERT_NE(handle, nullptr);
    ASSERT_NE(entry, nullptr);
    *entry = static_cast<char>(value);
    ASSERT_TRUE(index.Insert(handle, key));
  };
  for (uint8_t value : {uint8_t{0}, uint8_t{63}, uint8_t{64},
                        uint8_t{127}, uint8_t{128}, uint8_t{191},
                        uint8_t{192}, uint8_t{255}}) {
    insert(value);
  }

  const auto seek = [&](uint8_t probe, bool previous, uint8_t expected) {
    CedarPureRadixIndex::Key key{};
    key[39] = probe;
    CedarPureRadixIndex::Cursor cursor(&index);
    if (previous) cursor.SeekForPrev(key);
    else cursor.Seek(key);
    ASSERT_TRUE(cursor.Valid());
    EXPECT_EQ(static_cast<uint8_t>(*cursor.entry()), expected);
  };
  seek(1, false, 63);
  seek(62, true, 0);
  seek(65, false, 127);
  seek(126, true, 64);
  seek(129, false, 191);
  seek(190, true, 128);
  seek(193, false, 255);
  seek(254, true, 192);
}

TEST(PartitionedVersionRadixMemTableTest,
     FortyByteBytePatriciaPathPreservesOrderAndBounds) {
  ConcurrentArena arena;
  CedarPureRadixIndex index(&arena);
  std::vector<std::pair<CedarPureRadixIndex::Key, uint8_t>> expected;
  const auto insert = [&](const CedarPureRadixIndex::Key& key, uint8_t value) {
    char* entry = nullptr;
    void* handle = index.Allocate(1, &entry);
    ASSERT_NE(handle, nullptr);
    ASSERT_NE(entry, nullptr);
    *entry = static_cast<char>(value);
    ASSERT_TRUE(index.Insert(handle, key));
    expected.emplace_back(key, value);
  };
  CedarPureRadixIndex::Key zero{};
  insert(zero, 40);
  for (uint8_t byte = 0; byte < 39; ++byte) {
    CedarPureRadixIndex::Key key{};
    key[byte] = 1;
    insert(key, byte);
  }
  std::sort(expected.begin(), expected.end(), [](const auto& left, const auto& right) {
    return left.first < right.first;
  });

  const auto stats = index.GetStructureStatsForTesting();
  EXPECT_EQ(stats.max_depth, 39U);
  CedarPureRadixIndex::Cursor cursor(&index);
  cursor.SeekToFirst();
  for (const auto& [key, value] : expected) {
    ASSERT_TRUE(cursor.Valid());
    EXPECT_EQ(static_cast<uint8_t>(*cursor.entry()), value);
    cursor.Next();
  }
  EXPECT_FALSE(cursor.Valid());

  CedarPureRadixIndex::Key between{};
  between[38] = 1;
  cursor.SeekForPrev(between);
  ASSERT_TRUE(cursor.Valid());
  EXPECT_EQ(static_cast<uint8_t>(*cursor.entry()), 38U);
}

TEST(PartitionedVersionRadixMemTableTest,
     FrozenByteCursorLoadsBranchesOnlyWhileSeeking) {
  ConcurrentArena arena;
  std::atomic<size_t> branch_loads{0};
  CedarPureRadixIndex::TestHooks hooks;
  hooks.branch_load_for_testing = [&] {
    branch_loads.fetch_add(1, std::memory_order_relaxed);
  };
  CedarPureRadixIndex index(&arena, hooks);
  for (uint16_t value = 0; value < 256; ++value) {
    CedarPureRadixIndex::Key key{};
    key[39] = static_cast<uint8_t>(value);
    char* entry = nullptr;
    void* handle = index.Allocate(1, &entry);
    ASSERT_NE(handle, nullptr);
    ASSERT_NE(entry, nullptr);
    *entry = static_cast<char>(value);
    ASSERT_TRUE(index.Insert(handle, key));
  }
  index.MarkReadOnly();
  index.PrepareForFlush();
  branch_loads.store(0, std::memory_order_relaxed);
  CedarPureRadixIndex::Cursor cursor(&index);
  CedarPureRadixIndex::Key seek_key{};
  seek_key[39] = 127;
  cursor.Seek(seek_key);
  ASSERT_TRUE(cursor.Valid());
  EXPECT_GT(branch_loads.load(std::memory_order_relaxed), 0U);
  branch_loads.store(0, std::memory_order_relaxed);
  for (size_t count = 0; count < 64; ++count) {
    ASSERT_TRUE(cursor.Valid());
    cursor.Next();
  }
  EXPECT_EQ(branch_loads.load(std::memory_order_relaxed), 0U);
}

TEST(PartitionedVersionRadixMemTableTest,
     SeekBranchVisitsRemainLinearInPatriciaHeight) {
  TestKeyComparator comparator;
  Arena arena;
  std::atomic<size_t> branch_visits{0};
  PartitionedVersionRadixFactory::Options options;
  options.branch_visit_observer_for_testing = [&] {
    branch_visits.fetch_add(1, std::memory_order_relaxed);
  };
  PartitionedVersionRadixFactory factory(options);
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));
  for (uint64_t entity_id = 1; entity_id <= 256; ++entity_id) {
    ASSERT_TRUE(Insert(table.get(), InternalKeyFor(entity_id, 1, kTypeValue),
                       "value"));
  }

  std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
  const size_t linear_bound = 2 * CedarPureRadixIndex::kMaxBits + 2;
  branch_visits.store(0, std::memory_order_relaxed);
  iterator->Seek(InternalKeyFor(129, 1, kTypeValue), nullptr);
  ASSERT_TRUE(iterator->Valid());
  EXPECT_LE(branch_visits.load(std::memory_order_relaxed), linear_bound);

  branch_visits.store(0, std::memory_order_relaxed);
  iterator->SeekForPrev(InternalKeyFor(0, 1, kTypeValue), nullptr);
  EXPECT_FALSE(iterator->Valid());
  EXPECT_LE(branch_visits.load(std::memory_order_relaxed), linear_bound);
}

TEST(PartitionedVersionRadixMemTableTest,
     AdversarialBoundarySeeksVisitEachDeepPatriciaBranchOnce) {
  TestKeyComparator comparator;
  Arena arena;
  std::atomic<size_t> branch_visits{0};
  PartitionedVersionRadixFactory::Options options;
  options.branch_visit_observer_for_testing = [&] {
    branch_visits.fetch_add(1, std::memory_order_relaxed);
  };
  PartitionedVersionRadixFactory factory(options);
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  // These keys share the longest possible entity-id prefix with entity 1.
  // Descending to the zero child at every branch forced the former recursive
  // boundary algorithm to repeat FirstLeaf/LastLeaf walks at each level.
  ASSERT_TRUE(Insert(table.get(), InternalKeyFor(1, 1, kTypeValue), "value"));
  for (uint64_t bit = 1; bit < 64; ++bit) {
    ASSERT_TRUE(Insert(table.get(),
                       InternalKeyFor((uint64_t{1} << bit) | 1U, 1,
                                      kTypeValue),
                       "value"));
  }

  std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
  const size_t linear_bound = 64;
  branch_visits.store(0, std::memory_order_relaxed);
  iterator->Seek(InternalKeyFor(0, 1, kValueTypeForSeek), nullptr);
  ASSERT_TRUE(iterator->Valid());
  EXPECT_LE(branch_visits.load(std::memory_order_relaxed), linear_bound);

  branch_visits.store(0, std::memory_order_relaxed);
  iterator->SeekForPrev(InternalKeyFor(0, 1, kValueTypeForSeek), nullptr);
  EXPECT_FALSE(iterator->Valid());
  EXPECT_LE(branch_visits.load(std::memory_order_relaxed), linear_bound);
}

TEST(PartitionedVersionRadixMemTableTest,
     FrozenMemTableForwardScansRemainOrderedAcrossRepeatedCursors) {
  TestKeyComparator comparator;
  Arena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));
  for (uint64_t entity_id = 1; entity_id <= 64; ++entity_id) {
    ASSERT_TRUE(Insert(table.get(), InternalKeyFor(entity_id, 1, kTypeValue),
                       "value"));
  }
  const std::vector<std::string> expected = Collect(table.get());

  table->MarkReadOnly();
  table->PrepareForFlush();
  EXPECT_EQ(Collect(table.get()), expected);
  EXPECT_EQ(Collect(table.get()), expected);

  std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
  iterator->Seek(expected[expected.size() / 2], nullptr);
  ASSERT_TRUE(iterator->Valid());
  for (size_t index = expected.size() / 2; index < expected.size(); ++index) {
    ASSERT_TRUE(iterator->Valid());
    EXPECT_EQ(GetLengthPrefixedSlice(iterator->key()), Slice(expected[index]));
    iterator->Next();
  }
}

TEST(PartitionedVersionRadixMemTableTest,
     ReadyFrozenChainAvoidsBranchLoadsAfterForwardSeek) {
  TestKeyComparator comparator;
  Arena arena;
  std::atomic<size_t> branch_loads{0};
  PartitionedVersionRadixFactory::Options options;
  options.branch_load_observer_for_testing = [&] {
    branch_loads.fetch_add(1, std::memory_order_relaxed);
  };
  PartitionedVersionRadixFactory factory(options);
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));
  for (uint64_t entity_id = 1; entity_id <= 64; ++entity_id) {
    ASSERT_TRUE(Insert(table.get(), InternalKeyFor(entity_id, 1, kTypeValue),
                       "value"));
  }
  table->MarkReadOnly();
  table->PrepareForFlush();
  std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
  iterator->Seek(InternalKeyFor(16, 1, kTypeValue), nullptr);
  ASSERT_TRUE(iterator->Valid());
  branch_loads.store(0, std::memory_order_relaxed);
  for (int index = 0; index < 8 && iterator->Valid(); ++index) {
    iterator->Next();
  }
  EXPECT_EQ(branch_loads.load(std::memory_order_relaxed), 0U);
}

TEST(PartitionedVersionRadixMemTableTest,
     DeepCanonicalPatriciaPathRemainsOrdered) {
  TestKeyComparator comparator;
  Arena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  // Every key is a valid v2 vertex fact. Inserting progressively earlier
  // entity-id bits creates a path deeper than the inline insertion cache.
  std::vector<std::string> expected;
  for (uint64_t bit = 1; bit < 64; ++bit) {
    const uint64_t entity_id = (uint64_t{1} << bit) | 1U;
    const std::string key = InternalKeyFor(entity_id, 1, kTypeValue);
    ASSERT_TRUE(Insert(table.get(), key, "value"));
    expected.push_back(key);
  }
  expected.push_back(InternalKeyFor(1, 1, kTypeValue));
  ASSERT_TRUE(Insert(table.get(), expected.back(), "value"));
  std::sort(expected.begin(), expected.end(), InternalKeyLess);

  EXPECT_EQ(Collect(table.get()), expected);
  table->MarkReadOnly();
  table->PrepareForFlush();
  EXPECT_EQ(Collect(table.get()), expected);
}

TEST(PartitionedVersionRadixMemTableTest,
     MarkReadOnlyBuildsFrozenChainWithoutBlockingPathReader) {
  TestKeyComparator comparator;
  Arena arena;
  std::mutex mutex;
  std::condition_variable condition;
  bool builder_paused = false;
  bool release_builder = false;
  PartitionedVersionRadixFactory::Options options;
  options.frozen_chain_builder_observer_for_testing = [&] {
    std::unique_lock<std::mutex> lock(mutex);
    builder_paused = true;
    condition.notify_all();
    condition.wait(lock, [&] { return release_builder; });
  };
  PartitionedVersionRadixFactory factory(options);
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));
  for (uint64_t entity_id = 1; entity_id <= 64; ++entity_id) {
    ASSERT_TRUE(Insert(table.get(), InternalKeyFor(entity_id, 1, kTypeValue),
                       "value"));
  }
  const std::vector<std::string> expected = Collect(table.get());
  table->MarkReadOnly();
  {
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_FALSE(builder_paused);
  }
  std::thread builder([&] { table->PrepareForFlush(); });
  bool observed_builder = false;
  {
    std::unique_lock<std::mutex> lock(mutex);
    observed_builder = condition.wait_for(lock, std::chrono::seconds(10), [&] {
      return builder_paused;
    });
  }
  EXPECT_TRUE(observed_builder);

  std::atomic<bool> reader_done{false};
  std::vector<std::string> reader_result;
  std::thread reader([&] {
    reader_result = Collect(table.get());
    reader_done.store(true, std::memory_order_release);
  });
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(10);
  while (!reader_done.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  EXPECT_TRUE(reader_done.load(std::memory_order_acquire));
  if (reader_done.load(std::memory_order_acquire)) {
    EXPECT_EQ(reader_result, expected);
  }
  {
    std::lock_guard<std::mutex> lock(mutex);
    release_builder = true;
  }
  condition.notify_all();
  reader.join();
  builder.join();
  EXPECT_EQ(Collect(table.get()), expected);
}

TEST(PartitionedVersionRadixMemTableTest,
     SeekForPrevFindsTheLastEntryOfALargeLowerSubtree) {
  TestKeyComparator comparator;
  Arena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  constexpr uint64_t kLastEntity = 4096;
  for (uint64_t entity_id = 1; entity_id <= kLastEntity; ++entity_id) {
    ASSERT_TRUE(Insert(table.get(),
                       InternalKeyFor(entity_id, 1, kTypeValue), "value"));
  }

  std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
  iterator->SeekForPrev(
      InternalKeyFor(uint64_t{1} << 56, 1, kTypeValue), nullptr);

  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(GetLengthPrefixedSlice(iterator->key()),
            Slice(InternalKeyFor(kLastEntity, 1, kTypeValue)));
}

TEST(PartitionedVersionRadixMemTableTest, RejectsNonV2InternalKeys) {
  TestKeyComparator comparator;
  Arena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  std::string legacy_user_key(28, '\0');
  legacy_user_key[0] = 1;
  std::string legacy_internal_key;
  AppendInternalKey(&legacy_internal_key,
                    ParsedInternalKey(legacy_user_key, 1, kTypeValue));
  EXPECT_FALSE(Insert(table.get(), legacy_internal_key, "legacy"));

  std::string unknown_version = InternalKeyFor(7, 1, kTypeValue);
  unknown_version[0] = 3;
  EXPECT_FALSE(Insert(table.get(), unknown_version, "unknown"));
  EXPECT_TRUE(Collect(table.get()).empty());
}

TEST(PartitionedVersionRadixMemTableTest, RejectsFactKeysOutsideTheV2Contract) {
  TestKeyComparator comparator;
  Arena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  std::string zero_entity = InternalKeyFor(7, 1, kTypeValue);
  std::fill(zero_entity.begin() + 8, zero_entity.begin() + 16, '\0');
  EXPECT_FALSE(Insert(table.get(), zero_entity, "zero-entity"));

  std::string unknown_family = InternalKeyFor(7, 1, kTypeValue);
  unknown_family[5] = 99;
  EXPECT_FALSE(Insert(table.get(), unknown_family, "unknown-family"));

  std::string state_property = InternalKeyFor(7, 1, kTypeValue);
  state_property[7] = 1;
  EXPECT_FALSE(Insert(table.get(), state_property, "state-property"));

  std::string property_without_property = InternalKeyFor(7, 1, kTypeValue);
  property_without_property[5] = 2;
  EXPECT_FALSE(Insert(table.get(), property_without_property, "no-property"));

  EXPECT_FALSE(Insert(table.get(), InternalKeyFor(7, 1, kTypeMerge), "merge"));
  EXPECT_TRUE(Collect(table.get()).empty());
}

TEST(PartitionedVersionRadixMemTableTest,
     RandomizedV2HistoriesMatchUpstreamSkipListForIterationAndSeek) {
  TestKeyComparator comparator;
  Arena radix_arena;
  Arena skiplist_arena;
  PartitionedVersionRadixFactory radix_factory;
  SkipListFactory skiplist_factory;
  std::unique_ptr<MemTableRep> radix(
      radix_factory.CreateMemTableRep(comparator, &radix_arena, nullptr, nullptr));
  std::unique_ptr<MemTableRep> skiplist(
      skiplist_factory.CreateMemTableRep(comparator, &skiplist_arena, nullptr,
                                         nullptr));

  std::vector<std::string> keys;
  for (uint64_t entity_id = 1; entity_id <= 48; ++entity_id) {
    for (SequenceNumber version = 1; version <= 4; ++version) {
      const SequenceNumber sequence = entity_id * 32 + version;
      const ValueType type = version % 3 == 0 ? kTypeDeletion : kTypeValue;
      keys.push_back(InternalKeyFor(entity_id, sequence, type));
    }
  }
  std::mt19937_64 generator(0xCEDA20260804ULL);
  std::shuffle(keys.begin(), keys.end(), generator);
  for (const std::string& key : keys) {
    ASSERT_TRUE(Insert(radix.get(), key, "value"));
    ASSERT_TRUE(Insert(skiplist.get(), key, "value"));
  }

  EXPECT_EQ(Collect(radix.get()), Collect(skiplist.get()));

  const auto lower_bound = [](MemTableRep* table, const std::string& key) {
    std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
    iterator->Seek(key, nullptr);
    return iterator->Valid() ? GetLengthPrefixedSlice(iterator->key()).ToString()
                             : std::string();
  };
  for (uint64_t entity_id = 0; entity_id <= 50; ++entity_id) {
    for (SequenceNumber sequence : {0U, 1U, 33U, 97U, 1600U}) {
      const std::string seek =
          InternalKeyFor(entity_id, sequence, kValueTypeForSeek);
      EXPECT_EQ(lower_bound(radix.get(), seek), lower_bound(skiplist.get(), seek))
          << "entity_id=" << entity_id << " sequence=" << sequence;
    }
  }
}

TEST(PartitionedVersionRadixMemTableTest,
     GetMatchesUpstreamSkipListAcrossSnapshotSequences) {
  TestKeyComparator comparator;
  Arena radix_arena;
  Arena skiplist_arena;
  PartitionedVersionRadixFactory radix_factory;
  SkipListFactory skiplist_factory;
  std::unique_ptr<MemTableRep> radix(
      radix_factory.CreateMemTableRep(comparator, &radix_arena, nullptr, nullptr));
  std::unique_ptr<MemTableRep> skiplist(
      skiplist_factory.CreateMemTableRep(comparator, &skiplist_arena, nullptr,
                                         nullptr));

  std::vector<std::string> keys;
  for (uint64_t entity_id = 1; entity_id <= 32; ++entity_id) {
    for (SequenceNumber version = 1; version <= 5; ++version) {
      const SequenceNumber sequence = entity_id * 64 + version * 3;
      const ValueType type = version % 2 == 0 ? kTypeDeletion : kTypeValue;
      keys.push_back(InternalKeyFor(entity_id, sequence, type));
    }
  }
  std::mt19937_64 generator(0xCEDA20260805ULL);
  std::shuffle(keys.begin(), keys.end(), generator);
  for (const std::string& key : keys) {
    ASSERT_TRUE(Insert(radix.get(), key, "value"));
    ASSERT_TRUE(Insert(skiplist.get(), key, "value"));
  }

  for (uint64_t entity_id = 0; entity_id <= 34; ++entity_id) {
    const std::string user_key = V2UserKey(entity_id);
    for (SequenceNumber snapshot : {0U, 1U, 67U, 256U, 4096U}) {
      EXPECT_EQ(CollectGetCandidates(radix.get(), user_key, snapshot),
                CollectGetCandidates(skiplist.get(), user_key, snapshot))
          << "entity_id=" << entity_id << " snapshot=" << snapshot;
    }
  }
}

TEST(PartitionedVersionRadixMemTableTest,
     RandomizedHistoryMatchesExactVisibilityModelAtEverySequence) {
  TestKeyComparator comparator;
  Arena radix_arena;
  Arena skiplist_arena;
  PartitionedVersionRadixFactory radix_factory;
  SkipListFactory skiplist_factory;
  std::unique_ptr<MemTableRep> radix(
      radix_factory.CreateMemTableRep(comparator, &radix_arena, nullptr, nullptr));
  std::unique_ptr<MemTableRep> skiplist(
      skiplist_factory.CreateMemTableRep(comparator, &skiplist_arena, nullptr,
                                         nullptr));

  std::vector<std::string> keys;
  for (uint64_t entity_id = 1; entity_id <= 24; ++entity_id) {
    for (SequenceNumber sequence = 1; sequence <= 12; ++sequence) {
      keys.push_back(InternalKeyFor(entity_id, sequence,
                                    sequence % 4 == 0 ? kTypeDeletion
                                                      : kTypeValue));
    }
  }
  std::mt19937_64 generator(0xCEDA20260806ULL);
  std::shuffle(keys.begin(), keys.end(), generator);
  for (const std::string& key : keys) {
    ASSERT_TRUE(Insert(radix.get(), key, "value"));
    ASSERT_TRUE(Insert(skiplist.get(), key, "value"));
  }

  std::vector<std::string> expected = keys;
  std::sort(expected.begin(), expected.end(), InternalKeyLess);
  EXPECT_EQ(Collect(radix.get()), expected);
  EXPECT_EQ(Collect(skiplist.get()), expected);
  std::reverse(expected.begin(), expected.end());
  EXPECT_EQ(CollectReverse(radix.get()), expected);
  EXPECT_EQ(CollectReverse(skiplist.get()), expected);

  for (uint64_t entity_id = 0; entity_id <= 25; ++entity_id) {
    const std::string user_key = V2UserKey(entity_id);
    for (SequenceNumber snapshot = 0; snapshot <= 12; ++snapshot) {
      const std::vector<std::string> model =
          VisibleCandidatesFromModel(keys, user_key, snapshot);
      EXPECT_EQ(CollectGetCandidates(radix.get(), user_key, snapshot), model)
          << "entity_id=" << entity_id << " snapshot=" << snapshot;
      EXPECT_EQ(CollectGetCandidates(skiplist.get(), user_key, snapshot), model)
          << "entity_id=" << entity_id << " snapshot=" << snapshot;
    }
  }
}

TEST(PartitionedVersionRadixMemTableTest,
     SortedMultiGetAndUnsortedAdapterMatchSkipList) {
  TestKeyComparator comparator;
  Arena radix_arena;
  Arena skiplist_arena;
  PartitionedVersionRadixFactory radix_factory;
  SkipListFactory skiplist_factory;
  std::unique_ptr<MemTableRep> radix(
      radix_factory.CreateMemTableRep(comparator, &radix_arena, nullptr, nullptr));
  std::unique_ptr<MemTableRep> skiplist(
      skiplist_factory.CreateMemTableRep(comparator, &skiplist_arena, nullptr,
                                         nullptr));
  for (MemTableRep* table : {radix.get(), skiplist.get()}) {
    for (uint64_t entity_id = 1; entity_id <= 10; ++entity_id) {
      for (SequenceNumber sequence = 1; sequence <= 5; ++sequence) {
        ASSERT_TRUE(Insert(table, InternalKeyFor(
                                      entity_id, sequence,
                                      sequence == 3 ? kTypeDeletion : kTypeValue),
                           "value"));
      }
    }
  }

  const std::vector<LookupRequest> requests = {
      {V2UserKey(3), 0},  {V2UserKey(3), 2}, {V2UserKey(3), 5},
      {V2UserKey(3), 5},  {V2UserKey(7), 4}, {V2UserKey(99), 5},
      {V2UserKey(10), 1}, {V2UserKey(1), 5},
  };
  const std::vector<size_t> sorted_order = SortedLookupOrder(requests);
  const MultiGetCandidateResults radix_sorted =
      CollectMultiGetInOrder(radix.get(), requests, sorted_order);
  const MultiGetCandidateResults skiplist_sorted =
      CollectMultiGetInOrder(skiplist.get(), requests, sorted_order);
  ASSERT_TRUE(radix_sorted.status.ok());
  ASSERT_TRUE(skiplist_sorted.status.ok());
  EXPECT_EQ(radix_sorted.entries, skiplist_sorted.entries);
  for (size_t index = 0; index < requests.size(); ++index) {
    EXPECT_EQ(radix_sorted.entries[index],
              CollectGetCandidates(skiplist.get(), requests[index].user_key,
                                   requests[index].snapshot))
        << "sorted slot=" << index;
  }

  std::vector<LookupRequest> caller_order = requests;
  std::mt19937_64 generator(0xCEDA20260807ULL);
  std::shuffle(caller_order.begin(), caller_order.end(), generator);
  const MultiGetCandidateResults radix_adapter =
      CollectMultiGetInCallerOrder(radix.get(), caller_order);
  const MultiGetCandidateResults skiplist_adapter =
      CollectMultiGetInCallerOrder(skiplist.get(), caller_order);
  ASSERT_TRUE(radix_adapter.status.ok());
  ASSERT_TRUE(skiplist_adapter.status.ok());
  EXPECT_EQ(radix_adapter.entries, skiplist_adapter.entries);
  for (size_t index = 0; index < caller_order.size(); ++index) {
    EXPECT_EQ(radix_adapter.entries[index],
              CollectGetCandidates(skiplist.get(), caller_order[index].user_key,
                                   caller_order[index].snapshot))
        << "caller slot=" << index;
  }
}

TEST(PartitionedVersionRadixMemTableTest,
     ValidatedGetAndMultiGetMatchUpstreamSkipListCandidates) {
  TestKeyComparator comparator;
  Arena radix_arena;
  Arena skiplist_arena;
  PartitionedVersionRadixFactory radix_factory;
  SkipListFactory skiplist_factory;
  std::unique_ptr<MemTableRep> radix(
      radix_factory.CreateMemTableRep(comparator, &radix_arena, nullptr, nullptr));
  std::unique_ptr<MemTableRep> skiplist(
      skiplist_factory.CreateMemTableRep(comparator, &skiplist_arena, nullptr,
                                         nullptr));
  for (MemTableRep* table : {radix.get(), skiplist.get()}) {
    ASSERT_TRUE(Insert(table, InternalKeyFor(7, 30, kTypeValue), "v30"));
    ASSERT_TRUE(Insert(table, InternalKeyFor(7, 20, kTypeDeletion), "d20"));
    ASSERT_TRUE(Insert(table, InternalKeyFor(7, 10, kTypeValue), "v10"));
    ASSERT_TRUE(Insert(table, InternalKeyFor(8, 40, kTypeValue), "v40"));
  }

  const auto accept = [](const char*, bool) { return Status::OK(); };
  LookupKey lookup(V2UserKey(7), 25);
  GetCandidates radix_get{lookup.user_key(), {}};
  GetCandidates skiplist_get{lookup.user_key(), {}};
  ASSERT_TRUE(radix
                  ->GetAndValidate(lookup, &radix_get, CollectMatchingUserKey,
                                   false, true, accept)
                  .ok());
  ASSERT_TRUE(skiplist
                  ->GetAndValidate(lookup, &skiplist_get, CollectMatchingUserKey,
                                   false, true, accept)
                  .ok());
  EXPECT_EQ(radix_get.entries, skiplist_get.entries);

  LookupKey first(V2UserKey(7), 25);
  LookupKey second(V2UserKey(8), 50);
  const char* keys[] = {first.memtable_key().data(), second.memtable_key().data()};
  GetCandidates radix_first{first.user_key(), {}};
  GetCandidates radix_second{second.user_key(), {}};
  GetCandidates skiplist_first{first.user_key(), {}};
  GetCandidates skiplist_second{second.user_key(), {}};
  void* radix_args[] = {&radix_first, &radix_second};
  void* skiplist_args[] = {&skiplist_first, &skiplist_second};
  ASSERT_TRUE(radix
                  ->MultiGet(2, keys, radix_args, CollectMatchingUserKey, false,
                             true, accept)
                  .ok());
  ASSERT_TRUE(skiplist
                  ->MultiGet(2, keys, skiplist_args, CollectMatchingUserKey, false,
                             true, accept)
                  .ok());
  EXPECT_EQ(radix_first.entries, skiplist_first.entries);
  EXPECT_EQ(radix_second.entries, skiplist_second.entries);

  const auto reject = [](const char*, bool) {
    return Status::Corruption("expected validation failure");
  };
  GetCandidates rejected{lookup.user_key(), {}};
  const Status rejected_status = radix->GetAndValidate(
      lookup, &rejected, CollectMatchingUserKey, false, true, reject);
  EXPECT_TRUE(rejected_status.IsCorruption());
  EXPECT_TRUE(rejected.entries.empty());
}

TEST(PartitionedVersionRadixMemTableTest,
     PointGetUsesStackCursorWithoutIteratorHeapAllocation) {
  TestKeyComparator comparator;
  Arena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));
  ASSERT_TRUE(Insert(table.get(), InternalKeyFor(7, 30, kTypeValue), "value"));

  LookupKey lookup(V2UserKey(7), 40);
  size_t callbacks = 0;
  const size_t allocations_before =
      g_heap_allocations.load(std::memory_order_relaxed);
  table->Get(lookup, &callbacks, [](void* argument, const char*) {
    ++*static_cast<size_t*>(argument);
    return false;
  });
  const size_t allocations_after =
      g_heap_allocations.load(std::memory_order_relaxed);

  EXPECT_EQ(callbacks, 1U);
  EXPECT_EQ(allocations_after, allocations_before);
}

TEST(PartitionedVersionRadixMemTableTest,
     ConcurrentInsertionsRemainOrderedAndRejectConcurrentDuplicates) {
  TestKeyComparator comparator;
  ConcurrentArena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  constexpr size_t kThreads = 4;
  constexpr size_t kKeysPerThread = 128;
  std::vector<std::thread> writers;
  for (size_t thread_index = 0; thread_index < kThreads; ++thread_index) {
    writers.emplace_back([&, thread_index] {
      for (size_t key_index = 0; key_index < kKeysPerThread; ++key_index) {
        const uint64_t entity_id =
            1 + thread_index * kKeysPerThread + key_index;
        ASSERT_TRUE(InsertConcurrently(
            table.get(), InternalKeyFor(entity_id, 1, kTypeValue), "value"));
      }
    });
  }
  for (auto& writer : writers) writer.join();

  const std::vector<std::string> collected = Collect(table.get());
  ASSERT_EQ(collected.size(), kThreads * kKeysPerThread);
  for (size_t index = 1; index < collected.size(); ++index) {
    EXPECT_TRUE(InternalKeyLess(collected[index - 1], collected[index]));
  }

  std::vector<std::thread> duplicate_writers;
  const std::string duplicate = InternalKeyFor(10000, 1, kTypeValue);
  std::atomic<size_t> duplicate_winners{0};
  for (size_t index = 0; index < kThreads; ++index) {
    duplicate_writers.emplace_back([&] {
      if (InsertConcurrently(table.get(), duplicate, "duplicate")) {
        duplicate_winners.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (auto& writer : duplicate_writers) writer.join();
  EXPECT_EQ(duplicate_winners.load(std::memory_order_relaxed), 1U);
}

TEST(PartitionedVersionRadixMemTableTest,
     ThreeWriterUniqueAndDuplicateStressMatchesUpstreamSkipList) {
  TestKeyComparator comparator;
  ConcurrentArena radix_arena;
  ConcurrentArena skiplist_arena;
  PartitionedVersionRadixFactory radix_factory;
  SkipListFactory skiplist_factory;
  std::unique_ptr<MemTableRep> radix(
      radix_factory.CreateMemTableRep(comparator, &radix_arena, nullptr,
                                      nullptr));
  std::unique_ptr<MemTableRep> skiplist(
      skiplist_factory.CreateMemTableRep(comparator, &skiplist_arena, nullptr,
                                         nullptr));

  constexpr size_t kWriters = 3;
  constexpr size_t kKeysPerWriter = 192;
  std::atomic<bool> start{false};
  std::atomic<bool> insert_failed{false};
  std::vector<std::thread> writers;
  writers.reserve(kWriters);
  for (size_t writer = 0; writer < kWriters; ++writer) {
    writers.emplace_back([&, writer] {
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      for (size_t index = 0; index < kKeysPerWriter; ++index) {
        const uint64_t entity_id =
            1 + writer * kKeysPerWriter + ((index * 37) % kKeysPerWriter);
        const std::string key = InternalKeyFor(
            entity_id, 1 + (index % 3),
            index % 17 == 0 ? kTypeDeletion : kTypeValue);
        if (!InsertConcurrently(radix.get(), key, "radix") ||
            !InsertConcurrently(skiplist.get(), key, "skiplist")) {
          insert_failed.store(true, std::memory_order_release);
        }
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (std::thread& writer : writers) writer.join();

  EXPECT_FALSE(insert_failed.load(std::memory_order_acquire));
  const std::vector<std::string> radix_keys = Collect(radix.get());
  const std::vector<std::string> skiplist_keys = Collect(skiplist.get());
  ASSERT_EQ(radix_keys.size(), kWriters * kKeysPerWriter);
  EXPECT_EQ(radix_keys, skiplist_keys);
  EXPECT_EQ(OrderedKeyHash(radix_keys), OrderedKeyHash(skiplist_keys));
  for (size_t index = 1; index < radix_keys.size(); ++index) {
    EXPECT_TRUE(InternalKeyLess(radix_keys[index - 1], radix_keys[index]));
  }

  const std::string duplicate = InternalKeyFor(999999, 7, kTypeValue);
  std::atomic<size_t> duplicate_winners{0};
  std::vector<std::thread> duplicate_writers;
  duplicate_writers.reserve(kWriters);
  for (size_t writer = 0; writer < kWriters; ++writer) {
    duplicate_writers.emplace_back([&] {
      if (InsertConcurrently(radix.get(), duplicate, "duplicate")) {
        duplicate_winners.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (std::thread& writer : duplicate_writers) writer.join();

  EXPECT_EQ(duplicate_winners.load(std::memory_order_relaxed), 1U);
  EXPECT_EQ(Collect(radix.get()).size(), kWriters * kKeysPerWriter + 1U);
}

TEST(PartitionedVersionRadixMemTableTest,
     PausedBeforeCasDoesNotBlockUnrelatedWriter) {
  TestKeyComparator comparator;
  ConcurrentArena arena;
  std::mutex mutex;
  std::condition_variable condition;
  bool first_writer_paused = false;
  bool release_first_writer = false;
  uint32_t pauses = 0;
  PartitionedVersionRadixFactory::Options options;
  options.before_cas_observer_for_testing = [&] {
    std::unique_lock<std::mutex> lock(mutex);
    if (pauses++ != 0) return;
    first_writer_paused = true;
    condition.notify_all();
    condition.wait_for(lock, std::chrono::seconds(10), [&] {
      return release_first_writer;
    });
  };
  PartitionedVersionRadixFactory factory(options);
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  std::atomic<bool> first_inserted{false};
  std::atomic<bool> second_inserted{false};
  std::thread first_writer([&] {
    first_inserted.store(
        InsertConcurrently(table.get(), InternalKeyFor(1, 1, kTypeValue), "one"),
        std::memory_order_release);
  });
  bool observed_first_writer = false;
  {
    std::unique_lock<std::mutex> lock(mutex);
    observed_first_writer = condition.wait_for(lock, std::chrono::seconds(10), [&] {
      return first_writer_paused;
    });
  }
  EXPECT_TRUE(observed_first_writer);
  std::thread second_writer([&] {
    second_inserted.store(
        InsertConcurrently(table.get(), InternalKeyFor(2, 1, kTypeValue), "two"),
        std::memory_order_release);
  });
  const auto progress_deadline = std::chrono::steady_clock::now() +
                                 std::chrono::seconds(10);
  while (!second_inserted.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < progress_deadline) {
    std::this_thread::yield();
  }
  EXPECT_TRUE(second_inserted.load(std::memory_order_acquire));
  {
    std::lock_guard<std::mutex> lock(mutex);
    release_first_writer = true;
  }
  condition.notify_all();
  first_writer.join();
  second_writer.join();

  EXPECT_TRUE(first_inserted.load(std::memory_order_acquire));
  EXPECT_TRUE(second_inserted.load(std::memory_order_acquire));
  const std::vector<std::string> collected = Collect(table.get());
  ASSERT_EQ(collected.size(), 2U);
  for (size_t index = 1; index < collected.size(); ++index) {
    EXPECT_TRUE(InternalKeyLess(collected[index - 1], collected[index]));
  }
}

TEST(PartitionedVersionRadixMemTableTest,
     StaleWrappingCasRetriesAfterAnotherWriterPublishesAncestor) {
  TestKeyComparator comparator;
  ConcurrentArena arena;

  std::mutex mutex;
  std::condition_variable condition;
  bool paused = false;
  bool release = false;
  bool arm_pause = false;
  uint32_t before_cas_calls = 0;
  PartitionedVersionRadixFactory::Options options;
  options.before_cas_observer_for_testing = [&] {
    std::unique_lock<std::mutex> lock(mutex);
    if (!arm_pause || before_cas_calls++ != 0) return;
    paused = true;
    condition.notify_all();
    condition.wait(lock, [&] { return release; });
  };
  PartitionedVersionRadixFactory factory(options);
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));
  ASSERT_TRUE(InsertConcurrently(table.get(), InternalKeyFor(1, 1, kTypeValue),
                                 "base"));
  {
    std::lock_guard<std::mutex> lock(mutex);
    arm_pause = true;
  }

  std::atomic<bool> stale_writer_succeeded{false};
  std::thread stale_writer([&] {
    stale_writer_succeeded.store(
        InsertConcurrently(table.get(), InternalKeyFor(2, 1, kTypeValue),
                           "stale"),
        std::memory_order_release);
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(10), [&] {
      return paused;
    }));
  }

  // This writer publishes a branch over the leaf observed by stale_writer.
  ASSERT_TRUE(InsertConcurrently(table.get(), InternalKeyFor(8, 1, kTypeValue),
                                 "ancestor"));
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
  }
  condition.notify_all();
  stale_writer.join();

  ASSERT_TRUE(stale_writer_succeeded.load(std::memory_order_acquire));
  const std::vector<std::string> collected = Collect(table.get());
  ASSERT_EQ(collected.size(), 3U);
  EXPECT_EQ(Slice(collected[0]),
            Slice(InternalKeyFor(1, 1, kTypeValue)));
  EXPECT_EQ(Slice(collected[1]),
            Slice(InternalKeyFor(2, 1, kTypeValue)));
  EXPECT_EQ(Slice(collected[2]),
            Slice(InternalKeyFor(8, 1, kTypeValue)));
}

TEST(PartitionedVersionRadixMemTableTest,
     ByteSegmentDirectInsertAndWrapperBothPublishWithoutRetry) {
  ConcurrentArena arena;
  std::mutex mutex;
  std::condition_variable condition;
  bool paused = false;
  bool release = false;
  size_t observer_calls = 0;
  CedarPureRadixIndex::TestHooks hooks;
  hooks.table_snapshot_cas_for_testing = [&] {
    std::unique_lock<std::mutex> lock(mutex);
    if (observer_calls++ != 0) return;
    paused = true;
    condition.notify_all();
    condition.wait(lock, [&] { return release; });
  };
  CedarPureRadixIndex index(&arena, hooks);

  const auto allocate = [&](const CedarPureRadixIndex::Key& key) {
    char* entry = nullptr;
    void* handle = index.Allocate(1, &entry);
    EXPECT_NE(handle, nullptr);
    EXPECT_NE(entry, nullptr);
    if (entry != nullptr) *entry = static_cast<char>(key[0]);
    return handle;
  };
  CedarPureRadixIndex::Key key64{};
  CedarPureRadixIndex::Key key128{};
  CedarPureRadixIndex::Key key192{};
  CedarPureRadixIndex::Key key193{};
  key64[0] = 64;
  key128[0] = 128;
  key192[0] = 192;
  key193[0] = 193;
  ASSERT_TRUE(index.Insert(allocate(key64), key64));
  ASSERT_TRUE(index.Insert(allocate(key192), key192));

  std::atomic<bool> direct_inserted{false};
  std::thread direct_writer([&] {
    direct_inserted.store(index.Insert(allocate(key128), key128),
                          std::memory_order_release);
  });
  bool observed_pause = false;
  {
    std::unique_lock<std::mutex> lock(mutex);
    observed_pause = condition.wait_for(lock, std::chrono::seconds(2), [&] {
      return paused;
    });
  }
  EXPECT_TRUE(observed_pause);
  if (observed_pause) {
    ASSERT_TRUE(index.Insert(allocate(key193), key193));
    {
      std::lock_guard<std::mutex> lock(mutex);
      release = true;
    }
    condition.notify_all();
  }
  direct_writer.join();

  EXPECT_TRUE(direct_inserted.load(std::memory_order_acquire));
  // The direct child occupies segment 16 and the wrapper replaces segment 24.
  // Their independent CAS edges mean the paused direct writer does not retry.
  EXPECT_EQ(observer_calls, 2U);
  for (const auto& key : {key64, key128, key192, key193}) {
    EXPECT_TRUE(index.Contains(key));
  }
  CedarPureRadixIndex::Cursor cursor(&index);
  cursor.SeekToFirst();
  for (const auto expected : {64, 128, 192, 193}) {
    ASSERT_TRUE(cursor.Valid());
    EXPECT_EQ(static_cast<unsigned char>(*cursor.entry()), expected);
    cursor.Next();
  }
  EXPECT_FALSE(cursor.Valid());
}

TEST(PartitionedVersionRadixMemTableTest,
     SegmentPublicationEdgesUseIndependent128ByteStrides) {
  EXPECT_EQ(CedarPureRadixIndex::kByteSegmentCount, 32U);
  EXPECT_EQ(CedarPureRadixIndex::SegmentEdgeStrideForTesting(), 128U);
}

TEST(PartitionedVersionRadixMemTableTest,
     SnapshotDiagnosticsSeparateAllocationCopyAndPublication) {
  ConcurrentArena arena;
  size_t branch_bytes = 0;
  size_t block_bytes = 0;
  size_t copied_pointers = 0;
  size_t root_successes = 0;
  size_t segment_successes = 0;
  CedarPureRadixIndex::TestHooks hooks;
  hooks.branch_allocation_bytes_for_testing = [&](size_t bytes) {
    branch_bytes += bytes;
  };
  hooks.block_allocation_bytes_for_testing = [&](size_t bytes) {
    block_bytes += bytes;
  };
  hooks.copied_child_pointers_for_testing = [&](size_t count) {
    copied_pointers += count;
  };
  hooks.root_cas_result_for_testing = [&](bool success) {
    root_successes += success;
  };
  hooks.segment_cas_result_for_testing = [&](bool success) {
    segment_successes += success;
  };
  CedarPureRadixIndex index(&arena, hooks);

  for (uint8_t value : {0U, 128U, 64U, 1U}) {
    CedarPureRadixIndex::Key key{};
    key[0] = value;
    char* entry = nullptr;
    void* handle = index.Allocate(1, &entry);
    ASSERT_NE(handle, nullptr);
    ASSERT_NE(entry, nullptr);
    *entry = static_cast<char>(value);
    ASSERT_TRUE(index.Insert(handle, key));
  }
  EXPECT_GE(branch_bytes, 32U * 128U);
  EXPECT_EQ(block_bytes, 72U);
  EXPECT_EQ(copied_pointers, 1U);
  EXPECT_EQ(root_successes, 2U);
  EXPECT_EQ(segment_successes, 2U);
}

TEST(PartitionedVersionRadixMemTableTest,
     ByteSegmentStaleBlockRejectsAndRetries) {
  ConcurrentArena arena;
  std::mutex mutex;
  std::condition_variable condition;
  bool paused = false;
  bool release = false;
  size_t observer_calls = 0;
  CedarPureRadixIndex::TestHooks hooks;
  hooks.table_snapshot_cas_for_testing = [&] {
    std::unique_lock<std::mutex> lock(mutex);
    if (observer_calls++ != 0) return;
    paused = true;
    condition.notify_all();
    condition.wait(lock, [&] { return release; });
  };
  CedarPureRadixIndex index(&arena, hooks);

  const auto allocate = [&](const CedarPureRadixIndex::Key& key) {
    char* entry = nullptr;
    void* handle = index.Allocate(1, &entry);
    EXPECT_NE(handle, nullptr);
    EXPECT_NE(entry, nullptr);
    if (entry != nullptr) *entry = static_cast<char>(key[0]);
    return handle;
  };
  CedarPureRadixIndex::Key key0{};
  CedarPureRadixIndex::Key key1{};
  CedarPureRadixIndex::Key key2{};
  CedarPureRadixIndex::Key key3{};
  key0[0] = 64;
  key1[0] = 66;
  key2[0] = 69;
  key3[0] = 71;
  ASSERT_TRUE(index.Insert(allocate(key0), key0));
  ASSERT_TRUE(index.Insert(allocate(key1), key1));

  std::atomic<bool> stale_inserted{false};
  std::thread stale_writer([&] {
    stale_inserted.store(index.Insert(allocate(key2), key2),
                         std::memory_order_release);
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(2), [&] {
      return paused;
    }));
  }
  ASSERT_TRUE(index.Insert(allocate(key3), key3));
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
  }
  condition.notify_all();
  stale_writer.join();

  EXPECT_TRUE(stale_inserted.load(std::memory_order_acquire));
  // One paused stale CAS, one competing publish, then stale retry.
  EXPECT_EQ(observer_calls, 3U);
  for (const auto& key : {key0, key1, key2, key3}) EXPECT_TRUE(index.Contains(key));
}

TEST(PartitionedVersionRadixMemTableTest,
     ConcurrentReadersObserveOnlyOrderedCanonicalEntries) {
  TestKeyComparator comparator;
  ConcurrentArena arena;
  PartitionedVersionRadixFactory factory;
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  constexpr size_t kThreads = 4;
  constexpr size_t kKeysPerThread = 128;
  std::vector<std::string> universe;
  universe.reserve(kThreads * kKeysPerThread);
  for (size_t thread_index = 0; thread_index < kThreads; ++thread_index) {
    for (size_t key_index = 0; key_index < kKeysPerThread; ++key_index) {
      const uint64_t entity_id =
          1 + thread_index * kKeysPerThread + key_index;
      universe.push_back(InternalKeyFor(entity_id, 1, kTypeValue));
    }
  }

  std::atomic<bool> start{false};
  std::atomic<bool> reader_started{false};
  std::atomic<size_t> writers_remaining{kThreads};
  std::atomic<size_t> observations{0};
  std::atomic<bool> failure{false};
  std::vector<std::thread> writers;
  for (size_t thread_index = 0; thread_index < kThreads; ++thread_index) {
    writers.emplace_back([&, thread_index] {
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      while (!reader_started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (size_t key_index = 0; key_index < kKeysPerThread; ++key_index) {
        const uint64_t entity_id =
            1 + thread_index * kKeysPerThread + key_index;
        if (!InsertConcurrently(table.get(),
                                InternalKeyFor(entity_id, 1, kTypeValue),
                                "value")) {
          failure.store(true, std::memory_order_release);
        }
        if (key_index % 16 == 0) std::this_thread::yield();
      }
      writers_remaining.fetch_sub(1, std::memory_order_release);
    });
  }

  std::thread reader([&] {
    while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
    reader_started.store(true, std::memory_order_release);
    while (writers_remaining.load(std::memory_order_acquire) != 0) {
      const std::vector<std::string> observed = Collect(table.get());
      const std::vector<std::string> reverse_observed =
          CollectReverse(table.get());
      observations.fetch_add(1, std::memory_order_relaxed);
      for (const std::string& key : observed) {
        if (std::find(universe.begin(), universe.end(), key) == universe.end()) {
          failure.store(true, std::memory_order_release);
        }
      }
      for (const std::string& key : reverse_observed) {
        if (std::find(universe.begin(), universe.end(), key) == universe.end()) {
          failure.store(true, std::memory_order_release);
        }
      }
      for (size_t index = 1; index < observed.size(); ++index) {
        std::string previous_entry;
        std::string current_entry;
        PutVarint32(&previous_entry,
                    static_cast<uint32_t>(observed[index - 1].size()));
        previous_entry.append(observed[index - 1]);
        PutVarint32(&current_entry, static_cast<uint32_t>(observed[index].size()));
        current_entry.append(observed[index]);
        if (comparator(previous_entry.data(), current_entry.data()) >= 0) {
          failure.store(true, std::memory_order_release);
        }
      }
      for (size_t index = 1; index < reverse_observed.size(); ++index) {
        if (!InternalKeyLess(reverse_observed[index],
                             reverse_observed[index - 1])) {
          failure.store(true, std::memory_order_release);
        }
      }
      const uint64_t probe_id = 1 + observations.load(std::memory_order_relaxed) %
                                            (kThreads * kKeysPerThread);
      const std::vector<std::string> candidates =
          CollectGetCandidates(table.get(), V2UserKey(probe_id),
                               kMaxSequenceNumber);
      for (const std::string& candidate : candidates) {
        if (ExtractUserKey(candidate) != Slice(V2UserKey(probe_id))) {
          failure.store(true, std::memory_order_release);
        }
      }
    }
  });

  start.store(true, std::memory_order_release);
  for (auto& writer : writers) writer.join();
  reader.join();

  EXPECT_GT(observations.load(std::memory_order_relaxed), 0U);
  EXPECT_FALSE(failure.load(std::memory_order_acquire));
  EXPECT_EQ(Collect(table.get()).size(), universe.size());
}

TEST(PartitionedVersionRadixMemTableTest,
     ConcurrentSnapshotCandidatesMatchUpstreamSkipList) {
  TestKeyComparator comparator;
  ConcurrentArena radix_arena;
  ConcurrentArena skiplist_arena;
  PartitionedVersionRadixFactory radix_factory;
  SkipListFactory skiplist_factory;
  std::unique_ptr<MemTableRep> radix(
      radix_factory.CreateMemTableRep(comparator, &radix_arena, nullptr,
                                      nullptr));
  std::unique_ptr<MemTableRep> skiplist(
      skiplist_factory.CreateMemTableRep(comparator, &skiplist_arena, nullptr,
                                         nullptr));
  for (MemTableRep* table : {radix.get(), skiplist.get()}) {
    ASSERT_TRUE(InsertConcurrently(table, InternalKeyFor(1, 30, kTypeValue),
                                   "v30"));
    ASSERT_TRUE(InsertConcurrently(table, InternalKeyFor(1, 20, kTypeDeletion),
                                   "d20"));
    ASSERT_TRUE(InsertConcurrently(table, InternalKeyFor(1, 10, kTypeValue),
                                   "v10"));
  }

  constexpr size_t kWriterCount = 2;
  constexpr size_t kKeysPerWriter = 128;
  std::mutex mutex;
  std::condition_variable condition;
  bool reader_started = false;
  bool first_writer_in_window = false;
  bool overlap_observed = false;
  std::atomic<size_t> writers_remaining{kWriterCount};
  std::atomic<size_t> observations{0};
  std::atomic<bool> failure{false};
  std::vector<std::thread> writers;
  for (size_t writer = 0; writer < kWriterCount; ++writer) {
    writers.emplace_back([&, writer] {
      {
        std::unique_lock<std::mutex> lock(mutex);
        condition.wait(lock, [&] { return reader_started; });
      }
      for (size_t index = 0; index < kKeysPerWriter; ++index) {
        const uint64_t entity_id =
            2 + writer * kKeysPerWriter + index;
        const std::string key = InternalKeyFor(entity_id, 1, kTypeValue);
        if (!InsertConcurrently(radix.get(), key, "value")) {
          failure.store(true, std::memory_order_release);
        }
        if (writer == 0 && index == 0) {
          std::unique_lock<std::mutex> lock(mutex);
          first_writer_in_window = true;
          condition.notify_all();
          if (!condition.wait_for(lock, std::chrono::seconds(10), [&] {
                return overlap_observed;
              })) {
            failure.store(true, std::memory_order_release);
          }
        }
        if (!InsertConcurrently(skiplist.get(), key, "value")) {
          failure.store(true, std::memory_order_release);
        }
        if (index % 16 == 0) std::this_thread::yield();
      }
      writers_remaining.fetch_sub(1, std::memory_order_release);
    });
  }

  std::thread reader([&] {
    {
      std::lock_guard<std::mutex> lock(mutex);
      reader_started = true;
    }
    condition.notify_all();
    for (;;) {
      {
        std::unique_lock<std::mutex> lock(mutex);
        if (!overlap_observed && first_writer_in_window) {
          for (SequenceNumber snapshot : {35U, 25U, 15U}) {
            if (CollectGetCandidates(radix.get(), V2UserKey(1), snapshot) !=
                CollectGetCandidates(skiplist.get(), V2UserKey(1), snapshot)) {
              failure.store(true, std::memory_order_release);
            }
          }
          overlap_observed = true;
          condition.notify_all();
        }
      }
      for (SequenceNumber snapshot : {35U, 25U, 15U}) {
        if (CollectGetCandidates(radix.get(), V2UserKey(1), snapshot) !=
            CollectGetCandidates(skiplist.get(), V2UserKey(1), snapshot)) {
          failure.store(true, std::memory_order_release);
        }
      }
      observations.fetch_add(1, std::memory_order_relaxed);
      if (writers_remaining.load(std::memory_order_acquire) == 0) break;
    }
  });

  for (std::thread& writer : writers) writer.join();
  reader.join();

  EXPECT_TRUE(overlap_observed);
  EXPECT_GT(observations.load(std::memory_order_relaxed), 0U);
  EXPECT_FALSE(failure.load(std::memory_order_acquire));
  for (SequenceNumber snapshot : {35U, 25U, 15U}) {
    EXPECT_EQ(CollectGetCandidates(radix.get(), V2UserKey(1), snapshot),
              CollectGetCandidates(skiplist.get(), V2UserKey(1), snapshot));
  }
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE
