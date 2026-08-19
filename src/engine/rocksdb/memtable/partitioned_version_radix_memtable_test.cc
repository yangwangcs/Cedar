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
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "memory/arena.h"
#include "memory/concurrent_arena.h"
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
  EXPECT_GT(table->ApproximateMemoryUsage(), 0U);
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
  iterator_arena.AllocateAligned(4096);
  const size_t allocations_before =
      g_heap_allocations.load(std::memory_order_relaxed);
  MemTableRep::Iterator* iterator = table->GetIterator(&iterator_arena);
  const size_t allocations_after =
      g_heap_allocations.load(std::memory_order_relaxed);
  iterator->~Iterator();

  EXPECT_EQ(allocations_after, allocations_before);
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
     SeekForPrevVisitsOnlyTheRightmostPathOfALargeLowerSubtree) {
  TestKeyComparator comparator;
  Arena arena;
  std::atomic<size_t> last_entry_visits{0};
  PartitionedVersionRadixFactory::Options options;
  options.last_entry_visit_observer_for_testing = [&last_entry_visits] {
    last_entry_visits.fetch_add(1, std::memory_order_relaxed);
  };
  PartitionedVersionRadixFactory factory(options);
  std::unique_ptr<MemTableRep> table(
      factory.CreateMemTableRep(comparator, &arena, nullptr, nullptr));

  constexpr uint64_t kLastEntity = 4096;
  for (uint64_t entity_id = 1; entity_id <= kLastEntity; ++entity_id) {
    ASSERT_TRUE(Insert(table.get(),
                       InternalKeyFor(entity_id, 1, kTypeValue), "value"));
  }

  last_entry_visits.store(0, std::memory_order_relaxed);
  std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
  iterator->SeekForPrev(
      InternalKeyFor(uint64_t{1} << 56, 1, kTypeValue), nullptr);

  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(GetLengthPrefixedSlice(iterator->key()),
            Slice(InternalKeyFor(kLastEntity, 1, kTypeValue)));
  EXPECT_LE(last_entry_visits.load(std::memory_order_relaxed), 64U);
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
     RetriesContendedWriteLockWithoutDroppingOrDuplicatingEntries) {
  TestKeyComparator comparator;
  ConcurrentArena arena;
  std::mutex mutex;
  std::condition_variable condition;
  bool first_writer_has_lock = false;
  bool release_first_writer = false;
  uint32_t acquired = 0;
  std::atomic<uint32_t> retries{0};
  PartitionedVersionRadixFactory::Options options;
  options.write_lock_acquired_observer_for_testing = [&] {
    std::unique_lock<std::mutex> lock(mutex);
    if (acquired++ != 0) return;
    first_writer_has_lock = true;
    condition.notify_all();
    condition.wait_for(lock, std::chrono::seconds(10), [&] {
      return release_first_writer;
    });
  };
  options.write_lock_retry_observer_for_testing = [&retries] {
    retries.fetch_add(1, std::memory_order_relaxed);
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
      return first_writer_has_lock;
    });
  }
  EXPECT_TRUE(observed_first_writer);
  std::thread second_writer([&] {
    second_inserted.store(
        InsertConcurrently(table.get(), InternalKeyFor(2, 1, kTypeValue), "two"),
        std::memory_order_release);
  });
  const auto retry_deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(10);
  while (retries.load(std::memory_order_acquire) == 0 &&
         std::chrono::steady_clock::now() < retry_deadline) {
    std::this_thread::yield();
  }
  {
    std::lock_guard<std::mutex> lock(mutex);
    release_first_writer = true;
  }
  condition.notify_all();
  first_writer.join();
  second_writer.join();

  EXPECT_TRUE(first_inserted.load(std::memory_order_acquire));
  EXPECT_TRUE(second_inserted.load(std::memory_order_acquire));
  EXPECT_GT(retries.load(std::memory_order_relaxed), 0U);
  const std::vector<std::string> collected = Collect(table.get());
  ASSERT_EQ(collected.size(), 2U);
  for (size_t index = 1; index < collected.size(); ++index) {
    EXPECT_TRUE(InternalKeyLess(collected[index - 1], collected[index]));
  }
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

}  // namespace
}  // namespace ROCKSDB_NAMESPACE
