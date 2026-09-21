// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "rocksdb/memtablerep.h"

#include <array>
#include <cassert>
#include <cstring>
#include <memory>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "memtable/cedar_pure_radix_index.h"
#include "memory/allocator.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
namespace {

constexpr size_t kV2UserKeyBytes = 32;
constexpr size_t kV2InternalKeyBytes = 40;

void StoreBigEndian64(char* destination, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    *destination++ = static_cast<char>(value >> shift);
  }
}

CedarPureRadixIndex::Key NormalizeInternalKey(const Slice& internal_key) {
  CedarPureRadixIndex::Key result{};
  const bool is_short_v2_seek =
      internal_key.size() > 8 && internal_key.size() < kV2UserKeyBytes &&
      static_cast<unsigned char>(internal_key[0]) == 2;
  const size_t user_key_bytes = is_short_v2_seek
                                    ? internal_key.size() - 8
                                    : std::min(internal_key.size(),
                                               kV2UserKeyBytes);
  std::memcpy(result.data(), internal_key.data(), user_key_bytes);
  if (internal_key.size() >= kV2InternalKeyBytes) {
    const uint64_t tag = DecodeFixed64(internal_key.data() + kV2UserKeyBytes);
    StoreBigEndian64(reinterpret_cast<char*>(result.data() + kV2UserKeyBytes),
                     ~tag);
  }
  return result;
}

bool IsCanonicalV2InternalKey(const Slice& internal_key) {
  if (internal_key.size() != kV2InternalKeyBytes ||
      static_cast<unsigned char>(internal_key[0]) != 2) {
    return false;
  }
  const unsigned char family = static_cast<unsigned char>(internal_key[5]);
  const uint16_t property_id =
      (static_cast<uint16_t>(static_cast<unsigned char>(internal_key[6])) << 8) |
      static_cast<unsigned char>(internal_key[7]);
  uint64_t entity_id = 0;
  for (size_t index = 8; index < 16; ++index) {
    entity_id = (entity_id << 8) | static_cast<unsigned char>(internal_key[index]);
  }
  const ValueType value_type = static_cast<ValueType>(
      DecodeFixed64(internal_key.data() + kV2UserKeyBytes) & 0xffU);
  const bool is_state_or_identity = family == 1 || family == 3 || family == 4;
  const bool is_property = family == 2 || family == 5;
  return entity_id != 0 &&
         ((is_state_or_identity && property_id == 0) ||
          (is_property && property_id != 0)) &&
         (value_type == kTypeValue || value_type == kTypeDeletion);
}

class PartitionedVersionRadixMemTable final : public MemTableRep {
 private:
  class Iterator final : public MemTableRep::Iterator {
   public:
    Iterator(const PartitionedVersionRadixMemTable& table, Arena* arena)
        : cursor_(&table.index_) {
      (void)arena;
    }

    bool Valid() const override { return cursor_.Valid(); }
    const char* key() const override { return cursor_.entry(); }
    void Next() override { cursor_.Next(); }
    void Prev() override { cursor_.Prev(); }
    void Seek(const Slice& internal_key, const char* memtable_key) override {
      cursor_.Seek(NormalizeInternalKey(
          memtable_key == nullptr ? internal_key
                                  : GetLengthPrefixedSlice(memtable_key)));
    }
    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override {
      cursor_.SeekForPrev(NormalizeInternalKey(
          memtable_key == nullptr ? internal_key
                                  : GetLengthPrefixedSlice(memtable_key)));
    }
    void SeekToFirst() override { cursor_.SeekToFirst(); }
    void SeekToLast() override { cursor_.SeekToLast(); }

   private:
    CedarPureRadixIndex::Cursor cursor_;
  };

 public:
  PartitionedVersionRadixMemTable(
      const KeyComparator& comparator, Allocator* allocator,
      const PartitionedVersionRadixFactory::Options& options)
      : MemTableRep(allocator), comparator_(comparator),
        index_(allocator,
               {options.before_cas_observer_for_testing,
                options.after_cas_observer_for_testing,
                options.frozen_chain_builder_observer_for_testing,
                options.branch_visit_observer_for_testing,
                options.branch_load_observer_for_testing,
                options.boundary_candidate_observer_for_testing,
                options.table_snapshot_cas_observer_for_testing}) {}

  KeyHandle Allocate(size_t length, char** buffer) override {
    return index_.Allocate(length, buffer);
  }

  void Insert(KeyHandle handle) override {
    const bool inserted = InsertEntry(handle);
    assert(inserted);
    (void)inserted;
  }

  bool InsertKey(KeyHandle handle) override { return InsertEntry(handle); }
  void InsertConcurrently(KeyHandle handle) override { Insert(handle); }
  bool InsertKeyConcurrently(KeyHandle handle) override { return InsertEntry(handle); }

  bool Contains(const char* key) const override {
    return index_.Contains(NormalizeInternalKey(GetLengthPrefixedSlice(key)));
  }

  void Get(const LookupKey& lookup_key, void* callback_args,
           bool (*callback_func)(void*, const char*)) override {
    CedarPureRadixIndex::Cursor cursor(&index_);
    SeekCursor(&cursor, lookup_key.internal_key(),
               lookup_key.memtable_key().data());
    while (cursor.Valid() && callback_func(callback_args, cursor.entry())) {
      cursor.Next();
    }
  }

  Status GetAndValidate(
      const LookupKey& lookup_key, void* callback_args,
      bool (*callback_func)(void*, const char*), bool allow_data_in_errors,
      bool detect_key_out_of_order,
      const std::function<Status(const char*, bool)>& key_validation_callback)
      override {
    CedarPureRadixIndex::Cursor cursor(&index_);
    SeekCursor(&cursor, lookup_key.internal_key(),
               lookup_key.memtable_key().data());
    return ValidateAndVisit(&cursor, callback_args, callback_func,
                            allow_data_in_errors, detect_key_out_of_order,
                            key_validation_callback);
  }

  Status MultiGet(
      size_t num_keys, const char* const* keys, void** callback_args,
      bool (*callback_func)(void*, const char*), bool allow_data_in_errors,
      bool detect_key_out_of_order,
      const std::function<Status(const char*, bool)>& key_validation_callback)
      override {
    CedarPureRadixIndex::Cursor cursor(&index_);
    Slice ignored_internal_key;
    for (size_t index = 0; index < num_keys; ++index) {
      SeekCursor(&cursor, ignored_internal_key, keys[index]);
      Status status = ValidateAndVisit(
          &cursor, callback_args[index], callback_func,
          allow_data_in_errors, detect_key_out_of_order,
          key_validation_callback);
      if (!status.ok()) return status;
    }
    return Status::OK();
  }

  size_t ApproximateMemoryUsage() override {
    return index_.ExternalMemoryUsage();
  }

  Iterator* GetIterator(Arena* arena = nullptr) override {
    void* storage = arena == nullptr ? operator new(sizeof(Iterator))
                                     : arena->AllocateAligned(sizeof(Iterator));
    return new (storage) Iterator(*this, arena);
  }

  bool IsMergeOperatorSupported() const override { return false; }
  void MarkReadOnly() override { index_.MarkReadOnly(); }
  void PrepareForFlush() override { index_.PrepareForFlush(); }

 private:
  static void SeekCursor(CedarPureRadixIndex::Cursor* cursor,
                         const Slice& internal_key,
                         const char* memtable_key) {
    cursor->Seek(NormalizeInternalKey(
        memtable_key == nullptr ? internal_key
                                : GetLengthPrefixedSlice(memtable_key)));
  }

  bool InsertEntry(KeyHandle handle) {
    if (handle == nullptr) return false;
    const char* entry = index_.EntryForHandle(handle);
    const Slice internal_key = GetLengthPrefixedSlice(entry);
    if (!IsCanonicalV2InternalKey(internal_key)) return false;
    return index_.InsertWithBorrowedKey(
        handle, NormalizeInternalKey(internal_key),
        reinterpret_cast<const unsigned char*>(internal_key.data()));
  }

  Status ValidateAndVisit(
      CedarPureRadixIndex::Cursor* iterator, void* callback_args,
      bool (*callback_func)(void*, const char*), bool allow_data_in_errors,
      bool detect_key_out_of_order,
      const std::function<Status(const char*, bool)>& key_validation_callback) {
    const char* previous = nullptr;
    while (iterator->Valid()) {
      const char* entry = iterator->entry();
      if (key_validation_callback != nullptr) {
        Status status = key_validation_callback(entry, allow_data_in_errors);
        if (!status.ok()) return status;
      }
      if (detect_key_out_of_order && previous != nullptr &&
          comparator_(previous, entry) >= 0) {
        return Status::Corruption("Cedar pure-radix MemTable order");
      }
      if (!callback_func(callback_args, entry)) return Status::OK();
      previous = entry;
      iterator->Next();
    }
    return Status::OK();
  }

  const KeyComparator& comparator_;
  CedarPureRadixIndex index_;
};

}  // namespace

MemTableRep* PartitionedVersionRadixFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& comparator, Allocator* allocator,
    const SliceTransform*, Logger*) {
  return new PartitionedVersionRadixMemTable(comparator, allocator, options_);
}

}  // namespace ROCKSDB_NAMESPACE
