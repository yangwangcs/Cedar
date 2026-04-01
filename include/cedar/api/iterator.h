// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef FERN_ITERATOR_H_
#define FERN_ITERATOR_H_

#include "cedar/core/slice.h"
#include "cedar/core/status.h"

namespace cedar {

// A Cedar Iterator provides a way to traverse through the key-value pairs
// in a database or a specific column family. It supports both forward and
// backward iteration.
//
// Multiple threads can invoke const methods on an Iterator without external
// synchronization, but if any of the threads may call a non-const method,
// all threads accessing the same Iterator must use external synchronization.
//
// Example usage:
//   std::unique_ptr<Iterator> it(db->NewIterator(read_options));
//   for (it->SeekToFirst(); it->Valid(); it->Next()) {
//     process(it->key(), it->value());
//   }
//   if (!it->status().ok()) {
//     // handle error
//   }
class Iterator {
 public:
  Iterator();

  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;

  virtual ~Iterator();

  // An iterator is either positioned at a key/value pair, or
  // not valid. This method returns true iff the iterator is valid.
  virtual bool Valid() const = 0;

  // Position at the first key in the source.
  // The iterator is Valid() after this call iff the source is not empty.
  virtual void SeekToFirst() = 0;

  // Position at the last key in the source.
  // The iterator is Valid() after this call iff the source is not empty.
  virtual void SeekToLast() = 0;

  // Position at the first key in the source that is at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& target) = 0;

  // Moves to the next entry in the source.
  // After this call, Valid() is true iff the iterator was not positioned
  // at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // Moves to the previous entry in the source.
  // After this call, Valid() is true iff the iterator was not positioned
  // at the first entry in the source.
  // REQUIRES: Valid()
  virtual void Prev() = 0;

  // Return the key for the current entry.
  // The underlying storage for the returned slice is valid only until
  // the next modification of the iterator.
  // REQUIRES: Valid()
  virtual Slice key() const = 0;

  // Return the value for the current entry.
  // The underlying storage for the returned slice is valid only until
  // the next modification of the iterator.
  // REQUIRES: Valid()
  virtual Slice value() const = 0;

  // If an error has occurred, return it. Else return an ok status.
  // If non-OK is returned, the iterator is invalidated and Seek*()
  // need not be called again.
  virtual Status status() const = 0;

  // Clients are allowed to register function/arg1/arg2 triples that
  // will be invoked when this iterator is destroyed.
  //
  // Note that unlike all of the preceding methods, this method is
  // not abstract and therefore clients should not override it.
  using CleanupFunction = void (*)(void* arg1, void* arg2);
  void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);

  // Cedar extensions for temporal data

  // Return the timestamp for the current entry (if temporal).
  // Returns 0 if not a temporal entry.
  // REQUIRES: Valid()
  virtual uint64_t timestamp() const { return 0; }

  // Position at the first entry with timestamp <= target_time.
  // Useful for point-in-time queries.
  virtual void SeekToTime(uint64_t target_time) {
    (void)target_time;
    SeekToFirst();
  }

 private:
  // Cleanup functions are stored in a single-linked list.
  // The list's head node is inlined in the iterator.
  struct CleanupNode {
    bool IsEmpty() const { return function == nullptr; }
    void Run() {
      assert(function != nullptr);
      (*function)(arg1, arg2);
    }

    CleanupFunction function;
    void* arg1;
    void* arg2;
    CleanupNode* next;
  };
  CleanupNode cleanup_head_;
};

// Return an empty iterator (yields nothing).
Iterator* NewEmptyIterator();

// Return an empty iterator with the specified status.
Iterator* NewErrorIterator(const Status& status);

}  // namespace cedar

#endif  // FERN_ITERATOR_H_
