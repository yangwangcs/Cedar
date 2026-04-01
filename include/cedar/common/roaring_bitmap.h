// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Lightweight Roaring Bitmap implementation for column ID tracking

#ifndef FERN_ROARING_BITMAP_H_
#define FERN_ROARING_BITMAP_H_

#include <cstdint>
#include <vector>
#include <algorithm>
#include <cstring>

namespace cedar {

// Lightweight roaring bitmap for uint16_t values (column IDs)
// Optimized for small sets (typically < 100 values)
class RoaringBitmap {
 public:
  RoaringBitmap() = default;
  
  // Add a value to the set
  void Add(uint16_t value) {
    auto it = std::lower_bound(values_.begin(), values_.end(), value);
    if (it == values_.end() || *it != value) {
      values_.insert(it, value);
    }
  }
  
  // Remove a value from the set
  void Remove(uint16_t value) {
    auto it = std::lower_bound(values_.begin(), values_.end(), value);
    if (it != values_.end() && *it == value) {
      values_.erase(it);
    }
  }
  
  // Check if value exists
  bool Contains(uint16_t value) const {
    return std::binary_search(values_.begin(), values_.end(), value);
  }
  
  // Get all values as vector
  std::vector<uint16_t> ToVector() const {
    return values_;
  }
  
  // Get number of values
  size_t Size() const {
    return values_.size();
  }
  
  // Check if empty
  bool Empty() const {
    return values_.empty();
  }
  
  // Clear all values
  void Clear() {
    values_.clear();
  }
  
  // Iterate over all values
  template<typename Func>
  void ForEach(Func&& func) const {
    for (uint16_t v : values_) {
      func(v);
    }
  }
  
  // Memory usage in bytes
  size_t MemoryUsage() const {
    return values_.capacity() * sizeof(uint16_t) + sizeof(*this);
  }
  
  // Serialize to bytes
  std::vector<uint8_t> Serialize() const {
    std::vector<uint8_t> result;
    result.resize(sizeof(uint32_t) + values_.size() * sizeof(uint16_t));
    
    uint32_t size = static_cast<uint32_t>(values_.size());
    memcpy(result.data(), &size, sizeof(size));
    
    if (!values_.empty()) {
      memcpy(result.data() + sizeof(size), values_.data(), 
             values_.size() * sizeof(uint16_t));
    }
    
    return result;
  }
  
  // Deserialize from bytes
  bool Deserialize(const uint8_t* data, size_t len) {
    if (len < sizeof(uint32_t)) return false;
    
    uint32_t size;
    memcpy(&size, data, sizeof(size));
    
    if (len != sizeof(uint32_t) + size * sizeof(uint16_t)) return false;
    
    values_.resize(size);
    if (size > 0) {
      memcpy(values_.data(), data + sizeof(size), size * sizeof(uint16_t));
    }
    
    return true;
  }
  
 private:
  std::vector<uint16_t> values_;  // Sorted array of values
};

}  // namespace cedar

#endif  // FERN_ROARING_BITMAP_H_
