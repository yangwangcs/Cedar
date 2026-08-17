// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_INDEX_MEMTABLE_DELTA_INDEX_H_
#define CEDAR_INDEX_MEMTABLE_DELTA_INDEX_H_

#include <algorithm>
#include <map>
#include <optional>
#include <set>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "cedar/index/canonical_value.h"
#include "cedar/index/index_definition.h"
#include "cedar/storage/temporal_event.h"
#include "cedar/tcypher/runtime/cancellation.h"

namespace cedar {

class MemtableDeltaIndex {
 public:
  using CanonicalKey = std::tuple<uint8_t, uint8_t, std::string>;

  struct CanonicalKeyLess {
    using is_transparent = void;

    bool operator()(const CanonicalKey& left,
                    const CanonicalKey& right) const {
      return left < right;
    }
    bool operator()(const CanonicalKey& left,
                    const IndexCanonicalValue& right) const {
      if (std::get<0>(left) != static_cast<uint8_t>(right.type)) {
        return std::get<0>(left) < static_cast<uint8_t>(right.type);
      }
      if (std::get<1>(left) != static_cast<uint8_t>(right.kind)) {
        return std::get<1>(left) < static_cast<uint8_t>(right.kind);
      }
      return std::get<2>(left) < right.bytes;
    }
    bool operator()(const IndexCanonicalValue& left,
                    const CanonicalKey& right) const {
      if (static_cast<uint8_t>(left.type) != std::get<0>(right)) {
        return static_cast<uint8_t>(left.type) < std::get<0>(right);
      }
      if (static_cast<uint8_t>(left.kind) != std::get<1>(right)) {
        return static_cast<uint8_t>(left.kind) < std::get<1>(right);
      }
      return left.bytes < std::get<2>(right);
    }
  };

  struct CanonicalValueView {
    PhysicalType type;
    std::string_view bytes;
    IndexCanonicalKind kind = IndexCanonicalKind::kInline;
  };

  struct CanonicalValueLess {
    using is_transparent = void;

    uint64_t* comparison_counter = nullptr;
    const bool* count_comparisons = nullptr;

    void Count() const {
      if (comparison_counter != nullptr && count_comparisons != nullptr &&
          *count_comparisons) {
        ++*comparison_counter;
      }
    }
    bool operator()(const IndexCanonicalValue& left,
                    const IndexCanonicalValue& right) const {
      Count();
      return CompareIndexCanonicalValues(left, right) < 0;
    }
    bool operator()(const IndexCanonicalValue& left,
                    CanonicalValueView right) const {
      Count();
      if (left.type != right.type) {
        return static_cast<uint8_t>(left.type) <
            static_cast<uint8_t>(right.type);
      }
      if (left.kind != right.kind) {
        return static_cast<uint8_t>(left.kind) <
            static_cast<uint8_t>(right.kind);
      }
      return std::string_view(left.bytes) < right.bytes;
    }
    bool operator()(CanonicalValueView left,
                    const IndexCanonicalValue& right) const {
      Count();
      if (left.type != right.type) {
        return static_cast<uint8_t>(left.type) <
            static_cast<uint8_t>(right.type);
      }
      if (left.kind != right.kind) {
        return static_cast<uint8_t>(left.kind) <
            static_cast<uint8_t>(right.kind);
      }
      return left.bytes < std::string_view(right.bytes);
    }
  };

  using LookupValueSet = std::set<IndexCanonicalValue, CanonicalValueLess>;

  using ValueMap =
      std::map<CanonicalKey, std::vector<uint64_t>, CanonicalKeyLess>;
  enum class LookupKind : uint8_t { kEqualityOrIn, kRange, kPrefix };

  class LookupCursor {
   public:
    LookupCursor() = default;
    LookupCursor(LookupCursor&&) noexcept = default;
    LookupCursor& operator=(LookupCursor&&) noexcept = default;
    LookupCursor(const LookupCursor&) = delete;
    LookupCursor& operator=(const LookupCursor&) = delete;

    bool done() const { return done_; }

    Status Advance(uint32_t max_items,
                   const QueryCancellation* cancellation,
                   std::vector<uint64_t>* ordinals,
                   uint32_t* processed) {
      if (max_items == 0 || ordinals == nullptr || processed == nullptr) {
        return Status::InvalidArgument(
            "memtable delta index", "invalid lookup cursor output quantum");
      }
      ordinals->clear();
      *processed = 0;
      while (!done_ && *processed < max_items) {
        if (cancellation != nullptr && cancellation->IsCancelled()) {
          return Status::QueryCancelled(
              "memtable delta index", "lookup cursor cancelled");
        }
        if (active_postings_ != nullptr) {
          ordinals->push_back((*active_postings_)[posting_offset_++]);
          ++*processed;
          if (posting_offset_ == active_postings_->size()) {
            active_postings_ = nullptr;
            posting_offset_ = 0;
          }
          continue;
        }
        if (kind_ == LookupKind::kEqualityOrIn) {
          if (keys_ == nullptr || key_iterator_ == keys_->end()) {
            done_ = true;
            continue;
          }
          const auto found = values_->find(*key_iterator_++);
          ++*processed;
          if (found != values_->end() && !found->second.empty()) {
            active_postings_ = &found->second;
          }
          continue;
        }
        if (iterator_ == values_->end()) {
          done_ = true;
          continue;
        }
        if (kind_ == LookupKind::kRange && range_type_.has_value() &&
            (std::get<0>(iterator_->first) !=
                 static_cast<uint8_t>(*range_type_) ||
             std::get<1>(iterator_->first) !=
                 static_cast<uint8_t>(IndexCanonicalKind::kInline))) {
          done_ = true;
          continue;
        }
        if (kind_ == LookupKind::kPrefix) {
          const IndexCanonicalValue& prefix = *keys_->begin();
          if (std::get<0>(iterator_->first) !=
                  static_cast<uint8_t>(prefix.type) ||
              std::get<1>(iterator_->first) !=
                  static_cast<uint8_t>(IndexCanonicalKind::kInline) ||
              std::get<2>(iterator_->first).compare(
                  0, prefix.bytes.size(), prefix.bytes) != 0) {
            done_ = true;
            continue;
          }
        } else if (upper_ != nullptr) {
          const CanonicalKeyLess less;
          const bool after_upper = less(*upper_, iterator_->first);
          const bool equal_upper =
              !after_upper && !less(iterator_->first, *upper_);
          if (after_upper || (equal_upper && !upper_inclusive_)) {
            done_ = true;
            continue;
          }
        }
        active_postings_ = &iterator_->second;
        ++iterator_;
        ++*processed;
      }
      return Status::OK();
    }

   private:
    friend class MemtableDeltaIndex;
    LookupCursor(
        const ValueMap* values, LookupKind kind,
        const LookupValueSet* keys,
        const IndexCanonicalValue* lower, bool lower_inclusive,
        const IndexCanonicalValue* upper, bool upper_inclusive)
        : values_(values), kind_(kind), keys_(keys), upper_(upper),
          upper_inclusive_(upper_inclusive),
          iterator_(values == nullptr ? ValueMap::const_iterator{}
                                      : values->end()),
          key_iterator_(keys == nullptr ? LookupValueSet::const_iterator{}
                                        : keys->begin()) {
      if (values_ == nullptr) {
        done_ = true;
        return;
      }
      if (kind_ == LookupKind::kRange) {
        range_type_ = lower != nullptr
            ? std::optional<PhysicalType>{lower->type}
            : upper != nullptr ? std::optional<PhysicalType>{upper->type}
                               : std::nullopt;
        if (lower != nullptr) {
          iterator_ = values_->lower_bound(*lower);
        } else if (upper != nullptr) {
          iterator_ = values_->lower_bound(IndexCanonicalValue{
              upper->type, "", IndexCanonicalKind::kInline});
        } else {
          iterator_ = values_->begin();
        }
        if (lower != nullptr && !lower_inclusive &&
            iterator_ != values_->end()) {
          const CanonicalKeyLess less;
          if (!less(iterator_->first, *lower) &&
              !less(*lower, iterator_->first)) {
            ++iterator_;
          }
        }
      } else if (kind_ == LookupKind::kPrefix) {
        iterator_ = values_->lower_bound(*keys_->begin());
      }
    }

    const ValueMap* values_ = nullptr;
    LookupKind kind_ = LookupKind::kEqualityOrIn;
    const LookupValueSet* keys_ = nullptr;
    const IndexCanonicalValue* upper_ = nullptr;
    bool upper_inclusive_ = true;
    std::optional<PhysicalType> range_type_;
    ValueMap::const_iterator iterator_;
    LookupValueSet::const_iterator key_iterator_;
    const std::vector<uint64_t>* active_postings_ = nullptr;
    size_t posting_offset_ = 0;
    bool done_ = false;
  };

  void Reset(uint64_t source_generation) {
    source_generation_ = source_generation;
    values_.clear();
  }

  Status Add(const IndexDefinition& definition, uint64_t ordinal,
             const TemporalEvent& event) {
    const LogicalKey& key = event.logical_key();
    if (event.is_delete() ||
        key.kind() != LogicalKeyKind::kProperty ||
        key.entity_type() != definition.entity_type ||
        key.column_id() != definition.column_id ||
        event.schema_epoch() != definition.schema_epoch) {
      return Status::OK();
    }
    if (event.is_blob_reference()) {
      values_[Key(EncodeIndexBlobHash(*event.blob_ref()))].push_back(ordinal);
      return Status::OK();
    }
    const auto canonical = EncodeIndexCanonicalValue(event.value());
    if (!canonical.ok()) return canonical.status();
    values_[Key(canonical.ValueOrDie())].push_back(ordinal);
    return Status::OK();
  }

  Status Rebuild(const IndexDefinition& definition, uint64_t source_generation,
                 const std::vector<TemporalEvent>& events) {
    Reset(source_generation);
    for (uint64_t ordinal = 0; ordinal < events.size(); ++ordinal) {
      const Status added = Add(definition, ordinal, events[ordinal]);
      if (!added.ok()) return added;
    }
    return Status::OK();
  }

  StatusOr<std::vector<uint64_t>> Lookup(uint64_t source_generation,
                                         const Value& value) const {
    const Status source = ValidateSource(source_generation);
    if (!source.ok()) return source;
    const auto canonical = EncodeIndexCanonicalValue(value);
    if (!canonical.ok()) return canonical.status();
    std::vector<uint64_t> ordinals;
    const auto found = values_.find(Key(canonical.ValueOrDie()));
    if (found != values_.end()) ordinals = found->second;
    if (value.type() == PhysicalType::kString ||
        value.type() == PhysicalType::kBinary) {
      const auto hash = EncodeIndexBlobHash(value);
      if (!hash.ok()) return hash.status();
      const auto hashed = values_.find(Key(hash.ValueOrDie()));
      if (hashed != values_.end()) {
        ordinals.insert(ordinals.end(), hashed->second.begin(),
                        hashed->second.end());
      }
    }
    std::sort(ordinals.begin(), ordinals.end());
    ordinals.erase(std::unique(ordinals.begin(), ordinals.end()),
                   ordinals.end());
    return ordinals;
  }

  StatusOr<std::vector<uint64_t>> LookupRange(
      uint64_t source_generation, const std::optional<Value>& lower,
      bool lower_inclusive, const std::optional<Value>& upper,
      bool upper_inclusive) const {
    const Status source = ValidateSource(source_generation);
    if (!source.ok()) return source;
    std::optional<IndexCanonicalValue> lower_value;
    std::optional<IndexCanonicalValue> upper_value;
    if (lower.has_value()) {
      const auto canonical = EncodeIndexCanonicalValue(*lower);
      if (!canonical.ok()) return canonical.status();
      lower_value = canonical.ValueOrDie();
    }
    if (upper.has_value()) {
      const auto canonical = EncodeIndexCanonicalValue(*upper);
      if (!canonical.ok()) return canonical.status();
      upper_value = canonical.ValueOrDie();
    }
    if (lower_value.has_value() && upper_value.has_value() &&
        (lower_value->type != upper_value->type ||
         CompareIndexCanonicalValues(*lower_value, *upper_value) > 0)) {
      return Status::InvalidArgument("memtable delta index", "invalid range bounds");
    }
    std::vector<uint64_t> ordinals;
    for (const auto& entry : values_) {
      if (std::get<1>(entry.first) !=
          static_cast<uint8_t>(IndexCanonicalKind::kInline)) {
        continue;
      }
      const IndexCanonicalValue candidate{
          static_cast<PhysicalType>(std::get<0>(entry.first)),
          std::get<2>(entry.first), IndexCanonicalKind::kInline};
      if (lower_value.has_value()) {
        const int comparison = CompareIndexCanonicalValues(candidate, *lower_value);
        if (candidate.type != lower_value->type || comparison < 0 ||
            (comparison == 0 && !lower_inclusive)) {
          continue;
        }
      }
      if (upper_value.has_value()) {
        const int comparison = CompareIndexCanonicalValues(candidate, *upper_value);
        if (candidate.type != upper_value->type || comparison > 0 ||
            (comparison == 0 && !upper_inclusive)) {
          continue;
        }
      }
      ordinals.insert(ordinals.end(), entry.second.begin(), entry.second.end());
    }
    return ordinals;
  }

  StatusOr<std::vector<uint64_t>> LookupPrefix(
      uint64_t source_generation, const Value& prefix) const {
    const Status source = ValidateSource(source_generation);
    if (!source.ok()) return source;
    if (prefix.type() != PhysicalType::kString &&
        prefix.type() != PhysicalType::kBinary) {
      return Status::SchemaMismatch(
          "memtable delta index", "prefix requires string or binary value");
    }
    const auto canonical = EncodeIndexCanonicalValue(prefix);
    if (!canonical.ok()) return canonical.status();
    std::vector<uint64_t> ordinals;
    for (const auto& entry : values_) {
      if (std::get<0>(entry.first) !=
              static_cast<uint8_t>(canonical.ValueOrDie().type) ||
          std::get<1>(entry.first) !=
              static_cast<uint8_t>(IndexCanonicalKind::kInline) ||
          std::get<2>(entry.first).compare(
              0, canonical.ValueOrDie().bytes.size(),
              canonical.ValueOrDie().bytes) != 0) {
        continue;
      }
      ordinals.insert(ordinals.end(), entry.second.begin(), entry.second.end());
    }
    return ordinals;
  }

  StatusOr<LookupCursor> OpenLookupCursor(
      uint64_t source_generation, LookupKind kind,
      const LookupValueSet* keys,
      const IndexCanonicalValue* lower, bool lower_inclusive,
      const IndexCanonicalValue* upper, bool upper_inclusive) const {
    const Status source = ValidateSource(source_generation);
    if (!source.ok()) return source;
    if (kind == LookupKind::kEqualityOrIn &&
        (keys == nullptr || keys->empty())) {
      return Status::InvalidArgument(
          "memtable delta index", "equality lookup keys are missing");
    }
    if (kind == LookupKind::kPrefix &&
        (keys == nullptr || keys->size() != 1 ||
         (keys->begin()->type != PhysicalType::kString &&
          keys->begin()->type != PhysicalType::kBinary))) {
      return Status::InvalidArgument(
          "memtable delta index", "prefix lookup key is invalid");
    }
    if (lower != nullptr && upper != nullptr &&
        (lower->type != upper->type ||
         CompareIndexCanonicalValues(*lower, *upper) > 0)) {
      return Status::InvalidArgument(
          "memtable delta index", "invalid range bounds");
    }
    return LookupCursor(&values_, kind, keys, lower, lower_inclusive,
                        upper, upper_inclusive);
  }

 private:
  static CanonicalKey Key(const IndexCanonicalValue& value) {
    return {static_cast<uint8_t>(value.type),
            static_cast<uint8_t>(value.kind), value.bytes};
  }

  Status ValidateSource(uint64_t source_generation) const {
    return source_generation == source_generation_
        ? Status::OK()
        : Status::NotFound("memtable delta index", "source is not indexed");
  }

  uint64_t source_generation_ = 0;
  ValueMap values_;
};

}  // namespace cedar

#endif  // CEDAR_INDEX_MEMTABLE_DELTA_INDEX_H_
