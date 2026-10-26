#ifndef CEDAR_QUERY_RESOURCE_QUERY_READ_MEMORY_H_
#define CEDAR_QUERY_RESOURCE_QUERY_READ_MEMORY_H_

#include "cedar/fact/canonical_reader.h"
#include "query/runtime/relational.h"

namespace cedar::internal {

class QueryReadMemory final : public FactReadMemory {
 public:
  explicit QueryReadMemory(QueryReservation query) : query_(std::move(query)) {}
  Status Reserve(uint64_t bytes) override { return query_.ReserveMemory(bytes); }
  void Release(uint64_t bytes) override { query_.ReleaseMemory(bytes); }
  std::shared_ptr<const void> Pin(const void* owner, uint64_t bytes) override {
    auto handle = query_.TryRetain(128);
    if (!handle) return {};
    auto payload = query_.TryRetainOwner(owner, bytes);
    if (!payload) return {};
    struct PinLease {
      std::shared_ptr<QueryReservationLease> payload;
      std::shared_ptr<QueryReservationLease> handle;
    };
    return std::make_shared<PinLease>(PinLease{std::move(payload), std::move(handle)});
  }
 private:
  QueryReservation query_;
};

// Budget is attached per invocation; the snapshot reader remains immutable.
class BudgetedCanonicalReader final : public CanonicalFactReader {
 public:
  BudgetedCanonicalReader(const CanonicalFactReader& reader, QueryReservation* query)
      : reader_(reader), memory_(query ? std::make_shared<QueryReadMemory>(*query) : nullptr) {}
  StatusOr<std::optional<FactEvent>> ReadStateAt(
      const FactReadSpec& spec, ValidTime time, CommitSeq seq) const override {
    auto bounded = spec;
    if (memory_) bounded.read_memory = memory_;
    return reader_.ReadStateAt(bounded, time, seq);
  }
  Status ReadStateRows(const CanonicalStateReadSpec& spec,
      const CanonicalStateBatchVisitor& visitor) const override {
    auto bounded = spec;
    if (memory_) bounded.facts.read_memory = memory_;
    return reader_.ReadStateRows(bounded, visitor);
  }
  Status ReadEvents(const FactReadSpec& spec,
      const CanonicalFactBatchVisitor& visitor) const override {
    auto bounded = spec;
    if (memory_) bounded.read_memory = memory_;
    return reader_.ReadEvents(bounded, visitor);
  }
  Status ReadColumnar(const FactReadSpec& spec,
      const CanonicalColumnarBatchVisitor& visitor) const override {
    auto bounded = spec;
    if (memory_) bounded.read_memory = memory_;
    return reader_.ReadColumnar(bounded, visitor);
  }
  StatusOr<FactEventBatch> ReadExact(
      const std::vector<std::string>& keys,
      std::shared_ptr<FactReadMemory> read_memory = {}) const override {
    return reader_.ReadExact(keys, memory_ ? memory_ : std::move(read_memory));
  }
 private:
  const CanonicalFactReader& reader_;
  std::shared_ptr<FactReadMemory> memory_;
};

}  // namespace cedar::internal
#endif
