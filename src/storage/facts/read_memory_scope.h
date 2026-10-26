#ifndef CEDAR_STORAGE_FACTS_READ_MEMORY_SCOPE_H_
#define CEDAR_STORAGE_FACTS_READ_MEMORY_SCOPE_H_

#include "cedar/fact/read_spec.h"

namespace cedar::internal {

struct FactReadMemoryLease {
  FactReadMemoryLease(std::shared_ptr<FactReadMemory> budget, uint64_t charge)
      : memory(std::move(budget)), bytes(charge) {}
  ~FactReadMemoryLease() { memory->Release(bytes); }
  std::shared_ptr<FactReadMemory> memory;
  uint64_t bytes;
};

class FactReadMemoryScope {
 public:
  explicit FactReadMemoryScope(FactReadMemory* memory) : memory_(memory) {}
  ~FactReadMemoryScope() { Reset(); }
  FactReadMemoryScope(const FactReadMemoryScope&) = delete;
  FactReadMemoryScope& operator=(const FactReadMemoryScope&) = delete;

  Status Reserve(uint64_t bytes) {
    if (!memory_) return Status::OK();
    if (bytes > UINT64_MAX - bytes_) return Overflow();
    const Status status = memory_->Reserve(bytes);
    if (status.ok()) bytes_ += bytes;
    return status;
  }
  Status ReserveElements(uint64_t count, uint64_t size) {
    if (size != 0 && count > UINT64_MAX / size) return Overflow();
    return Reserve(count * size);
  }
  StatusOr<std::shared_ptr<const void>> Retain(
      std::shared_ptr<FactReadMemory> memory) {
    if (!memory_) return std::shared_ptr<const void>{};
    const Status status = Reserve(sizeof(FactReadMemoryLease) + 64);
    if (!status.ok()) return status;
    auto lease = std::make_shared<FactReadMemoryLease>(std::move(memory), bytes_);
    bytes_ = 0;
    return std::shared_ptr<const void>(std::move(lease));
  }
  void Release(uint64_t bytes) {
    if (!memory_) return;
    memory_->Release(bytes);
    bytes_ -= bytes;
  }
  void Reset() {
    if (memory_) memory_->Release(bytes_);
    bytes_ = 0;
  }

 private:
  static Status Overflow() {
    return Status::ResourceExhausted("canonical reader", "read memory overflow");
  }
  FactReadMemory* memory_;
  uint64_t bytes_ = 0;
};

inline uint64_t FactValueHeapBytes(const std::optional<Value>& value) {
  if (!value) return 0;
  const auto* text = std::get_if<std::string>(&value->data());
  return text ? text->capacity() + 1 : 0;
}

template <typename T>
Status ReserveFactAppend(std::vector<T>* values, FactReadMemoryScope* memory) {
  if (values->size() < values->capacity()) return Status::OK();
  const uint64_t previous = values->capacity();
  if (previous > values->max_size() / 2) {
    return Status::ResourceExhausted("canonical reader", "read vector overflow");
  }
  const uint64_t next = previous == 0 ? 1 : previous * 2;
  // During reallocation both old and new buffers are live.
  const Status status = memory->ReserveElements(next, sizeof(T));
  if (!status.ok()) return status;
  values->reserve(next);
  memory->Release(previous * sizeof(T));
  return Status::OK();
}

}  // namespace cedar::internal
#endif
