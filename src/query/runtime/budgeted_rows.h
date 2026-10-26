#ifndef CEDAR_QUERY_RUNTIME_BUDGETED_ROWS_H_
#define CEDAR_QUERY_RUNTIME_BUDGETED_ROWS_H_

#include <algorithm>

#include "storage/facts/read_memory_scope.h"

namespace cedar::internal {

// The collection keeps its allocation lease until all row payloads are gone.
template <typename T>
class BudgetedRows {
 public:
  BudgetedRows() = default;
  BudgetedRows(std::vector<T> rows, std::shared_ptr<const void> lease)
      : lease_(std::move(lease)), rows_(std::move(rows)) {}
  BudgetedRows(const BudgetedRows&) = delete;
  BudgetedRows& operator=(const BudgetedRows&) = delete;
  BudgetedRows(BudgetedRows&&) noexcept = default;
  BudgetedRows& operator=(BudgetedRows&& other) noexcept {
    if (this != &other) {
      std::vector<T>().swap(rows_);
      lease_.reset();
      rows_ = std::move(other.rows_);
      lease_ = std::move(other.lease_);
    }
    return *this;
  }
  operator const std::vector<T>&() const { return rows_; }
  auto begin() { return rows_.begin(); }
  auto end() { return rows_.end(); }
  auto begin() const { return rows_.begin(); }
  auto end() const { return rows_.end(); }
  size_t size() const { return rows_.size(); }
  bool empty() const { return rows_.empty(); }
  T& front() { return rows_.front(); }
  const T& front() const { return rows_.front(); }
  T& back() { return rows_.back(); }
  const T& back() const { return rows_.back(); }
  T& operator[](size_t index) { return rows_[index]; }
  const T& operator[](size_t index) const { return rows_[index]; }
  template <typename Predicate>
  void EraseIf(Predicate predicate) { std::erase_if(rows_, std::move(predicate)); }

 private:
  std::shared_ptr<const void> lease_;
  std::vector<T> rows_;
};

template <typename T>
StatusOr<BudgetedRows<T>> RetainRows(std::vector<T> rows,
                                   FactReadMemoryScope* scope,
                                   std::shared_ptr<FactReadMemory> memory) {
  auto retained = scope->Retain(std::move(memory));
  if (!retained.ok()) return retained.status();
  return BudgetedRows<T>{std::move(rows), retained.ConsumeValueOrDie()};
}

}  // namespace cedar::internal
#endif
