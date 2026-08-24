#ifndef CEDAR_QUERY_RESOURCE_QUERY_RESOURCE_POOL_H_
#define CEDAR_QUERY_RESOURCE_QUERY_RESOURCE_POOL_H_

#include <atomic>
#include <array>
#include <cstdint>
#include <filesystem>
#include <memory>

#include "cedar/core/status.h"
#include "cedar/query/types.h"
#include "query/runtime/relational.h"

namespace cedar::internal {

struct QueryResourcePoolOptions {
  uint64_t memory_bytes = 0;
  uint64_t scratch_bytes = 0;
  uint64_t read_bytes = 0;
  uint64_t prefetch_bytes = 0;
  uint64_t decoded_rows = 0;
  uint64_t output_rows = 0;
  uint64_t output_bytes = 0;
  uint64_t interval_fragments = 0;
  uint64_t graph_labels = 0;
  uint64_t visited_vertices = 0;
  uint64_t cpu_us = 0;
  uint64_t scratch_free_space_reserve_bytes = 0;
  uint64_t read_bytes_per_second = 0;
  uint64_t scratch_bytes_per_second = 0;
  uint32_t max_parallelism = 1;
  uint32_t query_workers = 4;
  uint32_t reserved_interactive_workers = 1;
  std::atomic<bool>* wal_sync_critical = nullptr;
  std::filesystem::path scratch_root;
  std::string scratch_instance = "active";
};

enum class QueryWorkClass : uint8_t {
  kDurability = 0,
  kInteractive,
  kMaintenance,
  kAnalytical,
  kProjection,
};

class IoPermit {
 public:
  IoPermit() = default;
  IoPermit(const IoPermit&) = delete;
  IoPermit& operator=(const IoPermit&) = delete;
  IoPermit(IoPermit&& other) noexcept;
  IoPermit& operator=(IoPermit&& other) noexcept;
  ~IoPermit();
  bool valid() const { return bytes_ != 0; }
  uint64_t bytes() const { return bytes_; }

 private:
  explicit IoPermit(uint64_t bytes) : bytes_(bytes) {}
  uint64_t bytes_ = 0;
  std::shared_ptr<std::atomic<uint64_t>> aggregate_;
  friend class QueryResourcePool;
};

class QueryResourcePool {
 public:
  explicit QueryResourcePool(QueryResourcePoolOptions options);
  StatusOr<QueryReservation> Admit(
      const QueryBudget& budget,
      QueryExecutionMode mode = QueryExecutionMode::kAuto);
  StatusOr<IoPermit> AcquireIo(QueryWorkClass work_class, uint64_t bytes);
  const QueryResourcePoolOptions& options() const { return options_; }

 private:
  QueryResourcePoolOptions options_;
  std::shared_ptr<std::atomic<uint64_t>> io_bytes_;
  std::array<std::shared_ptr<std::atomic<uint64_t>>,
             static_cast<size_t>(ResourceDimension::kCount)>
      admitted_dimensions_;
  std::shared_ptr<std::atomic<uint32_t>> admitted_workers_;
};

}  // namespace cedar::internal

#endif
