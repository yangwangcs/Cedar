#include "query/resource/query_resource_pool.h"

#include <string>
#include <array>
#include <limits>
#include <utility>

namespace cedar::internal {
namespace {
uint64_t BudgetDim(const QueryBudget& b, ResourceDimension d) {
  switch (d) {
    case ResourceDimension::kMemory: return b.memory_bytes;
    case ResourceDimension::kScratch: return b.scratch_bytes;
    case ResourceDimension::kReadBytes: return b.read_bytes;
    case ResourceDimension::kPrefetchBytes: return b.prefetch_bytes;
    case ResourceDimension::kDecodedRows: return b.decoded_rows;
    case ResourceDimension::kOutputRows: return b.output_rows;
    case ResourceDimension::kOutputBytes: return b.output_bytes;
    case ResourceDimension::kIntervalFragments: return b.interval_fragments;
    case ResourceDimension::kGraphLabels: return b.graph_labels;
    case ResourceDimension::kVisitedVertices: return b.visited_vertices;
    case ResourceDimension::kCpuMicros: return b.cpu_us;
    case ResourceDimension::kCount: break;
  }
  return 0;
}
uint64_t PoolDim(const QueryResourcePoolOptions& o, ResourceDimension d) {
  switch (d) {
    case ResourceDimension::kMemory: return o.memory_bytes;
    case ResourceDimension::kScratch: return o.scratch_bytes;
    case ResourceDimension::kReadBytes: return o.read_bytes;
    case ResourceDimension::kPrefetchBytes: return o.prefetch_bytes;
    case ResourceDimension::kDecodedRows: return o.decoded_rows;
    case ResourceDimension::kOutputRows: return o.output_rows;
    case ResourceDimension::kOutputBytes: return o.output_bytes;
    case ResourceDimension::kIntervalFragments: return o.interval_fragments;
    case ResourceDimension::kGraphLabels: return o.graph_labels;
    case ResourceDimension::kVisitedVertices: return o.visited_vertices;
    case ResourceDimension::kCpuMicros: return o.cpu_us;
    case ResourceDimension::kCount: break;
  }
  return 0;
}
const char* Name(ResourceDimension d) {
  static constexpr const char* kNames[] = {
      "memory_bytes", "scratch_bytes", "read_bytes", "prefetch_bytes",
      "decoded_rows", "output_rows", "output_bytes", "interval_fragments",
      "graph_labels", "visited_vertices", "cpu_us"};
  const size_t index = static_cast<size_t>(d);
  return index < sizeof(kNames) / sizeof(kNames[0]) ? kNames[index] : "resource";
}
}  // namespace

QueryResourcePool::QueryResourcePool(QueryResourcePoolOptions options)
    : options_(std::move(options)),
      io_bytes_(std::make_shared<std::atomic<uint64_t>>(0)),
      admitted_memory_(std::make_shared<std::atomic<uint64_t>>(0)),
      admitted_workers_(std::make_shared<std::atomic<uint32_t>>(0)) {}

IoPermit::IoPermit(IoPermit&& other) noexcept
    : bytes_(other.bytes_), aggregate_(std::move(other.aggregate_)) {
  other.bytes_ = 0;
}
IoPermit& IoPermit::operator=(IoPermit&& other) noexcept {
  if (this != &other) {
    if (aggregate_ && bytes_ != 0) {
      aggregate_->fetch_sub(bytes_, std::memory_order_acq_rel);
    }
    bytes_ = other.bytes_;
    aggregate_ = std::move(other.aggregate_);
    other.bytes_ = 0;
  }
  return *this;
}
IoPermit::~IoPermit() {
  if (aggregate_ && bytes_ != 0) {
    aggregate_->fetch_sub(bytes_, std::memory_order_acq_rel);
  }
}

StatusOr<IoPermit> QueryResourcePool::AcquireIo(QueryWorkClass work_class,
                                                uint64_t bytes) {
  if (bytes == 0) return IoPermit();
  if (options_.wal_sync_critical != nullptr &&
      options_.wal_sync_critical->load(std::memory_order_acquire) &&
      (work_class == QueryWorkClass::kAnalytical ||
       work_class == QueryWorkClass::kProjection)) {
    return Status::ResourceExhausted("query", "wal_sync_critical io permit waits");
  }
  if (options_.read_bytes_per_second != 0 && bytes > options_.read_bytes_per_second) {
    return Status::ResourceExhausted("query", "read_bytes_per_second rate exhausted");
  }
  uint64_t current = io_bytes_->load(std::memory_order_acquire);
  for (;;) {
    if (bytes > std::numeric_limits<uint64_t>::max() - current) {
      return Status::ResourceExhausted("query", "read_bytes accounting overflow");
    }
    if (options_.read_bytes != 0 &&
        (bytes > options_.read_bytes - std::min(options_.read_bytes, current))) {
      return Status::ResourceExhausted("query", "read_bytes budget exhausted");
    }
    if (io_bytes_->compare_exchange_weak(current, current + bytes,
                                         std::memory_order_acq_rel)) break;
  }
  IoPermit permit(bytes);
  permit.aggregate_ = io_bytes_;
  return permit;
}

StatusOr<QueryReservation> QueryResourcePool::Admit(const QueryBudget& budget) {
  if (budget.max_parallelism == 0 || budget.max_parallelism > options_.max_parallelism) {
    return Status::ResourceExhausted("query", "max_parallelism budget exhausted");
  }
  uint64_t current_memory = admitted_memory_->load(std::memory_order_acquire);
  for (;;) {
    if (budget.memory_bytes > options_.memory_bytes -
                             std::min(options_.memory_bytes, current_memory)) {
      return Status::ResourceExhausted("query", "memory_bytes pool admission exhausted");
    }
    if (admitted_memory_->compare_exchange_weak(
            current_memory, current_memory + budget.memory_bytes,
            std::memory_order_acq_rel)) break;
  }
  uint32_t current_workers = admitted_workers_->load(std::memory_order_acquire);
  for (;;) {
    if (budget.max_parallelism > options_.max_parallelism -
                                  std::min(options_.max_parallelism, current_workers)) {
      admitted_memory_->fetch_sub(budget.memory_bytes, std::memory_order_acq_rel);
      return Status::ResourceExhausted("query", "parallelism pool admission exhausted");
    }
    if (admitted_workers_->compare_exchange_weak(
            current_workers, current_workers + budget.max_parallelism,
            std::memory_order_acq_rel)) break;
  }
  std::array<uint64_t, static_cast<size_t>(ResourceDimension::kCount)> limits{};
  for (size_t i = 0; i < static_cast<size_t>(ResourceDimension::kCount); ++i) {
    const auto dimension = static_cast<ResourceDimension>(i);
    const uint64_t requested = BudgetDim(budget, dimension);
    if (requested > PoolDim(options_, dimension)) {
      admitted_workers_->fetch_sub(budget.max_parallelism, std::memory_order_acq_rel);
      admitted_memory_->fetch_sub(budget.memory_bytes, std::memory_order_acq_rel);
      return Status::ResourceExhausted("query", std::string(Name(dimension)) + " budget exhausted");
    }
    limits[i] = requested;
  }
  QueryReservation reservation(limits);
  reservation.AttachPoolAdmission(admitted_memory_, admitted_workers_,
                                  budget.memory_bytes, budget.max_parallelism);
  return reservation;
}
}  // namespace cedar::internal
