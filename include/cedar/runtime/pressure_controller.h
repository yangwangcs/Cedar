// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_RUNTIME_PRESSURE_CONTROLLER_H_
#define CEDAR_RUNTIME_PRESSURE_CONTROLLER_H_

#include <cstdint>

namespace cedar {

enum class PressureState : uint8_t { kNormal, kSoft, kHard };

// Values are deliberately expressed as monotonic samples so the controller is
// usable with RocksDB properties as well as deterministic unit-test inputs.
struct PressureSample {
  uint64_t arrival_rate = 0;
  uint64_t l0_files = 0;
  uint64_t pending_compaction_bytes = 0;
  uint64_t immutable_memtable_percent = 0;
  uint64_t write_stopped = 0;
  uint64_t background_error = 0;
  uint64_t queue_depth = 0;
  uint64_t bytes_per_txn = 0;
  uint64_t wal_sync_us = 0;
  uint64_t memtable_us = 0;
  uint64_t queue_age_us = 0;
  uint64_t free_disk_bytes = UINT64_MAX;
  uint64_t free_disk_percent = 100;
  // Optional columnar/background signals. A zero interval means that rate
  // fields are not updated for this sample (useful for deterministic probes).
  uint64_t sample_interval_us = 0;
  uint64_t admitted_facts_bytes_per_sec = 0;
  uint64_t completed_background_bytes_per_sec = 0;
  uint64_t storage_debt_bytes = 0;
  uint64_t immutable_memtable_count = 0;
  uint64_t columnar_backlog_buffers = 0;
  uint64_t columnar_builder_bytes = 0;
  uint64_t columnar_flush_pending_bytes = 0;
  // Sum of RocksDB WAL files retained for recovery. This is sampled by Cedar
  // and is a hard admission signal independent of RocksDB's write-stop path.
  uint64_t retained_wal_bytes = 0;
};

struct AdmissionDecision {
  bool admit = true;
  uint32_t max_count = 128;
  uint64_t max_bytes = 2ULL * 1024ULL * 1024ULL;
  uint64_t collection_window_us = 200;
};

class PressureController {
 public:
  PressureController() = default;

  void Observe(const PressureSample& sample);
  AdmissionDecision DecideAdmission(uint64_t request_count,
                                     uint64_t request_bytes,
                                     uint64_t deadline_us) const;

  PressureState state() const { return state_; }
  uint32_t target_count() const { return target_count_; }
  uint64_t target_bytes() const { return target_bytes_; }
  uint64_t collection_window_us() const { return collection_window_us_; }
  uint64_t admitted_rate_ewma() const { return admitted_rate_ewma_; }
  uint64_t background_rate_ewma() const { return background_rate_ewma_; }
  uint64_t projected_debt_bytes() const { return projected_debt_bytes_; }

 private:
  static uint64_t Ewma(uint64_t old_value, uint64_t sample);

  PressureState state_ = PressureState::kNormal;
  uint32_t target_count_ = 128;
  uint64_t target_bytes_ = 2ULL * 1024ULL * 1024ULL;
  uint64_t collection_window_us_ = 200;
  uint32_t busy_observations_ = 0;
  uint64_t arrival_rate_ewma_ = 0;
  uint64_t bytes_per_txn_ewma_ = 0;
  uint64_t wal_sync_us_ewma_ = 0;
  uint64_t memtable_us_ewma_ = 0;
  uint64_t queue_age_us_ewma_ = 0;
  uint64_t admitted_rate_ewma_ = 0;
  uint64_t background_rate_ewma_ = 0;
  uint64_t projected_debt_bytes_ = 0;
  uint32_t deficit_observations_ = 0;
  uint64_t deficit_duration_us_ = 0;
  uint32_t recovery_observations_ = 0;
};

}  // namespace cedar

#endif  // CEDAR_RUNTIME_PRESSURE_CONTROLLER_H_
