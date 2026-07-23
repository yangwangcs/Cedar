// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_QUERY_SPILL_H_
#define CEDAR_TCYPHER_RUNTIME_QUERY_SPILL_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/runtime/resource_profile.h"
#include "cedar/tcypher/runtime/cancellation.h"
#include "cedar/tcypher/runtime/query_result.h"

namespace cedar {

// Ephemeral, query-private ResultBatch storage. Spill files are never added to
// a Manifest and are removed when their query state is destroyed.
class QuerySpillFile {
 public:
  explicit QuerySpillFile(std::string directory,
                          std::shared_ptr<QueryCancellation> cancellation = nullptr,
                          std::shared_ptr<ResourceGovernorExtension> resources = nullptr,
                          std::shared_ptr<QueryMemoryAccount> memory_account = nullptr,
                          std::function<void(uint64_t)> write_observer = {});
  ~QuerySpillFile();

  QuerySpillFile(const QuerySpillFile&) = delete;
  QuerySpillFile& operator=(const QuerySpillFile&) = delete;

  Status Open();
  Status AppendRecord(const std::string& record);
  Status Append(const ResultBatch& batch);
  Status Seal();
  Status Rewind();
  Status NextRecord(std::string* record);
  Status Next(ResultBatch* batch);
  Status Close();

  const std::string& path() const { return path_; }
  uint64_t bytes_written() const { return bytes_written_; }

 private:
  Status CheckCancelled();
  void ReleaseReadBuffer();

  std::string directory_;
  std::shared_ptr<QueryCancellation> cancellation_;
  std::shared_ptr<ResourceGovernorExtension> resources_;
  std::shared_ptr<QueryMemoryAccount> memory_account_;
  std::function<void(uint64_t)> write_observer_;
  ResourceLease descriptor_lease_;
  ResourceLease temporary_lease_;
  std::string path_;
  int fd_ = -1;
  uint64_t bytes_written_ = 0;
  uint64_t read_buffer_reserved_bytes_ = 0;
  bool opened_ = false;
  bool reading_ = false;
};

class SpillResultStream final : public QueryResultStream {
 public:
  explicit SpillResultStream(std::unique_ptr<QuerySpillFile> spill)
      : spill_(std::move(spill)) {}

  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override { return terminal_status_; }

 private:
  std::unique_ptr<QuerySpillFile> spill_;
  Status terminal_status_ = Status::OK();
  bool rewound_ = false;
};

// A bounded collection of query-private spill partitions. Partitions are
// created lazily so empty radix buckets do not consume file descriptors.
class PartitionedSpillSet {
 public:
  PartitionedSpillSet(
      std::string directory, uint32_t partition_count,
      std::shared_ptr<QueryCancellation> cancellation = nullptr,
      std::shared_ptr<ResourceGovernorExtension> resources = nullptr,
      std::shared_ptr<QueryMemoryAccount> memory_account = nullptr,
      std::function<void(uint64_t)> write_observer = {})
      : directory_(std::move(directory)), partition_count_(partition_count),
        cancellation_(std::move(cancellation)), resources_(std::move(resources)),
        memory_account_(std::move(memory_account)),
        write_observer_(std::move(write_observer)) {}
  ~PartitionedSpillSet();

  PartitionedSpillSet(const PartitionedSpillSet&) = delete;
  PartitionedSpillSet& operator=(const PartitionedSpillSet&) = delete;

  Status Open();
  Status AppendRecord(uint32_t partition, const std::string& record);
  Status Append(uint32_t partition, const ResultBatch& batch);
  Status Seal();
  Status Seal(uint32_t partition);
  Status Rewind(uint32_t partition);
  Status NextRecord(uint32_t partition, std::string* record);
  Status Next(uint32_t partition, ResultBatch* batch);
  Status Close();

  uint32_t partition_count() const { return partition_count_; }
  bool HasData(uint32_t partition) const;
  uint64_t bytes_written() const;

 private:
  Status CheckCancelled();
  Status ValidatePartition(uint32_t partition) const;

  std::string directory_;
  uint32_t partition_count_ = 0;
  std::shared_ptr<QueryCancellation> cancellation_;
  std::shared_ptr<ResourceGovernorExtension> resources_;
  std::shared_ptr<QueryMemoryAccount> memory_account_;
  std::function<void(uint64_t)> write_observer_;
  std::vector<std::unique_ptr<QuerySpillFile>> partitions_;
  std::vector<bool> has_data_;
  bool opened_ = false;
};

}  // namespace cedar

#endif  // CEDAR_TCYPHER_RUNTIME_QUERY_SPILL_H_
