#ifndef CEDAR_QUERY_RESOURCE_QUERY_SCRATCH_H_
#define CEDAR_QUERY_RESOURCE_QUERY_SCRATCH_H_

#include <cstdint>
#include <filesystem>
#include <string>

#include "cedar/core/status.h"
#include "query/runtime/relational.h"

namespace cedar::internal {

class QueryScratch {
 public:
  QueryScratch(std::filesystem::path database_root, std::string instance,
               std::string query_id, uint64_t disk_budget_bytes,
               QueryReservation* reservation = nullptr);
  QueryScratch(std::filesystem::path database_root, std::string instance,
               std::string query_id, uint64_t disk_budget_bytes,
               uint64_t free_space_reserve_bytes);
  ~QueryScratch();

  StatusOr<std::filesystem::path> WriteRun(const std::string& name,
                                           const std::string& payload);
  StatusOr<std::filesystem::path> WritePartition(const std::string& name,
                                                 const std::string& payload) {
    return WriteRun(name, payload);
  }
  StatusOr<std::string> ReadRun(const std::filesystem::path& path) const;
  Status Cleanup();
  void SetReservation(QueryReservation* reservation) { reservation_ = reservation; }
  static Status CleanupOldInstances(const std::filesystem::path& database_root,
                                    const std::string& active_instance);
  const std::filesystem::path& query_directory() const { return query_dir_; }

 private:
  Status EnsureDirectory() const;
  Status ValidateChild(const std::string& name) const;
  std::filesystem::path database_root_;
  std::filesystem::path query_dir_;
  std::string query_id_;
  uint64_t disk_budget_bytes_ = 0;
  uint64_t free_space_reserve_bytes_ = 0;
  uint64_t written_bytes_ = 0;
  uint64_t reserved_bytes_ = 0;
  QueryReservation* reservation_ = nullptr;
  mutable bool created_ = false;
};

}  // namespace cedar::internal

#endif
