// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <chrono>
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "cedar/database.h"
#include "storage/facts/fact_store.h"
#include "storage/rocks/rocksdb_config.h"

#include <rocksdb/db.h>
#include <rocksdb/metadata.h>

namespace {

using Clock = std::chrono::steady_clock;

cedar::Status ParseUnsigned(const char* name, const char* text, uint64_t* value) {
  const std::string_view input(text);
  uint64_t parsed = 0;
  const auto result = std::from_chars(input.data(), input.data() + input.size(), parsed);
  if (input.empty() || result.ec != std::errc() ||
      result.ptr != input.data() + input.size() || parsed == 0) {
    return cedar::Status::InvalidArgument("cedar_kernel_columnar_bench",
                                          std::string(name) + " must be positive");
  }
  *value = parsed;
  return cedar::Status::OK();
}

cedar::Status WriteVertex(cedar::Database* database, uint64_t id) {
  auto transaction = database->BeginTransaction();
  if (!transaction.ok()) return transaction.status();
  const cedar::Status asserted = transaction.ValueOrDie()->Assert(
      cedar::EntityFact::Vertex({cedar::PartId{1}, cedar::VertexId{id}}),
      cedar::ValidTime{1});
  if (!asserted.ok()) return asserted;
  auto committed = transaction.ValueOrDie()->Commit();
  if (!committed.ok()) return committed.status();
  if (committed.ValueOrDie().outcome != cedar::CommitOutcome::kCommitted) {
    return committed.ValueOrDie().status;
  }
  return cedar::Status::OK();
}

cedar::Status FlushFacts(const cedar::DatabaseOptions& database_options,
                         uint64_t* live_sst_bytes) {
  cedar::FactStoreOptions store_options;
  store_options.path = database_options.path;
  store_options.write_buffer_bytes = database_options.write_buffer_bytes;
  store_options.block_cache_bytes = database_options.block_cache_bytes;
  rocksdb::Options options = cedar::internal::MakeRocksDbOptions(store_options, false);
  options.create_if_missing = false;
  options.create_missing_column_families = false;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> database;
  const auto descriptors = cedar::internal::MakeRocksDbColumnFamilyDescriptors(
      store_options, options);
  rocksdb::Status opened = rocksdb::DB::Open(
      options, database_options.path, descriptors, &handles, &database);
  if (!opened.ok()) return cedar::Status::IOError("columnar benchmark", opened.ToString());
  rocksdb::FlushOptions flush_options;
  flush_options.wait = true;
  const rocksdb::Status flushed = database->Flush(flush_options, handles[1]);
  if (!flushed.ok()) {
    for (auto* handle : handles) database->DestroyColumnFamilyHandle(handle);
    database.reset();
    return cedar::Status::IOError("columnar benchmark flush", flushed.ToString());
  }
  std::vector<rocksdb::LiveFileMetaData> files;
  database->GetLiveFilesMetaData(&files);
  *live_sst_bytes = 0;
  for (const auto& file : files) {
    if (file.column_family_name == "facts" && file.file_type == rocksdb::kTableFile) {
      *live_sst_bytes += file.size;
    }
  }
  for (auto* handle : handles) database->DestroyColumnFamilyHandle(handle);
  database.reset();
  return cedar::Status::OK();
}

struct Options {
  std::string path;
  uint64_t rows = 8192;
  uint64_t iterations = 5;
};

cedar::StatusOr<Options> ParseOptions(int argc, char** argv) {
  Options options;
  for (int index = 1; index < argc;) {
    if (index + 1 >= argc) {
      return cedar::Status::InvalidArgument("cedar_kernel_columnar_bench",
                                            "options require values");
    }
    const std::string name(argv[index++]);
    const char* value = argv[index++];
    if (name == "--path") {
      options.path = value;
      if (!std::filesystem::path(options.path).is_absolute()) {
        return cedar::Status::InvalidArgument("cedar_kernel_columnar_bench",
                                              "--path must be absolute");
      }
    } else if (name == "--rows") {
      const cedar::Status status = ParseUnsigned("--rows", value, &options.rows);
      if (!status.ok()) return status;
    } else if (name == "--iterations") {
      const cedar::Status status =
          ParseUnsigned("--iterations", value, &options.iterations);
      if (!status.ok()) return status;
    } else {
      return cedar::Status::InvalidArgument("cedar_kernel_columnar_bench",
                                            "unknown option " + name);
    }
  }
  if (options.path.empty()) {
    return cedar::Status::InvalidArgument("cedar_kernel_columnar_bench",
                                          "--path is required");
  }
  return options;
}

int Run(const Options& options) {
  std::filesystem::remove_all(options.path);
  cedar::DatabaseOptions database_options;
  database_options.path = options.path;
  database_options.storage_profile = cedar::StorageProfile::kDeveloper;
  database_options.write_buffer_bytes = 8ULL * 1024ULL * 1024ULL;
  database_options.block_cache_bytes = 8ULL * 1024ULL * 1024ULL;
  auto opened = cedar::Database::Open(database_options);
  if (!opened.ok()) {
    std::cerr << opened.status().ToString() << '\n';
    return 1;
  }
  auto database = std::move(opened).ConsumeValueOrDie();
  for (uint64_t id = 1; id <= options.rows; ++id) {
    const cedar::Status status = WriteVertex(database.get(), id);
    if (!status.ok()) {
      std::cerr << "seed write: " << status.ToString() << '\n';
      return 1;
    }
  }
  const cedar::Status closed = database->Close();
  if (!closed.ok()) {
    std::cerr << "close before flush: " << closed.ToString() << '\n';
    return 1;
  }
  uint64_t live_sst_bytes = 0;
  const cedar::Status flushed = FlushFacts(database_options, &live_sst_bytes);
  if (!flushed.ok()) {
    std::cerr << flushed.ToString() << '\n';
    return 1;
  }
  auto reopened = cedar::Database::Open(database_options);
  if (!reopened.ok()) {
    std::cerr << "reopen: " << reopened.status().ToString() << '\n';
    return 1;
  }
  database = std::move(reopened).ConsumeValueOrDie();
  auto snapshot = database->BeginSnapshot();
  if (!snapshot.ok()) {
    std::cerr << "snapshot: " << snapshot.status().ToString() << '\n';
    return 1;
  }
  const cedar::FactScanSpec spec{cedar::PartId{1}, cedar::FactFamily::kVertexState,
                                 cedar::PropertyId{}, cedar::ValidTime{1}, 1024,
                                 1, options.rows};
  const std::vector<cedar::FactColumnId> projection = {
      cedar::FactColumnId::kEntityId, cedar::FactColumnId::kValidFrom,
      cedar::FactColumnId::kCedarCommitSeq};
  uint64_t scanned_rows = 0;
  const auto started = Clock::now();
  for (uint64_t iteration = 0; iteration < options.iterations; ++iteration) {
    const cedar::Status status = snapshot.ValueOrDie().EventColumnarScan(
        spec, projection, [&scanned_rows](const cedar::FactColumnarBatch& batch) {
          scanned_rows += batch.row_count();
          return cedar::Status::OK();
        });
    if (!status.ok()) {
      std::cerr << "scan: " << status.ToString() << '\n';
      return 1;
    }
  }
  const double elapsed =
      std::chrono::duration<double>(Clock::now() - started).count();
  const auto runtime = database->SampleRuntimeMetrics();
  if (!runtime.ok()) {
    std::cerr << "runtime sample: " << runtime.status().ToString() << '\n';
    return 1;
  }
  std::cout << "schema_version,workload,dataset_rows,iterations,live_sst_bytes,"
               "scanned_rows,elapsed_seconds,rows_per_second,projected_scan_bytes_read,"
               "qualification\n"
            << "1,persisted-columnar-scan," << options.rows << ','
            << options.iterations << ',' << live_sst_bytes << ',' << scanned_rows << ','
            << elapsed << ',' << (elapsed == 0 ? 0 : scanned_rows / elapsed) << ','
            << runtime.ValueOrDie().projected_scan_bytes_read << ','
            << ((live_sst_bytes != 0 && scanned_rows != 0)
                    ? "persisted_sst_and_rows"
                    : "invalid_no_persisted_scan")
            << '\n';
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  const auto options = ParseOptions(argc, argv);
  if (!options.ok()) {
    std::cerr << options.status().ToString() << '\n';
    return 2;
  }
  return Run(options.ValueOrDie());
}
