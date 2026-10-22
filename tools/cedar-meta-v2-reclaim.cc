// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <charconv>
#include <cstddef>
#include <iostream>
#include <string>
#include <system_error>

#include "cedar/fact/meta_v2_reclaim.h"

namespace {

constexpr size_t kDefaultBatchRecords = 1024;

void PrintUsage() {
  std::cerr
      << "usage: cedar-meta-v2-reclaim --db=PATH [--batch=N] [--sync] "
         "[--no-verify]\n"
      << "                            [--profile=NAME] [--wal-dir=PATH]\n"
      << "  --db=PATH       offline Cedar database to rewrite in place\n"
      << "  --batch=N       records per atomic write batch (default "
      << kDefaultBatchRecords << ")\n"
      << "  --sync          sync every write batch instead of once at the end\n"
      << "  --no-verify     skip re-decoding rewritten records\n"
      << "  --profile=NAME  storage profile the database uses: developer "
         "(default),\n"
         "                  kernel-test, debug-small or production\n"
      << "  --wal-dir=PATH  absolute WAL directory when the database keeps its "
         "WAL\n"
         "                  outside the database directory\n";
}

bool ParseBatchSize(const std::string& text, size_t* batch_records) {
  if (text.empty()) return false;
  size_t value = 0;
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto parsed = std::from_chars(begin, end, value);
  if (parsed.ec != std::errc() || parsed.ptr != end || value == 0) {
    return false;
  }
  *batch_records = value;
  return true;
}

bool ParseProfile(const std::string& text, cedar::StorageProfile* profile) {
  if (text == "developer") {
    *profile = cedar::StorageProfile::kDeveloper;
  } else if (text == "kernel-test") {
    *profile = cedar::StorageProfile::kKernelTest;
  } else if (text == "debug-small") {
    *profile = cedar::StorageProfile::kDebugSmallThresholds;
  } else if (text == "production" || text == "production-append") {
    *profile = cedar::StorageProfile::kProductionAppend;
  } else {
    return false;
  }
  return true;
}

void PrintStats(const cedar::MetaV2ReclaimStats& stats) {
  std::cout << "scanned=" << stats.scanned << '\n'
            << "rewritten=" << stats.rewritten << '\n'
            << "already_v2=" << stats.already_v2 << '\n'
            << "bytes_before=" << stats.bytes_before << '\n'
            << "bytes_after=" << stats.bytes_after << '\n';
}

}  // namespace

int main(int argc, char* argv[]) {
  std::string db_path;
  size_t batch_records = kDefaultBatchRecords;
  bool sync_each_batch = false;
  bool verify = true;
  cedar::StorageProfile storage_profile = cedar::StorageProfile::kDeveloper;
  std::string wal_directory;

  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    if (argument.rfind("--db=", 0) == 0) {
      db_path = argument.substr(std::string("--db=").size());
    } else if (argument.rfind("--batch=", 0) == 0) {
      if (!ParseBatchSize(argument.substr(std::string("--batch=").size()),
                          &batch_records)) {
        std::cerr << "invalid --batch value\n";
        PrintUsage();
        return 1;
      }
    } else if (argument.rfind("--profile=", 0) == 0) {
      if (!ParseProfile(argument.substr(std::string("--profile=").size()),
                        &storage_profile)) {
        std::cerr << "invalid --profile value\n";
        PrintUsage();
        return 1;
      }
    } else if (argument.rfind("--wal-dir=", 0) == 0) {
      wal_directory = argument.substr(std::string("--wal-dir=").size());
      if (wal_directory.empty()) {
        std::cerr << "invalid --wal-dir value\n";
        PrintUsage();
        return 1;
      }
    } else if (argument == "--sync") {
      sync_each_batch = true;
    } else if (argument == "--no-verify") {
      verify = false;
    } else {
      std::cerr << "unknown argument: " << argument << '\n';
      PrintUsage();
      return 1;
    }
  }
  if (db_path.empty()) {
    std::cerr << "--db=PATH is required\n";
    PrintUsage();
    return 1;
  }

  cedar::MetaV2ReclaimOptions options;
  options.batch_records = batch_records;
  options.sync_each_batch = sync_each_batch;
  options.verify = verify;
  options.storage_profile = storage_profile;
  options.production.wal_directory = wal_directory;
  cedar::MetaV2ReclaimStats stats;
  const cedar::Status status =
      cedar::ReclaimSequenceRecordsV2(db_path, options, &stats);
  if (!status.ok()) {
    std::cerr << status.ToString() << '\n';
    return 1;
  }
  PrintStats(stats);
  return 0;
}
