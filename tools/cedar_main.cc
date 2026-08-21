// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "cedar/storage_files.h"

namespace {

const char* RoleName(cedar::StorageFileRole role) {
  switch (role) {
    case cedar::StorageFileRole::kAuthoritativeFacts:
      return "authoritative-facts";
    case cedar::StorageFileRole::kTransactionMetadata:
      return "transaction-metadata";
    case cedar::StorageFileRole::kEngineInternal:
      return "engine-internal";
  }
  return "unknown";
}

const char* TableFormatName(cedar::StorageTableFormat format) {
  switch (format) {
    case cedar::StorageTableFormat::kCedarParquet:
      return "CedarParquet";
    case cedar::StorageTableFormat::kBlockBased:
      return "BlockBased";
  }
  return "unknown";
}

std::string FormatBytes(uint64_t bytes) {
  constexpr uint64_t kKiB = 1024;
  constexpr uint64_t kMiB = 1024 * kKiB;
  constexpr uint64_t kGiB = 1024 * kMiB;
  if (bytes < kKiB) return std::to_string(bytes) + " B";
  const auto format = [bytes](uint64_t unit, const char* suffix) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(bytes % unit == 0 ? 0 : 1)
        << static_cast<double>(bytes) / static_cast<double>(unit) << ' ' << suffix;
    return out.str();
  };
  if (bytes < kMiB) return format(kKiB, "KiB");
  if (bytes < kGiB) return format(kMiB, "MiB");
  return format(kGiB, "GiB");
}

std::string SequenceRange(const cedar::StorageFileInfo& file) {
  if (file.smallest_seqno == 0 && file.largest_seqno == 0) return "-";
  return std::to_string(file.smallest_seqno) + ".." +
         std::to_string(file.largest_seqno);
}

void WriteJsonString(const std::string& value) {
  std::cout << '"';
  for (unsigned char character : value) {
    switch (character) {
      case '"': std::cout << "\\\""; break;
      case '\\': std::cout << "\\\\"; break;
      case '\b': std::cout << "\\b"; break;
      case '\f': std::cout << "\\f"; break;
      case '\n': std::cout << "\\n"; break;
      case '\r': std::cout << "\\r"; break;
      case '\t': std::cout << "\\t"; break;
      default:
        if (character < 0x20) {
          std::cout << "\\u00" << std::hex << std::setw(2) << std::setfill('0')
                    << static_cast<unsigned int>(character) << std::dec
                    << std::setfill(' ');
        } else {
          std::cout << static_cast<char>(character);
        }
    }
  }
  std::cout << '"';
}

void PrintJson(const std::vector<cedar::StorageFileInfo>& files) {
  std::cout << "{\"files\":[";
  for (size_t i = 0; i < files.size(); ++i) {
    if (i != 0) std::cout << ',';
    const auto& file = files[i];
    std::cout << '{';
    std::cout << "\"relative_filename\":";
    WriteJsonString(file.relative_filename);
    std::cout << ",\"column_family_name\":";
    WriteJsonString(file.column_family_name);
    std::cout << ",\"role\":";
    WriteJsonString(RoleName(file.role));
    std::cout << ",\"table_format\":";
    WriteJsonString(TableFormatName(file.table_format));
    std::cout << ",\"level\":" << file.level;
    std::cout << ",\"size_bytes\":" << file.size_bytes;
    std::cout << ",\"smallest_seqno\":" << file.smallest_seqno;
    std::cout << ",\"largest_seqno\":" << file.largest_seqno;
    std::cout << ",\"smallest_key_hex\":";
    WriteJsonString(file.smallest_key_hex);
    std::cout << ",\"largest_key_hex\":";
    WriteJsonString(file.largest_key_hex);
    std::cout << '}';
  }
  std::cout << "]}\n";
}

void PrintText(const std::vector<cedar::StorageFileInfo>& files) {
  std::cout << std::left << std::setw(18) << "FILE" << std::setw(10) << "CF"
            << std::setw(24) << "ROLE" << std::setw(16) << "FORMAT"
            << std::setw(8) << "LEVEL" << std::setw(12) << "SIZE" << "SEQ\n";
  for (const auto& file : files) {
    std::cout << std::left << std::setw(18) << file.relative_filename
              << std::setw(10) << file.column_family_name << std::setw(24)
              << RoleName(file.role) << std::setw(16)
              << TableFormatName(file.table_format) << std::setw(8)
              << ("L" + std::to_string(file.level)) << std::setw(12)
              << FormatBytes(file.size_bytes) << SequenceRange(file) << '\n';
  }
}

void PrintUsage() {
  std::cerr << "usage: cedar files --path DB_PATH [--json]\n";
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc < 4 || std::string(argv[1]) != "files" ||
      std::string(argv[2]) != "--path") {
    PrintUsage();
    return 2;
  }
  cedar::StorageFileInspectionOptions options;
  options.path = argv[3];
  bool json = false;
  if (argc == 5 && std::string(argv[4]) == "--json") {
    json = true;
  } else if (argc != 4) {
    PrintUsage();
    return 2;
  }
  if (options.path.empty()) {
    PrintUsage();
    return 2;
  }
  const auto inspected = cedar::InspectStorageFiles(std::move(options));
  if (!inspected.ok()) {
    std::cerr << inspected.status().ToString() << '\n';
    return 1;
  }
  if (json) {
    PrintJson(inspected.ValueOrDie());
  } else {
    PrintText(inspected.ValueOrDie());
  }
  return 0;
}
