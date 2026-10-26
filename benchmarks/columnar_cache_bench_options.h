#pragma once

#include <charconv>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace cedar::benchmarks {

inline constexpr uint64_t kCacheDatasetCommitEntities = 20000;

struct CacheBenchOptions {
  uint64_t seconds = 1;
  uint32_t entities = 1000;
  uint32_t sessions = 1;
  bool exact_part = false;
  uint32_t part_id = 0;
  std::string db_path;
  bool setup_only = false;
  bool read_existing = false;
  bool write_existing = false;
  std::string admission = "lookup-only";
  std::string workload = "wide";
  uint64_t seed = 1;
  uint64_t cache_bytes = 64ULL << 20;
  uint64_t working_bytes = 64ULL << 20;
  // Experimental fixture pool for retained Parquet reader metadata. Zero keeps
  // the benchmark fallback instead of overriding it (a nonzero value is a test
  // configuration, not a production default).
  uint64_t storage_reader_metadata_bytes = 0;
  uint64_t query_memory_bytes = 64ULL << 20;
  uint64_t warmup_operations = 0;
};

inline bool ParseCacheBenchOptions(const std::vector<std::string>& arguments,
                                  CacheBenchOptions* output, std::string* error) {
  const auto fail = [error](const std::string& message) {
    if (error) *error = message;
    return false;
  };
  if (!output) return fail("missing options destination");
  CacheBenchOptions options;
  size_t positional = 0;
  for (size_t i = 0; i < arguments.size(); ++i) {
    const auto& argument = arguments[i];
    if (argument == "--setup-only") { options.setup_only = true; continue; }
    if (argument == "--read-existing") { options.read_existing = true; continue; }
    if (argument == "--write-existing") { options.write_existing = true; continue; }
    const bool named = argument.starts_with("--");
    const std::string* value = &argument;
    if (named) {
      if (++i == arguments.size()) return fail("missing value for " + argument);
      value = &arguments[i];
      if (value->starts_with("--")) return fail("missing value for " + argument);
    }
    if (argument == "--db-path") {
      if (value->empty()) return fail("empty database path");
      options.db_path = *value;
      continue;
    }
    if (argument == "--cache-admission") {
      if (*value != "lookup-only" && *value != "admit") return fail("invalid admission");
      options.admission = *value;
      continue;
    }
    if (argument == "--workload") {
      if (*value != "point" && *value != "narrow" && *value != "wide" &&
          *value != "temporal" && *value != "mixed" && *value != "concurrent")
        return fail("invalid workload");
      options.workload = *value;
      continue;
    }
    if (argument == "--part-scope") {
      if (*value != "exact" && *value != "all" && *value != "wildcard")
        return fail("invalid part scope");
      options.exact_part = *value == "exact";
      continue;
    }
    uint64_t number = 0;
    const auto parsed = std::from_chars(value->data(), value->data() + value->size(), number);
    if (parsed.ec != std::errc{} || parsed.ptr != value->data() + value->size())
      return fail("invalid unsigned number for " + argument);
    std::string name = argument;
    if (!named) {
      if (positional == 0) name = "--seconds";
      else if (positional == 1) name = "--entities";
      else return fail("too many positional arguments");
      ++positional;
    }
    if (name == "--seconds") options.seconds = number;
    else if (name == "--seed") options.seed = number;
    else if (name == "--cache-bytes") options.cache_bytes = number;
    else if (name == "--storage-read-working-bytes") options.working_bytes = number;
    else if (name == "--storage-reader-metadata-bytes")
      options.storage_reader_metadata_bytes = number;
    else if (name == "--query-memory-bytes") options.query_memory_bytes = number;
    else if (name == "--warmup-operations") options.warmup_operations = number;
    else {
      if (number > std::numeric_limits<uint32_t>::max()) return fail("number exceeds uint32");
      if (name == "--entities") options.entities = static_cast<uint32_t>(number);
      else if (name == "--sessions") options.sessions = static_cast<uint32_t>(number);
      else if (name == "--part-id") options.part_id = static_cast<uint32_t>(number);
      else return fail("unknown argument " + argument);
    }
  }
  if (options.seconds == 0 || options.entities == 0 || options.sessions == 0 ||
      options.sessions > 64) return fail("seconds/entities/sessions outside bounds");
  if (!options.working_bytes || !options.query_memory_bytes)
    return fail("working/query memory capacities must be positive");
  if (options.warmup_operations > 1024) return fail("warmup exceeds 1024 operations per session");
  if (options.setup_only && options.read_existing) return fail("setup and existing conflict");
  if (options.write_existing && (options.setup_only || options.read_existing))
    return fail("writable mode conflicts with setup/read-existing");
  if (options.write_existing != (options.workload == "concurrent"))
    return fail("concurrent workload requires --write-existing and vice versa");
  if (options.write_existing && (options.sessions != 1 || options.warmup_operations != 0))
    return fail("concurrent mode uses four fixed lanes without warmup");
  if ((options.setup_only || options.read_existing || options.write_existing) && options.db_path.empty())
    return fail("dataset mode requires --db-path");
  *output = std::move(options);
  if (error) error->clear();
  return true;
}

inline bool ValidateCacheBenchPath(const CacheBenchOptions& options,
                                   std::string* error) {
  const auto fail = [error](const std::string& message) {
    if (error) *error = message;
    return false;
  };
  if (options.db_path.empty()) return !options.setup_only && !options.read_existing && !options.write_existing;
  std::error_code ec;
  const std::filesystem::path directory(options.db_path);
  const bool exists = std::filesystem::exists(directory, ec);
  if (ec) return fail("cannot inspect dataset: " + ec.message());
  if (!exists) {
    if (options.read_existing || options.write_existing) return fail("existing dataset does not exist");
    return true;
  }
  if (!std::filesystem::is_directory(directory, ec) || ec)
    return fail("dataset path is not a readable directory");
  if (options.read_existing || options.write_existing) {
    for (const auto* name : {"CURRENT", "cedar-cache-bench-manifest.json"}) {
      if (!std::filesystem::is_regular_file(directory / name, ec) || ec)
        return fail("existing dataset is missing " + std::string(name));
    }
    return true;
  }
  if (!std::filesystem::is_empty(directory, ec) || ec)
    return fail("setup requires an empty or absent dataset directory");
  return true;
}

// An owned fixture descriptor, deliberately canonical so read mode rejects
// unrecognized versions/fields instead of guessing how to interpret a dataset.
inline std::string CacheBenchDatasetManifest(const CacheBenchOptions& options) {
  return "{\"format_version\":2,\"commit_entities\":" +
      std::to_string(kCacheDatasetCommitEntities) + ",\"entities\":" + std::to_string(options.entities) +
      ",\"seed\":" + std::to_string(options.seed) +
      ",\"part_id\":" + std::to_string(options.part_id) +
      ",\"fact_count\":" + std::to_string(uint64_t(options.entities) * 3) +
      ",\"facts_flushed\":true}\n";
}

inline bool ValidateCacheBenchDatasetManifest(const std::string& bytes,
                                             const CacheBenchOptions& options) {
  return bytes == CacheBenchDatasetManifest(options);
}

}  // namespace cedar::benchmarks
