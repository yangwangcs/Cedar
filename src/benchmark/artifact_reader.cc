// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/artifact_reader.h"

#include <array>
#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <iterator>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string_view>
#include <sys/stat.h>
#include <sys/wait.h>
#if defined(__APPLE__)
#include <sys/event.h>
#endif
#include <unistd.h>
#include <utility>
#include <vector>

extern "C" {
#include "blake3.h"
}

#include "cedar/benchmark/artifact_writer.h"
#include "cedar/benchmark/report_builder.h"
#include "cedar/benchmark/regression_compare.h"
#include "cedar/benchmark/run_manifest.h"
#include "cedar/benchmark/workload_driver.h"
#include "cedar/blob/blob_store.h"
#include "cedar/observability/production_metric_schema.h"

extern char** environ;

namespace cedar {

struct BenchmarkExecutableSnapshotState {
  BenchmarkExecutableSnapshotState(int descriptor, std::string path,
                                   std::string directory, int directory_fd,
                                   int monitor_fd)
      : descriptor(descriptor), path(std::move(path)),
        directory(std::move(directory)), directory_fd(directory_fd),
        monitor_fd(monitor_fd) {}
  ~BenchmarkExecutableSnapshotState() {
    if (monitor_fd >= 0) ::close(monitor_fd);
    if (directory_fd >= 0) ::close(directory_fd);
    if (descriptor >= 0) ::close(descriptor);
    if (!path.empty()) ::unlink(path.c_str());
    if (!directory.empty()) ::rmdir(directory.c_str());
  }

  int descriptor = -1;
  std::string path;
  std::string directory;
  int directory_fd = -1;
  int monitor_fd = -1;
  mutable bool tampered = false;
};

int BenchmarkExecutableSnapshot::fd() const {
  return state == nullptr ? -1 : state->descriptor;
}

namespace {

constexpr uint64_t kMaximumArtifactBytes = 64ULL * 1024ULL * 1024ULL;
constexpr size_t kMaximumJsonDepth = 64;

struct JsonValue {
  enum class Kind { kNull, kBool, kNumber, kString, kArray, kObject };

  Kind kind = Kind::kNull;
  bool boolean = false;
  std::string scalar;
  std::vector<JsonValue> array;
  std::map<std::string, JsonValue> object;
};

class JsonParser {
 public:
  explicit JsonParser(std::string_view input) : input_(input) {}

  StatusOr<JsonValue> Parse() {
    JsonValue value;
    const Status status = ParseValue(0, &value);
    if (!status.ok()) return status;
    SkipWhitespace();
    if (offset_ != input_.size()) return Error("trailing content");
    return value;
  }

 private:
  Status Error(const std::string& detail) const {
    return Status::Corruption("benchmark artifact JSON",
                              detail + " at byte " + std::to_string(offset_));
  }

  void SkipWhitespace() {
    while (offset_ < input_.size()) {
      const char character = input_[offset_];
      if (character != ' ' && character != '\n' && character != '\r' &&
          character != '\t') {
        break;
      }
      ++offset_;
    }
  }

  Status ParseValue(size_t depth, JsonValue* output) {
    if (depth > kMaximumJsonDepth) return Error("nesting limit exceeded");
    SkipWhitespace();
    if (offset_ == input_.size()) return Error("unexpected end of input");
    switch (input_[offset_]) {
      case 'n': return ParseLiteral("null", JsonValue::Kind::kNull, false, output);
      case 't': return ParseLiteral("true", JsonValue::Kind::kBool, true, output);
      case 'f': return ParseLiteral("false", JsonValue::Kind::kBool, false, output);
      case '"':
        output->kind = JsonValue::Kind::kString;
        return ParseString(&output->scalar);
      case '[': return ParseArray(depth, output);
      case '{': return ParseObject(depth, output);
      default:
        if (input_[offset_] == '-' ||
            (input_[offset_] >= '0' && input_[offset_] <= '9')) {
          return ParseNumber(output);
        }
        return Error("unexpected token");
    }
  }

  Status ParseLiteral(std::string_view literal, JsonValue::Kind kind,
                      bool boolean, JsonValue* output) {
    if (input_.substr(offset_, literal.size()) != literal) {
      return Error("invalid literal");
    }
    offset_ += literal.size();
    output->kind = kind;
    output->boolean = boolean;
    return Status::OK();
  }

  static int HexDigit(char character) {
    if (character >= '0' && character <= '9') return character - '0';
    if (character >= 'a' && character <= 'f') return character - 'a' + 10;
    if (character >= 'A' && character <= 'F') return character - 'A' + 10;
    return -1;
  }

  Status ParseHexCodeUnit(uint32_t* output) {
    if (input_.size() - offset_ < 4) return Error("truncated Unicode escape");
    uint32_t value = 0;
    for (size_t index = 0; index < 4; ++index) {
      const int digit = HexDigit(input_[offset_++]);
      if (digit < 0) return Error("invalid Unicode escape");
      value = value * 16 + static_cast<uint32_t>(digit);
    }
    *output = value;
    return Status::OK();
  }

  static void AppendUtf8(uint32_t code_point, std::string* output) {
    if (code_point <= 0x7f) {
      output->push_back(static_cast<char>(code_point));
    } else if (code_point <= 0x7ff) {
      output->push_back(static_cast<char>(0xc0 | (code_point >> 6)));
      output->push_back(static_cast<char>(0x80 | (code_point & 0x3f)));
    } else if (code_point <= 0xffff) {
      output->push_back(static_cast<char>(0xe0 | (code_point >> 12)));
      output->push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3f)));
      output->push_back(static_cast<char>(0x80 | (code_point & 0x3f)));
    } else {
      output->push_back(static_cast<char>(0xf0 | (code_point >> 18)));
      output->push_back(static_cast<char>(0x80 | ((code_point >> 12) & 0x3f)));
      output->push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3f)));
      output->push_back(static_cast<char>(0x80 | (code_point & 0x3f)));
    }
  }

  Status ParseString(std::string* output) {
    if (input_[offset_++] != '"') return Error("expected string");
    output->clear();
    while (offset_ < input_.size()) {
      const unsigned char character =
          static_cast<unsigned char>(input_[offset_++]);
      if (character == '"') return Status::OK();
      if (character < 0x20) return Error("unescaped control character");
      if (character != '\\') {
        output->push_back(static_cast<char>(character));
        continue;
      }
      if (offset_ == input_.size()) return Error("truncated escape");
      const char escape = input_[offset_++];
      switch (escape) {
        case '"': output->push_back('"'); break;
        case '\\': output->push_back('\\'); break;
        case '/': output->push_back('/'); break;
        case 'b': output->push_back('\b'); break;
        case 'f': output->push_back('\f'); break;
        case 'n': output->push_back('\n'); break;
        case 'r': output->push_back('\r'); break;
        case 't': output->push_back('\t'); break;
        case 'u': {
          uint32_t code_point = 0;
          Status status = ParseHexCodeUnit(&code_point);
          if (!status.ok()) return status;
          if (code_point >= 0xd800 && code_point <= 0xdbff) {
            if (input_.size() - offset_ < 6 || input_[offset_] != '\\' ||
                input_[offset_ + 1] != 'u') {
              return Error("missing low surrogate");
            }
            offset_ += 2;
            uint32_t low = 0;
            status = ParseHexCodeUnit(&low);
            if (!status.ok()) return status;
            if (low < 0xdc00 || low > 0xdfff) {
              return Error("invalid low surrogate");
            }
            code_point = 0x10000 + ((code_point - 0xd800) << 10) +
                         (low - 0xdc00);
          } else if (code_point >= 0xdc00 && code_point <= 0xdfff) {
            return Error("unexpected low surrogate");
          }
          AppendUtf8(code_point, output);
          break;
        }
        default: return Error("invalid escape");
      }
    }
    return Error("unterminated string");
  }

  Status ParseNumber(JsonValue* output) {
    const size_t begin = offset_;
    if (input_[offset_] == '-') ++offset_;
    if (offset_ == input_.size()) return Error("truncated number");
    if (input_[offset_] == '0') {
      ++offset_;
    } else if (input_[offset_] >= '1' && input_[offset_] <= '9') {
      while (offset_ < input_.size() && input_[offset_] >= '0' &&
             input_[offset_] <= '9') {
        ++offset_;
      }
    } else {
      return Error("invalid number");
    }
    if (offset_ < input_.size() && input_[offset_] == '.') {
      ++offset_;
      const size_t digits = offset_;
      while (offset_ < input_.size() && input_[offset_] >= '0' &&
             input_[offset_] <= '9') {
        ++offset_;
      }
      if (digits == offset_) return Error("invalid fraction");
    }
    if (offset_ < input_.size() &&
        (input_[offset_] == 'e' || input_[offset_] == 'E')) {
      ++offset_;
      if (offset_ < input_.size() &&
          (input_[offset_] == '+' || input_[offset_] == '-')) {
        ++offset_;
      }
      const size_t digits = offset_;
      while (offset_ < input_.size() && input_[offset_] >= '0' &&
             input_[offset_] <= '9') {
        ++offset_;
      }
      if (digits == offset_) return Error("invalid exponent");
    }
    output->kind = JsonValue::Kind::kNumber;
    output->scalar.assign(input_.substr(begin, offset_ - begin));
    return Status::OK();
  }

  Status ParseArray(size_t depth, JsonValue* output) {
    ++offset_;
    output->kind = JsonValue::Kind::kArray;
    SkipWhitespace();
    if (offset_ < input_.size() && input_[offset_] == ']') {
      ++offset_;
      return Status::OK();
    }
    while (true) {
      JsonValue value;
      Status status = ParseValue(depth + 1, &value);
      if (!status.ok()) return status;
      output->array.push_back(std::move(value));
      SkipWhitespace();
      if (offset_ == input_.size()) return Error("unterminated array");
      const char delimiter = input_[offset_++];
      if (delimiter == ']') return Status::OK();
      if (delimiter != ',') return Error("expected array delimiter");
    }
  }

  Status ParseObject(size_t depth, JsonValue* output) {
    ++offset_;
    output->kind = JsonValue::Kind::kObject;
    SkipWhitespace();
    if (offset_ < input_.size() && input_[offset_] == '}') {
      ++offset_;
      return Status::OK();
    }
    while (true) {
      SkipWhitespace();
      if (offset_ == input_.size() || input_[offset_] != '"') {
        return Error("expected object key");
      }
      std::string key;
      Status status = ParseString(&key);
      if (!status.ok()) return status;
      SkipWhitespace();
      if (offset_ == input_.size() || input_[offset_++] != ':') {
        return Error("expected object colon");
      }
      JsonValue value;
      status = ParseValue(depth + 1, &value);
      if (!status.ok()) return status;
      if (!output->object.emplace(std::move(key), std::move(value)).second) {
        return Error("duplicate object key");
      }
      SkipWhitespace();
      if (offset_ == input_.size()) return Error("unterminated object");
      const char delimiter = input_[offset_++];
      if (delimiter == '}') return Status::OK();
      if (delimiter != ',') return Error("expected object delimiter");
    }
  }

  std::string_view input_;
  size_t offset_ = 0;
};

StatusOr<std::string> ReadArtifact(const std::string& path) {
  std::error_code error;
  const uint64_t size = std::filesystem::file_size(path, error);
  if (error) return Status::IOError(path, error.message());
  if (size > kMaximumArtifactBytes) {
    return Status::ResourceExhausted("benchmark artifact", "JSON file exceeds 64 MiB");
  }
  std::ifstream input(path, std::ios::binary);
  if (!input) return Status::IOError(path, "cannot open artifact");
  std::string content(static_cast<size_t>(size), '\0');
  if (size != 0) input.read(content.data(), static_cast<std::streamsize>(size));
  if (!input && size != 0) return Status::IOError(path, "cannot read complete artifact");
  return content;
}

StatusOr<JsonValue> ReadJson(const std::string& path) {
  const auto content = ReadArtifact(path);
  if (!content.ok()) return content.status();
  return JsonParser(content.ValueOrDie()).Parse();
}

Status RequireKind(const JsonValue& value, JsonValue::Kind kind,
                   const std::string& field) {
  if (value.kind != kind) {
    return Status::Corruption("benchmark artifact", field + " has the wrong JSON type");
  }
  return Status::OK();
}

StatusOr<const JsonValue*> Field(const JsonValue& object,
                                 const std::string& name) {
  const Status status = RequireKind(object, JsonValue::Kind::kObject, "root");
  if (!status.ok()) return status;
  const auto found = object.object.find(name);
  if (found == object.object.end()) {
    return Status::Corruption("benchmark artifact", "missing field " + name);
  }
  return &found->second;
}

Status ReadString(const JsonValue& object, const char* name, std::string* output) {
  const auto field = Field(object, name);
  if (!field.ok()) return field.status();
  const Status status = RequireKind(*field.ValueOrDie(), JsonValue::Kind::kString, name);
  if (!status.ok()) return status;
  *output = field.ValueOrDie()->scalar;
  return Status::OK();
}

Status ReadBool(const JsonValue& object, const char* name, bool* output) {
  const auto field = Field(object, name);
  if (!field.ok()) return field.status();
  const Status status = RequireKind(*field.ValueOrDie(), JsonValue::Kind::kBool, name);
  if (!status.ok()) return status;
  *output = field.ValueOrDie()->boolean;
  return Status::OK();
}

Status ReadU64(const JsonValue& object, const char* name, uint64_t* output) {
  const auto field = Field(object, name);
  if (!field.ok()) return field.status();
  const Status status = RequireKind(*field.ValueOrDie(), JsonValue::Kind::kNumber, name);
  if (!status.ok()) return status;
  const std::string& number = field.ValueOrDie()->scalar;
  uint64_t value = 0;
  const auto parsed = std::from_chars(number.data(), number.data() + number.size(), value);
  if (parsed.ec != std::errc() || parsed.ptr != number.data() + number.size()) {
    return Status::Corruption("benchmark artifact", std::string(name) + " is not UInt64");
  }
  *output = value;
  return Status::OK();
}

Status ReadU32(const JsonValue& object, const char* name, uint32_t* output) {
  uint64_t value = 0;
  const Status status = ReadU64(object, name, &value);
  if (!status.ok()) return status;
  if (value > std::numeric_limits<uint32_t>::max()) {
    return Status::Corruption("benchmark artifact", std::string(name) + " exceeds UInt32");
  }
  *output = static_cast<uint32_t>(value);
  return Status::OK();
}

Status ReadDouble(const JsonValue& object, const char* name, double* output) {
  const auto field = Field(object, name);
  if (!field.ok()) return field.status();
  const Status status = RequireKind(*field.ValueOrDie(), JsonValue::Kind::kNumber, name);
  if (!status.ok()) return status;
  const std::string& number = field.ValueOrDie()->scalar;
  char* end = nullptr;
  errno = 0;
  const double value = std::strtod(number.c_str(), &end);
  if (errno == ERANGE || end != number.c_str() + number.size() ||
      !std::isfinite(value)) {
    return Status::Corruption("benchmark artifact", std::string(name) + " is not finite");
  }
  *output = value;
  return Status::OK();
}

Status ReadManifest(const JsonValue& root, BenchmarkRunManifest* manifest,
                    std::string* archived_run_id) {
  Status status = ReadU32(root, "benchmark_protocol_version", &manifest->protocol_version);
  if (!status.ok()) return status;
  if (manifest->protocol_version != 1) {
    return Status::NotSupported("benchmark artifact", "unsupported manifest protocol version");
  }
#define CEDAR_READ_MANIFEST_STRING(field_name, member)                 \
  status = ReadString(root, field_name, &manifest->member);            \
  if (!status.ok()) return status
  CEDAR_READ_MANIFEST_STRING("source_commit", source_commit);
  status = ReadBool(root, "source_dirty_state", &manifest->source_dirty);
  if (!status.ok()) return status;
  CEDAR_READ_MANIFEST_STRING("binary_hash", binary_hash);
  CEDAR_READ_MANIFEST_STRING("compiler_and_flags", compiler_and_flags);
  CEDAR_READ_MANIFEST_STRING("os_kernel", os_kernel);
  CEDAR_READ_MANIFEST_STRING("cpu_model_and_count", cpu_model_and_count);
  status = ReadU64(root, "memory_limit_bytes", &manifest->memory_limit_bytes);
  if (!status.ok()) return status;
  CEDAR_READ_MANIFEST_STRING("storage_device_and_filesystem", storage_device_and_filesystem);
  CEDAR_READ_MANIFEST_STRING("resource_profile_id", resource_profile_id);
  const auto instrumentation = root.object.find("instrumentation_profile_id");
  const bool implicit_instrumentation_profile =
      instrumentation == root.object.end();
  if (implicit_instrumentation_profile) {
    manifest->instrumentation_profile_id = kInstrumentationProfileTier0Tier1;
  } else {
    status = ReadString(root, "instrumentation_profile_id",
                        &manifest->instrumentation_profile_id);
    if (!status.ok()) return status;
  }
  if (manifest->instrumentation_profile_id !=
          kInstrumentationProfileTier0Tier1 &&
      manifest->instrumentation_profile_id !=
          kInstrumentationProfileTier0Minimal) {
    return Status::NotSupported("benchmark artifact",
                                "unknown instrumentation profile");
  }
  status = ReadU32(root, "database_format_version", &manifest->database_format_version);
  if (!status.ok()) return status;
  if (manifest->database_format_version != kCedarDatabaseFormatVersion) {
    return Status::NotSupported("benchmark artifact",
                               "unsupported database format version");
  }
  CEDAR_READ_MANIFEST_STRING("language_version", language_version);
  CEDAR_READ_MANIFEST_STRING("schema_hash", schema_hash);
  CEDAR_READ_MANIFEST_STRING("dataset_id", dataset_id);
  CEDAR_READ_MANIFEST_STRING("dataset_hash", dataset_hash);
  CEDAR_READ_MANIFEST_STRING("dataset_profile_id", dataset_profile_id);
  status = ReadU64(root, "dataset_vertex_count",
                   &manifest->dataset_vertex_count);
  if (!status.ok()) return status;
  status = ReadU64(root, "dataset_edge_count", &manifest->dataset_edge_count);
  if (!status.ok()) return status;
  status = ReadU32(root, "dataset_property_events_per_vertex",
                   &manifest->dataset_property_events_per_vertex);
  if (!status.ok()) return status;
  status = ReadU64(root, "dataset_valid_time_span",
                   &manifest->dataset_valid_time_span);
  if (!status.ok()) return status;
  CEDAR_READ_MANIFEST_STRING("source_dataset_kind", source_dataset_kind);
  CEDAR_READ_MANIFEST_STRING("source_dataset_license", source_dataset_license);
  CEDAR_READ_MANIFEST_STRING("source_transform_policy", source_transform_policy);
  if (!status.ok()) return status;
  status = ReadU64(root, "generator_seed", &manifest->generator_seed);
  if (!status.ok()) return status;
  CEDAR_READ_MANIFEST_STRING("workload_id", workload_id);
  CEDAR_READ_MANIFEST_STRING("workload_hash", workload_hash);
  CEDAR_READ_MANIFEST_STRING("durability_mode", durability_mode);
  CEDAR_READ_MANIFEST_STRING("cache_mode", cache_mode);
  status = ReadU32(root, "worker_limit", &manifest->worker_limit);
  if (!status.ok()) return status;
  CEDAR_READ_MANIFEST_STRING("execution_nonce", execution_nonce);
  status = ReadString(root, "run_id", archived_run_id);
#undef CEDAR_READ_MANIFEST_STRING
  if (!status.ok()) return status;
  const bool current_run_id = *archived_run_id == BenchmarkRunId(*manifest);
  const bool implicit_profile_run_id = implicit_instrumentation_profile &&
      *archived_run_id ==
          BenchmarkRunIdWithImplicitTier0Tier1(*manifest);
  if (!current_run_id && !implicit_profile_run_id) {
    return Status::Corruption("benchmark artifact", "manifest run_id does not match provenance");
  }
  return Status::OK();
}

Status ReadRatio(const JsonValue& metrics, const char* name, BenchmarkRatio* ratio) {
  const auto field = Field(metrics, name);
  if (!field.ok()) return field.status();
  Status status = ReadU64(*field.ValueOrDie(), "numerator", &ratio->numerator);
  if (!status.ok()) return status;
  status = ReadU64(*field.ValueOrDie(), "denominator", &ratio->denominator);
  if (!status.ok()) return status;
  bool defined = false;
  status = ReadBool(*field.ValueOrDie(), "defined", &defined);
  if (!status.ok()) return status;
  if (defined != (ratio->denominator != 0)) {
    return Status::Corruption("benchmark artifact", std::string(name) + " defined flag mismatch");
  }
  const auto value = Field(*field.ValueOrDie(), "value");
  if (!value.ok()) return value.status();
  if ((defined && value.ValueOrDie()->kind != JsonValue::Kind::kNumber) ||
      (!defined && value.ValueOrDie()->kind != JsonValue::Kind::kNull)) {
    return Status::Corruption("benchmark artifact", std::string(name) + " value mismatch");
  }
  return Status::OK();
}

Status ReadTransactionDistribution(
    const JsonValue& measurements, const char* name,
    TransactionMeasurementDistribution* distribution) {
  const auto field = Field(measurements, name);
  if (!field.ok()) return field.status();
  const Status object = RequireKind(*field.ValueOrDie(), JsonValue::Kind::kObject, name);
  if (!object.ok()) return object;
  bool defined = false;
  Status status = ReadBool(*field.ValueOrDie(), "defined", &defined);
  if (!status.ok()) return status;
  status = ReadU64(*field.ValueOrDie(), "sample_count", &distribution->sample_count);
  if (!status.ok()) return status;
  if (defined != (distribution->sample_count != 0)) {
    return Status::Corruption("benchmark artifact",
                              std::string(name) + " defined flag mismatch");
  }
  distribution->defined = defined;
  const std::array<std::pair<const char*, uint64_t*>, 7> fields = {{
      {"min_ns", &distribution->min_ns}, {"p50_ns", &distribution->p50_ns},
      {"p95_ns", &distribution->p95_ns}, {"p99_ns", &distribution->p99_ns},
      {"p999_ns", &distribution->p999_ns}, {"max_ns", &distribution->max_ns},
      {"sum_ns", &distribution->sum_ns}}};
  for (const auto& item : fields) {
    const auto value = Field(*field.ValueOrDie(), item.first);
    if (!value.ok()) return value.status();
    if (defined) {
      if (value.ValueOrDie()->kind != JsonValue::Kind::kNumber) {
        return Status::Corruption("benchmark artifact",
                                  std::string(name) + " numeric value is null");
      }
      status = ReadU64(*field.ValueOrDie(), item.first, item.second);
      if (!status.ok()) return status;
    } else if (value.ValueOrDie()->kind != JsonValue::Kind::kNull) {
      return Status::Corruption("benchmark artifact",
                                std::string(name) + " undefined value is numeric");
    }
  }
  return Status::OK();
}

Status ReadTransactionMeasurements(const JsonValue& root,
                                   TransactionMeasurementWindow* measurements) {
  const auto field = Field(root, "transaction_measurements");
  if (!field.ok()) return field.status();
  const Status object = RequireKind(*field.ValueOrDie(), JsonValue::Kind::kObject,
                                    "transaction_measurements");
  if (!object.ok()) return object;
  Status status = ReadBool(*field.ValueOrDie(), "available", &measurements->available);
  if (!status.ok()) return status;
  status = ReadString(*field.ValueOrDie(), "availability_reason",
                      &measurements->availability_reason);
  if (!status.ok()) return status;
  const std::array<std::pair<const char*, uint64_t*>, 6> counters = {{
      {"started", &measurements->started}, {"committed", &measurements->committed},
      {"aborted", &measurements->aborted}, {"indeterminate", &measurements->indeterminate},
      {"conflicts", &measurements->conflicts},
      {"visible_prefix_nonzero_stalls", &measurements->visible_prefix_nonzero_stalls}}};
  for (const auto& counter : counters) {
    status = ReadU64(*field.ValueOrDie(), counter.first, counter.second);
    if (!status.ok()) return status;
  }
  if (measurements->conflicts > measurements->aborted) {
    return Status::Corruption("benchmark artifact", "conflicts exceed aborts");
  }
  const auto lag = Field(*field.ValueOrDie(), "visible_prefix_max_lag_seq");
  if (!lag.ok()) return lag.status();
  bool lag_defined = false;
  status = ReadBool(*lag.ValueOrDie(), "defined", &lag_defined);
  if (!status.ok()) return status;
  const auto lag_value = Field(*lag.ValueOrDie(), "value");
  if (!lag_value.ok()) return lag_value.status();
  if (lag_defined) {
    status = ReadU64(*lag.ValueOrDie(), "value",
                     &measurements->visible_prefix_max_lag_seq);
    if (!status.ok()) return status;
  } else if (lag_value.ValueOrDie()->kind != JsonValue::Kind::kNull) {
    return Status::Corruption("benchmark artifact", "undefined lag is numeric");
  }
  measurements->visible_prefix_max_lag_defined = lag_defined;
  const auto rate = Field(*field.ValueOrDie(), "conflict_abort_rate");
  if (!rate.ok()) return rate.status();
  status = ReadBool(*rate.ValueOrDie(), "defined",
                    &measurements->conflict_abort_rate.defined);
  if (!status.ok()) return status;
  status = ReadU64(*rate.ValueOrDie(), "numerator",
                   &measurements->conflict_abort_rate.numerator);
  if (!status.ok()) return status;
  status = ReadU64(*rate.ValueOrDie(), "denominator",
                   &measurements->conflict_abort_rate.denominator);
  if (!status.ok()) return status;
  if (measurements->conflict_abort_rate.numerator != measurements->conflicts ||
      measurements->conflict_abort_rate.denominator != measurements->aborted ||
      measurements->conflict_abort_rate.defined != (measurements->aborted != 0)) {
    return Status::Corruption("benchmark artifact", "conflict abort rate mismatch");
  }
  const auto rate_value = Field(*rate.ValueOrDie(), "value");
  if (!rate_value.ok()) return rate_value.status();
  if ((measurements->conflict_abort_rate.defined &&
       rate_value.ValueOrDie()->kind != JsonValue::Kind::kNumber) ||
      (!measurements->conflict_abort_rate.defined &&
       rate_value.ValueOrDie()->kind != JsonValue::Kind::kNull)) {
    return Status::Corruption("benchmark artifact", "conflict abort rate value mismatch");
  }
  const std::array<std::pair<const char*, TransactionMeasurementDistribution*>, 6>
      distributions = {{
          {"commit_latency", &measurements->commit_latency},
          {"prepare_latency", &measurements->prepare_latency},
          {"decision_latency", &measurements->decision_latency},
          {"decision_fsync_latency", &measurements->decision_fsync_latency},
          {"visible_prefix_wait_success", &measurements->visible_prefix_wait_success},
          {"visible_prefix_wait_failure", &measurements->visible_prefix_wait_failure}}};
  for (const auto& distribution : distributions) {
    status = ReadTransactionDistribution(*field.ValueOrDie(), distribution.first,
                                         distribution.second);
    if (!status.ok()) return status;
  }
  if (!measurements->available && measurements->availability_reason.empty()) {
    return Status::Corruption("benchmark artifact", "unavailable measurements lack reason");
  }
  if (!measurements->available) {
    const bool observed_counter = measurements->started != 0 ||
        measurements->committed != 0 || measurements->aborted != 0 ||
        measurements->indeterminate != 0 || measurements->conflicts != 0 ||
        measurements->visible_prefix_nonzero_stalls != 0;
    const bool observed_value = measurements->visible_prefix_max_lag_defined ||
        measurements->conflict_abort_rate.defined;
    bool observed_distribution = false;
    for (const auto& distribution : distributions) {
      observed_distribution = observed_distribution ||
          distribution.second->defined || distribution.second->sample_count != 0;
    }
    if (observed_counter || observed_value || observed_distribution) {
      return Status::Corruption(
          "benchmark artifact",
          "unavailable transaction measurements contain observed values");
    }
  }
  return Status::OK();
}

bool IsSha256(const std::string& value) {
  return value.size() == 64 && std::all_of(
      value.begin(), value.end(), [](unsigned char character) {
        return std::isxdigit(character) != 0;
      });
}

std::string Lowercase(std::string value) {
  for (char& character : value) {
    character = static_cast<char>(std::tolower(
        static_cast<unsigned char>(character)));
  }
  return value;
}

uint32_t RotateRight(uint32_t value, uint32_t shift) {
  return (value >> shift) | (value << (32 - shift));
}

class Sha256 {
 public:
  void Update(const uint8_t* data, size_t size) {
    bit_count_ += static_cast<uint64_t>(size) * 8;
    while (size != 0) {
      const size_t copied = std::min(size, block_.size() - block_size_);
      std::memcpy(block_.data() + block_size_, data, copied);
      block_size_ += copied;
      data += copied;
      size -= copied;
      if (block_size_ == block_.size()) {
        Transform(block_.data());
        block_size_ = 0;
      }
    }
  }

  std::string FinalHex() {
    const uint64_t original_bit_count = bit_count_;
    const uint8_t one = 0x80;
    Update(&one, 1);
    const uint8_t zero = 0;
    while (block_size_ != 56) Update(&zero, 1);
    std::array<uint8_t, 8> length{};
    for (size_t index = 0; index < length.size(); ++index) {
      length[length.size() - 1 - index] =
          static_cast<uint8_t>(original_bit_count >> (index * 8));
    }
    Update(length.data(), length.size());

    static constexpr char kHex[] = "0123456789abcdef";
    std::string result;
    result.reserve(64);
    for (uint32_t word : state_) {
      for (int shift = 28; shift >= 0; shift -= 4) {
        result.push_back(kHex[(word >> shift) & 0x0f]);
      }
    }
    return result;
  }

 private:
  void Transform(const uint8_t* block) {
    static constexpr std::array<uint32_t, 64> kConstants = {{
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
        0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
        0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
        0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
        0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
        0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
        0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
        0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    }};
    std::array<uint32_t, 64> schedule{};
    for (size_t index = 0; index < 16; ++index) {
      schedule[index] = (static_cast<uint32_t>(block[index * 4]) << 24) |
          (static_cast<uint32_t>(block[index * 4 + 1]) << 16) |
          (static_cast<uint32_t>(block[index * 4 + 2]) << 8) |
          static_cast<uint32_t>(block[index * 4 + 3]);
    }
    for (size_t index = 16; index < schedule.size(); ++index) {
      const uint32_t small_sigma0 = RotateRight(schedule[index - 15], 7) ^
          RotateRight(schedule[index - 15], 18) ^ (schedule[index - 15] >> 3);
      const uint32_t small_sigma1 = RotateRight(schedule[index - 2], 17) ^
          RotateRight(schedule[index - 2], 19) ^ (schedule[index - 2] >> 10);
      schedule[index] = schedule[index - 16] + small_sigma0 +
          schedule[index - 7] + small_sigma1;
    }
    uint32_t a = state_[0], b = state_[1], c = state_[2], d = state_[3];
    uint32_t e = state_[4], f = state_[5], g = state_[6], h = state_[7];
    for (size_t index = 0; index < schedule.size(); ++index) {
      const uint32_t sum1 = RotateRight(e, 6) ^ RotateRight(e, 11) ^
          RotateRight(e, 25);
      const uint32_t choose = (e & f) ^ (~e & g);
      const uint32_t temporary1 = h + sum1 + choose + kConstants[index] +
          schedule[index];
      const uint32_t sum0 = RotateRight(a, 2) ^ RotateRight(a, 13) ^
          RotateRight(a, 22);
      const uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
      const uint32_t temporary2 = sum0 + majority;
      h = g; g = f; f = e; e = d + temporary1;
      d = c; c = b; b = a; a = temporary1 + temporary2;
    }
    state_[0] += a; state_[1] += b; state_[2] += c; state_[3] += d;
    state_[4] += e; state_[5] += f; state_[6] += g; state_[7] += h;
  }

  std::array<uint32_t, 8> state_ = {{
      0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
      0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
  }};
  std::array<uint8_t, 64> block_{};
  size_t block_size_ = 0;
  uint64_t bit_count_ = 0;
};

class ScopedFd {
 public:
  ScopedFd() = default;
  explicit ScopedFd(int fd) : fd_(fd) {}
  ~ScopedFd() {
    if (fd_ >= 0) ::close(fd_);
  }

  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;
  ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
  }
  ScopedFd& operator=(ScopedFd&& other) noexcept {
    if (this != &other) {
      if (fd_ >= 0) ::close(fd_);
      fd_ = other.fd_;
      other.fd_ = -1;
    }
    return *this;
  }

  int get() const { return fd_; }
  int Release() {
    const int fd = fd_;
    fd_ = -1;
    return fd;
  }

 private:
  int fd_ = -1;
};

StatusOr<std::string> Sha256File(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) return Status::IOError(path.string(), "cannot open for hashing");
  Sha256 hash;
  std::array<uint8_t, 64 * 1024> buffer{};
  while (input) {
    input.read(reinterpret_cast<char*>(buffer.data()), buffer.size());
    const std::streamsize read = input.gcount();
    if (read > 0) hash.Update(buffer.data(), static_cast<size_t>(read));
  }
  if (!input.eof()) return Status::IOError(path.string(), "cannot read for hashing");
  return hash.FinalHex();
}

StatusOr<std::filesystem::path> EvidenceRelativePath(
    const std::string& raw_path) {
  if (raw_path.find('\0') != std::string::npos) {
    return Status::Corruption("release evidence",
                              "ledger path contains a NUL byte");
  }
  const std::filesystem::path relative(raw_path);
  if (raw_path.empty() || relative.is_absolute() ||
      raw_path.find("//") != std::string::npos || raw_path.back() == '/') {
    return Status::Corruption("release evidence", "ledger path is not relative");
  }
  for (const auto& component : relative) {
    if (component == "." || component == "..") {
      return Status::Corruption("release evidence", "ledger path escapes evidence root");
    }
  }
  return relative;
}

StatusOr<ScopedFd> OpenEvidenceFileAt(int root_fd,
                                      const std::string& raw_path) {
  const auto relative = EvidenceRelativePath(raw_path);
  if (!relative.ok()) return relative.status();
  int directory_fd = root_fd;
  ScopedFd owned_directory;
  auto component = relative.ValueOrDie().begin();
  const auto end = relative.ValueOrDie().end();
  for (; component != end; ++component) {
    const auto next = std::next(component);
    const bool final_component = next == end;
    const int flags = O_RDONLY | O_CLOEXEC | O_NOFOLLOW |
        (final_component ? 0 : O_DIRECTORY);
    const std::string name = component->string();
    const int opened = ::openat(directory_fd, name.c_str(), flags);
    if (opened < 0) {
      return Status::Corruption(
          "release evidence",
          "cannot safely open ledger entry " + raw_path + ": " +
              std::strerror(errno));
    }
    if (!final_component) {
      owned_directory = ScopedFd(opened);
      directory_fd = owned_directory.get();
      continue;
    }
    ScopedFd file(opened);
    struct stat metadata {};
    if (::fstat(file.get(), &metadata) != 0) {
      return Status::Corruption(
          "release evidence",
          "cannot inspect ledger entry " + raw_path + ": " +
              std::strerror(errno));
    }
    if (!S_ISREG(metadata.st_mode)) {
      return Status::Corruption(
          "release evidence",
          "ledger entry is not a regular file: " + raw_path);
    }
    return file;
  }
  return Status::Corruption("release evidence", "ledger path is not relative");
}

StatusOr<std::string> ReadFdContent(int fd, const std::string& path) {
  struct stat metadata {};
  if (::fstat(fd, &metadata) != 0) {
    return Status::Corruption("release evidence",
                              "cannot inspect " + path + ": " +
                                  std::strerror(errno));
  }
  if (metadata.st_size < 0 ||
      static_cast<uint64_t>(metadata.st_size) > kMaximumArtifactBytes) {
    return Status::ResourceExhausted("release evidence",
                                    path + " exceeds 64 MiB");
  }
  std::string content;
  content.reserve(static_cast<size_t>(metadata.st_size));
  std::array<char, 64 * 1024> buffer{};
  while (true) {
    const ssize_t read = ::read(fd, buffer.data(), buffer.size());
    if (read < 0) {
      if (errno == EINTR) continue;
      return Status::Corruption("release evidence",
                                "cannot read " + path + ": " +
                                    std::strerror(errno));
    }
    if (read == 0) break;
    if (content.size() + static_cast<size_t>(read) > kMaximumArtifactBytes) {
      return Status::ResourceExhausted("release evidence",
                                      path + " exceeds 64 MiB");
    }
    content.append(buffer.data(), static_cast<size_t>(read));
  }
  return content;
}

StatusOr<std::string> Sha256Fd(int fd, const std::string& path,
                               std::string* captured_content) {
  Sha256 hash;
  std::array<uint8_t, 64 * 1024> buffer{};
  while (true) {
    const ssize_t read = ::read(fd, buffer.data(), buffer.size());
    if (read < 0) {
      if (errno == EINTR) continue;
      return Status::Corruption("release evidence",
                                "cannot read ledger entry " + path + ": " +
                                    std::strerror(errno));
    }
    if (read == 0) break;
    const size_t size = static_cast<size_t>(read);
    hash.Update(buffer.data(), size);
    if (captured_content != nullptr) {
      if (captured_content->size() + size > kMaximumArtifactBytes) {
        return Status::ResourceExhausted("release evidence",
                                        path + " exceeds 64 MiB");
      }
      captured_content->append(reinterpret_cast<const char*>(buffer.data()),
                               size);
    }
  }
  return hash.FinalHex();
}

StatusOr<std::map<std::string, std::string>> ReadSha256Sums(
    int root_fd) {
  auto ledger = OpenEvidenceFileAt(root_fd, "SHA256SUMS");
  if (!ledger.ok()) return ledger.status();
  ScopedFd ledger_fd = std::move(ledger).ConsumeValueOrDie();
  const auto content = ReadFdContent(ledger_fd.get(), "SHA256SUMS");
  if (!content.ok()) return content.status();
  std::map<std::string, std::string> entries;
  size_t offset = 0;
  while (offset < content.ValueOrDie().size()) {
    const size_t end = content.ValueOrDie().find('\n', offset);
    std::string_view line(content.ValueOrDie().data() + offset,
                          (end == std::string::npos ? content.ValueOrDie().size() : end) - offset);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    offset = end == std::string::npos ? content.ValueOrDie().size() : end + 1;
    if (line.empty()) continue;
    if (line.size() < 67 || line.substr(64, 2) != "  ") {
      return Status::Corruption("release evidence", "invalid SHA256SUMS entry");
    }
    const std::string digest(line.substr(0, 64));
    const std::string path(line.substr(66));
    if (!IsSha256(digest)) {
      return Status::Corruption("release evidence", "invalid SHA256SUMS digest");
    }
    const auto relative = EvidenceRelativePath(path);
    if (!relative.ok()) return relative.status();
    if (!entries.emplace(relative.ValueOrDie().generic_string(), Lowercase(digest)).second) {
      return Status::Corruption("release evidence", "duplicate SHA256SUMS path");
    }
  }
  if (entries.empty()) {
    return Status::Corruption("release evidence", "SHA256SUMS is empty");
  }
  return entries;
}

bool HasLegacyExternalVersionName(const std::string& value) {
  std::string lower;
  lower.reserve(value.size());
  for (unsigned char character : value) {
    lower.push_back(static_cast<char>(std::tolower(character)));
  }
  return lower.find("v2") != std::string::npos ||
      lower.find("legacy") != std::string::npos;
}

Status ValidateReleaseEvidenceManifestJson(const JsonValue& root) {
  uint32_t schema_version = 0;
  Status status = ReadU32(root, "schema_version", &schema_version);
  if (!status.ok()) return status;
  if (schema_version != 1) {
    return Status::NotSupported("release evidence",
                                "unsupported manifest schema");
  }
  std::string artifact_id;
  status = ReadString(root, "artifact_id", &artifact_id);
  if (!status.ok()) return status;
  if (artifact_id.empty()) {
    return Status::Corruption("release evidence", "artifact_id is empty");
  }
  if (HasLegacyExternalVersionName(artifact_id)) {
    return Status::NotSupported("release evidence",
                                "legacy external artifact naming");
  }
  bool flag = false;
  status = ReadBool(root, "release_gate_eligible", &flag);
  if (!status.ok()) return status;
  status = ReadBool(root, "paper_gate_eligible", &flag);
  if (!status.ok()) return status;
  uint32_t database_format_version = 0;
  status = ReadU32(root, "database_format_version", &database_format_version);
  if (!status.ok()) return status;
  if (database_format_version != kCedarDatabaseFormatVersion) {
    return Status::NotSupported("release evidence",
                                "unsupported database format version");
  }
  bool clean_break_naming = false;
  status = ReadBool(root, "clean_break_naming", &clean_break_naming);
  if (!status.ok()) return status;
  if (!clean_break_naming) {
    return Status::NotSupported("release evidence",
                                "clean-break naming is required");
  }

  const auto source = Field(root, "source");
  if (!source.ok()) return source.status();
  std::string commit;
  status = ReadString(*source.ValueOrDie(), "commit", &commit);
  if (!status.ok()) return status;
  if (commit.size() != 40 || !std::all_of(
          commit.begin(), commit.end(), [](unsigned char character) {
            return std::isxdigit(character) != 0;
          })) {
    return Status::Corruption("release evidence", "source commit is not a SHA-1");
  }

  const auto execution = Field(root, "execution");
  if (!execution.ok()) return execution.status();
  uint64_t parallelism = 0;
  status = ReadU64(*execution.ValueOrDie(), "parallelism", &parallelism);
  if (!status.ok()) return status;
  if (parallelism != 1) {
    return Status::Corruption("release evidence", "parallelism must be one");
  }
  std::string command;
  status = ReadString(*execution.ValueOrDie(), "command", &command);
  if (!status.ok()) return status;
  if (command.empty()) {
    return Status::Corruption("release evidence", "execution command is empty");
  }
  uint64_t passed = 0;
  uint64_t failed = 0;
  status = ReadU64(*execution.ValueOrDie(), "passed", &passed);
  if (!status.ok()) return status;
  status = ReadU64(*execution.ValueOrDie(), "failed", &failed);
  if (!status.ok()) return status;
  if (passed == 0 || failed != 0) {
    return Status::Corruption("release evidence", "execution result is incomplete");
  }
  for (const char* field_name : {"binary", "log"}) {
    const auto entry = Field(*execution.ValueOrDie(), field_name);
    if (!entry.ok()) return entry.status();
    std::string sha256;
    status = ReadString(*entry.ValueOrDie(), "sha256", &sha256);
    if (!status.ok()) return status;
    if (!IsSha256(sha256)) {
      return Status::Corruption("release evidence",
                                std::string(field_name) + " SHA-256 is invalid");
    }
  }

  const auto audit = Field(root, "audit");
  if (!audit.ok()) return audit.status();
  uint32_t audit_format_version = 0;
  status = ReadU32(*audit.ValueOrDie(), "format_version", &audit_format_version);
  if (!status.ok()) return status;
  if (audit_format_version != 1) {
    return Status::NotSupported("release evidence", "unsupported audit format");
  }
  bool sha256_verified = false;
  status = ReadBool(*audit.ValueOrDie(), "sha256_verified", &sha256_verified);
  if (!status.ok()) return status;
  bool provenance_bound = false;
  status = ReadBool(*audit.ValueOrDie(), "provenance_bound_to_current_binary",
                    &provenance_bound);
  if (!status.ok()) return status;
  if (!sha256_verified || !provenance_bound) {
    return Status::Corruption("release evidence",
                              "hash or binary provenance is not verified");
  }
  return Status::OK();
}

StatusOr<BenchmarkPhase> ParsePhase(const std::string& name) {
  for (uint8_t value = static_cast<uint8_t>(BenchmarkPhase::kEnvironmentCheck);
       value <= static_cast<uint8_t>(BenchmarkPhase::kArtifactFinalize); ++value) {
    const auto phase = static_cast<BenchmarkPhase>(value);
    if (name == BenchmarkPhaseName(phase)) return phase;
  }
  return Status::Corruption("benchmark artifact", "unknown benchmark phase " + name);
}

Status ReadSummary(const JsonValue& root, BenchmarkArtifactSummary* summary) {
  Status status = ReadU32(root, "artifact_schema_version", &summary->schema_version);
  if (!status.ok()) return status;
  if (summary->schema_version != 3) {
    return Status::NotSupported("benchmark artifact", "unsupported summary schema version");
  }
#define CEDAR_READ_SUMMARY_STRING(field_name, member)                 \
  status = ReadString(root, field_name, &summary->member);             \
  if (!status.ok()) return status
#define CEDAR_READ_SUMMARY_U64(field_name, member)                    \
  status = ReadU64(root, field_name, &summary->member);                \
  if (!status.ok()) return status
  CEDAR_READ_SUMMARY_STRING("measurement_mode", measurement_mode);
  CEDAR_READ_SUMMARY_STRING("cache_preparation", cache_preparation);
  CEDAR_READ_SUMMARY_STRING("maintenance_state", maintenance_state);
  CEDAR_READ_SUMMARY_U64("warmup_sample_count", warmup_sample_count);
  CEDAR_READ_SUMMARY_U64("measurement_elapsed_ns", measurement_elapsed_ns);
  CEDAR_READ_SUMMARY_U64("measured_work_units", measured_work_units);
  status = ReadDouble(root, "measurement_throughput", &summary->measurement_throughput);
  if (!status.ok()) return status;
  CEDAR_READ_SUMMARY_U64("latency_p50_ns", latency_p50_ns);
  CEDAR_READ_SUMMARY_U64("latency_p95_ns", latency_p95_ns);
  CEDAR_READ_SUMMARY_U64("latency_p99_ns", latency_p99_ns);
  CEDAR_READ_SUMMARY_U64("latency_p999_ns", latency_p999_ns);
  CEDAR_READ_SUMMARY_U64("logical_work_units", logical_work_units);
  CEDAR_READ_SUMMARY_U64("physical_read_bytes", physical_read_bytes);
  CEDAR_READ_SUMMARY_U64("physical_write_bytes", physical_write_bytes);
#undef CEDAR_READ_SUMMARY_U64
#undef CEDAR_READ_SUMMARY_STRING
  status = ReadBool(root, "physical_read_bytes_available",
                    &summary->physical_read_bytes_available);
  if (!status.ok()) return status;
  status = ReadBool(root, "physical_write_bytes_available",
                    &summary->physical_write_bytes_available);
  if (!status.ok()) return status;
  const auto breakdown = root.object.find("durable_write_bytes");
  if (breakdown != root.object.end()) {
    if (breakdown->second.kind != JsonValue::Kind::kObject) {
      return Status::Corruption(
          "benchmark artifact", "durable_write_bytes must be an object");
    }
#define CEDAR_READ_DURABLE_BYTES(field_name, member)                 \
    status = ReadU64(breakdown->second, field_name,                   \
                     &summary->durable_write_bytes.member);          \
    if (!status.ok()) return status
    CEDAR_READ_DURABLE_BYTES("wal", wal);
    CEDAR_READ_DURABLE_BYTES("decision_log", decision_log);
    CEDAR_READ_DURABLE_BYTES("sst_flush", sst_flush);
    CEDAR_READ_DURABLE_BYTES("compaction", compaction);
    CEDAR_READ_DURABLE_BYTES("blob", blob);
    CEDAR_READ_DURABLE_BYTES("manifest", manifest);
#undef CEDAR_READ_DURABLE_BYTES
  }
  status = ReadBool(root, "metrics_artifact_present", &summary->metrics_artifact_present);
  if (!status.ok()) return status;
  status = ReadBool(root, "histograms_artifact_present", &summary->histograms_artifact_present);
  if (!status.ok()) return status;
  status = ReadBool(root, "traces_artifact_present", &summary->traces_artifact_present);
  if (!status.ok()) return status;
  status = ReadBool(root, "explain_artifact_present", &summary->explain_artifact_present);
  if (!status.ok()) return status;

  const auto metrics = Field(root, "derived_metrics");
  if (!metrics.ok()) return metrics.status();
#define CEDAR_READ_RATIO(field_name, member)                          \
  status = ReadRatio(*metrics.ValueOrDie(), field_name,                \
                     &summary->derived_metrics.member);               \
  if (!status.ok()) return status
  CEDAR_READ_RATIO("write_amplification", write_amplification);
  CEDAR_READ_RATIO("read_amplification", read_amplification);
  CEDAR_READ_RATIO("space_amplification", space_amplification);
  CEDAR_READ_RATIO("index_survival", index_survival);
  CEDAR_READ_RATIO("interval_survival", interval_survival);
  CEDAR_READ_RATIO("blob_materialization", blob_materialization);
  CEDAR_READ_RATIO("cache_admission", cache_admission);
  CEDAR_READ_RATIO("maintenance_share", maintenance_share);
#undef CEDAR_READ_RATIO
  const auto lag = Field(*metrics.ValueOrDie(), "visible_prefix_lag");
  if (!lag.ok()) return lag.status();
  bool lag_defined = false;
  status = ReadBool(*lag.ValueOrDie(), "defined", &lag_defined);
  if (!status.ok()) return status;
  status = ReadU64(*lag.ValueOrDie(), "committed_seq",
                   &summary->derived_metrics.visible_prefix_lag.committed_seq);
  if (!status.ok()) return status;
  status = ReadU64(*lag.ValueOrDie(), "visible_seq",
                   &summary->derived_metrics.visible_prefix_lag.visible_seq);
  if (!status.ok()) return status;
  summary->derived_metrics.visible_prefix_lag.available = lag_defined;
  const bool valid_lag = lag_defined &&
      summary->derived_metrics.visible_prefix_lag.committed_seq >=
          summary->derived_metrics.visible_prefix_lag.visible_seq;
  if (lag_defined != valid_lag) {
    return Status::Corruption("benchmark artifact", "visible_prefix_lag definition mismatch");
  }
  const auto lag_value = Field(*lag.ValueOrDie(), "value");
  if (!lag_value.ok()) return lag_value.status();
  if ((lag_defined && lag_value.ValueOrDie()->kind != JsonValue::Kind::kNumber) ||
      (!lag_defined && lag_value.ValueOrDie()->kind != JsonValue::Kind::kNull)) {
    return Status::Corruption("benchmark artifact", "visible_prefix_lag value mismatch");
  }

  status = ReadTransactionMeasurements(root, &summary->transaction_measurements);
  if (!status.ok()) return status;

  const auto phases = Field(root, "phases");
  if (!phases.ok()) return phases.status();
  status = RequireKind(*phases.ValueOrDie(), JsonValue::Kind::kArray, "phases");
  if (!status.ok()) return status;
  for (const JsonValue& item : phases.ValueOrDie()->array) {
    std::string name;
    status = ReadString(item, "name", &name);
    if (!status.ok()) return status;
    const auto parsed_phase = ParsePhase(name);
    if (!parsed_phase.ok()) return parsed_phase.status();
    BenchmarkPhaseRecord record;
    record.phase = parsed_phase.ValueOrDie();
    status = ReadU64(item, "elapsed_ns", &record.elapsed_ns);
    if (!status.ok()) return status;
    status = ReadString(item, "terminal_status", &record.terminal_status);
    if (!status.ok()) return status;
    summary->phases.push_back(std::move(record));
  }

  const auto samples = Field(root, "measured_samples");
  if (!samples.ok()) return samples.status();
  status = RequireKind(*samples.ValueOrDie(), JsonValue::Kind::kArray, "measured_samples");
  if (!status.ok()) return status;
  for (const JsonValue& item : samples.ValueOrDie()->array) {
    BenchmarkOperationSample sample;
    status = ReadU64(item, "requested_arrival_ns", &sample.requested_arrival_ns);
    if (!status.ok()) return status;
    status = ReadU64(item, "admitted_ns", &sample.admitted_ns);
    if (!status.ok()) return status;
    status = ReadU64(item, "started_ns", &sample.started_ns);
    if (!status.ok()) return status;
    status = ReadU64(item, "completed_ns", &sample.completed_ns);
    if (!status.ok()) return status;
    status = ReadString(item, "terminal_status", &sample.terminal_status);
    if (!status.ok()) return status;
    summary->measured_samples.push_back(std::move(sample));
  }
  return Status::OK();
}

bool ProtocolComplete(const BenchmarkArtifactSummary& summary) {
  if (summary.phases.size() != 11) return false;
  for (size_t index = 0; index < summary.phases.size(); ++index) {
    if (summary.phases[index].phase != static_cast<BenchmarkPhase>(index) ||
        summary.phases[index].terminal_status != "PASS") {
      return false;
    }
  }
  return true;
}

Status ReadVerification(const JsonValue& root, BenchmarkVerification* verification,
                        bool* protocol_complete) {
  uint32_t schema_version = 0;
  Status status = ReadU32(root, "verification_schema_version", &schema_version);
  if (!status.ok()) return status;
  if (schema_version != 1) {
    return Status::NotSupported("benchmark artifact", "unsupported verification schema version");
  }
  status = ReadBool(root, "protocol_complete", protocol_complete);
  if (!status.ok()) return status;
  status = ReadBool(root, "load_passed", &verification->load_passed);
  if (!status.ok()) return status;
  status = ReadBool(root, "result_passed", &verification->result_passed);
  if (!status.ok()) return status;
  status = ReadBool(root, "reopen_passed", &verification->reopen_passed);
  if (!status.ok()) return status;
  status = ReadString(root, "result_checksum", &verification->result_checksum);
  if (!status.ok()) return status;
  status = ReadString(root, "detail", &verification->detail);
  if (!status.ok()) return status;
  std::string archived_status;
  status = ReadString(root, "status", &archived_status);
  if (!status.ok()) return status;
  const bool passed = *protocol_complete && verification->load_passed &&
      verification->result_passed && verification->reopen_passed;
  if (archived_status != (passed ? "PASS" : "INVALID")) {
    return Status::Corruption("benchmark artifact", "verification status mismatch");
  }
  return Status::OK();
}

Status WriteAll(int fd, const std::string& content, const std::string& path) {
  const char* cursor = content.data();
  size_t remaining = content.size();
  while (remaining != 0) {
    const ssize_t written = ::write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    cursor += written;
    remaining -= static_cast<size_t>(written);
  }
  return Status::OK();
}

Status WriteReportAtomically(const std::string& path, const std::string& report) {
  const std::string temporary = path + ".tmp";
  const int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  Status status = WriteAll(fd, report, temporary);
  if (status.ok() && ::fsync(fd) != 0) {
    status = Status::IOError(temporary, std::strerror(errno));
  }
  if (::close(fd) != 0 && status.ok()) {
    status = Status::IOError(temporary, std::strerror(errno));
  }
  if (!status.ok()) {
    ::unlink(temporary.c_str());
    return status;
  }
  if (::rename(temporary.c_str(), path.c_str()) != 0) {
    const Status rename_status = Status::IOError(path, std::strerror(errno));
    ::unlink(temporary.c_str());
    return rename_status;
  }
  const std::filesystem::path parent = std::filesystem::path(path).parent_path();
  const int directory_fd = ::open(parent.c_str(), O_RDONLY);
  if (directory_fd < 0) return Status::IOError(parent.string(), std::strerror(errno));
  if (::fsync(directory_fd) != 0) {
    const Status fsync_status = Status::IOError(parent.string(), std::strerror(errno));
    ::close(directory_fd);
    return fsync_status;
  }
  if (::close(directory_fd) != 0) {
    return Status::IOError(parent.string(), std::strerror(errno));
  }
  return Status::OK();
}

}  // namespace

StatusOr<BenchmarkArtifactRecord> ReadBenchmarkArtifact(
    const std::string& run_directory) {
  if (run_directory.empty()) {
    return Status::InvalidArgument("benchmark artifact", "run directory is required");
  }
  const std::filesystem::path directory(run_directory);
  const auto manifest_json = ReadJson((directory / "manifest.json").string());
  if (!manifest_json.ok()) return manifest_json.status();
  const auto summary_json = ReadJson((directory / "summary.json").string());
  if (!summary_json.ok()) return summary_json.status();
  const auto verification_json = ReadJson((directory / "verification.json").string());
  if (!verification_json.ok()) return verification_json.status();

  BenchmarkArtifactRecord record;
  Status status = ReadManifest(manifest_json.ValueOrDie(), &record.manifest,
                               &record.run_id);
  if (!status.ok()) return status;
  status = ReadSummary(summary_json.ValueOrDie(), &record.summary);
  if (!status.ok()) return status;
  bool archived_protocol_complete = false;
  status = ReadVerification(verification_json.ValueOrDie(), &record.verification,
                            &archived_protocol_complete);
  if (!status.ok()) return status;
  const bool derived_protocol_complete = ProtocolComplete(record.summary);
  if (archived_protocol_complete != derived_protocol_complete) {
    return Status::Corruption("benchmark artifact",
                              "protocol completeness mismatch");
  }
  record.protocol_complete = derived_protocol_complete;
  return record;
}

Status ValidateReleaseEvidenceManifest(std::string_view manifest_json) {
  if (manifest_json.size() > kMaximumArtifactBytes) {
    return Status::ResourceExhausted("release evidence",
                                    "manifest exceeds 64 MiB");
  }
  const auto parsed = JsonParser(manifest_json).Parse();
  if (!parsed.ok()) return parsed.status();
  return ValidateReleaseEvidenceManifestJson(parsed.ValueOrDie());
}

Status VerifyReleaseEvidenceDirectory(const std::string& evidence_directory) {
  if (evidence_directory.empty()) {
    return Status::InvalidArgument("release evidence", "evidence directory is required");
  }
  if (evidence_directory.find('\0') != std::string::npos ||
      evidence_directory.back() == '/') {
    return Status::InvalidArgument("release evidence",
                                   "evidence directory is invalid");
  }
  const int opened_root = ::open(evidence_directory.c_str(),
                                 O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                                     O_CLOEXEC);
  if (opened_root < 0) {
    return Status::InvalidArgument("release evidence", "evidence directory is invalid");
  }
  ScopedFd root_fd(opened_root);
  struct stat root_metadata {};
  if (::fstat(root_fd.get(), &root_metadata) != 0 ||
      !S_ISDIR(root_metadata.st_mode)) {
    return Status::InvalidArgument("release evidence", "evidence directory is invalid");
  }
  const auto entries = ReadSha256Sums(root_fd.get());
  if (!entries.ok()) return entries.status();
  const auto manifest_entry = entries.ValueOrDie().find("manifest.json");
  if (manifest_entry == entries.ValueOrDie().end()) {
    return Status::Corruption("release evidence", "manifest.json is not in SHA256SUMS");
  }
  std::string manifest;
  for (const auto& entry : entries.ValueOrDie()) {
    auto opened = OpenEvidenceFileAt(root_fd.get(), entry.first);
    if (!opened.ok()) return opened.status();
    ScopedFd file = std::move(opened).ConsumeValueOrDie();
    std::string* captured = entry.first == "manifest.json" ? &manifest : nullptr;
    const auto actual = Sha256Fd(file.get(), entry.first, captured);
    if (!actual.ok()) return actual.status();
    if (actual.ValueOrDie() != entry.second) {
      return Status::Corruption("release evidence",
                                "SHA256SUMS digest mismatch for " + entry.first);
    }
  }
  const Status manifest_status = ValidateReleaseEvidenceManifest(manifest);
  if (!manifest_status.ok()) return manifest_status;
  const auto parsed = JsonParser(manifest).Parse();
  if (!parsed.ok()) return parsed.status();
  const auto execution = Field(parsed.ValueOrDie(), "execution");
  if (!execution.ok()) return execution.status();
  for (const char* name : {"binary", "log"}) {
    const auto entry = Field(*execution.ValueOrDie(), name);
    if (!entry.ok()) return entry.status();
    std::string path;
    std::string digest;
    Status status = ReadString(*entry.ValueOrDie(), "path", &path);
    if (!status.ok()) return status;
    status = ReadString(*entry.ValueOrDie(), "sha256", &digest);
    if (!status.ok()) return status;
    const auto relative = EvidenceRelativePath(path);
    if (!relative.ok()) return relative.status();
    const auto ledger = entries.ValueOrDie().find(relative.ValueOrDie().generic_string());
    if (ledger == entries.ValueOrDie().end() || ledger->second != Lowercase(digest)) {
      return Status::Corruption("release evidence",
                                std::string(name) + " digest is not bound by SHA256SUMS");
    }
  }
  return Status::OK();
}

Status VerifySha256LedgerDirectory(
    const std::string& directory,
    const std::vector<std::string>& expected_relative_paths) {
  if (directory.empty() || directory.find('\0') != std::string::npos ||
      directory.back() == '/') {
    return Status::InvalidArgument("SHA-256 ledger",
                                   "directory is invalid");
  }
  const int opened_root = ::open(directory.c_str(),
                                 O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                                     O_CLOEXEC);
  if (opened_root < 0) {
    return Status::InvalidArgument("SHA-256 ledger",
                                   "directory is invalid");
  }
  ScopedFd root_fd(opened_root);
  const auto entries = ReadSha256Sums(root_fd.get());
  if (!entries.ok()) return entries.status();
  std::set<std::string> expected;
  for (const std::string& path : expected_relative_paths) {
    const auto relative = EvidenceRelativePath(path);
    if (!relative.ok()) return relative.status();
    if (!expected.insert(relative.ValueOrDie().generic_string()).second) {
      return Status::InvalidArgument("SHA-256 ledger",
                                     "expected path is duplicated");
    }
  }
  std::set<std::string> actual;
  for (const auto& entry : entries.ValueOrDie()) actual.insert(entry.first);
  if (actual != expected) {
    return Status::Corruption("SHA-256 ledger",
                              "ledger paths do not match expected files");
  }
  for (const auto& entry : entries.ValueOrDie()) {
    auto opened = OpenEvidenceFileAt(root_fd.get(), entry.first);
    if (!opened.ok()) return opened.status();
    ScopedFd file = std::move(opened).ConsumeValueOrDie();
    const auto digest = Sha256Fd(file.get(), entry.first, nullptr);
    if (!digest.ok()) return digest.status();
    if (digest.ValueOrDie() != entry.second) {
      return Status::Corruption("SHA-256 ledger",
                                "digest mismatch for " + entry.first);
    }
  }
  return Status::OK();
}

StatusOr<std::string> BenchmarkFileSha256(const std::string& path) {
  return Sha256File(std::filesystem::path(path));
}

namespace {

bool DescriptorIsCloseOnExec(int descriptor) {
  const int flags = ::fcntl(descriptor, F_GETFD);
  return flags >= 0 && (flags & FD_CLOEXEC) != 0;
}

}  // namespace

StatusOr<BenchmarkExecutableSnapshot> CreateBenchmarkExecutableSnapshot(
    const std::string& path) {
  if (path.empty() || path.find('\0') != std::string::npos) {
    return Status::InvalidArgument("benchmark executable", "path is invalid");
  }
  ScopedFd source(::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
  if (source.get() < 0) {
    return Status::IOError(path, std::strerror(errno));
  }
  struct stat metadata {};
  if (::fstat(source.get(), &metadata) != 0 || !S_ISREG(metadata.st_mode) ||
      (metadata.st_mode & (S_IXUSR | S_IXGRP | S_IXOTH)) == 0) {
    return Status::InvalidArgument("benchmark executable",
                                   "source is not an executable regular file");
  }

  std::filesystem::path temporary_directory =
      std::filesystem::temp_directory_path() / "cedar-bench-snapshot-XXXXXX";
  std::string directory_writable = temporary_directory.string();
  directory_writable.push_back('\0');
  if (::mkdtemp(directory_writable.data()) == nullptr) {
    return Status::IOError("benchmark executable snapshot",
                           std::strerror(errno));
  }
  const std::string snapshot_directory(directory_writable.data());
  const std::string snapshot_path = snapshot_directory + "/benchmark";
  const int writer_fd = ::open(snapshot_path.c_str(),
                               O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC,
                               S_IRUSR | S_IWUSR);
  if (writer_fd < 0) {
    ::rmdir(snapshot_directory.c_str());
    return Status::IOError("benchmark executable snapshot",
                           std::strerror(errno));
  }
  ScopedFd writer(writer_fd);
  if (::fchmod(writer.get(), S_IRUSR | S_IWUSR | S_IXUSR) != 0) {
    return Status::IOError("benchmark executable snapshot",
                           std::strerror(errno));
  }

  Sha256 sha256;
  blake3_hasher blake3;
  blake3_hasher_init(&blake3);
  std::array<uint8_t, 64 * 1024> buffer{};
  while (true) {
    const ssize_t read = ::read(source.get(), buffer.data(), buffer.size());
    if (read < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    if (read == 0) break;
    const size_t size = static_cast<size_t>(read);
    sha256.Update(buffer.data(), size);
    blake3_hasher_update(&blake3, buffer.data(), size);
    size_t offset = 0;
    while (offset < size) {
      const ssize_t written =
          ::write(writer.get(), buffer.data() + offset, size - offset);
      if (written < 0) {
        if (errno == EINTR) continue;
        return Status::IOError("benchmark executable snapshot",
                               std::strerror(errno));
      }
      offset += static_cast<size_t>(written);
    }
  }
  if (::fsync(writer.get()) != 0 ||
      ::fchmod(writer.get(), S_IRUSR | S_IXUSR) != 0) {
    return Status::IOError("benchmark executable snapshot",
                           std::strerror(errno));
  }
  ScopedFd snapshot(
      ::open(snapshot_path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
  if (snapshot.get() < 0) {
    return Status::IOError("benchmark executable snapshot",
                           std::strerror(errno));
  }
  struct stat writer_metadata {};
  struct stat snapshot_metadata {};
  if (::fstat(writer.get(), &writer_metadata) != 0 ||
      ::fstat(snapshot.get(), &snapshot_metadata) != 0 ||
      writer_metadata.st_dev != snapshot_metadata.st_dev ||
      writer_metadata.st_ino != snapshot_metadata.st_ino) {
    return Status::IOError("benchmark executable snapshot",
                           "snapshot inode changed during creation");
  }
  writer = ScopedFd();
#if defined(__APPLE__)
  ScopedFd directory_fd(
      ::open(snapshot_directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
  if (directory_fd.get() < 0) {
    return Status::IOError("benchmark executable snapshot",
                           std::strerror(errno));
  }
  ScopedFd monitor(::kqueue());
  if (monitor.get() < 0) {
    return Status::IOError("benchmark executable snapshot",
                           std::strerror(errno));
  }
  if (::fcntl(monitor.get(), F_SETFD, FD_CLOEXEC) != 0) {
    return Status::IOError("benchmark executable snapshot",
                           std::strerror(errno));
  }
  struct kevent changes[2];
  const uint32_t notes = NOTE_WRITE | NOTE_DELETE |
      NOTE_RENAME | NOTE_REVOKE;
  EV_SET(&changes[0], snapshot.get(), EVFILT_VNODE,
         EV_ADD | EV_ENABLE | EV_CLEAR, notes, 0, nullptr);
  EV_SET(&changes[1], directory_fd.get(), EVFILT_VNODE,
         EV_ADD | EV_ENABLE | EV_CLEAR, notes, 0, nullptr);
  if (::kevent(monitor.get(), changes, 2, nullptr, 0, nullptr) != 0) {
    return Status::IOError("benchmark executable snapshot",
                           std::strerror(errno));
  }
#endif
  std::array<uint8_t, 32> blake3_bytes{};
  blake3_hasher_finalize(&blake3, blake3_bytes.data(), blake3_bytes.size());
  BlobHash blake3_hash;
  blake3_hash.bytes = blake3_bytes;

  BenchmarkExecutableSnapshot result;
  result.path = snapshot_path;
  result.blake3 = BlobHashHex(blake3_hash);
  result.sha256 = sha256.FinalHex();
  result.state = std::make_shared<BenchmarkExecutableSnapshotState>(
      snapshot.Release(), snapshot_path, snapshot_directory,
#if defined(__APPLE__)
      directory_fd.Release(), monitor.Release()
#else
      -1, -1
#endif
  );
  const Status verified = VerifyBenchmarkExecutableSnapshot(result);
  if (!verified.ok()) return verified;
  return result;
}

Status VerifyBenchmarkExecutableSnapshot(
    const BenchmarkExecutableSnapshot& snapshot) {
  if (snapshot.state == nullptr || snapshot.fd() < 0 ||
      snapshot.path.empty()) {
    return Status::InvalidArgument("benchmark executable snapshot",
                                   "snapshot is incomplete");
  }
  if (!DescriptorIsCloseOnExec(snapshot.fd())) {
    snapshot.state->tampered = true;
  }
#if defined(__APPLE__)
  if (snapshot.state->monitor_fd < 0 || snapshot.state->directory_fd < 0) {
    return Status::Corruption("benchmark executable snapshot",
                              "tamper monitor is unavailable");
  }
  if (!DescriptorIsCloseOnExec(snapshot.state->monitor_fd) ||
      !DescriptorIsCloseOnExec(snapshot.state->directory_fd)) {
    snapshot.state->tampered = true;
  }
  struct kevent event;
  const timespec immediate{0, 0};
  const int observed = ::kevent(snapshot.state->monitor_fd, nullptr, 0,
                                &event, 1, &immediate);
  if (observed < 0) {
    return Status::Corruption("benchmark executable snapshot",
                              "tamper monitor failed");
  }
  if (observed != 0) snapshot.state->tampered = true;
#endif
  struct stat descriptor_metadata {};
  struct stat path_metadata {};
  if (::fstat(snapshot.fd(), &descriptor_metadata) != 0 ||
      ::lstat(snapshot.path.c_str(), &path_metadata) != 0 ||
      !S_ISREG(path_metadata.st_mode) ||
      descriptor_metadata.st_dev != path_metadata.st_dev ||
      descriptor_metadata.st_ino != path_metadata.st_ino) {
    snapshot.state->tampered = true;
  }
  const int hash_fd = ::dup(snapshot.fd());
  if (hash_fd < 0 || ::lseek(hash_fd, 0, SEEK_SET) < 0) {
    if (hash_fd >= 0) ::close(hash_fd);
    return Status::Corruption("benchmark executable snapshot",
                              "cannot verify snapshot bytes");
  }
  ScopedFd verifier(hash_fd);
  const auto actual_sha256 = Sha256Fd(
      verifier.get(), "benchmark executable snapshot", nullptr);
  if (!actual_sha256.ok() || actual_sha256.ValueOrDie() != snapshot.sha256) {
    snapshot.state->tampered = true;
  }
  if (snapshot.state->tampered) {
    return Status::Corruption("benchmark executable snapshot",
                              "snapshot path or bytes changed");
  }
  return Status::OK();
}

StatusOr<BenchmarkBinaryProvenance> ReadBenchmarkBinaryProvenance(
    const BenchmarkExecutableSnapshot& snapshot) {
  Status verified = VerifyBenchmarkExecutableSnapshot(snapshot);
  if (!verified.ok()) return verified;
  int descriptors[2];
  if (::pipe(descriptors) != 0) {
    return Status::IOError("benchmark executable provenance",
                           std::strerror(errno));
  }
  const pid_t child = ::fork();
  if (child < 0) {
    ::close(descriptors[0]);
    ::close(descriptors[1]);
    return Status::IOError("benchmark executable provenance",
                           std::strerror(errno));
  }
  if (child == 0) {
    ::close(descriptors[0]);
    if (::dup2(descriptors[1], STDOUT_FILENO) < 0) _exit(127);
    ::close(descriptors[1]);
#if defined(__linux__)
    const int executable_fd = ::dup(snapshot.fd());
    if (executable_fd < 0) _exit(127);
    const std::string executable =
        "/proc/self/fd/" + std::to_string(executable_fd);
    char* const arguments[] = {
        const_cast<char*>(executable.c_str()),
        const_cast<char*>("--build-provenance"), nullptr};
    ::fexecve(executable_fd, arguments, environ);
#else
    ::execl(snapshot.path.c_str(), snapshot.path.c_str(),
            "--build-provenance", nullptr);
#endif
    _exit(127);
  }
  ::close(descriptors[1]);
  std::string output;
  std::array<char, 512> buffer{};
  for (;;) {
    const ssize_t count = ::read(descriptors[0], buffer.data(), buffer.size());
    if (count > 0) {
      if (output.size() + static_cast<size_t>(count) > 4096) {
        ::close(descriptors[0]);
        while (::waitpid(child, nullptr, 0) < 0 && errno == EINTR) {}
        return Status::Corruption("benchmark executable provenance",
                                  "probe output is too large");
      }
      output.append(buffer.data(), static_cast<size_t>(count));
      continue;
    }
    if (count < 0 && errno == EINTR) continue;
    if (count < 0) {
      const Status read_status = Status::IOError(
          "benchmark executable provenance", std::strerror(errno));
      ::close(descriptors[0]);
      while (::waitpid(child, nullptr, 0) < 0 && errno == EINTR) {}
      return read_status;
    }
    break;
  }
  ::close(descriptors[0]);
  int child_status = 0;
  while (::waitpid(child, &child_status, 0) < 0) {
    if (errno != EINTR) {
      return Status::IOError("benchmark executable provenance",
                             std::strerror(errno));
    }
  }
  if (!WIFEXITED(child_status) || WEXITSTATUS(child_status) != 0) {
    return Status::Corruption("benchmark executable provenance",
                              "probe exited unsuccessfully");
  }
  if (output.empty() || output.back() != '\n' ||
      output.find('\n') != output.size() - 1) {
    return Status::Corruption("benchmark executable provenance",
                              "probe output is malformed");
  }
  output.pop_back();
  std::istringstream input(output);
  std::vector<std::string> fields;
  for (std::string field; input >> field;) fields.push_back(std::move(field));
  if (fields.size() != 4) {
    return Status::Corruption("benchmark executable provenance",
                              "probe field count is invalid");
  }
  const auto value = [&](size_t index, const char* prefix)
      -> StatusOr<std::string> {
    const std::string name(prefix);
    if (fields[index].rfind(name, 0) != 0 ||
        fields[index].size() == name.size()) {
      return Status::Corruption("benchmark executable provenance",
                                "probe field is malformed");
    }
    return fields[index].substr(name.size());
  };
  const auto commit = value(0, "source_commit=");
  const auto dirty = value(1, "source_dirty=");
  const auto instrumentation = value(2, "instrumentation_profile_id=");
  const auto format = value(3, "database_format_version=");
  if (!commit.ok() || !dirty.ok() || !instrumentation.ok() || !format.ok() ||
      (dirty.ok() && dirty.ValueOrDie() != "0" &&
       dirty.ValueOrDie() != "1")) {
    return Status::Corruption("benchmark executable provenance",
                              "probe values are malformed");
  }
  const auto parsed_format = ParseBenchmarkUnsigned(format.ValueOrDie());
  if (!parsed_format.ok() ||
      parsed_format.ValueOrDie() > std::numeric_limits<uint32_t>::max()) {
    return Status::Corruption("benchmark executable provenance",
                              "probe format is malformed");
  }
  BenchmarkBinaryProvenance provenance;
  provenance.source_commit = commit.ValueOrDie();
  provenance.source_dirty = dirty.ValueOrDie() == "1";
  provenance.instrumentation_profile_id = instrumentation.ValueOrDie();
  provenance.database_format_version =
      static_cast<uint32_t>(parsed_format.ValueOrDie());
  verified = VerifyBenchmarkExecutableSnapshot(snapshot);
  if (!verified.ok()) return verified;
  return provenance;
}

Status ValidateProductionMetricArtifact(
    std::string_view metrics_json,
    const std::vector<MetricActivityRequirement>& activity_requirements) {
  if (metrics_json.size() > kMaximumArtifactBytes) {
    return Status::ResourceExhausted("production metrics",
                                    "metrics snapshot exceeds 64 MiB");
  }
  const auto parsed = JsonParser(metrics_json).Parse();
  if (!parsed.ok()) return parsed.status();
  uint32_t schema_version = 0;
  Status status = ReadU32(parsed.ValueOrDie(), "metric_schema_version",
                          &schema_version);
  if (!status.ok()) return status;
  if (schema_version != 1) {
    return Status::NotSupported("production metrics",
                                "unsupported metric schema version");
  }

  const std::vector<MetricDefinition>& expected = ProductionMetricDefinitions();
  std::map<std::string, const MetricDefinition*> expected_by_name;
  for (const MetricDefinition& definition : expected) {
    if (!expected_by_name.emplace(definition.name, &definition).second) {
      return Status::Corruption("production metrics",
                                "duplicate required metric definition");
    }
  }
  std::map<std::string, MetricDefinition> observed_definitions;
  std::map<std::pair<std::string, std::string>, uint64_t> activity;

  const auto parse_family = [&](const char* family, bool histogram) -> Status {
    const auto field = Field(parsed.ValueOrDie(), family);
    if (!field.ok()) return field.status();
    status = RequireKind(*field.ValueOrDie(), JsonValue::Kind::kArray, family);
    if (!status.ok()) return status;
    for (const JsonValue& item : field.ValueOrDie()->array) {
      std::string name;
      std::string type;
      std::string unit;
      uint32_t item_schema = 0;
      status = ReadString(item, "name", &name);
      if (!status.ok()) return status;
      status = ReadString(item, "type", &type);
      if (!status.ok()) return status;
      status = ReadString(item, "unit", &unit);
      if (!status.ok()) return status;
      status = ReadU32(item, "schema_version", &item_schema);
      if (!status.ok()) return status;
      const auto expected_definition = expected_by_name.find(name);
      const MetricDefinition* requirement = expected_definition ==
              expected_by_name.end()
          ? nullptr
          : expected_definition->second;
      if (requirement != nullptr) {
        const std::string expected_type =
            requirement->type == MetricType::kCounter
                ? "counter"
                : requirement->type == MetricType::kGauge ? "gauge" : "histogram";
        if (type != expected_type || unit != requirement->unit ||
            item_schema != requirement->schema_version || histogram !=
                (requirement->type == MetricType::kHistogram)) {
          return Status::InvalidArgument("production metrics",
                                         "metric schema mismatch for " + name);
        }
        if (!observed_definitions.emplace(
                name, MetricDefinition{name, requirement->type, unit,
                                       item_schema, requirement->histogram_bounds})
                 .second) {
          return Status::Corruption("production metrics",
                                    "duplicate exported metric " + name);
        }
      }
      if (histogram && requirement != nullptr) {
        const auto bounds = Field(item, "bounds");
        if (!bounds.ok()) return bounds.status();
        status = RequireKind(*bounds.ValueOrDie(), JsonValue::Kind::kArray,
                             "bounds");
        if (!status.ok()) return status;
        if (bounds.ValueOrDie()->array.size() !=
            requirement->histogram_bounds.size()) {
          return Status::InvalidArgument("production metrics",
                                         "histogram bounds mismatch for " + name);
        }
        for (size_t index = 0; index < bounds.ValueOrDie()->array.size(); ++index) {
          const JsonValue& bound = bounds.ValueOrDie()->array[index];
          if (bound.kind != JsonValue::Kind::kNumber) {
            return Status::Corruption("production metrics",
                                      "histogram bound is not numeric");
          }
          uint64_t value = 0;
          const auto parsed_bound = std::from_chars(
              bound.scalar.data(), bound.scalar.data() + bound.scalar.size(),
              value);
          if (parsed_bound.ec != std::errc() ||
              parsed_bound.ptr != bound.scalar.data() + bound.scalar.size() ||
              value != requirement->histogram_bounds[index]) {
            return Status::InvalidArgument("production metrics",
                                           "histogram bounds mismatch for " + name);
          }
        }
      }
      const auto values = Field(item, "values");
      if (!values.ok()) return values.status();
      status = RequireKind(*values.ValueOrDie(), JsonValue::Kind::kObject,
                           "values");
      if (!status.ok()) return status;
      for (const auto& label : values.ValueOrDie()->object) {
        uint64_t value = 0;
        if (histogram) {
          status = ReadU64(label.second, "count", &value);
        } else {
          if (label.second.kind != JsonValue::Kind::kNumber) {
            return Status::Corruption("production metrics",
                                      "metric value is not numeric");
          }
          const auto parsed_value = std::from_chars(
              label.second.scalar.data(),
              label.second.scalar.data() + label.second.scalar.size(), value);
          if (parsed_value.ec != std::errc() ||
              parsed_value.ptr != label.second.scalar.data() +
                                      label.second.scalar.size()) {
            return Status::Corruption("production metrics",
                                      "metric value is not UInt64");
          }
        }
        if (!status.ok()) return status;
        activity[{name, label.first}] = value;
      }
    }
    return Status::OK();
  };

  status = parse_family("metrics", false);
  if (!status.ok()) return status;
  status = parse_family("histograms", true);
  if (!status.ok()) return status;
  if (observed_definitions.size() < expected_by_name.size()) {
    for (const auto& definition : expected_by_name) {
      if (observed_definitions.find(definition.first) ==
          observed_definitions.end()) {
        return Status::InvalidArgument("production metrics",
                                       "missing metric " + definition.first);
      }
    }
  }
  for (const MetricActivityRequirement& requirement : activity_requirements) {
    const auto value = activity.find({requirement.name, requirement.label});
    if (value == activity.end()) {
      return Status::InvalidArgument(
          "production metrics", "missing activity " + requirement.name +
              "[" + requirement.label + "]");
    }
    if (value->second < requirement.minimum) {
      return Status::InvalidArgument(
          "production metrics", "activity below minimum " + requirement.name +
              "[" + requirement.label + "]");
    }
  }
  return Status::OK();
}

Status ValidateBenchmarkArtifactProductionMetrics(
    const std::string& run_directory,
    const std::vector<MetricActivityRequirement>& activity_requirements) {
  const auto record = ReadBenchmarkArtifact(run_directory);
  if (!record.ok()) return record.status();
  if (!record.ValueOrDie().summary.metrics_artifact_present) {
    return Status::InvalidArgument("production metrics",
                                   "benchmark artifact has no metrics.json");
  }
  const std::filesystem::path path =
      std::filesystem::path(run_directory) / "metrics.json";
  const auto metrics = ReadArtifact(path.string());
  if (!metrics.ok()) return metrics.status();
  return ValidateProductionMetricArtifact(metrics.ValueOrDie(),
                                          activity_requirements);
}

Status RegenerateBenchmarkReport(const std::string& run_directory) {
  const auto record = ReadBenchmarkArtifact(run_directory);
  if (!record.ok()) return record.status();
  return WriteReportAtomically(
      (std::filesystem::path(run_directory) / "report.md").string(),
      BuildBenchmarkReport(record.ValueOrDie().manifest,
                           record.ValueOrDie().summary,
                           record.ValueOrDie().verification,
                           record.ValueOrDie().protocol_complete));
}

Status ValidatePairedCampaignSeedIdentity(
    uint32_t paired_schema_version, uint64_t ledger_generator_seed,
    uint64_t ledger_order_seed, uint64_t artifact_generator_seed,
    uint64_t expected_generator_seed, uint64_t expected_order_seed) {
  if (paired_schema_version != 2 ||
      ledger_generator_seed != expected_generator_seed ||
      ledger_order_seed != expected_order_seed ||
      artifact_generator_seed != expected_generator_seed) {
    return Status::Corruption("paired benchmark output",
                              "paired campaign seed identity mismatches");
  }
  return Status::OK();
}

Status VerifyPairedBenchmarkOutput(const std::string& output_root,
                                   uint32_t expected_pair_count,
                                   const std::string& expected_profile,
                                   const std::string& expected_workload,
                                   const std::string& expected_cache_mode,
                                   const std::string& expected_baseline_blake3,
                                   const std::string& expected_candidate_blake3,
                                   const std::string& expected_baseline_sha256,
                                   const std::string& expected_candidate_sha256,
                                   std::optional<uint64_t> expected_seed,
                                   std::optional<uint64_t> expected_order_seed) {
  if (expected_pair_count < 5) {
    return Status::InvalidArgument("paired benchmark output",
                                   "expected pair count must be at least five");
  }
  std::error_code error;
  const std::filesystem::path root =
      std::filesystem::canonical(output_root, error);
  if (error || !std::filesystem::is_directory(root, error) || error) {
    return Status::IOError(output_root, "paired output root is unavailable");
  }
  const auto gate = ReadJson((root / "regression-gate.json").string());
  if (!gate.ok()) return gate.status();
  uint32_t schema_version = 0;
  Status status = ReadU32(gate.ValueOrDie(), "regression_schema_version",
                          &schema_version);
  if (!status.ok()) return status;
  std::string gate_status;
  status = ReadString(gate.ValueOrDie(), "status", &gate_status);
  if (!status.ok()) return status;
  bool release_gate_passed = false;
  status = ReadBool(gate.ValueOrDie(), "release_gate_passed",
                    &release_gate_passed);
  if (!status.ok()) return status;
  uint64_t pair_count = 0;
  status = ReadU64(gate.ValueOrDie(), "pair_count", &pair_count);
  if (!status.ok()) return status;
  if (schema_version != 1 || gate_status != "PASS" ||
      !release_gate_passed || pair_count != expected_pair_count) {
    return Status::Corruption("paired benchmark output",
                              "release gate is not a matching PASS");
  }
  std::string archived_baseline_key_id;
  status = ReadString(gate.ValueOrDie(), "baseline_key_id",
                      &archived_baseline_key_id);
  if (!status.ok()) return status;
  std::string archived_candidate_key_id;
  status = ReadString(gate.ValueOrDie(), "candidate_key_id",
                      &archived_candidate_key_id);
  if (!status.ok()) return status;
  if (!expected_profile.empty()) {
    const auto preflight =
        ReadJson((root / "production-preflight.json").string());
    if (!preflight.ok()) return preflight.status();
    uint32_t preflight_schema = 0;
    status = ReadU32(preflight.ValueOrDie(),
                     "production_preflight_schema_version",
                     &preflight_schema);
    if (!status.ok()) return status;
    bool approved = false;
    status = ReadBool(preflight.ValueOrDie(),
                      "approved_production_baseline", &approved);
    if (!status.ok()) return status;
    std::string profile;
    status = ReadString(preflight.ValueOrDie(), "profile", &profile);
    if (!status.ok()) return status;
    std::string workload;
    status = ReadString(preflight.ValueOrDie(), "workload", &workload);
    if (!status.ok()) return status;
    std::string cache_mode;
    status = ReadString(preflight.ValueOrDie(), "cache_mode", &cache_mode);
    if (!status.ok()) return status;
    std::string baseline_sha256;
    status = ReadString(preflight.ValueOrDie(), "baseline_sha256",
                        &baseline_sha256);
    if (!status.ok()) return status;
    std::string candidate_sha256;
    status = ReadString(preflight.ValueOrDie(), "candidate_sha256",
                        &candidate_sha256);
    if (!status.ok()) return status;
    if (preflight_schema != 1 || !approved || profile != expected_profile ||
        workload != expected_workload || cache_mode != expected_cache_mode ||
        (!expected_baseline_sha256.empty() &&
         baseline_sha256 != expected_baseline_sha256) ||
        (!expected_candidate_sha256.empty() &&
         candidate_sha256 != expected_candidate_sha256)) {
      return Status::Corruption("paired benchmark output",
                                "production preflight identity mismatches");
    }
  }

  const auto provenance = ReadJson((root / "paired-runs.json").string());
  if (!provenance.ok()) return provenance.status();
  uint32_t paired_schema = 0;
  status = ReadU32(provenance.ValueOrDie(), "paired_schema_version",
                   &paired_schema);
  if (!status.ok()) return status;
  if (paired_schema != 1 && paired_schema != 2) {
    return Status::Corruption("paired benchmark output",
                              "paired run ledger schema is unsupported");
  }
  uint64_t ledger_generator_seed = 0;
  uint64_t ledger_order_seed = 0;
  if (expected_seed.has_value() || expected_order_seed.has_value()) {
    if (!expected_seed.has_value() || !expected_order_seed.has_value()) {
      return Status::InvalidArgument(
          "paired benchmark output",
          "campaign generator and order seeds must be verified together");
    }
    status = ReadU64(provenance.ValueOrDie(), "generator_seed",
                     &ledger_generator_seed);
    if (!status.ok()) return status;
    status = ReadU64(provenance.ValueOrDie(), "order_seed",
                     &ledger_order_seed);
    if (!status.ok()) return status;
  }
  const auto records = Field(provenance.ValueOrDie(), "records");
  if (!records.ok()) return records.status();
  status = RequireKind(*records.ValueOrDie(), JsonValue::Kind::kArray,
                       "records");
  if (!status.ok()) return status;
  if (records.ValueOrDie()->array.size() !=
          static_cast<size_t>(expected_pair_count) * 2) {
    return Status::Corruption("paired benchmark output",
                              "paired run ledger is incomplete");
  }
  std::vector<uint8_t> seen(expected_pair_count, 0);
  std::vector<BenchmarkRegressionSample> baseline_samples(expected_pair_count);
  std::vector<BenchmarkRegressionSample> candidate_samples(expected_pair_count);
  BenchmarkBaselineKey baseline_key;
  BenchmarkBaselineKey candidate_key;
  bool have_baseline_key = false;
  bool have_candidate_key = false;
  const std::vector<BenchmarkRunArm> expected_order =
      expected_order_seed.has_value()
          ? BuildAlternatingPairedOrder(expected_pair_count,
                                        expected_order_seed.value())
          : std::vector<BenchmarkRunArm>();
  for (size_t record_index = 0;
       record_index < records.ValueOrDie()->array.size(); ++record_index) {
    const JsonValue& record = records.ValueOrDie()->array[record_index];
    uint32_t pair = 0;
    status = ReadU32(record, "pair", &pair);
    if (!status.ok()) return status;
    std::string arm;
    status = ReadString(record, "arm", &arm);
    if (!status.ok()) return status;
    std::string record_status;
    status = ReadString(record, "status", &record_status);
    if (!status.ok()) return status;
    std::string artifact;
    status = ReadString(record, "artifact", &artifact);
    if (!status.ok()) return status;
    if (pair >= expected_pair_count || record_status != "PASS" ||
        (arm != "baseline" && arm != "candidate") || artifact.empty()) {
      return Status::Corruption("paired benchmark output",
                                "paired run record is invalid");
    }
    if (!expected_order.empty() &&
        (pair != static_cast<uint32_t>(record_index / 2) ||
         arm != (expected_order[record_index] == BenchmarkRunArm::kBaseline
                    ? "baseline"
                    : "candidate"))) {
      return Status::Corruption("paired benchmark output",
                                "paired execution order mismatches");
    }
    std::string binary_hash;
    status = ReadString(record, "binary_hash", &binary_hash);
    if (!status.ok()) return status;
    const std::string& expected_blake3 =
        arm == "baseline" ? expected_baseline_blake3
                          : expected_candidate_blake3;
    if (!expected_blake3.empty() && binary_hash != expected_blake3) {
      return Status::Corruption("paired benchmark output",
                                "paired binary identity mismatches");
    }
    const uint8_t arm_bit = arm == "baseline" ? 1 : 2;
    if ((seen[pair] & arm_bit) != 0) {
      return Status::Corruption("paired benchmark output",
                                "paired run arm is duplicated");
    }
    seen[pair] |= arm_bit;
    const std::filesystem::path artifact_path =
        std::filesystem::canonical(artifact, error);
    if (error || artifact_path.string().size() <= root.string().size() ||
        artifact_path.string().compare(0, root.string().size(),
                                       root.string()) != 0 ||
        artifact_path.string()[root.string().size()] !=
            std::filesystem::path::preferred_separator) {
      return Status::Corruption("paired benchmark output",
                                "artifact escapes paired output root");
    }
    const auto parsed = ReadBenchmarkArtifact(artifact_path.string());
    if (!parsed.ok()) return parsed.status();
    if (!parsed.ValueOrDie().protocol_complete ||
        !parsed.ValueOrDie().verification.load_passed ||
        !parsed.ValueOrDie().verification.result_passed ||
        !parsed.ValueOrDie().verification.reopen_passed) {
      return Status::Corruption("paired benchmark output",
                                "child artifact verification is incomplete");
    }
    if (!expected_profile.empty()) {
      const BenchmarkRunManifest& manifest = parsed.ValueOrDie().manifest;
      if (expected_seed.has_value()) {
        status = ValidatePairedCampaignSeedIdentity(
            paired_schema, ledger_generator_seed, ledger_order_seed,
            manifest.generator_seed, expected_seed.value(),
            expected_order_seed.value());
        if (!status.ok()) return status;
      }
      const auto scale = ParseBenchmarkScaleProfile(expected_profile);
      if (!scale.ok()) return scale.status();
      const BenchmarkProfile profile = ResolveBenchmarkProfile(
          scale.ValueOrDie(), manifest.generator_seed);
      if (expected_workload.empty() || expected_cache_mode.empty() ||
          manifest.dataset_profile_id != expected_profile ||
          manifest.dataset_vertex_count != profile.dataset.vertex_count ||
          manifest.dataset_edge_count != profile.dataset.edge_count ||
          manifest.dataset_property_events_per_vertex !=
              profile.dataset.property_events_per_vertex ||
          manifest.dataset_valid_time_span != profile.dataset.valid_time_span ||
          manifest.worker_limit != profile.worker_count ||
          manifest.workload_id != "cedar-public-" + expected_workload ||
          manifest.cache_mode != expected_cache_mode ||
          manifest.durability_mode != "durable" ||
          manifest.source_dirty ||
          manifest.instrumentation_profile_id !=
              kInstrumentationProfileTier0Tier1 ||
          (!expected_blake3.empty() &&
           manifest.binary_hash != expected_blake3)) {
        return Status::Corruption(
            "paired benchmark output",
            "child production identity does not match the command");
      }
      status = ValidateProductionArtifactSourceProvenance(
          manifest.source_commit, manifest.source_dirty);
      if (!status.ok()) return status;
      const auto workload = ParseBenchmarkWorkloadFamily(expected_workload);
      if (!workload.ok()) return workload.status();
      status = ValidateBenchmarkArtifactProductionMetrics(
          artifact_path.string(),
          ProductionMetricActivityRequirements(workload.ValueOrDie()));
      if (!status.ok()) return status;
    }
    const BenchmarkRunManifest& manifest = parsed.ValueOrDie().manifest;
    BenchmarkBaselineKey key;
    key.hardware_profile = manifest.os_kernel + "\n" +
        manifest.cpu_model_and_count + "\n" +
        manifest.storage_device_and_filesystem;
    key.dataset_hash = manifest.dataset_hash;
    key.workload_hash = manifest.workload_hash;
    key.durability_mode = manifest.durability_mode;
    key.cache_mode = manifest.cache_mode;
    key.resource_profile_id = manifest.resource_profile_id;
    key.database_format_version = manifest.database_format_version;
    const BenchmarkArtifactSummary& summary = parsed.ValueOrDie().summary;
    const double amplification =
        summary.derived_metrics.space_amplification.denominator == 0
            ? 0.0
            : static_cast<double>(
                  summary.derived_metrics.space_amplification.numerator) /
                  static_cast<double>(
                      summary.derived_metrics.space_amplification.denominator);
    BenchmarkRegressionSample sample{
        true, summary.measurement_throughput,
        static_cast<double>(summary.latency_p99_ns), 0.0, amplification};
    if (arm == "baseline") {
      baseline_samples[pair] = sample;
      if (!have_baseline_key) {
        baseline_key = key;
        have_baseline_key = true;
      } else if (BenchmarkBaselineKeyId(key) !=
                 BenchmarkBaselineKeyId(baseline_key)) {
        return Status::Corruption("paired benchmark output",
                                  "baseline keys are inconsistent");
      }
    } else {
      candidate_samples[pair] = sample;
      if (!have_candidate_key) {
        candidate_key = key;
        have_candidate_key = true;
      } else if (BenchmarkBaselineKeyId(key) !=
                 BenchmarkBaselineKeyId(candidate_key)) {
        return Status::Corruption("paired benchmark output",
                                  "candidate keys are inconsistent");
      }
    }
    status = RegenerateBenchmarkReport(artifact_path.string());
    if (!status.ok()) return status;
  }
  for (const uint8_t arms : seen) {
    if (arms != 3) {
      return Status::Corruption("paired benchmark output",
                                "pair does not contain both arms");
    }
  }
  const BenchmarkRegressionResult recomputed = ComparePairedBenchmarkRuns(
      baseline_key, candidate_key, baseline_samples, candidate_samples);
  if (recomputed.status != BenchmarkRegressionStatus::kPass ||
      archived_baseline_key_id != BenchmarkBaselineKeyId(baseline_key) ||
      archived_candidate_key_id != BenchmarkBaselineKeyId(candidate_key)) {
    return Status::Corruption("paired benchmark output",
                              "recomputed release gate does not pass");
  }
  return Status::OK();
}

}  // namespace cedar
