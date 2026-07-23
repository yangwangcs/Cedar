// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/artifact_reader.h"

#include <array>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <limits>
#include <map>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

#include "cedar/benchmark/artifact_writer.h"
#include "cedar/benchmark/report_builder.h"
#include "cedar/benchmark/run_manifest.h"

namespace cedar {
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

}  // namespace cedar
