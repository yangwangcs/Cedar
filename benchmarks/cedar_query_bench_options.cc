#include "benchmarks/cedar_query_bench_options.h"

#include <charconv>
#include <cstdlib>
#include <filesystem>
#include <string_view>

namespace cedar::benchmark {
namespace {
Status Invalid(std::string message) {
  return Status::InvalidArgument("cedar_query_bench", message);
}
bool ParseU(std::string_view text, uint64_t* out) {
  auto result = std::from_chars(text.data(), text.data() + text.size(), *out);
  return !text.empty() && result.ec == std::errc() &&
         result.ptr == text.data() + text.size();
}
bool ParseDouble(std::string_view text, double* out) {
  std::string copy(text);
  char* end = nullptr;
  *out = std::strtod(copy.c_str(), &end);
  return !copy.empty() && end == copy.c_str() + copy.size();
}
}

const char* QueryBenchmarkOperationName(QueryBenchmarkOperation op) {
  static constexpr const char* names[] = {"state-at", "history", "events", "changes", "expand-out", "expand-in", "expand-both", "property-filter", "temporal-aggregate", "interval-join", "k-hop", "coexisting-shortest-path", "earliest-arrival", "latest-departure", "fastest-duration"};
  return names[static_cast<size_t>(op)];
}
const char* ProjectionStateName(ProjectionState state) {
  static constexpr const char* names[] = {"canonical-only", "base", "short-delta", "long-delta", "partial-coverage"};
  return names[static_cast<size_t>(state)];
}

StatusOr<QueryBenchmarkOptions> ParseQueryBenchmarkOptions(
    const std::vector<std::string>& args) {
  QueryBenchmarkOptions options;
  for (size_t i = 0; i < args.size();) {
    const std::string& arg = args[i++];
    const auto equal = arg.find('=');
    if (equal == std::string::npos) return Invalid("options require --name=value");
    const std::string name = arg.substr(0, equal);
    const std::string value = arg.substr(equal + 1);
    if (name == "--path") {
      if (!std::filesystem::path(value).is_absolute()) return Invalid("--path must be absolute");
      options.path = value;
    } else if (name == "--operation") {
      bool found = false;
      for (uint8_t n = 0; n <= static_cast<uint8_t>(QueryBenchmarkOperation::kFastestDuration); ++n) {
        auto op = static_cast<QueryBenchmarkOperation>(n);
        if (value == QueryBenchmarkOperationName(op)) { options.operation = op; found = true; break; }
      }
      if (!found) return Invalid("unsupported operation");
    } else if (name == "--projection-state") {
      bool found = false;
      for (uint8_t n = 0; n <= static_cast<uint8_t>(ProjectionState::kPartialCoverage); ++n) {
        auto state = static_cast<ProjectionState>(n);
        if (value == ProjectionStateName(state)) { options.projection = state; found = true; break; }
      }
      if (!found) return Invalid("unsupported projection-state");
    } else if (name == "--degree") {
      uint64_t v = 0; if (!ParseU(value, &v) || (v != 1 && v != 10 && v != 100 && v != 1000 && v != 10000)) return Invalid("degree must be 1,10,100,1000,10000"); options.degree = static_cast<uint32_t>(v);
    } else if (name == "--selectivity-percent") {
      double v = 0; if (!ParseDouble(value, &v) || (v != 0.1 && v != 1.0 && v != 10.0 && v != 100.0)) return Invalid("selectivity must be 0.1,1,10,100"); options.selectivity_percent = v;
    } else if (name == "--readers") {
      uint64_t v = 0; if (!ParseU(value, &v) || (v != 1 && v != 8 && v != 32)) return Invalid("readers must be 1,8,32"); options.readers = static_cast<uint32_t>(v);
    } else if (name == "--cache-state") {
      if (value == "cold") options.cache = QueryCacheState::kCold; else if (value == "warm") options.cache = QueryCacheState::kWarm; else return Invalid("cache-state must be cold or warm");
    } else if (name == "--projection-work") {
      if (value == "paused") options.projection_work = ProjectionWork::kPaused; else if (value == "active") options.projection_work = ProjectionWork::kActive; else return Invalid("projection-work must be paused or active");
    } else if (name == "--writers") {
      uint64_t v=0; if (!ParseU(value,&v) || (v!=1&&v!=2&&v!=8&&v!=32&&v!=64)) return Invalid("writers must be 1,2,8,32,64"); options.writers=static_cast<uint32_t>(v);
    } else if (name == "--max-hops") { uint64_t v = 0; if (!ParseU(value, &v) || v == 0) return Invalid("max-hops must be positive"); options.max_hops = static_cast<uint32_t>(v);
    } else if (name == "--result-limit") { if (!ParseU(value, &options.result_limit) || options.result_limit == 0) return Invalid("result-limit must be positive");
    } else if (name == "--capture-profile") { if (value == "true") options.capture_profile = true; else if (value == "false") options.capture_profile = false; else return Invalid("capture-profile must be true or false");
    } else if (name == "--seed") { if (!ParseU(value, &options.seed)) return Invalid("seed must be unsigned");
    } else if (name == "--duration-seconds") { if (!ParseU(value, &options.duration_seconds) || options.duration_seconds == 0) return Invalid("duration must be positive");
    } else if (name == "--facts-per-txn") { uint64_t v=0; if (!ParseU(value, &v) || (v!=1&&v!=4&&v!=8&&v!=16&&v!=32&&v!=64&&v!=128&&v!=256&&v!=512&&v!=1024&&v!=2048)) return Invalid("facts-per-txn has an unsupported value"); options.facts_per_txn=v;
    } else if (name == "--reopen-verify") { if (value == "true") options.verify_reopen = true; else if (value == "false") options.verify_reopen = false; else return Invalid("reopen-verify must be true or false");
    } else return Invalid("unknown option " + name);
  }
  if (options.path.empty()) return Invalid("--path is required");
  return options;
}
}  // namespace cedar::benchmark
