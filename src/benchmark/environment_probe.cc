// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/environment_probe.h"

#include <algorithm>
#include <charconv>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <limits>
#include <optional>
#include <sstream>
#include <string_view>
#include <vector>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/utsname.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <sys/mount.h>
#include <sys/sysctl.h>
#elif defined(__linux__)
#include <sched.h>
#include <sys/sysmacros.h>
#endif

namespace cedar {
namespace {

bool ParseUint64(std::string_view text, uint64_t* value) {
  if (text.empty() || value == nullptr) return false;
  uint64_t parsed = 0;
  const auto result = std::from_chars(
      text.data(), text.data() + text.size(), parsed, 10);
  if (result.ec != std::errc() || result.ptr != text.data() + text.size()) {
    return false;
  }
  *value = parsed;
  return true;
}

bool ReadTrimmedFile(const std::filesystem::path& path, std::string* content) {
  if (content == nullptr) return false;
  std::ifstream input(path, std::ios::binary);
  if (!input) return false;
  std::string value((std::istreambuf_iterator<char>(input)),
                    std::istreambuf_iterator<char>());
  if (input.bad()) return false;
  const size_t first = value.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) {
    content->clear();
    return true;
  }
  const size_t last = value.find_last_not_of(" \t\r\n");
  *content = value.substr(first, last - first + 1);
  return true;
}

std::string DecodeMountInfoField(std::string_view encoded) {
  std::string decoded;
  decoded.reserve(encoded.size());
  for (size_t index = 0; index < encoded.size(); ++index) {
    if (encoded[index] == '\\' && index + 3 < encoded.size() &&
        encoded[index + 1] >= '0' && encoded[index + 1] <= '7' &&
        encoded[index + 2] >= '0' && encoded[index + 2] <= '7' &&
        encoded[index + 3] >= '0' && encoded[index + 3] <= '7') {
      const unsigned int value =
          static_cast<unsigned int>(encoded[index + 1] - '0') * 64U +
          static_cast<unsigned int>(encoded[index + 2] - '0') * 8U +
          static_cast<unsigned int>(encoded[index + 3] - '0');
      decoded.push_back(static_cast<char>(value));
      index += 3;
    } else {
      decoded.push_back(encoded[index]);
    }
  }
  return decoded;
}

#if defined(__linux__)
struct MountInfo {
  uint64_t device_major = 0;
  uint64_t device_minor = 0;
  std::string root;
  std::string mount_point;
  std::string filesystem_type;
  std::string source;
};

bool ParseDeviceId(std::string_view text, uint64_t* device_major,
                   uint64_t* device_minor) {
  const size_t separator = text.find(':');
  if (separator == std::string_view::npos ||
      text.find(':', separator + 1) != std::string_view::npos) {
    return false;
  }
  return ParseUint64(text.substr(0, separator), device_major) &&
      ParseUint64(text.substr(separator + 1), device_minor);
}

bool ReadMountInfo(std::vector<MountInfo>* mounts) {
  if (mounts == nullptr) return false;
  std::ifstream input("/proc/self/mountinfo");
  if (!input) return false;
  std::string line;
  while (std::getline(input, line)) {
    std::istringstream stream(line);
    std::vector<std::string> fields;
    std::string field;
    while (stream >> field) fields.push_back(field);
    const auto separator = std::find(fields.begin(), fields.end(), "-");
    if (separator == fields.end() || separator - fields.begin() < 6 ||
        fields.end() - separator < 4) {
      return false;
    }
    MountInfo mount;
    if (!ParseDeviceId(fields[2], &mount.device_major,
                       &mount.device_minor)) {
      return false;
    }
    mount.root = DecodeMountInfoField(fields[3]);
    mount.mount_point = DecodeMountInfoField(fields[4]);
    mount.filesystem_type = *(separator + 1);
    mount.source = DecodeMountInfoField(*(separator + 2));
    if (mount.root.empty() || mount.mount_point.empty() ||
        mount.filesystem_type.empty() || mount.source.empty()) {
      return false;
    }
    mounts->push_back(std::move(mount));
  }
  return input.eof() && !mounts->empty();
}

bool PathIsWithin(std::string_view path, std::string_view directory) {
  if (directory == "/") return !path.empty() && path.front() == '/';
  return path == directory ||
      (path.size() > directory.size() &&
       path.compare(0, directory.size(), directory) == 0 &&
       path[directory.size()] == '/');
}

bool ReadSelfCgroupPath(std::string* path) {
  if (path == nullptr) return false;
  std::ifstream input("/proc/self/cgroup");
  if (!input) return false;
  std::string line;
  size_t matched = 0;
  while (std::getline(input, line)) {
    if (line.rfind("0::", 0) != 0) continue;
    *path = line.substr(3);
    ++matched;
  }
  if (!input.eof() || matched != 1 || path->empty() || (*path)[0] != '/') {
    return false;
  }
  const std::filesystem::path parsed(*path);
  for (const auto& component : parsed) {
    if (component == "..") return false;
  }
  return true;
}

bool ResolveCgroupDirectory(const std::vector<MountInfo>& mounts,
                            std::filesystem::path* mount_point,
                            std::filesystem::path* cgroup_directory,
                            std::string* cgroup_name,
                            std::string* hierarchy_root) {
  if (mount_point == nullptr || cgroup_directory == nullptr ||
      cgroup_name == nullptr || hierarchy_root == nullptr ||
      !ReadSelfCgroupPath(cgroup_name)) {
    return false;
  }
  const MountInfo* cgroup_mount = nullptr;
  for (const MountInfo& mount : mounts) {
    if (mount.filesystem_type == "cgroup2" &&
        (cgroup_mount == nullptr ||
         mount.mount_point.size() > cgroup_mount->mount_point.size())) {
      cgroup_mount = &mount;
    }
  }
  if (cgroup_mount == nullptr) return false;
  *hierarchy_root = cgroup_mount->root;
  *mount_point = std::filesystem::path(cgroup_mount->mount_point).lexically_normal();
  std::string relative_name = *cgroup_name;
  if (cgroup_mount->root != "/") {
    if (!PathIsWithin(relative_name, cgroup_mount->root)) return false;
    relative_name.erase(0, cgroup_mount->root.size());
  }
  while (!relative_name.empty() && relative_name.front() == '/') {
    relative_name.erase(relative_name.begin());
  }
  std::filesystem::path relative(relative_name);
  for (const auto& component : relative) {
    if (component == "..") return false;
  }
  *cgroup_directory = (*mount_point / relative).lexically_normal();
  return PathIsWithin(cgroup_directory->string(), mount_point->string());
}

bool ParseCpuSet(std::string_view text, uint32_t* cpu_count) {
  if (text.empty() || cpu_count == nullptr) return false;
  uint64_t total = 0;
  uint64_t previous_end = 0;
  bool have_previous = false;
  size_t offset = 0;
  while (offset < text.size()) {
    const size_t comma = text.find(',', offset);
    const std::string_view range = text.substr(
        offset, (comma == std::string_view::npos ? text.size() : comma) - offset);
    if (range.empty()) return false;
    const size_t dash = range.find('-');
    uint64_t first = 0;
    uint64_t last = 0;
    if (dash == std::string_view::npos) {
      if (!ParseUint64(range, &first)) return false;
      last = first;
    } else {
      if (range.find('-', dash + 1) != std::string_view::npos ||
          !ParseUint64(range.substr(0, dash), &first) ||
          !ParseUint64(range.substr(dash + 1), &last) || first > last) {
        return false;
      }
    }
    if (last > std::numeric_limits<uint32_t>::max() ||
        (have_previous && first <= previous_end)) {
      return false;
    }
    const uint64_t range_size = last - first + 1;
    if (total > std::numeric_limits<uint32_t>::max() - range_size) {
      return false;
    }
    total += range_size;
    previous_end = last;
    have_previous = true;
    offset = comma == std::string_view::npos ? text.size() : comma + 1;
  }
  if (total == 0) return false;
  *cpu_count = static_cast<uint32_t>(total);
  return true;
}

bool ParseCpuMax(std::string_view text, std::optional<uint32_t>* cpu_limit) {
  if (cpu_limit == nullptr) return false;
  std::istringstream stream{std::string(text)};
  std::string quota_text;
  std::string period_text;
  std::string extra;
  if (!(stream >> quota_text >> period_text) || (stream >> extra)) return false;
  uint64_t period = 0;
  if (!ParseUint64(period_text, &period) || period == 0) return false;
  if (quota_text == "max") {
    cpu_limit->reset();
    return true;
  }
  uint64_t quota = 0;
  if (!ParseUint64(quota_text, &quota)) return false;
  const uint64_t whole_cpus = quota / period;
  *cpu_limit = static_cast<uint32_t>(std::min<uint64_t>(
      whole_cpus, std::numeric_limits<uint32_t>::max()));
  return true;
}

bool ParseMemoryMax(std::string_view text,
                    std::optional<uint64_t>* memory_limit) {
  if (memory_limit == nullptr) return false;
  if (text == "max") {
    memory_limit->reset();
    return true;
  }
  uint64_t value = 0;
  if (!ParseUint64(text, &value)) return false;
  *memory_limit = value;
  return true;
}

bool ProbeLinuxResourceLimits(const std::vector<MountInfo>& mounts,
                              BenchmarkEnvironment* environment) {
  if (environment == nullptr) return false;
  cpu_set_t affinity;
  CPU_ZERO(&affinity);
  if (::sched_getaffinity(0, sizeof(affinity), &affinity) != 0) return false;
  const int affinity_count = CPU_COUNT(&affinity);
  if (affinity_count <= 0) return false;

  const long pages = ::sysconf(_SC_PHYS_PAGES);
  const long page_size = ::sysconf(_SC_PAGESIZE);
  if (pages <= 0 || page_size <= 0 ||
      static_cast<uint64_t>(pages) >
          std::numeric_limits<uint64_t>::max() /
              static_cast<uint64_t>(page_size)) {
    return false;
  }
  const uint64_t physical_memory =
      static_cast<uint64_t>(pages) * static_cast<uint64_t>(page_size);

  std::filesystem::path mount_point;
  std::filesystem::path cgroup_directory;
  std::string cgroup_name;
  std::string hierarchy_root;
  if (!ResolveCgroupDirectory(mounts, &mount_point, &cgroup_directory,
                              &cgroup_name, &hierarchy_root)) {
    return false;
  }
  struct stat namespace_metadata {};
  if (::stat("/proc/self/ns/cgroup", &namespace_metadata) != 0) return false;
  const bool ancestry_complete =
      LinuxCgroupAncestryProvenanceCompleteForTesting(
          hierarchy_root, static_cast<uint64_t>(namespace_metadata.st_ino));
  std::string cpuset_text;
  uint32_t cpuset_count = 0;
  if (!ReadTrimmedFile(cgroup_directory / "cpuset.cpus.effective",
                       &cpuset_text) ||
      !ParseCpuSet(cpuset_text, &cpuset_count)) {
    return false;
  }

  uint32_t effective_cpu_count = std::min<uint32_t>(
      static_cast<uint32_t>(affinity_count), cpuset_count);
  uint64_t effective_memory = physical_memory;
  for (std::filesystem::path directory = cgroup_directory;;
       directory = directory.parent_path()) {
    std::string cpu_max_text;
    std::optional<uint32_t> cpu_limit;
    if (!ReadTrimmedFile(directory / "cpu.max", &cpu_max_text) ||
        !ParseCpuMax(cpu_max_text, &cpu_limit)) {
      return false;
    }
    if (cpu_limit.has_value()) {
      effective_cpu_count = std::min(effective_cpu_count, *cpu_limit);
    }
    std::string memory_max_text;
    std::optional<uint64_t> memory_limit;
    if (!ReadTrimmedFile(directory / "memory.max", &memory_max_text) ||
        !ParseMemoryMax(memory_max_text, &memory_limit)) {
      return false;
    }
    if (memory_limit.has_value()) {
      effective_memory = std::min(effective_memory, *memory_limit);
    }
    if (directory == mount_point) break;
    if (!PathIsWithin(directory.parent_path().string(), mount_point.string()) ||
        directory == directory.parent_path()) {
      return false;
    }
  }
  environment->logical_cpu_count = effective_cpu_count;
  environment->memory_limit_bytes = effective_memory;
  environment->cpu_model_and_count =
      "logical_cpus=" + std::to_string(effective_cpu_count) +
      ";affinity_cpus=" + std::to_string(affinity_count) +
      ";cpuset_effective=" + cpuset_text + ";cgroup_v2=" + cgroup_name;
  return ancestry_complete;
}

bool ProbeLinuxStorage(const std::filesystem::path& probed_path,
                       const std::vector<MountInfo>& mounts,
                       BenchmarkEnvironment* environment) {
  if (environment == nullptr) return false;
  struct stat status {};
  if (::stat(probed_path.c_str(), &status) != 0) return false;
  const uint64_t device_major = static_cast<uint64_t>(major(status.st_dev));
  const uint64_t device_minor = static_cast<uint64_t>(minor(status.st_dev));
  const std::string path = probed_path.string();
  const MountInfo* selected = nullptr;
  for (const MountInfo& mount : mounts) {
    if (mount.device_major != device_major ||
        mount.device_minor != device_minor ||
        !PathIsWithin(path, mount.mount_point)) {
      continue;
    }
    if (selected == nullptr ||
        mount.mount_point.size() > selected->mount_point.size()) {
      selected = &mount;
    }
  }
  if (selected == nullptr) return false;
  environment->storage_device_and_filesystem =
      "device=" + selected->source + ";device_id=" +
      std::to_string(device_major) + ":" + std::to_string(device_minor) +
      ";filesystem=" + selected->filesystem_type +
      ";mount=" + selected->mount_point;
  return true;
}
#endif

}  // namespace

bool LinuxCgroupAncestryProvenanceCompleteForTesting(
    const std::string& hierarchy_root, uint64_t namespace_inode) {
  constexpr uint64_t kInitialCgroupNamespaceInode = 0xEFFFFFFBULL;
  return hierarchy_root == "/" &&
      namespace_inode == kInitialCgroupNamespaceInode;
}

BenchmarkEnvironment ProbeBenchmarkEnvironment(const std::string& storage_path) {
  BenchmarkEnvironment environment;
  struct utsname information {};
  if (::uname(&information) == 0) {
    environment.os_kernel = std::string(information.sysname) + " " + information.release +
        " " + information.machine;
  } else {
    environment.os_kernel = "unknown";
  }
#if defined(__APPLE__)
  uint32_t logical_cpus = 0;
  size_t logical_cpu_size = sizeof(logical_cpus);
  const bool logical_cpu_ok =
      ::sysctlbyname("hw.logicalcpu", &logical_cpus, &logical_cpu_size,
                     nullptr, 0) == 0 && logical_cpus != 0;
  if (logical_cpu_ok) environment.logical_cpu_count = logical_cpus;
  uint64_t physical_memory = 0;
  size_t physical_memory_size = sizeof(physical_memory);
  const bool memory_ok =
      ::sysctlbyname("hw.memsize", &physical_memory, &physical_memory_size,
                     nullptr, 0) == 0 && physical_memory != 0;
  if (memory_ok) {
    environment.memory_limit_bytes = physical_memory;
  }
  environment.cpu_model_and_count =
      "logical_cpus=" + std::to_string(environment.logical_cpu_count);
  environment.resource_limit_provenance_complete =
      logical_cpu_ok && memory_ok;
#elif defined(__linux__)
  std::vector<MountInfo> mounts;
  const bool mountinfo_ok = ReadMountInfo(&mounts);
  environment.resource_limit_provenance_complete =
      mountinfo_ok && ProbeLinuxResourceLimits(mounts, &environment);
#else
  environment.cpu_model_and_count = "resource provenance unsupported";
#endif
  std::filesystem::path probed_path = storage_path.empty()
      ? std::filesystem::path(".") : std::filesystem::path(storage_path);
  std::error_code error;
  probed_path = std::filesystem::absolute(probed_path, error);
  if (error) probed_path = ".";
  while (!std::filesystem::exists(probed_path, error) &&
         probed_path.has_parent_path() &&
         probed_path != probed_path.parent_path()) {
    error.clear();
    probed_path = probed_path.parent_path();
  }
  error.clear();
  const std::filesystem::path canonical_path =
      std::filesystem::canonical(probed_path, error);
  if (!error) probed_path = canonical_path;
  struct statvfs storage {};
  if (::statvfs(probed_path.c_str(), &storage) == 0 &&
      storage.f_frsize != 0 &&
      static_cast<uint64_t>(storage.f_bavail) <=
          std::numeric_limits<uint64_t>::max() /
              static_cast<uint64_t>(storage.f_frsize)) {
    environment.storage_free_bytes =
        static_cast<uint64_t>(storage.f_bavail) *
        static_cast<uint64_t>(storage.f_frsize);
  }
#if defined(__APPLE__)
  struct statfs filesystem {};
  if (::statfs(probed_path.c_str(), &filesystem) == 0 &&
      filesystem.f_mntfromname[0] != '\0' &&
      filesystem.f_fstypename[0] != '\0' &&
      filesystem.f_mntonname[0] != '\0') {
    environment.storage_device_and_filesystem =
        std::string("device=") + filesystem.f_mntfromname +
        ";filesystem=" + filesystem.f_fstypename +
        ";mount=" + filesystem.f_mntonname;
    environment.storage_provenance_complete = true;
  }
#elif defined(__linux__)
  environment.storage_provenance_complete =
      mountinfo_ok && ProbeLinuxStorage(probed_path, mounts, &environment);
#endif
#if defined(__clang__)
  environment.compiler_and_flags = std::string("clang ") + __clang_version__;
#elif defined(__GNUC__)
  environment.compiler_and_flags = std::string("gcc ") + __VERSION__;
#else
  environment.compiler_and_flags = "unknown compiler";
#endif
  return environment;
}

}  // namespace cedar
