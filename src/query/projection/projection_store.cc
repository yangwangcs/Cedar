#include "query/projection/projection_store.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <map>
#include <set>

#include "cedar/core/crc32c.h"

namespace cedar::internal {
namespace {
namespace fs = std::filesystem;

Status SyncFile(const fs::path& path) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError("projection store", "open for sync failed");
  const int result = ::fsync(fd);
  ::close(fd);
  return result == 0 ? Status::OK() : Status::IOError("projection store", "file sync failed");
}
Status SyncDirectory(const fs::path& path) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) return Status::IOError("projection store", "open directory failed");
  const int result = ::fsync(fd);
  ::close(fd);
  return result == 0 ? Status::OK() : Status::IOError("projection store", "directory sync failed");
}
Status WriteFile(const fs::path& path, const std::string& bytes) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) return Status::IOError("projection store", "cannot create file");
  out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  out.flush();
  return out ? Status::OK() : Status::IOError("projection store", "write failed");
}
StatusOr<std::string> ReadFile(const fs::path& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return Status::NotFound("projection store", "file not found");
  return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}
bool SafeName(const std::string& name) {
  return !name.empty() && name != "PROJECTION-CURRENT" && name != "PROJECTION-CURRENT.tmp" && name.rfind("PROJECTION-CURRENT.", 0) != 0 && name.front() != '/' && name.find("..") == std::string::npos && name.find('/') == std::string::npos && name.find('\\') == std::string::npos && name.size() < 256;
}
std::string CurrentBytes(uint64_t generation) {
  std::string out("CPC1", 4);
  for (int i=0;i<8;++i) out.push_back(char(generation >> (i*8)));
  const uint32_t crc = crc32c::Value(out.data(), out.size());
  for (int i=0;i<4;++i) out.push_back(char(crc >> (i*8)));
  return out;
}
bool ParseCurrent(const std::string& in, uint64_t* generation) {
  if (in.size() != 16 || in.compare(0,4,"CPC1") != 0) return false;
  *generation = 0; for (int i=0;i<8;++i) *generation |= uint64_t(uint8_t(in[4+i])) << (i*8);
  uint32_t crc = 0; for (int i=0;i<4;++i) crc |= uint32_t(uint8_t(in[12+i])) << (i*8);
  return crc32c::Value(in.data(), 12) == crc;
}
}  // namespace

struct ProjectionGeneration::State {
  ProjectionManifest manifest;
  std::string directory;
  std::vector<std::string> segment_files;
  std::string manifest_file;
  std::atomic<uint64_t> pins{0};
  std::atomic<bool> present{true};
  std::atomic<bool> retired{false};
  mutable std::set<std::string> unavailable_segments;
};

StatusOr<std::vector<ProjectionChain>> QueryProjectionStore::ReadChainsForGeneration(
    const std::shared_ptr<ProjectionGeneration::State>& state,
    const CoverageRequest& request) {
  if (!state || !state->present.load()) {
    return Status::NotFound("projection store", "projection generation is no longer available");
  }
  if (request.generation_id && *request.generation_id != state->manifest.generation_id) {
    return Status::Conflict("projection store", "projection generation changed");
  }
  if (request.expected_base_seq && *request.expected_base_seq != state->manifest.base_seq) {
    return Status::Conflict("projection store", "projection base changed");
  }
  if (!request.database_identity.empty() &&
      request.database_identity != state->manifest.database_identity) {
    return Status::IdentityConflict("projection store", "projection identity changed");
  }
  if (request.snapshot_seq.value < state->manifest.base_seq.value) {
    return Status::NotFound("projection store", "coverage is unavailable");
  }
  std::vector<ProjectionChain> result;
  for (const auto& region : state->manifest.regions) {
    if (region.kind != request.kind || region.part_id != request.part_id ||
        region.property_id != request.property_id || region.schema_epoch != request.schema_epoch ||
        region.entity_min > request.entity_min || region.entity_max_exclusive < request.entity_max_exclusive ||
        region.valid_time.from.value > request.valid_time.from.value ||
        (region.valid_time.to && (!request.valid_time.to ||
                                  region.valid_time.to->value < request.valid_time.to->value))) {
      continue;
    }
    for (const auto& segment : region.segments) {
      if (state->unavailable_segments.count(segment.filename) != 0) {
        continue;
      }
      std::ifstream in(fs::path(state->directory) / segment.filename, std::ios::binary);
      if (!in) {
        state->unavailable_segments.insert(segment.filename);
        continue;
      }
      std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
      auto decoded = DecodeProjectionPage(bytes);
      if (!decoded.ok()) {
        state->unavailable_segments.insert(segment.filename);
        continue;
      }
      result.push_back(std::move(decoded).ConsumeValueOrDie());
    }
  }
  if (result.empty()) return Status::NotFound("projection store", "coverage is unavailable");
  return result;
}

ProjectionGeneration::ProjectionGeneration(std::shared_ptr<State> state, bool pin)
    : state_(std::move(state)), pin_(pin) { if (pin_ && state_) ++state_->pins; }
ProjectionGeneration::~ProjectionGeneration() { if (pin_ && state_) --state_->pins; }
ProjectionGeneration::ProjectionGeneration(const ProjectionGeneration& other) : state_(other.state_), pin_(other.pin_) { if (pin_ && state_) ++state_->pins; }
ProjectionGeneration& ProjectionGeneration::operator=(const ProjectionGeneration& other) { if (this != &other) { if (pin_ && state_) --state_->pins; state_=other.state_; pin_=other.pin_; if (pin_ && state_) ++state_->pins; } return *this; }
ProjectionGeneration::ProjectionGeneration(ProjectionGeneration&& other) noexcept : state_(std::move(other.state_)), pin_(other.pin_) { other.pin_=false; }
ProjectionGeneration& ProjectionGeneration::operator=(ProjectionGeneration&& other) noexcept { if (this != &other) { if (pin_ && state_) --state_->pins; state_=std::move(other.state_); pin_=other.pin_; other.pin_=false; } return *this; }
bool ProjectionGeneration::exists() const { return state_ && state_->present.load(); }
uint64_t ProjectionGeneration::generation_id() const { return state_ ? state_->manifest.generation_id : 0; }
const ProjectionManifest* ProjectionGeneration::manifest() const { return state_ ? &state_->manifest : nullptr; }

QueryProjectionStore::QueryProjectionStore(ProjectionStoreOptions options)
    : options_(std::move(options)), projections_path_(options_.path), manifests_path_(fs::path(options_.path) / "manifests") {}
QueryProjectionStore::~QueryProjectionStore() {
  std::lock_guard<std::mutex> lock(mutex_);
  closed_ = true;
  if (current_) current_->present = false;
  for (const auto& generation : retired_) {
    if (generation) generation->present = false;
  }
  current_.reset();
  retired_.clear();
}

StatusOr<std::unique_ptr<QueryProjectionStore>> QueryProjectionStore::Open(ProjectionStoreOptions options) {
  if (options.path.empty() || options.database_identity.empty()) return Status::InvalidArgument("projection store", "path and database identity are required");
  auto store = std::unique_ptr<QueryProjectionStore>(new QueryProjectionStore(std::move(options)));
  std::error_code ec; fs::create_directories(store->manifests_path_, ec); if (ec) return Status::IOError("projection store", ec.message());
  // Crash recovery leaves only unpublished temporary artifacts. They are
  // never authoritative; remove them before loading CURRENT so a killed
  // writer cannot accumulate stale segments across reopen cycles.
  for (const fs::path& root : {fs::path(store->projections_path_),
                               fs::path(store->manifests_path_)}) {
    for (fs::directory_iterator it(root, ec); !ec && it != fs::directory_iterator(); it.increment(ec)) {
      if (it->is_regular_file(ec) && it->path().extension() == ".tmp") {
        fs::remove(it->path(), ec);
      }
    }
    ec.clear();
  }
  const Status loaded = store->LoadCurrent();
  if (!loaded.ok() && !loaded.IsNotFound()) { store->enabled_ = false; }
  return store;
}

Status QueryProjectionStore::LoadCurrent() {
  auto current = ReadFile(fs::path(projections_path_) / "PROJECTION-CURRENT");
  if (!current.ok()) return current.status();
  uint64_t generation = 0; if (!ParseCurrent(current.ValueOrDie(), &generation)) return Status::Corruption("projection store", "bad PROJECTION-CURRENT");
  if (generation == 0) { enabled_ = false; return Status::NotFound("projection store", "projections retired"); }
  const fs::path manifest_path = fs::path(manifests_path_) / (std::to_string(generation) + ".cmanifest");
  auto bytes = ReadFile(manifest_path); if (!bytes.ok()) return bytes.status();
  auto manifest = DecodeProjectionManifest(bytes.ValueOrDie(), options_.database_identity); if (!manifest.ok()) return manifest.status();
  if (manifest.ValueOrDie().generation_id != generation) return Status::Corruption("projection store", "manifest generation mismatch");
  if (options_.visible_seq && manifest.ValueOrDie().base_seq.value > options_.visible_seq->value) {
    enabled_ = false;
    return Status::NotFound("projection store", "projection base is ahead of authoritative visible watermark");
  }
  if (options_.oldest_readable_seq && manifest.ValueOrDie().base_seq.value < options_.oldest_readable_seq->value) {
    enabled_ = false;
    return Status::NotFound("projection store", "projection base is below authoritative retention watermark");
  }
  for (const auto& r : manifest.ValueOrDie().regions) for (const auto& seg : r.segments) {
    const fs::path segment_path = fs::path(projections_path_) / seg.filename;
    std::error_code ec; if (!fs::exists(segment_path, ec) || ec || fs::file_size(segment_path, ec) != seg.file_bytes || ec) return Status::Corruption("projection store", "referenced segment is missing or sized differently");
    auto segment = ReadFile(segment_path); if (!segment.ok() || crc32c::Value(segment.ValueOrDie().data(), segment.ValueOrDie().size()) != seg.checksum) return Status::Corruption("projection store", "referenced segment checksum mismatch");
    auto decoded = DecodeProjectionPage(segment.ValueOrDie());
    if (!decoded.ok() || !(decoded.ValueOrDie().header == seg.header)) return Status::Corruption("projection store", "referenced segment header mismatch");
    const auto& header = decoded.ValueOrDie().header;
    if (header.generation_id != generation || header.base_seq != manifest.ValueOrDie().base_seq || header.kind != r.kind || header.part_id != r.part_id || header.schema_epoch != r.schema_epoch || r.entity_min > header.entity_min || r.entity_max_exclusive < header.entity_max_exclusive || r.valid_time.from.value > header.valid_from_min.value || (r.valid_time.to && (!header.valid_to_max || r.valid_time.to->value < header.valid_to_max->value)) || (r.property_id.has_value() != (header.property_id.value != 0)) || (r.property_id && r.property_id->value != header.property_id.value)) return Status::Corruption("projection store", "referenced segment metadata mismatch");
  }
  auto state = std::make_shared<ProjectionGeneration::State>(); state->manifest = manifest.ConsumeValueOrDie(); state->directory = projections_path_; state->manifest_file = manifest_path.string(); for (const auto& r : state->manifest.regions) for (const auto& seg : r.segments) state->segment_files.push_back(seg.filename); current_ = std::move(state); enabled_ = true; return Status::OK();
}

Status QueryProjectionStore::PublishCurrent(uint64_t generation) {
  const fs::path temp = fs::path(projections_path_) / "PROJECTION-CURRENT.tmp";
  const Status written = WriteFile(temp, CurrentBytes(generation)); if (!written.ok()) return written;
  if (options_.fault_injector) { auto s=options_.fault_injector(ProjectionStoreFaultPoint::kCurrentTemporaryWrite); if (!s.ok()) return s; }
  auto sync = SyncFile(temp); if (!sync.ok()) return sync;
  if (::rename(temp.c_str(), (fs::path(projections_path_) / "PROJECTION-CURRENT").c_str()) != 0) return Status::IOError("projection store", "CURRENT rename failed");
  if (options_.fault_injector) { auto s=options_.fault_injector(ProjectionStoreFaultPoint::kCurrentRename); if (!s.ok()) return s; }
  return SyncDirectory(projections_path_);
}

Status QueryProjectionStore::Build(const ProjectionBuild& build) {
  std::lock_guard<std::mutex> build_lock(mutex_);
  const Status manifest_status = ValidateProjectionManifest(build.manifest, options_.database_identity);
  if (!manifest_status.ok()) return manifest_status;
  if (build.manifest.generation_id == 0 || (current_ && build.manifest.generation_id <= current_->manifest.generation_id)) return Status::Conflict("projection store", "generation is not strictly newer");
  for (const auto& retired : retired_) if (build.manifest.generation_id <= retired->manifest.generation_id) return Status::Conflict("projection store", "generation is already retained");
  size_t manifest_segment_count = 0;
  std::map<std::string, const SegmentDescriptor*> expected;
  std::map<std::string, const CoverageRegion*> expected_regions;
  for (const auto& region : build.manifest.regions) {
    for (const auto& segment : region.segments) {
      ++manifest_segment_count;
      if (!expected.emplace(segment.segment_id, &segment).second) return Status::Corruption("projection store", "duplicate manifest segment");
      expected_regions.emplace(segment.segment_id, &region);
    }
  }
  if (manifest_segment_count != build.segments.size()) return Status::InvalidArgument("projection store", "manifest and build segment sets differ");
  std::set<std::string> seen;
  for (const auto& input : build.segments) {
    if (!seen.insert(input.descriptor.segment_id).second || !SafeName(input.descriptor.filename)) return Status::InvalidArgument("projection store", "invalid segment input");
    const auto expected_it = expected.find(input.descriptor.segment_id);
    if (expected_it == expected.end() || *expected_it->second != input.descriptor) return Status::InvalidArgument("projection store", "manifest segment descriptor differs from build input");
    if (crc32c::Value(input.bytes.data(), input.bytes.size()) != input.descriptor.checksum) return Status::Corruption("projection store", "segment checksum mismatch");
    auto decoded = DecodeProjectionPage(input.bytes);
    if (!decoded.ok()) return decoded.status();
    const auto& header = decoded.ValueOrDie().header;
    if (!(header == input.descriptor.header) || header.generation_id != build.manifest.generation_id || header.base_seq != build.manifest.base_seq) return Status::Corruption("projection store", "segment header differs from descriptor");
    const auto* region = expected_regions[input.descriptor.segment_id];
    if (region->kind != header.kind || region->part_id != header.part_id || region->schema_epoch != header.schema_epoch || region->entity_min > header.entity_min || region->entity_max_exclusive < header.entity_max_exclusive || region->valid_time.from.value > header.valid_from_min.value || (region->valid_time.to && (!header.valid_to_max || region->valid_time.to->value < header.valid_to_max->value)) || (region->property_id.has_value() != (header.property_id.value != 0)) || (region->property_id && region->property_id->value != header.property_id.value)) return Status::Corruption("projection store", "segment metadata is outside coverage region");
    const fs::path destination = fs::path(projections_path_) / input.descriptor.filename;
    std::error_code exists_error;
    if (fs::exists(destination, exists_error) || exists_error || fs::is_symlink(destination, exists_error) || (fs::exists(destination, exists_error) && !fs::is_regular_file(destination, exists_error))) return Status::Conflict("projection store", "segment filename already exists or is not regular");
    const fs::path temp = fs::path(projections_path_) / (input.descriptor.filename + ".tmp");
    if (fs::exists(temp, exists_error) || exists_error || fs::is_symlink(temp, exists_error)) return Status::Conflict("projection store", "segment temporary filename already exists");
    auto s=WriteFile(temp,input.bytes); if (!s.ok()) return s; s=SyncFile(temp); if (!s.ok()) return s;
    if (options_.fault_injector) { s=options_.fault_injector(ProjectionStoreFaultPoint::kAfterSegmentSync); if (!s.ok()) return s; }
    if (::rename(temp.c_str(), (fs::path(projections_path_) / input.descriptor.filename).c_str()) != 0) return Status::IOError("projection store", "segment rename failed");
  }
  auto encoded = EncodeProjectionManifest(build.manifest); if (!encoded.ok()) return encoded.status();
  const fs::path manifest = fs::path(manifests_path_) / (std::to_string(build.manifest.generation_id) + ".cmanifest");
  std::error_code manifest_error;
  if (fs::exists(manifest, manifest_error) || manifest_error || fs::is_symlink(manifest, manifest_error)) return Status::Conflict("projection store", "manifest generation already exists");
  const fs::path temp_manifest = manifest.string() + ".tmp";
  if (fs::exists(temp_manifest, manifest_error) || manifest_error || fs::is_symlink(temp_manifest, manifest_error)) return Status::Conflict("projection store", "manifest temporary filename already exists");
  auto s=WriteFile(temp_manifest, encoded.ValueOrDie()); if (!s.ok()) return s; s=SyncFile(temp_manifest); if (!s.ok()) return s;
  if (options_.fault_injector) { s=options_.fault_injector(ProjectionStoreFaultPoint::kAfterManifestSync); if (!s.ok()) return s; }
  if (::rename(temp_manifest.c_str(), manifest.c_str()) != 0) return Status::IOError("projection store", "manifest rename failed");
  s=SyncDirectory(manifests_path_); if (!s.ok()) return s;
  if (options_.fault_injector) { s=options_.fault_injector(ProjectionStoreFaultPoint::kDirectorySync); if (!s.ok()) return s; }
  s=PublishCurrent(build.manifest.generation_id); if (!s.ok()) return s;
  if (current_) { current_->retired = true; retired_.push_back(current_); }
  auto state=std::make_shared<ProjectionGeneration::State>(); state->manifest=build.manifest; state->directory=projections_path_; state->manifest_file=(fs::path(manifests_path_) / (std::to_string(build.manifest.generation_id) + ".cmanifest")).string(); for (const auto& r : build.manifest.regions) for (const auto& seg : r.segments) state->segment_files.push_back(seg.filename); current_=std::move(state); enabled_=true;
  for (auto it=retired_.begin(); it!=retired_.end();) { if ((*it)->pins.load()==0) { (*it)->present=false; it=retired_.erase(it); } else ++it; }
  return Status::OK();
}

std::optional<ProjectionGeneration> QueryProjectionStore::Acquire(const CoverageRequest& request) const {
  std::lock_guard<std::mutex> lock(mutex_); if (!enabled_ || !current_ || current_->retired || request.snapshot_seq.value < current_->manifest.base_seq.value) return std::nullopt;
  if (request.generation_id && *request.generation_id != current_->manifest.generation_id) return std::nullopt;
  if (request.expected_base_seq && *request.expected_base_seq != current_->manifest.base_seq) return std::nullopt;
  if (!request.database_identity.empty() && request.database_identity != current_->manifest.database_identity) return std::nullopt;
  for (const auto& r : current_->manifest.regions) {
    if (r.kind != request.kind || r.part_id != request.part_id || r.schema_epoch != request.schema_epoch || r.entity_min > request.entity_min || r.entity_max_exclusive < request.entity_max_exclusive || (r.property_id != request.property_id)) continue;
    if (r.valid_time.from.value > request.valid_time.from.value || (r.valid_time.to && (!request.valid_time.to || r.valid_time.to->value < request.valid_time.to->value))) continue;
    return ProjectionGeneration(current_, true);
  }
  return std::nullopt;
}
Status QueryProjectionStore::RetireBefore(CommitSeq seq) { std::lock_guard<std::mutex> lock(mutex_); if (current_ && current_->manifest.base_seq.value < seq.value) { auto published = PublishCurrent(0); if (!published.ok()) return published; current_->retired=true; retired_.push_back(current_); current_.reset(); enabled_=false; } for (auto it=retired_.begin(); it!=retired_.end();) { if ((*it)->pins.load()==0) { for (const auto& file : (*it)->segment_files) { std::error_code ec; fs::remove(fs::path((*it)->directory) / file, ec); } if (!(*it)->manifest_file.empty()) { std::error_code ec; fs::remove((*it)->manifest_file, ec); } (*it)->present=false; it=retired_.erase(it); } else ++it; } return Status::OK(); }
void QueryProjectionStore::CollectRetired() { std::lock_guard<std::mutex> lock(mutex_); for (auto it=retired_.begin(); it!=retired_.end();) { if ((*it)->pins.load()==0) { for (const auto& file : (*it)->segment_files) { std::error_code ec; fs::remove(fs::path((*it)->directory) / file, ec); } if (!(*it)->manifest_file.empty()) { std::error_code ec; fs::remove((*it)->manifest_file, ec); } (*it)->present=false; it=retired_.erase(it); } else ++it; } }
Status QueryProjectionStore::Quarantine(const std::string& filename) { std::lock_guard<std::mutex> lock(mutex_); if (!SafeName(filename) || filename == "PROJECTION-CURRENT") return Status::InvalidArgument("projection store", "invalid quarantine filename"); auto referenced = [&](const std::shared_ptr<ProjectionGeneration::State>& generation) { if (!generation) return false; for (const auto& file : generation->segment_files) if (file == filename) return true; return generation->manifest_file == (fs::path(projections_path_) / filename).string(); }; if (referenced(current_)) return Status::Conflict("projection store", "cannot quarantine referenced current file"); for (const auto& generation : retired_) if (referenced(generation) && generation->pins.load() != 0) return Status::Conflict("projection store", "cannot quarantine pinned file"); std::error_code ec; fs::create_directories(fs::path(projections_path_) / "quarantine", ec); if (ec) return Status::IOError("projection store", ec.message()); fs::rename(fs::path(projections_path_) / filename, fs::path(projections_path_) / "quarantine" / filename, ec); if (ec) return Status::IOError("projection store", ec.message()); return SyncDirectory(fs::path(projections_path_) / "quarantine"); }
bool QueryProjectionStore::projections_enabled() const { std::lock_guard<std::mutex> lock(mutex_); return enabled_; }
std::optional<uint64_t> QueryProjectionStore::current_generation_id() const { std::lock_guard<std::mutex> lock(mutex_); return current_ ? std::optional<uint64_t>(current_->manifest.generation_id) : std::nullopt; }
std::optional<CommitSeq> QueryProjectionStore::current_base_seq() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_ ? std::optional<CommitSeq>(current_->manifest.base_seq)
                  : std::nullopt;
}
std::optional<ProjectionManifest> QueryProjectionStore::current_manifest() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_ ? std::optional<ProjectionManifest>(current_->manifest)
                  : std::nullopt;
}
StatusOr<std::vector<ProjectionChain>> QueryProjectionStore::ReadChains(
    const CoverageRequest& request) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!enabled_ || !current_) return Status::NotFound("projection store", "coverage is unavailable");
  if (request.generation_id && *request.generation_id != current_->manifest.generation_id) {
    return Status::Conflict("projection store", "projection generation is no longer current");
  }
  if (request.expected_base_seq && *request.expected_base_seq != current_->manifest.base_seq) {
    return Status::Conflict("projection store", "projection base changed");
  }
  if (!request.database_identity.empty() &&
      request.database_identity != current_->manifest.database_identity) {
    return Status::IdentityConflict("projection store", "projection identity changed");
  }
  auto chains = ReadChainsForGeneration(current_, request);
  if (!chains.ok() && (chains.status().IsCorruption() || chains.status().IsIOError())) {
    // A derived page is never authoritative. Disable the affected generation
    // immediately so subsequent readers fall back to CedarParquet facts; the
    // generation remains pinned until existing leases release it.
    current_->retired = true;
    retired_.push_back(current_);
    current_.reset();
    enabled_ = false;
  }
  return chains;
}

StatusOr<std::vector<ProjectionChain>> QueryProjectionStore::ReadChains(
    const CoverageRequest& request, const ProjectionGeneration& generation) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_ || !generation.state_) {
    return Status::NotFound("projection store", "projection generation is no longer available");
  }
  auto chains = ReadChainsForGeneration(generation.state_, request);
  return chains;
}
}  // namespace cedar::internal
