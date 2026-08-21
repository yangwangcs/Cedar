#include "query/resource/query_scratch.h"

#include <array>
#include <chrono>
#include <fstream>
#include <limits>

#include "cedar/core/crc32c.h"

namespace cedar::internal {
namespace {
std::atomic<uint64_t> g_reserved_free_space{0};
constexpr std::array<char, 8> kMagic = {'C', 'D', 'R', 'S', 'C', 'R', '1', '\0'};

bool SafeName(const std::string& name) {
  if (name.empty() || name == "." || name == ".." || name.find('/') != std::string::npos ||
      name.find('\\') != std::string::npos) return false;
  return true;
}

template <typename T>
bool Read(std::ifstream& in, T* value) {
  in.read(reinterpret_cast<char*>(value), sizeof(T));
  return in.good();
}
template <typename T>
void Write(std::ofstream& out, T value) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(T));
}
}

QueryScratch::QueryScratch(std::filesystem::path database_root,
                           std::string instance, std::string query_id,
                           uint64_t disk_budget_bytes,
                           QueryReservation* reservation)
    : database_root_(std::move(database_root)),
      query_dir_(database_root_ / "query" / "scratch" / instance / query_id),
      query_id_(std::move(query_id)), disk_budget_bytes_(disk_budget_bytes),
      reservation_(reservation),
      rate_window_start_(std::chrono::steady_clock::now()) {}

QueryScratch::QueryScratch(std::filesystem::path database_root,
                           std::string instance, std::string query_id,
                           uint64_t disk_budget_bytes,
                           uint64_t free_space_reserve_bytes)
    : QueryScratch(std::move(database_root), std::move(instance),
                   std::move(query_id), disk_budget_bytes, nullptr) {
  free_space_reserve_bytes_ = free_space_reserve_bytes;
}

QueryScratch::~QueryScratch() { Cleanup().IgnoreError(); }

Status QueryScratch::ValidateChild(const std::string& name) const {
  if (!SafeName(name)) return Status::InvalidArgument("query scratch", "invalid child name");
  if (std::filesystem::exists(query_dir_ / name) &&
      std::filesystem::is_symlink(query_dir_ / name)) {
    return Status::InvalidArgument("query scratch", "symlink child is not allowed");
  }
  return Status::OK();
}

void QueryScratch::SetRateLimits(uint64_t read_bytes_per_second,
                                 uint64_t scratch_bytes_per_second) {
  std::lock_guard<std::mutex> lock(rate_mutex_);
  read_bytes_per_second_ = read_bytes_per_second;
  scratch_bytes_per_second_ = scratch_bytes_per_second;
  rate_window_start_ = std::chrono::steady_clock::now();
  rate_read_bytes_ = 0;
  rate_scratch_bytes_ = 0;
}

void QueryScratch::SetIoAdmission(
    std::function<StatusOr<std::shared_ptr<IoPermit>>(uint64_t)> acquire) {
  io_admission_ = std::move(acquire);
}

void QueryScratch::SetAbortCheck(std::function<Status()> check) {
  abort_check_ = std::move(check);
}

Status QueryScratch::ConsumeRate(uint64_t bytes, bool read) const {
  const uint64_t limit = read ? read_bytes_per_second_ : scratch_bytes_per_second_;
  if (limit == 0 || bytes == 0) return Status::OK();
  std::lock_guard<std::mutex> lock(rate_mutex_);
  const auto now = std::chrono::steady_clock::now();
  if (now - rate_window_start_ >= std::chrono::seconds(1)) {
    rate_window_start_ = now;
    rate_read_bytes_ = 0;
    rate_scratch_bytes_ = 0;
  }
  uint64_t* used = read ? &rate_read_bytes_ : &rate_scratch_bytes_;
  if (bytes > limit || *used > limit - bytes) {
    return Status::ResourceExhausted(
        "query scratch", read ? "read_bytes_per_second rate exhausted"
                               : "scratch_bytes_per_second rate exhausted");
  }
  *used += bytes;
  return Status::OK();
}

Status QueryScratch::EnsureDirectory() const {
  if (!SafeName(query_dir_.parent_path().filename().string()) ||
      !SafeName(query_dir_.filename().string()) ||
      query_dir_.parent_path().parent_path().filename() != "scratch" ||
      query_dir_.parent_path().parent_path().parent_path().filename() != "query") {
    return Status::InvalidArgument("query scratch", "invalid scratch identity");
  }
  if (created_) return Status::OK();
  std::filesystem::path current = database_root_;
  const auto relative = query_dir_.lexically_relative(database_root_);
  for (const auto& component : relative) {
    current /= component;
    std::error_code component_ec;
    if (std::filesystem::exists(current, component_ec) &&
        std::filesystem::is_symlink(current, component_ec)) {
      return Status::InvalidArgument("query scratch", "scratch ancestor is a symlink");
    }
  }
  std::error_code ec;
  std::filesystem::create_directories(query_dir_, ec);
  if (ec) return Status::IOError("query scratch", ec.message());
  if (!std::filesystem::is_directory(query_dir_) || std::filesystem::is_symlink(query_dir_)) {
    return Status::InvalidArgument("query scratch", "scratch directory is not safe");
  }
  created_ = true;
  return Status::OK();
}

StatusOr<std::filesystem::path> QueryScratch::WriteRun(const std::string& name,
                                                       const std::string& payload) {
  if (abort_check_) {
    if (Status status = abort_check_(); !status.ok()) return status;
  }
  if (Status status = ValidateChild(name); !status.ok()) return status;
  const auto path = query_dir_ / name;
  if (std::filesystem::exists(path)) {
    return Status::InvalidArgument("query scratch", "scratch child already exists");
  }
  if (payload.size() > std::numeric_limits<uint64_t>::max() - 64) {
    return Status::ResourceExhausted("query scratch", "scratch_bytes overflow");
  }
  uint64_t bytes = kMagic.size();
  auto add = [&bytes](uint64_t value) {
    if (value > std::numeric_limits<uint64_t>::max() - bytes) return false;
    bytes += value;
    return true;
  };
  if (!add(sizeof(uint32_t)) || !add(query_id_.size()) || !add(sizeof(uint64_t)) ||
      !add(payload.size()) || !add(sizeof(uint32_t))) {
    return Status::ResourceExhausted("query scratch", "scratch_bytes overflow");
  }
  std::shared_ptr<IoPermit> io_permit;
  if (io_admission_) {
    auto admitted = io_admission_(bytes);
    if (!admitted.ok()) return admitted.status();
    io_permit = std::move(admitted).ConsumeValueOrDie();
  }
  if (bytes > disk_budget_bytes_ || written_bytes_ > disk_budget_bytes_ - bytes) {
    return Status::ResourceExhausted("query scratch", "scratch_bytes budget exhausted");
  }
  if (Status status = ConsumeRate(bytes, false); !status.ok()) return status;
  if (reservation_ != nullptr) {
    Status status = reservation_->ReserveScratch(bytes);
    if (!status.ok()) return status;
  }
  reserved_bytes_ += bytes;
  std::error_code ec;
  const auto space = std::filesystem::space(database_root_, ec);
  if (!ec && free_space_reserve_bytes_ != 0 && !free_space_admitted_) {
    bool admitted = false;
    uint64_t current = g_reserved_free_space.load(std::memory_order_acquire);
    if (free_space_reserve_bytes_ <= space.available) {
      for (;;) {
        if (current > space.available - free_space_reserve_bytes_) break;
        if (g_reserved_free_space.compare_exchange_weak(
                current, current + free_space_reserve_bytes_,
                std::memory_order_acq_rel)) {
          admitted = true;
          break;
        }
      }
    }
    if (!admitted) {
      if (reservation_ != nullptr) reservation_->ReleaseScratch(bytes);
      reserved_bytes_ -= bytes;
      return Status::ResourceExhausted("query scratch", "insufficient free disk space");
    }
    free_space_admitted_ = true;
  }
  if (!ec && (free_space_reserve_bytes_ > std::numeric_limits<uint64_t>::max() - bytes ||
              space.available < bytes + free_space_reserve_bytes_)) {
    if (reservation_ != nullptr) reservation_->ReleaseScratch(bytes);
    reserved_bytes_ -= bytes;
    return Status::ResourceExhausted("query scratch", "insufficient free disk space");
  }
  if (Status status = EnsureDirectory(); !status.ok()) {
    if (reservation_ != nullptr) reservation_->ReleaseScratch(bytes);
    reserved_bytes_ -= bytes;
    return status;
  }
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) {
    if (reservation_ != nullptr) reservation_->ReleaseScratch(bytes);
    reserved_bytes_ -= bytes;
    return Status::IOError("query scratch", "cannot create scratch block");
  }
  out.write(kMagic.data(), kMagic.size());
  Write<uint32_t>(out, static_cast<uint32_t>(query_id_.size()));
  out.write(query_id_.data(), static_cast<std::streamsize>(query_id_.size()));
  Write<uint64_t>(out, static_cast<uint64_t>(payload.size()));
  out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
  Write<uint32_t>(out, crc32c::Extend(0, payload.data(), payload.size()));
  out.close();
  if (!out) {
    if (reservation_ != nullptr) reservation_->ReleaseScratch(bytes);
    reserved_bytes_ -= bytes;
    return Status::IOError("query scratch", "cannot finalize scratch block");
  }
  written_bytes_ += bytes;
  return path;
}

StatusOr<std::string> QueryScratch::ReadRun(const std::filesystem::path& path) const {
  if (abort_check_) {
    if (Status status = abort_check_(); !status.ok()) return status;
  }
  if (path.parent_path() != query_dir_ || !SafeName(path.filename().string()) ||
      std::filesystem::is_symlink(path)) {
    return Status::InvalidArgument("query scratch", "scratch path escapes query directory");
  }
  std::ifstream in(path, std::ios::binary);
  if (!in) return Status::NotFound("query scratch", "scratch block missing");
  std::error_code size_ec;
  const uintmax_t file_size = std::filesystem::file_size(path, size_ec);
  if (size_ec || file_size < kMagic.size() + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t)) {
    return Status::Corruption("query scratch", "scratch block is truncated");
  }
  std::array<char, 8> magic{};
  in.read(magic.data(), magic.size());
  if (magic != kMagic) return Status::Corruption("query scratch", "scratch magic mismatch");
  uint32_t id_len = 0;
  if (!Read(in, &id_len) || id_len != query_id_.size()) return Status::Corruption("query scratch", "scratch query id mismatch");
  std::string id(id_len, '\0');
  in.read(id.data(), id.size());
  if (!in || id != query_id_) return Status::Corruption("query scratch", "scratch query id mismatch");
  uint64_t length = 0;
  const uint64_t fixed = kMagic.size() + sizeof(uint32_t) + id_len + sizeof(uint64_t) + sizeof(uint32_t);
  if (!Read(in, &length) || file_size < fixed ||
      length > std::numeric_limits<size_t>::max() ||
      length > file_size - fixed) return Status::Corruption("query scratch", "scratch length invalid");
  std::shared_ptr<IoPermit> io_permit;
  if (io_admission_) {
    auto admitted = io_admission_(length);
    if (!admitted.ok()) return admitted.status();
    io_permit = std::move(admitted).ConsumeValueOrDie();
  }
  if (reservation_ != nullptr) {
    if (Status status = reservation_->ReserveReadBytes(length); !status.ok()) {
      return status;
    }
  }
  bool read_reserved = reservation_ != nullptr;
  auto release_read = [&] {
    if (read_reserved) {
      reservation_->ReleaseReadBytes(length);
      read_reserved = false;
    }
  };
  if (Status status = ConsumeRate(length, true); !status.ok()) {
    release_read();
    return status;
  }
  std::string payload(static_cast<size_t>(length), '\0');
  in.read(payload.data(), payload.size());
  uint32_t checksum = 0;
  if (!in || !Read(in, &checksum) || checksum != crc32c::Extend(0, payload.data(), payload.size())) {
    release_read();
    return Status::Corruption("query scratch", "scratch checksum mismatch");
  }
  char trailing = 0;
  if (in.read(&trailing, 1)) {
    release_read();
    return Status::Corruption("query scratch", "scratch block has trailing bytes");
  }
  read_reserved = false;
  return payload;
}

Status QueryScratch::Cleanup() {
  if (!created_ && !std::filesystem::exists(query_dir_)) return Status::OK();
  std::error_code ec;
  const auto canonical_root = std::filesystem::weakly_canonical(database_root_, ec);
  const auto canonical_query = std::filesystem::weakly_canonical(query_dir_, ec);
  const auto relative = canonical_query.lexically_relative(canonical_root);
  if (ec || relative.empty() || relative.string().find("..") == 0) return Status::InvalidArgument("query scratch", "scratch cleanup path escapes root");
  std::filesystem::remove_all(query_dir_, ec);
  if (ec) return Status::IOError("query scratch", ec.message());
  for (auto parent = query_dir_.parent_path(); !parent.empty() && parent != database_root_; parent = parent.parent_path()) {
    std::error_code remove_ec;
    if (!std::filesystem::is_empty(parent, remove_ec) || remove_ec) break;
    std::filesystem::remove(parent, remove_ec);
    if (remove_ec) break;
  }
  created_ = false;
  written_bytes_ = 0;
  if (reservation_ != nullptr && reserved_bytes_ != 0) {
    reservation_->ReleaseScratch(reserved_bytes_);
  }
  reserved_bytes_ = 0;
  if (free_space_admitted_) {
    g_reserved_free_space.fetch_sub(free_space_reserve_bytes_,
                                     std::memory_order_acq_rel);
    free_space_admitted_ = false;
  }
  return Status::OK();
}

Status QueryScratch::CleanupOldInstances(
    const std::filesystem::path& database_root,
    const std::string& active_instance) {
  if (!SafeName(active_instance)) {
    return Status::InvalidArgument("query scratch", "invalid active instance");
  }
  const auto scratch_root = database_root / "query" / "scratch";
  std::error_code ec;
  if (!std::filesystem::exists(scratch_root, ec)) return Status::OK();
  if (std::filesystem::is_symlink(scratch_root)) {
    return Status::InvalidArgument("query scratch", "scratch root is a symlink");
  }
  for (const auto& child : std::filesystem::directory_iterator(scratch_root, ec)) {
    if (ec) return Status::IOError("query scratch", ec.message());
    if (child.path().filename() == active_instance) continue;
    if (!SafeName(child.path().filename().string()) || child.is_symlink()) {
      return Status::InvalidArgument("query scratch", "invalid scratch instance");
    }
    std::filesystem::remove_all(child.path(), ec);
    if (ec) return Status::IOError("query scratch", ec.message());
  }
  return Status::OK();
}

}  // namespace cedar::internal
