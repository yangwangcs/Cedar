#include "cedar/server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <sstream>

#include "cedar/cypher.h"
#include "cedar/server/bolt_codec.h"

namespace cedar::server {
namespace {

Status ServerError(const char* message) {
  return Status::InvalidArgument("cedar-server", message);
}

}  // namespace

Status ServerConfig::Validate() const {
  if (database_path.empty() || bind_address.empty() ||
      max_frame_bytes == 0 || max_frame_bytes > 64U * 1024U * 1024U) {
    return ServerError("invalid server configuration");
  }
  return Status::OK();
}

StatusOr<ServerConfig> ServerConfig::FromArgs(int argc, char** argv) {
  ServerConfig config;
  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    auto value = [&](const char* name) -> StatusOr<std::string> {
      if (index + 1 >= argc) return ServerError("missing option value");
      return std::string(argv[++index]);
    };
    if (arg == "--db") {
      auto parsed = value("--db");
      if (!parsed.ok()) return parsed.status();
      config.database_path = parsed.ValueOrDie();
    } else if (arg == "--bind") {
      auto parsed = value("--bind");
      if (!parsed.ok()) return parsed.status();
      config.bind_address = parsed.ValueOrDie();
    } else if (arg == "--port") {
      auto parsed = value("--port");
      if (!parsed.ok()) return parsed.status();
      config.port = static_cast<uint16_t>(std::stoul(parsed.ValueOrDie()));
    } else if (arg == "--lock") {
      auto parsed = value("--lock");
      if (!parsed.ok()) return parsed.status();
      config.lock_path = parsed.ValueOrDie();
    } else if (arg == "--pid") {
      auto parsed = value("--pid");
      if (!parsed.ok()) return parsed.status();
      config.pid_path = parsed.ValueOrDie();
    } else {
      return ServerError("unknown server option");
    }
  }
  if (config.lock_path.empty()) config.lock_path = config.database_path + ".lock";
  if (config.pid_path.empty()) config.pid_path = config.database_path + ".pid";
  const Status valid = config.Validate();
  if (!valid.ok()) return valid;
  return config;
}

Server::Server(ServerConfig config) : config_(std::move(config)) {}

Server::~Server() { Stop().IgnoreError(); }

Status Server::AcquireLock() {
  lock_fd_ = ::open(config_.lock_path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
  if (lock_fd_ < 0) return Status::Conflict("cedar-server", "database lock is held");
  const std::string pid = std::to_string(static_cast<long long>(::getpid())) + "\n";
  if (::write(lock_fd_, pid.data(), pid.size()) != static_cast<ssize_t>(pid.size())) {
    ReleaseLock();
    return Status::IOError("cedar-server", "cannot write lock file");
  }
  if (!config_.pid_path.empty()) {
    const int pid_fd = ::open(config_.pid_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (pid_fd >= 0) {
      ::write(pid_fd, pid.data(), pid.size());
      ::close(pid_fd);
    }
  }
  return Status::OK();
}

void Server::ReleaseLock() {
  if (lock_fd_ >= 0) {
    ::close(lock_fd_);
    lock_fd_ = -1;
  }
  std::error_code error;
  std::filesystem::remove(config_.lock_path, error);
  if (!config_.pid_path.empty()) std::filesystem::remove(config_.pid_path, error);
}

Status Server::Start() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (live_.load(std::memory_order_acquire)) return Status::OK();
  const Status valid = config_.Validate();
  if (!valid.ok()) return valid;
  const Status locked = AcquireLock();
  if (!locked.ok()) return locked;
  auto database = Database::Open(DatabaseOptions{.path = config_.database_path});
  if (!database.ok()) {
    ReleaseLock();
    return database.status();
  }
  database_ = std::move(database).ConsumeValueOrDie();
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    database_->Close().IgnoreError();
    database_.reset();
    ReleaseLock();
    return Status::IOError("cedar-server", "cannot create listener");
  }
  int reuse = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(config_.port);
  if (::inet_pton(AF_INET, config_.bind_address.c_str(), &address.sin_addr) != 1 ||
      ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
      ::listen(listen_fd_, 32) != 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    database_->Close().IgnoreError();
    database_.reset();
    ReleaseLock();
    return Status::IOError("cedar-server", "cannot bind listener");
  }
  socklen_t length = sizeof(address);
  ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&address), &length);
  bound_port_ = ntohs(address.sin_port);
  live_.store(true, std::memory_order_release);
  ready_.store(true, std::memory_order_release);
  stopping_.store(false, std::memory_order_release);
  accept_thread_ = std::thread(&Server::AcceptLoop, this);
  return Status::OK();
}

Status Server::Stop() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!live_.load(std::memory_order_acquire) && database_ == nullptr) return Status::OK();
  stopping_.store(true, std::memory_order_release);
  ready_.store(false, std::memory_order_release);
  if (listen_fd_ >= 0) {
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    listen_fd_ = -1;
  }
  if (accept_thread_.joinable()) accept_thread_.join();
  live_.store(false, std::memory_order_release);
  if (database_) {
    const Status closed = database_->Close();
    database_.reset();
    ReleaseLock();
    return closed;
  }
  ReleaseLock();
  return Status::OK();
}

uint16_t Server::port() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return bound_port_;
}

std::string Server::Metrics() const {
  std::ostringstream out;
  out << "cedar_server_live " << (Live() ? 1 : 0) << '\n'
      << "cedar_server_ready " << (Ready() ? 1 : 0) << '\n';
  return out.str();
}

void Server::AcceptLoop() {
  while (!stopping_.load(std::memory_order_acquire)) {
    const int fd = ::accept(listen_fd_, nullptr, nullptr);
    if (fd < 0) {
      if (stopping_.load(std::memory_order_acquire)) break;
      continue;
    }
    HandleClient(fd);
    ::close(fd);
  }
}

void Server::HandleClient(int fd) {
  std::string request;
  request.resize(config_.max_frame_bytes);
  const ssize_t count = ::recv(fd, request.data(), request.size(), 0);
  if (count <= 0 || static_cast<size_t>(count) > config_.max_frame_bytes) return;
  request.resize(static_cast<size_t>(count));
  std::string response;
  if (request.size() == 20 && request.compare(0, 4, "\x60\x60\xB0\x17", 4) == 0) {
    const auto handshake = NegotiateBoltHandshake(request);
    if (!handshake.ok()) return;
    response = handshake.ValueOrDie();
  } else if (request == "GET /live\n" || request == "GET /live\r\n") {
    response = Live() ? "200 OK\n" : "503 NOT_READY\n";
  } else if (request == "GET /ready\n" || request == "GET /ready\r\n") {
    response = Ready() ? "200 OK\n" : "503 NOT_READY\n";
  } else if (request == "GET /metrics\n" || request == "GET /metrics\r\n") {
    response = Metrics();
  } else if (request.rfind("RUN ", 0) == 0 && database_) {
    const std::string source = request.substr(4);
    auto prepared = cypher::PrepareCypher(*database_, source, cypher::SchemaCatalog{});
    response = prepared.ok() ? "200 OK " + std::to_string(prepared.ValueOrDie().fingerprint()) + "\n"
                             : "400 " + prepared.status().ToString() + "\n";
  } else {
    response = "400 BAD_REQUEST\n";
  }
  ::send(fd, response.data(), response.size(), 0);
}

}  // namespace cedar::server
