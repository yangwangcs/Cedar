#ifndef CEDAR_SERVER_H_
#define CEDAR_SERVER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "cedar/core/status.h"
#include "cedar/database.h"

namespace cedar::server {

struct ServerConfig {
  std::string database_path;
  std::string bind_address = "127.0.0.1";
  uint16_t port = 7687;
  std::string lock_path;
  std::string pid_path;
  uint32_t max_frame_bytes = 1U * 1024U * 1024U;

  Status Validate() const;
  static StatusOr<ServerConfig> FromArgs(int argc, char** argv);
};

class Server {
 public:
  explicit Server(ServerConfig config);
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  Status Start();
  Status Stop();
  bool Live() const { return live_.load(std::memory_order_acquire); }
  bool Ready() const { return ready_.load(std::memory_order_acquire); }
  uint16_t port() const;
  std::string Metrics() const;

 private:
  void AcceptLoop();
  void HandleClient(int fd);
  Status AcquireLock();
  void ReleaseLock();

  ServerConfig config_;
  mutable std::mutex mutex_;
  std::unique_ptr<Database> database_;
  int listen_fd_ = -1;
  int lock_fd_ = -1;
  uint16_t bound_port_ = 0;
  std::thread accept_thread_;
  std::atomic<bool> live_{false};
  std::atomic<bool> ready_{false};
  std::atomic<bool> stopping_{false};
  std::atomic<int> active_client_fd_{-1};
};

}  // namespace cedar::server

#endif  // CEDAR_SERVER_H_
