#ifndef CEDAR_SERVER_H_
#define CEDAR_SERVER_H_

#include <cstdint>
#include <memory>
#include <string>

#include "cedar/core/status.h"
#include "cedar/database.h"
#include "cedar/cypher/session.h"

namespace cedar::server {

struct ServerConfig {
  std::string database_path;
  std::string bind_address = "127.0.0.1";
  uint16_t port = 7687;
  std::string lock_path;
  std::string pid_path;
  std::string schema_path;
  // Optional constant-time Bolt HELLO credential. Empty keeps local anonymous
  // development behavior; production deployments should set this or use an
  // authenticated TLS terminator.
  std::string auth_token;
  std::string graph;
  PartId part_id{0};
  uint32_t worker_threads = 1;
  uint32_t max_sessions = 64;
  uint32_t max_pull_records = 128;
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
  bool Live() const;
  bool Ready() const;
  uint16_t port() const;
  std::string Metrics() const;

 private:
  class State;
  void AcceptLoop();
  void WorkerLoop();
  void HandleClient(int fd);
  Status AcquireLock();
  void ReleaseLock();

  std::unique_ptr<State> state_;
};

}  // namespace cedar::server

#endif  // CEDAR_SERVER_H_
