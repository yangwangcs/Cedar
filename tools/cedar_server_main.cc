#include <csignal>
#include <chrono>
#include <cstdio>
#include <thread>
#include <unistd.h>

#include "cedar/server.h"

namespace {
volatile std::sig_atomic_t shutdown_requested = 0;
void OnSignal(int) {
  if (shutdown_requested != 0) _exit(1);
  shutdown_requested = 1;
}
}  // namespace

int main(int argc, char** argv) {
  const auto config = cedar::server::ServerConfig::FromArgs(argc, argv);
  if (!config.ok()) {
    std::fprintf(stderr, "%s\n", config.status().ToString().c_str());
    return 2;
  }
  cedar::server::Server server(config.ValueOrDie());
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  const cedar::Status started = server.Start();
  if (!started.ok()) {
    std::fprintf(stderr, "%s\n", started.ToString().c_str());
    return 1;
  }
  while (server.Live()) {
    if (shutdown_requested != 0) server.Stop().IgnoreError();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return 0;
}
