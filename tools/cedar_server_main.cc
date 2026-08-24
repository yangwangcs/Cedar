#include <csignal>
#include <chrono>
#include <cstdio>
#include <thread>

#include "cedar/server.h"

namespace {
cedar::server::Server* active_server = nullptr;
void OnSignal(int) {
  if (active_server != nullptr) active_server->Stop().IgnoreError();
}
}  // namespace

int main(int argc, char** argv) {
  const auto config = cedar::server::ServerConfig::FromArgs(argc, argv);
  if (!config.ok()) {
    std::fprintf(stderr, "%s\n", config.status().ToString().c_str());
    return 2;
  }
  cedar::server::Server server(config.ValueOrDie());
  active_server = &server;
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  const cedar::Status started = server.Start();
  if (!started.ok()) {
    std::fprintf(stderr, "%s\n", started.ToString().c_str());
    return 1;
  }
  while (server.Live()) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  return 0;
}
