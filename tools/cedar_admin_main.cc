#include <cstdio>

#include "cedar/server.h"

int main(int argc, char** argv) {
  const auto config = cedar::server::ServerConfig::FromArgs(argc, argv);
  if (!config.ok()) {
    std::fprintf(stderr, "%s\n", config.status().ToString().c_str());
    return 2;
  }
  std::printf("database=%s bind=%s port=%u lock=%s pid=%s\n",
              config.ValueOrDie().database_path.c_str(),
              config.ValueOrDie().bind_address.c_str(), config.ValueOrDie().port,
              config.ValueOrDie().lock_path.c_str(), config.ValueOrDie().pid_path.c_str());
  return 0;
}
