#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "cedar/server.h"

namespace cedar::server {
namespace {

TEST(ServerLifecycleTest, OwnsOneDatabaseAndStopsIdempotently) {
  char pattern[] = "/tmp/cedar_server_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  ServerConfig config;
  config.database_path = pattern;
  config.lock_path = std::string(pattern) + ".lock";
  config.pid_path = std::string(pattern) + ".pid";
  config.port = 0;
  // Port zero is useful for test allocation even though production config
  // requires an explicit port; the listener still reports its bound port.
  ASSERT_TRUE(config.Validate().ok());
  Server server(config);
  ASSERT_TRUE(server.Start().ok());
  EXPECT_TRUE(server.Live());
  EXPECT_TRUE(server.Ready());
  EXPECT_NE(server.port(), 0U);
  EXPECT_TRUE(server.Stop().ok());
  EXPECT_TRUE(server.Stop().ok());
  EXPECT_FALSE(server.Live());
  EXPECT_FALSE(std::filesystem::exists(config.lock_path));
  std::filesystem::remove_all(pattern);
}

TEST(ServerConfigTest, ParsesAndRejectsUnknownOptions) {
  char arg0[] = "cedar-server";
  char arg1[] = "--db";
  char arg2[] = "/tmp/cedar-server-config";
  char* argv[] = {arg0, arg1, arg2};
  const auto parsed = ServerConfig::FromArgs(3, argv);
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  EXPECT_EQ(parsed.ValueOrDie().database_path, "/tmp/cedar-server-config");
  char bad0[] = "cedar-server";
  char bad1[] = "--unknown";
  char* bad_argv[] = {bad0, bad1};
  EXPECT_FALSE(ServerConfig::FromArgs(2, bad_argv).ok());
}

TEST(ServerLifecycleTest, ServesHealthAndCypherPrepareOnOneProcess) {
  char pattern[] = "/tmp/cedar_server_protocol_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  ServerConfig config;
  config.database_path = pattern;
  config.lock_path = std::string(pattern) + ".lock";
  config.pid_path = std::string(pattern) + ".pid";
  config.port = 0;
  Server server(config);
  ASSERT_TRUE(server.Start().ok());
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(server.port());
  ASSERT_EQ(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
  ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  const std::string request = "RUN MATCH (v) RETURN v\n";
  ASSERT_EQ(::send(fd, request.data(), request.size(), 0),
            static_cast<ssize_t>(request.size()));
  char response[128]{};
  const ssize_t count = ::recv(fd, response, sizeof(response), 0);
  EXPECT_GT(count, 0);
  EXPECT_EQ(std::string(response, static_cast<size_t>(count)).rfind("200 OK ", 0), 0U);
  ::close(fd);
  ASSERT_TRUE(server.Stop().ok());
  std::filesystem::remove_all(pattern);
}

}  // namespace
}  // namespace cedar::server
