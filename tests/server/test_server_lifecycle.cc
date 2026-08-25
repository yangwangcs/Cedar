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

void SendBoltMessage(int fd, const std::string& payload) {
  std::string frame;
  frame.push_back(static_cast<char>((payload.size() >> 8) & 0xff));
  frame.push_back(static_cast<char>(payload.size() & 0xff));
  frame += payload;
  frame.append("\x00\x00", 2);
  ASSERT_EQ(::send(fd, frame.data(), frame.size(), 0),
            static_cast<ssize_t>(frame.size()));
}

std::string ReceiveBoltMessage(int fd) {
  unsigned char header[2]{};
  if (::recv(fd, header, sizeof(header), MSG_WAITALL) != 2) return {};
  const size_t length = (static_cast<size_t>(header[0]) << 8) | header[1];
  std::string payload(length, '\0');
  if (::recv(fd, payload.data(), payload.size(), MSG_WAITALL) !=
      static_cast<ssize_t>(payload.size())) return {};
  unsigned char terminator[2]{};
  if (::recv(fd, terminator, sizeof(terminator), MSG_WAITALL) != 2) return {};
  return payload;
}

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
  const std::string metrics = server.Metrics();
  EXPECT_NE(metrics.find("cedar_server_live"), std::string::npos);
  EXPECT_NE(metrics.find("cedar_query_physical_bytes"), std::string::npos);
  EXPECT_NE(metrics.find("cedar_commit_submitted"), std::string::npos);
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

  char auth0[] = "cedar-server";
  char auth1[] = "--db";
  char auth2[] = "/tmp/cedar-server-auth-config";
  char auth3[] = "--auth-token";
  char auth4[] = "secret";
  char* auth_argv[] = {auth0, auth1, auth2, auth3, auth4};
  const auto authenticated = ServerConfig::FromArgs(5, auth_argv);
  ASSERT_TRUE(authenticated.ok()) << authenticated.status().ToString();
  EXPECT_EQ(authenticated.ValueOrDie().auth_token, "secret");
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
  timeval timeout{2, 0};
  ASSERT_EQ(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)), 0);
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

TEST(ServerLifecycleTest, AnswersBoundedBoltHelloOnTheSameDatabaseProcess) {
  char pattern[] = "/tmp/cedar_server_bolt_XXXXXX";
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
  const std::string hello("\x00\x03\xB1\x01\xA0\x00\x00", 7);
  ASSERT_EQ(::send(fd, hello.data(), hello.size(), 0),
            static_cast<ssize_t>(hello.size()));
  char response[16]{};
  const ssize_t count = ::recv(fd, response, sizeof(response), 0);
  ASSERT_EQ(count, 7);
  EXPECT_EQ(std::string(response, static_cast<size_t>(count)),
            std::string("\x00\x03\xB1\x70\xA0\x00\x00", 7));
  ::close(fd);
  ASSERT_TRUE(server.Stop().ok());
  std::filesystem::remove_all(pattern);
}

TEST(ServerLifecycleTest, RequiresConfiguredBoltAuthBeforeRequests) {
  char pattern[] = "/tmp/cedar_server_bolt_auth_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  ServerConfig config;
  config.database_path = pattern;
  config.lock_path = std::string(pattern) + ".lock";
  config.pid_path = std::string(pattern) + ".pid";
  config.port = 0;
  config.auth_token = "secret";
  Server server(config);
  ASSERT_TRUE(server.Start().ok());
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(server.port());
  ASSERT_EQ(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
  ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  const std::string handshake(
      "\x60\x60\xB0\x17\x00\x00\x04\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", 20);
  ASSERT_EQ(::send(fd, handshake.data(), handshake.size(), 0),
            static_cast<ssize_t>(handshake.size()));
  char negotiated[4]{};
  ASSERT_EQ(::recv(fd, negotiated, sizeof(negotiated), MSG_WAITALL), 4);
  SendBoltMessage(fd, std::string("\xB1\x10\xA0", 3));
  const std::string unauthenticated = ReceiveBoltMessage(fd);
  ASSERT_GE(unauthenticated.size(), 2U);
  EXPECT_EQ(static_cast<uint8_t>(unauthenticated[1]), 0x7F);
  const std::string hello = {static_cast<char>(0xB1), static_cast<char>(0x01),
                             static_cast<char>(0xA1), static_cast<char>(0x8B),
                             'c', 'r', 'e', 'd', 'e', 'n', 't', 'i', 'a', 'l', 's',
                             static_cast<char>(0x86), 's', 'e', 'c', 'r', 'e', 't'};
  SendBoltMessage(fd, hello);
  EXPECT_EQ(ReceiveBoltMessage(fd), std::string("\xB1\x70\xA0", 3));
  ::close(fd);
  ASSERT_TRUE(server.Stop().ok());
  std::filesystem::remove_all(pattern);
}

TEST(ServerLifecycleTest, RunsAndPullsThroughStatefulBoltSession) {
  char pattern[] = "/tmp/cedar_server_bolt_session_XXXXXX";
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
  timeval timeout{2, 0};
  ASSERT_EQ(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)), 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(server.port());
  ASSERT_EQ(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
  ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  const std::string handshake("\x60\x60\xB0\x17\x00\x00\x04\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", 20);
  ASSERT_EQ(::send(fd, handshake.data(), handshake.size(), 0),
            static_cast<ssize_t>(handshake.size()));
  char negotiated[4]{};
  ASSERT_EQ(::recv(fd, negotiated, sizeof(negotiated), MSG_WAITALL), 4);
  SendBoltMessage(fd, std::string("\xB1\x01\xA0", 3));
  EXPECT_EQ(ReceiveBoltMessage(fd), std::string("\xB1\x70\xA0", 3));
  const auto send_run = [&](const std::string& query) {
    std::string run{static_cast<char>(0xB3), static_cast<char>(0x10),
                    static_cast<char>(0xD0), static_cast<char>(query.size())};
    run += query;
    run.append("\xA0\xA0", 2);
    SendBoltMessage(fd, run);
    return ReceiveBoltMessage(fd);
  };
  SendBoltMessage(fd, std::string("\xB0\x11", 2));
  EXPECT_EQ(ReceiveBoltMessage(fd), std::string("\xB1\x70\xA0", 3));
  const std::string create_response = send_run("CREATE (a)");
  ASSERT_GE(create_response.size(), 2U);
  EXPECT_EQ(static_cast<uint8_t>(create_response[1]), 0x70);
  SendBoltMessage(fd, std::string("\xB0\x13", 2));
  EXPECT_EQ(ReceiveBoltMessage(fd), std::string("\xB1\x70\xA0", 3));
  const std::string run_response = send_run("MATCH (v) RETURN v");
  ASSERT_GE(run_response.size(), 2U);
  EXPECT_EQ(static_cast<uint8_t>(run_response[1]), 0x70);
  SendBoltMessage(fd, std::string("\xB1\x3F\xA0", 3));
  EXPECT_EQ(ReceiveBoltMessage(fd), std::string("\xB1\x70\xA0", 3));

  SendBoltMessage(fd, std::string("\xB0\x11", 2));
  EXPECT_EQ(ReceiveBoltMessage(fd), std::string("\xB1\x70\xA0", 3));
  ASSERT_GE(send_run("CREATE (a)").size(), 2U);
  const std::string dirty_read = send_run("MATCH (v) RETURN v");
  ASSERT_GE(dirty_read.size(), 2U);
  EXPECT_EQ(static_cast<uint8_t>(dirty_read[1]), 0x70);
  SendBoltMessage(fd, std::string("\xB1\x3F\xA0", 3));
  const std::string dirty_record = ReceiveBoltMessage(fd);
  ASSERT_FALSE(dirty_record.empty());
  EXPECT_EQ(static_cast<uint8_t>(dirty_record[1]), 0x71);
  EXPECT_EQ(ReceiveBoltMessage(fd), std::string("\xB1\x70\xA0", 3));
  SendBoltMessage(fd, std::string("\xB0\x13", 2));
  EXPECT_EQ(ReceiveBoltMessage(fd), std::string("\xB1\x70\xA0", 3));

  SendBoltMessage(fd, std::string("\xB0\x11", 2));
  EXPECT_EQ(ReceiveBoltMessage(fd), std::string("\xB1\x70\xA0", 3));
  ASSERT_GE(send_run("CREATE (a)").size(), 2U);
  SendBoltMessage(fd, std::string("\xB0\x12", 2));
  EXPECT_EQ(ReceiveBoltMessage(fd), std::string("\xB1\x70\xA0", 3));
  ASSERT_GE(send_run("MATCH (v) RETURN v").size(), 2U);
  SendBoltMessage(fd, std::string("\xB1\x3F\xA0", 3));
  const std::string record = ReceiveBoltMessage(fd);
  ASSERT_FALSE(record.empty());
  EXPECT_EQ(static_cast<uint8_t>(record[1]), 0x71);
  EXPECT_EQ(ReceiveBoltMessage(fd), std::string("\xB1\x70\xA0", 3));
  SendBoltMessage(fd, std::string("\xB0\x02", 2));
  EXPECT_EQ(ReceiveBoltMessage(fd), std::string("\xB1\x7E\xA0", 3));
  ::close(fd);
  ASSERT_TRUE(server.Stop().ok());
  std::filesystem::remove_all(pattern);
}

}  // namespace
}  // namespace cedar::server
