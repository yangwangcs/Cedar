#include "cedar/server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <optional>
#include <sstream>
#include <map>
#include <vector>

#include "cedar/cypher.h"
#include "cedar/cypher/write.h"
#include "cedar/server/bolt_codec.h"

namespace cedar::server {
namespace {

Status ServerError(const char* message) {
  return Status::InvalidArgument("cedar-server", message);
}

bool ReadExact(int fd, void* destination, size_t size) {
  auto* bytes = static_cast<char*>(destination);
  size_t offset = 0;
  while (offset < size) {
    const ssize_t count = ::recv(fd, bytes + offset, size - offset, 0);
    if (count <= 0) return false;
    offset += static_cast<size_t>(count);
  }
  return true;
}

bool WriteAll(int fd, const std::string& bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t count = ::send(fd, bytes.data() + offset, bytes.size() - offset, 0);
    if (count <= 0) return false;
    offset += static_cast<size_t>(count);
  }
  return true;
}

bool WriteBoltFrame(int fd, const std::string& frame) {
  std::string terminated = frame;
  terminated.append("\x00\x00", 2);
  return WriteAll(fd, terminated);
}

StatusOr<std::string> ReadBoltMessage(int fd, uint32_t max_frame_bytes) {
  std::string payload;
  while (true) {
    unsigned char header[2]{};
    if (!ReadExact(fd, header, sizeof(header))) {
      return Status::NotFound("bolt", "connection closed");
    }
    const uint32_t length = (static_cast<uint32_t>(header[0]) << 8) | header[1];
    if (length == 0) break;
    if (length > max_frame_bytes || payload.size() > max_frame_bytes - length) {
      return Status::ResourceExhausted("bolt", "message exceeds configured limit");
    }
    const size_t old_size = payload.size();
    payload.resize(old_size + length);
    if (!ReadExact(fd, payload.data() + old_size, length)) {
      return Status::ParseError("bolt", "truncated message payload");
    }
  }
  if (payload.empty()) return Status::ParseError("bolt", "empty Bolt message");
  return payload;
}

StatusOr<std::string> PackString(const std::string& value) {
  if (value.size() > 15) return Status::ResourceExhausted("bolt", "string exceeds tiny value limit");
  std::string encoded(1, static_cast<char>(0x80U + value.size()));
  encoded += value;
  return encoded;
}

StatusOr<std::pair<std::string, size_t>> ReadPackString(
    const std::string& payload, size_t offset, uint32_t max_bytes) {
  if (offset >= payload.size()) return Status::ParseError("bolt", "missing string");
  const uint8_t marker = static_cast<uint8_t>(payload[offset++]);
  uint32_t length = 0;
  if (marker >= 0x80 && marker <= 0x8F) {
    length = marker - 0x80;
  } else if (marker == 0xD0 && offset < payload.size()) {
    length = static_cast<uint8_t>(payload[offset++]);
  } else if (marker == 0xD1 && offset + 2 <= payload.size()) {
    length = (static_cast<uint32_t>(static_cast<uint8_t>(payload[offset])) << 8) |
             static_cast<uint8_t>(payload[offset + 1]);
    offset += 2;
  } else {
    return Status::ParseError("bolt", "unsupported string encoding");
  }
  if (length > max_bytes || offset > payload.size() - length) {
    return Status::ResourceExhausted("bolt", "string exceeds configured limit");
  }
  return std::make_pair(payload.substr(offset, length), offset + length);
}

StatusOr<Value> ReadPackValue(const std::string& payload, size_t* offset,
                              uint32_t max_bytes) {
  if (*offset >= payload.size()) return Status::ParseError("bolt", "missing value");
  const uint8_t marker = static_cast<uint8_t>(payload[(*offset)++]);
  if (marker <= 0x7F) return Value::Int64(marker);
  if (marker >= 0xF0) return Value::Int64(static_cast<int8_t>(marker));
  if (marker == 0xC2) return Value::Bool(false);
  if (marker == 0xC3) return Value::Bool(true);
  if (marker == 0xCC && *offset < payload.size()) {
    return Value::Int64(static_cast<uint8_t>(payload[(*offset)++]));
  }
  if ((marker >= 0x80 && marker <= 0x8F) || marker == 0xD0 || marker == 0xD1) {
    --*offset;
    auto string = ReadPackString(payload, *offset, max_bytes);
    if (!string.ok()) return string.status();
    *offset = string.ValueOrDie().second;
    return Value::String(string.ValueOrDie().first);
  }
  return Status::ParseError("bolt", "unsupported parameter value");
}

StatusOr<std::map<std::string, Value>> ReadPackMap(
    const std::string& payload, size_t* offset, uint32_t max_bytes) {
  if (*offset >= payload.size()) return Status::ParseError("bolt", "missing map");
  const uint8_t marker = static_cast<uint8_t>(payload[(*offset)++]);
  if (marker < 0xA0 || marker > 0xAF) {
    return Status::ParseError("bolt", "expected tiny map");
  }
  const size_t count = marker - 0xA0;
  std::map<std::string, Value> values;
  for (size_t index = 0; index < count; ++index) {
    auto key = ReadPackString(payload, *offset, max_bytes);
    if (!key.ok()) return key.status();
    *offset = key.ValueOrDie().second;
    auto value = ReadPackValue(payload, offset, max_bytes);
    if (!value.ok()) return value.status();
    values.emplace(std::move(key.ValueOrDie().first), std::move(value).ConsumeValueOrDie());
  }
  return values;
}

QueryType QueryTypeForBoltValue(const Value& value) {
  switch (value.type()) {
    case PhysicalType::kBool: return QueryType::kBool;
    case PhysicalType::kInt32: return QueryType::kInt32;
    case PhysicalType::kInt64: return QueryType::kInt64;
    case PhysicalType::kFloat32: return QueryType::kFloat32;
    case PhysicalType::kFloat64: return QueryType::kFloat64;
    case PhysicalType::kTimestamp64: return QueryType::kTimestamp64;
    case PhysicalType::kString: return QueryType::kString;
    case PhysicalType::kBinary: return QueryType::kBinary;
  }
  return QueryType::kBinary;
}

std::string PackInteger(uint64_t value) {
  if (value <= 0x7f) return std::string(1, static_cast<char>(value));
  if (value <= 0xff) return std::string({static_cast<char>(0xCC), static_cast<char>(value)});
  if (value <= 0xffffffffULL) {
    std::string result(5, '\0');
    result[0] = static_cast<char>(0xCE);
    for (int index = 0; index < 4; ++index) {
      result[1 + index] = static_cast<char>((value >> (24 - index * 8)) & 0xff);
    }
    return result;
  }
  std::string result(9, '\0');
  result[0] = static_cast<char>(0xCF);
  for (int index = 0; index < 8; ++index) {
    result[1 + index] = static_cast<char>((value >> (56 - index * 8)) & 0xff);
  }
  return result;
}

std::string PackRecordValue(const QueryColumn& column, size_t row) {
  switch (column.type) {
    case QueryType::kValidTime:
      return PackInteger(std::get<std::vector<ValidTime>>(column.values)[row].value);
    case QueryType::kCommitSeq:
      return PackInteger(std::get<std::vector<CommitSeq>>(column.values)[row].value);
    case QueryType::kVertexRef: {
      const VertexRef value = std::get<std::vector<VertexRef>>(column.values)[row];
      std::string encoded(1, static_cast<char>(0xA2));
      encoded += PackString("part_id").ValueOrDie();
      encoded += PackInteger(value.part_id.value);
      encoded += PackString("id").ValueOrDie();
      encoded += PackInteger(value.vertex_id.value);
      return encoded;
    }
    case QueryType::kEdgeRef: {
      const EdgeRef value = std::get<std::vector<EdgeRef>>(column.values)[row];
      std::string encoded(1, static_cast<char>(0xA2));
      encoded += PackString("part_id").ValueOrDie();
      encoded += PackInteger(value.home_part_id.value);
      encoded += PackString("id").ValueOrDie();
      encoded += PackInteger(value.edge_id.value);
      return encoded;
    }
    case QueryType::kString: {
      const auto& values = std::get<std::vector<std::string>>(column.values);
      return values[row].size() <= 15 ? std::string(1, static_cast<char>(0x80 + values[row].size())) + values[row]
                                      : std::string("\xC0", 1);
    }
    default:
      return PackInteger(0);
  }
}

StatusOr<std::string> EncodeBoltFields(
    const std::vector<std::string>& fields, uint32_t max_frame_bytes) {
  if (fields.size() > 15) return Status::ResourceExhausted("bolt", "too many result fields");
  std::string payload{static_cast<char>(0xB1), static_cast<char>(0x70),
                      static_cast<char>(0xA1)};
  payload += PackString("fields").ValueOrDie();
  payload.push_back(static_cast<char>(0x90 + fields.size()));
  for (const std::string& field : fields) {
    auto encoded = PackString(field);
    if (!encoded.ok()) return encoded.status();
    payload += encoded.ValueOrDie();
  }
  return EncodeBoltChunk(payload, max_frame_bytes);
}

StatusOr<std::string> EncodeBoltRecord(const QueryBatch& batch, size_t row,
                                       uint32_t max_frame_bytes) {
  if (batch.columns().size() > 15) return Status::ResourceExhausted("bolt", "too many result columns");
  std::string payload{static_cast<char>(0xB1), static_cast<char>(0x71),
                      static_cast<char>(0x90 + batch.columns().size())};
  for (const QueryColumn& column : batch.columns()) payload += PackRecordValue(column, row);
  return EncodeBoltChunk(payload, max_frame_bytes);
}

struct BoltSession {
  std::optional<cypher::PreparedCypher> prepared;
  std::optional<QueryCursor> cursor;
  std::unique_ptr<Transaction> transaction;
  bool transaction_dirty = false;
};

StatusOr<std::string> ProcessBoltMessage(
    Database& database, const std::string& payload, BoltSession* session,
    uint32_t max_frame_bytes, std::vector<std::string>* extra_frames) {
  const auto kind = DecodeBoltMessageKind(payload);
  if (!kind.ok()) return kind.status();
  switch (kind.ValueOrDie()) {
    case BoltMessageKind::kHello:
      return EncodeBoltSuccess(max_frame_bytes);
    case BoltMessageKind::kRun: {
      auto query = ReadPackString(payload, 2, max_frame_bytes);
      if (!query.ok()) return query.status();
      size_t value_offset = query.ValueOrDie().second;
      auto parameters = ReadPackMap(payload, &value_offset, max_frame_bytes);
      if (!parameters.ok()) return parameters.status();
      // The third RUN field is request metadata. Decode its bounded map even
      // when no options are currently consumed so malformed input is rejected
      // before touching Cedar.
      auto request_metadata = ReadPackMap(payload, &value_offset, max_frame_bytes);
      if (!request_metadata.ok()) return request_metadata.status();
      auto prepared = cypher::PrepareCypher(database, query.ValueOrDie().first,
                                            cypher::SchemaCatalog{});
      if (!prepared.ok()) return prepared.status();
      session->prepared = std::move(prepared).ConsumeValueOrDie();
      Bindings bindings;
      for (const auto& parameter : session->prepared->bound_statement().parameters) {
        const auto value = parameters.ValueOrDie().find(parameter.name);
        if (value == parameters.ValueOrDie().end()) {
          return Status::BindError("bolt", "named parameter is missing");
        }
        const Status bound = bindings.Bind(parameter.id,
                                           QueryTypeForBoltValue(value->second),
                                           value->second);
        if (!bound.ok()) return bound;
      }
      if (session->prepared->bound_statement().kind == cypher::StatementKind::kWrite) {
        ValidTime valid_time{0};
        const auto& scope = session->prepared->bound_statement().valid_time;
        if (scope.has_value()) {
          if (scope->as_of.has_value()) valid_time = ValidTime{*scope->as_of};
          else if (scope->to.has_value()) {
            return Status::InvalidArgument("bolt", "range writes require one valid-time point");
          } else {
            valid_time = ValidTime{scope->from};
          }
        }
        if (session->transaction) {
          const Status staged = cypher::StageWrite(
              database, *session->transaction,
              session->prepared->bound_statement(), bindings, valid_time);
          if (!staged.ok()) return staged;
          session->transaction_dirty = true;
        } else {
          const auto committed = cypher::ExecuteWrite(
              database, session->prepared->bound_statement(), bindings, valid_time);
          if (!committed.ok()) return committed.status();
        }
        return EncodeBoltFields({}, max_frame_bytes);
      }
      if (session->transaction_dirty) {
        return Status::InvalidArgument("bolt", "commit the open write transaction before reading");
      }
      auto snapshot = database.BeginSnapshot();
      if (!snapshot.ok()) return snapshot.status();
      auto cursor = session->prepared->Execute(
          std::move(snapshot).ConsumeValueOrDie(), bindings);
      if (!cursor.ok()) return cursor.status();
      session->cursor = std::move(cursor).ConsumeValueOrDie();
      std::vector<std::string> fields;
      for (const auto& projection : session->prepared->bound_statement().projections) {
        fields.push_back(projection.expression);
      }
      return EncodeBoltFields(fields, max_frame_bytes);
    }
    case BoltMessageKind::kPull: {
      if (!session->cursor.has_value()) return EncodeBoltSuccess(max_frame_bytes);
      auto batch = session->cursor->Next();
      if (!batch.ok()) return batch.status();
      if (batch.ValueOrDie().has_value()) {
        const QueryBatch& value = *batch.ValueOrDie();
        for (size_t row = 0; row < value.row_count(); ++row) {
          auto record = EncodeBoltRecord(value, row, max_frame_bytes);
          if (!record.ok()) return record.status();
          extra_frames->push_back(std::move(record).ConsumeValueOrDie());
        }
        return EncodeBoltSuccess(max_frame_bytes);
      }
      session->cursor.reset();
      return EncodeBoltSuccess(max_frame_bytes);
    }
    case BoltMessageKind::kBegin: {
      if (session->transaction) {
        return Status::Conflict("bolt", "transaction is already open");
      }
      auto transaction = database.BeginTransaction();
      if (!transaction.ok()) return transaction.status();
      session->transaction = std::move(transaction).ConsumeValueOrDie();
      session->transaction_dirty = false;
      return EncodeBoltSuccess(max_frame_bytes);
    }
    case BoltMessageKind::kCommit: {
      if (!session->transaction) return EncodeBoltSuccess(max_frame_bytes);
      const auto committed = session->transaction->Commit();
      if (!committed.ok()) return committed.status();
      if (committed.ValueOrDie().outcome != CommitOutcome::kCommitted) {
        return committed.ValueOrDie().status;
      }
      session->transaction.reset();
      session->transaction_dirty = false;
      session->cursor.reset();
      return EncodeBoltSuccess(max_frame_bytes);
    }
    case BoltMessageKind::kRollback:
      if (session->transaction) {
        const Status rolled_back = session->transaction->Rollback();
        if (!rolled_back.ok()) return rolled_back;
      }
      session->transaction.reset();
      session->transaction_dirty = false;
      session->cursor.reset();
      return EncodeBoltSuccess(max_frame_bytes);
    case BoltMessageKind::kReset:
      if (session->transaction) session->transaction->Rollback().IgnoreError();
      session->transaction.reset();
      session->transaction_dirty = false;
      session->cursor.reset();
      session->prepared.reset();
      return EncodeBoltSuccess(max_frame_bytes);
    case BoltMessageKind::kGoodbye:
      return EncodeBoltIgnored(max_frame_bytes);
  }
  return Status::ParseError("bolt", "unreachable message kind");
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
  const int active_client = active_client_fd_.load(std::memory_order_acquire);
  if (active_client >= 0) ::shutdown(active_client, SHUT_RDWR);
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
    active_client_fd_.store(fd, std::memory_order_release);
    HandleClient(fd);
    active_client_fd_.store(-1, std::memory_order_release);
    ::close(fd);
  }
}

void Server::HandleClient(int fd) {
  unsigned char probe[4]{};
  const ssize_t probe_count = ::recv(fd, probe, sizeof(probe), MSG_PEEK | MSG_WAITALL);
  if (probe_count >= 4 && std::memcmp(probe, "\x60\x60\xB0\x17", 4) == 0) {
    std::string handshake(20, '\0');
    if (!ReadExact(fd, handshake.data(), handshake.size())) return;
    const auto negotiated = NegotiateBoltHandshake(handshake);
    if (!negotiated.ok() || !WriteAll(fd, negotiated.ValueOrDie())) return;
    BoltSession session;
    while (!stopping_.load(std::memory_order_acquire)) {
      auto payload = ReadBoltMessage(fd, config_.max_frame_bytes);
      if (!payload.ok()) break;
      std::vector<std::string> extra_frames;
      auto response = ProcessBoltMessage(*database_, payload.ValueOrDie(), &session,
                                         config_.max_frame_bytes, &extra_frames);
      if (!response.ok()) break;
      for (const std::string& frame : extra_frames) {
        if (!WriteBoltFrame(fd, frame)) return;
      }
      if (!WriteBoltFrame(fd, response.ValueOrDie())) return;
      if (payload.ValueOrDie().size() >= 2 &&
          static_cast<uint8_t>(payload.ValueOrDie()[1]) == 0x02) break;
    }
    return;
  }
  if (probe_count >= 2 && probe[0] == 0 && probe[1] != 0 && database_) {
    BoltSession session;
    while (!stopping_.load(std::memory_order_acquire)) {
      auto payload = ReadBoltMessage(fd, config_.max_frame_bytes);
      if (!payload.ok()) break;
      std::vector<std::string> extra_frames;
      auto response = ProcessBoltMessage(*database_, payload.ValueOrDie(), &session,
                                         config_.max_frame_bytes, &extra_frames);
      if (!response.ok()) break;
      for (const std::string& frame : extra_frames) {
        if (!WriteBoltFrame(fd, frame)) return;
      }
      if (!WriteBoltFrame(fd, response.ValueOrDie())) return;
      if (payload.ValueOrDie().size() >= 2 &&
          static_cast<uint8_t>(payload.ValueOrDie()[1]) == 0x02) break;
    }
    return;
  }
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
  } else if (database_) {
    // Handle one bounded Bolt request per connection. PackStream values stay
    // opaque here; Cedar Cypher remains the only query compiler.
    const auto payload = DecodeBoltChunk(request, config_.max_frame_bytes);
    if (!payload.ok()) return;
    const auto kind = DecodeBoltMessageKind(payload.ValueOrDie());
    if (!kind.ok()) return;
    const auto encoded = kind.ValueOrDie() == BoltMessageKind::kGoodbye
                             ? EncodeBoltIgnored(config_.max_frame_bytes)
                             : EncodeBoltSuccess(config_.max_frame_bytes);
    if (!encoded.ok()) return;
    response = encoded.ValueOrDie();
  } else {
    response = "400 BAD_REQUEST\n";
  }
  ::send(fd, response.data(), response.size(), 0);
}

}  // namespace cedar::server
