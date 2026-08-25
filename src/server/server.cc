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
#include <atomic>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <optional>
#include <sstream>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include "cedar/cypher.h"
#include "cedar/cypher/schema_manifest.h"
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
  bool authenticated = false;
  std::optional<QueryBatch> pending_batch;
  size_t pending_row = 0;
};

size_t PullLimit(const std::string& payload, uint32_t configured) {
  if (payload.size() < 3 || static_cast<uint8_t>(payload[2]) == 0xA0) {
    return configured;
  }
  if (static_cast<uint8_t>(payload[2]) != 0xA1 || payload.size() < 6 ||
      static_cast<uint8_t>(payload[3]) != 0x81 ||
      static_cast<uint8_t>(payload[4]) != 0x6E) return 0;
  const uint8_t value = static_cast<uint8_t>(payload[5]);
  if (value <= 0x7F) return value;
  if (value == 0xCC && payload.size() >= 7) return static_cast<uint8_t>(payload[6]);
  return 0;
}

StatusOr<std::string> ProcessBoltMessage(
    Database& database, cypher::CypherSession& cypher_session,
    const std::string& payload, BoltSession* session,
    uint32_t max_frame_bytes, uint32_t max_pull_records,
    std::vector<std::string>* extra_frames,
    std::string_view expected_auth_token) {
  const auto kind = DecodeBoltMessageKind(payload);
  if (!kind.ok()) return kind.status();
  if (kind.ValueOrDie() != BoltMessageKind::kHello &&
      !expected_auth_token.empty() && !session->authenticated) {
    return Status::InvalidArgument("bolt", "authentication required before request");
  }
  switch (kind.ValueOrDie()) {
    case BoltMessageKind::kHello: {
      const Status authenticated = AuthenticateBoltHello(payload, expected_auth_token);
      if (!authenticated.ok()) return authenticated;
      session->authenticated = true;
      return EncodeBoltSuccess(max_frame_bytes);
    }
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
      auto prepared = cypher_session.Prepare(query.ValueOrDie().first);
      if (!prepared.ok()) return prepared.status();
      session->prepared = std::move(prepared).ConsumeValueOrDie();
      session->pending_batch.reset();
      session->pending_row = 0;
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
          const auto committed = cypher_session.ExecuteWrite(
              *session->prepared, cypher::CypherRequest{.bindings = bindings,
                                                        .valid_time = valid_time});
          if (!committed.ok()) return committed.status();
        }
        return EncodeBoltFields({}, max_frame_bytes);
      }
      StatusOr<QueryCursor> cursor = session->transaction
          ? cypher_session.Execute(
                *session->prepared, *session->transaction,
                cypher::CypherRequest{.bindings = bindings})
          : cypher_session.Execute(
                *session->prepared,
                cypher::CypherRequest{.bindings = bindings});
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
      const size_t limit = PullLimit(payload, max_pull_records);
      if (limit == 0 || limit > max_pull_records) {
        return Status::InvalidArgument("bolt", "invalid PULL fetch size");
      }
      size_t emitted = 0;
      while (emitted < limit) {
        if (!session->pending_batch.has_value()) {
          auto batch = session->cursor->Next();
          if (!batch.ok()) return batch.status();
          if (!batch.ValueOrDie().has_value()) {
            session->cursor.reset();
            session->pending_row = 0;
            break;
          }
          session->pending_batch = std::move(batch).ConsumeValueOrDie();
          session->pending_row = 0;
        }
        const QueryBatch& value = *session->pending_batch;
        auto record = EncodeBoltRecord(value, session->pending_row, max_frame_bytes);
        if (!record.ok()) return record.status();
        extra_frames->push_back(std::move(record).ConsumeValueOrDie());
        ++emitted;
        ++session->pending_row;
        if (session->pending_row == value.row_count()) {
          session->pending_batch.reset();
          session->pending_row = 0;
        }
      }
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
      session->pending_batch.reset();
      session->pending_row = 0;
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
      session->pending_batch.reset();
      session->pending_row = 0;
      return EncodeBoltSuccess(max_frame_bytes);
    case BoltMessageKind::kReset:
      if (session->transaction) session->transaction->Rollback().IgnoreError();
      session->transaction.reset();
      session->transaction_dirty = false;
      session->cursor.reset();
      session->prepared.reset();
      session->pending_batch.reset();
      session->pending_row = 0;
      return EncodeBoltSuccess(max_frame_bytes);
    case BoltMessageKind::kGoodbye:
      return EncodeBoltIgnored(max_frame_bytes);
  }
  return Status::ParseError("bolt", "unreachable message kind");
}

}  // namespace

Status ServerConfig::Validate() const {
  if (database_path.empty() || bind_address.empty() ||
      max_frame_bytes == 0 || max_frame_bytes > 64U * 1024U * 1024U ||
      max_pull_records == 0 || max_pull_records > 4096 || worker_threads == 0 ||
      max_sessions == 0 || auth_token.size() > 256) {
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
    } else if (arg == "--schema") {
      auto parsed = value("--schema");
      if (!parsed.ok()) return parsed.status();
      config.schema_path = parsed.ValueOrDie();
    } else if (arg == "--auth-token") {
      auto parsed = value("--auth-token");
      if (!parsed.ok()) return parsed.status();
      config.auth_token = parsed.ValueOrDie();
    } else if (arg == "--graph") {
      auto parsed = value("--graph");
      if (!parsed.ok()) return parsed.status();
      config.graph = parsed.ValueOrDie();
    } else if (arg == "--part-id") {
      auto parsed = value("--part-id");
      if (!parsed.ok()) return parsed.status();
      const unsigned long part = std::stoul(parsed.ValueOrDie());
      if (part > UINT32_MAX) return ServerError("PartID exceeds uint32");
      config.part_id = PartId{static_cast<uint32_t>(part)};
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

class Server::State {
 public:
  explicit State(ServerConfig value) : config(std::move(value)) {}

  ServerConfig config;
  mutable std::mutex mutex;
  std::unique_ptr<Database> database;
  std::unique_ptr<cypher::CypherSession> cypher_session;
  int listen_fd = -1;
  int lock_fd = -1;
  uint16_t bound_port = 0;
  std::thread accept_thread;
  std::vector<std::thread> workers;
  std::mutex queue_mutex;
  std::condition_variable queue_cv;
  std::deque<int> client_queue;
  std::atomic<bool> live{false};
  std::atomic<bool> ready{false};
  std::atomic<bool> stopping{false};
  std::atomic<int> active_client_fd{-1};
};

Server::Server(ServerConfig config)
    : state_(std::make_unique<State>(std::move(config))) {}

Server::~Server() { Stop().IgnoreError(); }

Status Server::AcquireLock() {
  state_->lock_fd = ::open(state_->config.lock_path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
  if (state_->lock_fd < 0) return Status::Conflict("cedar-server", "database lock is held");
  const std::string pid = std::to_string(static_cast<long long>(::getpid())) + "\n";
  if (::write(state_->lock_fd, pid.data(), pid.size()) != static_cast<ssize_t>(pid.size())) {
    ReleaseLock();
    return Status::IOError("cedar-server", "cannot write lock file");
  }
  if (!state_->config.pid_path.empty()) {
    const int pid_fd = ::open(state_->config.pid_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (pid_fd >= 0) {
      ::write(pid_fd, pid.data(), pid.size());
      ::close(pid_fd);
    }
  }
  return Status::OK();
}

void Server::ReleaseLock() {
  if (state_->lock_fd >= 0) {
    ::close(state_->lock_fd);
    state_->lock_fd = -1;
  }
  std::error_code error;
  std::filesystem::remove(state_->config.lock_path, error);
  if (!state_->config.pid_path.empty()) std::filesystem::remove(state_->config.pid_path, error);
}

Status Server::Start() {
  if (!state_) return Status::InvalidArgument("cedar-server", "moved-from server");
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->live.load(std::memory_order_acquire)) return Status::OK();
  const Status valid = state_->config.Validate();
  if (!valid.ok()) return valid;
  const Status locked = AcquireLock();
  if (!locked.ok()) return locked;
  auto database = Database::Open(DatabaseOptions{.path = state_->config.database_path});
  if (!database.ok()) {
    ReleaseLock();
    return database.status();
  }
  state_->database = std::move(database).ConsumeValueOrDie();
  cypher::SchemaCatalog catalog;
  cypher::BinderOptions binder_options;
  binder_options.graph = state_->config.graph;
  binder_options.part_id = state_->config.part_id;
  if (!state_->config.schema_path.empty()) {
    const auto manifest = cypher::LoadSchemaManifest(state_->config.schema_path);
    if (!manifest.ok()) {
      state_->database->Close().IgnoreError();
      state_->database.reset();
      ReleaseLock();
      return manifest.status();
    }
    catalog = manifest.ValueOrDie().catalog;
    if (state_->config.graph.empty()) binder_options.graph = manifest.ValueOrDie().graph;
    if (state_->config.part_id.value == 0) binder_options.part_id = manifest.ValueOrDie().part_id;
  }
  state_->cypher_session = std::make_unique<cypher::CypherSession>(
      *state_->database, std::move(catalog), std::move(binder_options));
  state_->listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (state_->listen_fd < 0) {
    state_->database->Close().IgnoreError();
    state_->cypher_session.reset();
    state_->database.reset();
    ReleaseLock();
    return Status::IOError("cedar-server", "cannot create listener");
  }
  int reuse = 1;
  ::setsockopt(state_->listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(state_->config.port);
  if (::inet_pton(AF_INET, state_->config.bind_address.c_str(), &address.sin_addr) != 1 ||
      ::bind(state_->listen_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
      ::listen(state_->listen_fd, 32) != 0) {
    ::close(state_->listen_fd);
    state_->listen_fd = -1;
    state_->database->Close().IgnoreError();
    state_->cypher_session.reset();
    state_->database.reset();
    ReleaseLock();
    return Status::IOError("cedar-server", "cannot bind listener");
  }
  socklen_t length = sizeof(address);
  ::getsockname(state_->listen_fd, reinterpret_cast<sockaddr*>(&address), &length);
  state_->bound_port = ntohs(address.sin_port);
  state_->live.store(true, std::memory_order_release);
  state_->ready.store(true, std::memory_order_release);
  state_->stopping.store(false, std::memory_order_release);
  state_->accept_thread = std::thread(&Server::AcceptLoop, this);
  state_->workers.reserve(state_->config.worker_threads);
  for (uint32_t i = 0; i < state_->config.worker_threads; ++i) {
    state_->workers.emplace_back(&Server::WorkerLoop, this);
  }
  return Status::OK();
}

Status Server::Stop() {
  if (!state_) return Status::OK();
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (!state_->live.load(std::memory_order_acquire) && state_->database == nullptr) return Status::OK();
  state_->stopping.store(true, std::memory_order_release);
  state_->ready.store(false, std::memory_order_release);
  if (state_->listen_fd >= 0) {
    ::shutdown(state_->listen_fd, SHUT_RDWR);
    ::close(state_->listen_fd);
    state_->listen_fd = -1;
  }
  const int active_client = state_->active_client_fd.load(std::memory_order_acquire);
  if (active_client >= 0) ::shutdown(active_client, SHUT_RDWR);
  if (state_->accept_thread.joinable()) state_->accept_thread.join();
  {
    std::lock_guard<std::mutex> queue_lock(state_->queue_mutex);
    for (const int fd : state_->client_queue) ::close(fd);
    state_->client_queue.clear();
  }
  state_->queue_cv.notify_all();
  for (auto& worker : state_->workers) {
    if (worker.joinable()) worker.join();
  }
  state_->workers.clear();
  state_->live.store(false, std::memory_order_release);
  if (state_->database) {
    const Status closed = state_->database->Close();
    state_->cypher_session.reset();
    state_->database.reset();
    ReleaseLock();
    return closed;
  }
  ReleaseLock();
  return Status::OK();
}

bool Server::Live() const {
  return state_ && state_->live.load(std::memory_order_acquire);
}

bool Server::Ready() const {
  return state_ && state_->ready.load(std::memory_order_acquire);
}

uint16_t Server::port() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->bound_port;
}

std::string Server::Metrics() const {
  std::ostringstream out;
  out << "cedar_server_live " << (Live() ? 1 : 0) << '\n'
      << "cedar_server_ready " << (Ready() ? 1 : 0) << '\n';
  if (state_->database) {
    const auto query = state_->database->SampleQueryMetrics();
    const auto commit = state_->database->GetCommitPipelineMetrics();
    const auto runtime = state_->database->SampleRuntimeMetrics();
    out << "cedar_query_physical_bytes " << query.physical_bytes << '\n'
        << "cedar_query_decoded_bytes " << query.decoded_bytes << '\n'
        << "cedar_query_interval_fragments " << query.interval_fragments << '\n'
        << "cedar_query_spill_bytes " << query.spill_bytes << '\n'
        << "cedar_commit_submitted " << commit.submitted << '\n'
        << "cedar_commit_published " << commit.published << '\n'
        << "cedar_commit_wal_sync_total_us " << commit.latency.wal_sync.total_us << '\n';
    if (runtime.ok()) {
      out << "cedar_runtime_active_fact_bytes " << runtime.ValueOrDie().active_fact_bytes << '\n'
          << "cedar_runtime_immutable_fact_bytes " << runtime.ValueOrDie().immutable_fact_bytes << '\n'
          << "cedar_runtime_l0_file_count " << runtime.ValueOrDie().l0_file_count << '\n'
          << "cedar_runtime_pending_compaction_bytes " << runtime.ValueOrDie().pending_compaction_bytes << '\n';
    }
  }
  return out.str();
}

void Server::AcceptLoop() {
  while (!state_->stopping.load(std::memory_order_acquire)) {
    const int fd = ::accept(state_->listen_fd, nullptr, nullptr);
    if (fd < 0) {
      if (state_->stopping.load(std::memory_order_acquire)) break;
      continue;
    }
    {
      std::lock_guard<std::mutex> lock(state_->queue_mutex);
      if (state_->client_queue.size() >= state_->config.max_sessions) {
        ::close(fd);
        continue;
      }
      state_->client_queue.push_back(fd);
    }
    state_->queue_cv.notify_one();
  }
}

void Server::WorkerLoop() {
  while (true) {
    int fd = -1;
    {
      std::unique_lock<std::mutex> lock(state_->queue_mutex);
      state_->queue_cv.wait(lock, [this] {
        return state_->stopping.load(std::memory_order_acquire) || !state_->client_queue.empty();
      });
      if (state_->client_queue.empty()) {
        if (state_->stopping.load(std::memory_order_acquire)) return;
        continue;
      }
      fd = state_->client_queue.front();
      state_->client_queue.pop_front();
    }
    state_->active_client_fd.store(fd, std::memory_order_release);
    HandleClient(fd);
    state_->active_client_fd.store(-1, std::memory_order_release);
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
    while (!state_->stopping.load(std::memory_order_acquire)) {
      auto payload = ReadBoltMessage(fd, state_->config.max_frame_bytes);
      if (!payload.ok()) break;
      std::vector<std::string> extra_frames;
      auto response = ProcessBoltMessage(*state_->database, *state_->cypher_session, payload.ValueOrDie(), &session,
                                         state_->config.max_frame_bytes, state_->config.max_pull_records,
                                         &extra_frames, state_->config.auth_token);
      if (!response.ok()) {
        const auto failure = EncodeBoltFailure(response.status(), state_->config.max_frame_bytes);
        if (!failure.ok() || !WriteBoltFrame(fd, failure.ValueOrDie())) return;
        const auto failed_kind = DecodeBoltMessageKind(payload.ValueOrDie());
        if (failed_kind.ok() && failed_kind.ValueOrDie() == BoltMessageKind::kGoodbye) break;
        continue;
      }
      for (const std::string& frame : extra_frames) {
        if (!WriteBoltFrame(fd, frame)) return;
      }
      if (!WriteBoltFrame(fd, response.ValueOrDie())) return;
      if (payload.ValueOrDie().size() >= 2 &&
          static_cast<uint8_t>(payload.ValueOrDie()[1]) == 0x02) break;
    }
    return;
  }
  if (probe_count >= 2 && probe[0] == 0 && probe[1] != 0 && state_->database) {
    BoltSession session;
    while (!state_->stopping.load(std::memory_order_acquire)) {
      auto payload = ReadBoltMessage(fd, state_->config.max_frame_bytes);
      if (!payload.ok()) break;
      std::vector<std::string> extra_frames;
      auto response = ProcessBoltMessage(*state_->database, *state_->cypher_session, payload.ValueOrDie(), &session,
                                         state_->config.max_frame_bytes, state_->config.max_pull_records,
                                         &extra_frames, state_->config.auth_token);
      if (!response.ok()) {
        const auto failure = EncodeBoltFailure(response.status(), state_->config.max_frame_bytes);
        if (!failure.ok() || !WriteBoltFrame(fd, failure.ValueOrDie())) return;
        const auto failed_kind = DecodeBoltMessageKind(payload.ValueOrDie());
        if (failed_kind.ok() && failed_kind.ValueOrDie() == BoltMessageKind::kGoodbye) break;
        continue;
      }
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
  request.resize(state_->config.max_frame_bytes);
  const ssize_t count = ::recv(fd, request.data(), request.size(), 0);
  if (count <= 0 || static_cast<size_t>(count) > state_->config.max_frame_bytes) return;
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
  } else if (request.rfind("RUN ", 0) == 0 && state_->database) {
    const std::string source = request.substr(4);
    auto prepared = state_->cypher_session->Prepare(source);
    response = prepared.ok() ? "200 OK " + std::to_string(prepared.ValueOrDie().fingerprint()) + "\n"
                             : "400 " + prepared.status().ToString() + "\n";
  } else if (state_->database) {
    // Handle one bounded Bolt request per connection. PackStream values stay
    // opaque here; Cedar Cypher remains the only query compiler.
    const auto payload = DecodeBoltChunk(request, state_->config.max_frame_bytes);
    if (!payload.ok()) return;
    const auto kind = DecodeBoltMessageKind(payload.ValueOrDie());
    if (!kind.ok()) return;
    const auto encoded = kind.ValueOrDie() == BoltMessageKind::kGoodbye
                             ? EncodeBoltIgnored(state_->config.max_frame_bytes)
                             : EncodeBoltSuccess(state_->config.max_frame_bytes);
    if (!encoded.ok()) return;
    response = encoded.ValueOrDie();
  } else {
    response = "400 BAD_REQUEST\n";
  }
  ::send(fd, response.data(), response.size(), 0);
}

}  // namespace cedar::server
