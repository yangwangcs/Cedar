#include "cedar/cypher/session.h"

#include <algorithm>
#include <list>
#include <unordered_map>
#include "cedar/cypher/parser.h"
#include "cedar/cypher/write.h"

namespace cedar::cypher {

class CypherSession::State {
 public:
  State(Database& database, SchemaCatalog catalog, BinderOptions defaults)
      : database(&database), catalog(std::move(catalog)),
        defaults(std::move(defaults)) {}

  Database* database;
  SchemaCatalog catalog;
  BinderOptions defaults;
  std::unordered_map<uint64_t, PreparedCypher> cache;
  std::list<uint64_t> lru;
  std::unordered_map<uint64_t, std::list<uint64_t>::iterator> lru_positions;
  static constexpr size_t kMaxPrepared = 64;
};

CypherSession::CypherSession(Database& database, SchemaCatalog catalog,
                             BinderOptions defaults)
    : state_(std::make_unique<State>(database, std::move(catalog),
                                     std::move(defaults))) {}

CypherSession::~CypherSession() = default;
CypherSession::CypherSession(CypherSession&&) noexcept = default;
CypherSession& CypherSession::operator=(CypherSession&&) noexcept = default;

void CypherSession::Touch(uint64_t fingerprint) {
  const auto position = state_->lru_positions.find(fingerprint);
  if (position != state_->lru_positions.end()) {
    state_->lru.erase(position->second);
    state_->lru_positions.erase(position);
  }
  state_->lru.push_front(fingerprint);
  state_->lru_positions[fingerprint] = state_->lru.begin();
  while (state_->lru.size() > State::kMaxPrepared) {
    const uint64_t evicted = state_->lru.back();
    state_->cache.erase(evicted);
    state_->lru_positions.erase(evicted);
    state_->lru.pop_back();
  }
}

StatusOr<PreparedCypher> CypherSession::Prepare(const std::string& source) {
  if (!state_ || !state_->database) {
    return Status::ShutdownInProgress("cypher session", "database is unavailable");
  }
  const auto parsed = Parse(source);
  if (!parsed.ok()) return parsed.status();
  const auto bound = Bind(parsed.ValueOrDie(), state_->catalog, state_->defaults);
  if (!bound.ok()) return bound.status();
  auto found = state_->cache.find(bound.ValueOrDie().fingerprint);
  if (found != state_->cache.end()) {
    Touch(found->first);
    return found->second;
  }
  const auto query = Compile(bound.ValueOrDie());
  if (!query.ok()) return query.status();
  auto prepared = state_->database->PrepareQuery(query.ValueOrDie());
  if (!prepared.ok()) return prepared.status();
  PreparedCypher result(std::move(prepared).ConsumeValueOrDie(),
                        bound.ValueOrDie());
  state_->cache.emplace(result.fingerprint(), result);
  Touch(result.fingerprint());
  return result;
}

Status CypherSession::ValidateRequest(const BoundStatement& statement,
                                      const CypherRequest& request) const {
  if (request.graph.has_value() && request.graph != statement.graph) {
    return Status::BindError("cypher session", "request graph differs from prepared graph");
  }
  if (request.part_id.has_value() && request.part_id != statement.part_id) {
    return Status::BindError("cypher session", "request PartID differs from prepared PartID");
  }
  if (request.system_time_as_of.has_value() &&
      (!statement.system_time.has_value() || !statement.system_time->as_of.has_value() ||
       request.system_time_as_of->value != *statement.system_time->as_of)) {
    return Status::BindError("cypher session", "request system-time differs from prepared scope");
  }
  if (request.system_time_from.has_value() || request.system_time_to.has_value()) {
    if (!statement.system_time.has_value() || statement.system_time->as_of.has_value() ||
        !statement.system_time->to.has_value() ||
        !request.system_time_from.has_value() || !request.system_time_to.has_value() ||
        request.system_time_from->value != statement.system_time->from ||
        request.system_time_to->value != *statement.system_time->to) {
      return Status::BindError("cypher session", "request system-time range differs from prepared scope");
    }
  }
  return Status::OK();
}

StatusOr<Snapshot> CypherSession::SnapshotFor(
    const BoundStatement& statement, const CypherRequest& request) const {
  if (!state_ || !state_->database) return Status::ShutdownInProgress("cypher session", "database is unavailable");
  auto current = state_->database->BeginSnapshot();
  if (!current.ok()) return current.status();
  if (!statement.system_time.has_value()) return current;
  const uint64_t caller_seq = current.ValueOrDie().commit_seq().value;
  if (caller_seq == 0) return current;
  if (statement.system_time->as_of.has_value()) {
    const uint64_t requested_seq = *statement.system_time->as_of;
    const uint64_t ceiling = std::min(caller_seq, requested_seq);
    return state_->database->BeginSnapshot(SnapshotOptions{CommitSeq{ceiling}});
  }
  if (!statement.system_time->to.has_value()) {
    return Status::InvalidArgument("cypher session", "system-time scope is incomplete");
  }
  const uint64_t ceiling = std::min(caller_seq, *statement.system_time->to);
  if (statement.system_time->from > ceiling) {
    return Status::InvalidArgument("cypher session", "system-time range starts after snapshot");
  }
  return state_->database->BeginSnapshot(SnapshotOptions{CommitSeq{ceiling}});
}

StatusOr<QueryCursor> CypherSession::Execute(const PreparedCypher& prepared,
                                             const CypherRequest& request) {
  const Status valid = ValidateRequest(prepared.bound_statement(), request);
  if (!valid.ok()) return valid;
  const auto decision = ChooseAccess(prepared.bound_statement(), request.options);
  if (!decision.ok()) return decision.status();
  auto snapshot = SnapshotFor(prepared.bound_statement(), request);
  if (!snapshot.ok()) return snapshot.status();
  return prepared.Execute(std::move(snapshot).ConsumeValueOrDie(), request.bindings,
                          decision.ValueOrDie().options);
}

StatusOr<QueryCursor> CypherSession::Execute(const PreparedCypher& prepared,
                                             Transaction& transaction,
                                             const CypherRequest& request) {
  const Status valid = ValidateRequest(prepared.bound_statement(), request);
  if (!valid.ok()) return valid;
  const auto decision = ChooseAccess(prepared.bound_statement(), request.options);
  if (!decision.ok()) return decision.status();
  return prepared.Execute(transaction, request.bindings,
                          decision.ValueOrDie().options);
}

StatusOr<CommitResult> CypherSession::ExecuteWrite(
    const PreparedCypher& prepared, const CypherRequest& request) {
  const Status valid = ValidateRequest(prepared.bound_statement(), request);
  if (!valid.ok()) return valid;
  if (prepared.bound_statement().kind != StatementKind::kWrite) {
    return Status::InvalidArgument("cypher session", "statement is not a write");
  }
  ValidTime valid_time{0};
  if (request.valid_time.has_value()) {
    valid_time = *request.valid_time;
  } else if (prepared.bound_statement().valid_time.has_value()) {
    const auto& scope = *prepared.bound_statement().valid_time;
    if (scope.to.has_value()) {
      return Status::InvalidArgument("cypher session", "write valid-time range is not supported");
    }
    valid_time = ValidTime{scope.as_of.value_or(scope.from)};
  }
  return cypher::ExecuteWrite(*state_->database, prepared.bound_statement(),
                              request.bindings, valid_time);
}

StatusOr<CypherExplain> CypherSession::Explain(
    const PreparedCypher& prepared, const CypherRequest& request) {
  const Status valid = ValidateRequest(prepared.bound_statement(), request);
  if (!valid.ok()) return valid;
  const auto decision = ChooseAccess(prepared.bound_statement(), request.options);
  if (!decision.ok()) return decision.status();
  auto snapshot = SnapshotFor(prepared.bound_statement(), request);
  if (!snapshot.ok()) return snapshot.status();
  auto logical = prepared.prepared_.ExplainLogical();
  if (!logical.ok()) return logical.status();
  std::string physical_text;
  QueryPhysicalSummary summary;
  const bool system_range = prepared.bound_statement().system_time.has_value() &&
                            !prepared.bound_statement().system_time->as_of.has_value();
  if (system_range) {
    physical_text = "source=canonical_fallback reason=system_time_range_requires_full_coverage";
  } else if (snapshot.ValueOrDie().commit_seq().value == 0) {
    physical_text = "source=canonical_fallback reason=zero_snapshot_sequence";
  } else {
    auto physical = prepared.prepared_.ExplainPhysical(
        snapshot.ValueOrDie(), decision.ValueOrDie().options);
    if (!physical.ok()) return physical.status();
    physical_text = physical.ValueOrDie();
    auto typed = prepared.prepared_.ExplainPhysicalSummary(
        snapshot.ValueOrDie(), decision.ValueOrDie().options);
    if (!typed.ok()) return typed.status();
    summary = typed.ValueOrDie();
  }
  std::string source = "canonical";
  if (summary.source == QueryAccessSource::kProjection) source = "projection";
  else if (summary.source == QueryAccessSource::kDeltaMerge) source = "delta-merge";
  else if (summary.source == QueryAccessSource::kMixed) source = "mixed";
  std::optional<CommitSeqRange> range;
  bool clamped = false;
  if (system_range) {
    const auto& scope = *prepared.bound_statement().system_time;
    range = CommitSeqRange{CommitSeq{scope.from}, CommitSeq{*scope.to}};
    clamped = snapshot.ValueOrDie().commit_seq().value < scope.to.value();
    range->to = snapshot.ValueOrDie().commit_seq();
  }
  return CypherExplain{prepared.fingerprint(), logical.ValueOrDie(),
                       physical_text, source,
                       summary.projection_generation, summary.projection_base,
                       decision.ValueOrDie().profile,
                       decision.ValueOrDie().options.mode,
                       range, clamped, false};
}

}  // namespace cedar::cypher
