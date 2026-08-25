#ifndef CEDAR_CYPHER_SESSION_H_
#define CEDAR_CYPHER_SESSION_H_

#include <memory>
#include <optional>
#include <string>

#include "cedar/cypher.h"
#include "cedar/cypher/access_profile.h"
#include "cedar/transaction.h"

namespace cedar::cypher {

struct CypherRequest {
  Bindings bindings;
  QueryOptions options;
  std::optional<CommitSeq> system_time_as_of;
  std::optional<CommitSeq> system_time_from;
  std::optional<CommitSeq> system_time_to;
  std::optional<ValidTime> valid_time;
  std::optional<PartId> part_id;
  std::optional<std::string> graph;
};

struct CypherExplain {
  uint64_t fingerprint = 0;
  std::string logical;
  std::string physical;
  std::string source;
  std::optional<uint64_t> projection_generation;
  std::optional<CommitSeq> projection_base;
  AccessProfile profile = AccessProfile::kPointState;
  QueryExecutionMode mode = QueryExecutionMode::kInteractive;
  std::optional<CommitSeqRange> system_time_range;
  bool system_time_clamped = false;
  bool transaction_overlay = false;
};

class CypherSession {
 public:
  explicit CypherSession(Database& database, SchemaCatalog catalog,
                          BinderOptions defaults = {});
  ~CypherSession();
  CypherSession(const CypherSession&) = delete;
  CypherSession& operator=(const CypherSession&) = delete;
  CypherSession(CypherSession&&) noexcept;
  CypherSession& operator=(CypherSession&&) noexcept;

  StatusOr<PreparedCypher> Prepare(const std::string& source);
  StatusOr<QueryCursor> Execute(const PreparedCypher& prepared,
                                const CypherRequest& request = {});
  StatusOr<QueryCursor> Execute(const PreparedCypher& prepared,
                                Transaction& transaction,
                                const CypherRequest& request = {});
  StatusOr<CommitResult> ExecuteWrite(const PreparedCypher& prepared,
                                      const CypherRequest& request = {});
  StatusOr<CypherExplain> Explain(const PreparedCypher& prepared,
                                  const CypherRequest& request = {});

 private:
  class State;
  Status ValidateRequest(const BoundStatement& statement,
                         const CypherRequest& request) const;
  StatusOr<Snapshot> SnapshotFor(const BoundStatement& statement,
                                 const CypherRequest& request) const;
  void Touch(uint64_t fingerprint);
  std::unique_ptr<State> state_;
};

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_SESSION_H_
