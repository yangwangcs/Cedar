#include "cedar/cypher.h"

#include <algorithm>

#include "cedar/cypher/parser.h"

namespace cedar::cypher {

StatusOr<PreparedCypher> PrepareCypher(Database& database,
                                       const std::string& source,
                                       const SchemaCatalog& catalog,
                                       BinderOptions options) {
  const auto parsed = Parse(source);
  if (!parsed.ok()) return parsed.status();
  const auto bound = Bind(parsed.ValueOrDie(), catalog, options);
  if (!bound.ok()) return bound.status();
  const auto query = Compile(bound.ValueOrDie());
  if (!query.ok()) return query.status();
  auto prepared = database.PrepareQuery(query.ValueOrDie());
  if (!prepared.ok()) return prepared.status();
  return PreparedCypher(std::move(prepared).ConsumeValueOrDie(),
                        bound.ValueOrDie(), &database);
}

StatusOr<QueryCursor> PreparedCypher::Execute(
    Snapshot snapshot, const Bindings& bindings,
    const QueryOptions& options) const {
  if (bound_.system_time.has_value()) {
    if (!bound_.system_time->as_of.has_value() || database_ == nullptr) {
      return Status::NotSupported("cypher", "system-time range requires a bounded snapshot cutoff");
    }
    const uint64_t caller_seq = snapshot.commit_seq().value;
    const uint64_t requested_seq = *bound_.system_time->as_of;
    const uint64_t ceiling = caller_seq == 0 ? requested_seq
                                             : std::min(caller_seq, requested_seq);
    auto bounded = database_->BeginSnapshot(SnapshotOptions{CommitSeq{ceiling}});
    if (!bounded.ok()) return bounded.status();
    snapshot = std::move(bounded).ConsumeValueOrDie();
  }
  return prepared_.Execute(std::move(snapshot), bindings, options);
}

}  // namespace cedar::cypher
