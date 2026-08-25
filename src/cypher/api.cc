#include "cedar/cypher.h"

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
                        bound.ValueOrDie());
}

StatusOr<QueryCursor> PreparedCypher::Execute(
    Snapshot snapshot, const Bindings& bindings,
    const QueryOptions& options) const {
  return prepared_.Execute(std::move(snapshot), bindings, options);
}

StatusOr<QueryCursor> PreparedCypher::Execute(
    Transaction& transaction, const Bindings& bindings,
    const QueryOptions& options) const {
  return prepared_.Execute(transaction, bindings, options);
}

}  // namespace cedar::cypher
