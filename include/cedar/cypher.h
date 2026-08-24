#ifndef CEDAR_CYPHER_H_
#define CEDAR_CYPHER_H_

#include <cstdint>
#include <string>

#include "cedar/cypher/binder.h"
#include "cedar/cypher/compiler.h"
#include "cedar/database.h"

namespace cedar::cypher {

class PreparedCypher {
 public:
  PreparedCypher(PreparedCypher&&) noexcept = default;
  PreparedCypher& operator=(PreparedCypher&&) noexcept = default;
  PreparedCypher(const PreparedCypher&) = delete;
  PreparedCypher& operator=(const PreparedCypher&) = delete;

  StatusOr<QueryCursor> Execute(Snapshot snapshot, const Bindings& bindings,
                                const QueryOptions& options = {}) const;
  uint64_t fingerprint() const { return bound_.fingerprint; }
  // Query text is deliberately not retained by the prepared object.
  std::string source_text() const { return {}; }
  const BoundStatement& bound_statement() const { return bound_; }

 private:
  PreparedCypher(PreparedQuery prepared, BoundStatement bound, Database* database)
      : prepared_(std::move(prepared)), bound_(std::move(bound)), database_(database) {}

  PreparedQuery prepared_;
  BoundStatement bound_;
  Database* database_ = nullptr;
  friend StatusOr<PreparedCypher> PrepareCypher(
      Database&, const std::string&, const SchemaCatalog&, BinderOptions);
};

StatusOr<PreparedCypher> PrepareCypher(
    Database& database, const std::string& source, const SchemaCatalog& catalog,
    BinderOptions options = {});

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_H_
