#ifndef CEDAR_CYPHER_WRITE_H_
#define CEDAR_CYPHER_WRITE_H_

#include "cedar/cypher/binder.h"
#include "cedar/database.h"
#include "cedar/transaction.h"

namespace cedar::cypher {

// Lowers one write statement into an existing Cedar transaction. The caller
// owns commit/rollback, so the canonical FactEvent publisher is reached only
// when Transaction::Commit is invoked.
Status StageWrite(Database& database, Transaction& transaction,
                  const BoundStatement& statement, const Bindings& bindings,
                  ValidTime valid_time);

StatusOr<CommitResult> ExecuteWrite(Database& database,
                                    const BoundStatement& statement,
                                    const Bindings& bindings,
                                    ValidTime valid_time);

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_WRITE_H_
