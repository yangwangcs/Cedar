#ifndef CEDAR_CYPHER_WRITE_H_
#define CEDAR_CYPHER_WRITE_H_

#include "cedar/cypher/binder.h"
#include "cedar/database.h"

namespace cedar::cypher {

StatusOr<CommitResult> ExecuteWrite(Database& database,
                                    const BoundStatement& statement,
                                    const Bindings& bindings,
                                    ValidTime valid_time);

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_WRITE_H_
