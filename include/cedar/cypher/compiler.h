#ifndef CEDAR_CYPHER_COMPILER_H_
#define CEDAR_CYPHER_COMPILER_H_

#include "cedar/core/status.h"
#include "cedar/cypher/binder.h"
#include "cedar/query/query.h"

namespace cedar::cypher {

StatusOr<Query> Compile(const BoundStatement& statement);

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_COMPILER_H_
