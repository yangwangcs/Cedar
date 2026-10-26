#ifndef CEDAR_CYPHER_SEMANTIC_KEY_H_
#define CEDAR_CYPHER_SEMANTIC_KEY_H_

#include <string>
#include <string_view>

#include "cedar/cypher/binder.h"

namespace cedar::cypher {

std::string PreparedSemanticKey(const BoundStatement& statement,
                                std::string_view schema_fingerprint,
                                const BinderOptions& options);

}  // namespace cedar::cypher

#endif
