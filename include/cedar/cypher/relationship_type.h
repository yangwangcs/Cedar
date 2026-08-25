#ifndef CEDAR_CYPHER_RELATIONSHIP_TYPE_H_
#define CEDAR_CYPHER_RELATIONSHIP_TYPE_H_

#include <cstdint>
#include <string_view>

#include "cedar/core/status.h"

namespace cedar::cypher {

StatusOr<uint64_t> ResolveRelationshipType(std::string_view name);

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_RELATIONSHIP_TYPE_H_
