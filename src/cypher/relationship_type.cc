#include "cedar/cypher/relationship_type.h"

namespace cedar::cypher {

StatusOr<uint64_t> ResolveRelationshipType(std::string_view name) {
  uint64_t hash = 1469598103934665603ULL;
  for (const unsigned char byte : name) {
    hash ^= byte;
    hash *= 1099511628211ULL;
  }
  return hash == 0 ? 1 : hash;
}

}  // namespace cedar::cypher
