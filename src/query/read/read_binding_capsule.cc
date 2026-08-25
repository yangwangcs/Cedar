#include "query/read/read_binding_capsule.h"

namespace cedar::internal {
// The capsule is intentionally a value-only holder. Construction remains in
// the existing execution-context factory so leases are acquired atomically.
}
