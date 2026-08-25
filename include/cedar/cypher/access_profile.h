#ifndef CEDAR_CYPHER_ACCESS_PROFILE_H_
#define CEDAR_CYPHER_ACCESS_PROFILE_H_

#include <cstdint>

#include "cedar/core/status.h"
#include "cedar/cypher/binder.h"
#include "cedar/query/types.h"

namespace cedar::cypher {

enum class AccessProfile : uint8_t {
  kPointState,
  kValidTimeRange,
  kValidTimeChanges,
  kSystemTimeAsOf,
  kSystemTimeRange,
  kGraphExpand,
  kMetadataEnrichment,
  kUnsupported,
};

struct AccessDecision {
  AccessProfile profile = AccessProfile::kPointState;
  QueryOptions options;
  bool expects_projection = false;
};

StatusOr<AccessDecision> ChooseAccess(const BoundStatement& statement,
                                      const QueryOptions& requested);
const char* AccessProfileName(AccessProfile profile);

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_ACCESS_PROFILE_H_
