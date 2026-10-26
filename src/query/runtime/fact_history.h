#ifndef CEDAR_QUERY_RUNTIME_FACT_HISTORY_H_
#define CEDAR_QUERY_RUNTIME_FACT_HISTORY_H_

#include <functional>
#include <memory>
#include <vector>

#include "cedar/snapshot.h"
#include "query/projection/query_delta.h"
#include "query/resource/query_resource_pool.h"
#include "query/temporal/corrected_chain.h"

namespace cedar::internal {

// The returned intervals retain their corrected chain and its memory lease.
StatusOr<std::shared_ptr<const std::vector<StateInterval>>> ReadPresentIntervals(
    Snapshot& snapshot, const FactRef& ref, const QueryDeltaView* delta,
    const std::function<Status()>& check_abort, QueryReservation* reservation);

}  // namespace cedar::internal
#endif
