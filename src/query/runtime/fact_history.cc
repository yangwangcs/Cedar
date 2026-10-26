#include "query/runtime/fact_history.h"

#include "query/resource/query_read_memory.h"
#include "query/runtime/fact_chain_cursor.h"

namespace cedar::internal {

StatusOr<std::shared_ptr<const std::vector<StateInterval>>> ReadPresentIntervals(
    Snapshot& snapshot, const FactRef& ref, const QueryDeltaView* delta,
    const std::function<Status()>& check_abort, QueryReservation* reservation) {
  if (check_abort) {
    if (Status status = check_abort(); !status.ok()) return status;
  }
  auto memory = reservation ? std::make_shared<QueryReadMemory>(*reservation)
                            : nullptr;
  FactChainCursor cursor(FactBatchOrder::kUnknown, snapshot.commit_seq(),
                         std::nullopt, memory);
  FactReadSpec spec;
  spec.part_scope = PartScope::Exact(ref.part_id());
  spec.family = ref.family();
  spec.property_id = ref.property_id();
  spec.read_memory = memory;
  spec.entity_range = EntityRange{
      ref.entity_id(), ref.entity_id() == UINT64_MAX
          ? std::nullopt : std::optional<uint64_t>(ref.entity_id() + 1)};
  Status status = snapshot.canonical_reader().ReadEvents(
      spec, [&cursor, &check_abort](const FactEventBatch& batch) {
        if (check_abort) {
          if (Status status = check_abort(); !status.ok()) return status;
        }
        return cursor.Consume(batch.events);
      });
  if (!status.ok()) return status;
  if (delta) {
    for (const auto& event : delta->EventRangeFor(ref)) {
      if (check_abort) {
        if (Status status = check_abort(); !status.ok()) return status;
      }
      if (Status status = cursor.Consume(event); !status.ok()) return status;
    }
  }
  if (Status status = cursor.Finish(snapshot.commit_seq()); !status.ok()) return status;
  auto retained = cursor.TakeChains();
  if (!retained.ok()) return retained.status();
  auto chains = retained.ConsumeValueOrDie();
  static const std::vector<StateInterval> empty;
  const auto* intervals = chains->empty() ? &empty : &chains->front().present;
  return std::shared_ptr<const std::vector<StateInterval>>(std::move(chains), intervals);
}

}  // namespace cedar::internal
