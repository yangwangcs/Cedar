#ifndef CEDAR_QUERY_RUNTIME_FACT_CHAIN_CURSOR_H_
#define CEDAR_QUERY_RUNTIME_FACT_CHAIN_CURSOR_H_

#include <cstdint>
#include <optional>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"
#include "cedar/fact/read_spec.h"
#include "query/temporal/corrected_chain.h"
#include "storage/facts/read_memory_scope.h"

namespace cedar::internal {

enum class FactBatchOrder : uint8_t {
  kUnknown,
  kIdentityValidDescCommitDesc,
};

struct FactChainView {
  FactRef ref;
  std::vector<CorrectedBoundary> boundaries;
  std::vector<StateInterval> present;
};

class FactChainCursor {
 public:
  explicit FactChainCursor(
      FactBatchOrder order = FactBatchOrder::kUnknown,
      std::optional<CommitSeq> snapshot_seq = std::nullopt,
      std::optional<CommitSeqRange> range = std::nullopt,
      std::shared_ptr<FactReadMemory> memory = {})
      : order_(order), snapshot_seq_(snapshot_seq), range_(range),
        memory_(std::move(memory)), history_memory_(memory_.get()),
        history_payload_(memory_.get()), chain_memory_(memory_.get()) {}

  Status Consume(const std::vector<FactEvent>& events);
  Status Consume(const FactEvent& event);
  Status Finish(CommitSeq snapshot_seq);

  const std::vector<FactChainView>& chains() const { return chains_; }
  StatusOr<std::shared_ptr<const std::vector<FactChainView>>> TakeChains(
      uint64_t extra_owner_bytes = 0);
  uint64_t sort_fallbacks() const { return sort_fallbacks_; }

 private:
  Status FlushActive();
  Status FinishBuffered(CommitSeq snapshot_seq);
  Status AppendChain(FactRef ref, std::vector<CorrectedBoundary> boundaries,
                     std::vector<StateInterval> present);

  FactBatchOrder order_;
  std::optional<CommitSeq> snapshot_seq_;
  std::optional<CommitSeqRange> range_;
  std::shared_ptr<FactReadMemory> memory_;
  FactReadMemoryScope history_memory_;
  FactReadMemoryScope history_payload_;
  FactReadMemoryScope chain_memory_;
  std::vector<FactEvent> events_;
  std::vector<FactEvent> active_events_;
  std::optional<FactRef> active_ref_;
  bool has_active_ = false;
  std::optional<FactEvent> last_event_;
  std::optional<bool> descending_within_identity_;
  bool ordered_ = true;
  std::vector<FactChainView> chains_;
  uint64_t sort_fallbacks_ = 0;
  bool finished_ = false;
};

}  // namespace cedar::internal

#endif
