#include "query/runtime/fact_chain_cursor.h"

#include <algorithm>

namespace cedar::internal {
namespace {

bool EventLess(const FactEvent& left, const FactEvent& right) {
  if (left.ref != right.ref) {
    if (left.ref.part_id().value != right.ref.part_id().value)
      return left.ref.part_id().value < right.ref.part_id().value;
    if (left.ref.family() != right.ref.family())
      return static_cast<uint8_t>(left.ref.family()) <
             static_cast<uint8_t>(right.ref.family());
    if (left.ref.property_id().value != right.ref.property_id().value)
      return left.ref.property_id().value < right.ref.property_id().value;
    return left.ref.entity_id() < right.ref.entity_id();
  }
  if (left.valid_from != right.valid_from)
    return left.valid_from.value < right.valid_from.value;
  return left.commit_seq.value < right.commit_seq.value;
}

bool OrderedBefore(const FactEvent& left, const FactEvent& right,
                   std::optional<bool> descending) {
  if (left.ref != right.ref) return EventLess(left, right);
  const bool desc = descending.value_or(false);
  if (left.valid_from != right.valid_from) {
    return desc ? left.valid_from.value > right.valid_from.value
                : left.valid_from.value < right.valid_from.value;
  }
  return desc ? left.commit_seq.value > right.commit_seq.value
              : left.commit_seq.value < right.commit_seq.value;
}

Status ReserveCorrection(const std::vector<FactEvent>& events,
                         FactReadMemoryScope* workspace) {
  // Covers filtering/sorting copies, geometric boundary/state growth and
  // the overlapping coalescing buffer in the existing correction helpers.
  Status status = workspace->ReserveElements(
      events.size(), 3 * sizeof(FactEvent) + 2 * sizeof(CorrectedBoundary) +
                         3 * sizeof(StateInterval));
  if (!status.ok()) return status;
  for (const auto& event : events) {
    status = workspace->ReserveElements(FactValueHeapBytes(event.value), 6);
    if (!status.ok()) return status;
  }
  return Status::OK();
}

}  // namespace

Status FactChainCursor::Consume(const std::vector<FactEvent>& events) {
  for (const FactEvent& event : events) {
    Status status = Consume(event);
    if (!status.ok()) return status;
  }
  return Status::OK();
}

Status FactChainCursor::Consume(const FactEvent& event) {
  if (finished_) return Status::InvalidArgument("fact chain cursor", "already finished");
  const Status valid = event.Validate();
  if (!valid.ok()) return valid;
  if (order_ == FactBatchOrder::kUnknown) {
    Status status = ReserveFactAppend(&events_, &history_memory_);
    if (!status.ok()) return status;
    status = history_payload_.Reserve(FactValueHeapBytes(event.value));
    if (!status.ok()) return status;
    events_.push_back(event);
    ordered_ = false;
    return Status::OK();
  }
  if (last_event_.has_value() && last_event_->ref == event.ref &&
      !descending_within_identity_.has_value()) {
    if (last_event_->valid_from != event.valid_from) {
      descending_within_identity_ =
          last_event_->valid_from.value > event.valid_from.value;
    } else if (last_event_->commit_seq != event.commit_seq) {
      descending_within_identity_ =
          last_event_->commit_seq.value > event.commit_seq.value;
    }
  }
  if (last_event_.has_value() &&
      OrderedBefore(event, *last_event_, descending_within_identity_)) {
    if (snapshot_seq_.has_value()) {
      return Status::Corruption("fact chain cursor",
                                "ordered reader violated fact key order");
    }
    ordered_ = false;
  }
  // Order checking only needs identity and timestamps, not another payload.
  last_event_ = FactEvent{event.ref, event.valid_from, event.commit_seq,
                          event.operation, event.schema_epoch, std::nullopt};
  if (!ordered_) {
    Status status = ReserveFactAppend(&events_, &history_memory_);
    if (!status.ok()) return status;
    status = history_payload_.Reserve(FactValueHeapBytes(event.value));
    if (!status.ok()) return status;
    events_.push_back(event);
    return Status::OK();
  }
  if (has_active_ && event.ref != *active_ref_) {
    Status status = FlushActive();
    if (!status.ok()) return status;
  }
  if (!has_active_) {
    active_ref_ = event.ref;
    has_active_ = true;
  }
  if (snapshot_seq_.has_value()) {
    Status status = ReserveFactAppend(&active_events_, &history_memory_);
    if (!status.ok()) return status;
    status = history_payload_.Reserve(FactValueHeapBytes(event.value));
    if (!status.ok()) return status;
    active_events_.push_back(event);
  } else {
    Status status = ReserveFactAppend(&events_, &history_memory_);
    if (!status.ok()) return status;
    status = history_payload_.Reserve(FactValueHeapBytes(event.value));
    if (!status.ok()) return status;
    events_.push_back(event);
  }
  return Status::OK();
}

Status FactChainCursor::Finish(CommitSeq snapshot_seq) {
  if (finished_) return Status::InvalidArgument("fact chain cursor", "already finished");
  finished_ = true;
  if (snapshot_seq_.has_value() && ordered_) return FlushActive();
  if (has_active_) {
    events_.insert(events_.end(), active_events_.begin(), active_events_.end());
    active_events_.clear();
    has_active_ = false;
  }
  if (!ordered_) {
    ++sort_fallbacks_;
    FactReadMemoryScope sort_memory(memory_.get());
    Status reserved = sort_memory.ReserveElements(events_.size(), sizeof(FactEvent));
    if (!reserved.ok()) return reserved;
    std::stable_sort(events_.begin(), events_.end(),
                     [this](const FactEvent& left, const FactEvent& right) {
                       return OrderedBefore(left, right,
                                             descending_within_identity_);
                     });
  }
  return FinishBuffered(snapshot_seq);
}

Status FactChainCursor::FlushActive() {
  if (!has_active_) return Status::OK();
  if (!snapshot_seq_.has_value()) return Status::OK();
  FactReadMemoryScope workspace(memory_.get());
  Status reserved = ReserveCorrection(active_events_, &workspace);
  if (!reserved.ok()) return reserved;
  std::vector<FactEvent> visible;
  visible.reserve(active_events_.size());
  for (const FactEvent& event : active_events_) {
    if (event.commit_seq.value <= snapshot_seq_->value) visible.push_back(event);
  }
  if (!visible.empty()) {
    auto boundaries = ResolveCorrectedBoundaries(visible, *snapshot_seq_);
    if (!boundaries.ok()) return boundaries.status();
    auto present = MaterializePresentState(boundaries.ValueOrDie());
    Status status = AppendChain(visible.front().ref,
                                boundaries.ConsumeValueOrDie(), std::move(present));
    if (!status.ok()) return status;
  }
  active_events_.clear();
  history_payload_.Reset();
  has_active_ = false;
  return Status::OK();
}

Status FactChainCursor::FinishBuffered(CommitSeq snapshot_seq) {
  FactReadMemoryScope workspace(memory_.get());
  Status reserved = ReserveCorrection(events_, &workspace);
  if (!reserved.ok()) return reserved;
  for (const FactEvent& event : events_) {
    const Status status = event.Validate();
    if (!status.ok()) return status;
  }
  for (size_t begin = 0; begin < events_.size();) {
    size_t end = begin + 1;
    while (end < events_.size() && events_[end].ref == events_[begin].ref) ++end;
    std::vector<FactEvent> chain_events;
    chain_events.reserve(end - begin);
    for (size_t i = begin; i < end; ++i) {
      if (events_[i].commit_seq.value <= snapshot_seq.value) chain_events.push_back(events_[i]);
    }
    if (!chain_events.empty()) {
      auto boundaries = ResolveCorrectedBoundaries(chain_events, snapshot_seq);
      if (!boundaries.ok()) return boundaries.status();
      auto present = MaterializePresentState(boundaries.ValueOrDie());
      Status status = AppendChain(chain_events.front().ref,
                                  boundaries.ConsumeValueOrDie(), std::move(present));
      if (!status.ok()) return status;
    }
    begin = end;
  }
  return Status::OK();
}

Status FactChainCursor::AppendChain(
    FactRef ref, std::vector<CorrectedBoundary> boundaries,
    std::vector<StateInterval> present) {
  Status status = ReserveFactAppend(&chains_, &chain_memory_);
  if (!status.ok()) return status;
  status = chain_memory_.ReserveElements(boundaries.capacity(), sizeof(CorrectedBoundary));
  if (!status.ok()) return status;
  status = chain_memory_.ReserveElements(present.capacity(), sizeof(StateInterval));
  if (!status.ok()) return status;
  for (const auto& boundary : boundaries) {
    status = chain_memory_.Reserve(FactValueHeapBytes(boundary.value));
    if (!status.ok()) return status;
  }
  for (const auto& state : present) {
    status = chain_memory_.Reserve(FactValueHeapBytes(state.value));
    if (!status.ok()) return status;
  }
  chains_.push_back({ref, std::move(boundaries), std::move(present)});
  return Status::OK();
}

StatusOr<std::shared_ptr<const std::vector<FactChainView>>> FactChainCursor::TakeChains(
    uint64_t extra_owner_bytes) {
  if (!finished_) return Status::InvalidArgument("fact chain cursor", "not finished");
  struct Owner {
    std::shared_ptr<const void> lease;
    std::vector<FactChainView> chains;
  };
  Status status = chain_memory_.Reserve(extra_owner_bytes);
  if (!status.ok()) return status;
  status = chain_memory_.Reserve(sizeof(Owner) + 64);
  if (!status.ok()) return status;
  auto retained = chain_memory_.Retain(memory_);
  if (!retained.ok()) return retained.status();
  auto owner = std::make_shared<Owner>(retained.ConsumeValueOrDie(), std::move(chains_));
  const auto* chains = &owner->chains;
  return std::shared_ptr<const std::vector<FactChainView>>(std::move(owner), chains);
}

}  // namespace cedar::internal
