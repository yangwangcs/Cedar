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
  last_event_ = event;
  if (!ordered_) {
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
    active_events_.push_back(event);
  } else {
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
  std::vector<FactEvent> visible;
  visible.reserve(active_events_.size());
  for (const FactEvent& event : active_events_) {
    if (event.commit_seq.value <= snapshot_seq_->value) visible.push_back(event);
  }
  if (!visible.empty()) {
    auto boundaries = ResolveCorrectedBoundaries(visible, *snapshot_seq_);
    if (!boundaries.ok()) return boundaries.status();
    auto present = MaterializePresentState(boundaries.ValueOrDie());
    chains_.push_back(FactChainView{visible.front().ref,
                                    boundaries.ConsumeValueOrDie(),
                                    std::move(present)});
  }
  active_events_.clear();
  has_active_ = false;
  return Status::OK();
}

Status FactChainCursor::FinishBuffered(CommitSeq snapshot_seq) {
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
      chains_.push_back(FactChainView{chain_events.front().ref,
                                      boundaries.ConsumeValueOrDie(), {}});
      chains_.back().present = MaterializePresentState(chains_.back().boundaries);
    }
    begin = end;
  }
  return Status::OK();
}

}  // namespace cedar::internal
