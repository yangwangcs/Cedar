// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TRANSACTION_COMMIT_TIMELINE_H_
#define CEDAR_TRANSACTION_COMMIT_TIMELINE_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/transaction/decision_log.h"
#include "cedar/transaction/system_hlc.h"

namespace cedar {

struct CommitTimelineEntry {
  uint64_t commit_seq;
  SystemHlc system_time_hlc;
};

// The timeline is rebuilt from durable DecisionLog records after its latest
// checksummed checkpoint. A mapping becomes query-visible only after the
// corresponding decision record is durable.
class CommitTimeline {
 public:
  explicit CommitTimeline(std::string checkpoint_path);

  Status Open();
  Status RestoreFromOutcomes(const std::vector<TransactionOutcome>& outcomes);
  Status RestoreFromDecisions(const std::vector<CommitDecision>& decisions);
  Status Allocate(uint64_t wall_clock_us, SystemHlc* result) const;
  Status AddDurableCommit(uint64_t commit_seq, SystemHlc system_time_hlc);
  Status Checkpoint() const;
  StatusOr<uint64_t> ResolveAsOf(uint64_t timestamp_us,
                                 uint64_t visible_seq_ceiling) const;
  const std::vector<CommitTimelineEntry>& entries() const { return entries_; }

 private:
  std::string checkpoint_path_;
  std::vector<CommitTimelineEntry> entries_;
};

}  // namespace cedar

#endif  // CEDAR_TRANSACTION_COMMIT_TIMELINE_H_
