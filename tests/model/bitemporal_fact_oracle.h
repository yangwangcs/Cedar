// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TESTS_MODEL_BITEMPORAL_FACT_ORACLE_H_
#define CEDAR_TESTS_MODEL_BITEMPORAL_FACT_ORACLE_H_

#include <algorithm>
#include <optional>
#include <utility>
#include <vector>

#include "cedar/fact/fact.h"
#include "cedar/query/types.h"

namespace cedar::test {

struct OracleStateInterval {
  ValidTimeInterval interval;
  std::optional<Value> value;

  bool operator==(const OracleStateInterval&) const = default;
};

struct OracleChange {
  ValidTime valid_from;
  std::optional<Value> before;
  std::optional<Value> after;

  bool operator==(const OracleChange&) const = default;
};

class BitemporalFactOracle {
 public:
  void Add(FactEvent event) { events_.push_back(std::move(event)); }

  std::optional<FactEvent> Read(const FactRef& ref, ValidTime valid_time,
                                CommitSeq snapshot_seq) const {
    std::optional<FactEvent> selected;
    for (const FactEvent& event : events_) {
      if (event.ref != ref || event.valid_from.value > valid_time.value ||
          event.commit_seq.value > snapshot_seq.value) {
        continue;
      }
      if (!selected.has_value() ||
          event.valid_from.value > selected->valid_from.value ||
          (event.valid_from == selected->valid_from &&
           event.commit_seq.value > selected->commit_seq.value)) {
        selected = event;
      }
    }
    if (selected.has_value() && selected->operation == FactOperation::kDelete) {
      return std::nullopt;
    }
    return selected;
  }

  std::vector<FactEvent> CorrectedEvents(const FactRef& ref,
                                          CommitSeq snapshot_seq) const {
    std::vector<FactEvent> visible;
    for (const FactEvent& event : events_) {
      if (event.ref == ref && event.commit_seq.value <= snapshot_seq.value) {
        visible.push_back(event);
      }
    }
    std::sort(visible.begin(), visible.end(), [](const FactEvent& left,
                                                  const FactEvent& right) {
      if (left.valid_from != right.valid_from) {
        return left.valid_from.value < right.valid_from.value;
      }
      return left.commit_seq.value < right.commit_seq.value;
    });

    std::vector<FactEvent> corrected;
    for (size_t index = 0; index < visible.size();) {
      size_t next = index + 1;
      while (next < visible.size() &&
             visible[next].valid_from == visible[index].valid_from) {
        ++next;
      }
      corrected.push_back(visible[next - 1]);
      index = next;
    }
    return corrected;
  }

  std::vector<OracleStateInterval> History(const FactRef& ref,
                                            CommitSeq snapshot_seq) const {
    const std::vector<FactEvent> corrected =
        CorrectedEvents(ref, snapshot_seq);
    bool is_present = false;
    std::optional<Value> value;
    std::vector<OracleStateInterval> history;
    for (size_t index = 0; index < corrected.size(); ++index) {
      is_present = corrected[index].operation == FactOperation::kPut;
      if (is_present) {
        value = corrected[index].value;
      } else {
        value.reset();
      }
      const std::optional<ValidTime> to =
          index + 1 == corrected.size()
              ? std::nullopt
              : std::optional<ValidTime>(corrected[index + 1].valid_from);
      if (!is_present) continue;
      if (!history.empty() && history.back().value == value &&
          history.back().interval.to.has_value() &&
          *history.back().interval.to == corrected[index].valid_from) {
        history.back().interval.to = to;
      } else {
        history.push_back({{corrected[index].valid_from, to}, value});
      }
    }
    return history;
  }

  std::vector<OracleChange> Changes(const FactRef& ref,
                                    CommitSeq snapshot_seq) const {
    const std::vector<FactEvent> corrected =
        CorrectedEvents(ref, snapshot_seq);
    std::optional<Value> current;
    std::vector<OracleChange> changes;
    for (const FactEvent& event : corrected) {
      std::optional<Value> next =
          event.operation == FactOperation::kPut ? event.value : std::nullopt;
      if (next != current) {
        changes.push_back({event.valid_from, current, next});
      }
      current = std::move(next);
    }
    return changes;
  }

 private:
  std::vector<FactEvent> events_;
};

}  // namespace cedar::test

#endif  // CEDAR_TESTS_MODEL_BITEMPORAL_FACT_ORACLE_H_
