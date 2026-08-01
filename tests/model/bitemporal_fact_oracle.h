// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TESTS_MODEL_BITEMPORAL_FACT_ORACLE_H_
#define CEDAR_TESTS_MODEL_BITEMPORAL_FACT_ORACLE_H_

#include <optional>
#include <utility>
#include <vector>

#include "cedar/fact/fact.h"

namespace cedar::test {

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

 private:
  std::vector<FactEvent> events_;
};

}  // namespace cedar::test

#endif  // CEDAR_TESTS_MODEL_BITEMPORAL_FACT_ORACLE_H_
