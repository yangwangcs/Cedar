// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RUNTIME_READ_CONTEXT_H_
#define CEDAR_QUERY_RUNTIME_READ_CONTEXT_H_

#include <cstdint>
#include <memory>
#include <map>
#include <mutex>
#include <optional>
#include <string>

#include "cedar/fact/canonical_reader.h"
#include "cedar/fact/read_spec.h"
#include "query/runtime/fact_chain_cursor.h"

namespace cedar::internal {

class AdjacencyIndex;
class QueryDeltaLease;

struct TemporalChainCache {
  std::mutex mutex;
  // The cache owns only corrected, snapshot-bound chains. Raw FactEvent
  // payloads are consumed by FactChainCursor and are never retained here.
  std::map<std::string, std::shared_ptr<const std::vector<FactChainView>>> chains;
};

// Private query read capability. It is deliberately composed from read-only
// interfaces and contains no Database::Impl, FactStore, or RocksDB handle.
struct QueryReadContext {
  const CanonicalFactReader& facts;
  CommitSeq snapshot_seq;
  PartScope part_scope = PartScope::All();
  std::shared_ptr<const AdjacencyIndex> adjacency;
  std::shared_ptr<const QueryDeltaLease> delta;
  std::shared_ptr<TemporalChainCache> chain_cache;
  // A reader-side row cap is advisory and is populated only after semantic
  // validation. It is never used to broaden an unbounded scan.
  std::optional<uint64_t> max_rows;
  std::optional<CommitSeqRange> system_time_range;
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_RUNTIME_READ_CONTEXT_H_
