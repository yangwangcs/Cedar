// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_META_V2_RECLAIM_H_
#define CEDAR_FACT_META_V2_RECLAIM_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

#include "cedar/core/status.h"

namespace cedar {

// Options for ReclaimSequenceRecordsV2, the offline migration that rewrites
// v1 `sequence/<seq>` metadata records in place as compact meta sequence v2
// records. Reclamation requires the durable format marker to already record 2.
struct MetaV2ReclaimOptions {
  // Maximum number of sequence records rewritten by one atomic write batch.
  // A crash between batches leaves the database consistent: the records of a
  // committed batch are v2 and every later record is still v1, so rerunning the
  // tool resumes where the stopped run left off. Must be positive.
  size_t batch_records = 1024;
  // Sync every write batch before returning. When false (the default) batches
  // are synced once at the end of a successful run.
  bool sync_each_batch = false;
  // Decode every rewritten record again and compare it, including the rebuilt
  // 32-byte fact keys, with the decoded v1 source before that batch is written.
  bool verify = true;
  // Test-only fault injection. Called with the zero-based index of each batch
  // after that batch was written and before the final sync. Throwing from the
  // callback simulates an abrupt stop; the committed batches remain and a later
  // run resumes from the first v1 record.
  std::function<void(size_t batch_index)> fault_for_testing;
};

// Accounting for one ReclaimSequenceRecordsV2 call. On a marker/format error the
// counters stay zero; on an I/O or decode error they describe the work accepted
// before the error.
struct MetaV2ReclaimStats {
  // Number of `sequence/` records examined.
  uint64_t scanned = 0;
  // Number of v1 records converted to v2 by this run.
  uint64_t rewritten = 0;
  // Number of records that were already v2 and were left untouched.
  uint64_t already_v2 = 0;
  // Encoded value bytes of the examined records before this run and after the
  // run. Rewritten records contribute their old size to `bytes_before` and their
  // new size to `bytes_after`; already-v2 records contribute their stored size
  // to both, so a rerun reports equal totals. The difference is exactly
  // 6 + 9 * facts bytes per rewritten record.
  uint64_t bytes_before = 0;
  uint64_t bytes_after = 0;
};

// Offline, resumable, idempotent rewrite of a database's committed v1 sequence
// records to meta sequence v2. The caller must ensure no Cedar process has the
// database open; the tool opens the storage engine itself and fails if it is
// already locked. The database must have been upgraded through the supported
// opt-in path first: if the durable meta sequence format marker is missing or
// does not record format 2, the call fails before writing anything.
//
// Records are rewritten per atomic batch of at most options.batch_records
// entries. A stopped or crashed run leaves a consistent mixed database
// (completed batches are v2, later records are v1); rerunning the function is
// idempotent and skips records that are already v2. `stats` may be null.
Status ReclaimSequenceRecordsV2(const std::string& db_path,
                                const MetaV2ReclaimOptions& options,
                                MetaV2ReclaimStats* stats);

}  // namespace cedar

#endif  // CEDAR_FACT_META_V2_RECLAIM_H_
