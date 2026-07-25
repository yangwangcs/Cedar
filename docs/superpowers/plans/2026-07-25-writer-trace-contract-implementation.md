# Cedar Writer and Trace Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the release source contract with an exact public-entry-to-durable-writer map and an exact production trace-producer/sink inventory, then archive self-contained source-contract r2 evidence.

**Architecture:** The existing CMake scanner remains the single static-contract owner. It parses bounded production source patterns and the correctness-kernel test declarations, validates exact reviewed inventories, emits deterministic TSV and zero-hit outputs, and transactionally publishes a schema-2 JSON summary; the focused CMake harness proves all new rules fail closed without changing accepted bytes.

**Tech Stack:** CMake script mode, CTest, C/C++ source inspection, Git read-only metadata, SHA-256, existing `cedar_evidence_verify`.

## Global Constraints

- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.
- Use `-j1` for every build and CTest command.
- Do not modify production runtime behavior, persistence format, trace sampling, trace labels, or serialization.
- Keep database format version `1` and current clean-break names.
- Do not weaken an exact inventory into a wildcard allowlist.
- Static source-contract evidence does not replace production concurrency, crash, saturation, qualified-host, or paired-performance campaigns.
- Exclude paper closure and external LDBC from this goal.
- A failed scan must leave the last accepted output byte-for-byte unchanged.

---

## File Map

- `tests/test_release_source_contract.cmake`: owns the accepted synthetic source fixture, new negative mutations, and accepted-output immutability assertions.
- `cmake/VerifyReleaseSourceContract.cmake`: owns exact public writer entries, required call edges, allowed functional tests, trace producers/sinks, drift detection, and deterministic r2 output.
- `CMakeLists.txt`: retains the existing `release_source_contract` and `release_source_contract_negative` registrations; no new test executable is needed.
- `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`: records only the two newly proven static rows.
- `docs/superpowers/plans/2026-07-23-cedar-six-design-batched-final-closure-goal.md`: records r2 evidence and preserves all runtime blockers.
- `.superpowers/sdd/progress.md`: records commands, counts, evidence identity, and remaining release blockers.
- `results/release-closure-20260725-source-contract-r2/`: immutable archived inputs, scanner, harness, outputs, logs, manifest, and exact SHA ledger.

### Task 1: Add the public-writer contract fixture and observe RED

**Files:**
- Modify: `tests/test_release_source_contract.cmake`
- Test: `build-current/CTestTestfile.cmake` via existing CTest registration

**Interfaces:**
- Consumes: existing `PREPARE_FIXTURE`, `EXPECT_SCANNER_FAILURE`, and accepted-output digest helpers.
- Produces: a fixture containing the 16 approved public durable entries, required writer edges, and 18 exact functional-test names.

- [ ] **Step 1: Extend the accepted fixture with exact public API declarations**

Add fixture files for the three public surfaces with these exact declarations:

```cmake
WRITE_FIXTURE_FILE(
    "include/cedar/db/cedar_database.h"
    "class CedarDatabase { public:\n  Status Open();\n  Status Close();\n  Status RegisterColumn();\n  Status RegisterIndex();\n  Status SetIndexState();\n  Status RepairIndexes();\n  Status DropIndex();\n  Status Put();\n  Status Delete();\n  Status Flush();\n  Status Compact();\n  Status RotateBlobSegments();\n  Status CollectBlobGarbage();\n  Status Checkpoint();\n};\n")
WRITE_FIXTURE_FILE(
    "include/cedar/tcypher/session.h"
    "class TcypherSession { public: Status Commit(uint64_t* commit_seq); };\n")
WRITE_FIXTURE_FILE(
    "include/cedar/tcypher/runtime/transaction_sink.h"
    "class TransactionSink { public: Status Submit(); };\n")
```

- [ ] **Step 2: Add the accepted ordered writer edges**

Append exact symbol/call spellings to the fixture owners so the positive fixture covers commit, schema/index, flush, compaction, Blob lifecycle, checkpoint, close, and open path families:

```cmake
APPEND_FIXTURE_FILE(
    "src/transaction/transaction_coordinator.cc"
    "\nStatus TransactionCoordinator::Commit() { return CommitInternal(); }\n"
    "Status TransactionCoordinator::CommitStrict() { return CommitInternal(); }\n"
    "Status TransactionCoordinator::CommitInternal() { blob_store_.PutBatch(); prepare_log_.Append(); decision_log_.AppendCommitWithResult(); return commit_timeline_.AddDurableCommit(); }\n"
    "Status TransactionCoordinator::Flush() { WriteSstFile(); return version_set_.ApplyEdit(); }\n"
    "Status TransactionCoordinator::Compact() { CompactSstPartition(); return version_set_.ApplyEdit(); }\n"
    "Status TransactionCoordinator::CheckpointDurableLogs() { return CheckpointDurableLogsInternal(); }\n")
APPEND_FIXTURE_FILE(
    "src/tcypher/runtime/transaction_sink.cc"
    "Status TransactionSink::Submit() { return coordinator_->Commit(); }\n")
```

Add these explicit facade/session definitions; keep calls visible rather than
hiding them behind fixture macros:

```cmake
APPEND_FIXTURE_FILE(
    "src/db/cedar_database.cc"
    "Status CedarDatabase::Open() { return coordinator_.Open(); }\n"
    "Status CedarDatabase::Close() { Status s = coordinator_.Flush(); return s.ok() ? coordinator_.CheckpointDurableLogs() : s; }\n"
    "Status CedarDatabase::RegisterColumn() { return coordinator_.RegisterColumn(); }\n"
    "Status CedarDatabase::RegisterIndex() { return coordinator_.RegisterIndex(); }\n"
    "Status CedarDatabase::SetIndexState() { return coordinator_.SetIndexState(); }\n"
    "Status CedarDatabase::RepairIndexes() { return coordinator_.RepairIndexes(); }\n"
    "Status CedarDatabase::DropIndex() { return coordinator_.DropIndex(); }\n"
    "Status CedarDatabase::Put() { return coordinator_.Commit(); }\n"
    "Status CedarDatabase::Delete() { return coordinator_.Commit(); }\n"
    "Status CedarDatabase::Flush() { return coordinator_.Flush(); }\n"
    "Status CedarDatabase::Compact() { return coordinator_.Compact(); }\n"
    "Status CedarDatabase::RotateBlobSegments() { return coordinator_.RotateBlobSegments(); }\n"
    "Status CedarDatabase::CollectBlobGarbage() { return coordinator_.CollectBlobGarbage(); }\n"
    "Status CedarDatabase::Checkpoint() { Status s = coordinator_.Flush(); return s.ok() ? coordinator_.CheckpointDurableLogs() : s; }\n")
APPEND_FIXTURE_FILE(
    "include/cedar/tcypher/session.h"
    "Status TcypherSession::Commit(uint64_t* out) { return coordinator_->Commit(); }\n")
```

- [ ] **Step 3: Add the exact allowed functional-test declarations**

Create `tests/test_correctness_kernel.cc` in the fixture and write one `TEST` declaration for each of the 18 approved names from the design. The scanner input must include this file and each name must occur exactly once.

```cmake
WRITE_FIXTURE_FILE(
    "tests/test_correctness_kernel.cc"
    "TEST(Fixture, OpenRunsRecoveryThroughTypedScheduler) {}\n"
    "TEST(Fixture, CloseRunsTheRealProtocolAsTypedShutdownWork) {}\n"
    "TEST(Fixture, TypedFacadeUsesOnlyNewTransactionAndSchemaContracts) {}\n"
    "TEST(Fixture, TransactionSinkConvertsTypedMutationsAndCommitsAtomically) {}\n"
    "TEST(Fixture, CoordinatorExternalizesLargeBinaryBeforePrepareAndRestoresIt) {}\n"
    "TEST(Fixture, CoordinatorPublishesSchemaOnlyThroughManifest) {}\n"
    "TEST(Fixture, PublicationFaultMatrixCleansUnmanifestedSstAndSidecarOutputs) {}\n"
    "TEST(Fixture, IndexCatalogValidatesSchemaAndPublishesLifecycleEdits) {}\n"
    "TEST(Fixture, RepairsCorruptIndexSidecarWhileQueriesRemainCorrect) {}\n"
    "TEST(Fixture, DropIndexRetiresSidecarsAfterPinnedVersionSnapshotsRelease) {}\n"
    "TEST(Fixture, FlushesCommittedShardEventsIntoPartitionedSstFiles) {}\n"
    "TEST(Fixture, CompactionPublishesOneOutputAndReclaimsReleasedInputs) {}\n"
    "TEST(Fixture, BlobRotationFaultRequiresReopenWithoutLosingManifestState) {}\n"
    "TEST(Fixture, BlobGcTombstonesDeadHashesAndReclaimsSealedSegments) {}\n"
    "TEST(Fixture, ManifestOwnsBlobSegmentRotationAndRetirement) {}\n"
    "TEST(Fixture, CheckpointRetainsCommittedOutcomesAfterDecisionPrefixReclamation) {}\n"
    "TEST(Fixture, IndexLifecycleCompactsRepairsDropsAndReopensWithoutIdReuse) {}\n"
    "TEST(Fixture, RetainedScheduledQueryStreamIsCancelledAfterDatabaseDestruction) {}\n")
```

- [ ] **Step 4: Require writer output and add four negative cases**

After the baseline scan, require `public-writer-call-paths.tsv`. Add fixture mutations for an unlisted public mutation method, an unlisted writer edge, a missing required edge, and a nonexistent functional-test binding. Each mutation must call `EXPECT_SCANNER_FAILURE` with the precise scanner error and verify unchanged accepted bytes.

```cmake
if(NOT EXISTS "${FIXTURE_OUTPUT}/public-writer-call-paths.tsv")
  message(FATAL_ERROR "baseline output does not contain public writer paths")
endif()

PREPARE_FIXTURE()
file(READ "${FIXTURE_SOURCE}/include/cedar/db/cedar_database.h" PUBLIC_HEADER)
string(REPLACE "Status Checkpoint();"
               "Status Checkpoint(); Status RewriteDurableState();"
               PUBLIC_HEADER "${PUBLIC_HEADER}")
file(WRITE "${FIXTURE_SOURCE}/include/cedar/db/cedar_database.h"
     "${PUBLIC_HEADER}")
APPEND_FIXTURE_FILE("src/db/cedar_database.cc"
                    "Status CedarDatabase::RewriteDurableState() { return coordinator_.Flush(); }\n")
EXPECT_SCANNER_FAILURE("unlisted public mutation"
                       "public durable-entry inventory drifted")
```

Add the other three mutations exactly as follows:

```cmake
PREPARE_FIXTURE()
file(READ "${FIXTURE_SOURCE}/src/db/cedar_database.cc" DATABASE_BODY)
string(REPLACE "Status CedarDatabase::Put() { return coordinator_.Commit(); }"
               "Status CedarDatabase::Put() { blob_store_.PutBatch(); return coordinator_.Commit(); }"
               DATABASE_BODY "${DATABASE_BODY}")
file(WRITE "${FIXTURE_SOURCE}/src/db/cedar_database.cc" "${DATABASE_BODY}")
EXPECT_SCANNER_FAILURE("unlisted writer edge"
                       "public writer edge drifted")

PREPARE_FIXTURE()
file(READ "${FIXTURE_SOURCE}/src/transaction/transaction_coordinator.cc" BODY)
string(REPLACE "decision_log_.AppendCommitWithResult()"
               "decision_log_.AppendCommitUnchecked()" BODY "${BODY}")
file(WRITE "${FIXTURE_SOURCE}/src/transaction/transaction_coordinator.cc" "${BODY}")
EXPECT_SCANNER_FAILURE("missing required writer edge"
                       "public writer edge drifted")

PREPARE_FIXTURE()
file(READ "${FIXTURE_SOURCE}/tests/test_correctness_kernel.cc" TEST_BODY)
string(REPLACE "TypedFacadeUsesOnlyNewTransactionAndSchemaContracts"
               "NonexistentWriterContractTest" TEST_BODY "${TEST_BODY}")
file(WRITE "${FIXTURE_SOURCE}/tests/test_correctness_kernel.cc" "${TEST_BODY}")
EXPECT_SCANNER_FAILURE("missing functional test binding"
                       "public writer functional-test binding drifted")
```

- [ ] **Step 5: Run the focused negative CTest and confirm RED**

Run:

```bash
cmake -S . -B build-current -DBUILD_TESTS=ON
ctest --test-dir build-current -j1 -R '^release_source_contract_negative$' --output-on-failure
```

Expected: FAIL because `public-writer-call-paths.tsv` is not emitted or because
the new public-writer rules are not implemented. Preserve the failing log for
the later r2 evidence manifest.

### Task 2: Implement the exact public-entry-to-writer scanner and reach GREEN

**Files:**
- Modify: `cmake/VerifyReleaseSourceContract.cmake`
- Test: `tests/test_release_source_contract.cmake`

**Interfaces:**
- Consumes: production headers/sources and `tests/test_correctness_kernel.cc`.
- Produces: deterministic `public-writer-call-paths.tsv` and writer drift failures.

- [ ] **Step 1: Bind the correctness kernel as a scanned input**

Add the correctness kernel to the required input set without adding the entire test tree:

```cmake
set(FUNCTIONAL_TEST_FILE tests/test_correctness_kernel.cc)
list(APPEND SCANNED_INPUT_FILES "${FUNCTIONAL_TEST_FILE}")
```

Apply the existing existence, root-containment, and SHA-256 checks to this file.

- [ ] **Step 2: Define exact entries, allowed tests, and writer rows**

Store the exact public entries and allowed test names as sorted CMake lists.
Store each call-path record as a pipe-delimited internal record whose fields are
converted to tab-separated output only after validation:

```cmake
set(EXPECTED_PUBLIC_DURABLE_ENTRIES
    CedarDatabase::Open
    CedarDatabase::Close
    CedarDatabase::RegisterColumn
    CedarDatabase::RegisterIndex
    CedarDatabase::SetIndexState
    CedarDatabase::RepairIndexes
    CedarDatabase::DropIndex
    CedarDatabase::Put
    CedarDatabase::Delete
    TcypherSession::Commit
    TransactionSink::Submit
    CedarDatabase::Flush
    CedarDatabase::Compact
    CedarDatabase::RotateBlobSegments
    CedarDatabase::CollectBlobGarbage
    CedarDatabase::Checkpoint)

set(EXPECTED_PUBLIC_WRITER_PATHS
    "CedarDatabase::Put|CedarDatabase::Put>TransactionCoordinator::Commit>TransactionCoordinator::CommitInternal>DecisionLog::AppendCommitWithResult|src/transaction/decision_log.cc|decision_log_append|TypedFacadeUsesOnlyNewTransactionAndSchemaContracts"
    "CedarDatabase::Put|CedarDatabase::Put>TransactionCoordinator::Commit>TransactionCoordinator::CommitInternal>CommitTimeline::AddDurableCommit|src/transaction/commit_timeline.cc|timeline_append|TypedFacadeUsesOnlyNewTransactionAndSchemaContracts")
```

Add the remaining exact record families below. Each record is validated against
the live source before it can be emitted:

```cmake
list(APPEND EXPECTED_PUBLIC_WRITER_PATHS
  "CedarDatabase::Open|CedarDatabase::Open>TransactionCoordinator::Open>TransactionCoordinator::OpenInternal>CreateOrValidateDatabaseFormat|src/transaction/database_format.cc|format_bootstrap|OpenRunsRecoveryThroughTypedScheduler"
  "CedarDatabase::Open|CedarDatabase::Open>TransactionCoordinator::Open>TransactionCoordinator::OpenInternal>VersionSet::Open|src/storage/version_set.cc|manifest_recovery|OpenRunsRecoveryThroughTypedScheduler"
  "CedarDatabase::Open|CedarDatabase::Open>TransactionCoordinator::Open>TransactionCoordinator::OpenInternal>BlobStore::Open|src/blob/blob_store.cc|blob_recovery|OpenRunsRecoveryThroughTypedScheduler"
  "CedarDatabase::Open|CedarDatabase::Open>TransactionCoordinator::Open>TransactionCoordinator::OpenInternal>LogicalIdAllocator::Open|src/transaction/logical_id_allocator.cc|id_recovery|OpenRunsRecoveryThroughTypedScheduler"
  "CedarDatabase::Open|CedarDatabase::Open>TransactionCoordinator::Open>TransactionCoordinator::OpenInternal>StatsSnapshotStore::Open|src/statistics/stats_snapshot.cc|stats_recovery|OpenRunsRecoveryThroughTypedScheduler"
  "CedarDatabase::Open|CedarDatabase::Open>TransactionCoordinator::Open>TransactionCoordinator::OpenInternal>ShardPrepareLog::Open|src/transaction/decision_log.cc|prepare_recovery|OpenRunsRecoveryThroughTypedScheduler"
  "CedarDatabase::Open|CedarDatabase::Open>TransactionCoordinator::Open>TransactionCoordinator::OpenInternal>DecisionLog::Open|src/transaction/decision_log.cc|decision_recovery|OpenRunsRecoveryThroughTypedScheduler"
  "CedarDatabase::Open|CedarDatabase::Open>TransactionCoordinator::Open>TransactionCoordinator::OpenInternal>CommitTimeline::Open|src/transaction/commit_timeline.cc|timeline_recovery|OpenRunsRecoveryThroughTypedScheduler"
  "CedarDatabase::RegisterColumn|CedarDatabase::RegisterColumn>TransactionCoordinator::RegisterColumn>VersionSet::ApplyEditWithAdmission|src/storage/version_set.cc|schema_publish|CoordinatorPublishesSchemaOnlyThroughManifest"
  "CedarDatabase::RegisterIndex|CedarDatabase::RegisterIndex>TransactionCoordinator::RegisterIndex>IndexCatalog::RegisterIndex>VersionSet::ApplyEdit|src/storage/version_set.cc|index_register|IndexCatalogValidatesSchemaAndPublishesLifecycleEdits"
  "CedarDatabase::SetIndexState|CedarDatabase::SetIndexState>TransactionCoordinator::SetIndexState>IndexCatalog::SetIndexState>VersionSet::ApplyEdit|src/storage/version_set.cc|index_state|IndexCatalogValidatesSchemaAndPublishesLifecycleEdits"
  "CedarDatabase::RepairIndexes|CedarDatabase::RepairIndexes>TransactionCoordinator::RepairIndexes>WriteIndexSidecarFile|src/index/index_sidecar.cc|index_repair_write|RepairsCorruptIndexSidecarWhileQueriesRemainCorrect"
  "CedarDatabase::RepairIndexes|CedarDatabase::RepairIndexes>TransactionCoordinator::RepairIndexes>VersionSet::ApplyEdit|src/storage/version_set.cc|index_repair_publish|RepairsCorruptIndexSidecarWhileQueriesRemainCorrect"
  "CedarDatabase::DropIndex|CedarDatabase::DropIndex>TransactionCoordinator::DropIndex>IndexCatalog::DropIndex>VersionSet::ApplyEdit|src/storage/version_set.cc|index_drop_publish|DropIndexRetiresSidecarsAfterPinnedVersionSnapshotsRelease"
  "CedarDatabase::Delete|CedarDatabase::Delete>TransactionCoordinator::Commit>TransactionCoordinator::CommitInternal>ShardPrepareLog::Append|src/transaction/decision_log.cc|prepare_append|TypedFacadeUsesOnlyNewTransactionAndSchemaContracts"
  "CedarDatabase::Delete|CedarDatabase::Delete>TransactionCoordinator::Commit>TransactionCoordinator::CommitInternal>DecisionLog::AppendCommitWithResult|src/transaction/decision_log.cc|decision_append|TypedFacadeUsesOnlyNewTransactionAndSchemaContracts"
  "CedarDatabase::Delete|CedarDatabase::Delete>TransactionCoordinator::Commit>TransactionCoordinator::CommitInternal>CommitTimeline::AddDurableCommit|src/transaction/commit_timeline.cc|timeline_append|TypedFacadeUsesOnlyNewTransactionAndSchemaContracts"
  "TcypherSession::Commit|TcypherSession::Commit>TransactionCoordinator::CommitStrict>TransactionCoordinator::CommitInternal>DecisionLog::AppendCommitWithResult|src/transaction/decision_log.cc|strict_decision_append|TransactionSinkConvertsTypedMutationsAndCommitsAtomically"
  "TransactionSink::Submit|TransactionSink::Submit>TransactionCoordinator::Commit>TransactionCoordinator::CommitInternal>DecisionLog::AppendCommitWithResult|src/transaction/decision_log.cc|query_decision_append|TransactionSinkConvertsTypedMutationsAndCommitsAtomically"
  "CedarDatabase::Flush|CedarDatabase::Flush>TransactionCoordinator::Flush>FlushFrozenShard>WriteSstFile|src/columnar/sst.cc|sst_write|FlushesCommittedShardEventsIntoPartitionedSstFiles"
  "CedarDatabase::Flush|CedarDatabase::Flush>TransactionCoordinator::Flush>FlushFrozenShard>VersionSet::ApplyEdit|src/storage/version_set.cc|sst_publish|FlushesCommittedShardEventsIntoPartitionedSstFiles"
  "CedarDatabase::Compact|CedarDatabase::Compact>TransactionCoordinator::Compact>CompactSstPartition|src/storage/sst_compaction.cc|sst_compaction|CompactionPublishesOneOutputAndReclaimsReleasedInputs"
  "CedarDatabase::Compact|CedarDatabase::Compact>TransactionCoordinator::Compact>VersionSet::ApplyEdit|src/storage/version_set.cc|compaction_publish|CompactionPublishesOneOutputAndReclaimsReleasedInputs"
  "CedarDatabase::RotateBlobSegments|CedarDatabase::RotateBlobSegments>TransactionCoordinator::RotateBlobSegments>BlobStore::RotateActiveSegments|src/blob/blob_store.cc|blob_rotate|BlobRotationFaultRequiresReopenWithoutLosingManifestState"
  "CedarDatabase::RotateBlobSegments|CedarDatabase::RotateBlobSegments>TransactionCoordinator::ReconcileBlobSegments>VersionSet::ApplyEdit|src/storage/version_set.cc|blob_manifest_publish|ManifestOwnsBlobSegmentRotationAndRetirement"
  "CedarDatabase::CollectBlobGarbage|CedarDatabase::CollectBlobGarbage>TransactionCoordinator::CollectBlobGarbage>BlobStore::RelocateLiveHashes|src/blob/blob_store.cc|blob_relocate|BlobGcTombstonesDeadHashesAndReclaimsSealedSegments"
  "CedarDatabase::CollectBlobGarbage|CedarDatabase::CollectBlobGarbage>TransactionCoordinator::CollectBlobGarbage>BlobStore::DeleteRetiredSegments|src/blob/blob_store.cc|blob_reclaim|BlobGcTombstonesDeadHashesAndReclaimsSealedSegments"
  "CedarDatabase::Checkpoint|CedarDatabase::Checkpoint>TransactionCoordinator::CheckpointDurableLogs>TransactionCoordinator::CheckpointDurableLogsInternal>DecisionLog::CheckpointThrough|src/transaction/decision_log.cc|decision_checkpoint|CheckpointRetainsCommittedOutcomesAfterDecisionPrefixReclamation"
  "CedarDatabase::Checkpoint|CedarDatabase::Checkpoint>TransactionCoordinator::CheckpointDurableLogs>TransactionCoordinator::CheckpointDurableLogsInternal>CommitTimeline::Checkpoint|src/transaction/commit_timeline.cc|timeline_checkpoint|CheckpointRetainsCommittedOutcomesAfterDecisionPrefixReclamation"
  "CedarDatabase::Checkpoint|CedarDatabase::Checkpoint>TransactionCoordinator::CheckpointDurableLogs>TransactionCoordinator::CheckpointDurableLogsInternal>BlobStore::CheckpointIndex|src/blob/blob_store.cc|blob_checkpoint|CheckpointRetainsCommittedOutcomesAfterDecisionPrefixReclamation"
  "CedarDatabase::Close|CedarDatabase::Close>TransactionCoordinator::Flush>FlushFrozenShard>WriteSstFile|src/columnar/sst.cc|close_flush|CloseRunsTheRealProtocolAsTypedShutdownWork"
  "CedarDatabase::Close|CedarDatabase::Close>TransactionCoordinator::CheckpointDurableLogs>TransactionCoordinator::CheckpointDurableLogsInternal|src/transaction/transaction_coordinator.cc|close_checkpoint|CloseRunsTheRealProtocolAsTypedShutdownWork")
```

Append these two remaining `CedarDatabase::Put` records:

```cmake
list(APPEND EXPECTED_PUBLIC_WRITER_PATHS
  "CedarDatabase::Put|CedarDatabase::Put>TransactionCoordinator::Commit>TransactionCoordinator::CommitInternal>BlobStore::PutBatch|src/blob/blob_store.cc|blob_batch_write|CoordinatorExternalizesLargeBinaryBeforePrepareAndRestoresIt"
  "CedarDatabase::Put|CedarDatabase::Put>TransactionCoordinator::Commit>TransactionCoordinator::CommitInternal>ShardPrepareLog::Append|src/transaction/decision_log.cc|prepare_append|TypedFacadeUsesOnlyNewTransactionAndSchemaContracts")
```

- [ ] **Step 3: Add bounded source helpers**

Implement helpers that count exact regex matches, require each expected edge,
and append a normalized row only after validation:

```cmake
function(REQUIRE_EXACT_COUNT CONTENT REGEX EXPECTED LABEL)
  string(REGEX MATCHALL "${REGEX}" MATCHES "${CONTENT}")
  list(LENGTH MATCHES ACTUAL)
  if(NOT ACTUAL EQUAL EXPECTED)
    message(FATAL_ERROR "${LABEL} count drifted: expected ${EXPECTED}, found ${ACTUAL}")
  endif()
endfunction()

function(REQUIRE_SYMBOL_EDGE CONTENT CALLER CALLEE LABEL)
  string(REGEX MATCH "${CALLER}[^}]*${CALLEE}" EDGE "${CONTENT}")
  if(EDGE STREQUAL "")
    message(FATAL_ERROR "public writer edge drifted: ${LABEL}")
  endif()
endfunction()
```

Callers whose bodies contain nested braces must be checked with explicit
start/end anchors selected from the next fully qualified definition, rather
than a global `[^}]*` expression. Keep that bounded extraction in one helper so
the fixture and live source use identical logic.

- [ ] **Step 4: Detect unlisted public mutation entries and validate tests**

Extract fully qualified public definitions that invoke one of the reviewed
coordinator mutation methods. Compare the sorted result to
`EXPECTED_PUBLIC_DURABLE_ENTRIES`. For every allowed functional-test name,
require exactly one declaration while accepting both single-line and multiline
`TEST`/`TEST_F` formatting.

Error strings are fixed as:

```cmake
message(FATAL_ERROR "public durable-entry inventory drifted")
message(FATAL_ERROR "public writer edge drifted")
message(FATAL_ERROR "public writer functional-test binding drifted")
```

- [ ] **Step 5: Emit deterministic TSV and rerun the focused test**

Write the header and sorted rows into the temporary output directory:

```cmake
set(PUBLIC_WRITER_TSV
    "public_operation\tordered_symbols\tdurable_owner\tmutation_kind\tfunctional_test\n")
list(SORT VALIDATED_PUBLIC_WRITER_PATHS)
foreach(RECORD IN LISTS VALIDATED_PUBLIC_WRITER_PATHS)
  string(REPLACE "|" "\t" ROW "${RECORD}")
  string(APPEND PUBLIC_WRITER_TSV "${ROW}\n")
endforeach()
file(WRITE "${TEMP_OUTPUT}/public-writer-call-paths.tsv" "${PUBLIC_WRITER_TSV}")
```

Run:

```bash
ctest --test-dir build-current -j1 -R '^release_source_contract_negative$' --output-on-failure
```

Expected: writer positive and four writer negative cases PASS; trace output is
not yet required in this task.

### Task 3: Add the trace-producer/sink fixture and observe RED

**Files:**
- Modify: `tests/test_release_source_contract.cmake`
- Test: existing `release_source_contract_negative` CTest

**Interfaces:**
- Consumes: accepted writer fixture from Tasks 1-2.
- Produces: nine root sites, helper-owned terminal/error/correctness sites, one abandoned site, and reviewed sink sites.

- [ ] **Step 1: Add exact accepted trace sites to the fixture**

Transform the accepted database fixture so each root call is inside its live
enclosing symbol:

```cmake
file(READ "${FIXTURE_SOURCE}/src/db/cedar_database.cc" TRACE_BODY)
string(REPLACE
  "Status CedarDatabase::Put() { return coordinator_.Commit(); }"
  "Status CedarDatabase::Put() { auto trace = telemetry_->NewTrace(TracePriority::kNormal); Status s = coordinator_.Commit(); RecordOperationTrace(telemetry_, trace, \"transaction\", \"put\", 0, s); return s; }"
  TRACE_BODY "${TRACE_BODY}")
string(REPLACE
  "Status CedarDatabase::Delete() { return coordinator_.Commit(); }"
  "Status CedarDatabase::Delete() { auto trace = telemetry_->NewTrace(TracePriority::kNormal); Status s = coordinator_.Commit(); RecordOperationTrace(telemetry_, trace, \"transaction\", \"delete\", 0, s); return s; }"
  TRACE_BODY "${TRACE_BODY}")
foreach(ENTRY IN ITEMS
        "Flush|flush"
        "Compact|compaction"
        "RotateBlobSegments|blob_rotation"
        "CollectBlobGarbage|blob_gc")
  string(REPLACE "|" ";" PARTS "${ENTRY}")
  list(GET PARTS 0 METHOD)
  list(GET PARTS 1 TRACE_NAME)
  string(REGEX REPLACE
    "Status CedarDatabase::${METHOD}\\(\\) \\{ return ([^;]+); \\}"
    "Status CedarDatabase::${METHOD}() { auto trace = telemetry_->NewTrace(TracePriority::kNormal); Status s = \\1; RecordOperationTrace(telemetry_, trace, \"maintenance\", \"${TRACE_NAME}\", 0, s); return s; }"
    TRACE_BODY "${TRACE_BODY}")
endforeach()
string(REPLACE
  "Status CedarDatabase::Checkpoint() { Status s = coordinator_.Flush(); return s.ok() ? coordinator_.CheckpointDurableLogs() : s; }"
  "Status CedarDatabase::Checkpoint() { auto trace = telemetry_->NewTrace(TracePriority::kNormal); Status s = coordinator_.Flush(); if (s.ok()) s = coordinator_.CheckpointDurableLogs(); RecordOperationTrace(telemetry_, trace, \"maintenance\", \"checkpoint\", 0, s); return s; }"
  TRACE_BODY "${TRACE_BODY}")
string(APPEND TRACE_BODY
  "Status CedarDatabase::Get() { auto trace = telemetry_->NewTrace(TracePriority::kNormal); Status s; RecordOperationTrace(telemetry_, trace, \"point_read\", \"get\", 0, s); return s; }\n"
  "Status CedarDatabase::ExecuteTcypherWithSession(bool analytical) { auto trace = telemetry_->NewTrace(TracePriority::kNormal); const char* trace_name = analytical ? \"analytical_execute\" : \"interactive_execute\"; return Status::OK(); }\n")
file(WRITE "${FIXTURE_SOURCE}/src/db/cedar_database.cc" "${TRACE_BODY}")
```

Wrap those spellings in the same nine enclosing symbols as the live source and
add the exact helper-owned roles and sinks:

```cmake
APPEND_FIXTURE_FILE(
    "src/db/cedar_database.cc"
    "void RecordOperationTrace() { if (!trace.sampled) telemetry->NewTrace(TracePriority::kError); telemetry->RecordSpan(trace, priority, category, name, 0, 0, status); telemetry->RecordCorrectness(category + std::string(\"_failure\"), 1); }\n"
    "void TracedResultStream::Finish() { RecordOperationTrace(); }\n"
    "TracedResultStream::~TracedResultStream() { telemetry_->RecordSpan(trace_, TracePriority::kTail, \"query\", name_, 0, 0, \"ABANDONED\"); }\n"
    "const char* QueryTraceName(bool analytical) { return analytical ? \"analytical_execute\" : \"interactive_execute\"; }\n")
APPEND_FIXTURE_FILE(
    "src/observability/telemetry_aggregator.cc"
    "bool TelemetryAggregator::RecordSpan() { return input_.Push(); }\n"
    "bool TelemetryAggregator::RecordCorrectness() { return input_.Push(); }\n"
    "std::string TelemetryAggregator::ExportTracesJson() { return impl_->Export(); }\n")
APPEND_FIXTURE_FILE(
    "src/observability/event_ring.cc"
    "bool EventRing::Push() { return true; }\n")
```

- [ ] **Step 2: Require all new trace output files**

After the accepted scan, require:

```cmake
foreach(REQUIRED_OUTPUT IN ITEMS
        trace-producer-sites.tsv
        trace-sink-sites.tsv
        forbidden-unlisted-public-writer-edges.txt
        forbidden-unlisted-trace-producers.txt
        forbidden-duplicate-trace-producers.txt)
  if(NOT EXISTS "${FIXTURE_OUTPUT}/${REQUIRED_OUTPUT}")
    message(FATAL_ERROR "baseline output is missing ${REQUIRED_OUTPUT}")
  endif()
endforeach()
```

- [ ] **Step 3: Add six trace negative fixtures**

Add isolated fixture mutations for:

```cmake
APPEND_FIXTURE_FILE("src/db/cedar_database.cc"
                    "auto extra = telemetry_->NewTrace(TracePriority::kNormal);\n")
EXPECT_SCANNER_FAILURE("unlisted trace root"
                       "production trace producer inventory drifted")

APPEND_FIXTURE_FILE("src/db/cedar_database.cc"
                    "telemetry_->RecordSpan(trace, TracePriority::kNormal, \"maintenance\", \"flush\", 0, 0, \"OK\");\n")
EXPECT_SCANNER_FAILURE("duplicate trace producer"
                       "duplicate production trace producer")
```

Add the remaining four cases with exact mutations:

```cmake
PREPARE_FIXTURE()
APPEND_FIXTURE_FILE("src/db/cedar_database.cc"
                    "void bypass_span() { telemetry_->RecordSpan(trace, TracePriority::kNormal, \"query\", \"bypass\", 0, 0, \"OK\"); }\n")
EXPECT_SCANNER_FAILURE("direct span bypass"
                       "production trace producer inventory drifted")

PREPARE_FIXTURE()
APPEND_FIXTURE_FILE("src/db/cedar_database.cc"
                    "void bypass_correctness() { telemetry_->RecordCorrectness(\"bypass_failure\", 1); }\n")
EXPECT_SCANNER_FAILURE("direct correctness bypass"
                       "production trace producer inventory drifted")

PREPARE_FIXTURE()
file(READ "${FIXTURE_SOURCE}/src/db/cedar_database.cc" QUERY_BODY)
string(REPLACE
  "analytical ? \"analytical_execute\" : \"interactive_execute\""
  "analytical ? \"analytical_execute\" : \"batch_execute\""
  QUERY_BODY "${QUERY_BODY}")
file(WRITE "${FIXTURE_SOURCE}/src/db/cedar_database.cc" "${QUERY_BODY}")
EXPECT_SCANNER_FAILURE("third query trace name"
                       "query trace-name domain drifted")

PREPARE_FIXTURE()
WRITE_FIXTURE_FILE("src/observability/direct_trace_serializer.cc"
                   "std::string SerializeTraceDirectly() { return \"{}\"; }\n")
EXPECT_SCANNER_FAILURE("unreviewed trace sink"
                       "production trace sink inventory drifted")
```

Each call to `EXPECT_SCANNER_FAILURE` retains the accepted-output digest by its
existing before/after digest assertion.

- [ ] **Step 4: Run the focused test and confirm RED**

Run:

```bash
ctest --test-dir build-current -j1 -R '^release_source_contract_negative$' --output-on-failure
```

Expected: FAIL because the trace TSV/zero-hit outputs or trace drift rules are
not implemented.

### Task 4: Implement the exact trace inventory and source-contract schema 2

**Files:**
- Modify: `cmake/VerifyReleaseSourceContract.cmake`
- Test: `tests/test_release_source_contract.cmake`

**Interfaces:**
- Produces: `trace-producer-sites.tsv`, `trace-sink-sites.tsv`, three empty forbidden files, and `source-contract.json` schema version 2.

- [ ] **Step 1: Define the exact producer and sink records**

Use pipe-delimited internal records with fixed roles:

```cmake
set(EXPECTED_TRACE_PRODUCERS
    "CedarDatabase::Put|root|transaction|put|1"
    "CedarDatabase::Delete|root|transaction|delete|1"
    "CedarDatabase::Get|root|point_read|get|1"
    "CedarDatabase::ExecuteTcypherWithSession|root|query|interactive_execute|1"
    "CedarDatabase::ExecuteTcypherWithSession|root|query|analytical_execute|1"
    "CedarDatabase::Flush|root|maintenance|flush|1"
    "CedarDatabase::Compact|root|maintenance|compaction|1"
    "CedarDatabase::RotateBlobSegments|root|maintenance|blob_rotation|1"
    "CedarDatabase::CollectBlobGarbage|root|maintenance|blob_gc|1"
    "CedarDatabase::Checkpoint|root|maintenance|checkpoint|1"
    "RecordOperationTrace|error_resample|dynamic|dynamic|1"
    "RecordOperationTrace|terminal|dynamic|dynamic|1"
    "RecordOperationTrace|correctness|dynamic_failure|correctness|1"
    "TracedResultStream::~TracedResultStream|abandoned|query|dynamic|1")

set(EXPECTED_TRACE_SINKS
    "TelemetryAggregator::RecordSpan|src/observability/telemetry_aggregator.cc|span"
    "TelemetryAggregator::RecordCorrectness|src/observability/telemetry_aggregator.cc|correctness"
    "TelemetryAggregator::ExportTracesJson|src/observability/telemetry_aggregator.cc|serialize"
    "EventRing::Push|src/observability/event_ring.cc|queue")
```

The query source has one root creation site but two approved resolved names.
Record `expected_count=1` for the source site and validate the name-domain
conditional separately; do not count it as two `NewTrace` calls.

- [ ] **Step 2: Count all production producer API spellings**

Across `SCANNED_SOURCE_FILES`, count `NewTrace(`, `RecordSpan(`, and
`RecordCorrectness(` occurrences. Subtract only the reviewed method definitions
and forwarding sink definitions; compare the remaining call sites with the
exact producer inventory. Write every mismatch to the corresponding forbidden
file before failing.

Use one helper for deterministic source-location records:

```cmake
function(APPEND_MATCH_SITES RELATIVE_PATH CONTENT REGEX OUTPUT_VARIABLE)
  string(REGEX MATCHALL "${REGEX}" MATCHES "${CONTENT}")
  foreach(MATCHED IN LISTS MATCHES)
    string(SHA256 SITE_ID "${RELATIVE_PATH}:${MATCHED}")
    list(APPEND SITES "${RELATIVE_PATH}|${SITE_ID}")
  endforeach()
  set(${OUTPUT_VARIABLE} "${SITES}" PARENT_SCOPE)
endfunction()
```

The SHA-derived site id is evidence identity only; acceptance still depends on
the enclosing symbol, role, category, name, and exact count.

- [ ] **Step 3: Enforce duplicate tuples and query-name domain**

For fixed producer records, reject repeated `(role, category, name)` tuples.
Validate the live query conditional contains exactly
`interactive_execute` and `analytical_execute` and no third string assignment
to `trace_name` within `ExecuteTcypherWithSession`.

Write empty success files and nonempty diagnostic files on the temporary path:

```cmake
file(WRITE "${TEMP_OUTPUT}/forbidden-unlisted-public-writer-edges.txt"
     "${FORBIDDEN_PUBLIC_WRITER_EDGES}")
file(WRITE "${TEMP_OUTPUT}/forbidden-unlisted-trace-producers.txt"
     "${FORBIDDEN_TRACE_PRODUCERS}")
file(WRITE "${TEMP_OUTPUT}/forbidden-duplicate-trace-producers.txt"
     "${FORBIDDEN_DUPLICATE_TRACE_PRODUCERS}")
```

Fail before publication if any string is nonempty.

- [ ] **Step 4: Emit deterministic producer and sink TSV files**

Use headers:

```text
enclosing_symbol\trole\tcategory\tname\texpected_count
enclosing_symbol\tsource_file\tsink_role
```

Sort records lexicographically before replacing `|` with tabs and writing the
temporary files.

- [ ] **Step 5: Upgrade the JSON summary to schema 2**

Retain every r1 field and add:

```json
"schema_version": 2,
"public_writer_path_count": 0,
"trace_producer_record_count": 0,
"trace_sink_record_count": 0,
"forbidden_unlisted_public_writer_edge_count": 0,
"forbidden_unlisted_trace_producer_count": 0,
"forbidden_duplicate_trace_producer_count": 0
```

Populate counts from validated lists and include SHA-256 digests for all three
new TSV files and all three forbidden files. Continue hashing every scanned
input and every raw output before transactional publication.

- [ ] **Step 6: Run both focused gates and reach GREEN**

Run:

```bash
ctest --test-dir build-current -j1 -R '^release_source_contract(_negative)?$' --output-on-failure
```

Expected: `2/2` PASS, including all prior r1 negative fixtures and all ten new
writer/trace fixtures.

### Task 5: Run full and sanitizer verification

**Files:**
- Modify only if a test exposes a defect: scanner or focused harness above
- Verify: complete configured test matrices

**Interfaces:**
- Consumes: source-contract r2 implementation.
- Produces: fresh normal, ASAN, UBSAN, and TSAN logs suitable for the r2 evidence root.

- [ ] **Step 1: Run formatting and focused checks**

```bash
git diff --check
ctest --test-dir build-current -j1 -R '^release_source_contract(_negative)?$' --output-on-failure
```

Expected: formatting PASS and `2/2` focused PASS.

- [ ] **Step 2: Run the complete normal matrix**

```bash
cmake --build build-current -j1
ctest --test-dir build-current -j1 --output-on-failure
```

Expected: all discovered tests PASS. Record the discovered count from this
fresh run; do not reuse the previous `930/930` count as proof.

- [ ] **Step 3: Run focused sanitizer gates**

```bash
cmake --build build-asan -j1
ctest --test-dir build-asan -j1 -R '^release_source_contract(_negative)?$' --output-on-failure
cmake --build build-ubsan -j1
ctest --test-dir build-ubsan -j1 -R '^release_source_contract(_negative)?$' --output-on-failure
cmake --build build-tsan -j1
ctest --test-dir build-tsan -j1 -R '^release_source_contract(_negative)?$' --output-on-failure
```

Expected: `2/2` PASS in each configured sanitizer build. If a sanitizer build
directory is absent or configured incompatibly, reconfigure it with the same
project options used by its existing cache and record the exact command.

- [ ] **Step 4: Recheck accepted-output immutability**

Run the negative gate twice and hash the accepted output after each run:

```bash
ctest --test-dir build-current -j1 -R '^release_source_contract_negative$' --output-on-failure
find build-current/release-source-contract -type f -print0 | sort -z | xargs -0 shasum -a 256
ctest --test-dir build-current -j1 -R '^release_source_contract_negative$' --output-on-failure
find build-current/release-source-contract -type f -print0 | sort -z | xargs -0 shasum -a 256
```

Expected: both file sets and hashes are identical.

### Task 6: Create self-contained r2 evidence and update only proven rows

**Files:**
- Create: `results/release-closure-20260725-source-contract-r2/`
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `docs/superpowers/plans/2026-07-23-cedar-six-design-batched-final-closure-goal.md`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:**
- Evidence root reproduces accepted output from archived inputs only.
- Matrix changes prove only HTAP static public-writer mapping and Observability static duplicate-trace inventory.

- [ ] **Step 1: Assemble the evidence root without live-source dependencies**

Create these exact subdirectories with non-destructive `cmake -E`
operations: `inputs/`, `scanner/`, `tests/`, `outputs/`, and `logs/`. Copy every
path listed by `source-input-files.txt` into `inputs/` preserving repository
relative paths. Copy the scanner, focused harness, accepted output, RED log,
focused GREEN log, normal log, and three sanitizer logs.

- [ ] **Step 2: Reproduce from archived inputs**

Run:

```bash
cmake \
  -DSOURCE_ROOT=results/release-closure-20260725-source-contract-r2/inputs \
  -DOUTPUT_ROOT=results/release-closure-20260725-source-contract-r2/reproduced \
  -P results/release-closure-20260725-source-contract-r2/scanner/VerifyReleaseSourceContract.cmake
diff -ru \
  results/release-closure-20260725-source-contract-r2/outputs \
  results/release-closure-20260725-source-contract-r2/reproduced
```

Expected: scanner PASS and `diff` produces no output. Remove only the verified
`reproduced/` directory afterward using a precisely resolved evidence-root
path; do not touch other result roots.

- [ ] **Step 3: Write the manifest and exact ledger**

The manifest records source commit, dirty status, scanner schema 2, all exact
commands, input/output counts, public writer path count, trace producer/sink
counts, all six zero forbidden counts, normal/sanitizer results, and explicit
remaining production blockers. Generate `SHA256SUMS` over the exact sorted set
of all non-ledger regular files; reject symlinks and unexpected files first.

- [ ] **Step 4: Verify evidence bytes**

```bash
build-current/cedar_evidence_verify \
  results/release-closure-20260725-source-contract-r2
cd results/release-closure-20260725-source-contract-r2
shasum -a 256 -c SHA256SUMS
```

Expected: evidence verifier PASS and every ledger entry reports `OK`.

- [ ] **Step 5: Update the completion matrix conservatively**

Change HTAP completion row 1 from partial to proven only for the static
public-entry-to-writer mapping. Change Observability row 12 from partial to
proven only for the exact trace producer/sink and duplicate inventory. Preserve
the open production conflict, visible-prefix stall, crash-boundary,
multi-shard, workload activity, qualified-host, workstation/stress, and paired
baseline/release statements.

- [ ] **Step 6: Record progress and perform the final r2 audit**

Add the exact evidence identity, file counts, test counts, and remaining release
blockers to `.superpowers/sdd/progress.md` and the batched closure goal. Run:

```bash
git diff --check
git status --short --branch
```

Expected: `git diff --check` PASS; dirty user work remains preserved; no files
are staged; the overall six-item production goal remains open.
