# Cedar Public-Writer and Trace-Producer Contract Design

Date: 2026-07-25
Status: Approved design; implementation not started
Scope: HTAP public-entry-to-durable-writer mapping and Observability trace-producer inventory

## 1. Purpose

The existing release source contract proves the exact set of production files
that may mutate the filesystem, publish Manifest edits, delete persistent data,
or expose retained `*Stats` views. It does not yet prove two narrower claims:

1. every supported public durable-mutation entry reaches only reviewed durable
   writer boundaries through an explicitly reviewed call path; and
2. every production trace producer is part of one exact inventory, so a new or
   duplicate producer cannot silently bypass the observability design.

This design extends the existing fail-closed source-contract scanner. It does
not change production runtime behavior and it does not substitute for the
production-scale concurrency, crash, saturation, or paired-performance gates.

## 2. Chosen approach

The scanner will own two checked-in, exact inventories and will emit their
validated forms as release evidence:

- `public-writer-call-paths.tsv`
- `trace-producer-sites.tsv`

The inventories are implementation contracts rather than hand-written claims.
The scanner derives the relevant source facts, compares them with the exact
expected sets, validates referenced tests, and fails closed on drift. The
accepted output remains transactionally published using the existing checked
rollback and stranded-backup recovery protocol.

Rejected alternatives are:

- a Clang AST call graph, because it adds compiler-version and compilation
  database dependencies to evidence regeneration; and
- runtime writer-audit hooks, because they would modify every persistence
  component and add production overhead and failure surface.

## 3. Source scope and trust boundary

The extension uses the same source root, symlink containment checks, supported
source extensions, authoritative design inputs, SHA-256 input binding, and
unknown-file fail-closed behavior as source-contract r1.

The scanner is deliberately syntactic. It proves exact reviewed call edges and
producer sites in the current source layout. It does not claim whole-program
semantic reachability, dynamic dispatch resolution, or production workload
coverage. Those broader properties remain separate runtime gates.

## 4. Public durable-mutation entry inventory

The initial exact public-entry set is:

| Public entry | Mutation class |
| --- | --- |
| `CedarDatabase::Open` | recovery/bootstrap publication |
| `CedarDatabase::Close` | flush and durable checkpoint |
| `CedarDatabase::RegisterColumn` | schema Manifest publication |
| `CedarDatabase::RegisterIndex` | index-catalog Manifest publication |
| `CedarDatabase::SetIndexState` | index-state Manifest publication |
| `CedarDatabase::RepairIndexes` | sidecar creation and Manifest publication |
| `CedarDatabase::DropIndex` | Manifest publication and deferred sidecar deletion |
| `CedarDatabase::Put` | transactional Blob/log/timeline mutation |
| `CedarDatabase::Delete` | transactional log/timeline mutation |
| `TcypherSession::Commit` | snapshot or strict transactional mutation |
| `TransactionSink::Submit` | vectorized query mutation submission |
| `CedarDatabase::Flush` | SST creation and Manifest publication |
| `CedarDatabase::Compact` | SST/sidecar creation, publication, reclamation |
| `CedarDatabase::RotateBlobSegments` | Blob segment/index mutation |
| `CedarDatabase::CollectBlobGarbage` | Blob relocation, publication, reclamation |
| `CedarDatabase::Checkpoint` | flush and durable-log/timeline checkpoint |

`CedarDatabase::Get`, query execution without commit, metrics/traces export,
statistics accessors, session begin/stage/read/rollback, and lifecycle accessors
are not durable-mutation entries. The scanner must reject any new public method
that directly invokes a reviewed mutation coordinator method unless that method
is added to the exact entry inventory.

## 5. Public-writer call-path record

Each TSV row represents one reviewed ordered path from a public entry to one
durable owner. The columns are:

1. `public_operation`
2. `ordered_symbols`
3. `durable_owner`
4. `mutation_kind`
5. `functional_test`

`ordered_symbols` uses `>` between fully qualified symbols. A public operation
may have multiple rows when it reaches multiple independent durability
boundaries. The durable owner must belong to the existing exact database
durable-writer inventory.

The initial path families are:

- transactional commit: public entry > `TransactionCoordinator::Commit` or
  `CommitStrict` > `CommitInternal` > conditional `BlobStore::PutBatch`,
  `ShardPrepareLog::Append`, `DecisionLog::AppendCommitWithResult`, and
  `CommitTimeline::AddDurableCommit`;
- schema and index lifecycle: public entry > the corresponding coordinator or
  `IndexCatalog` method > `VersionSet::ApplyEdit` or
  `ApplyEditWithAdmission`, including sidecar writer and reviewed deletion
  paths where applicable;
- flush: public entry > `TransactionCoordinator::Flush` > the SST flush path >
  `WriteSstFile` > `VersionSet::ApplyEdit`;
- compaction: public entry > `TransactionCoordinator::Compact` >
  `CompactSstPartition` > SST/sidecar writer > `VersionSet::ApplyEdit` and
  reviewed reclamation;
- Blob lifecycle: public entry > coordinator Blob rotation/GC > Blob append or
  relocation/index mutation > Manifest publication or reviewed deletion;
- checkpoint and close: public entry > `TransactionCoordinator::Flush` and
  `CheckpointDurableLogs` > durable log/timeline checkpoint owners; and
- open: public entry > `TransactionCoordinator::Open` > recovery/bootstrap
  owners that may create, replace, truncate, or publish durable state.

The exact implementation rows must be derived from current source and must not
collapse conditional boundaries into a false unconditional claim.

## 6. Functional-test binding

Every call-path row names one existing test that executes the public operation
and asserts the relevant durable effect or recovery invariant. The scanner
validates the exact `TEST` or `TEST_F` name in `tests/test_correctness_kernel.cc`.

The exact initial allowed binding pool is:

- `OpenRunsRecoveryThroughTypedScheduler`
- `CloseRunsTheRealProtocolAsTypedShutdownWork`
- `TypedFacadeUsesOnlyNewTransactionAndSchemaContracts`
- `TransactionSinkConvertsTypedMutationsAndCommitsAtomically`
- `CoordinatorExternalizesLargeBinaryBeforePrepareAndRestoresIt`
- `CoordinatorPublishesSchemaOnlyThroughManifest`
- `PublicationFaultMatrixCleansUnmanifestedSstAndSidecarOutputs`
- `IndexCatalogValidatesSchemaAndPublishesLifecycleEdits`
- `RepairsCorruptIndexSidecarWhileQueriesRemainCorrect`
- `DropIndexRetiresSidecarsAfterPinnedVersionSnapshotsRelease`
- `FlushesCommittedShardEventsIntoPartitionedSstFiles`
- `CompactionPublishesOneOutputAndReclaimsReleasedInputs`
- `BlobRotationFaultRequiresReopenWithoutLosingManifestState`
- `BlobGcTombstonesDeadHashesAndReclaimsSealedSegments`
- `ManifestOwnsBlobSegmentRotationAndRetirement`
- `CheckpointRetainsCommittedOutcomesAfterDecisionPrefixReclamation`
- `IndexLifecycleCompactsRepairsDropsAndReopensWithoutIdReuse`
- `RetainedScheduledQueryStreamIsCancelledAfterDatabaseDestruction`

Adding or replacing a test binding requires an intentional change to this
allowed set; the scanner must not accept arbitrary test names merely because a
same-named test exists.

A test-name reference is supporting coverage, not proof of production scale.
The release evidence must continue to distinguish static ownership, focused
functional execution, and production-scale runtime evidence.

## 7. Trace-producer inventory

The trace inventory covers production sources only. Test-only trace creation is
excluded from the expected production count.

Each TSV row contains:

1. `enclosing_symbol`
2. `role`
3. `category`
4. `name`
5. `expected_count`

Allowed roles are:

- `root`: creates the operation trace context;
- `terminal`: records the terminal span exactly once;
- `abandoned`: records the tail span when a query stream is destroyed before
  reaching a terminal result;
- `error_resample`: creates an error-priority trace when the original trace was
  not sampled; and
- `correctness`: emits the failure correctness event.

The initial production baseline contains nine root creation sites in
`src/db/cedar_database.cc`:

- transaction `put`;
- transaction `delete`;
- point-read `get`;
- query execution, whose name is exactly one of `interactive_execute` and
  `analytical_execute`;
- maintenance `flush`;
- maintenance `compaction`;
- maintenance `blob_rotation`;
- maintenance `blob_gc`; and
- maintenance `checkpoint`.

`RecordOperationTrace` owns terminal span recording, error resampling, and the
`<category>_failure` correctness event. `TracedResultStream::Finish` is the
single query terminal path, and `TracedResultStream::~TracedResultStream` is the
single `ABANDONED` path. `TelemetryAggregator` and `EventRing` remain the only
production sink/serialization path and are inventoried as sinks, not operation
producers.

## 8. Duplicate and drift rules

The scanner fails when any of the following is true:

- the exact public durable-entry set changes;
- an expected public entry or ordered call edge disappears;
- a reviewed durable owner is referenced by no valid path or an unreviewed
  durable owner is referenced;
- a referenced functional test does not exist exactly once;
- a production `NewTrace`, `RecordSpan`, or `RecordCorrectness` producer site
  appears outside the expected inventory;
- an expected producer disappears or its count changes;
- two operation producers claim the same fixed `(role, category, name)` tuple;
- a fixed operation uses a category/name pair different from its inventory;
- the dynamic query name can produce a value outside the two approved names;
  or
- a sink is misclassified as an operation producer, or a new sink bypasses the
  reviewed `TelemetryAggregator`/`EventRing` path.

For the query site's two approved runtime names, uniqueness is evaluated per
resolved name rather than treating the source site's conditional expression as
a third name.

## 9. Scanner outputs

On success, source-contract r2 emits all r1 outputs plus:

- `public-writer-call-paths.tsv`
- `trace-producer-sites.tsv`
- `trace-sink-sites.tsv`
- `forbidden-unlisted-public-writer-edges.txt`
- `forbidden-unlisted-trace-producers.txt`
- `forbidden-duplicate-trace-producers.txt`

The three `forbidden-*` files must be present and empty. The JSON contract
summary records schema version 2, row counts, forbidden counts, the complete
input-file digest set, and the SHA-256 digest of every emitted inventory.

A failed scan must leave the last accepted output byte-for-byte unchanged.

## 10. Negative fixtures

The focused CMake regression must first demonstrate the accepted baseline and
then prove fail-closed behavior for at least:

1. a new unlisted public durable-mutation method;
2. a new unlisted edge from an existing public entry to a durable owner;
3. a missing expected call edge;
4. a call-path row naming a nonexistent functional test;
5. a new production root trace site;
6. a duplicate fixed `(role, category, name)` producer;
7. a new direct `RecordSpan` producer that bypasses the helper;
8. a new direct correctness producer;
9. a query trace name outside the approved two-value set; and
10. an unreviewed trace sink or serializer.

Every negative run verifies that the accepted output digest is unchanged. The
existing stranded-backup recovery and injected-publication-failure regressions
remain mandatory.

## 11. Evidence root

The implementation will create
`results/release-closure-20260725-source-contract-r2/` containing:

- a byte snapshot of every scanned input;
- the exact scanner and focused regression scripts;
- all scanner outputs;
- focused and sanitizer logs;
- a machine-readable manifest; and
- `SHA256SUMS` covering the exact non-ledger file set.

The archived scanner must reproduce the archived outputs from the archived
inputs without reading the live workspace. `cedar_evidence_verify` and
`shasum -a 256 -c SHA256SUMS` must both pass.

## 12. Verification and acceptance

Implementation acceptance requires:

1. TDD evidence showing the new focused checks fail before scanner support;
2. the focused positive and all negative fixtures pass;
3. the complete normal CTest matrix passes with `-j1`;
4. focused ASAN, UBSAN, and TSAN source-contract gates pass with `-j1`;
5. `git diff --check` passes;
6. the archived-input reproduction passes;
7. the evidence verifier and exact SHA ledger pass; and
8. the six-design matrix is updated conservatively:
   - HTAP public-entry-to-writer static mapping becomes proven; and
   - Observability duplicate-trace static inventory becomes proven.

The matrix must continue to leave production concurrency, crash-boundary,
workstation/stress, qualified-host, and paired-performance rows open until
their own production-scale evidence exists.

## 13. Non-goals

This stage does not:

- claim that Cedar is production-ready;
- execute the qualified-host production campaign;
- replace crash injection or serializability stress;
- alter trace sampling, labels, runtime serialization, or persistence code;
- add paper or external LDBC requirements; or
- stage, commit, push, reset, clean, or otherwise rewrite unrelated user work.
