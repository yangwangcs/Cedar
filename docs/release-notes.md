# Cedar Follow-ups Release Notes

This note covers the F11-F20 follow-ups, the F14 retention milestone, and the
2026-10-23 storage-review corrections. Verification figures in the 2026-10-25 section describe the earlier release gates; the 2026-10-26 follow-up section re-measures the subsequent corrections on the current source fingerprint.

## Query correctness corrections (2026-10-23)

Physical plans are bound for each execution context. Prepared Cypher reuse now
requires the complete bound semantics, including literals, parameters and LIMIT.
Variable-length paths currently support minimum hop count 1 (`*1..N`). Other
minimums, including `*0..N` and `*2..N`, return `NotSupported` at binding rather
than returning results with an incorrect path length.

## Columnar Cache And Read Budgets (2026-10-24, validated 2026-10-25)

Canonical typed column pages share a strict cache; derived projection pages use
their own configured capacity. Zero projection capacity disables residency.
DB read working reservations and query leases cover page input, decompression,
transient payloads and active pins. New metrics distinguish page requests,
decodes, residency, pins, working limits and reader metadata. Filesystem request
bytes do not measure physical device reads.

`storage_read_working_bytes=0` resolves to
`max(4096, min(64 MiB, storage_memory_budget / 16))`. This additional pool counts
in production capacity validation; previously full configurations must reserve
room or choose an explicit smaller pool. The limit can reject large pages or
parallel reads even when the cache has room.

`storage_reader_metadata_bytes=0` now resolves to
`max(4096, min(1 GiB, storage_memory_budget / 16))` instead of the previous
64 MiB cap, so a production budget of 55 GiB reserves 1 GiB for the footers,
indexes and blooms of concurrently pinned L0 readers. Budgets below 16 GiB keep
the linear 1/16 share and the 1 MiB kernel-test profile still resolves to
64 KiB; an explicit nonzero value always wins. The automatic value is part of
the Open capacity preflight. Reader-metadata rejections are now reported as
metadata rather than read working, and a unit test asserts the two messages
differ.

Production-shaped validation (2026-10-25, source `91b5b8a3…`; the D1 formula is unchanged in later benchmark-only deltas): on a fixture with
3,309,568 facts (the row count of a production 512 MiB facts file) and a 16 GiB
budget, one pinned reader costs 16,206,198 B current / 18,369,016 B peak, and 36
simultaneously pinned readers peak at 585,585,946 B (54.5% of 1 GiB) with zero
metadata rejections and zero pin failures. Mapped by row count to 36 production
512 MiB L0 files (162.56 B/fact, about 3.30M facts per file), the requirement is
582,199,414 B current / 659,896,547 B peak (54.2% / 61.5% of 1 GiB). 64 readers
still fit (96.8%, no rejections); 70 readers hit the pool and return
`Cedar Parquet reader metadata budget exhausted`. The 1 GiB break-even is about
5.37M facts per file (peak); the synthetic fixture is far denser (48.1 B/fact)
than the production fixture (162.56 B/fact), so capacity reviews must use
rows/density rather than file bytes alone. No default was raised again. Budgets at or below 16 GiB use the linear
1/16 share (16 GiB resolves to exactly 1 GiB); larger budgets stay capped at
1 GiB. Smaller budgets have a tighter Open preflight because metadata is not
proportionally smaller (16 GiB: 96.33% used; 55 GiB: 88.3%).

### kernel-test 1 MiB metadata boundary (test profile)

The 1 MiB kernel-test profile keeps its published envelope and was not relaxed:
160 KiB reader metadata covers about seven reopened readers (six constructed L0
SSTs, peak 158900/163840 B, less than 5 KiB headroom); eight constructed SSTs
(nine readers) are deterministically rejected with the metadata message; eight
SSTs pass with 256 KiB; twelve exceed the 672 KiB read-pool envelope regardless
of how the metadata/working split is chosen. The maintenance test passes 20/20
repeated runs on the current code baseline.

Custom `CanonicalFactReader` implementations must accept the new optional
`std::shared_ptr<FactReadMemory>` argument on `ReadExact`. Existing calls can
omit it. The persisted benchmark runner and matrix retain fixed datasets,
checksums and per-run evidence; see `columnar-cache-and-deployment.md` for
commands and the limits of current Debug smoke observations. Range admission
remains lookup-only pending controlled mixed-load measurements.

`CanonicalFactReader::ReadExact` now returns `FactEventBatch` rather than a bare
vector; callers access `.events`. The batch is movable and cannot be copied,
and holds its query reservation until destruction, including after clearing its
rows while retaining vector capacity. Point results similarly carry a runtime
reservation owner. These owners are not part of persisted fact encoding.
Ordinary event and state streams also charge owned buffers and correction
workspace before allocation; batch row limits cap output rather than demanding
an eager allocation of every possible row.

`FactEvent` clears its owned value before releasing the runtime reservation.
It now has an explicit field-order constructor and defaulted copy/move
operations; existing positional brace initialization remains valid, but C++
designated aggregate initialization must use that constructor or field assignment.
Its encoded representation is unchanged.

## Legacy block-based facts tables are rejected on first access (2026-10-25)

To keep the bounded reader-metadata pool able to evict idle readers, Cedar now
opens facts SST readers on demand: version loading skips the eager
`LoadTableHandlersHelper` pass for the Cedar Parquet factory, and the table
cache does not pin file-metadata handles. This changes **when** a legacy
block-based facts table is rejected, not whether it is rejected:

- `DB::Open` no longer opens every facts SST, so a legacy block-based facts SST
  is no longer rejected at Open. The first `Get`, iterator or compaction that
  touches the file returns `Corruption("facts table is not Cedar Parquet v2")`.
- A non-empty legacy database still fails at the Cedar layer: `FactStore::Open`
  performs full retained-history validation by default and scans facts, so the
  rejection surfaces during recovery validation.
- Edge case: an **empty** legacy facts column family has no fact to scan, so
  `DB::Open` can succeed; the legacy file is rejected on first access.
- There is no silent-read path: legacy data is never decoded as Cedar Parquet.
  On-disk formats, WAL, MVCC visibility and digests are unchanged.

`CedarParquetFactTableFactoryTest.RejectsLegacyBlockBasedFactsTable` still asserted
the old Open-time rejection and therefore failed on the 2026-10-25 baseline (Debug
CTest #556). On 2026-10-26 the test expectation was corrected — test-only, no
product change: it now asserts that `DB::Open` succeeds, then that the first
`Get`, the iterator (`SeekToFirst`/`Seek`) return
`Corruption("facts table is not Cedar Parquet v2")`, and it destroys both
column-family handles before exit (the old `last_ref` abort no longer occurs).
The behavior contract is unchanged: legacy block-based facts SSTs are rejected on
first access, never decoded as Cedar Parquet, and an empty legacy facts column
family may still open successfully. The full Debug gate now reports 1245/1245
passed with 0 failed.

Detection-timing caveat for the explicit bounded policy: because version loading
no longer pre-opens Cedar SSTs, a corrupt or legacy facts SST can be reported on
first read rather than during `DB::Open`. With the default full retained-history
validation, a non-empty database is still scanned at `FactStore::Open`; under
`deep_recovery_validation = false` that scan is bounded, and read-time detection
of a corrupt/legacy SST outside the window is **not separately tested in this
wrap-up**.

## Meta sequence v2 is the only supported format (F20)

V2 support is now the sole sequence-record path; the v1 `kSequence` format is
removed from the codebase. This follows F19, which made v2 the default writer.

- **Writer:** every commit writes a compact `kSequenceV2` record (on-disk kind
  9). The v1 encoder (`EncodeSequenceRecord`), the format-selection parameter in
  `AppendCandidateToWriteBatch`, and the format-aware estimate plumbing are
  gone; `EstimateCommitBatchBytes` always reserves the v2 record.
- **Fresh databases:** the initialization batch writes the durable
  `format/sequence-record-format` marker with value 2 (`Sync=true`) together
  with the system identity and watermarks, before any commit is accepted. Every
  database written by this release is a v2 database.
- **Reopen:** the marker must be 2. A missing marker, a marker recording 1, or
  any legacy kind-7 `kSequence` record fails `Open` with a `NotSupported` error
  stating that v1 is unsupported and that migration/downgrade is not supported;
  the database must be deleted and recreated, or restored from a format-2
  backup. v1 records are never decoded.
- **Option:** `FactStoreOptions::meta_sequence_format` and
  `DatabaseOptions::meta_sequence_format` remain for source compatibility, but
  only 2 (the default) is accepted. Value 1 → `InvalidArgument` stating that v1
  is no longer supported; any other value → `InvalidArgument`.
- **Removed compatibility machinery:** the `CEDAR_META_SEQUENCE_V1_ONLY` build
  option and `meta_sequence_compat.h` read gate, `tests/storage/test_meta_compat.cc`,
  `tests/storage/run_meta_compat_v1_only_ci.sh`, and the v1→v2 reclamation
  API, CLI tool, and tests. This release has no migration tool and no downgrade
  path; delete and recreate a v1 database, or restore a format-2 backup.
- **Unsupported, not migratable:** v1 is no longer supported. There is no
  migration and no downgrade in this release, and v1 binaries cannot read a v2
  database. Delete and recreate a v1 database, or restore a format-2 backup.

See `docs/README.md` ("Meta Sequence v2 (The Only Supported Format)") for the
operator-facing contract.

## Reopen fact validation defaults to full retained history

Cedar cross-checks **every retained fact** referenced by a committed sequence
during `FactStore::Open` by default (`deep_recovery_validation = true`). The
bounded recent window is an explicit performance policy selected with
`deep_recovery_validation = false`:

- The structural checks over the durable sequence records are unchanged: Open
  still requires a contiguous `[max(retention_watermark, 1)..visible]` range,
  decodes every record, and validates every transaction outcome.
- Under the bounded policy, fact contents are cross-checked only for sequences at or above
  `max(oldest_readable, visible - recovery_validation_recent_sequences + 1)`.
  The production window is `K = 4096` committed sequences.
- A missing fact outside the bounded window can be accepted at reopen. Later
  reads cannot always distinguish missing committed records from valid absent
  facts. A missing DELETE or correction can silently expose an older value.
- Databases with at most `K` committed sequences still cross-check every
  retained fact, so the switch only changes large, long-lived databases.

Historical measured benefit of explicitly choosing bounded validation
(F9 report, 640k facts / 10k transactions, Debug):
reopen validation drops from 158.8 s to 64.9 s (2.45x), and the P1 fact Gets
drop from 640,000 to 262,144.

### Explicit bounded policy

Select the bounded policy when reopen latency is preferred over full retained
fact validation:

```cpp
// Kernel API (new in F18).
cedar::DatabaseOptions options;
options.path = "/path/to/db";
options.deep_recovery_validation = false;  // bounded recent-window validation

// Storage API (FactStore).
cedar::FactStoreOptions store_options{"/path/to/db"};
store_options.deep_recovery_validation = false;
```

`recovery_validation_recent_sequences` (default `4096`, clamped to at least 1)
tunes the bounded window; it is ignored while `deep_recovery_validation` is
true. The option is a reopen **policy** only: it does not change the on-disk
format, the WAL, or any record encoding, so a database can be opened with
either policy at any time. The crash matrix continues to exercise both
policies.

## Other follow-up changes in this release

### Storage-review corrections (2026-10-23)

- Family scans use canonical bytewise lower bounds so compaction into L1 or
  deeper levels does not hide facts during file selection.
- Sequence pruning advances one iterator across bounded delete batches rather
  than repeatedly scanning earlier tombstones. A new invocation still starts
  at the sequence prefix, preserving crash-retry behavior.
- Transaction resolution reads the outcome, watermarks, and sequence from one
  RocksDB snapshot. Concurrent pruning returns a consistent historical result
  or `SnapshotExpired`, rather than reporting normal reclamation as corruption.
- Idempotent commit retries below the sequence-retention boundary return
  `SnapshotExpired`, since the complete original result can no longer be checked.

### F11 - bounded ScanFamily with prefix seeks

`FactStore::ScanFamily` is bounded again. It seeks each
`[version][partition][family]` range with a bytewise lower-bound key padded to
the canonical fact-key length, and advances by encoded
`[version][partition+1]` seeks instead of decoding every fact key in the
column family. Targets stay in the canonical fact-key byte range, so MemTable,
SST file selection, and Parquet readers use the same ordering. The encoding is
only a seek target: persisted keys, sort keys, digests, and the file format are
unchanged, and legacy seek paths are untouched. This also restores a bounded
adjacency rebuild during `Database::Open`.

### F12 - adjacency rebuild observability and strict mode

The Open-time adjacency rebuild (P3) is no longer silent. It reports
`AdjacencyRebuildObservation{phase, records, duration_us, failed}` through
`DatabaseOptions::adjacency_rebuild_observer_for_testing`, logs a structured
warning when a failure is tolerated, and leaves the adjacency index empty
instead of partially built. `DatabaseOptions::strict_adjacency_rebuild`
(opt-in, default `false`) makes the same failure fail `Database::Open`. The
default open remains tolerant.

### F13 - commit batch estimate matches the v2 encoder

`EstimateCommitBatchBytes` reserves the sequence record the writer actually
emits: the compact v2 encoding (a 23-byte descriptor per fact). The former
format parameter and the v1 reservation path were removed with the v2-only
cleanup. A near-2 MiB v2 batch that an old v1-sized reservation pre-rejected is
admitted at the same cap.

### F14 - opt-in sequence-record retention

`DatabaseOptions::prune_sequence_records_on_vacuum` (default `false`) lets
`Database::Vacuum` persist the `watermark/sequence-retention` boundary and
delete older `sequence/*` records in idempotent, at-most-1024-key batches.
Reopen validation and `ReadSequence`/`ReadSequenceRange` treat history below
the durable boundary as intentionally absent (`SnapshotExpired`) while any gap
at or above it remains `Corruption`. The default keeps every sequence record
and the historical contiguous reopen contract. See `docs/README.md`
("Sequence-record retention") for the full contract and the one-way
operational caveat.

### F16 - reclaim tool storage profiles (removed with v1 support)

The F16 reclaim tooling accepted
`--profile=developer|kernel-test|debug-small|production` and
`--wal-dir=PATH`, and opened the database with the same resolved storage
profile and WAL placement as the production reader. It existed only in the
release that still read v1 records and is not part of this release: there is no
reclaim tool and no migration path for v1 databases. Delete and recreate the
database, or restore a format-2 backup.

### F17 - v1-only compatibility build (removed in F20)

The `-DCEDAR_META_SEQUENCE_V1_ONLY=ON` build option and its read gate were
removed together with v1 support. All binaries built from this tree are
v2-only; there is no compatibility profile that reads v1 or simulates a pre-v2
binary. The legacy-database rejection is covered by
`tests/storage/test_legacy_meta_sequence.cc` instead.

### F15 - measurement-only follow-up

F15 measures the full-scale compressed meta v2 space and the v2 write matrix.
It changes no product code; its evidence is reported in the F15 benchmark
report rather than in these release notes.

## Compatibility summary

- **Meta sequence v1 is no longer supported.** The sequence-record encoding is
  v2 (`kSequenceV2`, kind 9) for both writes and reads; the durable marker must
  be 2. A legacy v1 database (missing/1 marker or any kind-7 record) is rejected
  at open as unsupported; it is never decoded.
- v1 is unsupported: there is no migration tool and no downgrade path in this
  release. Delete and recreate a v1 database, or restore a format-2 backup.
- `meta_sequence_format` remains for source compatibility but accepts only 2;
  value 1 is an `InvalidArgument`.
- Sequence-record retention is still opt-in and has the same one-way
  operational caveat as before.
- Full retained-history reopen validation is the default. Set
  `deep_recovery_validation = false` to explicitly select the bounded policy.
- Legacy block-based facts SSTs are rejected on first access rather than at
  `DB::Open`; an empty legacy facts column family may open successfully. The
  regression test now asserts this behavior (`DB::Open` succeeds, first access
  returns `Corruption`). See "Legacy block-based facts tables are rejected on
  first access".
- No persistent key-encoding, WAL, or digest change outside the removed v1
  sequence-record encoding.

## Verification

Final-code-baseline results (2026-10-25, code fingerprint
`aa1eedb9d53b5f8abd5ba6315dd15a4af34eda85876058d2b5540ca98bbe8849`; the final
working tree adds only Markdown documents over that baseline, with no test
rerun — its authoritative fingerprint is recorded in
`_gates/final-acceptance-20261025/REPORT.md`) are recorded
in `columnar-cache-and-deployment.md` and `_gates/final-acceptance-20261025/REPORT.md`:
full Debug CTest: all **1245 registered cases now have a verdict** — the
1244-case W2c run passed **1243** with one accepted known failure (#556, legacy
facts rejection timing) in 680.42 s, and `CedarInstallConsumer` (#1245) **passed
in W2d in 209.15 s** after prewarming the `deb3d58b…` RocksDB cache; a first run
of the same source had one additional
load-sensitive timing flake (#1095) that passed 3/3 on retry and in the final
run. Directed ASan (278 tests: 276 passed, 2 timing-sensitive failures, zero
sanitizer/leak reports) and directed UBSan (278 tests: 277 passed, 1
timing-sensitive failure, zero reports) both passed; LSan and UBSan were shown
to be active with deliberate probes. C4 four-lane concurrency passed 10/10
intervals (mechanism-only, RelWithDebInfo runner), and the three-mount
named-volume / Linux-ext4-bind / overlay read-shape matrix passed 120/120 with
actual filesystem/label checks. `CedarInstallConsumer` **passed in W2d
(209.15 s)**; the earlier 1.5-2 h estimate reflected a missing cached library plus
internal `--parallel 1`, not a different fingerprint. Full sanitizer suites were
not run in the 2026-10-25 phase (directed set per D3); no production latency or
capacity conclusion is claimed. The 2026-10-26 follow-up completed them — see
below.

### Follow-up acceptance (2026-10-26)

Source fingerprint `3c771167dfce134c333e5513a2279dd2f171b97a6d8b67b6fbeb03ee42f4e961`
(147 files, 0 mismatches) on HEAD `8bef978`, nothing committed or pushed; the
authoritative work-tree fingerprint is recorded in
`_gates/final-acceptance-20261026/REPORT.md`. This phase closed the items the
2026-10-25 wrap-up left open. Every number below was re-measured on that source
fingerprint; the timeout-patch reruns reported in the ASan and Debug bullets add
a later sync fingerprint `45521691…` (same 147 files, only the Python test
scaffold plus three Markdown documents changed, so the C++ binaries are
identical).

- **#556 test expectation fixed** (test-only, see the section above). Full Debug
  CTest: **1245/1245 passed, 0 failed** (760.50 s; `CedarInstallConsumer`
  passed in 256.03 s with a warm RocksDB cache). A first run of the same source
  had one load-sensitive timing flake (#1095) that passed 3/3 when rerun
  single-threaded and in the final run; no assertion was changed or skipped.
- **Sanitizer-aware benchmark timeout** (test scaffolding only):
  `run_bench` in `tests/performance/test_columnar_cache_dataset.py` launched
  every benchmark subprocess with a hard-coded `timeout=90`, and one
  ASan-instrumented setup took about 94.8 s (write 11.29 s + flush 7.05 s +
  recovery validation 76.49 s), so the test failed deterministically under ASan.
  The new `bench_timeout_seconds()` keeps the historical 90 s default and
  returns 270 s (3x) when `ASAN_OPTIONS` or `UBSAN_OPTIONS` is set, with
  `CEDAR_TEST_TIMEOUT_SCALE` as an explicit override. No assertion, workload,
  flag, product code or CMake change (2 hunks, zero assertion lines). #1233 now
  passes: 146.02/153.22/151.08 s in the three full ASan runs, 116.46 s when run
  single-threaded, and 76.23/83.29/85.20 s in Debug (where the default 90 s
  budget still applies).
- **Full ASan after the timeout patch** (all 1245 registered cases;
  `ASAN_OPTIONS=detect_leaks=1:halt_on_error=1`, `LSAN_OPTIONS=report_objects=0`,
  `-j4`): three complete runs gave 1244/1245 (only #1096), 1243/1245 (#1096 and
  #1225) and a green **1245/1245, 0 failed, ctest exit 0 in 939.20 s**, with
  **zero** AddressSanitizer/LeakSanitizer reports in any ctest, LastTest, build,
  reconfigure or rerun log. The earlier pre-patch run on the same C++ binaries
  was 1243/1245 (1372.20 s) with #1095 and #1233. Every remaining failure is a
  non-sanitizer admission/timing flake on binaries this patch did not rebuild:
  #1096 (`append admission is under pressure` / reader-metadata budget; 3
  passed, 5 failed when rerun single-threaded, a pre-existing open item),
  #1225 (append admission deadline; 3/3 passed single-threaded), and — Debug
  only — #242 (free-space reserve race; 5/5 passed single-threaded). No
  assertion was changed or skipped.
- **Full Debug after the timeout patch** (`-j8`, same 1245 registered cases):
  three complete runs gave 1243/1245 (#242 and #1096), 1244/1245 (#1225) and a
  green **1245/1245, 0 failed, ctest exit 0 in 724.15 s**, including
  `CedarInstallConsumer` in 258.56 s. #242 is a free-space reserve race in
  `FailedDirectoryCreationReleasesFreeSpaceAdmission` (the test derives its
  reserve from `space().available` while parallel tests move data around; 5/5
  passed single-threaded); #1096 and #1225 are the admission flakes above (2/3
  and 3/3 single-threaded).
- **Full UBSan** (all 1245 cases; `UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`):
  **1245/1245 passed, 0 failed**, and no `runtime error:` or
  `UndefinedBehaviorSanitizer` report in any log. That run predates the timeout
  patch; each benchmark subprocess stayed below the historical 90 s budget there
  (UBSan instrumentation is cheaper than ASan), and the patch changes only
  Python test scaffolding, so the result stands for the same C++ binaries.
- **C3 read-shape matrix on the current source**: 40/40 runs per tier (small,
  medium, large; 5 repetitions x 4 workloads x 2 admission policies) in a
  dedicated 128 MiB, swap-off, 2-CPU container. Reducing file requests and
  column decodes depends on whether the working set fits in cache: the small
  tier removed all measured requests and decodes (p50 0.36-0.70x of
  lookup-only), medium/large point lookups reached zero requests/decodes
  (p50 0.33-0.35x, about 2.9-3.0x the operations per second), while
  narrow/wide/temporal scans got no cache hits, no material reduction and p50
  0.99-1.07x. Range scans therefore stay lookup-only. Every run is
  `cold_unverified` (cgroup read-byte increment was under 32 KiB across all 120
  runs, i.e. reads came from the host page cache), so these are mechanism and
  observation results, not production latency or capacity claims.
- **C4 four-lane concurrency on bind and overlay mounts**: 10/10 intervals
  accepted on each mount (20/20 total) in a 1 GiB, swap-off, 2-CPU container.
  All four lanes performed real work, facts compactions (10-13 per interval)
  overlapped the other three lanes for 2.99-3.44 s of each 10 s interval,
  `wal_sync_writes` were 42-51 with zero unsynced writes, file reads were
  82,391-128,176 and column decodes 76,604-115,325, and there were zero errors,
  zero OOM events and unchanged source fixtures. Mechanism-only: the profile is
  a debug-small-thresholds fixture profile, not production.
- **Bind mount through a Windows path** (operator label `windows-bind`): the
  read-shape matrix runs end-to-end (40/40, 714 checks, 0 failures) on a fixture
  created directly on that path, including write and advisory-lock probes. But
  `findmnt` reports `fstype=9p` with source `E:\[...` (Docker Desktop/WSL2
  drvfs over the Windows E: NTFS volume), so this column is **not** an NTFS
  semantics or performance result and is never mixed with the Linux mount
  numbers. Its mechanism matches C3: point-lookup admission removed all file
  requests/decodes, and scans showed no benefit and were slightly slower.
- **Still not verified**: production-profile capacity and mixed load (including
  C2), true device cold reads (every run is `cold_unverified`), NTFS semantics
  (only 9p was exercised), and multi-host/multi-container contention. No product
  default changed in this phase, and no production uplift percentage is claimed.


### Earlier release gates (historical, earlier source trees)

Earlier release gates (`cedar-final-debug`, `cedar-final-asan`,
`cedar-final-ubsan`):

- Debug: full `ctest` **1109/1109 passed** (814.3 s). The affected suites
  passed **182/182**, including `FactStoreMetadataTest` 38,
  `FactStoreCommitTest` 32, `KernelCommitTest` 11, `KernelSnapshotTest` 21,
  `KernelLifecycleTest` 17, `RecoveryCrashMatrixTest` 9,
  `CommitPublisherTest` 8, `VacuumTest` 16, `SequenceRetentionTest` 11, and
  the new `LegacyMetaSequenceTest` 5.
- The previous documented full gate was 1119 test cases; this change removes
  19 gtest cases (`MetaCompat` 6, `MetaV2Reclaim` 7, and six v1-only cases in
  the `test_fact_store_metadata` binary) and adds the 5 legacy-rejection cases,
  a change-local net of -14. The remaining difference from the old documented
  total comes from tests added between that tree and this HEAD.
- ASan (leak detection on): affected suites **182/182 passed**, with no
  AddressSanitizer or LeakSanitizer reports.
- UBSan (`halt_on_error=1`): affected suites **182/182 passed**, with no
  runtime errors.
- The v1-only compatibility build gate is gone with the removed
  `CEDAR_META_SEQUENCE_V1_ONLY` option; its role is covered by
  `LegacyMetaSequenceTest`, which builds legacy markers/kind-7 records through
  raw RocksDB and asserts that the rejection is `NotSupported`.
- Python contract tests (`python -m unittest discover -s benchmarks -p
  "test_*.py"` run from the workspace root): **101 tests, 94 passed + 7
  skipped**, exit 0.
