# Cedar Follow-ups Release Notes

Status: local release candidate (not pushed). This note covers the F11-F20
follow-ups plus the F14 retention milestone that landed after the meta sequence
v2 work.

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
  any legacy kind-7 `kSequence` record fails `Open` with an actionable
  `NotSupported` error telling the operator to migrate the database with the
  previous Cedar release (`cedar-meta-v2-reclaim`). v1 records are never
  decoded.
- **Option:** `FactStoreOptions::meta_sequence_format` and
  `DatabaseOptions::meta_sequence_format` remain for source compatibility, but
  only 2 (the default) is accepted. Value 1 → `InvalidArgument` stating that v1
  is no longer supported; any other value → `InvalidArgument`.
- **Removed compatibility machinery:** the `CEDAR_META_SEQUENCE_V1_ONLY` build
  option and `meta_sequence_compat.h` read gate, `tests/storage/test_meta_compat.cc`,
  `tests/storage/run_meta_compat_v1_only_ci.sh`, and the v1→v2 reclamation
  API/CLI/tests (`include/cedar/fact/meta_v2_reclaim.h`,
  `src/storage/rocks/meta_v2_reclaim.cc`, `tools/cedar-meta-v2-reclaim.cc`,
  `tests/storage/test_meta_v2_reclaim.cc`). The offline reclamation tool must
  come from the previous release because it has to read v1 records.
- **Migration path:** back up, open the database once with the previous
  release's default v2 writer (that records marker 2 without decoding the old
  records), run the previous release's `cedar-meta-v2-reclaim` offline to
  rewrite the v1 sequence records as v2, then open it with this release.
- **No downgrade:** there is no path back to v1, and v1 binaries cannot read a
  v2 database. Treat the move to v2 as irreversible.

See `docs/README.md` ("Meta Sequence v2 (The Only Supported Format)") for the
operator-facing contract and the migration procedure.

## Reopen fact validation now defaults to a bounded recent window (F18)

Cedar previously cross-checked **every** fact referenced by a committed
sequence during `FactStore::Open` (`deep_recovery_validation = true`). The
default is now the bounded policy:

- The structural checks over the durable sequence records are unchanged: Open
  still requires a contiguous `[max(retention_watermark, 1)..visible]` range,
  decodes every record, and validates every transaction outcome.
- Fact contents are cross-checked only for committed sequences at or above
  `max(oldest_readable, visible - recovery_validation_recent_sequences + 1)`.
  The production window is `K = 4096` committed sequences.
- Damage to a fact older than that window is no longer a reopen failure. It is
  detected when the fact is read, through the same missing/corrupt-value
  handling every read path already uses.
- Databases with at most `K` committed sequences still cross-check every
  retained fact, so the switch only changes large, long-lived databases.

Measured benefit (F9 report, 640k facts / 10k transactions, Debug):
reopen validation drops from 158.8 s to 64.9 s (2.45x), and the P1 fact Gets
drop from 640,000 to 262,144.

### Rollback

Set the deep policy back on:

```cpp
// Kernel API (new in F18).
cedar::DatabaseOptions options;
options.path = "/path/to/db";
options.deep_recovery_validation = true;   // full-history fact validation

// Storage API (FactStore).
cedar::FactStoreOptions store_options{"/path/to/db"};
store_options.deep_recovery_validation = true;
```

`recovery_validation_recent_sequences` (default `4096`, clamped to at least 1)
tunes the bounded window; it is ignored while `deep_recovery_validation` is
true. The option is a reopen **policy** only: it does not change the on-disk
format, the WAL, or any record encoding, so a database can be opened with
either policy at any time. The crash matrix continues to exercise both
policies.

## Other follow-up changes in this release

### F11 - bounded ScanFamily with prefix seeks

`FactStore::ScanFamily` is bounded again. It seeks each
`[version][partition][family]` range with a transient Cedar Parquet
prefix-seek key (internal marker `0x04`) and advances by encoded
`[version][partition+1]` seeks instead of decoding every fact key in the
column family. The facts memtable learned the same encoding, so WAL/memtable
and flushed Cedar Parquet SST reads return identical rows. The encoding is
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

### F16 - reclaim tool storage profiles (previous release only)

The offline `cedar-meta-v2-reclaim` tool accepts
`--profile=developer|kernel-test|debug-small|production` and
`--wal-dir=PATH`, and opens the database with the same resolved storage profile
and WAL placement as the production reader. This lets a database whose format
marker and v1 records are still WAL-resident be reclaimed offline. The tool is
not part of this release (it must read v1 records); migration uses the tool
from the previous Cedar release.

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
  at open with an actionable migration error; it is never decoded.
- Migration uses the previous Cedar release (default open records marker 2, then
  its `cedar-meta-v2-reclaim` rewrites the old records) and then this release.
  There is no downgrade path.
- `meta_sequence_format` remains for source compatibility but accepts only 2;
  value 1 is an `InvalidArgument`.
- Sequence-record retention is still opt-in and has the same one-way
  operational caveat as before.
- The remaining default behavior change is the F18 reopen validation policy;
  `deep_recovery_validation = true` restores full-history fact validation.
- No persistent key-encoding, WAL, or digest change outside the removed v1
  sequence-record encoding.

## Verification

Final gates on this tree (`cedar-final-debug`, `cedar-final-asan`,
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
  raw RocksDB and asserts the actionable rejection.
- Python contract tests (`python -m unittest discover -s benchmarks -p
  "test_*.py"` run from the workspace root): **101 tests, 94 passed + 7
  skipped**, exit 0.
