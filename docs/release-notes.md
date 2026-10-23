# Cedar Follow-ups Release Notes

Status: local release candidate (not pushed). This note covers the F11-F19
follow-ups plus the F14 retention milestone that landed after the meta sequence
v2 work.

## Meta sequence v2 is now the default writer format (F19)

`FactStoreOptions::meta_sequence_format` and
`DatabaseOptions::meta_sequence_format` now default to **2**
(`kMetaSequenceFormatV2`). The format itself is unchanged; only which format a
fresh or still-v1 database writes by default changed.

- **New databases:** the first default `Open` writes the durable
  `format/sequence-record-format` marker with `Sync=true` before accepting any
  commit, then writes compact `kSequenceV2` records.
- **Existing v1 databases:** the same default `Open` upgrades them in place.
  The marker becomes 2, all later commits are v2, and the old v1 records remain
  readable through the dual-read paths. Reclamation
  (`cedar-meta-v2-reclaim`) can convert that v1 history forward.
- **Explicit v1 opt-out:** set `meta_sequence_format = 1` (storage or kernel
  API) to keep a database on v1. That writes no marker and keeps v1 record
  bytes identical to previous releases. It works only while the database has
  no v2 marker; after an upgrade the marker is authoritative and asking for 1
  is ignored.
- **One-way warning:** a default open is an upgrade. Binaries that predate
  dual-read support cannot open the database afterwards, and there is no
  downgrade path in this milestone. Back up before the first default open and
  finish the binary rollout first.
- The v1-only compatibility build (`CEDAR_META_SEQUENCE_V1_ONLY`) defaults to
  1 because it simulates a pre-v2 binary.

See `docs/README.md` ("Meta Sequence v2 (Default Writer Format, One-Way
Gate)") for the operator-facing contract.

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

### F13 - format-aware commit batch estimate

`FactStore::EstimateCommitBatchBytes` accepts the effective meta sequence
record format and mirrors the real encoder (`6 + 9` fewer reserved bytes per
fact for v2). The default argument keeps the v1 estimate byte-identical to
previous releases. A near-2 MiB v2 batch that the old v1 reservation
pre-rejected is now admitted, while v1 databases keep the old rejection
behavior at the same cap.

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

### F16 - reclaim tool storage profiles

`cedar-meta-v2-reclaim` accepts
`--profile=developer|kernel-test|debug-small|production`
and `--wal-dir=PATH`, and opens the database with the same resolved storage
profile and WAL placement as the production reader. This lets a database whose
format marker and v2 records are still WAL-resident be reclaimed offline.
The default (developer profile, no WAL override) is unchanged.

### F17 - v1-only compatibility build end-to-end

The build option `-DCEDAR_META_SEQUENCE_V1_ONLY=ON` compiles a binary that
behaves like a pre-v2 release: it rejects a database whose durable format
marker is 2 (or that contains a v2 record) with an actionable `Corruption`,
while still opening and committing to v1 databases. It also refuses to opt a
database into format 2. `test_meta_compat` and
`tests/storage/run_meta_compat_v1_only_ci.sh` exercise the gate end-to-end in
the normal and compatibility builds. The operational contract remains in
`docs/README.md` ("Meta Sequence v2 (Default Writer Format, One-Way Gate)").

### F15 - measurement-only follow-up

F15 measures the full-scale compressed meta v2 space and the v2 write matrix.
It changes no product code; its evidence is reported in the F15 benchmark
report rather than in these release notes.

## Compatibility summary

- No persistent format, key-encoding, WAL, or digest change.
- `meta_sequence_format` now defaults to v2 (F19); the v2 record encoding and
  its one-way gate are unchanged. Use `meta_sequence_format = 1` before an
  upgrade to opt out.
- Sequence-record retention is still opt-in and has the same one-way
  operational caveat as before.
- The default behavior changes are the F19 writer format and the F18 reopen
  validation policy. `meta_sequence_format = 1` and
  `deep_recovery_validation = true` restore the previous behavior where the
  database has not already been upgraded.

## Verification

Final gates on the merged tree (Debug container `cedar-final-debug`, ASan
`cedar-final-asan`, UBSan `cedar-final-ubsan`):

- Debug: full `ctest` **1119/1119 passed** (566.8 s), including
  `FactStoreMetadata` 51/51, `KernelLifecycle` 17/17,
  `RecoveryCrashMatrix` 9/9, `Vacuum` 16/16, `SequenceRetention` 11/11,
  `FactStoreCommit` 32/32, `MetaCompat` 6/6, `MetaV2Reclaim` 7/7, and
  `CedarInstallConsumer`.
- ASan (leak detection on): **149/149 passed** across `FactStoreMetadata`,
  `KernelLifecycle`, `RecoveryCrashMatrix`, `Vacuum`, `SequenceRetention`,
  `FactStoreCommit`, `MetaCompat`, and `MetaV2Reclaim`, with no
  AddressSanitizer or LeakSanitizer reports.
- UBSan (`halt_on_error=1`): **149/149 passed** across the same eight suites,
  with no runtime errors.
- v1-only compatibility build (`CEDAR_META_SEQUENCE_V1_ONLY=ON`):
  `MetaCompat` **5/5 passed**, including a default-option open of a v1
  database (the compatibility build defaults to the v1 writer).
- Python contract tests (`python -m unittest discover -s benchmarks -p
  "test_*.py"`): **101 tests, 94 passed + 7 skipped**, exit 0.
- The final acceptance gate (t10) re-runs Debug, ASan, UBSan, and the Python
  contracts on the merged tree; its report is the authoritative evidence for
  this release.
