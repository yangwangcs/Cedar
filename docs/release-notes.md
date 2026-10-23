# Cedar Follow-ups Release Notes

Status: local release candidate (not pushed). This note covers the F11-F18
follow-ups plus the F14 retention milestone that landed after the meta sequence
v2 work.

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
`docs/README.md` ("Opt-in Meta Sequence v2 (One-Way Format Gate)").

### F15 - measurement-only follow-up

F15 measures the full-scale compressed meta v2 space and the v2 write matrix.
It changes no product code; its evidence is reported in the F15 benchmark
report rather than in these release notes.

## Compatibility summary

- No persistent format, key-encoding, WAL, or digest change.
- `meta_sequence_format` still defaults to v1; the v2 writer and its one-way
  gate are unchanged.
- Sequence-record retention is still opt-in and has the same one-way
  operational caveat as before.
- The only default change is the F18 reopen validation policy; the documented
  `deep_recovery_validation = true` option restores the previous behavior.

## Verification

Final gates on the merged tree (Debug container `cedar-final-debug`, ASan
`cedar-final-asan`, UBSan `cedar-final-ubsan`):

- Debug: full `ctest` **1115/1115 passed** (427 s), including
  `FactStoreMetadata` 49/49, `KernelLifecycle` 15/15,
  `RecoveryCrashMatrix` 9/9, `Vacuum` 16/16, `SequenceRetention` 11/11,
  `MetaCompat`, and `CedarInstallConsumer`.
- ASan (leak detection on): **100/100 passed** across those five
  reopen/recovery/retention/vacuum suites, with no AddressSanitizer or
  LeakSanitizer reports.
- UBSan (`halt_on_error=1`): **100/100 passed** across the same five suites,
  with no runtime errors.
- The final acceptance gate (t10) re-runs Debug, ASan, UBSan, and the Python
  contracts on the merged tree; its report is the authoritative evidence for
  this release.
