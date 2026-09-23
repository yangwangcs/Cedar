# Cedar 32x8 Patricia Full-chain Acceptance

Status: **lifecycle, WAL, bidirectional compatibility, ASan, and TSan acceptance passed** for the frozen 32x8 pure Patricia implementation. The user explicitly accepted the existing Tier-0 performance measurements; the earlier eight-writer CV miss remains an accepted host-scheduling residual risk and is not restated as a passing performance gate. No representation or performance mechanism was added during this acceptance.

## Frozen Artifacts

- Production Patricia implementation is frozen at `b39a303`; later commits in this validation range change tests and evidence only.
- Final fixture commit: `ff3fbcfb3de9f825412eddea59261200d7a36404`.
- Old compatibility producer/consumer production revision: `76adf448257aaaa4eacac8ba5451ba38a51223f3`.
- Fixture source is byte-identical in both production trees, SHA-256 `cd926dbc233e7dd2a2f88df167a234d9f73b9a1dad837e06ad414f30e57ebf1d`.
- The old tree contains the fixture commit only; its production sources remain at the old revision.

## Lifecycle and Recovery

The fixture commits two values, then a delete of the first value. The canonical raw facts history is three entries at commit sequence 3. Verification checks point visibility (deleted fact absent, surviving fact present), decodes and validates all three key/value records, and compares complete forward and reverse iteration against hand-specified history order.

- `MarkReadOnly` and repeated `PrepareForFlush`: the all-256 byte-39 fixture preserves active/frozen forward and reverse scans, hashes, and entry bytes. Repeated preparation is idempotent; a ready frozen scan performs no additional branch loads after its initial seek.
- Flush/SST and restart: the lifecycle characterization stores a `Put` at commit sequence 8, a Cedar logical `Delete` at sequence 9 for that same fact, and a surviving fact at sequence 10. It performs a real facts-CF flush with `wait=true`, verifies Cedar Parquet `PAR1`, closes/reopens, and checks all three point entries plus full forward/reverse iteration.
- WAL/restart: a child process commits then exits with `_exit` so destructors cannot flush. Before recovery there is a WAL log and no SST; reopen reconstructs the same three-record state.
- Debug focused matrix: 67/67 passed across Patricia/freeze (45), RocksDB lifecycle (12), crash recovery (6), and format recovery (4). The corrected three-history SST test's negative control failed at the expected count assertion. Fixture CLI contract passed 1/1; stability contract passed 1/1.
- Independent Patricia abstract model and publication-interleaving model both passed. Each negative control found its expected counterexample.

## Old/New Format Compatibility

Four fresh-directory cross-binary directions were executed: old SST to current, current SST to old, old unflushed WAL to current, and current unflushed WAL to old. All producer and consumer invocations returned zero. Both SST directions produced a live facts file whose first four bytes are `PAR1`; both WAL directions had one `.log` and zero `.sst` files before consumer recovery. All four consumers returned exactly the same result:

```json
{"count":3,"commit_seq":3,"forward_order_hash":7899729986448698037,"forward_value_hash":13091264218047945013,"reverse_order_hash":16471980483761807157,"reverse_value_hash":6628625391539774829}
```

Machine-readable per-direction identities and file checks are in `compatibility.json`; exact build and run commands are in `commands.txt`.

## Sanitizers

- AddressSanitizer: all 67 focused tests passed with `ASAN_OPTIONS=halt_on_error=1`; no ASan report.
- ThreadSanitizer: CTest reported 67/67 successful entries, with 66 executed and passed and one expected conditional skip (`ReopenRecoversActualProcessCrashesAtCommitBoundaries`); no TSan race report. The test explicitly skips because TSan cannot create threads after a multithreaded fork.
- macOS AppleClang's LeakSanitizer runtime does not support `detect_leaks=1` and aborts before test bodies. Re-running with leak detection disabled passed; therefore address errors and races were checked, but LeakSanitizer coverage is unavailable on this host.

## Requirement Audit

| Requirement | Evidence | Result |
| --- | --- | --- |
| Fixed-width 40-byte key; byte 39 remains ordered | `FortyByteBytePatriciaPathPreservesOrderAndBounds`, `RepeatedFreezePreservesAllFinalByteEntriesAndScanHashes`, and fixed 32x8 source at `b39a303` | Pass |
| Immutable published Patricia nodes/blocks; CAS-only publication | Frozen production source and both deterministic publication models/negative controls | Pass |
| One-writer Release performance | Accepted Tier-0 matrix described in stability report | Pass under user's accepted measurement |
| Four-writer Release performance | Accepted Tier-0 matrix described in stability report | Pass under user's accepted measurement |
| Eight-writer ratio for every seed | Tier-0: seed 20260920 is 0.8540x; remaining seeds are 0.7728-0.8349x | Accepted residual; 20260920 is over 0.85 threshold |
| Eight-writer CV for all five seeds | Tier-0 CVs: 8.74%, 11.89%, 5.50%, 5.65%, 5.18% | Accepted host-variance residual; all exceed original 5% threshold |
| Arena at 1/4/8 writers | Accepted Tier-0 matrix reports all Arena gates passed | Pass under user's accepted measurement |
| `MarkReadOnly` and repeated `PrepareForFlush` | Byte-39 active/frozen bidirectional hash characterization; TSan/ASan focused suite | Pass |
| Facts Flush emits Cedar Parquet SST | Real `wait=true` flush; live facts SST `PAR1` | Pass |
| SST restart preserves point reads and forward/reverse history | Corrected Put/Delete/survivor lifecycle test plus fixture | Pass |
| Unflushed WAL replay after process exit | `_exit` writer fixture and existing lifecycle replay test; WAL present and no SST before recovery | Pass |
| Old SST -> current and current SST -> old | Independent binaries; `compatibility.json` | Pass |
| Old WAL -> current and current WAL -> old | Independent binaries; `compatibility.json` | Pass |
| AddressSanitizer | 67/67 focused suite | Pass; LeakSanitizer unavailable on this macOS runtime |
| ThreadSanitizer | 66 executed and passed, 1 explicitly skipped process-crash test | Pass with documented skip |

## Final Review and Residuals

The plan's final whole-branch review was performed as a read-only self-review (the current execution policy prohibited delegation). The review covered the added fixture and CMake registration against Task 7, corrected Task 6 lifecycle tests, the compatibility contract, and the frozen production implementation. The fixture validates real persisted records rather than source-text properties; iterator and column-family lifetimes are closed in RocksDB order. The review caught and fixed a lifecycle-test gap: it now verifies persisted Cedar `Put` and `Delete` history rather than a RocksDB tombstone. No Critical or Important issue remains. The branch-wide range includes 2080 changed paths, mostly preserved performance evidence; production review concentrated on the frozen Patricia and relevant RocksDB memtable/flush interfaces. Historical generated evidence rows were not individually re-reviewed.

Accepted residuals: eight-writer timing CV remains above the original 5% experimental threshold on this host as documented in the stability report and accepted by the user; macOS LeakSanitizer is unavailable; one TSan process-crash test is conditionally skipped. None changes the demonstrated SST/WAL format compatibility or logical recovery results.

## Freeze Decision

Final audit outcome: **the 32x8 production design is fixed and the requested persistence acceptance is complete**. The accepted performance residual is preserved verbatim as a residual, not hidden by averaging or relabeled as a gate pass. All requested freeze, flush, restart, WAL replay, and two-way old/current format behaviors have direct test or cross-binary evidence above. Production index, MemTableRep integration, WAL, and SST implementation files remain unchanged after freeze commit `b39a303`; validation commits add tests and evidence only. The only uncommitted worktree paths at final audit are the three pre-existing user-owned benchmark paths named in the plan.
