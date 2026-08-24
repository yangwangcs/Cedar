# Task 19 Evidence Report

Status: `DONE_WITH_CONCERNS`

## Scope and preservation

This task changed only generated evidence organization, the acceptance evidence
document, and this report. No Cedar source, benchmark threshold, or public
default was changed. Existing user-owned dirty reports were preserved. No
historical benchmark artifact was deleted or overwritten.

## Curated input

The full historical input had 930 raw `run.csv` files and 17 failed audit rows.
The failures were retained in their original directories and are not current
capability evidence. I copied the ten successful databases from
`build/query-release/evidence/mixed-sustained-final2/mixed-30-minute/` to:

`build/query-release/evidence/curated-successful/`

The curated input therefore contains exactly 10 `run.csv` files, each with an
existing database path and the successful mixed-run checksum/fact metadata.

## Reopen verification

Command:

```bash
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase reopen-verification \
  --input build/query-release/evidence/curated-successful \
  --output build/query-release/evidence/reopen-curated
```

Exit code: `0`.

`audit-summary.json`: `run_files=10`, `rows=10`, `failed_rows=0`, `pass=true`.
All ten rows have `terminal_status=OK`, `hard_gate_pass=true`,
`reopen_verified=true`. The command re-opened each database and verified the
stored facts/checksum. All rows reported `scratch_bytes=0`.

## Space audit

Command:

```bash
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase space-audit \
  --input build/query-release/evidence/curated-successful \
  --output build/query-release/evidence/space-curated
```

Exit code: `1` (expected for the current evidence, because the strict gate
fails closed).

`audit-summary.json`: `run_files=10`, `rows=10`, `failed_rows=10`, `pass=false`.
Every failure has the same reason:
`statistics_bytes exceeds 2% of derived bytes`.

Representative rows are approximately:

```text
authoritative_bytes=52,471,287
derived_bytes=17,552
statistics_bytes=16,850
scratch_bytes=0
space_amplification=0.000334507
```

The authoritative and derived-size bound passes, as does scratch cleanup and
database reopen. The failed ratio is caused by the `canonical-only` mixed
dataset: it has no material adjacency/property projection, so the statistics
block is close to the tiny derived metadata denominator. This is a real
design/evidence mismatch. The 2% threshold was not relaxed; a projected-data
campaign or reviewed accounting model is needed to resolve it.

## Sustained source evidence used

The source mixed campaign was:

```bash
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase mixed-30-minute \
  --duration-seconds 1800 --writers 32 --readers 32 \
  --facts-per-txn auto-turning-point \
  --input build/query-release/evidence/calibration-final \
  --output build/query-release/evidence/mixed-sustained-final2
```

Ten cases exited 0, with elapsed sum `2163.412 s`, facts/s range
`10029.3..17497.6`, and all case-level hard gates true. This evidence is
recorded in the acceptance document and is separate from the failed space
gate.

## Verification and concerns

- Acceptance document updated with exact commands, paths, counts, and the
  strict space failure.
- `git diff --check` passed after the documentation edits.
- Concern/blocker: strict space acceptance is not complete until a
  non-canonical projected dataset is audited or the statistics accounting
  denominator is explicitly redesigned and reviewed. No claim is made that
  the full Task 19 space gate passes.
