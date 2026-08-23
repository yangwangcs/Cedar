# Task 19 WAL/Idle Gate Report

## Status

Implemented the campaign metric propagation and focused hard-gate contract,
including the follow-up edge-case fixes from review.

## Changes

- Added `wal_sync_p99_us` to `summary.csv` and `summary.jsonl` rows.
- Added average WAL-sync p99 to `write-idle-overhead.csv`.
- Added a fail-closed idle query overhead comparator: a comparable baseline is
  required, facts/s must remain at least 97% of baseline, and WAL-sync p99 must
  remain at most 105% of baseline. Malformed or missing metrics fail the gate.
- Standalone idle runs now materialize a deterministic baseline from a complete
  five-repeat aggregate as same-run calibration only; this is not independent
  query-overhead capability evidence. Explicit input baselines remain strictly
  validated.
- Enforced exactly five successful samples for every facts/writers point and
  rejected missing/extra matrix keys and missing WAL columns in raw run CSVs.
- Kept audit summary CSV/JSON rows on the same metric schema, using explicit
  zero/null values for audit-only fields.
- Encoded comma-separated matrix metadata with `|` in the key/value baseline
  artifact so facts/writer comparability checks cannot be truncated by CSV
  parsing.
- Preserved active projection thresholds of at least 90% facts/s and at most
  115% end-to-end p99; no WAL metric was added to that active gate.
- Extended `QueryCampaignOptionsContract` with CSV/artifact schema assertions
  and a short deterministic threshold-failure case.

## Verification

- `bash -n benchmarks/run_cedar_query_campaign.sh`
- `git diff --check`
- Focused contract passed on the preceding follow-up commit; a final rerun is
  still required after the metadata-only commit `1f73df2`.

## Scope/concerns

Only the campaign runner and its focused contract test are included in this
change. Existing user-owned dirty report files remain unstaged.
