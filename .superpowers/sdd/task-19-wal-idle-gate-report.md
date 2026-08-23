# Task 19 WAL/Idle Gate Report

## Status

Implemented the campaign metric propagation and focused hard-gate contract.

## Changes

- Added `wal_sync_p99_us` to `summary.csv` and `summary.jsonl` rows.
- Added average WAL-sync p99 to `write-idle-overhead.csv`.
- Added a fail-closed idle query overhead comparator: a comparable baseline is
  required, facts/s must remain at least 97% of baseline, and WAL-sync p99 must
  remain at most 105% of baseline. Malformed or missing metrics fail the gate.
- Preserved active projection thresholds of at least 90% facts/s and at most
  115% end-to-end p99; no WAL metric was added to that active gate.
- Extended `QueryCampaignOptionsContract` with CSV/artifact schema assertions
  and a short deterministic threshold-failure case.

## Verification

- `bash -n benchmarks/run_cedar_query_campaign.sh`
- `git diff --check`
- Focused `QueryCampaignOptionsContract` run after commit (pass).

## Scope/concerns

Only the campaign runner and its focused contract test are included in this
change. Existing user-owned dirty report files remain unstaged.
