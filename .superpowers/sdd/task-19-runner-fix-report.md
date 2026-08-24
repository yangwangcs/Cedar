# Task 19 Runner Fix Report

Date: 2026-08-22

## Changes

- `run_cedar_query_campaign.sh` now rejects missing option values with exit 2,
  rejects empty CSV components and zero-case campaigns, and validates CSV
  components without shell word splitting or glob expansion.
- Release calibration runs each requested facts-per-transaction point and
  writes an auditable `turning-point.json`. `auto-turning-point` reads that
  artifact from `--input` or `--output` and fails explicitly when it is absent
  or invalid. Release and mixed commands pass one numeric value to the
  benchmark, never a CSV.
- Cold and warm matrices include `property-filter`.
- Idle runs persist an overhead baseline beside their output. Active runs use
  that baseline (or an explicit input baseline) and fail the campaign when the
  throughput/p99 overhead gate cannot be compared or does not pass.
- Reopen and space phases require an input directory containing run artifacts,
  record the supplied input, and fail explicitly because the current benchmark
  has no cross-artifact reopen/space-audit interface. They do not create new
  databases.
- `QueryCampaignOptionsContract` covers artifact absence, property-filter,
  malformed CSV, missing option values, and unsupported cross-artifact phases.

## Verification

- `bash -n benchmarks/run_cedar_query_campaign.sh`: passed.
- `git diff --check`: passed.
- Manual shell checks: empty CSV returned exit 2; unsupported CSV returned exit
  2; missing `--output` value returned exit 2; auto turning point without an
  artifact returned exit 2; release calibration with `4,16` produced commands
  containing `--facts-per-txn=4` and `--facts-per-txn=16` and wrote a valid
  turning-point artifact.
- `ctest --test-dir build/query-debug --output-on-failure -R
  QueryCampaignOptionsContract` was started but interrupted because the
  contract launches multiple real benchmark matrices; no passing result is
  claimed from that run.
