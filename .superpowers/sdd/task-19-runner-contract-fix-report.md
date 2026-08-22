# Task 19 Runner Contract Fix Report

Date: 2026-08-22
Base: `4d40409`

## Changes

- Replaced Bash `read -a` CSV parsing with explicit comma splitting in
  `run_cedar_query_campaign.sh`. Leading, middle, and trailing empty fields
  are now preserved and rejected with exit 2, including `1,`, `,1`, and
  `1,,2`; the implementation remains compatible with Bash 3.2/macOS.
- Hardened the property-filter contract to capture and require a successful
  `execute_process()` result, then inspect `summary.csv` for an actual
  `cold-property-filter` row with `exit_code=0` and `hard_gate_pass=true`.
- Added contract coverage for trailing, leading, and middle malformed CSV
  components. Reopen unsupported behavior remains an explicit nonzero result;
  its contract assertion now checks the persisted `unsupported` summary row,
  matching the existing runner interface without changing user dirty reports.

## Verification

- `bash -n benchmarks/run_cedar_query_campaign.sh`: passed.
- Manual malformed checks for `1,`, `,1`, `1,,2`, and `,`: each returned exit
  2 with an empty/whitespace component diagnostic.
- `git diff --check`: passed.
- `ctest --test-dir build/query-debug --output-on-failure -R
  QueryCampaignOptionsContract`: passed (1/1, 160.02 seconds). This executed
  the real write/read benchmark contract and validated the property-filter
  hard-gate summary.

## Worktree Note

Pre-existing user modifications to `.superpowers/sdd/task-12-report.md`,
`.superpowers/sdd/task-18-groupfill-contract-fix-report.md`, and
`.superpowers/sdd/task-18-report.md` were left untouched.
