# Task 24 Fix Report

## Changes

- Replaced the campaign contract's CMake 3.19-only `file(CHMOD)` call with
  `execute_process(COMMAND chmod +x ...)`, and directly executed the stub to
  assert that it is runnable.
- Mixed campaigns now require at least 20 seconds (ten operations times two
  timed phases). Per-case duration uses integer floor allocation, so the
  sustained 1,800-second campaign remains 90 seconds per case and the valid
  20-second short campaign uses one second per case.
- Updated mixed short contract invocations and asserted all ten one-second
  manifest entries.

## Verification

- `bash -n benchmarks/run_cedar_query_campaign.sh` passed.
- `git diff --check` passed.
- `ctest --test-dir build/query-release -R '^QueryCampaignOptionsContract$' --output-on-failure` passed (1/1, 213.71s).
- Short mixed campaign with `--duration-seconds 20`, one writer/reader, degree
  1, selectivity 1, and facts-per-txn 1 passed all ten cases (exit 0); its
  manifest contained ten `--duration-seconds=1` entries.
- A mixed campaign with `--duration-seconds 19` was rejected with exit 2 and
  `--duration-seconds must be at least 20 for mixed-30-minute (10 operations x 2 timed phases)`.

No sustained campaign was run.
