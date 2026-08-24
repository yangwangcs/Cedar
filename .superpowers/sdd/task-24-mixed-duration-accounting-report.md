# Task 24: Mixed Duration Accounting Report

## Status

Implemented the mixed campaign duration accounting fix. Each mixed operation
runs one timed write phase and one timed query phase, so the requested campaign
window is divided by ten operations and two timed phases. For
`--duration-seconds=1800`, every mixed command now receives
`--duration-seconds=90`.

The existing ten mixed operations and bounded admission controls remain
unchanged (`commit-deadline-us=5000000`, `group-queue-requests=2048`, and
`group-queue-bytes=33554432`).

## Tests

- `bash -n benchmarks/run_cedar_query_campaign.sh` passed.
- Focused CMake campaign contract passed:
  `tests/performance/test_query_campaign_options.cmake` now uses a stub
  benchmark to inspect the 1,800-second manifest without running a sustained
  campaign. It found ten `--duration-seconds=90` entries and all admission
  flags.
- Short mixed campaign passed with `--duration-seconds=1`, ten operations,
  exit code `0`, and ten manifest entries using `--duration-seconds=1`.

## Concerns

The short campaign only verifies accounting and exit behavior. It is not
evidence for the 1,800-second sustained gate, and no 30-minute campaign was
run.
