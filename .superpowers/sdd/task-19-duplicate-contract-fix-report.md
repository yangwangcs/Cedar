# Task 19 Duplicate-Header Contract Fix Report

Date: 2026-08-22
Base: `ac2b2ff`

## Contract Fix

The duplicate-header fixture now supplies an 11-column data row matching its
11-column header. This ensures the audit fails because the header repeats
`dataset_checksum`, rather than because the row field count is malformed.

The contract now reads both duplicate audit summaries. It requires the CSV to
contain `FAIL` with the `duplicate header field: dataset_checksum` reason and
requires the JSON to report `pass:false` and `failed_rows:1`.

## Verification

- `cmake -D CEDAR_CAMPAIGN=/tmp/cedar-task19/benchmarks/run_cedar_query_campaign.sh -D CEDAR_BENCHMARK=/Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query/build/query-debug/cedar_query_bench -P /tmp/cedar-task19/tests/performance/test_query_artifact_audit.cmake`: passed.
- `bash -n benchmarks/run_cedar_query_campaign.sh`: passed.
- `git diff --check`: passed.
