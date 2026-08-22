# Task 19 Artifact Audit Duplicate-Header Fix Report

Date: 2026-08-22
Base: `41965cf`

## Fix

`audit_run_csv` now snapshots header validation status and reason, then carries
them into every data row. A duplicate or missing header field therefore remains
`FAIL` even when a subsequent row passes all row-level checks; the previous
per-row `invalid = 0` reset was removed.

The artifact-audit contract now includes a duplicate-header synthetic CSV and
requires the `reopen-verification` audit to return non-zero.

## Evidence Scope

The `reopen-verification` and `space-audit` phases are read-only audits of
benchmark artifact evidence. They inspect the benchmark's `reopen_verified` and
space-accounting fields; they do not reopen a database or create a database.
Step 7's real cross-database reopen remains a blocker and is not claimed as
implemented by this change.

## Verification

- `bash -n benchmarks/run_cedar_query_campaign.sh`: passed.
- `cmake -P tests/performance/test_query_artifact_audit.cmake`: passed,
  including duplicate-header, reopen-mismatch, and space-violation failures.
- Direct synthetic duplicate-header invocation: returned non-zero.
- `git diff --check`: passed.
