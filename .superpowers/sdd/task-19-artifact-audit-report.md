# Task 19 Artifact Audit Report

Date: 2026-08-22
Base: `08ddeda`

## Changes

- Implemented the `reopen-verification` and `space-audit` phases in
  `benchmarks/run_cedar_query_campaign.sh` as read-only artifact audits.
- Audits recursively discover `run.csv` files under `--input`, strictly validate
  CSV headers and row widths, reject missing/duplicate required fields and
  malformed values, and require successful exit (when recorded), hard gate,
  terminal status, and `reopen_verified` evidence.
- `space-audit` enforces zero scratch bytes, `space_amplification <= 1.5`, and
  statistics bytes no greater than 2% of derived/projection bytes. No database
  is opened or created by either audit phase.
- Each audit writes `audit-summary.csv` and `audit-summary.json`, and returns
  zero only when every discovered row passes.
- Added a lightweight synthetic contract covering passing artifacts, reopen
  mismatch, and space violations. The contract uses the benchmark's existing
  close/reopen evidence; it does not claim cross-database reopening.

## Verification

- `bash -n benchmarks/run_cedar_query_campaign.sh`: passed.
- `git diff --check`: passed.
- `cmake -P tests/performance/test_query_artifact_audit.cmake`: passed with
  synthetic passing, reopen-mismatch, and space-violation cases.

## Worktree Note

Pre-existing user modifications to `.superpowers/sdd/task-12-report.md`,
`.superpowers/sdd/task-18-groupfill-contract-fix-report.md`, and
`.superpowers/sdd/task-18-report.md` were left untouched.
