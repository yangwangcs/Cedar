# Task 19 Real Reopen Verification Report

## Implemented

- Added typed `--verify-existing=true --expected-facts=N --expected-checksum=N`
  options. Existing verification requires both expected values and refuses a
  missing/nonexistent database path without deleting it.
- Added a database-level verification path that scans the existing database,
  closes it, reopens it, scans again, compares both scans to the expected fact
  count/checksum, and reports `InspectStorageFiles` byte accounting in the
  normal CSV/JSON result.
- Normal benchmark runs retain their clean-path behavior and now publish a
  final canonical `ScanFactChecksum` answer, so `facts` and `dataset_checksum`
  in artifacts are reopen-verifiable rather than the writer's incremental XOR.
- Campaign artifact audits execute a real verify-existing command for every
  artifact carrying `raw_sample_path`, `facts`, and `dataset_checksum`. The
  space phase additionally checks the command's actual scratch/derived bytes
  and amplification. Legacy schema-only synthetic fixtures remain supported.
- Added parser and real workload reopen/mismatch tests and registered the
  workload test target in CMake.

## Verification

```text
ctest --test-dir build/query-debug --output-on-failure -R 'QueryBench\\.'
  options: 12 discovered cases passed
  workload: VerifiesExistingDatabaseAcrossReopen passed

benchmarks/run_cedar_query_campaign.sh --phase reopen-verification
  real database artifact: PASS, exit 0, reopen_verified=true

benchmarks/run_cedar_query_campaign.sh --phase space-audit
  real database artifact: FAIL as expected at space_amplification=5.91 (>1.5)
```

Existing user-owned dirty report files were left untouched.
