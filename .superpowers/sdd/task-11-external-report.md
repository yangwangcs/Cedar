# Task 11 External Spill Report

## Scope

Implemented the missing external spill paths for non-temporal hash and sort-merge joins without changing public operator signatures or Task 10 contracts.

## Implementation

- Added binary length-framed row serialization/deserialization covering all `RelationalScalar` variants and optional effective intervals.
- Hash joins now partition both inputs into verified `QueryScratch` runs, read one partition at a time, and execute the bounded in-memory hash core for inner, semi, and anti joins.
- Sort-merge joins now partition both inputs into verified scratch runs, read and sort each partition, merge the partition results, and preserve sorted output for inner, semi, and anti joins.
- Removed the previous full-input `IndexNestedLoopJoin` fallback from hash and sort spill branches.
- Added forced-small-reservation coverage comparing external results to canonical joins and validating every scratch run through `ReadRun` and cleanup.

## Verification

```text
cmake --build build/query-debug -j2 --target test_query_relational test_query_resources
ctest --test-dir build/query-debug --output-on-failure -R 'QueryRelational|QueryResource'
49/49 tests passed
```
