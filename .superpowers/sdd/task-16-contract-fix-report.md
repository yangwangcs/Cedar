# Task 16 Contract Fix Report

## Changes

- Upgraded `CSTATS-CURRENT` to a CRC-protected `CSC2` pointer carrying the
  generation, projection base sequence, statistics payload checksum, and
  projection-manifest checksum. Statistics are written and synced before the
  pointer replacement. `Load()` rejects a stale generation/base, payload
  checksum mismatch, or a manifest checksum mismatch when the manifest is
  present.
- Added a strict CDRSCR1 framing decoder. Cedar file inspection now validates
  scratch magic, query id, payload length, trailing bytes, and payload CRC and
  reports temporary authority plus query/payload coverage. Malformed scratch
  files remain visible with `checksum_valid=false`.
- Query profiles and global metrics now use the physical plan's selected
  operator (including filter/project/aggregate/sort/expand), and batch
  accounting includes decoded/physical bytes, pages, and interval fragments.
  Profile clocks remain gated by `capture_profile`.

## Verification

```text
cmake --build build/query-debug -j2 --target \
  test_query_observability test_cedar_files test_query_planner \
  test_query_types test_projection_store                    PASS
ctest --test-dir build/query-debug --output-on-failure \
  -R 'QueryObservability|CedarFiles|QueryPlanner|QueryTypes|ProjectionStore' \
  35/35 passed
./build/query-debug/tests/test_query_resources \
  --gtest_filter='QueryScratchTest.InspectionDecoder*'          PASS
git diff --check                                                PASS
```

The pre-existing user change `.superpowers/sdd/task-12-report.md` was left
untouched.
