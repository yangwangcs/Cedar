# Task 13 Winner Fix Report

## Changes

- Reworked target witness selection into a global fixed-point queue. A target
  candidate rejected by an earlier winner is re-evaluated when that winner is
  replaced, while candidates that do not overlap the final winners remain
  independent results.
- Added a stable label-id fallback after interval-length, depth, and
  lexicographic edge-path ordering.
- Kept all shortest-depth target labels through frontier interval dominance so
  the global winner pass can reconsider a previously contained candidate.
- Added a regression for A=[0,100], C=[10,20], B=[20,200], asserting that C is
  re-accepted after B replaces A and the final paths are C and B.

## Verification

```text
cmake --build build/query-debug -j2 --target test_coexisting_path
Built target test_coexisting_path successfully.

ctest --test-dir build/query-debug --output-on-failure -R 'CoexistingPath'
100% tests passed, 0 tests failed out of 12
```
