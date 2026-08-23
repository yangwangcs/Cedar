# Task: Canonical Multiple-Property Binding

## Implemented

- Canonical materialization now executes every chained property binding.
- Each binding intersects the current row's temporal interval with the
  property's effective interval and carries values by output `SlotId`.
- Missing values remain explicit optional entries and graph context is retained.
- Canonical analysis now recognizes an arbitrary chain of `BindProperty` nodes.
- Added vertex and edge regression cases using two properties with distinct
  values (`101/202` and `303/404`).

## Verification

- `cmake --build build/query-debug --target test_query_canonical -j2` passed
  before the final analyzer-chain adjustment.
- `git diff --check` passed for the runtime change.
- The shared test file's parallel partition change was corrected while running
  the suite; the canonical target now builds and executes successfully.
- Debug `test_query_canonical`: `21/21` passed.
- Debug `test_temporal_expand`: `10/10` passed.

## Commit

- `f75b99b Fix canonical multiple property bindings`
