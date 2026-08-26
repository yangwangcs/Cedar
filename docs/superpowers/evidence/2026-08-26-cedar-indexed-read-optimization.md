# Cedar Indexed Read Optimization Evidence

Date: 2026-08-26

## Verification

- Build: `cmake --build build-indexed-debug -j1`
- Tests: `ctest --test-dir build-indexed-debug --output-on-failure -j1`
- Result: 818/818 tests passed; total CTest time 158.20 s.

## Implemented Read-Side Contracts

- `VertexPoint` lowers to an exact canonical `ReadStateAt` point read. Its
  expected lookup cost is `O(1)` in the canonical key path.
- State-row `LIMIT K` uses a bounded state stream. It retains one identity
  chain and stops after `K` emitted rows, giving `O(K + scanned prefix)` work
  and bounded output memory instead of materializing all chains.
- Property postings are sorted by typed in-memory order and use binary-search
  bounds, giving `O(log N + k)` catalog lookup for `k` matching postings.
- Source-bound adjacency uses the existing immutable adjacency index and
  records an explicit seek in the query profile; missing generation coverage
  keeps the existing bounded canonical fallback.
- T-Cypher predicates are parsed and bound once, then lowered to Cedar's
  `BindVertexProperty` and `Where` operators. No second executor or write path
  is introduced.

## Scope and Caveats

The current change adds the typed posting/catalog contract and planner marker
for a complete `kPropertyIndex` projection, but does not claim a production
persisted property-index generation until a maintenance builder publishes one.
Performance claims therefore remain limited to the exact point read, bounded
state stream, typed catalog seek, and existing adjacency index paths. WAL,
commit ordering, recovery, MemTable insertion, and VersionSet were unchanged.
