# Cedar Kernel-Only Benchmark Design

## Decision

Cedar Kernel is the only supported execution profile. The benchmark must
always open Cedar with `production.kernel_mode = true`; no command-line flag,
public benchmark option, campaign branch, or test may select a Lean profile.

## Scope

- Remove the `BenchmarkExecutionProfile` type and its profile-name helper.
- Remove `--profile` parsing and CSV profile reporting.
- Make benchmark database options unconditionally enable Cedar Kernel mode.
- Replace the Lean/Kernel campaign matrix with Kernel-only warm, preflight,
  and sustained runs.
- Replace Lean-specific tests with assertions of the Kernel-only invariant.

## Non-Goals

- Do not change the Cedar public database API or storage ownership.
- Do not alter the single RocksDB WAL, recovery, MemTable, VersionSet, or
  MANIFEST ownership boundaries.
- Do not retain a compatibility alias for `--profile lean` or `--profile kernel`.

## Validation

- Benchmark option tests reject the removed `--profile` argument.
- Bounded benchmark tests verify Kernel mode is always enabled.
- The CSV contract reflects the removed profile field.
- Debug performance tests and the full Debug CTest suite pass.
