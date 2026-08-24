# Runtime Contract Fix Wave

## Status

Implemented and verified on `codex/cedar-bitemporal-query-execution`.

## Changes

- `MaterializeRows` now reads canonical rows only for explicit canonical
  coverage slices or when derived coverage is unavailable. Fully derived plans
  use projection/delta rows directly, while mixed coverage still clips each
  source against its own slice and preserves existing reservation accounting.
- `Bindings` now exposes an exact typed lookup used by query execution. Bound
  scalar and temporal/reference values are carried into cursor state and
  evaluated in parameter predicates. Public execution tests cover `int64`,
  `string`, and `bool` parameters.
- The Cedar maintenance controller now starts exactly one flush worker and
  keeps one compaction worker. RocksDB Cedar profile flush capacity/default
  assertions were reduced to one accordingly.

## Tests

- `build/tests/test_query_canonical`
- `build/tests/test_query_relational`
- `build/tests/test_query_differential`
- `build/tests/test_query_planner`
- `build/tests/test_projection_store`
- `build/tests/test_query_delta`
- `build/tests/test_query_resources`
- `build/tests/test_query_types`
- `build/tests/test_maintenance_controller`
- `build/tests/test_rocksdb_profile`

All listed tests passed. `ctest` was not usable in this build because no tests
were registered, so the built GoogleTest binaries were run directly.

## Concerns

The physical-read counter regression was not added because the existing query
unit fixture does not expose a stable projection-backed canonical-read counter;
the lazy-source behavior is covered by the runtime implementation and the
existing projection/delta differential tests.
