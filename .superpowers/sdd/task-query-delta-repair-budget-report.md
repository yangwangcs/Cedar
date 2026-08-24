# QueryDelta Recovery Repair Budget

## Status

Implemented on `codex/cedar-bitemporal-query-execution`.

## Change

`Database::Open` now passes `options.query_runtime.query_delta_bytes` to
`QueryDelta::RepairThrough` as `QueryDeltaRepairLimits::max_bytes`. The value
is taken after `ResolveQueryRuntimeOptions`, so debug profiles retain their
resolved hard budget while developer and production profiles use their
explicit configured budget. The prior hard-coded 512 MiB recovery cap is gone.

A test-only budget observer and lifecycle regression cover a reopen with
`query_delta_bytes = 1`, verifying that recovery repair receives the configured
small cap rather than the old 512 MiB default.

## Verification

Command:

`cmake --build build --target test_kernel_lifecycle -j4`

`build/tests/test_kernel_lifecycle --gtest_filter=KernelLifecycleTest.RecoveryRepairUsesConfiguredQueryDeltaByteBudget`

Result: build succeeded; the focused test passed (`1/1`).
