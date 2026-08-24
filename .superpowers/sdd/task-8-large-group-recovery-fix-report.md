# Task 8 large-group recovery test fix

## Scope

Strengthened only `KernelGroupCommitFailureTest.ConcurrentIndeterminateRecoveryMatchesDurableAcceptance` and added the smallest required test-only control hook. No durability settings, production group-commit policy, or benchmark behavior changed.

The test uses a 32-transaction group with `sync=true` through the existing Cedar/RocksDB WAL path. It now proves that all 32 requests are queued before selection, form one 32-member WAL group, receive 32 durable async handles, report 32 durable acceptances, become indeterminate after the injected post-WAL fault, and resolve as committed after reopen.

## RED

Command:

```sh
cmake --build build-main-debug --target test_kernel_commit -j2 && \
ctest --test-dir build-main-debug \
  -R 'KernelGroupCommitFailureTest.ConcurrentIndeterminateRecoveryMatchesDurableAcceptance' \
  --output-on-failure
```

Result before the control hook, with the assertion intentionally requiring a group larger than the configured maximum to verify that the assertion observes group size:

```text
Expected equality of these values:
  metrics.group_fill.max_transactions
    Which is: 10
  kTransactions + 1
    Which is: 33
```

This also exposed the qualification gap: the former `>= 2` assertion could pass while the 32 concurrent submissions were split by the runtime's 200us collection limit.

## GREEN

Command:

```sh
cmake --build build-main-debug --target test_kernel_commit -j2 && \
ctest --test-dir build-main-debug \
  -R 'KernelGroupCommitFailureTest.ConcurrentIndeterminateRecoveryMatchesDurableAcceptance' \
  --repeat until-fail:10 --output-on-failure
```

Result:

```text
100% tests passed, 0 tests failed out of 1
Total Test time (real) = 2.08 sec
```

The one-time focused Debug invocation also passed in 0.18 sec.

## Implementation

`DatabaseOptions::append_commit_collection_observer_for_testing` is a test-only hook called after the append worker wakes and releases `append_commit_mutex`, before it selects an epoch. The target test waits in that hook until its existing enqueue observer has observed all 32 requests. Releasing the queue mutex is necessary: holding it would prevent the async executor from handing off the remaining requests.

The test asserts:

- `enqueued == 32`;
- `group_fill.total_transactions == 32` and `max_transactions == 32`;
- `durable_handle_count == 32` and `durably_accepted == 32`;
- all completion results are indeterminate under the injected post-WAL fault;
- every durable transaction resolves as committed after reopen.

## Concerns

The hook is intentionally test-only and unused by normal options, but it is an API-surface addition to make deterministic large-group qualification possible. The 2-second wait fails the test's exact group-size/enqueue assertions rather than hanging if the test infrastructure regresses. This focused change does not substitute for the separate crash-matrix, shutdown, ASAN, and sustained-release qualifications in the broader task plan.

## Commit

`a88fcd1 test: qualify 32-member WAL recovery group`

## Review corrections (2026-08-20)

Task review found three gaps in the original qualification. This follow-up changes
only the test-specific observer behavior and the targeted recovery test; it does
not change Cedar's group selection, durability policy, WAL ownership, recovery
ownership, or scheduler behavior.

### Durable identity and recovery coverage

The test no longer assumes that the 32 durable transaction IDs are `1..32`.
After all async handles have become durable, it now collects the actual IDs,
asserts that each is present and valid, and inserts their numeric values into a
set to prove uniqueness and exact cardinality. After reopen, it resolves exactly
those 32 IDs and requires each recovered outcome to be committed.

### One synchronous physical write

The existing `DatabaseOptions::commit_write_options_observer_for_testing` now
counts the physical Cedar/RocksDB epoch writes in this test. The test requires
exactly one observed write and rejects any `sync=false` observation. The injected
fault remains post-WAL, so the indeterminate public completion continues to test
the recovery boundary while the observer proves the group used one synchronous
write.

### Stop-state observer guard

`append_commit_collection_observer_for_testing` is a test-only hook. Before this
correction, an append worker awakened solely because `append_commit_stopping` was
set would invoke it before reaching the existing stop check. The worker now skips
the hook whenever the stop flag is set, including that wakeup path. Normal queued
request processing and all production paths are unchanged.

### RED

With the new stop-state regression test compiled but the stop guard intentionally
removed, the focused Debug test failed as expected:

```sh
cmake --build build-main-debug --target test_kernel_commit -j2 && \
ctest --test-dir build-main-debug \
  -R 'KernelGroupCommitFailureTest.CollectionObserverDoesNotRunAfterAppendPipelineStops' \
  --output-on-failure
```

```text
Expected equality of these values:
  collection_observations.load(std::memory_order_relaxed)
    Which is: 1
  0U
    Which is: 0
```

This demonstrates that the pre-existing wakeup path ran the hook after stop.

### GREEN

After restoring the minimal `!append_commit_stopping` guard, both the corrected
large-group recovery qualification and the new stop-state regression passed:

```sh
cmake --build build-main-debug --target test_kernel_commit -j2 && \
ctest --test-dir build-main-debug \
  -R 'KernelGroupCommitFailureTest.(ConcurrentIndeterminateRecoveryMatchesDurableAcceptance|CollectionObserverDoesNotRunAfterAppendPipelineStops)' \
  --output-on-failure
```

```text
100% tests passed, 0 tests failed out of 2
Total Test time (real) = 0.26 sec
```
