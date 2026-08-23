# Task 21 QueryDelta Contention Report

## Diagnosis

Task 20 reported the active projection gate at 18,715.4 facts/s versus an
idle baseline of 41,812.4 facts/s, with end-to-end p99 of 167,142 us versus
29,783.2 us. The append path in `src/kernel/database.cc` builds immutable
`published_delta` descriptors while holding `append_commit_mutex`, releases
that bookkeeping lock, and then calls `QueryDelta::EnqueuePublished`.

Before this change, `EnqueuePublished` and `QueryDelta::WorkerMain` both used
`QueryDelta::mutex_`. `WorkerMain` held that mutex while executing
`IndexLocked`, including descriptor normalization, vector copies, hash-chain
updates, edge-index updates, and memory accounting. Consequently the append
worker could block in `EnqueuePublished` behind the indexing worker after WAL
publication, delaying completion of the current epoch and admission of the
next epoch. This is the direct contention path; the WAL write itself is not
called while the QueryDelta mutex is held.

## Fix

- Added `queue_mutex_` for the published descriptor mailbox and enqueue
  sequence. `EnqueuePublished` now validates and queues without waiting for
  `IndexLocked`.
- `WorkerMain` pops a descriptor under `queue_mutex_`, releases it, then takes
  the state mutex only for indexing.
- Made visible and missing sequence metadata atomic so enqueue does not need to
  take the indexing state mutex for those publication markers.
- Lifecycle operations acquire queue then state locks, preserving the worker's
  lock order and preventing reset/repair from racing a popped descriptor.
- Added a test-only worker gate and a regression test that holds the worker in
  its index critical section and verifies a second enqueue completes promptly.

## Verification

Focused regression and existing QueryDelta tests:

```text
cmake --build build/query-release --target test_query_delta -j4
Built target test_query_delta
build/query-release/tests/test_query_delta
[==========] 12 tests from 2 test suites ran.
[  PASSED  ] 12 tests.
```

Lifecycle coverage:

```text
cmake --build build/query-release --target test_query_lifecycle test_query_delta -j4
build/query-release/tests/test_query_lifecycle
[==========] 5 tests from 1 test suite ran.
[  PASSED  ] 5 tests.
```

The new regression is `QueryDeltaTest.EnqueueDoesNotWaitForWorkerIndexLock`.
It would deadlock/time out at the enqueue completion assertion when enqueue
and indexing share the old mutex; with the split mailbox lock it passes.

A local five-repeat f512/w8 campaign after the fix produced:

```text
idle mean:   103520.6 facts/s, p99 90000.0 us
active mean:  65715.9 facts/s, p99 169496.8 us
```

The run was performed while other benchmark campaigns were active, so it is
not a replacement for Task 20's stable matrix. It confirms a large throughput
improvement over Task 20's active 18,715.4 facts/s, but the active projection
gate is still not fully restored (residual projection indexing CPU/queue lag
remains). No benchmark threshold was changed.
