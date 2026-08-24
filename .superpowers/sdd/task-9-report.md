# Task 9 Report

Status: DONE (implementation complete; independent review remains required).

## Delivered

- Added ordered `FactStore::ReadSequenceRange` with Snapshot validation,
  contiguous sequence checks, and canonical corruption on a missing record.
- Added `FactStore::ReadExactFacts` using one Snapshot-bound RocksDB MultiGet;
  requested key order is preserved and missing facts are canonical corruption.
- Added bounded Cedar-only `QueryDelta` with base/indexed/visible watermarks,
  first-missing continuity proof, queue and hard-memory/lag limits, bounded
  durable repair, immutable Snapshot-cut views, edge identity retention, and
  corrected-boundary merging. Queue permits have explicit
  `EnqueuePublished`/worker and `ConsumeThrough`/`RetireThrough` lifecycle;
  hard-bound generation handoff uses `ResetBase`.
- Published immutable commit descriptors after visible append publication;
  publisher work is push-only into the bounded Cedar queue; descriptor
  rejection never changes the committed result and is repairable from durable
  sequence metadata. No exact-fact read or chain indexing runs on the
  publisher.
- Added focused QueryDelta and batch-read tests.

## Verification

```text
cmake --build build/query-debug -j2 --target \
  test_query_delta test_fact_store_commit test_kernel_commit \
  test_query_canonical test_projection_store     PASS

ctest --test-dir build/query-debug --output-on-failure \
  -R 'QueryDelta|FactStoreCommit|KernelCommit|QueryCanonical|ProjectionStore'
68/68 tests passed

git diff --check                                  PASS
```

## Notes

`QueryDelta` is derived and in-memory. Restart/recovery uses the durable
`sequence/<CommitSeq>` records through `RepairThrough`; no derived delta log or
RocksDB ownership boundary was introduced.
