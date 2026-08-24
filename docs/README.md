# Cedar Documentation

This directory contains Cedar's design records, implementation plans, and
measured acceptance evidence. The links below identify the current Cedar
Kernel query system and its source-of-truth documents.

## Current Cedar Kernel Query System

Read these documents in order when evaluating the implementation on `main`:

1. [Bitemporal graph query design](superpowers/specs/2026-08-21-cedar-bitemporal-graph-query-design.md)
2. [Bitemporal graph query implementation plan](superpowers/plans/2026-08-21-cedar-bitemporal-graph-query-implementation.md)
3. [Bitemporal query acceptance evidence](superpowers/evidence/2026-08-21-cedar-bitemporal-query-acceptance.md)
4. [Local development-machine performance results](superpowers/evidence/2026-08-24-cedar-query-performance-local.md)

The current implementation preserves Cedar's core storage contracts:

- one Cedar-controlled RocksDB WAL;
- CedarParquet authoritative columnar facts;
- RocksDB ownership of WAL, recovery, MemTable, VersionSet, and MANIFEST;
- Cedar-owned temporal projections and QueryDelta overlays;
- canonical fallback when a projection is partial, stale, or unavailable.

## Storage And Maintenance

- [Single-WAL append/commit design](superpowers/specs/2026-08-02-cedar-single-wal-append-commit-design.md)
- [Kernel performance release design](superpowers/specs/2026-08-19-cedar-kernel-performance-release-design.md)
- [Kernel-only benchmark design](superpowers/specs/2026-08-20-cedar-kernel-only-benchmark-design.md)
- [WAL group-commit optimization plan](superpowers/plans/2026-08-20-cedar-wal-group-commit-optimization.md)
- [Storage file inspection design](superpowers/specs/2026-08-21-cedar-storage-file-inspection-design.md)
- [Storage file inspection plan](superpowers/plans/2026-08-21-cedar-storage-file-inspection.md)

## Query Acceptance Coverage

The current acceptance evidence covers:

- point-in-time and history reads;
- temporal events and changes;
- property filtering and typed binding;
- temporal expansion and k-hop traversal;
- coexisting shortest path;
- earliest arrival, latest departure, and fastest duration;
- interval join and temporal aggregate operators;
- projection base, short delta, long delta, and partial coverage;
- WAL/reopen recovery, sanitizer-focused suites, and space audits.

Raw Release artifacts remain under `build/query-release/evidence/` locally.
The committed evidence documents record the commands, gate results, and
provenance without committing generated database directories into the source
tree.

## Document Status

`superpowers/specs/` contains architecture and behavioral specifications.
`superpowers/plans/` contains implementation plans, including historical plans
that explain how the current system was built.
`superpowers/evidence/` contains measured results and acceptance records.
`superpowers/archive/` contains explicitly superseded designs.

Historical plans are retained for auditability. The four documents in
**Current Cedar Kernel Query System** are the authoritative starting point for
the implementation currently merged to `main`.

