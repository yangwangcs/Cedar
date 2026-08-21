# Task 8 Report

Status: DONE (implementation and focused verification complete).

Implemented Cedar-owned projection generations:

- `ProjectionManifest` and `CoverageRegion` use explicit region/segment references, database identity, generation/base snapshot, schema fingerprints, range metadata, and CRC32C-protected deterministic encoding.
- Manifest decoding rejects malformed ranges, reversed intervals, duplicate segment IDs, overlapping segment coverage, unsafe paths, identity mismatches, unsupported fields, and checksum errors.
- `QueryProjectionStore` writes verified temporary segment files, fsyncs and renames them, writes immutable generation manifests, and atomically replaces checksummed `PROJECTION-CURRENT`.
- Open fail-closes derived projections on malformed CURRENT/manifest or missing, sized-differently, or checksum-invalid referenced segments; orphan files never imply coverage.
- `Acquire` requires an explicit matching coverage region and returns a pin. Retired generations remain while pinned; `CollectRetired` removes only unpinned referenced segments/manifests. `Quarantine` moves a safe file into a quarantine directory.
- Database opens the projection store after authoritative FactStore recovery and releases it before RocksDB close; no commit-path or RocksDB sync-point dependency was added.

Verification:

```text
cmake --build build/query-debug -j2 --target test_projection_store test_query_canonical test_kernel_lifecycle: PASS
ctest -R 'ProjectionStore|QueryCanonical|KernelLifecycle': 27/27 PASS
git diff --check: PASS
```

Concern: builders consume an explicit `ProjectionBuild` (manifest plus already encoded Task 7 segment bytes). Snapshot family-specific extraction/build algorithms remain a later task; publication is invisible until the complete build is committed.
