# Cedar Public Documentation

This directory contains the public documentation and measured results for the
Cedar Kernel bitemporal graph database.

## Start Here

1. Read the [Cedar overview](../README.md) for the product model, LSM-backed
   columnar architecture, bitemporal fact definition, and public C++ API.
2. Read the [query acceptance summary](query-acceptance.md) for correctness,
   recovery, sanitizer, projection, and space gates.
3. Read the [development-host performance report](query-performance.md) for
   measured write, read, and sustained mixed-workload results.

## Public Storage Model

Cedar combines:

- one embedded LSM engine and one WAL;
- CedarParquet authoritative bitemporal columnar facts;
- engine-owned WAL recovery, MemTables, VersionSet, and MANIFEST; and
- Cedar-owned temporal and adjacency projections that can be rebuilt or
  bypassed through canonical fallback.

The public storage contract is described in the root README and is reflected
in the acceptance and performance evidence linked above.

## Evidence Interpretation

The published results are tied to their documented build, workload, host, and
admission parameters. They are intended to make correctness and performance
measurements reproducible, not to define a hardware-independent SLA.

- A sustained qualification requires at least 1,800 seconds of actual elapsed
  workload time.
- `facts/s` is the primary write metric when transactions contain multiple
  facts; `txn/s` is derived from the configured transaction size.
- Projection results are compared with authoritative canonical facts.
- Missing, stale, corrupt, or partial projections must fall back to canonical
  facts without returning silently stale results.
- Generated database directories and host-specific raw benchmark artifacts are
  not published in the source tree.

## Documentation Boundary

Detailed implementation plans, historical design drafts, and internal
execution records are maintained as local development material. They are not
part of the public GitHub documentation surface. The files in this directory,
the root README, and the source/tests are the public contract.
