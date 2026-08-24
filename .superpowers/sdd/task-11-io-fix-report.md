# Task 11 I/O and Spill Reservation Fix

## Changes

- Injected a `QueryScratch::SetIoAdmission` callback. Every spill write and
  read invokes the callback immediately before physical I/O. Analytical query
  runtime scratch wires this callback to `QueryResourcePool::AcquireIo`, so
  `wal_sync_critical` now rejects actual external spill I/O with
  `ResourceExhausted` rather than only rejecting standalone pool probes.
- `ReadRun` validates the framed payload length, then reserves the read-byte
  budget before constructing the payload string. This prevents a malformed
  or oversized run from allocating first and accounting later.
- External relational spill decoding now takes a transient reservation guard
  per framed row before `DeserializeRow` allocates its cell vector and scalar
  payloads. The guard ends after insertion, leaving persistent output
  accounting to the existing output lease and avoiding permanent double
  charging.

## Verification

- `build/query-debug/tests/test_query_resources` (13 tests passed), including
  analytical WAL-critical write/read rejection and read reservation ordering.
- `build/query-debug/tests/test_query_relational` (42 tests passed), including
  external spill joins and runtime relational reservation checks.
