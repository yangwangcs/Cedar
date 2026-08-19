# Cedar RocksDB Stage B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Cedar's clean-break explicit transaction and snapshot interface on the Stage A RocksDB `FactStore`.

**Architecture:** Public domain types are independent from storage. `Database` owns one `FactStore`; move-only `Transaction` and `Snapshot` delegate through narrow internal handles. One publisher mutex serializes final validation and synchronous WriteBatch publication, allowing strict exact-read validation without a prepared reservation state machine.

**Tech Stack:** C++20, RocksDB v11.1.2, GoogleTest, bitemporal oracle.

## Global Constraints

- Follow the master plan's Global Constraints.
- Public headers contain no RocksDB includes or types.
- The new public path never calls the old `TransactionCoordinator`.

---

### Task B1: Define Public Schema, Snapshot, Transaction, and Database Interfaces

**Files:**
- Create: `include/cedar/database.h`
- Create: `include/cedar/transaction.h`
- Create: `include/cedar/snapshot.h`
- Create: `include/cedar/schema.h`
- Create: `src/kernel/database_impl.h`
- Create: `src/kernel/database.cc`
- Create: `src/kernel/transaction.cc`
- Create: `src/kernel/snapshot.cc`
- Create: `tests/test_kernel_interface.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces the exact public interface approved in the design spec.
- Produces `DatabaseOptions`, `TransactionOptions`, `SnapshotOptions`, `IsolationLevel`, and typed commit outcomes.

- [ ] Write compile-time and lifecycle tests for move-only objects, explicit writes, closed/moved-from behavior, and absence of old autocommit methods.
- [ ] Run the target and verify RED on missing public headers.
- [ ] Implement pimpl shells backed by `FactStore`, with no mutation logic beyond argument/lifecycle validation.
- [ ] Run `test_kernel_interface` GREEN.
- [ ] Commit as `feat: add explicit Cedar kernel interface`.

### Task B2: Implement Snapshot Reads and Edge Visibility

**Files:**
- Modify: `src/kernel/snapshot.cc`
- Modify: `src/kernel/database.cc`
- Create: `tests/model/bitemporal_fact_oracle.h`
- Create: `tests/test_kernel_snapshot.cc`

**Interfaces:**
- Produces `Snapshot::Exists`, `Get`, and `Scan`.
- Edge reads resolve immutable identity, edge state, and both endpoint states at one Snapshot/valid time.

- [ ] Write RED oracle tests for state/property PUT/DELETE, out-of-order valid time, same-valid-time corrections, explicit `as_of`, edge endpoint intersection, vertex retraction, and vertex reassertion.
- [ ] Implement reads only through `FactStore::Read/Scan`.
- [ ] Run the snapshot target before/after forced RocksDB flush and manual compaction.
- [ ] Reopen and rerun the same oracle corpus.
- [ ] Commit as `feat: resolve bitemporal snapshots from FactStore`.

### Task B3: Implement Explicit Mutations and Schema Validation

**Files:**
- Modify: `src/kernel/transaction.cc`
- Modify: `src/kernel/database.cc`
- Create: `tests/test_kernel_mutations.cc`

**Interfaces:**
- Produces `Assert`, `Retract`, `Set`, `Unset`, `Rollback`, canonical mutation ordering, and duplicate rejection.

- [ ] Write RED tests proving existence needs no schema/value, properties resolve an exact schema epoch, wrong physical type is rejected, rollback writes nothing, and contradictory duplicate `(fact, valid_from)` mutations fail.
- [ ] Implement pending mutation accumulation without durable side effects.
- [ ] Run mutation tests GREEN.
- [ ] Commit as `feat: stage typed temporal mutations`.

### Task B4: Implement Snapshot Write Validation

**Files:**
- Create: `src/kernel/temporal_validation.h`
- Create: `src/kernel/temporal_validation.cc`
- Modify: `src/kernel/transaction.cc`
- Create: `tests/test_snapshot_transactions.cc`

**Interfaces:**
- Produces declarative `SnapshotWriteDependency` values consumed by
  `FactStore::Commit`.
- Extends `FactStore::Commit` so validation and publication execute under the
  same publisher mutex.

- [ ] Port the proven half-open interval oracle cases into a standalone RED target: same interval conflict, later correction conflict, disjoint valid intervals, write skew permitted, and endpoint/state independence.
- [ ] Implement current-boundary interval derivation in
  `src/kernel/temporal_validation.cc`, pass the resulting dependencies in
  `StoreCommitBatch`, and re-evaluate them inside `FactStore::Commit` while its
  publisher mutex remains held through `DB::Write(sync=true)`.
- [ ] Commit only after deterministic concurrent transaction tests pass repeatedly.
- [ ] Commit as `feat: validate bitemporal snapshot writes`.

### Task B5: Implement Strict Exact-Key Transactions

**Files:**
- Modify: `src/kernel/temporal_validation.h`
- Modify: `src/kernel/temporal_validation.cc`
- Modify: `src/kernel/transaction.cc`
- Create: `tests/test_strict_transactions.cc`

**Interfaces:**
- Produces captured read identity with observed event and predecessor/successor
  fences as declarative `StrictReadDependency` values consumed by
  `FactStore::Commit` under the publisher mutex.

- [ ] Write RED tests for write skew rejection, empty-read phantom, predecessor/successor insertion, edge plus endpoint dependencies, and unsupported strict range scan.
- [ ] Capture strict dependencies through transaction reads and revalidate against the latest state while holding the publisher mutex.
- [ ] Verify no separate values or prepared reservations are stored outside `FactStore`.
- [ ] Commit as `feat: add strict exact-key transactions`.

### Task B6: Bind Edge Identity and Commit Public Transactions

**Files:**
- Modify: `src/kernel/transaction.cc`
- Modify: `src/kernel/database.cc`
- Modify: `src/fact/fact_store.cc`
- Create: `tests/test_kernel_commit.cc`

**Interfaces:**
- Produces public `Committed/Aborted/Indeterminate`, `ResolveTransaction`, and immutable EdgeIdentity binding.

- [ ] Write RED tests for first assertion atomic binding, exact reassertion, conflicting binding, multi-fact atomicity, sync reopen, duplicate transaction resolution, and write rejection after indeterminate injection.
- [ ] Map validated mutations into one `StoreCommitBatch` and no other durable operation.
- [ ] Run Stage B oracle corpus before/after reopen, engine flush, and engine compaction.
- [ ] Commit as `feat: commit explicit Cedar transactions`.

### Task B7: Add Database ID Allocation and Lifecycle Closure

**Files:**
- Modify: `src/kernel/database.cc`
- Modify: `src/kernel/database_impl.h`
- Create: `tests/test_kernel_lifecycle.cc`
- Modify: `README.md`

- [x] Write RED tests for separate vertex/edge leases, close waiting for active commit calls, close with live Snapshots, rejection after close, and reopen.
- [x] Implement allocation and lifecycle without query registration or T-Cypher cancellation.
- [x] Replace README examples with explicit transaction and Snapshot usage.
- [x] Run all Stage A/B tests. The changes await user-directed commit handling.
