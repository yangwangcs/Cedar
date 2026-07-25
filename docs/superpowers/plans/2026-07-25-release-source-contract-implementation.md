# Cedar Release Source Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a deterministic source-contract CTest and self-contained evidence root for durable writers, Manifest publication/deletion ownership, retained stats views, and forbidden diagnostics/duplicate metrics/unbound performance claims.

**Architecture:** A CMake scanner consumes the repository source root, compares sorted exact inventories with reviewed allowlists, emits raw text plus a SHA-bound JSON summary, and fails closed on drift. A CMake harness supplies negative fixture mutations; the accepted output is copied into a schema-1 evidence directory and verified with the existing directory verifier.

**Tech Stack:** CMake script mode, CTest, Git, SHA-256, existing `cedar_evidence_verify`.

## Global Constraints

- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.
- Use `-j1` for every build and CTest command.
- Keep database format version `1` and current clean-break names.
- Do not classify benchmark/result writers as database durable writers.
- Do not weaken an exact inventory into a wildcard allowlist.
- Static evidence does not replace runtime or production-scale campaigns.

---

### Task 1: Register the source-contract gate and observe RED

**Files:**
- Modify: `CMakeLists.txt`
- Create later: `cmake/VerifyReleaseSourceContract.cmake`

**Interfaces:**
- Consumes: `SOURCE_ROOT`, `OUTPUT_ROOT`.
- Produces: CTest `release_source_contract`.

- [x] Add `release_source_contract` under `BUILD_TESTS`, invoking the missing scanner with `${CMAKE_SOURCE_DIR}` and `${CMAKE_BINARY_DIR}/release-source-contract`.
- [x] Reconfigure `build-current` and run `ctest --test-dir build-current -j1 -R '^release_source_contract$' --output-on-failure`.
- [x] Confirm RED reports that `cmake/VerifyReleaseSourceContract.cmake` is missing.

### Task 2: Implement exact inventories and deterministic output

**Files:**
- Create: `cmake/VerifyReleaseSourceContract.cmake`

**Interfaces:**
- Produces the seven text files and `source-contract.json` defined by the design.
- Publishes the output directory only after every inventory and forbidden scan passes.

- [x] Implement repository-relative source enumeration for C/C++ files under `include`, `src`, and `benchmarks`, plus `README.md` and the six authoritative specifications; exclude build/result roots, reject symlink escapes, and fail closed on unknown production-source extensions.
- [x] Detect persistence mutation APIs: POSIX open/write/pwrite/fsync/fdatasync/rename/unlink, `std::ofstream`, and `std::filesystem::{remove,remove_all,rename,copy_file,create_directories}`.
- [x] Compare database durable-writer, `ApplyEdit`, persistent-delete, and retained `*Stats` inventories with exact sorted allowlists.
- [x] Emit zero-hit files for direct `printf/fprintf`, ad-hoc average-latency/cache-hit-rate metrics, and numeric performance claims lacking both an exact `results/...` reference and a 64-hex artifact identity.
- [x] Emit `source-input-files.txt` with the SHA-256 of every scanned input.
- [x] Hash every raw output with `file(SHA256 ...)`, write deterministic `source-contract.json`, then transactionally publish `OUTPUT_ROOT` with checked rollback and stranded-backup recovery.
- [x] Run the focused CTest and confirm PASS.

### Task 3: Prove fail-closed behavior with fixtures

**Files:**
- Create: `tests/test_release_source_contract.cmake`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: scanner path and a temporary fixture root.
- Produces: CTest `release_source_contract_negative`.

- [x] Build a minimal fixture from the reviewed source files and run the scanner once successfully.
- [x] Add an unapproved durable writer file and require scanner failure.
- [x] Restore the fixture, add direct `fprintf`, and require scanner failure.
- [x] Restore the fixture, add an ad-hoc `average_latency_ms` metric, and require scanner failure.
- [x] Restore the fixture, add an unbound README throughput number, and require scanner failure.
- [x] Verify each failed run leaves the last valid output bytes unchanged, including a publication failure after preserving the accepted directory.
- [x] Reject unqualified POSIX `open`, unapproved `.c` mutation owners, source-root symlink escape, and misleading performance-evidence keywords; automatically recover and regression-test a stranded accepted-output backup.

### Task 4: Generate and verify the self-contained audit root

**Files:**
- Create: `results/release-closure-20260725-source-contract-r1/`
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `docs/superpowers/plans/2026-07-23-cedar-six-design-batched-final-closure-goal.md`

**Interfaces:**
- Evidence root contains scanner, raw outputs, manifest, and exact `SHA256SUMS`.

- [x] Run both source-contract CTests with `-j1`.
- [x] Copy the scanner, negative harness, exact scanned source snapshot, and accepted outputs into the evidence root; rerun the archived scanner against the archived snapshot.
- [x] Write a schema-1 manifest recording source commit, dirty state, exact commands, zero forbidden matches, inventory counts, and release blockers.
- [x] Generate `SHA256SUMS` over every non-ledger file.
- [x] Run `build-current/cedar_evidence_verify <root>` and `shasum -a 256 -c SHA256SUMS` from inside the root.
- [x] Update the completion matrix only for the inventory/static-scan rows directly proven by this artifact.
- [x] Run `git diff --check` and keep the active production goal open.
