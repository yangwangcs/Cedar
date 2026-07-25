# Release Evidence Integrity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make release-evidence validation prove the archived file bytes match `SHA256SUMS` and the manifest provenance hashes.

**Architecture:** Keep `ValidateReleaseEvidenceManifest` as a pure JSON schema validator. Add a directory-level verifier that parses `SHA256SUMS`, hashes each named regular file with an in-tree SHA-256 implementation, and requires the manifest binary/log digest entries to match their ledger entries. The command-line tool accepts an evidence root and delegates to that verifier.

**Tech Stack:** C++17, CMake, GoogleTest, POSIX file APIs.

## Global Constraints

- Preserve the existing dirty worktree and do not stage, commit, reset, or clean it.
- Do not invoke external hashing programs or trust manifest audit booleans as proof of file integrity.
- Keep the existing 64 MiB JSON input limit and format-1 release manifest contract.
- Return typed Cedar `Status` failures for missing, malformed, non-regular, or mismatched ledger entries.
- Use `-j1` for CTest commands.

---

### Task 1: Add a failing directory-integrity regression

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `include/cedar/benchmark/artifact_reader.h`

**Interfaces:**
- Produces: `Status VerifyReleaseEvidenceDirectory(const std::string& evidence_directory)`.

- [ ] **Step 1: Write the failing test**

Create a temporary directory containing a valid format-1 `manifest.json`, `binary.bin`, `run.log`, and `SHA256SUMS`. Assert the new API accepts correct SHA-256 values, rejects a modified `run.log`, and rejects a manifest digest that differs from the corresponding ledger digest.

- [ ] **Step 2: Run the test to verify it fails**

Run: `ctest --test-dir build-current -j1 -R '^BenchmarkArtifactTest\\.ReleaseEvidenceDirectoryValidatesLedgerAndManifestBindings$' --output-on-failure`

Expected: build or test failure because the directory verifier is not declared or implemented.

- [ ] **Step 3: Declare the directory verifier**

Add this public declaration beside `ValidateReleaseEvidenceManifest`:

```cpp
Status VerifyReleaseEvidenceDirectory(const std::string& evidence_directory);
```

- [ ] **Step 4: Re-run the focused test**

Expected: the test still fails at link time until the implementation is added.

### Task 2: Verify the ledger and manifest bindings

**Files:**
- Modify: `src/benchmark/artifact_reader.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `VerifyReleaseEvidenceDirectory`, `ValidateReleaseEvidenceManifest`, manifest `execution.binary/log.sha256` fields, and `SHA256SUMS` records.
- Produces: a successful status only when every ledger path is contained in the evidence root, is a regular file, hashes to the ledger digest, and the binary/log ledger digests equal the manifest digests. Historical roots with binary paths outside the root are intentionally rejected as archive-incomplete.

- [ ] **Step 1: Implement a small in-tree SHA-256 helper**

Add an internal streaming SHA-256 implementation in `artifact_reader.cc` using only fixed-width integer operations. Hash files in bounded chunks and emit lowercase hexadecimal output.

- [ ] **Step 2: Parse `SHA256SUMS` strictly**

Require one lowercase or uppercase 64-hex digest, two spaces, and a relative path per nonempty line. Reject duplicate entries, absolute paths, empty components, `.`/`..` components, directory targets, and missing files.

- [ ] **Step 3: Bind the manifest to the ledger**

Parse and schema-validate `manifest.json`, read its `execution.binary.path`, `execution.log.path`, and SHA-256 values, normalize each path below the evidence root, and require matching ledger entries and byte digests. Keep audit booleans as metadata only.

- [ ] **Step 4: Run the focused regression**

Run: `ctest --test-dir build-current -j1 -R '^BenchmarkArtifactTest\\.ReleaseEvidenceDirectoryValidatesLedgerAndManifestBindings$' --output-on-failure`

Expected: PASS.

### Task 3: Make the command line tool verify a directory

**Files:**
- Modify: `benchmarks/cedar_evidence_verify.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `VerifyReleaseEvidenceDirectory`.
- Produces: `cedar_evidence_verify <evidence-directory>` with exit zero only for a fully bound directory.

- [ ] **Step 1: Change the CLI argument contract**

Replace manifest-file loading with:

```cpp
const cedar::Status status = cedar::VerifyReleaseEvidenceDirectory(argv[1]);
```

and update usage to `cedar_evidence_verify <evidence-directory>`.

- [ ] **Step 2: Build and run the focused tests**

Run: `cmake --build build-current -j1 --target cedar_evidence_verify test_correctness_kernel && ctest --test-dir build-current -j1 -R 'BenchmarkArtifactTest\\.(ReleaseEvidenceManifestRejectsOldFormatAndNaming|ReleaseEvidenceDirectoryValidatesLedgerAndManifestBindings)' --output-on-failure`

Expected: both tests pass.

### Task 4: Correct frozen-evidence documentation

**Files:**
- Modify: `docs/superpowers/plans/2026-07-23-cedar-six-design-batched-final-closure-goal.md`

**Interfaces:**
- Consumes: the directory verifier’s actual scope and the archived roots present under `results/`.
- Produces: documentation that distinguishes four currently complete release-evidence roots from missing HTAP-correctness and observability final roots, and treats `final-matrix` as its separate legacy matrix manifest.

- [x] **Step 1: Narrow the evidence status statement**

State that the four retained final roots are archive-incomplete because the
referenced binary is absent from the root and ledger. Remove the claim that all
six CTest roots are validated; mark unavailable final roots as missing archived
evidence.

- [ ] **Step 2: Verify documentation and archived roots**

Run: `for root in results/release-closure-20260723-{columnar-final,htap-resource-final,tcypher-final,temporal-index-cbo-final}; do build-current/cedar_evidence_verify "$root" || exit 1; done; git diff --check`

Expected: each historical root is rejected for the missing binary ledger
binding and the diff is whitespace-clean.
