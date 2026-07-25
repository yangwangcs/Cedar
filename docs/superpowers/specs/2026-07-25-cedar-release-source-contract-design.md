# Cedar Release Source Contract Design

Date: 2026-07-25

## Scope

Add one deterministic, repository-local source-contract gate for four evidence
gaps that do not require a production host:

1. inventory every production source file that performs durable filesystem
   mutation;
2. inventory every `VersionSet::ApplyEdit` publication entry and every direct
   persistent-file deletion owner;
3. inventory every retained component-local `*Stats` type as an explicitly
   reviewed internal view;
4. reject direct production diagnostics, duplicate ad-hoc latency/cache
   metrics, and unbound numeric performance claims.

This gate is static evidence. It does not replace runtime crash injection,
serializability stress, production-scale resource distributions, or approved
paired benchmark evidence.

## Chosen Approach

Use a CMake script invoked by CTest. CMake is already required, can enumerate
and hash files without adding a runtime dependency, and works in all four
normal/sanitizer build trees. The gate owns exact allowlists rather than broad
regular-expression exceptions: a new writer, publisher, deleter, or `*Stats`
type fails until its ownership is reviewed and the contract is updated.

A one-off shell scan was rejected because it cannot prevent later drift. A new
C++ executable was rejected because source analysis is a build-time concern and
would add unnecessary production code.

## Inputs and Outputs

Inputs are every `.c`, `.cc`, `.cpp`, `.cxx`, `.h`, `.hh`, and `.hpp` file
under `include/`, `src/`, and `benchmarks/`, plus `README.md` and the six
authoritative specifications. Unknown production-source extensions fail closed;
build and result directories are excluded.

The script writes a deterministic output directory containing:

- `durable-writer-files.txt`;
- `manifest-publication-files.txt`;
- `persistent-delete-files.txt`;
- `retained-stats-types.txt`;
- `forbidden-direct-diagnostics.txt`;
- `forbidden-duplicate-metrics.txt`;
- `forbidden-unbound-performance-claims.txt`;
- `source-input-files.txt`, containing the sorted path and SHA-256 of every
  scanned input;
- `source-contract.json` with counts and SHA-256 values.

Forbidden outputs must be empty. Inventory outputs must exactly match the
reviewed allowlists. Paths are repository-relative, sorted, and unique.

## Failure Contract

The script exits nonzero when an expected owner disappears, an unreviewed owner
appears, a forbidden match is nonempty, an input path escapes the source root,
or an output hash cannot be generated. The CTest registration passes the source
and build-output roots explicitly and uses `-j1` like the rest of the closure
workflow.

## Evidence Contract

After the gate passes, create a self-contained focused audit root containing
the CMake scanner, negative harness, the exact scanned source snapshot, all raw
outputs, a schema-1 release-evidence manifest, and a complete `SHA256SUMS`.
The archived scanner is run against that archived snapshot. It records the
current full source commit and dirty state and remains
`release_gate_eligible: false` until the source is clean. The directory-level
evidence verifier must validate every byte.

## Testing

TDD begins by registering the missing CMake script and observing the expected
CTest failure. The implemented script is then run directly and through CTest.
Negative tests copy a minimal fixture tree and prove that an added unapproved
writer, direct diagnostic, duplicate metric, and unbound performance claim each
fail closed without replacing the last valid output directory.
