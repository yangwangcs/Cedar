# Pinned Temporal Scan Second Review Design

## Goal

Close the second reviewer findings without changing query semantics: stop
successor scans at the ordered boundary, account cursor-retained duplicate
state, account and pin point metadata derivation, and expose every exact SST
block attempt.

## Successor merge

All exact-key sources use persisted order `(LogicalKey, valid_from DESC,
commit_seq DESC)`, and the merge heap exposes the global next event. The
boundary loop continues to apply each source's snapshot cutoff before
duplicate validation for every candidate whose `valid_from` is later than the
selected fact. Once the global heap top has `valid_from <= after_valid_from`,
no unseen candidate can improve the minimum later boundary, so the function
returns without advancing that source or reading older blocks. Duplicate peers
at or below the selected boundary are deliberately irrelevant and unread.

Boundary SST attempts are reported separately from root scan blocks so tests
can prove that a long single-key history reads only the first necessary block.

## Cursor-retained duplicate state

`last_physical_event` remains a complete event so contradictory content is
compared exactly. Before copying a new event, the cursor reserves
`sizeof(TemporalEvent)` plus its retained value/blob payload. It creates the
new copy before replacing the old one, then releases the old charge. Reserve
failure preserves the old event and lease until the failure is made terminal;
terminal cleanup, EOF, overwrite, and destruction release the correct charge.
The lease is cursor-owned and never borrows a morsel staging/output lease.

## Point metadata transform

System-time metadata uses a query-pinned compact `vector<uint64_t>` indexed by
`commit_seq - 1`. The transform validates contiguous commit sequences and
reserves the full vector charge from `QueryMemoryAccount` before allocation.
The timeline lease is captured by the transform and therefore lives exactly as
long as the result stream.

`VectorBatchTransform` mutates the local scan batch in place. Existing input
vectors are not copied. Before appending any derived vector, the transform
reserves the complete fixed/value charge for every derived row; each appended
`FlatVector` retains the shared derived lease. Failure releases all newly
reserved memory. Commit lookup validates zero, overflow, and missing entries.

## Exact SST statistics

The public SST cursor synchronizes `blocks_read` from the underlying
`block_attempts()` counter and peak attempted bytes after Open and every
Advance, regardless of whether a valid event was produced. This covers Bloom
false-positive empty exact reads, recursive overlapping-block attempts,
successful matches, and terminal failures.

## Testing

- A 9000-version single-key SST verifies first-batch `valid_to` reads one root
  block and one boundary block; the draining implementation reads the tail.
- Cursor memory tests verify retained fixed/payload bytes survive morsel
  release, do not accumulate on overwrite, clear at EOF/destruction, and can
  trigger the hard limit before copying.
- Timeline tests verify reserve-before-copy failure, contiguous lookup, stream
  lifetime release, and derived-vector hard-limit behavior.
- An exact-key false-positive/empty test verifies attempted blocks are visible
  even when the public cursor is invalid.
- Focused, full executable, CTest, diff, whitespace, mutation, and self-review
  gates run before completion.

## Constraints

- Strict RED/GREEN TDD for each finding.
- No commit, reset, clean, checkout, revert, or unrelated refactor.
- Preserve session cutoff-before-dedupe and all existing terminal semantics.

