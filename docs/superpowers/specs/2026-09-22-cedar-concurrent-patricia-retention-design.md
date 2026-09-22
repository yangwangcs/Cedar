# Cedar Concurrent Patricia Retention Design

## Intent And Decision

Retain the current byte-Patricia single-writer efficiency while restoring a
clear four/eight-writer advantage over SkipList and reducing MemTable Arena
usage. First compare the last retained segmented-nibble implementation with
the current segmented-byte implementation under the **same** Release build,
benchmark source, host, and workload. If nibble satisfies all retention gates,
retain it without changing the branch representation. Only if nibble misses
the single-writer gate, evaluate a 16-segment byte branch whose immutable
blocks hold at most 16 children. Retain that candidate only if it satisfies
the same gates. Otherwise keep the last verified implementation and report
which gate failed; do not treat a plausible mechanism as a measured result.

The earlier nibble result (one writer 34.976 ms, four writers 11.819 ms,
eight writers 14.781 ms, Arena about 16.37-17.46 MB) used
`RelWithDebInfo -O2`. The reported current byte result (one writer 33.595 ms,
four writers 24.637 ms versus SkipList 21.547 ms, eight writers 40.724 ms
versus SkipList 19.364 ms, Arena about 50-51 MB) used `Release -O3`.
These figures motivate the experiment; their cross-build differences are
**not** evidence that one implementation is faster than the other. The
current four-by-64 byte blocks can copy up to 64 pointers per snapshot; this
is a hypothesis for the Arena and concurrency regression to test, not an
established root cause. Arena growth here is in-memory allocation, not
WAL/SST write amplification.

## Invariants And Scope

- The index remains one pure path-compressed Patricia tree over exactly 40
  normalized key bytes. Byte 39 participates in discrimination; no 39-byte
  shortcut or change to internal-key order is allowed.
- Published leaves, branches, and child-block payloads remain immutable.
  Root/child-edge publication uses release CAS, failed CAS observes with
  acquire ordering, and readers acquire-load an edge before dereferencing it.
  Unpublished or failed candidates stay Arena-owned until MemTable teardown.
- The existing monotonic `min_leaf`/`max_leaf` CAS updates, same-edge retry
  from root, duplicate handling, cursor order, and frozen successor chain
  remain valid. No lock, shard, background worker, staging structure, spare
  index, or Vector/SkipList fallback is introduced.
- Keep the MemTableRep interface, WAL, Flush/SST, recovery, and on-disk format
  unchanged. This is an in-memory representation choice, not a format change.
- Do not mix the existing dirty benchmark edits or untracked read-matrix
  script into the design change. Preserve them and document precisely which
  benchmark source revision and build flags are used for every comparison.

## Alternatives And Selection

1. **Retain the segmented nibble tree (preferred if it passes).** Use the
   source immediately before the byte conversion, including the path-frame
   optimization. Its four independent CAS edges each own four nibble values.
   This is the smallest validated representation; it needs no new production
   code. Its single-writer time must first be remeasured with the byte build's
   Release settings.
2. **Try a 16-by-16 byte tree only if nibble misses single-writer retention.**
   A branch still discriminates one of the 40 key bytes, but has 16 independent
   atomic child-block edges. `segment = value >> 4`, `local = value & 0x0f`;
   each exact-size immutable block has a 16-bit occupancy bitmap and packed
   pointers in local-byte order. Copy/replace touches at most 16 pointers per
   segment. Different segments publish independently; a stale same-segment
   CAS fails and retries from root. A branch costs 12 additional atomic
   pointers compared with four-by-64, so lower snapshot cost does not by
   itself guarantee lower total Arena use. No second index is kept at runtime.
3. **Keep four-by-64 byte blocks as the control.** They minimize branch-edge
   count and should not be replaced if neither candidate meets all gates.
   Rejecting a candidate does not authorize further representation changes
   within this design.

For the 16-by-16 candidate, collision wrapping builds a fully initialized
branch and one or two child blocks before publishing the parent/root edge.
Direct insertion copies only the observed segment; wrapper replacement copies
only its observed parent segment. Ranked lookup uses the 16-bit bitmap, and
ordered successor/predecessor search scans segments in byte order. `Contains`,
`Seek`, `SeekForPrev`, boundary reads, and freeze construction must all use the
same `segment/local` mapping. A frozen-ready forward cursor continues to use
`frozen_next` without branch loads. The 16-edge limit is the approved exception
to the older four-edge design constraint, not permission for mutable children.

## Comparable Evidence And Suggested Retention Gates

Build nibble, four-by-64 byte, and (only if needed) 16-by-16 byte from
identified source revisions with identical benchmark source, compiler,
`Release -O3` flags, dependencies, host, and run environment. Use separate
builds or worktrees; do not overwrite the existing dirty worktree. Verify
the effective compiler flags and benchmark hash before timing. For each
candidate, run 131072 random keys at seeds `20260920`, `20260921`, and
`20260922`; run five independent processes per seed and configuration,
interleaving implementation order. Time the same `phase=memtable` for one,
four, and eight writers; also record one-writer `phase=index` as diagnostic
context. Performance runs omit `--stats`. Each raw row must preserve the
28-column CSV contract, 131072 operations, zero errors, and a hash matching
all implementations for that seed. Record source/benchmark revisions,
configuration, raw samples, per-seed medians, Arena bytes, and peak RSS.

The following are **proposed acceptance thresholds**, not measured outcomes:

| Gate | Required per-seed median for a retained candidate |
| --- | --- |
| One-writer Radix MemTable | at most `1.05 x` same-seed four-by-64 byte Radix |
| Four-writer Radix MemTable | at most `0.80 x` same-seed SkipList |
| Eight-writer Radix MemTable | at most `0.85 x` same-seed SkipList |
| Arena at each writer count | at most `1.25 x` same-seed old nibble Radix |

Apply every gate to each seed's five-run median, not to a single favorable
run or an average across seeds. Median ratios use paired environment and
workload, not the historical O2/O3 figures. If the nibble tree passes, stop:
fix the nibble design and do not implement 16-by-16. If nibble fails only the
single-writer gate, test 16-by-16; a nibble failure of a correctness,
concurrency, or Arena gate requires investigation before it can serve as a
control. The 16-by-16 tree is retained only after all gates and validation
pass. A candidate's index-phase result is diagnostic and cannot override a
failed MemTable gate. Report the historical Vector one-writer `<=1.10 x`
criterion separately; retention under these proposed gates does not claim
that the older Vector goal has passed.

To test the mechanism, collect an **untimed**, stderr-only diagnostics run
with snapshot CAS attempts/failures, copied child-pointer counts, branch/block
allocations, and boundary candidates where available. Distinguish payload
allocated by successful publications from Arena-retained failed candidates.
Keep stdout's CSV schema unchanged and do not enable hooks in timed runs.
If the branch count, copy volume, or CAS failures do not explain the observed
Arena/concurrency movement, report that fact rather than inferring causality.

## Validation And Freeze-To-Disk Contract

For an unchanged nibble retention, rerun its existing targeted publication,
ordered-read, duplicate, and deep-key tests in the exact selected source.
For 16-by-16, write RED tests and sequential/interleaving model negative
controls before production edits: values 0-255 in final byte 39; boundaries
15/16 through 239/240; same-segment stale CAS; independent-segment direct
inserts and direct/wrapper races; duplicate races; sparse/deep paths;
`Contains`, `Seek`, `SeekForPrev`, and frozen ordered scans. GREEN validation
includes focused Debug, ASan, and TSan runs with no sanitizer reports.

For the **selected** representation, verify active reads, `MarkReadOnly`,
`PrepareForFlush`, and repeated frozen iteration (including branch-load-free
ready forward iteration). Exercise MemTable lifecycle through Flush/SST and
reopen/restart recovery: check count, order, key/value visibility, versions,
and hashes before freeze, after flush, and after recovery. Rerun direct
lifecycle, WAL/crash recovery, and format-compatibility suites; confirm old
SST/WAL data remain readable and newly flushed SST is readable by the normal
path. Validate 40-byte final-byte distinctions end to end. Any semantic,
sanitizer, persistence, or format failure vetoes retention irrespective of
timing. Keep full raw evidence and a pass/fail report for each gate; do not
mark the historical Vector target complete without its own measurement.
