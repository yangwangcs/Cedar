# Cedar 32x8 Stability: Failed Gate Audit

Status: **not frozen**. The 32x8 byte-segment Patricia representation remains;
the proposed 128-byte edge spacing and one compatible same-edge retry were
reverted after failing their isolated A/B retention rules. The active goal is
not complete. This is failed-trial evidence, not a performance acceptance.

## Comparable Builds

All five detached worktrees were clean. The benchmark source SHA-256 was
`4c4ea921641adcd529a995cfb1fa0e3e9bf398e77b4134a6c65023dc6594fa53`
for every build. All used `/usr/bin/c++`, `Release`, `-O3 -DNDEBUG`, and
single-job named builds. Each embedded RocksDB cache was separately hashed
and built with `CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1`.

| Variant | Production commit | Release binary |
| --- | --- | --- |
| Nibble | `77593a87a64f03d7716318e3c9ce7bb672a78327` | `/Volumes/E/CedarBuild/patricia-retention-nibble-o3-20260922/cedar_radix_memtable_bench` |
| Byte and SkipList | `76adf448257aaaa4eacac8ba5451ba38a51223f3` | `/Volumes/E/CedarBuild/patricia-retention-byte-o3-20260922/cedar_radix_memtable_bench` |
| 32x8 baseline | `ee9a887865e9d643103758b71b7469868c758eb9` | `/Volumes/E/CedarBuild/patricia-32x8-baseline-o3-20260922/cedar_radix_memtable_bench` |
| Edge isolation | `520a5ce9924470a619c4a0c7fe832a12833f8021` | `/Volumes/E/CedarBuild/patricia-32x8-isolation-o3-20260922/cedar_radix_memtable_bench` |
| Edge isolation + retry | `c8082029ed8f119ab76a27b5da611116803483df` | `/Volumes/E/CedarBuild/patricia-32x8-final-o3-20260922/cedar_radix_memtable_bench` |

Nine untimed `--entries 1024 --phase all` checks across baseline, isolation,
and final at 1/4/8 writers reported 1024 operations, zero errors, and the
same result hash `16340194407465152223`.

## Isolated A/B

Each A/B has five separate, interleaved processes per variant and seed at
131072 random keys and eight writers. The first raw matrix is in
`ab-first/matrix.csv`; the independent repeat is in `ab-repeat/matrix.csv`.
Values below are medians in milliseconds, ordered as baseline / isolation /
final, followed by the SkipList control.

| Seed | First A/B | Repeat A/B |
| --- | --- | --- |
| 20260920 | 25.239 / 23.589 / 23.950; 30.089 | 24.711 / 26.842 / 28.343; 33.962 |
| 20260921 | 23.153 / 25.549 / 27.468; 24.435 | 19.243 / 26.193 / 26.504; 30.869 |
| 20260922 | 25.641 / 26.433 / 24.334; 26.327 | 24.698 / 25.571 / 23.007; 25.490 |

Isolation regressed at two seeds in the first A/B and all three in the
repeat. Retry regressed versus isolation at two seeds in each A/B. Neither
change passed its retention rule. Revert commits: retry `235340a`, edge
isolation `34b5080`. The retained production header and source match
`ee9a887` exactly.

The host reported a 128-byte cache line and eight CPUs. During the first
A/B, a remote-desktop video-session process used about 140% CPU, and
WindowServer about 44%; load average was about 5.4. SkipList process CV
was 11%-19%, and one isolation process took 113.766 ms while its other
four samples were 21.705-26.667 ms. No external process was stopped.
These observations make causal attribution to a sub-change weak, but do not
relax any binding gate.

## Five-seed Gates

The unreverted candidate trial is in `matrix-raw/matrix.csv` and
`summary.json`; the reverted baseline audit is in
`baseline-matrix-raw/matrix.csv` and `baseline-summary.json`. Each matrix
contains 400 independent process rows, 131072 operations and zero errors
per row, with expected hash `454339457082336225`. Peak RSS is present in
each row (max 57049088 candidate, 57065472 baseline bytes).

| Seed | Candidate 1 / 4 / 8 ratios, CV8 | Baseline 1 / 4 / 8 ratios, CV8 |
| --- | --- | --- |
| 20260920 | 1.1578 / 0.8463 / 0.8200, 13.04% | 0.9071 / 0.7417 / 0.7760, 9.41% |
| 20260921 | 1.1160 / 0.9523 / 0.8975, 22.04% | 0.9888 / 0.6149 / 0.8177, 11.86% |
| 20260922 | 1.2260 / 0.8293 / 0.9263, 11.02% | 0.9699 / 0.6654 / 0.8700, 13.70% |
| 20260923 | 1.2153 / 0.9145 / 0.8389, 16.77% | 1.0457 / 0.5897 / 0.7481, 18.40% |
| 20260924 | 1.1328 / 1.0050 / 0.9249, 10.95% | 1.0101 / 0.7484 / 0.7794, 6.73% |

Limits are `<=1.05` against Byte (one writer), `<=0.80` against SkipList
(four writers), `<=0.85` against SkipList (eight writers), and `<=5%`
candidate eight-writer CV independently for each seed. Candidate Arena
ratios are 1.2006-1.2061x Nibble; reverted baseline Arena ratios are
1.0799-1.0914x. Both pass the `<=1.25x` memory limit, but neither passes
the whole performance matrix. The baseline misses the eight-writer ratio
at seed 20260922 and all five CV gates; the candidate misses more gates.

`diagnostics.json` contains 15 untimed direct-index runs. At eight writers,
segment CAS failures were 41-94 across 131072 insertions. Final's local
retry succeeded 28-51 times per seed and root restarts were 8-18. The
pre-retry revisions have `root_restarts: null` because that hook did not
exist. These rare conflicts cannot account for the measured multi-ms
cross-seed variation by themselves. The diagnostic uses the same 40-byte
normalization and seed shuffle, but does not enter the timed matrix.

## Scheduler Probe

The 2026-09-23 host audit found no thermal or power warning and AC power, but
only four performance cores plus four efficiency cores for the eight writer
threads. At the start of the probe, load average was 4.08 / 3.98 / 4.36 and
the largest competing remote-desktop process used 184.7% CPU. The benchmark
timer begins before constructing the eight writer threads, so thread creation,
placement, and join delay are included in the reported insertion interval.

An alternating 20-process probe per implementation used the existing clean
binaries, seed 20260922, and `taskpolicy -a`. Raw wall times and process
resource counters are in `scheduler-probe.csv`; calculated values are in
`scheduler-probe-summary.json`.

| Implementation | Median | Range | Population CV |
| --- | ---: | ---: | ---: |
| 32x8 baseline | 23.017 ms | 17.822-31.183 ms | 13.44% |
| SkipList | 29.708 ms | 20.976-92.198 ms | 43.79% |

The median ratio was 0.7748x, inside the eight-writer relative gate, while the
Patricia CV still failed the 5% stability gate. Retired instruction counts
were comparatively bounded while wall time varied; one Patricia process also
recorded 1282 involuntary context switches in a 21.807 ms measured interval.
This confirms that application task policy does not isolate these short,
eight-thread processes from host scheduling. Negative nice priority was
permission-denied and passwordless elevation was unavailable. No unrelated
user process was changed or terminated.

The highest valid task-policy setting was then tested separately. The CLI
values `-l 1 -t 1` map to the SDK's `LATENCY_QOS_TIER_0` and
`THROUGHPUT_QOS_TIER_0`; zero means unspecified, not the highest tier. Across
another 20 alternating processes per implementation, Patricia measured a
23.636 ms median, 8.99% CV, and 0.7764x SkipList ratio. SkipList CV was 17.51%.
Patricia retired-instruction CV was only 1.19%, while wall-time CV remained
above the binding limit. Raw rows and calculations are in
`scheduler-tier0-probe.csv` and `scheduler-tier0-probe-summary.json`. Thus the
highest available unprivileged latency/throughput policy improves but does not
solve process-level scheduling variance.

## Low-load Revalidation

After the prior blocked audit, host load dropped to about 2.0. The binding
seed 20260922 was rerun with 20 interleaved Patricia and SkipList processes.
During samples 12-20, Spotlight metadata indexing briefly reached 339.1% CPU;
that combined sample has Patricia CV 17.86% and ratio 0.7946x. After the
indexer returned to 0% CPU, load was 2.01 / 2.03 / 2.01. A separate 20+20
quiet-host run measured Patricia median 14.892 ms, CV 7.45%, and ratio
0.7484x SkipList; SkipList CV was 7.06%. Thus throughput passes, but the
binding CV limit still fails even with no sustained heavy background task.
Raw process values are in `lowload-revalidation.csv` and
`quiet-host-revalidation.csv`; calculations are in
`lowload-revalidation-summary.json`.

The unchanged benchmark starts its timer before creating writer threads and
stops after joins. The 13-17 ms operation window is therefore comparable to
thread launch and scheduler jitter. The spec explicitly requires unchanged
benchmark source hashes, so changing timer placement is not a valid way to
claim the existing gate. The three pre-existing dirty benchmark paths remain
user-owned and untouched.

## Variance Attribution

An additional 30+30 process resource probe spans all five seeds. Radix wall
time had 7.39% CV; SkipList had 5.43%. Wall time correlated weakly with
involuntary context switches (Radix `r=0.11`) and more strongly with elapsed
cycles (Radix `r=0.83`) while instructions stayed bounded. A 15-process-per-
writer-count probe at seed 20260922 found Radix CV of 1.21%, 5.25%, and 6.30%
at 1, 4, and 8 writers, respectively; SkipList showed the same trend at
3.18%, 4.75%, and 5.89%. This supports parallel core placement/frequency
variation as the major source, rather than a Patricia-specific increase in
structural retries. It does not directly prove thread migration.

The existing untimed publication diagnostic records just 41-76 failed
segment CAS per 131072 successful inserts on the compact baseline across the
five seeds. The retry candidate converts 28-51 failures but still performs
8-18 root restarts and previously failed isolated A/B retention. The 128-byte
spacing candidate increased Arena and failed its A/B retention. Neither
approved candidate currently has evidence to restore it.

macOS affinity tags are documented by the SDK as experimental hints to share
an L2 cache, not CPU affinity; DTrace probing of scheduling events requires
root, which is unavailable non-interactively. No system process or machine
configuration was changed.

## Seed-isolated Revalidation

A separate 8-writer-only matrix removed the one/four-writer runs and used ten
interleaved processes per seed under the highest unprivileged QoS policy. The
100 raw process rows are preserved in `seed-isolated-8writer.csv`; medians and
population CVs were independently recomputed from those rows. Three seeds
exceeded the 0.85x ratio limit (20260921 0.8513x, 20260922 0.8572x,
20260923 0.8587x), and every Patricia CV remained 6.08-7.37%. The other two
seed ratios were 0.8259x and 0.8177x.

The benchmark's Fisher-Yates shuffle was then modeled with exact uint64
overflow. Per-writer segment histograms and approximate within-batch fan-in
were similar for all five seeds; same-segment maximum fan-in was only 4-5.
This agrees with the existing direct-index counters: 44-76 failed segment
CAS operations per 131072 successful inserts. These counts do not track which
seed crosses the ratio/CV limits; key-distribution skew and publication
collision frequency are not a supported root cause. The reproducible
distribution summary is `workload-distribution.json`.

A third-turn revalidation under the same Tier 0 policy used the binding ten
processes for seed 20260922. Patricia measured 27.592 ms median, 14.54% CV,
and 0.9258x SkipList; both binding gates failed. SkipList CV was 12.29%.
The host load remained 5.48 / 4.54 / 4.41 with a competing process at 125.9%
CPU. Exact samples are in `scheduler-blocked-revalidation.json`. This is the
third consecutive goal turn with the same external scheduling condition after
the approved code candidates and all unprivileged policy options were
exhausted.

## Next Gate

The approved two sub-changes are exhausted. No third representation or
publication mechanism was introduced. A fresh, low-contention Release
matrix is required to separate environmental CV from the remaining baseline
seed-20260922 ratio failure. Even if that run passes, the approved spec
requires full `MarkReadOnly`, `PrepareForFlush`, Flush/SST, WAL/restart,
bidirectional format compatibility, ASan, and TSan evidence before design
freeze. None of those new full-chain gates is claimed here.
