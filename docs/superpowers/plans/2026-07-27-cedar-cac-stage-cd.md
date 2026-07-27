# Cedar CAC Stage C/D Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完成 CAC 的恢复、checkpoint、Blob/outcome、可观测性、旧 PREPARE/Decision 协议删除，以及性能、故障、sanitizer 和 100k+ Release 验收闭环。

**Architecture:** Manifest checkpoint 与 immutable outcome index定义恢复前缀；`AtomicCommitLog` 只扫描并重放 checkpoint 之后的完整 commit segment，不再跨 PREPARE/Decision 日志 join。Checkpoint 捕获连续 visible prefix 与连续 SST coverage 的最小值，发布 SST/outcome/Manifest 后仅删除完全覆盖的 commit segments。Blob 保持 durable-before-reference；可观测性、benchmark、fault campaign 全部切换为 AtomicCommitLog 术语，最终删除旧协议实现和构建入口。

**Tech Stack:** C++17、CMake/CTest、GoogleTest、POSIX `writev`/`fdatasync`/`fsync`/`rename`、BLAKE3、CRC32C、ASAN/UBSAN/TSAN、Cedar benchmark artifact/report pipeline。

## Global Constraints

- 新数据库格式是 clean break；旧 durable transaction format 只读拒绝，不迁移、不双写、不回退。
- `AtomicCommitRecord` 是唯一 durable commit fact。
- `C = min(captured continuous VisiblePrefix, continuous SST coverage frontier)`。
- Checkpoint 不得重写 live log suffix，只能删除 `max_commit_seq <= C` 的完整 commit segment。
- Blob payload、segment Manifest liveness 和目录 publication 必须在 commit record 引用 BlobRef 前 durable。
- synchronous `committed` 只在 durable 且进入 visible prefix 后返回。
- possible durability 后的失败返回 `indeterminate`，停止 writer 并要求 reopen。
- Release 主门槛：持续窗口至少 100,000 committed single-event durable tx/s，且 reopen 验证所有 acknowledged commits。
- physical syncs per committed transaction 必须 `<= 0.25`，高并发目标 `< 0.1`。
- 完整 Debug/Release CTest、ASAN、UBSAN、TSAN、deterministic crash campaign、source inventory 全部通过后才能接受。

---

## File Structure

### 新建

- `include/cedar/transaction/cac_checkpoint.h`
  checkpoint target、coverage frontier、reclamation result 和 fault points。
- `src/transaction/cac_checkpoint.cc`
  捕获 `C`、生成 outcome index、发布 Manifest、删除完整 commit segments和旧 outcome文件。
- `tests/test_cac_recovery_checkpoint.cc`
  recovery、outcome、checkpoint、Blob ordering、segment reclaim focused tests。
- `tests/test_cac_observability_benchmark.cc`
  CAC metrics、artifact schema、report、100k gate parser测试。
- `tests/test_cac_legacy_removal.cc`
  旧 FORMAT 只读拒绝和 source inventory测试。
- `cmake/VerifyCacSourceInventory.cmake`
  验证 PREPARE/Decision实现符号、旧源文件和旧 metric/fault名已从生产源码消失。
- `benchmarks/cedar_cac_gate.cc`
  独立、可复现的 single-event durable commit Release gate runner。

### 修改

- `include/cedar/transaction/atomic_commit_log.h`
- `src/transaction/atomic_commit_log.cc`
- `include/cedar/transaction/commit_installer.h`
- `src/transaction/commit_installer.cc`
- `include/cedar/transaction/transaction_coordinator.h`
- `src/transaction/transaction_coordinator.cc`
- `include/cedar/storage/version_set.h`
- `src/storage/version_set.cc`
- `include/cedar/blob/blob_store.h`
- `src/blob/blob_store.cc`
- `src/blob/blob_gc.cc`
- `include/cedar/transaction/database_format.h`
- `src/transaction/database_format.cc`
- `include/cedar/storage/storage_layout.h`
- `include/cedar/transaction/transaction_measurements.h`
- `src/transaction/transaction_measurements.cc`
- `src/observability/production_metric_schema.cc`
- `src/db/cedar_database.cc`
- `include/cedar/benchmark/artifact_writer.h`
- `src/benchmark/artifact_writer.cc`
- `src/benchmark/artifact_reader.cc`
- `src/benchmark/report_builder.cc`
- `include/cedar/benchmark/fault_campaign.h`
- `src/benchmark/fault_campaign.cc`
- `src/benchmark/production_campaign.cc`
- `include/cedar/benchmark/workload_driver.h`
- `src/benchmark/workload_driver.cc`
- `benchmarks/cedar_bench.cc`
- `CMakeLists.txt`
- `tests/CMakeLists.txt`
- `tests/test_release_source_contract.cmake`

### 删除

阶段 D 最后删除：

- `include/cedar/transaction/decision_log.h`
- `src/transaction/decision_log.cc`

并从生产源码移除：

- `PrepareRecord`
- `PrepareReference`
- `ShardPrepareLog`
- `CommitDecision`
- `DecisionLog`
- `RecoverCommittedTransactions`
- PREPARE/Decision fault points、metrics、resource estimates和测试 hooks。

---

### Task 1: 完成 CAC retained-segment 恢复与 outcome resolution

**Files:**

- Modify: `include/cedar/transaction/atomic_commit_log.h`
- Modify: `src/transaction/atomic_commit_log.cc`
- Modify: `include/cedar/transaction/commit_installer.h`
- Modify: `src/transaction/commit_installer.cc`
- Modify: `src/transaction/transaction_coordinator.cc`
- Test: `tests/test_cac_recovery_checkpoint.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**

- Consumes:

```cpp
struct AtomicCommitRecord;
struct AtomicCommitOutcome {
  uint64_t txn_id;
  uint64_t commit_seq;
  SystemHlc system_hlc;
};

struct AtomicCommitLogCheckpoint {
  uint64_t checkpoint_seq;
  SystemHlc checkpoint_hlc;
};

class CommitInstaller {
 public:
  Status InstallRecovered(const AtomicCommitRecord& record);
};
```

- Produces:

```cpp
struct AtomicCommitRecoveryResult {
  std::vector<AtomicCommitRecord> retained_records;
  uint64_t next_commit_seq = 1;
  SystemHlc last_system_hlc;
  uint64_t truncated_tail_bytes = 0;
};

class AtomicCommitLog {
 public:
  StatusOr<AtomicCommitRecoveryResult> Open(
      const AtomicCommitLogCheckpoint& checkpoint);
  std::optional<AtomicCommitOutcome> Resolve(uint64_t txn_id) const;
  uint64_t retained_bytes() const;
};
```

- [ ] **Step 1: 添加 retained suffix recovery 的失败测试**

```cpp
TEST_F(CacRecoveryCheckpointTest,
       ReplaysRetainedSegmentsAboveManifestCheckpointWithoutLegacyJoin) {
  ASSERT_TRUE(PublishCheckpointPrefix(2, {
      AtomicCommitOutcome{101, 1, SystemHlc{100, 0}},
      AtomicCommitOutcome{102, 2, SystemHlc{101, 0}},
  }).ok());
  ASSERT_TRUE(AppendCommitSegment({
      MakeAtomicRecord(103, 3, SystemHlc{102, 0}, {{0, {PutName(3)}}}),
      MakeAtomicRecord(104, 4, SystemHlc{103, 0}, {{1, {PutName(4)}}}),
  }).ok());

  TransactionCoordinator reopened(path_, 2, 17);
  ASSERT_TRUE(reopened.Open().ok());
  EXPECT_EQ(reopened.visible_seq(), 4U);
  EXPECT_EQ(reopened.ResolveTransaction(101).ValueOrDie(), 1U);
  EXPECT_EQ(reopened.ResolveTransaction(104).ValueOrDie(), 4U);
  EXPECT_EQ(reopened.Get(NameKey(4), 1), Value::String("name-4"));
}
```

同时添加：

- torn final frame被截断且 segment/directory同步；
- complete checksum corruption拒绝 open；
- segment gap、sequence gap、duplicate seq、duplicate txn_id、HLC regression拒绝 open；
- exact replay成功、conflicting replay返回 corruption；
- retained outcome map 与 checkpoint outcome index 两级 `Resolve(txn_id)`。

- [ ] **Step 2: 注册 focused test target**

```cmake
add_executable(test_cac_recovery_checkpoint
    test_cac_recovery_checkpoint.cc)
target_link_libraries(test_cac_recovery_checkpoint ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_cac_recovery_checkpoint)
```

- [ ] **Step 3: 运行 RED**

Run:

```bash
cmake -S . -B build-cac-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R 'CacRecoveryCheckpointTest.*(ReplaysRetained|TruncatesTorn|RejectsComplete|RejectsSegmentGap|RejectsSequenceGap|RejectsDuplicateTxn|RejectsHlcRegression)' \
  --output-on-failure
```

Expected: FAIL，因为 `AtomicCommitLog::Open(checkpoint)` 尚未返回 retained records，Coordinator 仍未按 CAC 直接 replay。

- [ ] **Step 4: 实现严格 segment scan**

核心实现形状：

```cpp
StatusOr<AtomicCommitRecoveryResult> AtomicCommitLog::Open(
    const AtomicCommitLogCheckpoint& checkpoint) {
  AtomicCommitRecoveryResult result;
  result.next_commit_seq = checkpoint.checkpoint_seq + 1;
  result.last_system_hlc = checkpoint.checkpoint_hlc;

  CEDAR_ASSIGN_OR_RETURN(auto segments, ListCommitSegments(directory_));
  uint64_t expected_segment = segments.empty() ? 1 : segments.front().number;
  uint64_t expected_seq = checkpoint.checkpoint_seq + 1;

  for (const CommitSegmentFile& segment : segments) {
    if (segment.number != expected_segment++) {
      return Status::Corruption("atomic commit log", "segment gap");
    }
    CEDAR_ASSIGN_OR_RETURN(
        CommitSegmentScan scan,
        ScanCommitSegment(segment, database_identity_, expected_seq,
                          result.last_system_hlc));
    result.truncated_tail_bytes += scan.truncated_tail_bytes;
    for (AtomicCommitRecord& record : scan.records) {
      if (record.commit_seq <= checkpoint.checkpoint_seq) continue;
      if (record.commit_seq != expected_seq++) {
        return Status::Corruption("atomic commit log", "sequence gap");
      }
      CEDAR_RETURN_IF_ERROR(PublishRecoveredOutcome(record));
      result.last_system_hlc = record.system_hlc;
      result.retained_records.push_back(std::move(record));
    }
  }
  result.next_commit_seq = expected_seq;
  return result;
}
```

`TransactionCoordinator::OpenInternal()` 必须按以下顺序执行：

```cpp
LoadCheckpointOutcomes();
RestorePublishedSstEvents();
commit_timeline_.RestoreFromOutcomes(checkpoint_outcomes_);
visible_prefix_.RestorePersistedPrefix(checkpoint.checkpoint_seq);

auto recovery = atomic_commit_log_.Open({
    checkpoint.checkpoint_seq, checkpoint.system_hlc});
for (const AtomicCommitRecord& record :
     recovery.ValueOrDie().retained_records) {
  CEDAR_RETURN_IF_ERROR(commit_installer_.InstallRecovered(record));
}
```

- [ ] **Step 5: 运行 GREEN**

Run:

```bash
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R 'CacRecoveryCheckpointTest' --output-on-failure
```

Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add include/cedar/transaction/atomic_commit_log.h \
        src/transaction/atomic_commit_log.cc \
        include/cedar/transaction/commit_installer.h \
        src/transaction/commit_installer.cc \
        src/transaction/transaction_coordinator.cc \
        tests/test_cac_recovery_checkpoint.cc \
        tests/CMakeLists.txt
git commit -m "feat: recover CAC records from retained commit segments"
```

---

### Task 2: 实现连续 visible/SST checkpoint 与完整 segment reclaim

**Files:**

- Create: `include/cedar/transaction/cac_checkpoint.h`
- Create: `src/transaction/cac_checkpoint.cc`
- Modify: `include/cedar/storage/version_set.h`
- Modify: `src/storage/version_set.cc`
- Modify: `include/cedar/transaction/atomic_commit_log.h`
- Modify: `src/transaction/atomic_commit_log.cc`
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`
- Modify: `src/db/cedar_database.cc`
- Test: `tests/test_cac_recovery_checkpoint.cc`
- Modify: `CMakeLists.txt`

**Interfaces:**

- Produces:

```cpp
struct CacCheckpointTarget {
  uint64_t captured_visible_seq = 0;
  uint64_t continuous_sst_coverage_seq = 0;

  uint64_t checkpoint_seq() const {
    return std::min(captured_visible_seq, continuous_sst_coverage_seq);
  }
};

struct CommitSegmentReclaimResult {
  uint64_t reclaimed_segments = 0;
  uint64_t reclaimed_bytes = 0;
};

enum class CacCheckpointFaultPoint : uint8_t {
  kAfterSstSync,
  kAfterOutcomeFileSync,
  kAfterOutcomeRename,
  kAfterOutcomeDirectorySync,
  kAfterManifestRename,
  kBeforeCommitSegmentDelete,
  kAfterCommitSegmentDelete,
  kBeforeCommitDirectorySync,
  kAfterObsoleteOutcomeDelete,
};

class AtomicCommitLog {
 public:
  StatusOr<CommitSegmentReclaimResult> ReclaimSegmentsThrough(
      uint64_t checkpoint_seq);
};

class CacCheckpointManager {
 public:
  Status Run();
};
```

`DurableCheckpoint` 调整为：

```cpp
struct DurableCheckpoint {
  uint64_t checkpoint_seq = 0;
  uint64_t manifest_generation = 0;
  SystemHlc system_hlc;
  std::string outcome_index_relative_path;
  std::array<uint8_t, 32> outcome_index_checksum{};
  std::vector<uint64_t> shard_coverage_frontiers;
};
```

删除 `decision_safe_seq` 和 `wal_safe_lsns`。

- [ ] **Step 1: 写 checkpoint RED 测试**

必须包含：

```cpp
TEST_F(CacRecoveryCheckpointTest,
       CheckpointExcludesDurableButNotVisibleRecord) {
  BlockInstallForCommit(2);
  ASSERT_TRUE(CommitAndDurablyQueue(1).ok());
  ASSERT_TRUE(CommitAndDurablyQueue(2).IsIndeterminate());
  ASSERT_TRUE(FlushCoverageThrough(2).ok());

  ASSERT_TRUE(coordinator_->CheckpointDurableLogs().ok());
  EXPECT_EQ(coordinator_->version_snapshot()->checkpoint.checkpoint_seq, 1U);
  EXPECT_TRUE(CommitSegmentContaining(2).exists());
}
```

以及：

- shard watermark为 5 但 coverage缺口在 3 时，checkpoint只能到 2；
- outcome durable、Manifest未发布时不能删 segment；
- Manifest已发布、删除失败时 reopen仍成功且只多占空间；
- 跨 checkpoint边界的 segment不得删除；
- `max_commit_seq <= C` 的 segment必须删除并 fsync commit directory；
- obsolete outcome index只在新 Manifest durable后删除；
- concurrent commit继续写更高 segment，checkpoint不阻塞 later generation。

- [ ] **Step 2: 运行 RED**

```bash
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R 'CacRecoveryCheckpointTest.*Checkpoint' --output-on-failure
```

Expected: FAIL，当前逻辑仍以最大 watermark或旧 WAL safe LSN为边界。

- [ ] **Step 3: 实现 checkpoint manager**

核心顺序必须固定：

```cpp
Status CacCheckpointManager::Run() {
  const uint64_t captured_visible = visible_prefix_->visible_seq();
  CEDAR_ASSIGN_OR_RETURN(
      uint64_t coverage,
      flush_->FlushContinuousPrefix(captured_visible));

  const uint64_t checkpoint_seq = std::min(captured_visible, coverage);
  if (checkpoint_seq <= versions_->Snapshot()->checkpoint.checkpoint_seq) {
    return Status::OK();
  }

  CEDAR_ASSIGN_OR_RETURN(
      std::vector<AtomicCommitOutcome> outcomes,
      outcomes_->CompletePrefix(checkpoint_seq));

  CEDAR_ASSIGN_OR_RETURN(
      OutcomeIndexPublication publication,
      WriteTransactionOutcomeIndexAtomically(
          checkpoint_directory_, outcomes, fault_injector_));

  VersionEdit edit;
  edit.checkpoint = DurableCheckpoint{
      checkpoint_seq,
      0,
      outcomes.back().system_hlc,
      publication.relative_path,
      publication.checksum,
      flush_->ShardCoverageFrontiers()};
  CEDAR_RETURN_IF_ERROR(versions_->ApplyEdit(edit));

  CEDAR_RETURN_IF_ERROR(log_->ReclaimSegmentsThrough(checkpoint_seq));
  return DeleteObsoleteOutcomeIndexes(
      versions_->Snapshot()->checkpoint.outcome_index_relative_path);
}
```

`ReclaimSegmentsThrough`：

```cpp
for (const CommitSegmentMeta& segment : segments_) {
  if (segment.active || segment.max_commit_seq > checkpoint_seq) continue;
  CEDAR_RETURN_IF_ERROR(DeleteCommitSegment(segment));
  ++result.reclaimed_segments;
  result.reclaimed_bytes += segment.file_size;
}
CEDAR_RETURN_IF_ERROR(FsyncDirectory(directory_));
```

- [ ] **Step 4: 运行 GREEN**

```bash
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R 'CacRecoveryCheckpointTest.*Checkpoint' --output-on-failure
```

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add include/cedar/transaction/cac_checkpoint.h \
        src/transaction/cac_checkpoint.cc \
        include/cedar/storage/version_set.h \
        src/storage/version_set.cc \
        include/cedar/transaction/atomic_commit_log.h \
        src/transaction/atomic_commit_log.cc \
        include/cedar/transaction/transaction_coordinator.h \
        src/transaction/transaction_coordinator.cc \
        src/db/cedar_database.cc \
        tests/test_cac_recovery_checkpoint.cc \
        CMakeLists.txt
git commit -m "feat: checkpoint continuous CAC visibility and reclaim segments"
```

---

### Task 3: 封闭 Blob durable-before-reference 与 checkpoint/GC 故障边界

**Files:**

- Modify: `include/cedar/blob/blob_store.h`
- Modify: `src/blob/blob_store.cc`
- Modify: `src/blob/blob_gc.cc`
- Modify: `src/transaction/transaction_coordinator.cc`
- Test: `tests/test_cac_recovery_checkpoint.cc`

**Interfaces:**

- Produces：

```cpp
struct BlobDurabilityStats {
  uint64_t segment_sync_count = 0;
  uint64_t segment_sync_latency_ns = 0;
  uint64_t index_sync_count = 0;
  uint64_t index_sync_latency_ns = 0;
  uint64_t orphan_bytes = 0;
  uint64_t reclaimed_segment_count = 0;
  uint64_t reclaimed_segment_bytes = 0;
  uint64_t retired_pending_reader_bytes = 0;
};

enum class BlobStoreFaultPoint : uint8_t {
  kAfterPartialRecordWrite,
  kAfterRecordFsync,
  kBeforeRecordDirectoryFsync,
  kAfterPartialIndexWrite,
  kAfterIndexFsync,
  kBeforeIndexDirectoryFsync,
  kAfterCheckpointFileFsync,
  kAfterCheckpointRename,
  kBeforeCheckpointDirectoryFsync,
  kBeforeSegmentDelete,
  kAfterSegmentDelete,
  kBeforeSegmentDeleteDirectoryFsync,
};
```

- [ ] **Step 1: 写 Blob ordering RED 测试**

```cpp
TEST_F(CacRecoveryCheckpointTest,
       AtomicCommitAppendNeverPrecedesBlobDurabilityAndManifestLiveness) {
  std::vector<std::string> barriers;
  blob_->SetBarrierObserverForTesting(
      [&](BlobDurabilityBarrier barrier) {
        barriers.push_back(BlobDurabilityBarrierName(barrier));
      });
  log_->SetAppendObserverForTesting(
      [&] { barriers.push_back("atomic_commit_append"); });

  ASSERT_TRUE(PutLargeBlobValue().ok());
  EXPECT_EQ(barriers, std::vector<std::string>({
      "blob_segment_fsync",
      "blob_segment_directory_fsync",
      "blob_index_fsync",
      "blob_index_directory_fsync",
      "blob_manifest_publish",
      "atomic_commit_append",
  }));
}
```

另加：

- segment fsync成功、INDEX失败：commit log无 record，reopen后 blob仅为 collectible orphan；
- Manifest publication失败：commit log无引用；
- abort后 Blob可由GC回收；
- pinned VersionSnapshot下 retired segment不删除；
- unlink后 directory fsync失败必须 recovery-required；
- stale BlobRef经 hash relocation仍能解析。

- [ ] **Step 2: 运行 RED**

```bash
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R 'CacRecoveryCheckpointTest.*Blob' --output-on-failure
```

Expected: FAIL，新 barrier observer/stats/fault points尚不存在。

- [ ] **Step 3: 实现并固定 barrier顺序**

`AppendBlobBlocksLocked` 保持：

```cpp
CEDAR_ASSIGN_OR_RETURN(
    DurableAppendResult payload,
    AppendDurably(segment_path, encoded_blocks,
                  BlobDurabilityBarrier::kSegmentFsync));
RecordSegmentSync(payload.fsync_latency_ns);

CEDAR_ASSIGN_OR_RETURN(
    DurableAppendResult index,
    AppendDurably(IndexPath(shard_id), index_records,
                  BlobDurabilityBarrier::kIndexFsync));
RecordIndexSync(index.fsync_latency_ns);

PublishLocationsInMemory(requests, locations);
return BuildBlobRefs(requests, locations);
```

Coordinator 入队前必须：

```cpp
CEDAR_RETURN_IF_ERROR(EnsureBlobSegmentsManifested());
CEDAR_ASSIGN_OR_RETURN(auto refs, blob_store_.PutBatch(payloads));
CEDAR_RETURN_IF_ERROR(
    ValidateBlobSegmentsManifestLive(refs, *version_set_.Snapshot()));
return commit_queue_.Enqueue(BuildAtomicCommitRecord(refs));
```

- [ ] **Step 4: 运行 GREEN**

```bash
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R 'CacRecoveryCheckpointTest.*Blob' --output-on-failure
```

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add include/cedar/blob/blob_store.h \
        src/blob/blob_store.cc \
        src/blob/blob_gc.cc \
        src/transaction/transaction_coordinator.cc \
        tests/test_cac_recovery_checkpoint.cc
git commit -m "feat: enforce CAC blob durability barriers"
```

---

### Task 4: 切换 CAC 可观测性、artifact 和 report schema

**Files:**

- Modify: `include/cedar/transaction/transaction_measurements.h`
- Modify: `src/transaction/transaction_measurements.cc`
- Modify: `src/observability/production_metric_schema.cc`
- Modify: `src/db/cedar_database.cc`
- Modify: `include/cedar/benchmark/artifact_writer.h`
- Modify: `src/benchmark/artifact_writer.cc`
- Modify: `src/benchmark/artifact_reader.cc`
- Modify: `src/benchmark/report_builder.cc`
- Create: `tests/test_cac_observability_benchmark.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**

新增 measurement kinds：

```cpp
enum class TransactionMeasurementKind : uint8_t {
  kStarted,
  kTerminal,
  kOccValidationLatency,
  kReservationLatency,
  kCommitQueueDelay,
  kAtomicLogSyncLatency,
  kDurableToInstalledLatency,
  kParticipantInstallLatency,
  kVisiblePrefixWait,
  kCheckpointLatency,
};
```

新增 snapshot/window：

```cpp
Histogram atomic_log_sync_latency;
Histogram commit_queue_delay;
Histogram durable_to_installed_latency;
Histogram participant_install_latency;
Histogram checkpoint_latency;
uint64_t atomic_log_logical_bytes = 0;
uint64_t atomic_log_physical_bytes = 0;
uint64_t atomic_log_sync_count = 0;
uint64_t generation_records = 0;
uint64_t generation_bytes = 0;
uint64_t generation_leaders = 0;
uint64_t generation_followers = 0;
uint64_t recovery_replayed_records = 0;
uint64_t recovery_truncated_tail_bytes = 0;
uint64_t reclaimed_commit_segments = 0;
```

- [ ] **Step 1: 写 metric schema RED 测试**

断言存在：

```text
cedar_cac_logical_bytes_total
cedar_cac_physical_bytes_total
cedar_cac_queue_depth
cedar_cac_queued_bytes
cedar_cac_generation_records
cedar_cac_generation_bytes
cedar_cac_sync_total
cedar_cac_sync_latency_ns
cedar_cac_syncs_per_committed_txn
cedar_cac_durable_to_installed_latency_ns
cedar_cac_participant_install_latency_ns
cedar_cac_install_parallelism
cedar_cac_recovery_replayed_records_total
cedar_cac_recovery_truncated_tail_bytes_total
cedar_cac_reclaimed_segments_total
```

断言不存在：

```text
cedar_wal_fsync_latency_ns
cedar_decisionlog_fsync_latency_ns
cedar_wal_append_bytes_total
PREPARE latency
Decision latency
DecisionLog durable bytes
```

Artifact schema从 `3` 升到 `4`，reader拒绝 schema 3，report使用 “AtomicCommitLog”。

- [ ] **Step 2: 运行 RED**

```bash
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R 'CacObservabilityBenchmarkTest' --output-on-failure
```

Expected: FAIL，旧 PREPARE/Decision字段仍存在且 schema_version仍为3。

- [ ] **Step 3: 实现 metric publication 与 schema 4**

Artifact writer输出：

```json
"cac_measurements":{
  "atomic_log_sync_count":8,
  "committed":64,
  "syncs_per_committed_txn":{
    "defined":true,
    "numerator":8,
    "denominator":64
  },
  "generation_records":64,
  "generation_bytes":16384,
  "recovery_replayed_records":0,
  "recovery_truncated_tail_bytes":0,
  "reclaimed_commit_segments":2
}
```

Report必须输出：

```markdown
| AtomicCommitLog sync latency | ... |
| Physical syncs / committed transaction | 8 / 64 |
| Durable-to-installed latency | ... |
| Participant install latency | ... |
| Install parallelism | ... |
| Reclaimed commit segments | 2 |
```

- [ ] **Step 4: 运行 GREEN**

```bash
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R 'CacObservabilityBenchmarkTest' --output-on-failure
```

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add include/cedar/transaction/transaction_measurements.h \
        src/transaction/transaction_measurements.cc \
        src/observability/production_metric_schema.cc \
        src/db/cedar_database.cc \
        include/cedar/benchmark/artifact_writer.h \
        src/benchmark/artifact_writer.cc \
        src/benchmark/artifact_reader.cc \
        src/benchmark/report_builder.cc \
        tests/test_cac_observability_benchmark.cc \
        tests/CMakeLists.txt
git commit -m "feat: publish CAC metrics and benchmark artifacts"
```

---

### Task 5: 扩充 checkpoint、recovery、Blob deterministic fault campaign

**Files:**

- Modify: `include/cedar/benchmark/fault_campaign.h`
- Modify: `src/benchmark/fault_campaign.cc`
- Modify: `src/benchmark/production_campaign.cc`
- Modify: `benchmarks/cedar_bench.cc`
- Test: `tests/test_cac_recovery_checkpoint.cc`
- Test: `tests/test_cac_observability_benchmark.cc`

**Interfaces:**

```cpp
enum class BenchmarkFaultScenario : uint8_t {
  kCacPartialFrameWrite,
  kCacAfterFrameWrite,
  kCacAfterSync,
  kCacParticipantInstall,
  kCacDuplicateReplay,
  kCacTornTailRecovery,
  kCacCheckpointAfterSstSync,
  kCacCheckpointAfterOutcomeSync,
  kCacCheckpointAfterOutcomeRename,
  kCacCheckpointAfterManifestRename,
  kCacCheckpointAfterSegmentDelete,
  kCacBlobAfterSegmentSync,
  kCacBlobAfterIndexSync,
  kCacBlobAfterManifestRename,
  kCacShutdownActiveGeneration,
};
```

- [ ] **Step 1: 写 fault scenario RED 测试**

```cpp
TEST(CacObservabilityBenchmarkTest, ProductionCampaignContainsEveryCacFault) {
  const auto plan = BuildProductionCampaignPlan(config);
  std::set<std::string> faults;
  for (const auto& command : plan.ValueOrDie()) {
    if (!command.fault_scenario.empty()) faults.insert(command.fault_scenario);
  }
  EXPECT_EQ(faults, ExpectedCacFaultScenarioNames());
}
```

每个场景必须：

- 注入一次；
- 销毁 process-local DB；
- reopen；
- 验证 acknowledged/indeterminate/absent结果；
- 验证 visible prefix连续；
- 验证无 dangling BlobRef；
- 验证 retained segment/outcome authority。

- [ ] **Step 2: 运行 RED**

```bash
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R '.*Cac.*Fault.*' --output-on-failure
```

Expected: FAIL，production campaign仍列举旧 PREPARE/Decision场景。

- [ ] **Step 3: 实现场景与 verification detail**

```cpp
std::string BenchmarkFaultVerificationDetail(
    BenchmarkFaultScenario scenario) {
  return "expected CAC fault, durable reopen, contiguous visibility, "
         "outcome resolution, and value verification: " +
      std::string(BenchmarkFaultScenarioName(scenario));
}
```

`kCacAfterSync` 和 participant install fault预期 `indeterminate`，reopen后 committed；partial final frame预期 absent；Manifest rename后的 checkpoint fault允许旧或新 authoritative generation，但必须可 reopen且不丢 acknowledged commit。

- [ ] **Step 4: 运行 GREEN**

```bash
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R '.*Cac.*Fault.*' --output-on-failure
```

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add include/cedar/benchmark/fault_campaign.h \
        src/benchmark/fault_campaign.cc \
        src/benchmark/production_campaign.cc \
        benchmarks/cedar_bench.cc \
        tests/test_cac_recovery_checkpoint.cc \
        tests/test_cac_observability_benchmark.cc
git commit -m "test: add deterministic CAC crash campaign"
```

---

### Task 6: Clean-break format拒绝并删除旧 PREPARE/Decision协议

**Files:**

- Modify: `include/cedar/transaction/database_format.h`
- Modify: `src/transaction/database_format.cc`
- Modify: `include/cedar/storage/storage_layout.h`
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`
- Delete: `include/cedar/transaction/decision_log.h`
- Delete: `src/transaction/decision_log.cc`
- Create: `tests/test_cac_legacy_removal.cc`
- Create: `cmake/VerifyCacSourceInventory.cmake`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`
- Modify: `tests/test_release_source_contract.cmake`

**Interfaces:**

新 FORMAT 字段：

```cpp
struct DatabaseFormat {
  uint32_t format_version = kCedarDatabaseFormatVersion;
  uint32_t shard_count = 0;
  uint64_t hash_seed = 0;
  std::array<uint8_t, 32> database_identity{};
  std::string manifest_location = "manifest/MANIFEST";
  std::string atomic_commit_log_directory = "commit";
};
```

删除：

```cpp
std::string decision_log_location;
std::vector<std::string> shard_wal_locations;
```

- [ ] **Step 1: 写旧格式只读拒绝 RED 测试**

```cpp
TEST_F(CacLegacyRemovalTest,
       OldFormatRejectionDoesNotTouchPrepareOrDecisionBytes) {
  WriteOldFormatV2();
  WriteExactBytes("decision/DECISION", "decision-sentinel");
  WriteExactBytes("shards/0/wal/PREPARE.1", "prepare-sentinel");

  const DirectoryDigest before = DigestDatabaseTree(path_);
  TransactionCoordinator coordinator(path_, 1, 17);
  EXPECT_TRUE(coordinator.Open().IsNotSupportedError());
  const DirectoryDigest after = DigestDatabaseTree(path_);

  EXPECT_EQ(after, before);
}
```

该 digest 必须覆盖路径、文件长度、BLAKE3 内容和mtime；测试不得调用会写 `.tmp`、truncate、rename或delete的旧 log open。

- [ ] **Step 2: 写 source inventory RED 检查**

`cmake/VerifyCacSourceInventory.cmake`：

```cmake
set(FORBIDDEN_CAC_LEGACY_SYMBOLS
    PrepareRecord
    PrepareReference
    ShardPrepareLog
    CommitDecision
    DecisionLog
    RecoverCommittedTransactions
    DecisionLogFaultPoint
    EstimateDurableCommitWriteBytes)

set(FORBIDDEN_CAC_LEGACY_METRICS
    cedar_wal_fsync_latency_ns
    cedar_decisionlog_fsync_latency_ns
    cedar_wal_append_bytes_total)

foreach(ROOT include/cedar src)
  file(GLOB_RECURSE SOURCES
       "${SOURCE_ROOT}/${ROOT}/*.h"
       "${SOURCE_ROOT}/${ROOT}/*.cc")
  foreach(SOURCE IN LISTS SOURCES)
    file(READ "${SOURCE}" CONTENTS)
    foreach(SYMBOL IN LISTS FORBIDDEN_CAC_LEGACY_SYMBOLS
                            FORBIDDEN_CAC_LEGACY_METRICS)
      if(CONTENTS MATCHES "(^|[^A-Za-z0-9_])${SYMBOL}([^A-Za-z0-9_]|$)")
        message(FATAL_ERROR
                "CAC legacy source inventory violation: ${SYMBOL} in ${SOURCE}")
      endif()
    endforeach()
  endforeach()
endforeach()

foreach(FILE
    "${SOURCE_ROOT}/include/cedar/transaction/decision_log.h"
    "${SOURCE_ROOT}/src/transaction/decision_log.cc")
  if(EXISTS "${FILE}")
    message(FATAL_ERROR "CAC legacy file remains: ${FILE}")
  endif()
endforeach()
```

注册：

```cmake
add_test(NAME cac_source_inventory
  COMMAND ${CMAKE_COMMAND}
    -DSOURCE_ROOT=${CMAKE_SOURCE_DIR}
    -P ${CMAKE_SOURCE_DIR}/cmake/VerifyCacSourceInventory.cmake)
```

- [ ] **Step 3: 运行 RED**

```bash
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R 'CacLegacyRemovalTest|cac_source_inventory' \
  --output-on-failure
```

Expected: FAIL，旧文件、符号、metrics和FORMAT字段仍存在。

- [ ] **Step 4: 删除旧协议并切换 Coordinator**

Coordinator成员只保留：

```cpp
AtomicCommitLog atomic_commit_log_;
CommitQueue commit_queue_;
CommitInstaller commit_installer_;
CacCheckpointManager checkpoint_manager_;
```

构造路径：

```cpp
atomic_commit_log_(
    db_path_ + "/" + storage_layout::kAtomicCommitLogDirectory,
    database_identity)
```

`OpenInternal()` 必须先 `CreateOrValidateDatabaseFormat()`；FORMAT version不匹配立即返回，之后才能打开 Manifest、Blob、commit segments。

- [ ] **Step 5: 运行 GREEN**

```bash
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R 'CacLegacyRemovalTest|cac_source_inventory|test_release_source_contract' \
  --output-on-failure
```

Expected: PASS；生产 `src/` 和 `include/` 无旧实现符号。

- [ ] **Step 6: 提交**

```bash
git add -A include/cedar/transaction \
           src/transaction \
           include/cedar/storage/storage_layout.h \
           cmake/VerifyCacSourceInventory.cmake \
           CMakeLists.txt \
           tests
git commit -m "refactor: remove PREPARE and Decision commit protocol"
```

---

### Task 7: 增加独立 CAC Release throughput gate 与报告校验

**Files:**

- Create: `benchmarks/cedar_cac_gate.cc`
- Modify: `include/cedar/benchmark/workload_driver.h`
- Modify: `src/benchmark/workload_driver.cc`
- Modify: `src/benchmark/artifact_writer.cc`
- Modify: `src/benchmark/artifact_reader.cc`
- Modify: `src/benchmark/report_builder.cc`
- Modify: `CMakeLists.txt`
- Test: `tests/test_cac_observability_benchmark.cc`

**Interfaces:**

```cpp
struct CacReleaseGateConfig {
  uint64_t transaction_count = 2'000'000;
  uint32_t worker_count = 32;
  uint32_t shard_count = 4;
  uint32_t queue_capacity = 65'536;
  uint64_t value_bytes = 32;
  uint64_t measurement_seconds = 20;
};

struct CacReleaseGateResult {
  double committed_txn_per_second = 0.0;
  BenchmarkRatio physical_syncs_per_committed_txn;
  uint64_t acknowledged_commits = 0;
  uint64_t reopened_visible_seq = 0;
  std::string acknowledged_commit_checksum;
  std::string reopened_commit_checksum;
};
```

- [ ] **Step 1: 写 gate result RED 测试**

```cpp
TEST(CacObservabilityBenchmarkTest,
     ReleaseGateRejectsThroughputBelowOneHundredThousand) {
  CacReleaseGateResult result;
  result.committed_txn_per_second = 99'999.0;
  result.physical_syncs_per_committed_txn = {25, 100};
  result.acknowledged_commits = 1'000'000;
  result.reopened_visible_seq = 1'000'000;
  result.acknowledged_commit_checksum = "same";
  result.reopened_commit_checksum = "same";

  EXPECT_TRUE(ValidateCacReleaseGate(result).IsResourceExhausted());
  result.committed_txn_per_second = 100'000.0;
  EXPECT_TRUE(ValidateCacReleaseGate(result).ok());
}
```

另验证：

- sync ratio `> 0.25` 失败；
- reopen visible/checksum不一致失败；
- indeterminate非零失败；
- report缺 p50/p95/p99、generation size、queue delay、install lag时失败。

- [ ] **Step 2: 运行 RED**

```bash
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R 'CacObservabilityBenchmarkTest.*ReleaseGate' \
  --output-on-failure
```

Expected: FAIL，gate API不存在。

- [ ] **Step 3: 实现独立 Release gate executable**

CMake：

```cmake
add_executable(cedar_cac_gate benchmarks/cedar_cac_gate.cc)
target_link_libraries(cedar_cac_gate PRIVATE cedar)
```

命令行：

```text
cedar_cac_gate <results-root> <seed> <transactions> <workers>
               <shards> <queue-capacity> <value-bytes> <seconds>
```

runner必须：

1. 创建全新数据库；
2. load/warm-up不计时；
3. 测量期间每事务一个 event，同步等待 commit结果；
4. 停止计时后记录 acknowledged txn_id/commit_seq checksum；
5. close/reopen；
6. 验证 visible_seq、所有 acknowledged outcomes和checksum；
7. 写 artifact/report/environment；
8. 调用 `ValidateCacReleaseGate()`。

- [ ] **Step 4: 运行 GREEN**

```bash
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug \
  -R 'CacObservabilityBenchmarkTest.*ReleaseGate' \
  --output-on-failure
```

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add benchmarks/cedar_cac_gate.cc \
        include/cedar/benchmark/workload_driver.h \
        src/benchmark/workload_driver.cc \
        src/benchmark/artifact_writer.cc \
        src/benchmark/artifact_reader.cc \
        src/benchmark/report_builder.cc \
        tests/test_cac_observability_benchmark.cc \
        CMakeLists.txt
git commit -m "perf: add CAC 100k durable commit release gate"
```

---

### Task 8: 完成 Debug/Release、fault、sanitizer 和 100k+ 验收闭环

**Files:**

- Modify only when a failing gate exposes a defect in files from Tasks 1–7.
- Evidence output: `results/cac-release-gate/`
- No benchmark threshold weakening is permitted.

- [ ] **Step 1: clean Debug build/full CTest**

```bash
rm -rf build-cac-debug
cmake -S . -B build-cac-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCEDAR_MINIMAL_INSTRUMENTATION=OFF
cmake --build build-cac-debug -j
ctest --test-dir build-cac-debug --output-on-failure
```

Expected: 100% tests passed。

- [ ] **Step 2: clean Release build/full CTest**

```bash
rm -rf build-cac-release
cmake -S . -B build-cac-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DCEDAR_MINIMAL_INSTRUMENTATION=OFF
cmake --build build-cac-release -j
ctest --test-dir build-cac-release --output-on-failure
```

Expected: 100% tests passed。

- [ ] **Step 3: deterministic crash campaign**

```bash
for scenario in \
  cac_partial_frame_write \
  cac_after_frame_write \
  cac_after_sync \
  cac_participant_install \
  cac_duplicate_replay \
  cac_torn_tail_recovery \
  cac_checkpoint_after_sst_sync \
  cac_checkpoint_after_outcome_sync \
  cac_checkpoint_after_outcome_rename \
  cac_checkpoint_after_manifest_rename \
  cac_checkpoint_after_segment_delete \
  cac_blob_after_segment_sync \
  cac_blob_after_index_sync \
  cac_blob_after_manifest_rename \
  cac_shutdown_active_generation
do
  ./build-cac-release/cedar_bench \
    --fault "$scenario" workstation 20260727 \
    "results/cac-fault/$scenario"
done
```

Expected: 每个 run 的 verification status为 PASS，reopen verification通过。

- [ ] **Step 4: ASAN**

```bash
rm -rf build-cac-asan
cmake -S . -B build-cac-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCEDAR_ENABLE_ASAN=ON
cmake --build build-cac-asan -j
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
ctest --test-dir build-cac-asan \
  -R 'Cac|AtomicCommit|Blob|Checkpoint' \
  --output-on-failure
```

Expected: PASS，无 sanitizer report。

- [ ] **Step 5: UBSAN**

```bash
rm -rf build-cac-ubsan
cmake -S . -B build-cac-ubsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCEDAR_ENABLE_UBSAN=ON
cmake --build build-cac-ubsan -j
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
ctest --test-dir build-cac-ubsan \
  -R 'Cac|AtomicCommit|Blob|Checkpoint' \
  --output-on-failure
```

Expected: PASS，无 undefined behavior。

- [ ] **Step 6: TSAN**

```bash
rm -rf build-cac-tsan
cmake -S . -B build-cac-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCEDAR_ENABLE_TSAN=ON
cmake --build build-cac-tsan -j
TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 \
ctest --test-dir build-cac-tsan \
  -R 'Cac|AtomicCommit|Parallel|Concurrent|Checkpoint' \
  --output-on-failure
```

Expected: PASS，无 data race或lock-order inversion。

- [ ] **Step 7: source inventory与 release source contract**

```bash
ctest --test-dir build-cac-release \
  -R 'cac_source_inventory|test_release_source_contract' \
  --output-on-failure

rg -n \
  'PrepareRecord|PrepareReference|ShardPrepareLog|CommitDecision|DecisionLog|RecoverCommittedTransactions|DecisionLogFaultPoint|EstimateDurableCommitWriteBytes|cedar_wal_fsync_latency_ns|cedar_decisionlog_fsync_latency_ns' \
  include/cedar src CMakeLists.txt
```

Expected:

- CTest PASS；
- `rg` exit code 1，无输出；
- `include/cedar/transaction/decision_log.h` 和 `src/transaction/decision_log.cc` 不存在；
- `CMakeLists.txt` 不再列出旧源文件。

- [ ] **Step 8: 三轮 clean Release 100k+ gate**

每轮使用独立结果目录和全新数据库：

```bash
for round in 1 2 3
do
  ./build-cac-release/cedar_cac_gate \
    "results/cac-release-gate/round-$round" \
    20260727 \
    2000000 \
    32 \
    4 \
    65536 \
    32 \
    20
done
```

每轮 Expected：

```text
status=PASS
committed_txn_per_second>=100000
physical_syncs_per_committed_txn<=0.25
acknowledged_commits=2000000
reopened_visible_seq=2000000
acknowledged_commit_checksum==reopened_commit_checksum
indeterminate=0
```

三轮都必须通过；不得用三轮平均掩盖某一轮低于100k。

- [ ] **Step 9: secondary performance gates**

```bash
for workers in 1 2 4 8 16 32 64
do
  ./build-cac-release/cedar_cac_gate \
    "results/cac-scaling/workers-$workers" \
    20260727 1000000 "$workers" 4 65536 32 15
done

./build-cac-release/cedar_bench \
  --profile workstation 20260727 \
  results/cac-htap htap-balanced \
  steady_state_with_background_maintenance

./build-cac-release/cedar_bench \
  --profile workstation 20260727 \
  results/cac-point point-read \
  warm_full_working_set
```

Expected：

- 高并发 sync/txn目标 `< 0.1`；
- one-shard和multi-shard都只走一个 AtomicCommitLog sync path；
- HTAP各类操作都有进展；
- report包含 p50/p95/p99、generation records/bytes、queue delay、install lag、abort/indeterminate rate。

- [ ] **Step 10: 最终提交**

```bash
git add -A
git commit -m "test: close CAC recovery performance and sanitizer gates"
```

---

## Final Acceptance Checklist

- [ ] Recovery只读取 Manifest checkpoint、outcome index和 commit segments。
- [ ] 无 PREPARE/Decision join、双写或fallback。
- [ ] torn physical tail可安全截断；完整 corruption拒绝 open。
- [ ] `Resolve(txn_id)` 同时覆盖 retained outcome map和checkpoint outcome index。
- [ ] Checkpoint严格使用连续 visible/SST coverage的最小值。
- [ ] 只删除 `max_commit_seq <= C` 的完整 commit segments。
- [ ] 新 Manifest durable前不删除log或旧 outcome index。
- [ ] Blob durable-before-reference所有 barrier都有测试和fault injection。
- [ ] 旧 FORMAT拒绝不修改任何 PREPARE/Decision字节。
- [ ] CAC metrics、artifact schema 4和report无 PREPARE/Decision术语。
- [ ] deterministic crash campaign全部 PASS。
- [ ] Debug与Release完整 CTest PASS。
- [ ] ASAN、UBSAN、TSAN PASS。
- [ ] source inventory证明旧实现符号、文件、metrics和CMake入口不存在。
- [ ] 三轮独立 Release gate每轮均达到至少100,000 durable tx/s。
- [ ] 每轮 reopen验证所有 acknowledged commits。
- [ ] physical syncs/committed txn `<= 0.25`，高并发测量报告是否达到 `< 0.1`。
