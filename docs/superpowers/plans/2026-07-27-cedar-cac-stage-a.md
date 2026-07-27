# Cedar CAC Stage A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `subagent-driven-development` or `executing-plans` to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** 实现 `AtomicCommitRecord`、单请求同步耐久的分段 `AtomicCommitLog`，并将 FORMAT 升级为不兼容旧事务日志的 v2 clean break。

**Architecture:** 新增独立 record codec 和 segmented log，不在本阶段切换 `TransactionCoordinator`、实现 group commit 或删除旧 Decision/PREPARE 协议。`PendingEvent`移入 record/intents 层，旧 `decision_log.h`临时复用它。AtomicCommitLog 接收已分配的 `commit_seq`/HLC，后续阶段再把分配职责迁入 commit writer。

**Tech Stack:** C++17、GoogleTest、POSIX `open/write/fsync/fdatasync/ftruncate`、CRC32C、BLAKE3、CMake/CTest。

## Global Constraints

- `kCedarDatabaseFormatVersion`从 1 增至 2。
- FORMAT v2只持久化 atomic commit-log directory，不含 Decision/PREPARE位置。
- 打开 FORMAT v1返回 `NotSupported`，且不得写、截断、重命名或删除旧日志。
- 默认完整 CAC record上限定义为新的显式常量 16 MiB；估算与编码共享同一实现。
- 物理 frame必须包含固定 header、payload length、header checksum、payload checksum和重复 frame length suffix。
- 只截断最终不完整 frame；完整坏 checksum、完整 suffix不匹配和中间损坏均返回 corruption。
- 本阶段 append每 record一次 sync；group fsync属于后续阶段。
- 本阶段不修改 coordinator提交路径，不删除旧协议。

---

### Task 1: 提取 PendingEvent 并定义 AtomicCommitRecord

**Files:**

- Create: `include/cedar/transaction/atomic_commit_record.h`
- Create: `src/transaction/atomic_commit_record.cc`
- Modify: `include/cedar/transaction/decision_log.h`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`
- Create: `tests/test_atomic_commit_log.cc`

**Interfaces:**

```cpp
namespace cedar {

constexpr uint32_t kAtomicCommitRecordFormatVersion = 1;
constexpr uint64_t kDefaultMaximumAtomicCommitRecordBytes =
    16ULL * 1024ULL * 1024ULL;

enum class TransactionMode : uint8_t {
  kSnapshot = 0,
  kStrict = 1,
};

struct PendingEvent {
  LogicalKey logical_key;
  uint64_t valid_from = 0;
  uint32_t schema_epoch = 0;
  TemporalOperation operation = TemporalOperation::kPut;
  Value value;
  std::optional<BlobRef> blob_ref;

  static PendingEvent Put(LogicalKey, uint64_t, uint32_t, Value);
  static PendingEvent PutBlob(LogicalKey, uint64_t, uint32_t, BlobRef);
  static PendingEvent Delete(LogicalKey, uint64_t, uint32_t);
};

struct AtomicCommitShardBatch {
  uint32_t shard_id = 0;
  std::vector<PendingEvent> events;
};

struct AtomicCommitRecord {
  uint32_t record_format_version = kAtomicCommitRecordFormatVersion;
  uint64_t txn_id = 0;
  uint64_t snapshot_seq = 0;
  uint64_t commit_seq = 0;
  SystemHlc system_time_hlc;
  TransactionMode transaction_mode = TransactionMode::kSnapshot;
  std::vector<AtomicCommitShardBatch> shard_batches;
};

struct AtomicCommitOutcome {
  uint64_t txn_id = 0;
  uint64_t commit_seq = 0;
  SystemHlc system_time_hlc;
};

// Temporary source-compatibility alias removed with the legacy protocol.
using TransactionOutcome = AtomicCommitOutcome;

Status CanonicalizeAtomicCommitRecord(AtomicCommitRecord* record);
Status ValidateAtomicCommitRecord(
    const AtomicCommitRecord& record,
    uint64_t maximum_frame_bytes = kDefaultMaximumAtomicCommitRecordBytes);

}  // namespace cedar
```

- [ ] **Step 1: Add the failing model test and test target**

```cpp
TEST(AtomicCommitRecordTest, CanonicalizesShardBatchesAndEvents) {
  AtomicCommitRecord record;
  record.txn_id = 7;
  record.commit_seq = 1;
  record.system_time_hlc = SystemHlc{100, 0};
  record.shard_batches = {
      {2, {PendingEvent::Put(LogicalKey::VertexProperty(9, 3), 20, 1,
                             Value::String("b"))}},
      {0, {PendingEvent::Delete(LogicalKey::VertexProperty(2, 3), 10, 1),
           PendingEvent::Put(LogicalKey::VertexProperty(1, 3), 10, 1,
                             Value::String("a"))}},
  };

  ASSERT_TRUE(CanonicalizeAtomicCommitRecord(&record).ok());
  ASSERT_EQ(record.shard_batches.size(), 2U);
  EXPECT_EQ(record.shard_batches[0].shard_id, 0U);
  EXPECT_EQ(record.shard_batches[1].shard_id, 2U);
  EXPECT_EQ(record.shard_batches[0].events[0].logical_key,
            LogicalKey::VertexProperty(1, 3));
}
```

Add:

```cmake
add_executable(test_atomic_commit_log test_atomic_commit_log.cc)
target_link_libraries(test_atomic_commit_log ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_atomic_commit_log)
```

- [ ] **Step 2: Run RED**

Run:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --target test_atomic_commit_log -j4
```

Expected: compilation fails because `cedar/transaction/atomic_commit_record.h` does not exist.

- [ ] **Step 3: Add the minimal record model**

Implementation requirements:

```cpp
Status CanonicalizeAtomicCommitRecord(AtomicCommitRecord* record) {
  if (record == nullptr) {
    return Status::InvalidArgument("atomic commit record", "missing record");
  }
  std::sort(record->shard_batches.begin(), record->shard_batches.end(),
            [](const auto& a, const auto& b) {
              return a.shard_id < b.shard_id;
            });
  for (auto& batch : record->shard_batches) {
    std::sort(batch.events.begin(), batch.events.end(),
              [](const PendingEvent& a, const PendingEvent& b) {
                if (a.logical_key != b.logical_key) {
                  return a.logical_key < b.logical_key;
                }
                return a.valid_from < b.valid_from;
              });
  }
  return ValidateAtomicCommitRecord(*record);
}
```

`ValidateAtomicCommitRecord`至少拒绝：

- `txn_id == 0`或`commit_seq == 0`
- 空 participant集合或空 batch
- 非递增 shard ID
- 非 canonical event顺序
- 重复 `(logical_key, valid_from)`
- delete携带 BlobRef
- BlobRef `raw_length == 0`或`segment_id == 0`
- 未知 operation/mode/record version

从 `decision_log.h`删除原 `PendingEvent`和`TransactionOutcome`结构定义，改为 include新 header并使用临时 alias，保持旧代码编译。

- [ ] **Step 4: Run GREEN**

```bash
cmake --build build --target test_atomic_commit_log test_correctness_kernel -j4
./build/tests/test_atomic_commit_log \
  --gtest_filter=AtomicCommitRecordTest.CanonicalizesShardBatchesAndEvents
```

Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add include/cedar/transaction/atomic_commit_record.h \
        src/transaction/atomic_commit_record.cc \
        include/cedar/transaction/decision_log.h \
        CMakeLists.txt tests/CMakeLists.txt tests/test_atomic_commit_log.cc
git commit -m "feat: define canonical atomic commit records"
```

---

### Task 2: 实现共享尺寸估算和 logical record codec

**Files:**

- Modify: `include/cedar/transaction/atomic_commit_record.h`
- Modify: `src/transaction/atomic_commit_record.cc`
- Modify: `tests/test_atomic_commit_log.cc`

**Interfaces:**

```cpp
StatusOr<uint64_t> EstimateAtomicCommitPayloadBytes(
    const AtomicCommitRecord& record,
    uint64_t maximum_frame_bytes = kDefaultMaximumAtomicCommitRecordBytes);

StatusOr<uint64_t> EstimateAtomicCommitFrameBytes(
    const AtomicCommitRecord& record,
    uint64_t maximum_frame_bytes = kDefaultMaximumAtomicCommitRecordBytes);

StatusOr<std::string> EncodeAtomicCommitRecord(
    const AtomicCommitRecord& record,
    uint64_t maximum_frame_bytes = kDefaultMaximumAtomicCommitRecordBytes);

StatusOr<AtomicCommitRecord> DecodeAtomicCommitRecord(
    const std::string& payload,
    uint64_t maximum_frame_bytes = kDefaultMaximumAtomicCommitRecordBytes);
```

- [ ] **Step 1: Add failing codec tests**

Add tests for:

```cpp
TEST(AtomicCommitRecordTest, RoundTripsInlineBlobAndDeleteEvents);
TEST(AtomicCommitRecordTest, EstimateEqualsEncodedFrameSize);
TEST(AtomicCommitRecordTest, RejectsDuplicateEventIdentity);
TEST(AtomicCommitRecordTest, RejectsNonCanonicalShardOrder);
TEST(AtomicCommitRecordTest, RejectsOversizedCompleteRecord);
TEST(AtomicCommitRecordTest, RejectsTrailingPayloadBytes);
```

Core assertion:

```cpp
const auto encoded = EncodeAtomicCommitRecord(record);
ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
const auto decoded = DecodeAtomicCommitRecord(encoded.ValueOrDie());
ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
EXPECT_EQ(decoded.ValueOrDie().txn_id, record.txn_id);
EXPECT_EQ(decoded.ValueOrDie().commit_seq, record.commit_seq);
EXPECT_EQ(decoded.ValueOrDie().shard_batches.size(),
          record.shard_batches.size());
```

- [ ] **Step 2: Run RED**

```bash
cmake --build build --target test_atomic_commit_log -j4
./build/tests/test_atomic_commit_log \
  --gtest_filter='AtomicCommitRecordTest.*'
```

Expected: link/compile failure for missing codec APIs.

- [ ] **Step 3: Implement one checked encoder path**

Use little-endian fixed-width primitives. Encode:

```text
record_version
txn_id
snapshot_seq
commit_seq
hlc.physical_us
hlc.logical_counter
transaction_mode
participant_count
event_count
repeated shard batches and events
```

`EstimateAtomicCommitPayloadBytes()` must execute the same overflow-checked field accounting used by the encoder. `EstimateAtomicCommitFrameBytes()` adds the physical framing overhead exported by the log header:

```cpp
constexpr uint64_t kAtomicCommitFrameOverheadBytes = 40;
```

Do not estimate by encoding a second temporary record in production code.

- [ ] **Step 4: Run GREEN**

```bash
cmake --build build --target test_atomic_commit_log -j4
./build/tests/test_atomic_commit_log \
  --gtest_filter='AtomicCommitRecordTest.*'
```

Expected: all AtomicCommitRecord tests PASS.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/transaction/atomic_commit_record.h \
        src/transaction/atomic_commit_record.cc \
        tests/test_atomic_commit_log.cc
git commit -m "feat: encode atomic commit records canonically"
```

---

### Task 3: FORMAT v2 clean break与数据库身份

**Files:**

- Modify: `include/cedar/transaction/database_format.h`
- Modify: `src/transaction/database_format.cc`
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**

```cpp
constexpr uint32_t kCedarDatabaseFormatVersion = 2;

using DatabaseFormatIdentity = std::array<uint8_t, 32>;

struct DatabaseFormat {
  uint32_t format_version = kCedarDatabaseFormatVersion;
  uint32_t shard_count = 0;
  CedarHashAlgorithm hash_algorithm = CedarHashAlgorithm::kFnv1a64;
  uint64_t hash_seed = 0;
  std::string manifest_location;
  std::string atomic_commit_log_directory;
};

StatusOr<DatabaseFormatIdentity> ReadDatabaseFormatIdentity(
    const std::string& path);
```

- [ ] **Step 1: Add failing FORMAT tests**

Add/replace tests:

```cpp
TEST_F(DurableLogTest, CoordinatorCreatesAtomicCommitFormatIdentity);
TEST_F(DurableLogTest, DatabaseRejectsV1FormatWithoutMutatingLegacyLogs);
TEST_F(DurableLogTest, DatabaseFormatIdentityChangesWithDurableIdentity);
```

The v1 rejection test must snapshot bytes and sizes of:

```text
FORMAT
decision/DECISION
shards/0/wal/PREPARE.1
```

Then call `TransactionCoordinator::Open()` and assert:

```cpp
EXPECT_TRUE(status.IsNotSupportedError());
EXPECT_EQ(ReadBytes(path_ + "/decision/DECISION"), old_decision);
EXPECT_EQ(ReadBytes(path_ + "/shards/0/wal/PREPARE.1"), old_prepare);
```

- [ ] **Step 2: Run RED**

```bash
cmake --build build --target test_correctness_kernel -j4
./build/tests/test_correctness_kernel \
  --gtest_filter='DurableLogTest.*Format*'
```

Expected: old field assertions or new tests fail.

- [ ] **Step 3: Implement v2 encoding and explicit v1 rejection**

`MakeDatabaseFormat()` sets:

```cpp
format.manifest_location = storage_layout::kManifestRelativePath;
format.atomic_commit_log_directory = "commit";
```

Decoder flow:

```cpp
read and verify envelope/checksum;
read format_version;
if (format_version == 1) {
  return Status::NotSupported(
      "FORMAT", "Cedar database format version 1 is not supported");
}
if (format_version != kCedarDatabaseFormatVersion) {
  return Status::NotSupported(...);
}
decode only the v2 payload;
```

`ReadDatabaseFormatIdentity()` verifies FORMAT using `ReadDatabaseFormat()` and returns BLAKE3 of the exact durable FORMAT bytes.

Update hard-coded test benchmark/artifact JSON values from database format 1 to 2 where the fixture is intended to remain valid.

- [ ] **Step 4: Run GREEN**

```bash
cmake --build build --target test_correctness_kernel -j4
./build/tests/test_correctness_kernel \
  --gtest_filter='DurableLogTest.*Format*'
```

Expected: PASS; v1 test reports `NotSupported` and legacy files remain unchanged.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/transaction/database_format.h \
        src/transaction/database_format.cc \
        tests/test_correctness_kernel.cc
git commit -m "feat: make atomic commit format a clean break"
```

---

### Task 4: 实现 segment header、发现和首次创建

**Files:**

- Create: `include/cedar/transaction/atomic_commit_log.h`
- Create: `src/transaction/atomic_commit_log.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/test_atomic_commit_log.cc`

**Interfaces:**

```cpp
struct AtomicCommitLogOptions {
  uint64_t maximum_record_bytes =
      kDefaultMaximumAtomicCommitRecordBytes;
  uint64_t target_segment_bytes = 256ULL * 1024ULL * 1024ULL;
};

enum class AtomicCommitLogFaultPoint : uint8_t {
  kBeforeFrameWrite,
  kAfterPartialFrameWrite,
  kAfterFrameWrite,
  kAfterFrameSync,
  kAfterSegmentHeaderSync,
  kBeforeDirectorySync,
  kBeforeRecoveryDirectorySync,
};

enum class AtomicAppendCertainty : uint8_t {
  kDefinitelyAbsent,
  kIndeterminate,
  kDefinitelyDurable,
};

struct AtomicCommitAppendResult {
  Status status = Status::OK();
  bool requires_reopen = false;
  bool sync_attempted = false;
  uint64_t sync_latency_ns = 0;
  AtomicAppendCertainty certainty =
      AtomicAppendCertainty::kDefinitelyAbsent;
  std::vector<std::shared_ptr<const AtomicCommitRecord>> durable_records;
};

class AtomicCommitLog {
 public:
  AtomicCommitLog(std::string directory,
                  DatabaseFormatIdentity database_identity,
                  AtomicCommitLogOptions options = {});

  Status Open(
      const std::vector<AtomicCommitOutcome>& checkpoint_outcomes = {});
  AtomicCommitAppendResult Append(const AtomicCommitRecord& record);
  Status Close();

  uint64_t next_commit_seq() const;
  uint64_t retained_bytes() const;
  bool requires_reopen() const;
  std::vector<AtomicCommitRecord> records() const;
  std::optional<AtomicCommitOutcome> Resolve(uint64_t txn_id) const;

  void SetFaultInjectorForTesting(
      std::function<Status(AtomicCommitLogFaultPoint)> injector);
};
```

Segment filename:

```cpp
commit/COMMIT-%08u.log
```

Segment header fields:

```text
magic
encoding_version
header_bytes
cedar_database_format_version
database_identity[32]
segment_number
first_commit_seq
header_checksum
```

- [ ] **Step 1: Add failing segment tests**

```cpp
TEST_F(AtomicCommitLogTest, FirstAppendCreatesChecksummedSegment);
TEST_F(AtomicCommitLogTest, RotatesAtConfiguredSegmentBoundary);
TEST_F(AtomicCommitLogTest, RejectsSegmentNumberGap);
TEST_F(AtomicCommitLogTest, RejectsDatabaseIdentityMismatch);
TEST_F(AtomicCommitLogTest, RejectsBadSegmentHeaderChecksum);
```

Use a small test option:

```cpp
AtomicCommitLogOptions options;
options.maximum_record_bytes = 4096;
options.target_segment_bytes = 512;
```

- [ ] **Step 2: Run RED**

```bash
cmake --build build --target test_atomic_commit_log -j4
```

Expected: missing `atomic_commit_log.h` or unresolved APIs.

- [ ] **Step 3: Implement segment lifecycle**

Minimal rules:

- `Open()` does not create the directory for an empty log.
- First append durably creates directory and segment.
- Persist segment header before any frame.
- Sync segment header, then sync commit directory before acknowledging a record in the new segment.
- Discover only exact `COMMIT-[0-9]{8}.log` names.
- Reject invalid names, segment gaps, duplicate segment numbers, header identity mismatch and first-sequence mismatch.
- Rotate only before a frame; never split a frame between segments.

- [ ] **Step 4: Run GREEN**

```bash
cmake --build build --target test_atomic_commit_log -j4
./build/tests/test_atomic_commit_log \
  --gtest_filter='AtomicCommitLogTest.*Segment*:AtomicCommitLogTest.FirstAppend*'
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/transaction/atomic_commit_log.h \
        src/transaction/atomic_commit_log.cc \
        CMakeLists.txt tests/test_atomic_commit_log.cc
git commit -m "feat: add atomic commit log segments"
```

---

### Task 5: 实现单请求 durable append和连续性验证

**Files:**

- Modify: `src/transaction/atomic_commit_log.cc`
- Modify: `tests/test_atomic_commit_log.cc`

- [ ] **Step 1: Add failing append tests**

```cpp
TEST_F(AtomicCommitLogTest, AppendSyncsAndReturnsExactCallingRecord);
TEST_F(AtomicCommitLogTest, ReopenRestoresRecordsAndNextSequence);
TEST_F(AtomicCommitLogTest, RejectsSequenceGapDuplicateAndHlcRegression);
TEST_F(AtomicCommitLogTest, RejectsDuplicateTransactionId);
TEST_F(AtomicCommitLogTest, ResolvesRetainedTransactionOutcome);
TEST_F(AtomicCommitLogTest, AcceptsCheckpointOutcomeFollowedByNextSequence);
```

Exact ownership assertion:

```cpp
const AtomicCommitAppendResult result = log.Append(record);
ASSERT_TRUE(result.status.ok()) << result.status.ToString();
ASSERT_EQ(result.certainty, AtomicAppendCertainty::kDefinitelyDurable);
ASSERT_EQ(result.durable_records.size(), 1U);
EXPECT_EQ(result.durable_records[0]->txn_id, record.txn_id);
EXPECT_EQ(result.durable_records[0]->commit_seq, record.commit_seq);
EXPECT_TRUE(result.sync_attempted);
```

- [ ] **Step 2: Run RED**

```bash
cmake --build build --target test_atomic_commit_log -j4
./build/tests/test_atomic_commit_log \
  --gtest_filter='AtomicCommitLogTest.Append*:AtomicCommitLogTest.Reopen*:AtomicCommitLogTest.RejectsSequence*'
```

Expected: tests fail because frames are not appended/scanned.

- [ ] **Step 3: Implement physical frames and append**

Frame layout:

```text
u32 magic
u16 frame_version
u16 header_bytes
u64 payload_bytes
u64 total_frame_bytes
u32 header_checksum
u32 payload_checksum
payload
u64 repeated_total_frame_bytes
```

Append rules:

- Validate/encode before opening or writing.
- Check `record.commit_seq == next_commit_seq_`.
- Check HLC strictly exceeds checkpoint/recovered tail.
- Check txn ID is absent from checkpoint outcomes and retained map.
- Write one complete frame, then `fdatasync` or `fsync`.
- Publish `records_`, outcome map and `next_commit_seq_` only after sync succeeds.
- Return a copy of the caller’s exact record; never expose `records().back()` by reference.

- [ ] **Step 4: Run GREEN**

```bash
cmake --build build --target test_atomic_commit_log -j4
./build/tests/test_atomic_commit_log \
  --gtest_filter='AtomicCommitLogTest.Append*:AtomicCommitLogTest.Reopen*:AtomicCommitLogTest.RejectsSequence*:AtomicCommitLogTest.RejectsDuplicateTransactionId:AtomicCommitLogTest.Resolves*'
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/transaction/atomic_commit_log.cc tests/test_atomic_commit_log.cc
git commit -m "feat: append durable atomic commit records"
```

---

### Task 6: torn-tail恢复、完整 corruption拒绝和 ambiguity gate

**Files:**

- Modify: `src/transaction/atomic_commit_log.cc`
- Modify: `tests/test_atomic_commit_log.cc`

- [ ] **Step 1: Add recovery matrix tests**

```cpp
TEST_F(AtomicCommitLogTest, TruncatesPartialFinalFrameHeader);
TEST_F(AtomicCommitLogTest, TruncatesPartialFinalPayload);
TEST_F(AtomicCommitLogTest, TruncatesPartialFinalChecksumOrSuffix);
TEST_F(AtomicCommitLogTest, RejectsCompleteTailWithBadPayloadChecksum);
TEST_F(AtomicCommitLogTest, RejectsCompleteTailWithMismatchedLengthSuffix);
TEST_F(AtomicCommitLogTest, RejectsCorruptionBetweenValidFramesWithoutTruncating);
TEST_F(AtomicCommitLogTest, PartialWriteRequiresReopenAndBlocksLaterAppend);
TEST_F(AtomicCommitLogTest, SyncFailureIsAmbiguousAndBlocksLaterAppend);
TEST_F(AtomicCommitLogTest, PostSyncFailureResolvesCommittedAfterReopen);
TEST_F(AtomicCommitLogTest, RecoveryTruncationSyncFailureKeepsLogBlocked);
```

For every corruption test, capture the file bytes before `Open()` and verify they remain unchanged.

- [ ] **Step 2: Run RED**

```bash
cmake --build build --target test_atomic_commit_log -j4
./build/tests/test_atomic_commit_log \
  --gtest_filter='AtomicCommitLogTest.*Partial*:AtomicCommitLogTest.*Corrupt*:AtomicCommitLogTest.*Checksum*:AtomicCommitLogTest.*SyncFailure*'
```

Expected: recovery classification tests fail.

- [ ] **Step 3: Implement strict scanner**

Scanner behavior:

```cpp
if (remaining < kFrameHeaderBytes) {
  truncate_from = frame_offset;
} else if (header is structurally invalid) {
  return Corruption;
} else if (remaining < header.total_frame_bytes) {
  truncate_from = frame_offset;
} else if (stored_suffix != header.total_frame_bytes) {
  return Corruption;
} else if (header_checksum_bad || payload_checksum_bad) {
  return Corruption;
}
```

After torn-tail truncation:

```cpp
ftruncate(fd, last_complete_offset);
fsync(fd);
fsync(commit_directory_fd);
```

Ambiguity rules:

- failure before any frame byte is written: `certainty=kDefinitelyAbsent`; writer may remain usable only when file position is known unchanged;
- any partial/full write failure, sync failure, post-sync injected failure or uncertain close: `certainty=kIndeterminate`, `requires_reopen=true`;
- confirmed frame and required directory sync: `certainty=kDefinitelyDurable` with exactly one `durable_records` entry;
- once gated, every later `Append()` returns `RecoveryRequired`;
- `Open()` is the only operation that may clear the gate after a successful strict scan.

- [ ] **Step 4: Run GREEN**

```bash
cmake --build build --target test_atomic_commit_log -j4
./build/tests/test_atomic_commit_log
```

Expected: all record/log tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/transaction/atomic_commit_log.cc tests/test_atomic_commit_log.cc
git commit -m "feat: recover atomic commit log tails safely"
```

---

### Task 7: 更新 durable-writer inventory并完成 Stage A 回归

**Files:**

- Modify: `cmake/VerifyReleaseSourceContract.cmake`
- Modify: `tests/test_release_source_contract.cmake`
- Modify as required by compiler errors:
  - `include/cedar/storage/storage_shard.h`
  - `include/cedar/transaction/commit_timeline.h`
  - `src/transaction/decision_log.cc`
  - benchmark fixtures containing literal database format version

- [ ] **Step 1: Run source-contract RED**

```bash
cmake --build build -j4
ctest --test-dir build --output-on-failure \
  -R 'release_source_contract|atomic_commit|DatabaseFormat'
```

Expected: source inventory fails because `src/transaction/atomic_commit_log.cc` is a new durable writer not present in expected lists.

- [ ] **Step 2: Update exact source inventories**

Add:

```text
src/transaction/atomic_commit_record.cc
src/transaction/atomic_commit_log.cc
```

to the complete source lists.

Add only `src/transaction/atomic_commit_log.cc` to:

- mutation files
- durable database writers
- persistent delete files

Do not add `atomic_commit_record.cc` to mutation/durable-writer lists.

- [ ] **Step 3: Run focused GREEN**

```bash
cmake --build build -j4
./build/tests/test_atomic_commit_log
./build/tests/test_correctness_kernel \
  --gtest_filter='DurableLogTest.*Format*'
ctest --test-dir build --output-on-failure \
  -R 'release_source_contract'
```

Expected: PASS.

- [ ] **Step 4: Run complete Debug regression**

```bash
ctest --test-dir build --output-on-failure -j4
```

Expected: 100% tests passed.

- [ ] **Step 5: Verify no unintended coordinator migration**

```bash
rg -n 'AtomicCommitLog' \
  include/cedar/transaction/transaction_coordinator.h \
  src/transaction/transaction_coordinator.cc
```

Expected: no matches. Stage A provides the new primitive but does not yet wire the production commit path.

Verify FORMAT no longer exposes old locations:

```bash
rg -n 'decision_log_location|shard_wal_locations' \
  include/cedar/transaction/database_format.h \
  src/transaction/database_format.cc
```

Expected: matches only in explicit legacy-v1 rejection diagnostics, if any.

- [ ] **Step 6: Commit**

```bash
git add cmake/VerifyReleaseSourceContract.cmake \
        tests/test_release_source_contract.cmake \
        include src tests CMakeLists.txt
git commit -m "test: complete atomic commit stage A gates"
```
