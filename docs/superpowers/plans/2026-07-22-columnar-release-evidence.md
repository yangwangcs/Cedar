# Cedar Columnar and Blob Release Evidence

Date: 2026-07-22

Scope: sections 30 and 32 of
`docs/superpowers/specs/2026-07-17-cedar-columnar-design.md`.

Status: historical stage-one artifact plus current correctness evidence. The
fixed-seed artifacts below predate the current `CBB1` Blob block layout, so a
regenerated release artifact remains required by the persistent six-design
Goal.

## Format And Codec Evidence

- `PageFormatTest.GoldenBytesAreStableForHeaderDirectoryAndCodecs` fixes the
  PageHeader, PageDirectory, and every codec/compression byte sequence.
- `SstTest.GoldenBytesAreStableForFileBlockAndFooterHeaders` fixes the current SST
  FileHeader, granule BlockHeader, and Footer byte sequences.
- `DurableLogTest.BlobBlockAndIndexHaveStableGoldenBytes` fixes the current
  `CBB1` block, record directory, BlobRecord and INDEX delta bytes.
- `PageFormatTest.CoversEveryPhysicalCodecAndCompressionCombination` and
  `PageFormatTest.DeterministicRandomPhysicalCodecRoundTrips` cover Bool,
  Int32, Int64, Float32, Float64, Timestamp64, String, and Binary with Plain,
  RLE, Delta, Dictionary, None, and LZ4 where the shape is legal.
- `PageFormatTest.RejectsCorruptPayloadSizesAndDirectoryOffsets`,
  `PageFormatTest.DictionaryEncodingRoundTripsAndRejectsBadIndex`, and the
  malformed RLE/Delta tests cover truncation, bit flips, CRC, decoded-size
  bombs, offsets, dictionary indices, unsupported flags, and invalid shapes.
- `PageFormatTest.PageDirectoryUses64BitBlockRelativeLocations` proves the
  required little-endian 64-bit page locations.

## SST And Identity Evidence

- Continuations and oversized chains are covered by
  `SstTest.SplitsLargeEventSetsAcrossGranuleBlocks`,
  `GranuleBlockTest.FragmentsLargeDenseValuePagesWithoutLosingTemporalRows`,
  and `DurableLogTest.CompactionPreservesVersionChainAcrossContinuationBlocks`.
- Complete edge identity and directional projection are covered by
  `LogicalKeyTest.*`, `DurableLogTest.EdgeExistenceKindSurvivesFlushAndReopen`,
  and `PropertyGatherTest.PreservesCompleteEdgeIdentityWhenGatheringProperties`.
- `DurableLogTest.RandomMultiSstSchemaEpochHistoryMatchesOracleAcrossCompactionAndReopen`
  covers fixed-seed out-of-order PUT/DELETE/resurrection histories across more
  than ten SSTs and twelve schema epochs for point, range, and change reads.
- `DurableLogTest.RandomMultiSstEdgePathProvenanceMatchesOracleAcrossCompactionAndReopen`
  uses seed `0xED6E20260722`, more than ten edge SSTs, six edge schema epochs,
  EdgeOut/EdgeIn point reads, two-hop interval intersections, and per-edge
  commit provenance before/after every flush, compaction, and reopen.
- The edge oracle exposed and now guards the fixed defect where EdgeIn reads
  could ignore newer EdgeOut SST history when a MemTable candidate existed.
  `MergeTemporalReadEvent` now merges both physical edge directions as one
  logical fact timeline.
- Selective reads are covered by
  `DurableLogTest.PointReadFetchesOnlyTheSelectedValuePageFragment`,
  `DurableLogTest.SstOrdinalReadDecodesOnlyPostingBlocks`, and lazy pinned root
  scan tests.

## Blob And Compaction Evidence

- Concurrent content-addressed publication is covered by
  `DurableLogTest.ConcurrentEqualBlobPutsPublishOnePhysicalRecord`; distinct
  content and threshold placement are covered by the durable Blob round-trip
  and `MediumAndLargeValuesUseSeparateSstAndBlobPlacementTiers` tests.
- `BlobStoreWritesIndexedBlobBlockAndReopensIt`,
  `CommitPacksMultipleSmallBlobsIntoOneBlobBlock`,
  `BlobBatchStartsANewBlockAtTheOneMiBTarget`, and
  `BlobLargerThanBlockTargetUsesOversizedBlock` cover block framing, transaction
  packing, the 1 MiB boundary and dedicated oversized blocks.
- `BlobBatchWriteEstimateMatchesBlockAndIndexBytes` proves the protected write
  estimate equals the actual segment, INDEX and ACTIVE byte counts;
  `BlobGcRelocationUsesTheSamePackedBlockWriterAsPut` proves relocation uses the
  same physical writer.
- Record, index, and transaction boundaries are covered by
  `BlobRecordFaultBoundariesLeaveOnlyReclaimableUnindexedOrphans`,
  `PartialBlobIndexWriteRequiresReopenAndTruncatesTornTail`, and
  `CommitFaultMatrixReopensAndPreservesOnlyDurableOutcomes`.
- CRC and BLAKE3 are independently exercised by
  `BlobPayloadDetectsCrcAndBlake3Corruption`.
- Stale hints, concurrent writer/GC behavior, relocation, and long snapshot
  pins are covered by
  `BlobGcConcurrentWriterRespectsLongSnapshotPinAndStaleReferences` and the
  Blob GC relocation/resource tests.
- Zero-payload-read reference compaction is asserted by
  `DatabaseExportsRealColumnarBlobCompactionAndGcMetrics` with
  `compaction_blob_payload_read == 0`.
- Streaming compaction bounds are covered by
  `CompactionStreamsSortedSstBlocksWithBoundedEventBuffering`; reader bounds
  and lazy metadata are covered by `SstCursorVisitsOneGranuleBlockAtATime` and
  `SstMetadataOpenValidatesOwnershipWithoutDecodingDataBlocks`.

## Fault And Legacy Evidence

- SST and index publication boundaries are covered by
  `PublicationFaultMatrixCleansUnmanifestedSstAndSidecarOutputs`.
- Manifest-generation ambiguity and retirement are covered by
  `CompactionRetainsPossiblyPublishedOutputAfterManifestRename`,
  `VersionSetRejectsStaleManifestGenerationCas`, Blob retirement tests, and
  directory-sync accounting tests.
- Startup orphan cleanup and referenced-file corruption are covered by the
  reopen cleanup and manifest ownership/size/partition/BlobRef mismatch tests.
- Static scan over `CMakeLists.txt`, `include/cedar`, `src`, and `tests` has no
  production references to Frond, ZoneColumnar, SimpleSSTBlobManager,
  AutoBlobStorage, old BlobGCManager/BlobFileManager, or a dual-format switch.

## Resource And Artifact Evidence

- `DatabaseExportsRealColumnarBlobCompactionAndGcMetrics` proves real page,
  Blob, compaction, GC, lookup latency, page compression, metadata/page cache,
  and Blob-value cache samples. SST header/footer metadata is cached through
  the shared CacheManager and cache hits do not count as physical reads.
- Final fixed-seed artifacts are under
  `results/columnar-closure-20260722-r4`:
  - valid-time-range: `2fb265afa5bf813f2708cb7ca3f3e93c7ed59f9127011522dc7268c1d459f69c`
  - durable-ingestion: `819fefe532d278066de00a44ae4640615d4caf31c2f51fb1781a918cf1796a04`
  - blob-projection: `00b860c9ad4656725e19e0853b0832e09dfd3fd0db5036c3ece2af2d18115d7c`
- All three artifacts use seed `20260722`, pass load/result/reopen and protocol
  verification, preserve durable component-byte sums, and contain the page
  compression and cache metric families. Blob-projection and valid-time-range
  each record two metadata-cache hits and two misses.

## Historical Full Verification

- Normal CTest: 618/618, 12.98 seconds.
- ASAN CTest: 618/618, 30.00 seconds.
- UBSAN CTest: 618/618, 14.07 seconds.
- TSAN CTest: 618/618, 111.99 seconds.
- `git diff --check`: clean.
- Explicit trailing-whitespace scan of the three edited source/test files:
  no matches.

The first attempted ASAN/UBSAN run was intentionally discarded because the
two matrices were launched concurrently and one pre-existing test uses the
same hard-coded `/tmp/cedar_physical_multi_join_intermediate_spill` path in
every build. The authoritative sanitizer evidence above comes from isolated,
serial full runs.

The current `CBB1` functionality checkpoint passes 755/755 in the normal
correctness kernel. The complete Blob-named focused set passes 49/49 under
ASAN, UBSAN and TSAN. These focused results verify the new implementation but
do not replace the final full release sanitizer matrix or regenerate the
fixed-seed artifacts.

## Later-Stage Items

These remain active Goal work but belong to Observability/Benchmark rather
than Columnar format correctness: `metrics.jsonl.zst` output (current artifact
is `metrics.json`), failed-operation partial physical-write attribution,
separate index-sidecar durable bytes, exact Blob GC/rotation non-payload byte
breakdown, and instrumentation-overhead release gates.
