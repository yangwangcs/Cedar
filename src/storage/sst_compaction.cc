// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/storage/sst_compaction.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <set>

#include "cedar/columnar/sst.h"
#include "cedar/storage/storage_layout.h"

namespace cedar {
namespace {

bool SamePartition(const BlockPartition& left, const BlockPartition& right) {
  return left.entity_type == right.entity_type && left.column_id == right.column_id &&
         left.schema_epoch == right.schema_epoch && left.physical_type == right.physical_type &&
         left.edge_type == right.edge_type && left.compression_id == right.compression_id &&
         left.key_kind == right.key_kind &&
         left.storage_shard_id == right.storage_shard_id &&
         left.logical_type_id == right.logical_type_id;
}

bool SameFileMetadata(const SstFileMeta& left, const SstFileMeta& right) {
  return left.file_number == right.file_number &&
      left.relative_path == right.relative_path &&
      SamePartition(left.partition, right.partition) &&
      left.file_size == right.file_size && left.blob_refs == right.blob_refs &&
      left.format == right.format && left.identity == right.identity &&
      left.statistics == right.statistics &&
      left.statistics_crc32c == right.statistics_crc32c;
}

StatusOr<uint32_t> ShardIdFor(const SstFileMeta& file) {
  const std::filesystem::path relative(file.relative_path);
  const std::filesystem::path shard = relative.parent_path().parent_path();
  if (relative.parent_path().filename() != "sst" || shard.parent_path().filename() != "shards") {
    return Status::InvalidArgument("sst compaction", "input path is not a current shard SST path");
  }
  try {
    const unsigned long value = std::stoul(shard.filename().string());
    if (value > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument("sst compaction", "shard id exceeds UInt32");
    }
    return static_cast<uint32_t>(value);
  } catch (const std::exception&) {
    return Status::InvalidArgument("sst compaction", "invalid shard id in SST path");
  }
}

}  // namespace

StatusOr<CompactionResult> CompactSstPartition(
    const std::string& db_path, const std::vector<SstFileMeta>& inputs,
    uint64_t output_file_number, VersionSet* version_set,
    std::shared_ptr<WorkCancellation> cancellation) {
  if (version_set == nullptr || inputs.size() < 2 || output_file_number == 0) {
    return Status::InvalidArgument("sst compaction", "inputs, output number, and version set are required");
  }
  if (cancellation != nullptr) {
    const Status checkpoint = cancellation->Checkpoint("sst compaction");
    if (!checkpoint.ok()) return checkpoint;
  }
  const auto shard_id = ShardIdFor(inputs.front());
  if (!shard_id.ok()) return shard_id.status();
  const BlockPartition partition = inputs.front().partition;
  const std::shared_ptr<const VersionSnapshot> snapshot = version_set->Snapshot();
  std::set<uint64_t> input_numbers;
  std::vector<std::string> input_paths;
  input_paths.reserve(inputs.size());
  for (const SstFileMeta& input : inputs) {
    const auto input_shard = ShardIdFor(input);
    if (!input_shard.ok()) return input_shard.status();
    if (input_shard.ValueOrDie() != shard_id.ValueOrDie() ||
        !SamePartition(input.partition, partition)) {
      return Status::InvalidArgument("sst compaction", "inputs do not form one partition closure");
    }
    if (!input_numbers.insert(input.file_number).second) {
      return Status::InvalidArgument("sst compaction", "duplicate input file number");
    }
    const auto live = std::find_if(
        snapshot->files.begin(), snapshot->files.end(), [&input](const SstFileMeta& file) {
          return file.file_number == input.file_number;
        });
    if (live == snapshot->files.end() || !SameFileMetadata(*live, input)) {
      return Status::InvalidArgument("sst compaction", "input is not exact live metadata");
    }
    input_paths.push_back(db_path + "/" + input.relative_path);
  }

  const std::string relative = "shards/" + std::to_string(shard_id.ValueOrDie()) +
      "/sst/" + std::to_string(output_file_number) +
          storage_layout::kSstExtension;
  for (const SstFileMeta& live : snapshot->files) {
    const auto live_shard = ShardIdFor(live);
    if (!live_shard.ok()) return Status::Corruption("sst compaction", "invalid live SST path");
    if (live.file_number == output_file_number || live.relative_path == relative) {
      return Status::InvalidArgument("sst compaction", "output identity is already live");
    }
    if (live_shard.ValueOrDie() == shard_id.ValueOrDie() &&
        SamePartition(live.partition, partition) &&
        input_numbers.count(live.file_number) == 0) {
      return Status::InvalidArgument("sst compaction", "incomplete live partition closure");
    }
  }
  const std::string output_path = db_path + "/" + relative;
  SstStreamingWriteStats streaming_stats;
  const auto written = MergeSstFilesStreaming(
      output_path, partition, input_paths, &streaming_stats, cancellation);
  if (!written.ok()) return written.status();
  std::error_code error;
  const uint64_t output_size = std::filesystem::file_size(output_path, error);
  if (error) return Status::IOError(output_path, error.message());

  std::vector<uint64_t> deletes;
  deletes.reserve(inputs.size());
  for (const SstFileMeta& input : inputs) deletes.push_back(input.file_number);
  const SstFileMeta output{output_file_number, relative, partition, output_size,
                           written.ValueOrDie().blob_refs,
                           written.ValueOrDie().format,
                           written.ValueOrDie().identity,
                           written.ValueOrDie().statistics,
                           written.ValueOrDie().statistics_crc32c};
  if (cancellation != nullptr) {
    const Status checkpoint = cancellation->Checkpoint("sst compaction");
    if (!checkpoint.ok()) {
      std::filesystem::remove(output_path, error);
      return checkpoint;
    }
  }
  VersionEdit edit{{output}, deletes};
  edit.expected_generation = snapshot->generation;
  const Status publish = version_set->ApplyEdit(edit);
  if (!publish.ok()) {
    if (!publish.IsIndeterminate()) std::filesystem::remove(output_path, error);
    return publish;
  }
  return CompactionResult{output_file_number + 1, output, inputs,
                            streaming_stats.peak_buffered_events,
                            streaming_stats.input_bytes_read,
                            streaming_stats.output_bytes_written,
                            streaming_stats.blob_payload_bytes_read,
                            streaming_stats.compression};
}

}  // namespace cedar
