// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/cedar_commit.h"

#include <queue>

#include "db/cedar_columnar_scan.h"
#include "db/column_family.h"
#include "db/dbformat.h"
#include "db/db_impl/db_impl.h"
#include "db/version_set.h"
#include "memory/arena.h"
#include "table/cedar_parquet/cedar_parquet_table_reader.h"
#include "table/internal_iterator.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

using cedar_parquet::CedarParquetColumnId;
using cedar_parquet::CedarParquetColumnarBatch;
using cedar_parquet::CedarParquetColumnVector;
using cedar_parquet::CedarParquetMaterializedFact;

Status MakeColumnVector(CedarParquetColumnId id,
                        CedarParquetColumnVector* column) {
  column->id = id;
  switch (id) {
    case CedarParquetColumnId::kPartId:
    case CedarParquetColumnId::kFactFamily:
    case CedarParquetColumnId::kPropertyId:
    case CedarParquetColumnId::kOperation:
    case CedarParquetColumnId::kSchemaEpoch:
    case CedarParquetColumnId::kPhysicalType:
    case CedarParquetColumnId::kSourcePartId:
    case CedarParquetColumnId::kTargetPartId:
      column->values = std::vector<uint32_t>{};
      return Status::OK();
    case CedarParquetColumnId::kEntityId:
    case CedarParquetColumnId::kValidFrom:
    case CedarParquetColumnId::kCedarCommitSeq:
    case CedarParquetColumnId::kRocksdbSequence:
    case CedarParquetColumnId::kTimestamp64Value:
    case CedarParquetColumnId::kSourceVertexId:
    case CedarParquetColumnId::kTargetVertexId:
    case CedarParquetColumnId::kEdgeType:
      column->values = std::vector<uint64_t>{};
      return Status::OK();
    case CedarParquetColumnId::kBoolValue:
      column->values = std::vector<uint8_t>{};
      return Status::OK();
    case CedarParquetColumnId::kInt32Value:
      column->values = std::vector<int32_t>{};
      return Status::OK();
    case CedarParquetColumnId::kInt64Value:
      column->values = std::vector<int64_t>{};
      return Status::OK();
    case CedarParquetColumnId::kFloat32Value:
      column->values = std::vector<float>{};
      return Status::OK();
    case CedarParquetColumnId::kFloat64Value:
      column->values = std::vector<double>{};
      return Status::OK();
    case CedarParquetColumnId::kBytesValue:
      column->values = std::vector<std::string>{};
      return Status::OK();
    default:
      return Status::InvalidArgument("Cedar columnar scan projection is not materialized");
  }
}

template <typename T>
void AppendRequiredValue(CedarParquetColumnVector* column, T value) {
  auto* values = std::get_if<std::vector<T>>(&column->values);
  assert(values != nullptr);
  values->push_back(value);
  column->present.push_back(1);
}

template <typename T>
void AppendOptionalValue(CedarParquetColumnVector* column,
                         const std::optional<T>& value) {
  auto* values = std::get_if<std::vector<T>>(&column->values);
  assert(values != nullptr);
  values->push_back(value.value_or(T{}));
  column->present.push_back(value.has_value() ? 1 : 0);
}

Status AppendMaterializedValue(CedarParquetColumnVector* column,
                               const CedarParquetMaterializedFact& fact) {
  switch (column->id) {
    case CedarParquetColumnId::kPartId:
      AppendRequiredValue(column, fact.part_id);
      return Status::OK();
    case CedarParquetColumnId::kFactFamily:
      AppendRequiredValue(column, fact.fact_family);
      return Status::OK();
    case CedarParquetColumnId::kPropertyId:
      AppendRequiredValue(column, fact.property_id);
      return Status::OK();
    case CedarParquetColumnId::kEntityId:
      AppendRequiredValue(column, fact.entity_id);
      return Status::OK();
    case CedarParquetColumnId::kValidFrom:
      AppendRequiredValue(column, fact.valid_from);
      return Status::OK();
    case CedarParquetColumnId::kCedarCommitSeq:
      AppendRequiredValue(column, fact.cedar_commit_seq);
      return Status::OK();
    case CedarParquetColumnId::kRocksdbSequence:
      AppendRequiredValue(column, fact.rocksdb_sequence);
      return Status::OK();
    case CedarParquetColumnId::kOperation:
      AppendRequiredValue(column, fact.operation);
      return Status::OK();
    case CedarParquetColumnId::kSchemaEpoch:
      AppendRequiredValue(column, fact.schema_epoch);
      return Status::OK();
    case CedarParquetColumnId::kPhysicalType:
      AppendRequiredValue(column, fact.physical_type);
      return Status::OK();
    case CedarParquetColumnId::kBoolValue:
      if (fact.bool_value.has_value()) {
        AppendOptionalValue<uint8_t>(column, static_cast<uint8_t>(*fact.bool_value));
      } else {
        AppendOptionalValue<uint8_t>(column, std::nullopt);
      }
      return Status::OK();
    case CedarParquetColumnId::kInt32Value:
      AppendOptionalValue(column, fact.int32_value);
      return Status::OK();
    case CedarParquetColumnId::kInt64Value:
      AppendOptionalValue(column, fact.int64_value);
      return Status::OK();
    case CedarParquetColumnId::kFloat32Value:
      AppendOptionalValue(column, fact.float32_value);
      return Status::OK();
    case CedarParquetColumnId::kFloat64Value:
      AppendOptionalValue(column, fact.float64_value);
      return Status::OK();
    case CedarParquetColumnId::kTimestamp64Value:
      AppendOptionalValue(column, fact.timestamp64_value);
      return Status::OK();
    case CedarParquetColumnId::kBytesValue:
      AppendOptionalValue(column, fact.bytes_value);
      return Status::OK();
    case CedarParquetColumnId::kSourcePartId:
      AppendOptionalValue(column, fact.source_part_id);
      return Status::OK();
    case CedarParquetColumnId::kSourceVertexId:
      AppendOptionalValue(column, fact.source_vertex_id);
      return Status::OK();
    case CedarParquetColumnId::kTargetPartId:
      AppendOptionalValue(column, fact.target_part_id);
      return Status::OK();
    case CedarParquetColumnId::kTargetVertexId:
      AppendOptionalValue(column, fact.target_vertex_id);
      return Status::OK();
    case CedarParquetColumnId::kEdgeType:
      AppendOptionalValue(column, fact.edge_type);
      return Status::OK();
    default:
      return Status::InvalidArgument("Cedar columnar scan projection is not materialized");
  }
}

Status ValidateScanSpec(const cedar_parquet::CedarParquetScanSpec& spec,
                        bool* request_encoded_values,
                        std::vector<CedarParquetColumnVector>* columns) {
  if (spec.batch_row_limit == 0) {
    return Status::InvalidArgument("Cedar columnar scan has zero batch row limit");
  }
  if ((spec.sort_key_lower.has_value() &&
       spec.sort_key_lower->size() != cedar_parquet::kCedarParquetV2SortKeyBytes) ||
      (spec.sort_key_upper.has_value() &&
       spec.sort_key_upper->size() != cedar_parquet::kCedarParquetV2SortKeyBytes)) {
    return Status::InvalidArgument("Cedar columnar scan bounds must be normalized Cedar keys");
  }
  if (spec.sort_key_lower.has_value() && spec.sort_key_upper.has_value() &&
      *spec.sort_key_lower > *spec.sort_key_upper) {
    return Status::InvalidArgument("Cedar columnar scan has invalid sort-key range");
  }
  *request_encoded_values = false;
  columns->clear();
  columns->reserve(spec.projection.size());
  for (CedarParquetColumnId id : spec.projection) {
    if (id == CedarParquetColumnId::kEncodedValue) {
      *request_encoded_values = true;
      continue;
    }
    for (const CedarParquetColumnVector& existing : *columns) {
      if (existing.id == id) {
        return Status::InvalidArgument("Cedar columnar scan has duplicate projection column");
      }
    }
    CedarParquetColumnVector column;
    Status status = MakeColumnVector(id, &column);
    if (!status.ok()) return status;
    columns->push_back(std::move(column));
  }
  return Status::OK();
}

std::string InternalSeekKeyForSortLower(const std::string& sort_key) {
  // Cedar sort keys hold the user key followed by the bitwise-inverted packed
  // RocksDB sequence/type in big-endian order. InternalIterator expects that
  // packed word in little-endian order. It need not be a valid value type for
  // a lower-bound seek: the internal comparator only needs its ordering.
  std::string internal_key = sort_key.substr(
      0, cedar_parquet::kCedarParquetV2UserKeyBytes);
  internal_key.reserve(cedar_parquet::kCedarParquetV2SortKeyBytes);
  for (size_t index = 0; index < 8; ++index) {
    internal_key.push_back(static_cast<char>(
        ~static_cast<unsigned char>(sort_key[39 - index])));
  }
  return internal_key;
}

template <typename T>
Status AppendProjectedColumnValue(const CedarParquetColumnVector& source,
                                  size_t row,
                                  CedarParquetColumnVector* destination) {
  const auto* source_values = std::get_if<std::vector<T>>(&source.values);
  auto* destination_values = std::get_if<std::vector<T>>(&destination->values);
  if (source_values == nullptr || destination_values == nullptr ||
      row >= source_values->size() || row >= source.present.size()) {
    return Status::Corruption("Cedar projected scan column vector is malformed");
  }
  destination_values->push_back((*source_values)[row]);
  destination->present.push_back(source.present[row]);
  return Status::OK();
}

Status AppendProjectedColumnRow(const CedarParquetColumnVector& source,
                                size_t row,
                                CedarParquetColumnVector* destination) {
  if (source.id != destination->id) {
    return Status::Corruption("Cedar projected scan column order changed");
  }
  switch (source.id) {
    case CedarParquetColumnId::kPartId:
    case CedarParquetColumnId::kFactFamily:
    case CedarParquetColumnId::kPropertyId:
    case CedarParquetColumnId::kOperation:
    case CedarParquetColumnId::kSchemaEpoch:
    case CedarParquetColumnId::kPhysicalType:
    case CedarParquetColumnId::kSourcePartId:
    case CedarParquetColumnId::kTargetPartId:
      return AppendProjectedColumnValue<uint32_t>(source, row, destination);
    case CedarParquetColumnId::kEntityId:
    case CedarParquetColumnId::kValidFrom:
    case CedarParquetColumnId::kCedarCommitSeq:
    case CedarParquetColumnId::kRocksdbSequence:
    case CedarParquetColumnId::kTimestamp64Value:
    case CedarParquetColumnId::kSourceVertexId:
    case CedarParquetColumnId::kTargetVertexId:
    case CedarParquetColumnId::kEdgeType:
      return AppendProjectedColumnValue<uint64_t>(source, row, destination);
    case CedarParquetColumnId::kBoolValue:
      return AppendProjectedColumnValue<uint8_t>(source, row, destination);
    case CedarParquetColumnId::kInt32Value:
      return AppendProjectedColumnValue<int32_t>(source, row, destination);
    case CedarParquetColumnId::kInt64Value:
      return AppendProjectedColumnValue<int64_t>(source, row, destination);
    case CedarParquetColumnId::kFloat32Value:
      return AppendProjectedColumnValue<float>(source, row, destination);
    case CedarParquetColumnId::kFloat64Value:
      return AppendProjectedColumnValue<double>(source, row, destination);
    case CedarParquetColumnId::kBytesValue:
      return AppendProjectedColumnValue<std::string>(source, row, destination);
    default:
      return Status::InvalidArgument("Cedar projected scan column is not materialized");
  }
}

CedarParquetColumnarBatch EmptyProjectedBatch(
    const std::vector<CedarParquetColumnVector>& columns) {
  CedarParquetColumnarBatch batch;
  batch.columns = columns;
  for (CedarParquetColumnVector& column : batch.columns) {
    std::visit([](auto& values) { values.clear(); }, column.values);
    column.present.clear();
  }
  return batch;
}

// On a pinned SuperVersion with exactly one immutable Cedar table and no
// MemTable rows, table-local physical projection preserves normal storage
// visibility without opening canonical value pages. Multi-table and MemTable
// versions use the cursor merge below.
Status ScanSingleCedarParquetTable(
    ColumnFamilyData* cfd, SuperVersion* super_version,
    const ReadOptions& read_options, const cedar_parquet::CedarParquetScanSpec& spec,
    const cedar_parquet::CedarParquetColumnarBatchVisitor& visitor,
    SequenceNumber storage_snapshot, bool* used) {
  *used = false;
  if (super_version->mem == nullptr || super_version->imm == nullptr ||
      !super_version->mem->IsEmpty() ||
      super_version->imm->GetTotalNumEntries() != 0) {
    return Status::OK();
  }

  const VersionStorageInfo* storage = super_version->current->storage_info();
  const FileMetaData* only_file = nullptr;
  int only_level = -1;
  for (int level = 0; level < storage->num_levels(); ++level) {
    const auto& files = storage->LevelFiles(level);
    if (files.empty()) continue;
    if (only_file != nullptr || files.size() != 1) return Status::OK();
    only_file = files.front();
    only_level = level;
  }
  if (only_file == nullptr) return Status::OK();

  TableCache::TypedHandle* table_handle = nullptr;
  TableReader* table_reader = nullptr;
  Status status = cfd->table_cache()->FindTable(
      read_options, cfd->table_cache()->file_options(), cfd->internal_comparator(),
      *only_file, &table_handle, super_version->mutable_cf_options, &table_reader,
      /*no_io=*/false, cfd->internal_stats()->GetFileReadHist(only_level),
      /*skip_filters=*/false, only_level,
      /*prefetch_index_and_filter_in_cache=*/true,
      /*max_file_size_for_l0_meta_pin=*/0, only_file->temperature);
  if (!status.ok()) return status;
  const auto* reader = dynamic_cast<const cedar_parquet::CedarParquetTableReader*>(
      table_reader);
  if (reader == nullptr) {
    if (table_handle != nullptr) cfd->table_cache()->get_cache().Release(table_handle);
    return Status::OK();
  }

  CedarParquetColumnarBatch visible;
  bool visible_initialized = false;
  std::string previous_user_key;
  bool selected_user_key = false;
  const auto flush = [&]() -> Status {
    if (visible.internal_keys.empty()) return Status::OK();
    Status flush_status = visitor(visible);
    if (!flush_status.ok()) return flush_status;
    visible = EmptyProjectedBatch(visible.columns);
    return Status::OK();
  };
  status = reader->ScanProjected(
      spec, [&](const CedarParquetColumnarBatch& source) -> Status {
        if (!visible_initialized) {
          visible = EmptyProjectedBatch(source.columns);
          visible_initialized = true;
        }
        if (source.columns.size() != visible.columns.size() ||
            (!source.encoded_values.empty() &&
             source.encoded_values.size() != source.internal_keys.size())) {
          return Status::Corruption("Cedar projected table batch is malformed");
        }
        for (size_t row = 0; row < source.internal_keys.size(); ++row) {
          ParsedInternalKey parsed;
          Status row_status = ParseInternalKey(source.internal_keys[row], &parsed, false);
          if (!row_status.ok()) return row_status;
          const std::string user_key = parsed.user_key.ToString();
          if (user_key != previous_user_key) {
            previous_user_key = user_key;
            selected_user_key = false;
          }
          if (selected_user_key || parsed.sequence > storage_snapshot) continue;
          selected_user_key = true;
          if (parsed.type == kTypeDeletion) continue;
          if (parsed.type != kTypeValue) {
            return Status::NotSupported(
                "Cedar columnar scan supports only value and point-deletion records");
          }
          visible.internal_keys.push_back(source.internal_keys[row]);
          if (!source.encoded_values.empty()) {
            visible.encoded_values.push_back(source.encoded_values[row]);
          }
          for (size_t column = 0; column < source.columns.size(); ++column) {
            row_status = AppendProjectedColumnRow(source.columns[column], row,
                                                  &visible.columns[column]);
            if (!row_status.ok()) return row_status;
          }
          if (visible.row_count() == spec.batch_row_limit) {
            row_status = flush();
            if (!row_status.ok()) return row_status;
          }
        }
        return Status::OK();
      });
  if (status.ok()) status = flush();
  if (table_handle != nullptr) cfd->table_cache()->get_cache().Release(table_handle);
  if (status.ok()) *used = true;
  return status;
}

Status ScanMultipleCedarParquetTables(
    DBImpl* impl, ColumnFamilyData* cfd, SuperVersion* super_version,
    const ReadOptions& read_options, const cedar_parquet::CedarParquetScanSpec& spec,
    const cedar_parquet::CedarParquetColumnarBatchVisitor& visitor,
    SequenceNumber storage_snapshot,
    const std::vector<CedarParquetColumnVector>& projection_columns,
    bool request_encoded_values, bool* used) {
  *used = false;

  struct Source {
    std::unique_ptr<cedar_parquet::CedarParquetProjectedCursor> cursor;
    InternalIterator* memtable_iterator = nullptr;
    TableCache::TypedHandle* handle = nullptr;
  };
  std::vector<Source> sources;
  const VersionStorageInfo* storage = super_version->current->storage_info();
  for (int level = 0; level < storage->num_levels(); ++level) {
    for (const FileMetaData* file : storage->LevelFiles(level)) {
      TableReader* table_reader = nullptr;
      TableCache::TypedHandle* handle = nullptr;
      Status status = cfd->table_cache()->FindTable(
          read_options, cfd->table_cache()->file_options(), cfd->internal_comparator(),
          *file, &handle, super_version->mutable_cf_options, &table_reader,
          false, cfd->internal_stats()->GetFileReadHist(level), false, level, true, 0,
          file->temperature);
      if (!status.ok()) {
        for (auto& source : sources) {
          if (source.handle != nullptr) cfd->table_cache()->get_cache().Release(source.handle);
        }
        return status;
      }
      auto* reader = dynamic_cast<cedar_parquet::CedarParquetTableReader*>(table_reader);
      if (reader == nullptr) {
        if (handle != nullptr) cfd->table_cache()->get_cache().Release(handle);
        for (auto& source : sources) {
          if (source.handle != nullptr) cfd->table_cache()->get_cache().Release(source.handle);
        }
        return Status::OK();
      }
      Source source;
      source.handle = handle;
      status = reader->NewProjectedCursor(spec, &source.cursor);
      if (!status.ok()) {
        if (handle != nullptr) cfd->table_cache()->get_cache().Release(handle);
        for (auto& existing : sources) {
          if (existing.handle != nullptr) cfd->table_cache()->get_cache().Release(existing.handle);
        }
        return status;
      }
      sources.push_back(std::move(source));
    }
  }
  if (sources.empty()) return Status::OK();

  // Retain a second SuperVersion reference for the memtable-only iterator.
  // The caller owns the original reference while projected table cursors are
  // active, so the same pinned version backs every source in this merge.
  Arena memtable_arena;
  ReadOptions memtable_read_options = read_options;
  memtable_read_options.read_tier = kMemtableTier;
  super_version->Ref();
  ScopedArenaPtr<InternalIterator> memtable_iterator(impl->NewInternalIterator(
      memtable_read_options, cfd, super_version, &memtable_arena,
      storage_snapshot, /*allow_unprepared_value=*/false));
  if (spec.sort_key_lower.has_value()) {
    memtable_iterator->Seek(InternalSeekKeyForSortLower(*spec.sort_key_lower));
  } else {
    memtable_iterator->SeekToFirst();
  }
  if (!memtable_iterator->status().ok()) {
    for (auto& source : sources) {
      if (source.handle != nullptr) cfd->table_cache()->get_cache().Release(source.handle);
    }
    return memtable_iterator->status();
  }
  if (memtable_iterator->Valid()) {
    Source source;
    source.memtable_iterator = memtable_iterator.get();
    sources.push_back(std::move(source));
  }

  CedarParquetColumnarBatch output = EmptyProjectedBatch(projection_columns);
  std::string previous_user_key;
  bool selected_user_key = false;
  const auto release = [&]() {
    for (auto& source : sources) {
      if (source.handle != nullptr) {
        cfd->table_cache()->get_cache().Release(source.handle);
        source.handle = nullptr;
      }
    }
  };
  const auto flush = [&]() -> Status {
    if (output.internal_keys.empty()) return Status::OK();
    Status result = visitor(output);
    if (result.ok()) output = EmptyProjectedBatch(output.columns);
    return result;
  };
  Status status = Status::OK();
  struct SourceCompare {
    const InternalKeyComparator* comparator;
    const std::vector<Source>* sources;
    bool operator()(size_t left, size_t right) const {
      const Source& left_source = (*sources)[left];
      const Source& right_source = (*sources)[right];
      const Slice left_key = left_source.cursor != nullptr
                                 ? left_source.cursor->internal_key()
                                 : left_source.memtable_iterator->key();
      const Slice right_key = right_source.cursor != nullptr
                                  ? right_source.cursor->internal_key()
                                  : right_source.memtable_iterator->key();
      return comparator->Compare(left_key, right_key) > 0;
    }
  };
  std::priority_queue<size_t, std::vector<size_t>, SourceCompare> ready(
      SourceCompare{&cfd->internal_comparator(), &sources});
  for (size_t index = 0; index < sources.size(); ++index) {
    const Source& source = sources[index];
    const bool valid = source.cursor != nullptr ? source.cursor->Valid()
                                                : source.memtable_iterator->Valid();
    const Status source_status = source.cursor != nullptr ? source.cursor->status()
                                                           : source.memtable_iterator->status();
    if (valid) ready.push(index);
    else if (!source_status.ok()) status = source_status;
  }
  while (true) {
    if (!status.ok() || ready.empty()) break;
    const size_t best = ready.top();
    ready.pop();
    auto& source = sources[best];
    const bool is_projected_table = source.cursor != nullptr;
    const Slice internal_key = is_projected_table
                                   ? source.cursor->internal_key()
                                   : source.memtable_iterator->key();
    ParsedInternalKey parsed;
    status = ParseInternalKey(internal_key, &parsed, false);
    if (!status.ok()) break;
    if (spec.sort_key_upper.has_value()) {
      std::string sort_key;
      status = cedar_parquet::EncodeCedarParquetSortKey(internal_key, &sort_key);
      if (!status.ok()) break;
      if (sort_key > *spec.sort_key_upper) break;
    }
    const std::string user_key = parsed.user_key.ToString();
    if (user_key != previous_user_key) {
      previous_user_key = user_key;
      selected_user_key = false;
    }
    if (!selected_user_key && parsed.sequence <= storage_snapshot) {
      selected_user_key = true;
      if (parsed.type == kTypeValue) {
        if (is_projected_table) {
          const auto& source_batch = source.cursor->batch();
          const size_t row = source.cursor->row_index();
          if (source_batch.columns.size() != output.columns.size() ||
              ((!source_batch.encoded_values.empty() || request_encoded_values) &&
               source_batch.internal_keys.size() != source_batch.encoded_values.size())) {
            status = Status::Corruption("Cedar projected table batch is malformed");
          } else {
            output.internal_keys.push_back(internal_key.ToString());
            if (request_encoded_values) {
              output.encoded_values.push_back(source_batch.encoded_values[row]);
            }
            for (size_t column = 0; column < source_batch.columns.size(); ++column) {
              status = AppendProjectedColumnRow(source_batch.columns[column], row,
                                                &output.columns[column]);
              if (!status.ok()) break;
            }
          }
        } else {
          cedar_parquet::CedarParquetMaterializedFact fact;
          status = cedar_parquet::DecodeCedarParquetMaterializedFact(
              internal_key, source.memtable_iterator->value(), &fact);
          if (status.ok()) {
            output.internal_keys.push_back(internal_key.ToString());
            if (request_encoded_values) {
              output.encoded_values.emplace_back(source.memtable_iterator->value().ToString());
            }
            for (CedarParquetColumnVector& column : output.columns) {
              status = AppendMaterializedValue(&column, fact);
              if (!status.ok()) break;
            }
          }
        }
        if (status.ok() && output.row_count() == spec.batch_row_limit) status = flush();
      } else if (parsed.type != kTypeDeletion) {
        status = Status::NotSupported("Cedar columnar scan supports only value and point-deletion records");
      }
    }
    if (status.ok()) {
      if (is_projected_table) source.cursor->Next();
      else source.memtable_iterator->Next();
      const Status source_status = is_projected_table ? source.cursor->status()
                                                      : source.memtable_iterator->status();
      const bool valid = is_projected_table ? source.cursor->Valid()
                                            : source.memtable_iterator->Valid();
      if (!source_status.ok()) status = source_status;
      else if (valid) ready.push(best);
    }
  }
  if (status.ok()) status = flush();
  release();
  if (status.ok()) *used = true;
  return status;
}

}  // namespace

Status WriteCedarEpoch(DB* db, const CedarEpochOptions& options,
                       WriteBatch* batch,
                       WalDurableCallback on_wal_durable,
                       void* callback_context,
                       CedarEpochMetrics* metrics) {
  if (db == nullptr) {
    return Status::InvalidArgument("DB is nullptr");
  }
  return static_cast_with_check<DBImpl>(db->GetRootDB())
      ->WriteCedarEpoch(options, batch, on_wal_durable, callback_context,
                        metrics);
}

Status MakeCedarParquetSortLowerBound(const Slice& user_key, std::string* sort_key) {
  if (sort_key == nullptr) {
    return Status::InvalidArgument("Cedar Parquet sort lower bound requires output");
  }
  if (user_key.size() != cedar_parquet::kCedarParquetV2UserKeyBytes) {
    return Status::InvalidArgument("Cedar Parquet sort lower bound has invalid user key");
  }
  std::string internal_key;
  AppendInternalKey(&internal_key,
                    ParsedInternalKey(user_key, kMaxSequenceNumber, kValueTypeForSeek));
  return cedar_parquet::EncodeCedarParquetSortKey(internal_key, sort_key);
}

Status ScanCedarParquetFacts(
    DB* db, ColumnFamilyHandle* column_family, const ReadOptions& read_options,
    const cedar_parquet::CedarParquetScanSpec& spec,
    const cedar_parquet::CedarParquetColumnarBatchVisitor& visitor) {
  if (db == nullptr || column_family == nullptr) {
    return Status::InvalidArgument("Cedar columnar scan requires database and column family");
  }
  if (!visitor) return Status::InvalidArgument("Cedar columnar scan requires visitor");

  bool request_encoded_values = false;
  std::vector<CedarParquetColumnVector> columns;
  Status status = ValidateScanSpec(spec, &request_encoded_values, &columns);
  if (!status.ok()) return status;

  DBImpl* impl = static_cast_with_check<DBImpl>(db->GetRootDB());
  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();
  if (cfd == nullptr) return Status::InvalidArgument("Cedar column family is unavailable");
  SuperVersion* super_version = cfd->GetReferencedSuperVersion(impl);
  if (super_version == nullptr) return Status::ShutdownInProgress();

  // NewInternalIterator takes ownership of this reference and releases it when
  // the iterator is destroyed. Its merge tree covers exactly this SuperVersion's
  // mutable MemTable, immutable MemTables, L0 overlaps, and all lower levels.
  const SequenceNumber storage_snapshot =
      read_options.snapshot == nullptr ? impl->GetLastPublishedSequence()
                                       : read_options.snapshot->GetSequenceNumber();
  bool used_single_table_projection = false;
  status = ScanMultipleCedarParquetTables(
      impl, cfd, super_version, read_options, spec, visitor, storage_snapshot,
      columns, request_encoded_values, &used_single_table_projection);
  if (!status.ok() || used_single_table_projection) {
    impl->CleanupSuperVersion(super_version);
    return status;
  }
  status = ScanSingleCedarParquetTable(
      cfd, super_version, read_options, spec, visitor, storage_snapshot,
      &used_single_table_projection);
  if (!status.ok() || used_single_table_projection) {
    impl->CleanupSuperVersion(super_version);
    return status;
  }
  Arena arena;
  ScopedArenaPtr<InternalIterator> iterator(impl->NewInternalIterator(
      read_options, cfd, super_version, &arena, storage_snapshot,
      /*allow_unprepared_value=*/false));

  cedar_parquet::CedarParquetColumnarBatch batch;
  batch.columns = columns;
  const auto flush = [&]() -> Status {
    if (batch.internal_keys.empty()) return Status::OK();
    Status flush_status = visitor(batch);
    if (!flush_status.ok()) return flush_status;
    batch = cedar_parquet::CedarParquetColumnarBatch();
    batch.columns = columns;
    return Status::OK();
  };
  const auto append = [&](const Slice& internal_key, const Slice& encoded_value) -> Status {
    cedar_parquet::CedarParquetMaterializedFact fact;
    Status append_status = cedar_parquet::DecodeCedarParquetMaterializedFact(
        internal_key, encoded_value, &fact);
    if (!append_status.ok()) return append_status;
    batch.internal_keys.emplace_back(internal_key.data(), internal_key.size());
    if (request_encoded_values) {
      batch.encoded_values.emplace_back(encoded_value.data(), encoded_value.size());
    }
    for (CedarParquetColumnVector& column : batch.columns) {
      append_status = AppendMaterializedValue(&column, fact);
      if (!append_status.ok()) return append_status;
    }
    return batch.row_count() == spec.batch_row_limit ? flush() : Status::OK();
  };

  std::string previous_user_key;
  bool selected_user_key = false;
  if (spec.sort_key_lower.has_value()) {
    const std::string seek_key =
        InternalSeekKeyForSortLower(*spec.sort_key_lower);
    iterator->Seek(seek_key);
  } else {
    iterator->SeekToFirst();
  }
  while (iterator->Valid()) {
    ParsedInternalKey parsed;
    status = ParseInternalKey(iterator->key(), &parsed, false);
    if (!status.ok()) return status;
    const std::string user_key = parsed.user_key.ToString();
    if (user_key != previous_user_key) {
      previous_user_key = user_key;
      selected_user_key = false;
    }
    if (!selected_user_key && parsed.sequence <= storage_snapshot) {
      selected_user_key = true;
      std::string sort_key;
      status = cedar_parquet::EncodeCedarParquetSortKey(iterator->key(), &sort_key);
      if (!status.ok()) return status;
      if (spec.sort_key_lower.has_value() && sort_key < *spec.sort_key_lower) {
        iterator->Next();
        continue;
      }
      if (spec.sort_key_upper.has_value() && sort_key > *spec.sort_key_upper) {
        break;
      }
      if (parsed.type == kTypeValue) {
        status = append(iterator->key(), iterator->value());
        if (!status.ok()) return status;
      } else if (parsed.type != kTypeDeletion) {
        return Status::NotSupported(
            "Cedar columnar scan supports only value and point-deletion records");
      }
    }
    iterator->Next();
  }
  if (!iterator->status().ok()) return iterator->status();
  return flush();
}

}  // namespace ROCKSDB_NAMESPACE
