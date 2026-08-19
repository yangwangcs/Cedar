// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "table/cedar_parquet/parquet_metadata.h"

#include <array>
#include <limits>

#include "table/cedar_parquet/compact_protocol.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {
namespace {

constexpr int32_t kPlain = 0;
constexpr int32_t kRle = 3;
bool IsSupportedCompressionCodec(CedarParquetCompressionCodec codec) {
  return codec == CedarParquetCompressionCodec::kUncompressed ||
         codec == CedarParquetCompressionCodec::kLz4Raw ||
         codec == CedarParquetCompressionCodec::kZstd;
}
constexpr char kParquetMagic[] = "PAR1";

struct ColumnSpec {
  const char* name;
  int32_t physical_type;
  int32_t repetition_type;
  int32_t type_length;
};

constexpr std::array<ColumnSpec, 25> kFactsColumns = {{
    {"sort_key", kParquetFixedLenByteArray, kParquetRequired, 40},
    {"internal_key", kParquetByteArray, kParquetRequired, 0},
    {"encoded_value", kParquetByteArray, kParquetRequired, 0},
    {"part_id", kParquetInt32, kParquetOptional, 0},
    {"fact_family", kParquetInt32, kParquetOptional, 0},
    {"property_id", kParquetInt32, kParquetOptional, 0},
    {"entity_id", kParquetInt64, kParquetOptional, 0},
    {"valid_from", kParquetInt64, kParquetOptional, 0},
    {"cedar_commit_seq", kParquetInt64, kParquetOptional, 0},
    {"rocksdb_sequence", kParquetInt64, kParquetOptional, 0},
    {"operation", kParquetInt32, kParquetOptional, 0},
    {"schema_epoch", kParquetInt32, kParquetOptional, 0},
    {"physical_type", kParquetInt32, kParquetOptional, 0},
    {"bool_value", kParquetBoolean, kParquetOptional, 0},
    {"int32_value", kParquetInt32, kParquetOptional, 0},
    {"int64_value", kParquetInt64, kParquetOptional, 0},
    {"float32_value", kParquetFloat, kParquetOptional, 0},
    {"float64_value", kParquetDouble, kParquetOptional, 0},
    {"timestamp64_value", kParquetInt64, kParquetOptional, 0},
    {"bytes_value", kParquetByteArray, kParquetOptional, 0},
    {"source_part_id", kParquetInt32, kParquetOptional, 0},
    {"source_vertex_id", kParquetInt64, kParquetOptional, 0},
    {"target_part_id", kParquetInt32, kParquetOptional, 0},
    {"target_vertex_id", kParquetInt64, kParquetOptional, 0},
    {"edge_type", kParquetInt64, kParquetOptional, 0},
}};

void AppendFixed32(std::string* destination, uint32_t value) {
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    destination->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

uint32_t DecodeFixed32(const char* source) {
  uint32_t value = 0;
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    value |= static_cast<uint32_t>(static_cast<unsigned char>(source[shift / 8])) << shift;
  }
  return value;
}

void WriteSchemaElement(CompactWriter* writer, const CedarParquetSchemaElement& element) {
  writer->WriteStructBegin();
  if (element.physical_type >= 0) writer->WriteI32Field(1, element.physical_type);
  if (element.type_length != 0) writer->WriteI32Field(2, element.type_length);
  if (element.repetition_type >= 0) writer->WriteI32Field(3, element.repetition_type);
  writer->WriteBinaryField(4, element.name);
  if (element.num_children != 0) writer->WriteI32Field(5, element.num_children);
  writer->WriteFieldStop();
  writer->WriteStructEnd();
}

Status ReadSchemaElement(CompactReader* reader, CedarParquetSchemaElement* element) {
  reader->ReadStructBegin();
  bool has_name = false;
  while (true) {
    int16_t field_id = 0;
    uint8_t type = 0;
    Status status = reader->ReadFieldBegin(&field_id, &type);
    if (!status.ok()) return status;
    if (type == kCompactStop) break;
    switch (field_id) {
      case 1:
        if (type != kCompactI32) return Status::Corruption("invalid schema type");
        status = reader->ReadI32(&element->physical_type);
        break;
      case 2:
        if (type != kCompactI32) return Status::Corruption("invalid schema type length");
        status = reader->ReadI32(&element->type_length);
        break;
      case 3:
        if (type != kCompactI32) return Status::Corruption("invalid schema repetition");
        status = reader->ReadI32(&element->repetition_type);
        break;
      case 4:
        if (type != kCompactBinary) return Status::Corruption("invalid schema name");
        status = reader->ReadBinary(&element->name);
        has_name = status.ok();
        break;
      case 5:
        if (type != kCompactI32) return Status::Corruption("invalid schema children");
        status = reader->ReadI32(&element->num_children);
        break;
      default:
        return Status::NotSupported("unsupported Parquet schema field");
    }
    if (!status.ok()) return status;
  }
  reader->ReadStructEnd();
  return has_name ? Status::OK() : Status::Corruption("missing schema name");
}

void WriteKeyValueMetadata(CompactWriter* writer,
                           const CedarParquetKeyValueMetadata& metadata) {
  writer->WriteStructBegin();
  writer->WriteBinaryField(1, metadata.key);
  if (metadata.value.has_value()) writer->WriteBinaryField(2, *metadata.value);
  writer->WriteFieldStop();
  writer->WriteStructEnd();
}

Status ReadKeyValueMetadata(CompactReader* reader,
                            CedarParquetKeyValueMetadata* metadata) {
  reader->ReadStructBegin();
  bool has_key = false;
  while (true) {
    int16_t field_id = 0;
    uint8_t type = 0;
    Status status = reader->ReadFieldBegin(&field_id, &type);
    if (!status.ok()) return status;
    if (type == kCompactStop) break;
    if (type != kCompactBinary || (field_id != 1 && field_id != 2)) {
      return Status::Corruption("invalid Parquet key-value metadata");
    }
    std::string value;
    status = reader->ReadBinary(&value);
    if (!status.ok()) return status;
    if (field_id == 1) {
      metadata->key = std::move(value);
      has_key = true;
    } else {
      metadata->value = std::move(value);
    }
  }
  reader->ReadStructEnd();
  if (!has_key || metadata->key.empty() || metadata->key.size() > 128 ||
      (metadata->value.has_value() && metadata->value->size() > 512)) {
    return Status::Corruption("invalid Parquet key-value metadata");
  }
  return Status::OK();
}

Status ValidateRequiredFactsSchema(const CedarParquetFooter& footer) {
  if (footer.version != 2 || footer.num_rows < 0 ||
      footer.schema.size() != kFactsColumns.size() + 1) {
    return Status::Corruption("unsupported Cedar Parquet footer");
  }
  if (footer.schema[0].name != "schema" || footer.schema[0].physical_type != -1 ||
      footer.schema[0].num_children != static_cast<int32_t>(kFactsColumns.size())) {
    return Status::Corruption("invalid Cedar Parquet root schema");
  }
  for (size_t index = 0; index < kFactsColumns.size(); ++index) {
    const CedarParquetSchemaElement& column = footer.schema[index + 1];
    const ColumnSpec& spec = kFactsColumns[index];
    if (column.name != spec.name || column.physical_type != spec.physical_type ||
        column.type_length != spec.type_length ||
        column.repetition_type != spec.repetition_type || column.num_children != 0) {
      return Status::Corruption("invalid Cedar Parquet facts schema");
    }
  }
  int64_t row_count = 0;
  for (const auto& row_group : footer.row_groups) {
    if (row_group.num_rows <= 0 || row_group.total_byte_size < 0 ||
        row_group.file_offset < 4 || row_group.columns.size() != kFactsColumns.size()) {
      return Status::Corruption("invalid Cedar Parquet row group");
    }
    if (row_group.num_rows > std::numeric_limits<int64_t>::max() - row_count) {
      return Status::Corruption("Cedar Parquet row count overflow");
    }
    row_count += row_group.num_rows;
    int64_t row_group_uncompressed_size = 0;
    for (size_t index = 0; index < row_group.columns.size(); ++index) {
      const auto& column = row_group.columns[index];
      const ColumnSpec& spec = kFactsColumns[index];
      if (column.path != spec.name || column.physical_type != spec.physical_type ||
          column.type_length != spec.type_length || column.data_page_offset < 4 ||
          column.total_compressed_size < 0 ||
          column.total_uncompressed_size < 0 ||
          !IsSupportedCompressionCodec(column.compression_codec) ||
          (column.compression_codec == CedarParquetCompressionCodec::kUncompressed &&
           column.total_uncompressed_size != column.total_compressed_size) ||
          column.num_values != row_group.num_rows ||
          column.offset_index_offset < 4 || column.offset_index_length <= 0) {
        return Status::Corruption("invalid Cedar Parquet column chunk");
      }
      if ((column.bloom_filter_offset >= 0) != (column.bloom_filter_length > 0) ||
          (column.bloom_filter_offset >= 0 && index != 0)) {
        return Status::Corruption("invalid Cedar Parquet Bloom metadata");
      }
      if (column.total_uncompressed_size >
          std::numeric_limits<int64_t>::max() - row_group_uncompressed_size) {
        return Status::Corruption("Cedar Parquet row-group size overflow");
      }
      row_group_uncompressed_size += column.total_uncompressed_size;
    }
    if (row_group.total_byte_size != row_group_uncompressed_size) {
      return Status::Corruption("Cedar Parquet row-group uncompressed size mismatch");
    }
  }
  if (row_count != footer.num_rows) {
    return Status::Corruption("Cedar Parquet footer row count mismatch");
  }
  return Status::OK();
}

void WriteColumnMetaData(CompactWriter* writer,
                         const CedarParquetFooter::ColumnChunk& column) {
  writer->WriteStructBegin();
  writer->WriteI32Field(1, column.physical_type);
  writer->WriteListFieldBegin(2, 2, kCompactI32);
  writer->WriteI32(kPlain);
  writer->WriteI32(kRle);
  writer->WriteListEnd();
  writer->WriteListFieldBegin(3, 1, kCompactBinary);
  writer->WriteBinary(column.path);
  writer->WriteListEnd();
  writer->WriteI32Field(4, static_cast<int32_t>(column.compression_codec));
  writer->WriteI64Field(5, column.num_values);
  writer->WriteI64Field(6, column.total_uncompressed_size);
  writer->WriteI64Field(7, column.total_compressed_size);
  writer->WriteI64Field(9, column.data_page_offset);
  if (column.bloom_filter_offset >= 0) {
    writer->WriteI64Field(14, column.bloom_filter_offset);
    writer->WriteI32Field(15, column.bloom_filter_length);
  }
  if (!column.min_value.empty() || !column.max_value.empty()) {
    writer->WriteStructFieldBegin(12);
    writer->WriteStructBegin();
    writer->WriteBinaryField(5, column.max_value);
    writer->WriteBinaryField(6, column.min_value);
    writer->WriteFieldStop();
    writer->WriteStructEnd();
  }
  writer->WriteFieldStop();
  writer->WriteStructEnd();
}

Status ReadColumnMetaData(CompactReader* reader,
                          CedarParquetFooter::ColumnChunk* column) {
  bool has_type = false;
  bool has_encodings = false;
  bool has_path = false;
  bool has_codec = false;
  bool has_num_values = false;
  bool has_uncompressed_size = false;
  bool has_compressed_size = false;
  bool has_data_page_offset = false;
  bool has_statistics = false;
  reader->ReadStructBegin();
  while (true) {
    int16_t field_id = 0;
    uint8_t type = 0;
    Status status = reader->ReadFieldBegin(&field_id, &type);
    if (!status.ok()) return status;
    if (type == kCompactStop) break;
    if (field_id == 2 || field_id == 3) {
      if (type != kCompactList) return Status::Corruption("invalid column metadata list");
      uint32_t count = 0;
      uint8_t element_type = 0;
      status = reader->ReadListBegin(&count, &element_type);
      if (!status.ok()) return status;
      if ((field_id == 2 && (element_type != kCompactI32 || count != 2)) ||
          (field_id == 3 && (element_type != kCompactBinary || count != 1))) {
        return Status::NotSupported("unsupported Cedar Parquet column metadata");
      }
      if (field_id == 2) {
        int32_t first = 0;
        int32_t second = 0;
        status = reader->ReadI32(&first);
        if (!status.ok()) return status;
        status = reader->ReadI32(&second);
        if (!status.ok()) return status;
        if (first != kPlain || second != kRle) return Status::NotSupported("unsupported Parquet encoding");
        has_encodings = true;
      } else {
        status = reader->ReadBinary(&column->path);
        if (!status.ok()) return status;
        has_path = true;
      }
      continue;
    }
    if (field_id == 1 || field_id == 4) {
      if (type != kCompactI32) return Status::Corruption("invalid column metadata enum");
      int32_t value = 0;
      status = reader->ReadI32(&value);
      if (!status.ok()) return status;
      if (field_id == 1) {
        if (value < kParquetBoolean || value > kParquetFixedLenByteArray ||
            value == 3) return Status::NotSupported("unsupported Parquet physical type");
        column->physical_type = value;
        has_type = true;
      } else {
        if (value != static_cast<int32_t>(CedarParquetCompressionCodec::kUncompressed) &&
            value != static_cast<int32_t>(CedarParquetCompressionCodec::kLz4Raw) &&
            value != static_cast<int32_t>(CedarParquetCompressionCodec::kZstd)) {
          return Status::NotSupported("unsupported Parquet codec");
        }
        column->compression_codec =
            static_cast<CedarParquetCompressionCodec>(value);
        has_codec = true;
      }
      continue;
    }
    if (field_id == 12) {
      if (type != kCompactStruct) return Status::Corruption("invalid column statistics");
      reader->ReadStructBegin();
      bool has_min = false;
      bool has_max = false;
      while (true) {
        int16_t statistics_field_id = 0;
        uint8_t statistics_type = 0;
        status = reader->ReadFieldBegin(&statistics_field_id, &statistics_type);
        if (!status.ok()) return status;
        if (statistics_type == kCompactStop) break;
        if (statistics_type != kCompactBinary ||
            (statistics_field_id != 5 && statistics_field_id != 6)) {
          return Status::NotSupported("unsupported Cedar Parquet column statistics");
        }
        std::string* value = statistics_field_id == 5 ? &column->max_value
                                                       : &column->min_value;
        status = reader->ReadBinary(value);
        if (!status.ok()) return status;
        if (statistics_field_id == 5) has_max = true;
        else has_min = true;
      }
      reader->ReadStructEnd();
      if (!has_min || !has_max) return Status::Corruption("incomplete column statistics");
      has_statistics = true;
      continue;
    }
    if (field_id == 5 || field_id == 6 || field_id == 7 || field_id == 9 ||
        field_id == 14) {
      if (type != kCompactI64) return Status::Corruption("invalid column metadata size");
      int64_t value = 0;
      status = reader->ReadI64(&value);
      if (!status.ok()) return status;
      if (field_id == 5) { column->num_values = value; has_num_values = true; }
      if (field_id == 6) { column->total_uncompressed_size = value; has_uncompressed_size = true; }
      if (field_id == 7) { column->total_compressed_size = value; has_compressed_size = true; }
      if (field_id == 9) { column->data_page_offset = value; has_data_page_offset = true; }
      if (field_id == 14) { column->bloom_filter_offset = value; }
      continue;
    }
    if (field_id == 15) {
      if (type != kCompactI32) return Status::Corruption("invalid Parquet Bloom length");
      status = reader->ReadI32(&column->bloom_filter_length);
      if (!status.ok()) return status;
      continue;
    }
    return Status::NotSupported("unsupported Parquet column metadata field");
  }
  reader->ReadStructEnd();
  if (!has_type || !has_encodings || !has_path || !has_codec || !has_num_values ||
      !has_uncompressed_size || !has_compressed_size || !has_data_page_offset) {
    return Status::Corruption("missing Parquet column metadata field");
  }
  if (column->path == "sort_key" && has_statistics &&
      column->min_value > column->max_value) {
    return Status::Corruption("invalid Cedar Parquet sort-key statistics");
  }
  return Status::OK();
}

void WriteColumnChunk(CompactWriter* writer,
                      const CedarParquetFooter::ColumnChunk& column) {
  writer->WriteStructBegin();
  writer->WriteI64Field(2, column.data_page_offset);
  writer->WriteStructFieldBegin(3);
  WriteColumnMetaData(writer, column);
  if (column.offset_index_offset >= 0) {
    writer->WriteI64Field(4, column.offset_index_offset);
    writer->WriteI32Field(5, column.offset_index_length);
  }
  if (column.column_index_offset >= 0) {
    writer->WriteI64Field(6, column.column_index_offset);
    writer->WriteI32Field(7, column.column_index_length);
  }
  writer->WriteFieldStop();
  writer->WriteStructEnd();
}

Status ReadColumnChunk(CompactReader* reader,
                       CedarParquetFooter::ColumnChunk* column) {
  bool has_offset = false;
  bool has_metadata = false;
  bool has_offset_index_offset = false;
  bool has_offset_index_length = false;
  reader->ReadStructBegin();
  while (true) {
    int16_t field_id = 0;
    uint8_t type = 0;
    Status status = reader->ReadFieldBegin(&field_id, &type);
    if (!status.ok()) return status;
    if (type == kCompactStop) break;
    if (field_id == 2) {
      if (type != kCompactI64) return Status::Corruption("invalid column chunk offset");
      status = reader->ReadI64(&column->data_page_offset);
      if (!status.ok()) return status;
      has_offset = true;
    } else if (field_id == 3) {
      if (type != kCompactStruct) return Status::Corruption("invalid column chunk metadata");
      status = ReadColumnMetaData(reader, column);
      if (!status.ok()) return status;
      has_metadata = true;
    } else if (field_id == 4) {
      if (type != kCompactI64) {
        return Status::Corruption("invalid offset-index offset");
      }
      status = reader->ReadI64(&column->offset_index_offset);
      if (!status.ok()) return status;
      has_offset_index_offset = true;
    } else if (field_id == 5) {
      if (type != kCompactI32) {
        return Status::Corruption("invalid offset-index length");
      }
      status = reader->ReadI32(&column->offset_index_length);
      if (!status.ok()) return status;
      has_offset_index_length = true;
    } else if (field_id == 6) {
      if (type != kCompactI64) {
        return Status::Corruption("invalid column-index offset");
      }
      status = reader->ReadI64(&column->column_index_offset);
      if (!status.ok()) return status;
    } else if (field_id == 7) {
      if (type != kCompactI32) {
        return Status::Corruption("invalid column-index length");
      }
      status = reader->ReadI32(&column->column_index_length);
      if (!status.ok()) return status;
    } else {
      return Status::NotSupported("unsupported Parquet column chunk field");
    }
  }
  reader->ReadStructEnd();
  if (!has_offset || !has_metadata || !has_offset_index_offset ||
      !has_offset_index_length || column->offset_index_offset < 4 ||
      column->offset_index_length <= 0) {
    return Status::Corruption("missing Parquet column chunk field");
  }
  return Status::OK();
}

void WriteRowGroup(CompactWriter* writer, const CedarParquetFooter::RowGroup& row_group) {
  writer->WriteStructBegin();
  writer->WriteListFieldBegin(1, static_cast<uint32_t>(row_group.columns.size()), kCompactStruct);
  for (const auto& column : row_group.columns) WriteColumnChunk(writer, column);
  writer->WriteListEnd();
  writer->WriteI64Field(2, row_group.total_byte_size);
  writer->WriteI64Field(3, row_group.num_rows);
  writer->WriteI64Field(5, row_group.file_offset);
  writer->WriteFieldStop();
  writer->WriteStructEnd();
}

Status ReadRowGroup(CompactReader* reader, CedarParquetFooter::RowGroup* row_group) {
  bool has_columns = false;
  bool has_total_bytes = false;
  bool has_num_rows = false;
  bool has_file_offset = false;
  reader->ReadStructBegin();
  while (true) {
    int16_t field_id = 0;
    uint8_t type = 0;
    Status status = reader->ReadFieldBegin(&field_id, &type);
    if (!status.ok()) return status;
    if (type == kCompactStop) break;
    if (field_id == 1) {
      if (type != kCompactList) return Status::Corruption("invalid row-group columns");
      uint32_t count = 0;
      uint8_t element_type = 0;
      status = reader->ReadListBegin(&count, &element_type);
      if (!status.ok()) return status;
      if (element_type != kCompactStruct || count != kFactsColumns.size()) {
        return Status::NotSupported("unsupported row-group columns");
      }
      row_group->columns.resize(count);
      for (auto& column : row_group->columns) {
        status = ReadColumnChunk(reader, &column);
        if (!status.ok()) return status;
      }
      has_columns = true;
    } else if (field_id == 2 || field_id == 3 || field_id == 5) {
      if (type != kCompactI64) return Status::Corruption("invalid row-group value");
      int64_t value = 0;
      status = reader->ReadI64(&value);
      if (!status.ok()) return status;
      if (field_id == 2) { row_group->total_byte_size = value; has_total_bytes = true; }
      if (field_id == 3) { row_group->num_rows = value; has_num_rows = true; }
      if (field_id == 5) { row_group->file_offset = value; has_file_offset = true; }
    } else {
      return Status::NotSupported("unsupported Parquet row-group field");
    }
  }
  reader->ReadStructEnd();
  if (!has_columns || !has_total_bytes || !has_num_rows || !has_file_offset) {
    return Status::Corruption("missing Parquet row-group field");
  }
  return Status::OK();
}

}  // namespace

CedarParquetFooter MakeRequiredFactsFooter(
    int64_t num_rows,
    const std::vector<CedarParquetKeyValueMetadata>& key_value_metadata) {
  CedarParquetFooter footer;
  footer.num_rows = num_rows;
  footer.key_value_metadata = key_value_metadata;
  footer.schema.push_back(
      {"schema", -1, 0, -1, static_cast<int32_t>(kFactsColumns.size())});
  for (const ColumnSpec& spec : kFactsColumns) {
    footer.schema.push_back(
        {spec.name, spec.physical_type, spec.type_length, spec.repetition_type, 0});
  }
  return footer;
}

Status AddCedarParquetRowGroup(CedarParquetFooter* footer,
                               CedarParquetFooter::RowGroup row_group) {
  if (row_group.num_rows <= 0 || row_group.columns.size() != kFactsColumns.size()) {
    return Status::InvalidArgument("Cedar Parquet row group must contain all facts columns");
  }
  if (footer->num_rows > std::numeric_limits<int64_t>::max() - row_group.num_rows) {
    return Status::MemoryLimit("Cedar Parquet row count overflow");
  }
  footer->num_rows += row_group.num_rows;
  footer->row_groups.push_back(std::move(row_group));
  return Status::OK();
}

Status EncodeCompactFooter(const CedarParquetFooter& footer, std::string* encoded) {
  Status status = ValidateRequiredFactsSchema(footer);
  if (!status.ok()) return status;
  CompactWriter writer;
  writer.WriteStructBegin();
  writer.WriteI32Field(1, footer.version);
  writer.WriteListFieldBegin(2, static_cast<uint32_t>(footer.schema.size()), kCompactStruct);
  for (const auto& element : footer.schema) WriteSchemaElement(&writer, element);
  writer.WriteListEnd();
  writer.WriteI64Field(3, footer.num_rows);
  writer.WriteListFieldBegin(4, static_cast<uint32_t>(footer.row_groups.size()), kCompactStruct);
  for (const auto& row_group : footer.row_groups) WriteRowGroup(&writer, row_group);
  writer.WriteListEnd();
  if (!footer.key_value_metadata.empty()) {
    writer.WriteListFieldBegin(5, static_cast<uint32_t>(footer.key_value_metadata.size()),
                               kCompactStruct);
    for (const auto& metadata : footer.key_value_metadata) {
      WriteKeyValueMetadata(&writer, metadata);
    }
    writer.WriteListEnd();
  }
  if (!footer.created_by.empty()) writer.WriteBinaryField(6, footer.created_by);
  writer.WriteFieldStop();
  writer.WriteStructEnd();
  *encoded = writer.data();
  return Status::OK();
}

Status DecodeCompactFooter(const std::string& encoded, CedarParquetFooter* footer) {
  if (encoded.empty()) return Status::Corruption("empty Parquet footer");
  CompactReader reader(encoded);
  CedarParquetFooter decoded;
  bool has_version = false;
  bool has_schema = false;
  bool has_num_rows = false;
  bool has_row_groups = false;
  reader.ReadStructBegin();
  while (true) {
    int16_t field_id = 0;
    uint8_t type = 0;
    Status status = reader.ReadFieldBegin(&field_id, &type);
    if (!status.ok()) return status;
    if (type == kCompactStop) break;
    switch (field_id) {
      case 1:
        if (type != kCompactI32) return Status::Corruption("invalid footer version");
        status = reader.ReadI32(&decoded.version);
        has_version = status.ok();
        break;
      case 2: {
        if (type != kCompactList) return Status::Corruption("invalid footer schema");
        uint32_t count = 0;
        uint8_t element_type = 0;
        status = reader.ReadListBegin(&count, &element_type);
        if (!status.ok()) return status;
        if (element_type != kCompactStruct || count != kFactsColumns.size() + 1) {
          return Status::NotSupported("unsupported Parquet schema");
        }
        decoded.schema.resize(count);
        for (auto& element : decoded.schema) {
          status = ReadSchemaElement(&reader, &element);
          if (!status.ok()) return status;
        }
        has_schema = true;
        break;
      }
      case 3:
        if (type != kCompactI64) return Status::Corruption("invalid footer row count");
        status = reader.ReadI64(&decoded.num_rows);
        has_num_rows = status.ok();
        break;
      case 4: {
        if (type != kCompactList) return Status::Corruption("invalid footer row groups");
        uint32_t count = 0;
        uint8_t element_type = 0;
        status = reader.ReadListBegin(&count, &element_type);
        if (!status.ok()) return status;
        if (element_type != kCompactStruct) return Status::Corruption("invalid Parquet row group");
        decoded.row_groups.resize(count);
        for (auto& row_group : decoded.row_groups) {
          status = ReadRowGroup(&reader, &row_group);
          if (!status.ok()) return status;
        }
        has_row_groups = true;
        break;
      }
      case 5: {
        if (type != kCompactList) {
          return Status::Corruption("invalid footer key-value metadata");
        }
        uint32_t count = 0;
        uint8_t element_type = 0;
        status = reader.ReadListBegin(&count, &element_type);
        if (!status.ok()) return status;
        if (element_type != kCompactStruct || count == 0 || count > 16) {
          return Status::Corruption("invalid footer key-value metadata");
        }
        decoded.key_value_metadata.resize(count);
        for (auto& metadata : decoded.key_value_metadata) {
          status = ReadKeyValueMetadata(&reader, &metadata);
          if (!status.ok()) return status;
        }
        break;
      }
      case 6:
        if (type != kCompactBinary) return Status::Corruption("invalid footer creator");
        status = reader.ReadBinary(&decoded.created_by);
        if (!status.ok() || decoded.created_by.empty() ||
            decoded.created_by.size() > 128) {
          return Status::Corruption("invalid footer creator");
        }
        break;
      default:
        return Status::NotSupported("unsupported Parquet footer field");
    }
    if (!status.ok()) return status;
  }
  reader.ReadStructEnd();
  if (!reader.empty()) return Status::Corruption("trailing compact footer data");
  if (!has_version || !has_schema || !has_num_rows || !has_row_groups) {
    return Status::Corruption("missing required Parquet footer field");
  }
  if (decoded.schema.size() == kFactsColumns.size() + 1) {
    for (auto& row_group : decoded.row_groups) {
      if (row_group.columns.size() != kFactsColumns.size()) continue;
      for (size_t index = 0; index < row_group.columns.size(); ++index) {
        row_group.columns[index].type_length =
            decoded.schema[index + 1].type_length;
      }
    }
  }
  Status status = ValidateRequiredFactsSchema(decoded);
  if (!status.ok()) return status;
  *footer = std::move(decoded);
  return Status::OK();
}

Status AppendParquetFooter(std::string* file, const CedarParquetFooter& footer) {
  if (file->empty()) file->append(kParquetMagic, 4);
  if (file->size() < 4 || file->compare(0, 4, kParquetMagic) != 0) {
    return Status::InvalidArgument("Parquet file must start with PAR1");
  }
  std::string encoded;
  Status status = EncodeCompactFooter(footer, &encoded);
  if (!status.ok()) return status;
  if (encoded.size() > std::numeric_limits<uint32_t>::max()) {
    return Status::MemoryLimit("Parquet footer exceeds 4 GiB");
  }
  file->append(encoded);
  AppendFixed32(file, static_cast<uint32_t>(encoded.size()));
  file->append(kParquetMagic, 4);
  return Status::OK();
}

Status ParseParquetFooter(const std::string& file, CedarParquetFooter* footer,
                          size_t* footer_offset) {
  if (file.size() < 12 || file.compare(0, 4, kParquetMagic) != 0 ||
      file.compare(file.size() - 4, 4, kParquetMagic) != 0) {
    return Status::Corruption("missing Parquet magic");
  }
  const uint32_t footer_size = DecodeFixed32(file.data() + file.size() - 8);
  const size_t trailer_offset = file.size() - 8;
  if (footer_size > trailer_offset - 4) return Status::Corruption("invalid Parquet footer size");
  const size_t offset = trailer_offset - footer_size;
  Status status = DecodeCompactFooter(file.substr(offset, footer_size), footer);
  if (!status.ok()) return status;
  if (footer_offset != nullptr) *footer_offset = offset;
  return Status::OK();
}

Status EncodeOffsetIndex(
    const std::vector<CedarParquetFooter::ColumnChunk::PageLocation>& locations,
    std::string* encoded) {
  if (locations.empty()) return Status::InvalidArgument("Parquet offset index has no pages");
  CompactWriter writer;
  writer.WriteStructBegin();
  writer.WriteListFieldBegin(1, static_cast<uint32_t>(locations.size()),
                             kCompactStruct);
  int64_t previous_offset = -1;
  int64_t previous_first_row = -1;
  for (const auto& location : locations) {
    if (location.offset < 4 || location.compressed_page_size <= 0 ||
        location.first_row_index < 0 ||
        (previous_offset >= 0 && location.offset <= previous_offset) ||
        (previous_first_row >= 0 &&
         location.first_row_index <= previous_first_row)) {
      return Status::InvalidArgument("invalid Parquet offset-index page location");
    }
    writer.WriteStructBegin();
    writer.WriteI64Field(1, location.offset);
    writer.WriteI32Field(2, location.compressed_page_size);
    writer.WriteI64Field(3, location.first_row_index);
    writer.WriteFieldStop();
    writer.WriteStructEnd();
    previous_offset = location.offset;
    previous_first_row = location.first_row_index;
  }
  writer.WriteListEnd();
  writer.WriteFieldStop();
  writer.WriteStructEnd();
  *encoded = writer.data();
  return Status::OK();
}

Status DecodeOffsetIndex(
    const std::string& encoded,
    std::vector<CedarParquetFooter::ColumnChunk::PageLocation>* locations) {
  CompactReader reader(encoded);
  reader.ReadStructBegin();
  bool has_locations = false;
  std::vector<CedarParquetFooter::ColumnChunk::PageLocation> decoded;
  while (true) {
    int16_t field_id = 0;
    uint8_t type = 0;
    Status status = reader.ReadFieldBegin(&field_id, &type);
    if (!status.ok()) return status;
    if (type == kCompactStop) break;
    if (field_id != 1 || type != kCompactList) {
      return Status::NotSupported("unsupported Parquet offset-index field");
    }
    uint32_t count = 0;
    uint8_t element_type = 0;
    status = reader.ReadListBegin(&count, &element_type);
    if (!status.ok()) return status;
    if (count == 0 || element_type != kCompactStruct) {
      return Status::Corruption("invalid Parquet offset-index locations");
    }
    decoded.reserve(count);
    int64_t previous_offset = -1;
    int64_t previous_first_row = -1;
    for (uint32_t index = 0; index < count; ++index) {
      CedarParquetFooter::ColumnChunk::PageLocation location;
      reader.ReadStructBegin();
      bool has_offset = false;
      bool has_size = false;
      bool has_first_row = false;
      while (true) {
        int16_t location_field_id = 0;
        uint8_t location_type = 0;
        status = reader.ReadFieldBegin(&location_field_id, &location_type);
        if (!status.ok()) return status;
        if (location_type == kCompactStop) break;
        if (location_field_id == 1 || location_field_id == 3) {
          if (location_type != kCompactI64) {
            return Status::Corruption("invalid Parquet page location");
          }
          int64_t value = 0;
          status = reader.ReadI64(&value);
          if (!status.ok()) return status;
          if (location_field_id == 1) {
            location.offset = value;
            has_offset = true;
          } else {
            location.first_row_index = value;
            has_first_row = true;
          }
        } else if (location_field_id == 2) {
          if (location_type != kCompactI32) {
            return Status::Corruption("invalid Parquet page size");
          }
          status = reader.ReadI32(&location.compressed_page_size);
          if (!status.ok()) return status;
          has_size = true;
        } else {
          return Status::NotSupported("unsupported Parquet page-location field");
        }
      }
      reader.ReadStructEnd();
      if (!has_offset || !has_size || !has_first_row || location.offset < 4 ||
          location.compressed_page_size <= 0 || location.first_row_index < 0 ||
          (previous_offset >= 0 && location.offset <= previous_offset) ||
          (previous_first_row >= 0 &&
           location.first_row_index <= previous_first_row)) {
        return Status::Corruption("invalid Parquet offset-index page location");
      }
      previous_offset = location.offset;
      previous_first_row = location.first_row_index;
      decoded.push_back(location);
    }
    has_locations = true;
  }
  reader.ReadStructEnd();
  if (!reader.empty() || !has_locations) {
    return Status::Corruption("invalid Parquet offset index");
  }
  *locations = std::move(decoded);
  return Status::OK();
}

Status EncodeColumnIndex(
    const std::vector<CedarParquetFooter::ColumnChunk::PageIndex>& pages,
    std::string* encoded) {
  if (pages.empty()) return Status::InvalidArgument("Parquet column index has no pages");
  CompactWriter writer;
  writer.WriteStructBegin();
  writer.WriteListFieldBegin(1, static_cast<uint32_t>(pages.size()), kCompactTrue);
  bool ascending = true;
  std::string previous_max;
  for (const auto& page : pages) {
    writer.WriteBool(page.all_null);
    if (page.all_null) continue;
    if (page.min_value > page.max_value ||
        (!previous_max.empty() && previous_max >= page.min_value)) {
      ascending = false;
    }
    previous_max = page.max_value;
  }
  writer.WriteListEnd();
  writer.WriteListFieldBegin(2, static_cast<uint32_t>(pages.size()), kCompactBinary);
  for (const auto& page : pages) writer.WriteBinary(page.min_value);
  writer.WriteListEnd();
  writer.WriteListFieldBegin(3, static_cast<uint32_t>(pages.size()), kCompactBinary);
  for (const auto& page : pages) {
    writer.WriteBinary(page.max_value);
  }
  writer.WriteListEnd();
  writer.WriteI32Field(4, ascending ? 1 : 0);
  writer.WriteFieldStop();
  writer.WriteStructEnd();
  *encoded = writer.data();
  return Status::OK();
}

Status DecodeColumnIndex(
    const std::string& encoded,
    std::vector<CedarParquetFooter::ColumnChunk::PageIndex>* pages) {
  CompactReader reader(encoded);
  reader.ReadStructBegin();
  std::vector<bool> null_pages;
  std::vector<std::string> minimums;
  std::vector<std::string> maximums;
  bool has_null_pages = false;
  bool has_minimums = false;
  bool has_maximums = false;
  bool has_boundary_order = false;
  while (true) {
    int16_t field_id = 0;
    uint8_t type = 0;
    Status status = reader.ReadFieldBegin(&field_id, &type);
    if (!status.ok()) return status;
    if (type == kCompactStop) break;
    if (field_id < 1 || field_id > 4) {
      return Status::NotSupported("unsupported Parquet column-index field");
    }
    if (field_id == 4) {
      if (type != kCompactI32) return Status::Corruption("invalid column-index order");
      int32_t boundary_order = 0;
      status = reader.ReadI32(&boundary_order);
      if (!status.ok()) return status;
      if (boundary_order != 0 && boundary_order != 1) {
        return Status::NotSupported("unsupported Cedar Parquet column-index order");
      }
      has_boundary_order = true;
      continue;
    }
    if (type != kCompactList) return Status::Corruption("invalid Parquet column-index list");
    uint32_t count = 0;
    uint8_t element_type = 0;
    status = reader.ReadListBegin(&count, &element_type);
    if (!status.ok()) return status;
    if (count == 0 || count > (64U << 20)) {
      return Status::Corruption("invalid Parquet column-index length");
    }
    if (field_id == 1) {
      if (element_type != kCompactTrue) return Status::Corruption("invalid column-index null pages");
      null_pages.reserve(count);
      for (uint32_t index = 0; index < count; ++index) {
        bool is_null = false;
        status = reader.ReadBool(&is_null);
        if (!status.ok()) return status;
        null_pages.push_back(is_null);
      }
      has_null_pages = true;
    } else {
      if (element_type != kCompactBinary) {
        return Status::Corruption("invalid column-index bound type");
      }
      std::vector<std::string>& values = field_id == 2 ? minimums : maximums;
      values.reserve(count);
      for (uint32_t index = 0; index < count; ++index) {
        std::string value;
        status = reader.ReadBinary(&value);
        if (!status.ok()) return status;
        values.push_back(std::move(value));
      }
      if (field_id == 2) has_minimums = true;
      else has_maximums = true;
    }
  }
  reader.ReadStructEnd();
  if (!reader.empty() || !has_null_pages || !has_minimums || !has_maximums ||
      !has_boundary_order || null_pages.size() != minimums.size() ||
      minimums.size() != maximums.size()) {
    return Status::Corruption("incomplete Parquet column index");
  }
  std::vector<CedarParquetFooter::ColumnChunk::PageIndex> decoded;
  decoded.reserve(minimums.size());
  for (size_t index = 0; index < minimums.size(); ++index) {
    if (null_pages[index]) {
      if (!minimums[index].empty() || !maximums[index].empty()) {
        return Status::Corruption("invalid all-null Parquet column-index page");
      }
      decoded.push_back({"", "", true});
      continue;
    }
    if (minimums[index] > maximums[index]) {
      return Status::Corruption("invalid ascending Parquet column-index bounds");
    }
    decoded.push_back({std::move(minimums[index]), std::move(maximums[index]), false});
  }
  *pages = std::move(decoded);
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
