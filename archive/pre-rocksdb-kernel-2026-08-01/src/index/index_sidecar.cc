#include "cedar/index/index_sidecar.h"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <utility>
#include <sys/stat.h>
#include <unistd.h>
#include "cedar/blob/blob_store.h"
#include "cedar/core/crc32c.h"
namespace cedar { namespace {
constexpr char kIndexSidecarMagic[] = "CSI3";
void U64(std::string* o,uint64_t v){for(int i=0;i<8;++i)o->push_back(static_cast<char>(v>>(8*i)));}
bool R64(const std::string& s,size_t* p,uint64_t* v){if(s.size()-*p<8)return false;*v=0;for(int i=0;i<8;++i)*v|=uint64_t(uint8_t(s[(*p)++]))<<(8*i);return true;}
void U32(std::string* o,uint32_t v){for(int i=0;i<4;++i)o->push_back(static_cast<char>(v>>(8*i)));}
bool R32(const std::string&s,size_t*p,uint32_t*v){if(s.size()-*p<4)return false;*v=0;for(int i=0;i<4;++i)*v|=uint32_t(uint8_t(s[(*p)++]))<<(8*i);return true;}
constexpr uint64_t kMaximumIndexSidecarBytes = 1ULL << 30;
Status WriteAll(int fd, const std::string& bytes, const std::string& path) {
 const char* data=bytes.data(); size_t remaining=bytes.size();
 while(remaining!=0){const ssize_t written=::write(fd,data,remaining);if(written<0){if(errno==EINTR)continue;return Status::IOError(path,std::strerror(errno));}data+=written;remaining-=static_cast<size_t>(written);}return Status::OK();
}
StatusOr<std::string> ReadAll(const std::string& path,
                              uint64_t max_bytes = kMaximumIndexSidecarBytes) {
 const int fd=::open(path.c_str(),O_RDONLY);if(fd<0)return Status::IOError(path,std::strerror(errno));
 struct stat metadata{};if(::fstat(fd,&metadata)!=0){const Status status=Status::IOError(path,std::strerror(errno));::close(fd);return status;}
 if(metadata.st_size<0||static_cast<uint64_t>(metadata.st_size)>max_bytes||static_cast<uint64_t>(metadata.st_size)>kMaximumIndexSidecarBytes){::close(fd);return Status::Corruption("index sidecar","file size exceeds bound");}
 std::string bytes(static_cast<size_t>(metadata.st_size),'\0');size_t offset=0;
 while(offset<bytes.size()){const ssize_t count=::read(fd,&bytes[offset],bytes.size()-offset);if(count<0){if(errno==EINTR)continue;const Status status=Status::IOError(path,std::strerror(errno));::close(fd);return status;}if(count==0){::close(fd);return Status::Corruption("index sidecar","truncated file");}offset+=static_cast<size_t>(count);}
 if(::close(fd)!=0)return Status::IOError(path,std::strerror(errno));return bytes;
}
}
namespace {
void PutVarint(std::string* output, uint64_t value) {
  while (value >= 0x80) {
    output->push_back(static_cast<char>((value & 0x7f) | 0x80));
    value >>= 7;
  }
  output->push_back(static_cast<char>(value));
}

bool GetVarint(const std::string& input, size_t* offset, uint64_t* value,
               size_t limit) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift < 64 && *offset < limit; shift += 7) {
    const uint8_t byte = static_cast<uint8_t>(input[(*offset)++]);
    if (shift == 63 && (byte & 0x7e) != 0) return false;
    result |= static_cast<uint64_t>(byte & 0x7f) << shift;
    if ((byte & 0x80) == 0) {
      *value = result;
      return true;
    }
  }
  return false;
}

void PutValue(std::string* output, const IndexCanonicalValue& value) {
  output->push_back(static_cast<char>(value.type));
  output->push_back(static_cast<char>(value.kind));
  U32(output, static_cast<uint32_t>(value.bytes.size()));
  output->append(value.bytes);
}

bool GetValue(const std::string& input, size_t* offset, size_t limit,
              IndexCanonicalValue* value) {
  if (*offset >= limit) return false;
  value->type = static_cast<PhysicalType>(static_cast<uint8_t>(input[(*offset)++]));
  if (*offset >= limit) return false;
  value->kind = static_cast<IndexCanonicalKind>(
      static_cast<uint8_t>(input[(*offset)++]));
  if (value->kind != IndexCanonicalKind::kInline &&
      value->kind != IndexCanonicalKind::kBlobHash) {
    return false;
  }
  uint32_t length = 0;
  if (!R32(input, offset, &length) || *offset > limit ||
      length > limit - *offset) {
    return false;
  }
  value->bytes.assign(input.data() + *offset, length);
  *offset += length;
  return true;
}

bool SupportedPhysicalEncoding(uint32_t encoding) {
  return IsSupportedIndexCanonicalEncoding(encoding);
}

StatusOr<uint64_t> CheckedAdd(uint64_t left, uint64_t right,
                              const char* dimension) {
  if (right > std::numeric_limits<uint64_t>::max() - left) {
    return Status::InvalidArgument(
        "index sidecar estimate", std::string(dimension) + " overflow");
  }
  return left + right;
}

StatusOr<uint64_t> CheckedMultiply(uint64_t left, uint64_t right,
                                   const char* dimension) {
  if (left != 0 && right > std::numeric_limits<uint64_t>::max() / left) {
    return Status::InvalidArgument(
        "index sidecar estimate", std::string(dimension) + " overflow");
  }
  return left * right;
}

uint64_t MaximumCanonicalValueBytes(const ColumnSchema& schema) {
  switch (schema.physical_type) {
    case PhysicalType::kBool:
      return 1;
    case PhysicalType::kInt32:
    case PhysicalType::kFloat32:
      return 4;
    case PhysicalType::kInt64:
    case PhysicalType::kFloat64:
    case PhysicalType::kTimestamp64:
      return 8;
    case PhysicalType::kString:
    case PhysicalType::kBinary:
      return std::max<uint64_t>(schema.blob_threshold, 40);
  }
  return 0;
}
}  // namespace

StatusOr<uint64_t> EstimateIndexSidecarEncodedBytes(
    const IndexDefinition& definition,
    const SstFileStatistics& source_statistics,
    uint64_t source_file_bytes, const ColumnSchema& schema) {
  if (definition.index_id == 0 || source_file_bytes == 0 ||
      !SupportedPhysicalEncoding(definition.canonical_encoding_id) ||
      definition.entity_type != schema.entity_type ||
      definition.column_id != schema.column_id ||
      definition.schema_epoch != schema.schema_epoch) {
    return Status::InvalidArgument("index sidecar estimate",
                                   "invalid source or definition");
  }
  const uint64_t postings = source_statistics.put_count;
  const auto value_bytes = CheckedAdd(
      MaximumCanonicalValueBytes(schema), 6, "canonical value bytes");
  if (!value_bytes.ok()) return value_bytes.status();

  uint64_t bytes_per_posting = 0;
  if (definition.canonical_encoding_id == kIndexCanonicalEncoding) {
    const auto with_metadata = CheckedAdd(value_bytes.ValueOrDie(), 24,
                                          "plain posting bytes");
    if (!with_metadata.ok()) return with_metadata.status();
    bytes_per_posting = with_metadata.ValueOrDie();
  } else if (definition.canonical_encoding_id ==
             kIndexCanonicalEncodingDictionary) {
    const auto with_dictionary_index = CheckedAdd(
        value_bytes.ValueOrDie(), 32, "dictionary posting bytes");
    if (!with_dictionary_index.ok()) return with_dictionary_index.status();
    bytes_per_posting = with_dictionary_index.ValueOrDie();
  } else if (definition.canonical_encoding_id ==
             kIndexCanonicalEncodingBitmap) {
    const uint64_t bitmap_bytes = source_statistics.row_count / 8 + 1;
    const auto with_group_fields = CheckedAdd(
        value_bytes.ValueOrDie(), 40, "bitmap group bytes");
    if (!with_group_fields.ok()) return with_group_fields.status();
    const auto with_bitmap = CheckedAdd(
        with_group_fields.ValueOrDie(), bitmap_bytes, "bitmap posting bytes");
    if (!with_bitmap.ok()) return with_bitmap.status();
    bytes_per_posting = with_bitmap.ValueOrDie();
  } else {
    const auto with_varints = CheckedAdd(value_bytes.ValueOrDie(), 30,
                                         "delta posting bytes");
    if (!with_varints.ok()) return with_varints.status();
    bytes_per_posting = with_varints.ValueOrDie();
  }

  const auto body = CheckedMultiply(postings, bytes_per_posting,
                                    "posting encoding bytes");
  if (!body.ok()) return body.status();
  const auto total = CheckedAdd(body.ValueOrDie(), 52, "encoded bytes");
  if (!total.ok()) return total.status();
  if (total.ValueOrDie() > kMaximumIndexSidecarBytes) {
    return Status::InvalidArgument("index sidecar estimate",
                                   "encoded sidecar exceeds size bound");
  }
  return total.ValueOrDie();
}

StatusOr<std::string> BuildIndexSidecar(const IndexDefinition& d, uint64_t source,
                                        std::vector<IndexPosting> p) {
  if (d.index_id == 0 || !SupportedPhysicalEncoding(d.canonical_encoding_id)) {
    return Status::InvalidArgument("index sidecar", "invalid definition");
  }
  std::sort(p.begin(), p.end(), [](const auto& a, const auto& b) {
    const int c = CompareIndexCanonicalValues(a.value, b.value);
    return c ? c < 0 : a.source_row_ordinal < b.source_row_ordinal;
  });
  std::string output = kIndexSidecarMagic;
  U64(&output, source);
  U64(&output, d.index_id);
  U32(&output, d.schema_epoch);
  U32(&output, d.canonical_encoding_id);
  U64(&output, p.size());
  if (d.canonical_encoding_id == kIndexCanonicalEncoding) {
    for (const auto& posting : p) {
      PutValue(&output, posting.value);
      U64(&output, posting.source_row_ordinal);
      U64(&output, posting.valid_from);
      U64(&output, posting.commit_seq);
    }
  } else if (d.canonical_encoding_id == kIndexCanonicalEncodingDictionary) {
    std::vector<IndexCanonicalValue> dictionary;
    for (const auto& posting : p) {
      if (dictionary.empty() ||
          CompareIndexCanonicalValues(dictionary.back(), posting.value) != 0) {
        dictionary.push_back(posting.value);
      }
    }
    U64(&output, dictionary.size());
    for (const auto& value : dictionary) PutValue(&output, value);
    for (const auto& posting : p) {
      const auto found = std::lower_bound(
          dictionary.begin(), dictionary.end(), posting.value,
          [](const auto& left, const auto& right) {
            return CompareIndexCanonicalValues(left, right) < 0;
          });
      U64(&output, static_cast<uint64_t>(found - dictionary.begin()));
      U64(&output, posting.source_row_ordinal);
      U64(&output, posting.valid_from);
      U64(&output, posting.commit_seq);
    }
  } else if (d.canonical_encoding_id == kIndexCanonicalEncodingBitmap) {
    std::vector<std::pair<IndexCanonicalValue, std::vector<IndexPosting>>> groups;
    for (const auto& posting : p) {
      if (groups.empty() ||
          CompareIndexCanonicalValues(groups.back().first, posting.value) != 0) {
        groups.push_back({posting.value, {}});
      }
      groups.back().second.push_back(posting);
    }
    U64(&output, groups.size());
    for (const auto& group : groups) {
      PutValue(&output, group.first);
      uint64_t max_ordinal = 0;
      for (const auto& posting : group.second) {
        max_ordinal = std::max(max_ordinal, posting.source_row_ordinal);
      }
      U64(&output, max_ordinal);
      const uint64_t bitmap_bytes = max_ordinal / 8 + 1;
      if (bitmap_bytes > kMaximumIndexSidecarBytes) {
        return Status::InvalidArgument("index sidecar", "bitmap exceeds size bound");
      }
      U64(&output, bitmap_bytes);
      std::string bitmap(bitmap_bytes, '\0');
      for (const auto& posting : group.second) {
        bitmap[posting.source_row_ordinal / 8] |=
            static_cast<char>(1U << (posting.source_row_ordinal % 8));
      }
      output.append(bitmap);
      U64(&output, group.second.size());
      for (const auto& posting : group.second) {
        U64(&output, posting.valid_from);
        U64(&output, posting.commit_seq);
      }
    }
  } else {
    uint64_t previous_ordinal = 0;
    uint64_t previous_valid = 0;
    uint64_t previous_commit = 0;
    for (const auto& posting : p) {
      PutValue(&output, posting.value);
      PutVarint(&output, posting.source_row_ordinal ^ previous_ordinal);
      PutVarint(&output, posting.valid_from ^ previous_valid);
      PutVarint(&output, posting.commit_seq ^ previous_commit);
      previous_ordinal = posting.source_row_ordinal;
      previous_valid = posting.valid_from;
      previous_commit = posting.commit_seq;
    }
  }
  U32(&output, crc32c::Value(output.data(), output.size()));
  return output;
}

StatusOr<IndexSidecar> ReadIndexSidecar(const std::string& input,
                                        const IndexDefinition& d,
                                        uint64_t source) {
  if (input.size() < 40 || input.substr(0, 4) != kIndexSidecarMagic) {
    return Status::Corruption("index sidecar", "header");
  }
  uint32_t stored = 0;
  size_t checksum_offset = input.size() - 4;
  if (!R32(input, &checksum_offset, &stored) ||
      stored != crc32c::Value(input.data(), input.size() - 4)) {
    return Status::Corruption("index sidecar", "checksum");
  }
  size_t offset = 4;
  uint64_t encoded_source = 0;
  uint64_t encoded_id = 0;
  uint64_t count = 0;
  uint32_t epoch = 0;
  uint32_t encoding = 0;
  if (!R64(input, &offset, &encoded_source) || !R64(input, &offset, &encoded_id) ||
      !R32(input, &offset, &epoch) || !R32(input, &offset, &encoding) ||
      !R64(input, &offset, &count) || encoded_source != source ||
      encoded_id != d.index_id || epoch != d.schema_epoch ||
      encoding != d.canonical_encoding_id || !SupportedPhysicalEncoding(encoding)) {
    return Status::Corruption("index sidecar", "identity");
  }
  if (count > kMaximumIndexSidecarBytes) {
    return Status::Corruption("index sidecar", "posting count exceeds bound");
  }
  const size_t limit = input.size() - 4;
  IndexSidecar result{source, {}};
  result.postings.reserve(static_cast<size_t>(count));
  auto append_metadata = [&](IndexCanonicalValue value) -> bool {
    IndexPosting posting{std::move(value), 0, 0, 0};
    if (!R64(input, &offset, &posting.source_row_ordinal) ||
        !R64(input, &offset, &posting.valid_from) ||
        !R64(input, &offset, &posting.commit_seq)) return false;
    result.postings.push_back(std::move(posting));
    return true;
  };
  if (encoding == kIndexCanonicalEncoding) {
    for (uint64_t i = 0; i < count; ++i) {
      IndexCanonicalValue value;
      if (!GetValue(input, &offset, limit, &value) ||
          !append_metadata(std::move(value))) {
        return Status::Corruption("index sidecar", "posting");
      }
    }
  } else if (encoding == kIndexCanonicalEncodingDictionary) {
    uint64_t dictionary_count = 0;
    if (!R64(input, &offset, &dictionary_count) || dictionary_count > count + 1) {
      return Status::Corruption("index sidecar", "dictionary");
    }
    std::vector<IndexCanonicalValue> dictionary;
    dictionary.reserve(static_cast<size_t>(dictionary_count));
    for (uint64_t i = 0; i < dictionary_count; ++i) {
      IndexCanonicalValue value;
      if (!GetValue(input, &offset, limit, &value)) {
        return Status::Corruption("index sidecar", "dictionary");
      }
      dictionary.push_back(std::move(value));
    }
    for (uint64_t i = 0; i < count; ++i) {
      uint64_t dictionary_index = 0;
      if (!R64(input, &offset, &dictionary_index) ||
          dictionary_index >= dictionary.size()) {
        return Status::Corruption("index sidecar", "dictionary posting");
      }
      if (!append_metadata(dictionary[static_cast<size_t>(dictionary_index)])) {
        return Status::Corruption("index sidecar", "dictionary posting");
      }
    }
  } else if (encoding == kIndexCanonicalEncodingBitmap) {
    uint64_t group_count = 0;
    if (!R64(input, &offset, &group_count) || group_count > count + 1) {
      return Status::Corruption("index sidecar", "bitmap");
    }
    for (uint64_t group = 0; group < group_count; ++group) {
      IndexCanonicalValue value;
      uint64_t max_ordinal = 0;
      uint64_t bitmap_bytes = 0;
      uint64_t posting_count = 0;
      if (!GetValue(input, &offset, limit, &value) ||
          !R64(input, &offset, &max_ordinal) ||
          !R64(input, &offset, &bitmap_bytes) ||
          bitmap_bytes != max_ordinal / 8 + 1 || offset > limit ||
          bitmap_bytes > limit - offset) {
        return Status::Corruption("index sidecar", "bitmap");
      }
      const std::string bitmap = input.substr(offset, static_cast<size_t>(bitmap_bytes));
      offset += static_cast<size_t>(bitmap_bytes);
      if (!R64(input, &offset, &posting_count) ||
          posting_count > count - result.postings.size()) {
        return Status::Corruption("index sidecar", "bitmap posting count");
      }
      std::vector<uint64_t> ordinals;
      for (uint64_t ordinal = 0; ordinal <= max_ordinal; ++ordinal) {
        if ((static_cast<uint8_t>(bitmap[ordinal / 8]) &
             (1U << (ordinal % 8))) != 0) {
          ordinals.push_back(ordinal);
        }
      }
      if (ordinals.size() != posting_count) {
        return Status::Corruption("index sidecar", "bitmap cardinality");
      }
      for (uint64_t ordinal : ordinals) {
        IndexPosting posting{value, ordinal, 0, 0};
        if (!R64(input, &offset, &posting.valid_from) ||
            !R64(input, &offset, &posting.commit_seq)) {
          return Status::Corruption("index sidecar", "bitmap posting");
        }
        result.postings.push_back(std::move(posting));
      }
    }
  } else {
    uint64_t ordinal = 0;
    uint64_t valid = 0;
    uint64_t commit = 0;
    for (uint64_t i = 0; i < count; ++i) {
      IndexCanonicalValue value;
      uint64_t delta_ordinal = 0;
      uint64_t delta_valid = 0;
      uint64_t delta_commit = 0;
      if (!GetValue(input, &offset, limit, &value) ||
          !GetVarint(input, &offset, &delta_ordinal, limit) ||
          !GetVarint(input, &offset, &delta_valid, limit) ||
          !GetVarint(input, &offset, &delta_commit, limit)) {
        return Status::Corruption("index sidecar", "sorted-delta posting");
      }
      ordinal ^= delta_ordinal;
      valid ^= delta_valid;
      commit ^= delta_commit;
      result.postings.push_back(
          IndexPosting{std::move(value), ordinal, valid, commit});
    }
  }
  const auto less = [](const IndexPosting& left, const IndexPosting& right) {
    const int compared = CompareIndexCanonicalValues(left.value, right.value);
    return compared != 0 ? compared < 0
                         : left.source_row_ordinal < right.source_row_ordinal;
  };
  if (offset != limit || result.postings.size() != count ||
      !std::is_sorted(result.postings.begin(), result.postings.end(), less)) {
    return Status::Corruption("index sidecar", "trailing or unsorted postings");
  }
  return result;
}
Status WriteIndexSidecarFile(const std::string& path, const IndexDefinition& definition,
                             uint64_t source_sst_id,
                             const std::vector<IndexPosting>& postings,
                             std::function<Status(
                                 IndexSidecarPublicationFaultPoint)>
                                 fault_injector) {
 const auto encoded=BuildIndexSidecar(definition,source_sst_id,postings);if(!encoded.ok())return encoded.status();
 if(encoded.ValueOrDie().size()>kMaximumIndexSidecarBytes)return Status::InvalidArgument("index sidecar","encoded sidecar exceeds size bound");
 const std::filesystem::path target(path);if(target.parent_path().empty())return Status::InvalidArgument("index sidecar","sidecar path requires a parent directory");
 std::error_code error;std::filesystem::create_directories(target.parent_path(),error);if(error)return Status::IOError(path,error.message());
 const std::string temporary=path+".tmp";const int fd=::open(temporary.c_str(),O_CREAT|O_TRUNC|O_WRONLY,0644);if(fd<0)return Status::IOError(temporary,std::strerror(errno));
 Status status=WriteAll(fd,encoded.ValueOrDie(),temporary);if(status.ok()&&::fsync(fd)!=0)status=Status::IOError(temporary,std::strerror(errno));if(::close(fd)!=0&&status.ok())status=Status::IOError(temporary,std::strerror(errno));if(!status.ok()){::unlink(temporary.c_str());return status;}
 if(fault_injector){const Status injected=fault_injector(IndexSidecarPublicationFaultPoint::kAfterFileFsync);if(!injected.ok())return injected;}
 if(::rename(temporary.c_str(),path.c_str())!=0){const Status rename_status=Status::IOError(path,std::strerror(errno));::unlink(temporary.c_str());return rename_status;}
 if(fault_injector){const Status injected=fault_injector(IndexSidecarPublicationFaultPoint::kAfterRename);if(!injected.ok())return Status::Indeterminate("index sidecar publication",injected.ToString());}
 const std::string directory=target.parent_path().string();const int directory_fd=::open(directory.c_str(),O_RDONLY);if(directory_fd<0)return Status::IOError(directory,std::strerror(errno));if(::fsync(directory_fd)!=0){const Status directory_status=Status::IOError(directory,std::strerror(errno));::close(directory_fd);return directory_status;}if(::close(directory_fd)!=0)return Status::IOError(directory,std::strerror(errno));
 if(fault_injector){const Status injected=fault_injector(IndexSidecarPublicationFaultPoint::kAfterDirectoryFsync);if(!injected.ok())return Status::Indeterminate("index sidecar publication",injected.ToString());}
 return Status::OK();
}
StatusOr<IndexSidecar> ReadIndexSidecarFile(const std::string& path,
                                            const IndexDefinition& definition,
                                            uint64_t source_sst_id) {
 const auto bytes=ReadAll(path);if(!bytes.ok())return bytes.status();return ReadIndexSidecar(bytes.ValueOrDie(),definition,source_sst_id);
}
StatusOr<IndexSidecar> ReadVerifiedIndexSidecarFile(
    const std::string& path, const IndexDefinition& definition,
    uint64_t source_sst_id,
    const std::array<uint8_t, 32>& expected_identity,
    uint64_t max_bytes) {
 const auto bytes=ReadAll(path,max_bytes);if(!bytes.ok())return bytes.status();if(Blake3Hash(bytes.ValueOrDie()).bytes!=expected_identity)return Status::Corruption("index sidecar","encoded identity checksum mismatch");return ReadIndexSidecar(bytes.ValueOrDie(),definition,source_sst_id);
}
StatusOr<IndexSidecar> BuildIndexCandidateSidecar(
    uint64_t source, const IndexDefinition& definition,
    const std::vector<TemporalEvent>& events,
    std::shared_ptr<WorkCancellation> cancellation) {
  if (source == 0 || definition.index_id == 0 ||
      !SupportedPhysicalEncoding(definition.canonical_encoding_id)) {
    return Status::InvalidArgument("index sidecar", "invalid definition");
  }
  std::vector<IndexPosting> postings;
  for (uint64_t ordinal = 0; ordinal < events.size(); ++ordinal) {
    if (cancellation != nullptr && ordinal % 64 == 0) {
      const Status checkpoint =
          cancellation->Checkpoint("index sidecar build");
      if (!checkpoint.ok()) return checkpoint;
    }
    const TemporalEvent& event = events[ordinal];
    const LogicalKey& key = event.logical_key();
    if (event.operation() != TemporalOperation::kPut ||
        key.kind() != LogicalKeyKind::kProperty ||
        key.entity_type() != definition.entity_type ||
        key.column_id() != definition.column_id ||
        event.schema_epoch() != definition.schema_epoch) {
      continue;
    }
    IndexCanonicalValue value;
    if (event.is_blob_reference()) {
      value = EncodeIndexBlobHash(*event.blob_ref());
    } else {
      const auto encoded = EncodeIndexCanonicalValue(event.value());
      if (!encoded.ok()) return encoded.status();
      value = encoded.ValueOrDie();
    }
    postings.push_back(IndexPosting{
        std::move(value), ordinal, event.valid_from(), event.commit_seq()});
  }
  std::sort(postings.begin(), postings.end(),
            [](const IndexPosting& a, const IndexPosting& b) {
              const int compared =
                  CompareIndexCanonicalValues(a.value, b.value);
              return compared == 0
                  ? a.source_row_ordinal < b.source_row_ordinal
                  : compared < 0;
            });
  if (cancellation != nullptr) {
    const Status checkpoint =
        cancellation->Checkpoint("index sidecar build");
    if (!checkpoint.ok()) return checkpoint;
  }
  return IndexSidecar{source, std::move(postings)};
}
StatusOr<std::vector<IndexPosting>> LookupIndexEquality(const IndexSidecar& sidecar,const Value& value){const auto canonical=EncodeIndexCanonicalValue(value);if(!canonical.ok())return canonical.status();std::optional<IndexCanonicalValue> blob_hash;if(value.type()==PhysicalType::kString||value.type()==PhysicalType::kBinary){const auto encoded=EncodeIndexBlobHash(value);if(!encoded.ok())return encoded.status();blob_hash=encoded.ValueOrDie();}std::vector<IndexPosting> results;for(const IndexPosting& posting:sidecar.postings){if(CompareIndexCanonicalValues(posting.value,canonical.ValueOrDie())==0||(blob_hash.has_value()&&CompareIndexCanonicalValues(posting.value,*blob_hash)==0))results.push_back(posting);}return results;}
StatusOr<std::vector<IndexPosting>> LookupIndexRange(
    const IndexSidecar& sidecar, const std::optional<Value>& lower,
    bool lower_inclusive, const std::optional<Value>& upper, bool upper_inclusive) {
  std::optional<IndexCanonicalValue> lower_value;
  std::optional<IndexCanonicalValue> upper_value;
  if (lower.has_value()) {
    const auto encoded = EncodeIndexCanonicalValue(*lower);
    if (!encoded.ok()) return encoded.status();
    lower_value = encoded.ValueOrDie();
  }
  if (upper.has_value()) {
    const auto encoded = EncodeIndexCanonicalValue(*upper);
    if (!encoded.ok()) return encoded.status();
    upper_value = encoded.ValueOrDie();
  }
  if (lower_value.has_value() && upper_value.has_value() &&
      (lower_value->type != upper_value->type ||
       CompareIndexCanonicalValues(*lower_value, *upper_value) > 0)) {
    return Status::InvalidArgument("index sidecar", "invalid index range bounds");
  }
  std::vector<IndexPosting> results;
  for (const IndexPosting& posting : sidecar.postings) {
    if (posting.value.kind != IndexCanonicalKind::kInline) continue;
    if (lower_value.has_value()) {
      const int comparison = CompareIndexCanonicalValues(posting.value, *lower_value);
      if (posting.value.type != lower_value->type || comparison < 0 ||
          (comparison == 0 && !lower_inclusive)) {
        continue;
      }
    }
    if (upper_value.has_value()) {
      const int comparison = CompareIndexCanonicalValues(posting.value, *upper_value);
      if (posting.value.type != upper_value->type || comparison > 0 ||
          (comparison == 0 && !upper_inclusive)) {
        if (posting.value.type == upper_value->type && comparison > 0) break;
        continue;
      }
    }
    results.push_back(posting);
  }
  return results;
}

StatusOr<std::vector<IndexPosting>> LookupIndexPrefix(const IndexSidecar& sidecar,
                                                       const Value& prefix) {
  if (prefix.type() != PhysicalType::kString && prefix.type() != PhysicalType::kBinary) {
    return Status::SchemaMismatch("index sidecar", "PREFIX requires string or binary values");
  }
  const auto encoded = EncodeIndexCanonicalValue(prefix);
  if (!encoded.ok()) return encoded.status();
  std::vector<IndexPosting> results;
  for (const IndexPosting& posting : sidecar.postings) {
    if (posting.value.kind != IndexCanonicalKind::kInline) continue;
    if (posting.value.type != encoded.ValueOrDie().type) continue;
    if (posting.value.bytes.size() >= encoded.ValueOrDie().bytes.size() &&
        posting.value.bytes.compare(0, encoded.ValueOrDie().bytes.size(),
                                    encoded.ValueOrDie().bytes) == 0) {
      results.push_back(posting);
    }
  }
  return results;
}
}  // namespace cedar
