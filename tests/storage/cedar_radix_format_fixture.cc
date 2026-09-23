// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "cedar/fact/fact_codec.h"
#include "storage/facts/fact_store.h"
#include "storage/rocks/rocksdb_config.h"

#include <rocksdb/db.h>
#include <rocksdb/metadata.h>
#include <rocksdb/options.h>

namespace {

using cedar::CommitSeq;
using cedar::EntityFact;
using cedar::FactOperation;
using cedar::FactRef;
using cedar::FactStore;
using cedar::FactStoreOptions;
using cedar::PartId;
using cedar::PendingFactMutation;
using cedar::Status;
using cedar::StoreCommitBatch;
using cedar::TxnId;
using cedar::ValidTime;
using cedar::VertexId;
using cedar::VertexRef;

constexpr uint64_t kHashOffset = 1469598103934665603ULL;
constexpr uint64_t kHashPrime = 1099511628211ULL;

FactRef Vertex(uint64_t id) {
  return EntityFact::Vertex(VertexRef{PartId{0}, VertexId{id}}).ref();
}

StoreCommitBatch Batch(uint64_t txn, FactRef ref, uint64_t valid,
                       FactOperation operation) {
  return {TxnId{txn}, 100 + txn,
          {PendingFactMutation{ref, ValidTime{valid}, operation, 0,
                               std::nullopt}}, {}};
}

Status Populate(FactStore* store) {
  for (const auto& batch : {
           Batch(1, Vertex(101), 10, FactOperation::kPut),
           Batch(2, Vertex(202), 20, FactOperation::kPut),
           Batch(3, Vertex(101), 10, FactOperation::kDelete)}) {
    const auto result = store->Commit(batch);
    if (!result.ok()) return result.status();
  }
  return Status::OK();
}

rocksdb::Status OpenRaw(const std::string& path,
                        std::unique_ptr<rocksdb::DB>* database,
                        std::vector<rocksdb::ColumnFamilyHandle*>* handles) {
  FactStoreOptions options{path};
  rocksdb::Options rocks_options =
      cedar::internal::MakeRocksDbOptions(options, false);
  rocks_options.create_if_missing = false;
  rocks_options.create_missing_column_families = false;
  return rocksdb::DB::Open(
      rocks_options, path,
      cedar::internal::MakeRocksDbColumnFamilyDescriptors(options,
                                                           rocks_options),
      handles, database);
}

void CloseRaw(std::unique_ptr<rocksdb::DB>* database,
              std::vector<rocksdb::ColumnFamilyHandle*>* handles) {
  for (auto* handle : *handles) (*database)->DestroyColumnFamilyHandle(handle);
  handles->clear();
  database->reset();
}

uint64_t HashBytes(uint64_t hash, const rocksdb::Slice& value) {
  for (size_t index = 0; index < value.size(); ++index) {
    const auto byte = static_cast<unsigned char>(value[index]);
    hash ^= byte;
    hash *= kHashPrime;
  }
  hash ^= 0xffU;
  return hash * kHashPrime;
}

struct ExpectedFact {
  FactRef ref;
  ValidTime valid_from;
  CommitSeq commit_seq;
  FactOperation operation;
};

bool MatchesExpectedFact(const rocksdb::Slice& key,
                         const rocksdb::Slice& value,
                         const ExpectedFact& expected) {
  const auto decoded_key = cedar::DecodeFactKey(key.ToString());
  if (!decoded_key.ok()) return false;
  const auto& fact_key = decoded_key.ValueOrDie();
  if (fact_key.ref != expected.ref ||
      fact_key.valid_from != expected.valid_from ||
      fact_key.commit_seq != expected.commit_seq) {
    return false;
  }
  const auto decoded_value = cedar::DecodeFactValue(
      fact_key.ref, fact_key.valid_from, fact_key.commit_seq, value.ToString());
  if (!decoded_value.ok()) return false;
  const auto& event = decoded_value.ValueOrDie();
  return event.ref == expected.ref && event.valid_from == expected.valid_from &&
         event.commit_seq == expected.commit_seq &&
         event.operation == expected.operation;
}

bool HasCedarFactsSst(rocksdb::DB* database, const std::string& path) {
  std::vector<rocksdb::LiveFileMetaData> files;
  database->GetLiveFilesMetaData(&files);
  for (const auto& file : files) {
    if (file.column_family_name != "facts") continue;
    std::ifstream input(std::filesystem::path(path) / file.relative_filename,
                        std::ios::binary);
    std::string magic(4, '\0');
    input.read(magic.data(), static_cast<std::streamsize>(magic.size()));
    if (input.gcount() == static_cast<std::streamsize>(magic.size()) &&
        magic == "PAR1") {
      return true;
    }
  }
  return false;
}

int Verify(const std::string& path) {
  FactStore store(FactStoreOptions{path});
  Status status = store.Open();
  if (!status.ok()) return 20;
  {
    const auto snapshot = store.BeginSnapshot();
    if (!snapshot.ok() || snapshot.ValueOrDie().commit_seq() != CommitSeq{3}) {
      return 21;
    }
    const auto deleted = store.Read(snapshot.ValueOrDie(), Vertex(101),
                                    ValidTime{10});
    const auto surviving = store.Read(snapshot.ValueOrDie(), Vertex(202),
                                      ValidTime{20});
    if (!deleted.ok() || deleted.ValueOrDie().has_value() || !surviving.ok() ||
        !surviving.ValueOrDie().has_value() ||
        surviving.ValueOrDie()->ref != Vertex(202)) {
      return 22;
    }
  }
  if (!store.Close().ok()) return 23;

  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  if (!OpenRaw(path, &database, &handles).ok() || handles.size() != 3) return 24;
  const std::vector<ExpectedFact> expected = {
      {Vertex(101), ValidTime{10}, CommitSeq{3}, FactOperation::kDelete},
      {Vertex(101), ValidTime{10}, CommitSeq{1}, FactOperation::kPut},
      {Vertex(202), ValidTime{20}, CommitSeq{2}, FactOperation::kPut},
  };
  uint64_t forward_order_hash = kHashOffset;
  uint64_t forward_value_hash = kHashOffset;
  uint64_t reverse_order_hash = kHashOffset;
  uint64_t reverse_value_hash = kHashOffset;
  size_t count = 0;
  std::unique_ptr<rocksdb::Iterator> forward(
      database->NewIterator(rocksdb::ReadOptions(), handles[1]));
  for (forward->SeekToFirst(); forward->Valid(); forward->Next()) {
    if (count >= expected.size() ||
        !MatchesExpectedFact(forward->key(), forward->value(), expected[count])) {
      return 25;
    }
    forward_order_hash = HashBytes(forward_order_hash, forward->key());
    forward_value_hash = HashBytes(forward_value_hash, forward->value());
    ++count;
  }
  if (!forward->status().ok() || count != expected.size()) return 25;
  std::unique_ptr<rocksdb::Iterator> reverse(
      database->NewIterator(rocksdb::ReadOptions(), handles[1]));
  size_t reverse_count = 0;
  for (reverse->SeekToLast(); reverse->Valid(); reverse->Prev()) {
    if (reverse_count >= expected.size() ||
        !MatchesExpectedFact(reverse->key(), reverse->value(),
                             expected[expected.size() - 1 - reverse_count])) {
      return 26;
    }
    reverse_order_hash = HashBytes(reverse_order_hash, reverse->key());
    reverse_value_hash = HashBytes(reverse_value_hash, reverse->value());
    ++reverse_count;
  }
  if (!reverse->status().ok() || reverse_count != expected.size()) return 26;
  forward.reset();
  reverse.reset();
  CloseRaw(&database, &handles);
  std::cout << "{\"count\":" << count << ",\"commit_seq\":3,"
            << "\"forward_order_hash\":" << forward_order_hash
            << ",\"forward_value_hash\":" << forward_value_hash
            << ",\"reverse_order_hash\":" << reverse_order_hash
            << ",\"reverse_value_hash\":" << reverse_value_hash << "}\n";
  return 0;
}

int WriteSst(const std::string& path) {
  FactStore store(FactStoreOptions{path});
  if (!store.Open().ok() || !Populate(&store).ok() || !store.Close().ok()) {
    return 30;
  }
  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  if (!OpenRaw(path, &database, &handles).ok() || handles.size() != 3) return 31;
  rocksdb::FlushOptions options;
  options.wait = true;
  if (!database->Flush(options, handles[1]).ok()) return 32;
  if (!HasCedarFactsSst(database.get(), path)) return 33;
  CloseRaw(&database, &handles);
  return 0;
}

int WriteWal(const std::string& path) {
  const pid_t child = fork();
  if (child < 0) return 40;
  if (child == 0) {
    FactStore store(FactStoreOptions{path});
    if (!store.Open().ok() || !Populate(&store).ok()) _exit(41);
    _exit(0);
  }
  int status = 0;
  if (waitpid(child, &status, 0) != child || !WIFEXITED(status) ||
      WEXITSTATUS(status) != 0) {
    return 42;
  }
  bool saw_wal = false;
  bool saw_sst = false;
  for (const auto& entry : std::filesystem::directory_iterator(path)) {
    saw_wal = saw_wal || entry.path().extension() == ".log";
    saw_sst = saw_sst || entry.path().extension() == ".sst";
  }
  return saw_wal && !saw_sst ? 0 : 43;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3) return 2;
  const std::string mode = argv[1];
  const std::filesystem::path path = argv[2];
  if (!path.is_absolute()) return 2;
  if (mode == "verify") {
    return std::filesystem::is_directory(path) ? Verify(path.string()) : 2;
  }
  if (mode != "write-sst" && mode != "write-wal") return 2;
  if (std::filesystem::exists(path)) return 2;
  if (!std::filesystem::create_directory(path)) return 2;
  return mode == "write-sst" ? WriteSst(path.string()) : WriteWal(path.string());
}
