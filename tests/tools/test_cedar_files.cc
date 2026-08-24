// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <sys/wait.h>
#include <vector>

#include <rocksdb/db.h>

#include "storage/facts/fact_store.h"
#include "storage/rocks/rocksdb_config.h"

namespace cedar {
namespace {

PendingFactMutation VertexPut(uint64_t vertex_id, uint64_t valid_from) {
  return {EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}).ref(),
          ValidTime{valid_from}, FactOperation::kPut, 0, std::nullopt};
}

StoreCommitBatch Batch(uint64_t transaction_id, uint64_t vertex_id) {
  return {TxnId{transaction_id}, 100,
          {VertexPut(vertex_id, transaction_id)}, {}};
}

void FlushFacts(const std::string& path) {
  FactStoreOptions store_options;
  store_options.path = path;
  rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  options.create_if_missing = false;
  auto descriptors = internal::MakeRocksDbColumnFamilyDescriptors(store_options,
                                                                    options);
  std::unique_ptr<rocksdb::DB> db;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(options, path, descriptors, &handles, &db).ok());
  ASSERT_EQ(handles.size(), 3U);
  ASSERT_TRUE(db->Put(rocksdb::WriteOptions(), handles[0], "cedar-test-default",
                      "inspection")
                  .ok());
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(db->Flush(rocksdb::FlushOptions(), handle).ok());
  }
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(db->DestroyColumnFamilyHandle(handle).ok());
  }
}

std::map<std::string, uintmax_t> DirectorySnapshot(const std::string& path) {
  std::map<std::string, uintmax_t> result;
  for (const auto& entry : std::filesystem::directory_iterator(path)) {
    if (entry.is_regular_file()) {
      result.emplace(entry.path().filename().string(), entry.file_size());
    }
  }
  return result;
}

struct CommandResult {
  int exit_code;
  std::string output;
};

CommandResult RunCommand(const std::string& command) {
  const std::string output_path =
      (std::filesystem::temp_directory_path() / "cedar_files_output.txt").string();
  const std::string command_with_redirect = command + " >'" + output_path + "' 2>&1";
  const int raw_status = std::system(command_with_redirect.c_str());
  std::ifstream input(output_path);
  std::string output((std::istreambuf_iterator<char>(input)),
                     std::istreambuf_iterator<char>());
  std::filesystem::remove(output_path);
  return {WEXITSTATUS(raw_status), std::move(output)};
}

class CedarFilesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_files_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    root_path_ = pattern;
    database_path_ = (std::filesystem::path(root_path_) / "database").string();
    FactStore store(FactStoreOptions{database_path_});
    ASSERT_TRUE(store.Open().ok());
    ASSERT_TRUE(store.Commit(Batch(1, 101)).ok());
    ASSERT_TRUE(store.Close().ok());
    FlushFacts(database_path_);
  }

  void TearDown() override { std::filesystem::remove_all(root_path_); }

  std::string root_path_;
  std::string database_path_;
};

TEST_F(CedarFilesTest, PrintsTextAndJsonWithoutMutatingTheDatabase) {
  const auto before = DirectorySnapshot(database_path_);
  const std::string base = std::string("'") + CEDAR_BINARY + "' files --path '" +
                           database_path_ + "'";
  const CommandResult text = RunCommand(base);
  EXPECT_EQ(text.exit_code, 0) << text.output;
  EXPECT_NE(text.output.find("FILE"), std::string::npos);
  EXPECT_NE(text.output.find("facts"), std::string::npos);
  EXPECT_NE(text.output.find("authoritative-facts"), std::string::npos);
  EXPECT_NE(text.output.find("CedarParquet"), std::string::npos);

  const CommandResult json = RunCommand(base + " --json");
  EXPECT_EQ(json.exit_code, 0) << json.output;
  EXPECT_TRUE(json.output.starts_with("{\"files\":["));
  EXPECT_NE(json.output.find("\"relative_filename\""), std::string::npos);
  EXPECT_NE(json.output.find("\"column_family_name\""), std::string::npos);
  EXPECT_NE(json.output.find("\"role\""), std::string::npos);
  EXPECT_NE(json.output.find("\"table_format\""), std::string::npos);
  EXPECT_NE(json.output.find("\"level\""), std::string::npos);
  EXPECT_NE(json.output.find("\"size_bytes\""), std::string::npos);
  EXPECT_NE(json.output.find("\"smallest_seqno\""), std::string::npos);
  EXPECT_NE(json.output.find("\"largest_seqno\""), std::string::npos);
  EXPECT_NE(json.output.find("\"smallest_key_hex\""), std::string::npos);
  EXPECT_NE(json.output.find("\"largest_key_hex\""), std::string::npos);
  EXPECT_NE(text.output.find("transaction-metadata"), std::string::npos);
  EXPECT_NE(text.output.find("engine-internal"), std::string::npos);
  EXPECT_EQ(DirectorySnapshot(database_path_), before);
}

TEST_F(CedarFilesTest, RejectsMissingPath) {
  const CommandResult result = RunCommand(std::string("'") + CEDAR_BINARY + "' files");
  EXPECT_EQ(result.exit_code, 2);
  EXPECT_NE(result.output.find("usage:"), std::string::npos);
}

TEST_F(CedarFilesTest, RequiresDocumentedArgumentOrder) {
  const CommandResult result = RunCommand(std::string("'") + CEDAR_BINARY +
                                           "' files --json --path '" +
                                           database_path_ + "'");
  EXPECT_EQ(result.exit_code, 2);
  EXPECT_NE(result.output.find("usage:"), std::string::npos);
}

}  // namespace
}  // namespace cedar
