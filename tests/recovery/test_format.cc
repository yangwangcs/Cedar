// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "storage/facts/fact_store.h"
#include "cedar/format.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>

namespace cedar {
namespace {

class FormatRecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_format_recovery_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
  }

  void TearDown() override { std::filesystem::remove_all(path_); }

  void ReplaceCurrentIdentity(const std::string& value) {
    rocksdb::Options options;
    options.create_if_missing = false;
    options.atomic_flush = true;
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName,
                             rocksdb::ColumnFamilyOptions(options));
    rocksdb::ColumnFamilyOptions facts_options(options);
    facts_options.prefix_extractor = std::shared_ptr<const rocksdb::SliceTransform>(
        rocksdb::NewFixedPrefixTransform(12));
    descriptors.emplace_back("facts", std::move(facts_options));
    descriptors.emplace_back("meta", rocksdb::ColumnFamilyOptions(options));
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    std::unique_ptr<rocksdb::DB> database;
    ASSERT_TRUE(rocksdb::DB::Open(options, path_, descriptors, &handles, &database).ok());
    ASSERT_TRUE(database->Put(rocksdb::WriteOptions(), handles[2],
                              EncodeCurrentFormatKey(), value)
                    .ok());
    for (rocksdb::ColumnFamilyHandle* handle : handles) {
      database->DestroyColumnFamilyHandle(handle);
    }
  }

  std::string path_;
};

TEST_F(FormatRecoveryTest, RejectsNonmatchingSystemIdentityWithoutChangingIt) {
  FactStore store(FactStoreOptions{path_});
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.Close().ok());
  const auto identity = EncodeSystemIdentity(SystemIdentity{
      "cedar.authoritative-columnar", 2, "part32.fact.v2",
      "cedar.parquet.facts.v3", "cedar.v2.internal-key.bytewise.v1"});
  ASSERT_FALSE(identity.ok());
  ReplaceCurrentIdentity("invalid identity record");

  EXPECT_TRUE(store.Open().IsCorruption());
  EXPECT_TRUE(store.Open().IsCorruption());
}

TEST_F(FormatRecoveryTest, RejectsCorruptFormatWithoutInitializingMetadata) {
  FactStore store(FactStoreOptions{path_});
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.Close().ok());
  ReplaceCurrentIdentity("corrupt format record");

  EXPECT_TRUE(store.Open().IsCorruption());
}

TEST_F(FormatRecoveryTest, PersistsExactAuthoritativeSystemIdentity) {
  FactStore store(FactStoreOptions{path_});
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.Close().ok());
  EXPECT_TRUE(store.Open().ok());
  EXPECT_TRUE(store.Close().ok());
}

TEST_F(FormatRecoveryTest, RejectsLegacyDirectoryWithoutMutation) {
  const std::filesystem::path marker = std::filesystem::path(path_) / "FORMAT";
  std::ofstream(marker) << "CEDAR-LEGACY";

  FactStore store(FactStoreOptions{path_});
  EXPECT_TRUE(store.Open().IsNotSupportedError());
  EXPECT_TRUE(std::filesystem::exists(marker));
  EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(path_) / "CURRENT"));
}

}  // namespace
}  // namespace cedar
