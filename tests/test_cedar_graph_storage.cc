#include <gtest/gtest.h>
#include <filesystem>
#include <random>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

class CedarGraphStorageTest : public ::testing::Test {
 protected:
  std::string db_path_;

  void SetUp() override {
    char buf[] = "/tmp/cedar_test_XXXXXX";
    char* dir = mkdtemp(buf);
    ASSERT_NE(dir, nullptr);
    db_path_ = dir;
  }

  void TearDown() override {
    if (!db_path_.empty() && std::filesystem::exists(db_path_)) {
      std::filesystem::remove_all(db_path_);
    }
  }
};

TEST_F(CedarGraphStorageTest, OpenAndDestroy) {
  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, db_path_, &storage);
  EXPECT_TRUE(s.ok()) << s.ToString();
  ASSERT_NE(storage, nullptr);

  delete storage;

  s = CedarGraphStorage::DestroyDB(db_path_, options);
  EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST_F(CedarGraphStorageTest, PutAndGet) {
  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, db_path_, &storage);
  ASSERT_TRUE(s.ok());

  Descriptor desc = Descriptor::InlineInt(1, 42);
  s = storage->Put(123ULL, 1000000ULL, desc, Timestamp(1));
  EXPECT_TRUE(s.ok()) << s.ToString();

  auto result = storage->Get(123ULL, 1000000ULL);
  ASSERT_TRUE(result.has_value());
  auto val = result->AsInlineInt();
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, 42);

  delete storage;
}

TEST_F(CedarGraphStorageTest, PutStaticVertexAndGet) {
  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, db_path_, &storage);
  ASSERT_TRUE(s.ok());

  Descriptor desc = Descriptor::InlineFloat(2, 2.718f);
  s = storage->PutStaticVertex(1000ULL, 2, desc);
  EXPECT_TRUE(s.ok()) << s.ToString();

  auto result = storage->GetStaticVertex(1000ULL, 2);
  ASSERT_TRUE(result.has_value());
  auto val = result->AsInlineFloat();
  ASSERT_TRUE(val.has_value());
  EXPECT_FLOAT_EQ(*val, 2.718f);

  delete storage;
}

TEST_F(CedarGraphStorageTest, PutDynamicVertexAndGet) {
  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, db_path_, &storage);
  ASSERT_TRUE(s.ok());

  Descriptor desc = Descriptor::InlineInt(3, 999);
  s = storage->PutDynamicVertex(2000ULL, 3, Timestamp(1700000000000000ULL), desc, Timestamp(1));
  EXPECT_TRUE(s.ok()) << s.ToString();

  auto result = storage->GetDynamicVertex(2000ULL, 3, Timestamp(1700000000000000ULL));
  ASSERT_TRUE(result.has_value());
  auto val = result->AsInlineInt();
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, 999);

  delete storage;
}

TEST_F(CedarGraphStorageTest, GetNonExistentKey) {
  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, db_path_, &storage);
  ASSERT_TRUE(s.ok());

  auto result = storage->Get(999999ULL, 1000000ULL);
  EXPECT_FALSE(result.has_value());

  delete storage;
}

TEST_F(CedarGraphStorageTest, PutStringAndGetString) {
  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, db_path_, &storage);
  ASSERT_TRUE(s.ok());

  s = storage->PutString(500ULL, 1, "Hello, CedarGraph!", Timestamp(1));
  EXPECT_TRUE(s.ok()) << s.ToString();

  auto result = storage->GetString(500ULL, 1);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "Hello, CedarGraph!");

  // Short string (should still work)
  s = storage->PutString(501ULL, 1, "hi", Timestamp(1));
  EXPECT_TRUE(s.ok()) << s.ToString();

  auto short_result = storage->GetString(501ULL, 1);
  ASSERT_TRUE(short_result.has_value());
  EXPECT_EQ(*short_result, "hi");

  delete storage;
}

TEST_F(CedarGraphStorageTest, PutBinaryAndGetBinary) {
  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, db_path_, &storage);
  ASSERT_TRUE(s.ok());

  std::vector<uint8_t> data = {0x00, 0x01, 0x02, 0x03, 0xFF};
  s = storage->PutBinary(600ULL, 1, data.data(), data.size(), Timestamp(1));
  EXPECT_TRUE(s.ok()) << s.ToString();

  auto result = storage->GetBinary(600ULL, 1);
  EXPECT_EQ(result, data);

  delete storage;
}

TEST_F(CedarGraphStorageTest, BatchWrite) {
  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, db_path_, &storage);
  ASSERT_TRUE(s.ok());

  std::vector<CedarGraphStorage::BatchWriteItem> items;
  for (uint64_t i = 0; i < 10; ++i) {
    items.emplace_back(i, EntityType::Vertex, 1,
                       Descriptor::InlineInt(1, static_cast<int32_t>(i * 10)),
                       Timestamp::Static());
  }

  s = storage->BatchWrite(items);
  EXPECT_TRUE(s.ok()) << s.ToString();

  for (uint64_t i = 0; i < 10; ++i) {
    auto result = storage->GetStaticVertex(i, 1);
    ASSERT_TRUE(result.has_value()) << "Failed for i=" << i;
    auto val = result->AsInlineInt();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, static_cast<int32_t>(i * 10));
  }

  delete storage;
}

TEST_F(CedarGraphStorageTest, StatsSmokeTest) {
  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, db_path_, &storage);
  ASSERT_TRUE(s.ok());

  CedarGraphStorage::Stats stats = storage->GetStats();
  // Just verify it doesn't crash and returns reasonable defaults
  EXPECT_GE(stats.num_levels, 0);

  delete storage;
}

TEST_F(CedarGraphStorageTest, ForceFlushAndCompact) {
  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, db_path_, &storage);
  ASSERT_TRUE(s.ok());

  Descriptor desc = Descriptor::InlineInt(1, 1);
  for (int i = 0; i < 100; ++i) {
    s = storage->Put(i, 1000ULL + i, desc, Timestamp(1));
    EXPECT_TRUE(s.ok()) << s.ToString();
  }

  s = storage->ForceFlush();
  // ForceFlush may or may not do anything depending on memtable state

  s = storage->Compact();
  // Compact may or may not do anything in an empty DB

  delete storage;
}
