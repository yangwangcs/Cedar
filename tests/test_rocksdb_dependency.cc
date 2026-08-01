#include <gtest/gtest.h>
#include <rocksdb/version.h>

TEST(RocksDbDependencyTest, UsesPinnedVersion) {
  EXPECT_EQ(ROCKSDB_MAJOR, 11);
  EXPECT_EQ(ROCKSDB_MINOR, 1);
  EXPECT_EQ(rocksdb::GetRocksVersionAsString(), "11.1.2");
}
