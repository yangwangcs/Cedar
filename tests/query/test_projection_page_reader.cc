#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "query/projection/projection_page_reader.h"

namespace cedar::internal {
namespace {

ProjectionChain Directory() {
  ProjectionChain chain;
  chain.header.kind = ProjectionKind::kState;
  chain.header.entity_min = 0;
  chain.header.entity_max_exclusive = 100;
  chain.header.valid_from_min = ValidTime{0};
  chain.header.valid_to_max = ValidTime{100};
  ProjectionPageDirectoryEntry first;
  first.entity_min = 0; first.entity_max_exclusive = 10;
  first.valid_from_min = ValidTime{0}; first.valid_to_max = ValidTime{20};
  ProjectionPageDirectoryEntry second;
  second.entity_min = 50; second.entity_max_exclusive = 60;
  second.valid_from_min = ValidTime{80}; second.valid_to_max = ValidTime{100};
  chain.page_directory = {first, second};
  return chain;
}

TEST(ProjectionPageReader, SelectsOnlyIntersectingPages) {
  ProjectionPageReader reader;
  CoverageRequest request;
  request.entity_min = 1;
  request.entity_max_exclusive = 9;
  request.valid_time = {ValidTime{5}, ValidTime{10}};
  auto selected = reader.Select(Directory(), request);
  ASSERT_TRUE(selected.ok()) << selected.status().ToString();
  ASSERT_EQ(selected.ValueOrDie().page_indexes, std::vector<size_t>({0}));
  EXPECT_EQ(selected.ValueOrDie().pages_skipped, 1U);
}

TEST(ProjectionPageReader, EmptySelectionDoesNotDecodeAnyPage) {
  ProjectionPageReader reader;
  CoverageRequest request;
  request.entity_min = 20;
  request.entity_max_exclusive = 30;
  request.valid_time = {ValidTime{5}, ValidTime{10}};
  auto selected = reader.Select(Directory(), request);
  ASSERT_TRUE(selected.ok());
  EXPECT_TRUE(selected.ValueOrDie().page_indexes.empty());
  EXPECT_EQ(selected.ValueOrDie().pages_skipped, 2U);
}

TEST(ProjectionPageReader, ReadsOnlySelectedPayloadRanges) {
  ProjectionChain chain = Directory();
  chain.intervals.push_back({ValidTimeInterval{ValidTime{0}, ValidTime{10}},
                             Value::Int64(7), 2});
  chain.intervals.push_back({ValidTimeInterval{ValidTime{80}, ValidTime{100}},
                             Value::Int64(8), 55});
  auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  const auto path = (std::filesystem::temp_directory_path() /
                     "cedar-page-reader-selected.csegment").string();
  std::ofstream(path, std::ios::binary) << encoded.ValueOrDie();

  ProjectionPageReader reader;
  auto directory = reader.ReadDirectory(path);
  ASSERT_TRUE(directory.ok()) << directory.status().ToString();
  ASSERT_EQ(directory.ValueOrDie().page_directory.size(), 2U);
  ProjectionPageSelection selection;
  selection.page_indexes = {0};
  auto pages = reader.ReadSelected(path, directory.ValueOrDie(), selection);
  std::filesystem::remove(path);
  ASSERT_TRUE(pages.ok()) << pages.status().ToString();
  ASSERT_EQ(pages.ValueOrDie().size(), 1U);
  ASSERT_EQ(pages.ValueOrDie()[0].intervals.size(), 1U);
  EXPECT_EQ(pages.ValueOrDie()[0].intervals[0].entity_id, 2U);
}

}  // namespace
}  // namespace cedar::internal
