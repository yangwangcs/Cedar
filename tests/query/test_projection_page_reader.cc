#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "query/projection/projection_page_reader.h"
#include "query/projection/projection_page_cache.h"
#include "query/runtime/relational.h"

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

TEST(ProjectionPageReader, RepeatedHotPageAvoidsDecode) {
  auto chain = Directory();
  chain.intervals.push_back({{ValidTime{0}, ValidTime{10}}, Value::Int64(7), 2});
  const auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok());
  const auto path = (std::filesystem::temp_directory_path() /
                     "cedar-page-reader-cache.csegment").string();
  std::ofstream(path, std::ios::binary) << encoded.ValueOrDie();
  ProjectionPageReader reader;
  const auto directory = reader.ReadDirectory(path);
  ASSERT_TRUE(directory.ok());
  ProjectionPageSelection selection;
  selection.page_indexes = {0};
  ProjectionPageReadCache cache;
  cache.pages = std::make_shared<ProjectionPageCache>(16384);
  cache.namespace_key = "database/generation/segment";
  cache.admit = true;
  ProjectionReadStats stats;
  cache.stats = &stats;
  QueryReservation query(1 << 20);
  size_t visits = 0;
  const auto visit = [&](const ProjectionChain& page) {
    ++visits;
    EXPECT_EQ(page.intervals.size(), 1U);
    return Status::OK();
  };
  ASSERT_TRUE(reader.VisitSelected(path, directory.ValueOrDie(), selection,
                                  query, visit, &cache).ok());
  EXPECT_GT(cache.pages->resident_bytes(), 0U);
  EXPECT_GT(stats.physical_bytes, 0U);
  stats = {};
  // Warm payload reads use the immutable page owner, without opening the file.
  std::filesystem::remove(path);
  EXPECT_TRUE(reader.VisitSelected(path, directory.ValueOrDie(), selection,
                                  query, visit, &cache).ok());
  EXPECT_EQ(visits, 2U);
  EXPECT_EQ(stats.physical_bytes, 0U);
  EXPECT_EQ(stats.decoded_bytes, 0U);
  EXPECT_EQ(query.used_bytes(), 0U);
  cache.namespace_key = "database/new-generation/segment";
  EXPECT_FALSE(reader.VisitSelected(path, directory.ValueOrDie(), selection,
                                   query, visit, &cache).ok());
}

TEST(ProjectionPageReader, SelectedPagesShareOneAllocationLimit) {
  ProjectionChain chain;
  chain.header.kind = ProjectionKind::kProperty;
  chain.header.property_id = PropertyId{7};
  chain.header.schema_epoch = 1;
  chain.header.entity_min = 0;
  chain.header.entity_max_exclusive = 16;
  chain.header.valid_from_min = ValidTime{0};
  chain.header.valid_to_max = ValidTime{100};
  ProjectionPageSelection selection;
  for (uint64_t index = 0; index < 16; ++index) {
    ProjectionPageDirectoryEntry page;
    page.entity_min = index;
    page.entity_max_exclusive = index + 1;
    page.valid_from_min = ValidTime{0};
    page.valid_to_max = ValidTime{100};
    chain.page_directory.push_back(page);
    chain.intervals.push_back({{ValidTime{0}, ValidTime{100}},
        Value::String(std::string(16384, static_cast<char>('a' + index))), index});
    selection.page_indexes.push_back(index);
  }
  const auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  const auto path = (std::filesystem::temp_directory_path() /
                    "cedar-page-reader-total-budget.csegment").string();
  std::ofstream(path, std::ios::binary) << encoded.ValueOrDie();
  ProjectionPageReader reader;
  const auto directory = reader.ReadDirectory(path);
  ASSERT_TRUE(directory.ok());
  const auto pages = reader.ReadSelected(
      path, directory.ValueOrDie(), selection, 65536);
  QueryReservation streaming(128ULL << 10);
  size_t visited = 0;
  const auto streamed = reader.VisitSelected(
      path, directory.ValueOrDie(), selection, streaming,
      [&](const ProjectionChain& page) {
        EXPECT_EQ(page.intervals.size(), 1U);
        ++visited;
        return Status::OK();
      });
  EXPECT_TRUE(streamed.ok()) << streamed.ToString();
  EXPECT_EQ(visited, 16U);
  EXPECT_EQ(streaming.used_bytes(), 0U);
  EXPECT_LT(streaming.peak_bytes(), 128ULL << 10);
  const auto cancelled = reader.VisitSelected(
      path, directory.ValueOrDie(), selection, streaming,
      [](const ProjectionChain&) { return Status::Conflict("test", "stop visitor"); });
  EXPECT_TRUE(cancelled.IsConflict());
  EXPECT_EQ(streaming.used_bytes(), 0U);
  QueryReservation tiny(1024);
  const auto exhausted = reader.VisitSelected(
      path, directory.ValueOrDie(), selection, tiny,
      [](const ProjectionChain&) { return Status::OK(); });
  EXPECT_TRUE(exhausted.IsResourceExhausted());
  EXPECT_EQ(tiny.used_bytes(), 0U);
  std::filesystem::remove(path);
  ASSERT_FALSE(pages.ok());
  EXPECT_TRUE(pages.status().IsResourceExhausted()) << pages.status().ToString();
}

TEST(ProjectionPageReader, LookupOnlyAndCorruptPagesNeverAdmit) {
  auto chain = Directory();
  chain.intervals.push_back({{ValidTime{0}, ValidTime{10}}, Value::Int64(7), 2});
  const auto encoded = EncodeProjectionPage(chain, CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok());
  const auto path = std::filesystem::temp_directory_path() /
      "cedar-page-reader-no-admit-corrupt.csegment";
  struct Cleanup {
    std::filesystem::path path;
    ~Cleanup() { std::filesystem::remove(path); }
  } cleanup{path};
  std::ofstream(path, std::ios::binary) << encoded.ValueOrDie();
  ProjectionPageReader reader;
  const auto directory = reader.ReadDirectory(path.string());
  ASSERT_TRUE(directory.ok());
  ProjectionPageSelection selection;
  selection.page_indexes = {0};
  ProjectionPageReadCache cache;
  cache.pages = std::make_shared<ProjectionPageCache>(16384);
  cache.namespace_key = "database/no-admit-test";
  ProjectionReadStats stats;
  cache.stats = &stats;
  QueryReservation query(1 << 20);
  size_t visits = 0;
  const auto visit = [&](const ProjectionChain& page) {
    ++visits;
    EXPECT_EQ(page.intervals[0].value, Value::Int64(7));
    return Status::OK();
  };
  for (int pass = 0; pass < 2; ++pass) {
    stats = {};
    ASSERT_TRUE(reader.VisitSelected(path.string(), directory.ValueOrDie(), selection,
                                    query, visit, &cache).ok());
    EXPECT_GT(stats.physical_bytes, 0U);
    EXPECT_GT(stats.decoded_bytes, 0U);
    EXPECT_EQ(cache.pages->resident_bytes(), 0U);
    EXPECT_EQ(query.used_bytes(), 0U);
  }
  auto corrupt = encoded.ValueOrDie();
  ASSERT_FALSE(directory.ValueOrDie().page_directory.empty());
  const auto offset = directory.ValueOrDie().page_directory[0].offset;
  ASSERT_LT(offset, corrupt.size());
  corrupt[offset] ^= 1;
  std::ofstream(path, std::ios::binary | std::ios::trunc) << corrupt;
  cache.admit = true;
  const auto failed = reader.VisitSelected(path.string(), directory.ValueOrDie(), selection,
                                          query, visit, &cache);
  EXPECT_TRUE(failed.IsCorruption()) << failed.ToString();
  EXPECT_EQ(visits, 2U);
  EXPECT_EQ(cache.pages->resident_bytes(), 0U);
  EXPECT_EQ(query.used_bytes(), 0U);
  std::ofstream(path, std::ios::binary | std::ios::trunc) << encoded.ValueOrDie();
  ASSERT_TRUE(reader.VisitSelected(path.string(), directory.ValueOrDie(), selection,
                                  query, visit, &cache).ok());
  EXPECT_EQ(visits, 3U);
  EXPECT_GT(cache.pages->resident_bytes(), 0U);
  EXPECT_EQ(query.used_bytes(), 0U);
}

}  // namespace
}  // namespace cedar::internal
