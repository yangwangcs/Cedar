#include <gtest/gtest.h>
#include <string>
#include "cedar/core/crc32c.h"
#if defined(__unix__)
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace {
uint32_t BitwiseCrc(uint32_t seed, const char* data, size_t n) {
  uint32_t value = ~seed;
  for (size_t i = 0; i < n; ++i) {
    value ^= static_cast<unsigned char>(data[i]);
    for (int b = 0; b < 8; ++b)
      value = (value >> 1) ^ (0x82f63b78U & (0U - (value & 1U)));
  }
  return ~value;
}

TEST(Crc32cCompatibilityTest, KnownCastagnoliVectorAndEmptyExtension) {
  EXPECT_EQ(cedar::crc32c::Value("123456789", 9), 0xe3069283U);
  EXPECT_EQ(cedar::crc32c::Extend(0x12345678U, nullptr, 0), 0x12345678U);
}

TEST(Crc32cCompatibilityTest, UnalignedBinaryInputAndEveryShortTailMatchBitwiseOracle) {
  std::string data(8192 + 16, '\0');
  uint32_t random = 1234567;
  for (char& c : data) {
    random = random * 1664525U + 1013904223U;
    c = static_cast<char>(random >> 24);
  }
  for (size_t offset = 0; offset < 16; ++offset) {
    for (size_t n = 0; n < 160; ++n) {
      for (uint32_t seed : {0U, 0xffffffffU, 0x12345678U}) {
        EXPECT_EQ(cedar::crc32c::Extend(seed, data.data() + offset, n),
                  BitwiseCrc(seed, data.data() + offset, n));
      }
    }
    EXPECT_EQ(cedar::crc32c::Value(data.data() + offset, 8192),
              BitwiseCrc(0, data.data() + offset, 8192));
  }
}

TEST(Crc32cCompatibilityTest, IncrementalExtensionMatchesSingleRecord) {
  const std::string data("\0\xff\x80hello-world\0\x01", 17);
  const uint32_t want = BitwiseCrc(0xabcdefU, data.data(), data.size());
  for (size_t split = 0; split <= data.size(); ++split) {
    const auto first = cedar::crc32c::Extend(0xabcdefU, data.data(), split);
    EXPECT_EQ(cedar::crc32c::Extend(first, data.data() + split, data.size() - split), want);
  }
}

#if defined(__unix__)
TEST(Crc32cCompatibilityTest, DoesNotReadPastAnInaccessiblePageBoundary) {
  const long page_size = sysconf(_SC_PAGESIZE);
  ASSERT_GT(page_size, 0);
  void* mapping = mmap(nullptr, page_size * 2, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ASSERT_NE(mapping, MAP_FAILED);
  char* boundary = static_cast<char*>(mapping) + page_size;
  const int protected_page = mprotect(boundary, page_size, PROT_NONE);
  if (protected_page == 0) {
    for (size_t n = 1; n <= 40; ++n) {
      char* input = boundary - n;
      for (size_t i = 0; i < n; ++i) input[i] = static_cast<char>(i * 17);
      EXPECT_EQ(cedar::crc32c::Extend(0x12345678, input, n),
                BitwiseCrc(0x12345678, input, n));
    }
  }
  EXPECT_EQ(munmap(mapping, page_size * 2), 0);
  EXPECT_EQ(protected_page, 0);
}
#endif
}  // namespace
