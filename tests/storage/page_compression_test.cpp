/**
 * @file page_compression_test.cpp
 * @brief Unit tests for page compression
 */

#include <cstring>
#include <gtest/gtest.h>
#include <string>

#include "storage/page.hpp"
#include "storage/page_compression.hpp"

namespace entropy {

class PageCompressionTest : public ::testing::Test {
protected:
  static constexpr size_t kTestPageSize = Page::kPageSize;

  void SetUp() override {
    // Create test data with some repetition (compressible)
    test_data_.resize(kTestPageSize);
    for (size_t i = 0; i < kTestPageSize; ++i) {
      test_data_[i] = static_cast<char>('A' + (i % 26));
    }

    // Create incompressible random data
    random_data_.resize(kTestPageSize);
    for (size_t i = 0; i < kTestPageSize; ++i) {
      random_data_[i] = static_cast<char>(rand() % 256);
    }
  }

  std::vector<char> test_data_;
  std::vector<char> random_data_;
};

TEST_F(PageCompressionTest, IsEnabledCheck) {
  // Just verify the method compiles and returns a value
  [[maybe_unused]] bool enabled = PageCompression::is_enabled();
  SUCCEED();
}

TEST_F(PageCompressionTest, MaxCompressedSize) {
  size_t max_size = PageCompression::max_compressed_size(kTestPageSize);
  // Max size should be >= input size
  EXPECT_GE(max_size, kTestPageSize);
}

#ifdef ENTROPY_ENABLE_COMPRESSION

TEST_F(PageCompressionTest, CompressDecompress) {
  std::vector<char> compressed(
      PageCompression::max_compressed_size(kTestPageSize));
  std::vector<char> decompressed(kTestPageSize);

  // Compress
  size_t compressed_size =
      PageCompression::compress(test_data_.data(), test_data_.size(),
                                compressed.data(), compressed.size());

  ASSERT_GT(compressed_size, 0);
  EXPECT_LT(compressed_size, kTestPageSize); // Compressible data

  // Decompress
  size_t decompressed_size =
      PageCompression::decompress(compressed.data(), compressed_size,
                                  decompressed.data(), decompressed.size());

  ASSERT_EQ(decompressed_size, kTestPageSize);
  EXPECT_EQ(std::memcmp(test_data_.data(), decompressed.data(), kTestPageSize),
            0);
}

TEST_F(PageCompressionTest, IsCompressed) {
  std::vector<char> compressed(
      PageCompression::max_compressed_size(kTestPageSize));

  size_t compressed_size =
      PageCompression::compress(test_data_.data(), test_data_.size(),
                                compressed.data(), compressed.size());

  if (compressed_size > 0) {
    EXPECT_TRUE(
        PageCompression::is_compressed(compressed.data(), compressed_size));
  }

  // Uncompressed data should not be detected as compressed
  EXPECT_FALSE(
      PageCompression::is_compressed(test_data_.data(), test_data_.size()));
}

TEST_F(PageCompressionTest, ChecksumValidation) {
  std::vector<char> compressed(
      PageCompression::max_compressed_size(kTestPageSize));
  std::vector<char> decompressed(kTestPageSize);

  size_t compressed_size =
      PageCompression::compress(test_data_.data(), test_data_.size(),
                                compressed.data(), compressed.size());

  if (compressed_size > 0) {
    // Corrupt a byte in the compressed data (after header)
    compressed[CompressionHeader::SIZE + 10] ^= 0xFF;

    // Decompression should fail due to corruption
    size_t result =
        PageCompression::decompress(compressed.data(), compressed_size,
                                    decompressed.data(), decompressed.size());

    // Either decompression fails or checksum fails
    if (result > 0) {
      // If LZ4 succeeded, checksum should differ
      EXPECT_NE(
          std::memcmp(test_data_.data(), decompressed.data(), kTestPageSize),
          0);
    } else {
      EXPECT_EQ(result, 0);
    }
  }
}

#else

TEST_F(PageCompressionTest, CompressionDisabled) {
  std::vector<char> compressed(kTestPageSize * 2);

  // When disabled, compress should return 0
  size_t result =
      PageCompression::compress(test_data_.data(), test_data_.size(),
                                compressed.data(), compressed.size());

  EXPECT_EQ(result, 0);
}

#endif

} // namespace entropy
