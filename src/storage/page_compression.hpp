#pragma once

/**
 * @file page_compression.hpp
 * @brief Page compression utilities using LZ4
 *
 * Design Goals:
 * 1. Transparent compression/decompression for pages
 * 2. Fast compression prioritizing speed over ratio
 * 3. Page-level granularity for random access
 * 4. Compile-time optional via ENTROPY_ENABLE_COMPRESSION
 *
 * Usage:
 * - compress_page(): Compress a page for disk write
 * - decompress_page(): Decompress a page after disk read
 * - is_enabled(): Check if compression is available
 */

#include <cstdint>
#include <cstring>
#include <vector>

#include "common/config.hpp"

#ifdef ENTROPY_ENABLE_COMPRESSION
#include <lz4.h>
#endif

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Compression Header
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Header stored at the start of compressed pages
 */
struct CompressionHeader {
  uint32_t magic = 0x4C5A3447;  // "LZ4G"
  uint32_t original_size = 0;   // Uncompressed size
  uint32_t compressed_size = 0; // Compressed size (excluding header)
  uint32_t checksum = 0;        // Simple checksum for validation

  static constexpr size_t SIZE = 16;
};

// ─────────────────────────────────────────────────────────────────────────────
// Page Compression
// ─────────────────────────────────────────────────────────────────────────────

class PageCompression {
public:
  /**
   * @brief Check if compression is enabled at compile time
   */
  static constexpr bool is_enabled() {
#ifdef ENTROPY_ENABLE_COMPRESSION
    return true;
#else
    return false;
#endif
  }

  /**
   * @brief Compress a page
   * @param src Source page data
   * @param src_size Size of source data
   * @param dst Destination buffer (must be large enough)
   * @param dst_capacity Capacity of destination buffer
   * @return Compressed size including header, or 0 on failure
   *
   * Note: Destination should be at least max_compressed_size(src_size)
   */
  static size_t compress(const char *src, size_t src_size, char *dst,
                         size_t dst_capacity) {
#ifdef ENTROPY_ENABLE_COMPRESSION
    if (dst_capacity < CompressionHeader::SIZE) {
      return 0;
    }

    // Compress data after header
    int compressed_size = LZ4_compress_default(
        src, dst + CompressionHeader::SIZE, static_cast<int>(src_size),
        static_cast<int>(dst_capacity - CompressionHeader::SIZE));

    if (compressed_size <= 0) {
      return 0; // Compression failed
    }

    // If compression didn't help, store uncompressed
    if (static_cast<size_t>(compressed_size) >= src_size) {
      return 0; // No benefit from compression
    }

    // Write header
    CompressionHeader header;
    header.original_size = static_cast<uint32_t>(src_size);
    header.compressed_size = static_cast<uint32_t>(compressed_size);
    header.checksum = compute_checksum(src, src_size);
    std::memcpy(dst, &header, CompressionHeader::SIZE);

    return CompressionHeader::SIZE + compressed_size;
#else
    (void)src;
    (void)src_size;
    (void)dst;
    (void)dst_capacity;
    return 0; // Compression not enabled
#endif
  }

  /**
   * @brief Decompress a page
   * @param src Compressed data (with header)
   * @param src_size Size of compressed data
   * @param dst Destination buffer
   * @param dst_capacity Capacity of destination buffer
   * @return Original size, or 0 on failure
   */
  static size_t decompress(const char *src, size_t src_size, char *dst,
                           size_t dst_capacity) {
#ifdef ENTROPY_ENABLE_COMPRESSION
    if (src_size < CompressionHeader::SIZE) {
      return 0;
    }

    // Read header
    CompressionHeader header;
    std::memcpy(&header, src, CompressionHeader::SIZE);

    // Validate magic
    if (header.magic != 0x4C5A3447) {
      return 0; // Not compressed or corrupted
    }

    if (dst_capacity < header.original_size) {
      return 0; // Destination too small
    }

    // Decompress
    int decompressed_size =
        LZ4_decompress_safe(src + CompressionHeader::SIZE, dst,
                            static_cast<int>(header.compressed_size),
                            static_cast<int>(dst_capacity));

    if (decompressed_size < 0 ||
        static_cast<uint32_t>(decompressed_size) != header.original_size) {
      return 0; // Decompression failed
    }

    // Validate checksum
    if (compute_checksum(dst, decompressed_size) != header.checksum) {
      return 0; // Checksum mismatch
    }

    return decompressed_size;
#else
    (void)src;
    (void)src_size;
    (void)dst;
    (void)dst_capacity;
    return 0;
#endif
  }

  /**
   * @brief Check if data is compressed (has valid header)
   */
  static bool is_compressed(const char *data, size_t size) {
    if (size < CompressionHeader::SIZE) {
      return false;
    }
    CompressionHeader header;
    std::memcpy(&header, data, CompressionHeader::SIZE);
    return header.magic == 0x4C5A3447;
  }

  /**
   * @brief Maximum possible compressed size for a given input size
   */
  static size_t max_compressed_size(size_t input_size) {
#ifdef ENTROPY_ENABLE_COMPRESSION
    return CompressionHeader::SIZE +
           LZ4_compressBound(static_cast<int>(input_size));
#else
    return input_size;
#endif
  }

private:
  /**
   * @brief Simple checksum for data validation
   */
  static uint32_t compute_checksum(const char *data, size_t size) {
    uint32_t checksum = 0;
    for (size_t i = 0; i < size; ++i) {
      checksum = checksum * 31 + static_cast<uint8_t>(data[i]);
    }
    return checksum;
  }
};

} // namespace entropy
