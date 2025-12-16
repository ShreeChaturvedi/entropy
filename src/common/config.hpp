#pragma once

/**
 * @file config.hpp
 * @brief Configuration constants for Entropy
 */

#include <cstddef>
#include <cstdint>

namespace entropy {
namespace config {

// ─────────────────────────────────────────────────────────────────────────────
// Page Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Default page size in bytes (4KB)
constexpr size_t kDefaultPageSize = 4096;

/// Minimum page size (1KB)
constexpr size_t kMinPageSize = 1024;

/// Maximum page size (64KB)
constexpr size_t kMaxPageSize = 65536;

/// Page header size in bytes
constexpr size_t kPageHeaderSize = 32;

// ─────────────────────────────────────────────────────────────────────────────
// Buffer Pool Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Default buffer pool size in pages
constexpr size_t kDefaultBufferPoolSize = 1024;  // 4MB with default page size

/// Minimum buffer pool size
constexpr size_t kMinBufferPoolSize = 16;

/// Maximum buffer pool size
constexpr size_t kMaxBufferPoolSize = 1048576;  // 4GB with default page size

// ─────────────────────────────────────────────────────────────────────────────
// B+ Tree Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Default B+ tree order (fan-out)
constexpr size_t kDefaultBPlusTreeOrder = 128;

/// Minimum keys in a B+ tree node (order / 2)
constexpr size_t kBPlusTreeMinKeys = 64;

// ─────────────────────────────────────────────────────────────────────────────
// Transaction Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Maximum concurrent transactions
constexpr size_t kMaxConcurrentTransactions = 1024;

/// Default lock timeout in milliseconds
constexpr uint32_t kDefaultLockTimeoutMs = 5000;

// ─────────────────────────────────────────────────────────────────────────────
// WAL Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// WAL buffer size in bytes
constexpr size_t kWALBufferSize = 1024 * 1024;  // 1MB

/// Checkpoint interval in log records
constexpr size_t kCheckpointInterval = 10000;

// ─────────────────────────────────────────────────────────────────────────────
// Catalog Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Maximum table name length
constexpr size_t kMaxTableNameLength = 128;

/// Maximum column name length
constexpr size_t kMaxColumnNameLength = 64;

/// Maximum columns per table
constexpr size_t kMaxColumnsPerTable = 256;

/// Maximum indexes per table
constexpr size_t kMaxIndexesPerTable = 32;

// ─────────────────────────────────────────────────────────────────────────────
// Tuple Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Maximum tuple size in bytes
constexpr size_t kMaxTupleSize = 8192;  // 8KB

/// Maximum VARCHAR length
constexpr size_t kMaxVarcharLength = 4096;

// ─────────────────────────────────────────────────────────────────────────────
// File Extensions
// ─────────────────────────────────────────────────────────────────────────────

/// Database file extension
constexpr const char* kDatabaseFileExtension = ".entropy";

/// WAL file extension
constexpr const char* kWALFileExtension = ".wal";

/// Catalog file extension
constexpr const char* kCatalogFileExtension = ".catalog";

}  // namespace config
}  // namespace entropy
