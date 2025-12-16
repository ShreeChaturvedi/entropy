#pragma once

/**
 * @file types.hpp
 * @brief Common type definitions for Entropy
 */

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Basic Type Aliases
// ─────────────────────────────────────────────────────────────────────────────

/// Page identifier type
using page_id_t = int32_t;

/// Frame identifier in buffer pool
using frame_id_t = int32_t;

/// Transaction identifier
using txn_id_t = uint64_t;

/// Log sequence number
using lsn_t = uint64_t;

/// Slot number within a page
using slot_id_t = uint16_t;

/// Offset within a page
using offset_t = uint16_t;

/// Object identifier (tables, indexes, etc.)
using oid_t = uint32_t;

/// Column identifier within a table
using column_id_t = uint16_t;

// ─────────────────────────────────────────────────────────────────────────────
// Invalid/Sentinel Values
// ─────────────────────────────────────────────────────────────────────────────

/// Invalid page ID
constexpr page_id_t INVALID_PAGE_ID = -1;

/// Invalid frame ID
constexpr frame_id_t INVALID_FRAME_ID = -1;

/// Invalid transaction ID
constexpr txn_id_t INVALID_TXN_ID = 0;

/// Invalid LSN
constexpr lsn_t INVALID_LSN = 0;

/// Invalid slot ID
constexpr slot_id_t INVALID_SLOT_ID = std::numeric_limits<slot_id_t>::max();

/// Invalid OID
constexpr oid_t INVALID_OID = 0;

// ─────────────────────────────────────────────────────────────────────────────
// Record ID (RID) - uniquely identifies a tuple
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Record ID - uniquely identifies a tuple location
 *
 * A RID consists of a page ID and a slot number within that page.
 */
struct RID {
    page_id_t page_id = INVALID_PAGE_ID;
    slot_id_t slot_id = INVALID_SLOT_ID;

    RID() = default;
    RID(page_id_t pid, slot_id_t sid) : page_id(pid), slot_id(sid) {}

    [[nodiscard]] bool is_valid() const noexcept {
        return page_id != INVALID_PAGE_ID && slot_id != INVALID_SLOT_ID;
    }

    bool operator==(const RID& other) const noexcept {
        return page_id == other.page_id && slot_id == other.slot_id;
    }

    bool operator!=(const RID& other) const noexcept {
        return !(*this == other);
    }

    bool operator<(const RID& other) const noexcept {
        if (page_id != other.page_id) return page_id < other.page_id;
        return slot_id < other.slot_id;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Data Types
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief SQL data types supported by Entropy
 */
enum class TypeId : uint8_t {
    INVALID = 0,
    BOOLEAN,
    TINYINT,    // 1 byte
    SMALLINT,   // 2 bytes
    INTEGER,    // 4 bytes
    BIGINT,     // 8 bytes
    DECIMAL,    // Fixed-point
    FLOAT,      // 4 bytes
    DOUBLE,     // 8 bytes
    VARCHAR,    // Variable-length string
    TIMESTAMP,  // Date/time
};

/**
 * @brief Get the size in bytes for a fixed-size type
 * @return Size in bytes, or 0 for variable-length types
 */
constexpr size_t type_size(TypeId type) noexcept {
    switch (type) {
        case TypeId::BOOLEAN:   return 1;
        case TypeId::TINYINT:   return 1;
        case TypeId::SMALLINT:  return 2;
        case TypeId::INTEGER:   return 4;
        case TypeId::BIGINT:    return 8;
        case TypeId::FLOAT:     return 4;
        case TypeId::DOUBLE:    return 8;
        case TypeId::TIMESTAMP: return 8;
        case TypeId::VARCHAR:   return 0;  // Variable length
        case TypeId::DECIMAL:   return 16; // Fixed 128-bit for now
        default:                return 0;
    }
}

/**
 * @brief Check if a type is variable-length
 */
constexpr bool is_variable_length(TypeId type) noexcept {
    return type == TypeId::VARCHAR;
}

}  // namespace entropy

// Hash support for RID
namespace std {
template <>
struct hash<entropy::RID> {
    size_t operator()(const entropy::RID& rid) const noexcept {
        return hash<int64_t>{}(
            (static_cast<int64_t>(rid.page_id) << 16) | rid.slot_id
        );
    }
};
}  // namespace std
