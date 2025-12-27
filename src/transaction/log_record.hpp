#pragma once

/**
 * @file log_record.hpp
 * @brief WAL (Write-Ahead Log) record format
 *
 * Log Record Format (on disk):
 * ┌────────────────────────────────────────────────────────────────────────┐
 * │ Header (32 bytes)                                                       │
 * │   - size (4 bytes): total record size including header                  │
 * │   - lsn (8 bytes): log sequence number                                  │
 * │   - txn_id (8 bytes): transaction ID                                    │
 * │   - prev_lsn (8 bytes): previous LSN for this transaction               │
 * │   - type (1 byte): log record type                                      │
 * │   - padding (3 bytes): for alignment                                    │
 * ├────────────────────────────────────────────────────────────────────────┤
 * │ Payload (variable size, depends on type)                                │
 * └────────────────────────────────────────────────────────────────────────┘
 *
 * Transaction Records:
 *   - BEGIN: no payload
 *   - COMMIT: no payload
 *   - ABORT: no payload
 *
 * Data Records:
 *   - INSERT: table_oid, rid, tuple_size, tuple_data
 *   - DELETE: table_oid, rid, tuple_size, tuple_data (for undo)
 *   - UPDATE: table_oid, rid, old_size, old_data, new_size, new_data
 *
 * System Records:
 *   - CHECKPOINT: active_txn_count, [txn_ids...]
 */

#include <cstring>
#include <vector>

#include "common/types.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Log Record Type
// ─────────────────────────────────────────────────────────────────────────────

enum class LogRecordType : uint8_t {
    INVALID = 0,

    // Transaction control
    BEGIN = 1,
    COMMIT = 2,
    ABORT = 3,

    // Data modification
    INSERT = 10,
    DELETE = 11,
    UPDATE = 12,

    // System
    CHECKPOINT = 20,
    END_CHECKPOINT = 21,
};

/**
 * @brief Convert log record type to string for debugging
 */
[[nodiscard]] inline const char* log_record_type_to_string(LogRecordType type) {
    switch (type) {
        case LogRecordType::INVALID:
            return "INVALID";
        case LogRecordType::BEGIN:
            return "BEGIN";
        case LogRecordType::COMMIT:
            return "COMMIT";
        case LogRecordType::ABORT:
            return "ABORT";
        case LogRecordType::INSERT:
            return "INSERT";
        case LogRecordType::DELETE:
            return "DELETE";
        case LogRecordType::UPDATE:
            return "UPDATE";
        case LogRecordType::CHECKPOINT:
            return "CHECKPOINT";
        case LogRecordType::END_CHECKPOINT:
            return "END_CHECKPOINT";
        default:
            return "UNKNOWN";
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Log Record Header
// ─────────────────────────────────────────────────────────────────────────────

static constexpr uint32_t LOG_RECORD_HEADER_SIZE = 32;

/**
 * @brief Fixed-size header for all log records
 * Ordered to avoid padding gaps
 */
struct LogRecordHeader {
    lsn_t lsn = INVALID_LSN;                  // Log sequence number (8 bytes)
    txn_id_t txn_id = INVALID_TXN_ID;         // Transaction ID (8 bytes)
    lsn_t prev_lsn = INVALID_LSN;             // Previous LSN for this transaction (8 bytes)
    uint32_t size = 0;                        // Total record size (header + payload) (4 bytes)
    LogRecordType type = LogRecordType::INVALID;  // (1 byte)
    uint8_t padding[3] = {0, 0, 0};           // Padding for alignment (3 bytes)
};

static_assert(sizeof(LogRecordHeader) == LOG_RECORD_HEADER_SIZE,
              "LogRecordHeader must be 32 bytes");

// ─────────────────────────────────────────────────────────────────────────────
// Log Record Class
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Represents a single log record for WAL
 *
 * Log records are used for:
 * 1. Durability: Ensures committed transactions survive crashes
 * 2. Atomicity: Allows undo of uncommitted transactions
 * 3. Recovery: Enables REDO/UNDO during crash recovery
 */
class LogRecord {
public:
    LogRecord() = default;

    // ─────────────────────────────────────────────────────────────────────────
    // Factory methods for different record types
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Create a BEGIN log record
     */
    static LogRecord make_begin(txn_id_t txn_id) {
        LogRecord record;
        record.header_.type = LogRecordType::BEGIN;
        record.header_.txn_id = txn_id;
        record.header_.size = LOG_RECORD_HEADER_SIZE;
        return record;
    }

    /**
     * @brief Create a COMMIT log record
     */
    static LogRecord make_commit(txn_id_t txn_id, lsn_t prev_lsn) {
        LogRecord record;
        record.header_.type = LogRecordType::COMMIT;
        record.header_.txn_id = txn_id;
        record.header_.prev_lsn = prev_lsn;
        record.header_.size = LOG_RECORD_HEADER_SIZE;
        return record;
    }

    /**
     * @brief Create an ABORT log record
     */
    static LogRecord make_abort(txn_id_t txn_id, lsn_t prev_lsn) {
        LogRecord record;
        record.header_.type = LogRecordType::ABORT;
        record.header_.txn_id = txn_id;
        record.header_.prev_lsn = prev_lsn;
        record.header_.size = LOG_RECORD_HEADER_SIZE;
        return record;
    }

    /**
     * @brief Create an INSERT log record
     * @param txn_id Transaction ID
     * @param prev_lsn Previous LSN for this transaction
     * @param table_oid Table OID
     * @param rid Record ID of inserted tuple
     * @param tuple_data The inserted tuple data
     */
    static LogRecord make_insert(txn_id_t txn_id, lsn_t prev_lsn, oid_t table_oid,
                                 RID rid, const std::vector<char>& tuple_data) {
        LogRecord record;
        record.header_.type = LogRecordType::INSERT;
        record.header_.txn_id = txn_id;
        record.header_.prev_lsn = prev_lsn;
        record.table_oid_ = table_oid;
        record.rid_ = rid;
        record.new_tuple_data_ = tuple_data;
        record.header_.size = LOG_RECORD_HEADER_SIZE + record.payload_size();
        return record;
    }

    /**
     * @brief Create a DELETE log record
     * @param txn_id Transaction ID
     * @param prev_lsn Previous LSN for this transaction
     * @param table_oid Table OID
     * @param rid Record ID of deleted tuple
     * @param tuple_data The deleted tuple data (for undo)
     */
    static LogRecord make_delete(txn_id_t txn_id, lsn_t prev_lsn, oid_t table_oid,
                                 RID rid, const std::vector<char>& tuple_data) {
        LogRecord record;
        record.header_.type = LogRecordType::DELETE;
        record.header_.txn_id = txn_id;
        record.header_.prev_lsn = prev_lsn;
        record.table_oid_ = table_oid;
        record.rid_ = rid;
        record.old_tuple_data_ = tuple_data;
        record.header_.size = LOG_RECORD_HEADER_SIZE + record.payload_size();
        return record;
    }

    /**
     * @brief Create an UPDATE log record
     */
    static LogRecord make_update(txn_id_t txn_id, lsn_t prev_lsn, oid_t table_oid,
                                 RID rid, const std::vector<char>& old_data,
                                 const std::vector<char>& new_data) {
        LogRecord record;
        record.header_.type = LogRecordType::UPDATE;
        record.header_.txn_id = txn_id;
        record.header_.prev_lsn = prev_lsn;
        record.table_oid_ = table_oid;
        record.rid_ = rid;
        record.old_tuple_data_ = old_data;
        record.new_tuple_data_ = new_data;
        record.header_.size = LOG_RECORD_HEADER_SIZE + record.payload_size();
        return record;
    }

    /**
     * @brief Create a CHECKPOINT log record
     */
    static LogRecord make_checkpoint(const std::vector<txn_id_t>& active_txns) {
        LogRecord record;
        record.header_.type = LogRecordType::CHECKPOINT;
        record.header_.txn_id = INVALID_TXN_ID;
        record.active_txns_ = active_txns;
        record.header_.size = LOG_RECORD_HEADER_SIZE + record.payload_size();
        return record;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────────────────────────────

    [[nodiscard]] uint32_t size() const noexcept { return header_.size; }
    [[nodiscard]] lsn_t lsn() const noexcept { return header_.lsn; }
    [[nodiscard]] txn_id_t txn_id() const noexcept { return header_.txn_id; }
    [[nodiscard]] lsn_t prev_lsn() const noexcept { return header_.prev_lsn; }
    [[nodiscard]] LogRecordType type() const noexcept { return header_.type; }

    // Data record accessors
    [[nodiscard]] oid_t table_oid() const noexcept { return table_oid_; }
    [[nodiscard]] RID rid() const noexcept { return rid_; }
    [[nodiscard]] const std::vector<char>& old_tuple_data() const noexcept {
        return old_tuple_data_;
    }
    [[nodiscard]] const std::vector<char>& new_tuple_data() const noexcept {
        return new_tuple_data_;
    }

    // Checkpoint accessors
    [[nodiscard]] const std::vector<txn_id_t>& active_txns() const noexcept {
        return active_txns_;
    }

    /**
     * @brief Set the LSN after appending to log
     */
    void set_lsn(lsn_t lsn) noexcept { header_.lsn = lsn; }

    // ─────────────────────────────────────────────────────────────────────────
    // Serialization
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Serialize the log record to a byte buffer
     * @return Serialized record data
     */
    [[nodiscard]] std::vector<char> serialize() const {
        std::vector<char> buffer(header_.size);
        char* ptr = buffer.data();

        // Write header
        std::memcpy(ptr, &header_, LOG_RECORD_HEADER_SIZE);
        ptr += LOG_RECORD_HEADER_SIZE;

        // Write payload based on type
        switch (header_.type) {
            case LogRecordType::INSERT:
                serialize_insert(ptr);
                break;
            case LogRecordType::DELETE:
                serialize_delete(ptr);
                break;
            case LogRecordType::UPDATE:
                serialize_update(ptr);
                break;
            case LogRecordType::CHECKPOINT:
                serialize_checkpoint(ptr);
                break;
            default:
                // BEGIN, COMMIT, ABORT have no payload
                break;
        }

        return buffer;
    }

    /**
     * @brief Deserialize a log record from a byte buffer
     * @param data Buffer containing serialized record
     * @return Deserialized log record
     */
    static LogRecord deserialize(const char* data, [[maybe_unused]] uint32_t size) {
        LogRecord record;

        // Read header
        std::memcpy(&record.header_, data, LOG_RECORD_HEADER_SIZE);
        data += LOG_RECORD_HEADER_SIZE;

        // Read payload based on type
        switch (record.header_.type) {
            case LogRecordType::INSERT:
                record.deserialize_insert(data);
                break;
            case LogRecordType::DELETE:
                record.deserialize_delete(data);
                break;
            case LogRecordType::UPDATE:
                record.deserialize_update(data);
                break;
            case LogRecordType::CHECKPOINT:
                record.deserialize_checkpoint(data);
                break;
            default:
                break;
        }

        return record;
    }

private:
    /**
     * @brief Calculate payload size based on record type
     */
    [[nodiscard]] uint32_t payload_size() const {
        switch (header_.type) {
            case LogRecordType::INSERT:
                // table_oid(4) + rid(8) + tuple_size(4) + tuple_data
                return 4 + 8 + 4 + static_cast<uint32_t>(new_tuple_data_.size());
            case LogRecordType::DELETE:
                // table_oid(4) + rid(8) + tuple_size(4) + tuple_data
                return 4 + 8 + 4 + static_cast<uint32_t>(old_tuple_data_.size());
            case LogRecordType::UPDATE:
                // table_oid(4) + rid(8) + old_size(4) + old_data + new_size(4) + new_data
                return 4 + 8 + 4 + static_cast<uint32_t>(old_tuple_data_.size()) + 4 +
                       static_cast<uint32_t>(new_tuple_data_.size());
            case LogRecordType::CHECKPOINT:
                // count(4) + txn_ids(8 * count)
                return 4 + static_cast<uint32_t>(active_txns_.size() * sizeof(txn_id_t));
            default:
                return 0;
        }
    }

    void serialize_insert(char* ptr) const {
        std::memcpy(ptr, &table_oid_, sizeof(table_oid_));
        ptr += sizeof(table_oid_);

        std::memcpy(ptr, &rid_, sizeof(rid_));
        ptr += sizeof(rid_);

        uint32_t tuple_size = static_cast<uint32_t>(new_tuple_data_.size());
        std::memcpy(ptr, &tuple_size, sizeof(tuple_size));
        ptr += sizeof(tuple_size);

        std::memcpy(ptr, new_tuple_data_.data(), tuple_size);
    }

    void deserialize_insert(const char* ptr) {
        std::memcpy(&table_oid_, ptr, sizeof(table_oid_));
        ptr += sizeof(table_oid_);

        std::memcpy(&rid_, ptr, sizeof(rid_));
        ptr += sizeof(rid_);

        uint32_t tuple_size;
        std::memcpy(&tuple_size, ptr, sizeof(tuple_size));
        ptr += sizeof(tuple_size);

        new_tuple_data_.resize(tuple_size);
        std::memcpy(new_tuple_data_.data(), ptr, tuple_size);
    }

    void serialize_delete(char* ptr) const {
        std::memcpy(ptr, &table_oid_, sizeof(table_oid_));
        ptr += sizeof(table_oid_);

        std::memcpy(ptr, &rid_, sizeof(rid_));
        ptr += sizeof(rid_);

        uint32_t tuple_size = static_cast<uint32_t>(old_tuple_data_.size());
        std::memcpy(ptr, &tuple_size, sizeof(tuple_size));
        ptr += sizeof(tuple_size);

        std::memcpy(ptr, old_tuple_data_.data(), tuple_size);
    }

    void deserialize_delete(const char* ptr) {
        std::memcpy(&table_oid_, ptr, sizeof(table_oid_));
        ptr += sizeof(table_oid_);

        std::memcpy(&rid_, ptr, sizeof(rid_));
        ptr += sizeof(rid_);

        uint32_t tuple_size;
        std::memcpy(&tuple_size, ptr, sizeof(tuple_size));
        ptr += sizeof(tuple_size);

        old_tuple_data_.resize(tuple_size);
        std::memcpy(old_tuple_data_.data(), ptr, tuple_size);
    }

    void serialize_update(char* ptr) const {
        std::memcpy(ptr, &table_oid_, sizeof(table_oid_));
        ptr += sizeof(table_oid_);

        std::memcpy(ptr, &rid_, sizeof(rid_));
        ptr += sizeof(rid_);

        uint32_t old_size = static_cast<uint32_t>(old_tuple_data_.size());
        std::memcpy(ptr, &old_size, sizeof(old_size));
        ptr += sizeof(old_size);

        std::memcpy(ptr, old_tuple_data_.data(), old_size);
        ptr += old_size;

        uint32_t new_size = static_cast<uint32_t>(new_tuple_data_.size());
        std::memcpy(ptr, &new_size, sizeof(new_size));
        ptr += sizeof(new_size);

        std::memcpy(ptr, new_tuple_data_.data(), new_size);
    }

    void deserialize_update(const char* ptr) {
        std::memcpy(&table_oid_, ptr, sizeof(table_oid_));
        ptr += sizeof(table_oid_);

        std::memcpy(&rid_, ptr, sizeof(rid_));
        ptr += sizeof(rid_);

        uint32_t old_size;
        std::memcpy(&old_size, ptr, sizeof(old_size));
        ptr += sizeof(old_size);

        old_tuple_data_.resize(old_size);
        std::memcpy(old_tuple_data_.data(), ptr, old_size);
        ptr += old_size;

        uint32_t new_size;
        std::memcpy(&new_size, ptr, sizeof(new_size));
        ptr += sizeof(new_size);

        new_tuple_data_.resize(new_size);
        std::memcpy(new_tuple_data_.data(), ptr, new_size);
    }

    void serialize_checkpoint(char* ptr) const {
        uint32_t count = static_cast<uint32_t>(active_txns_.size());
        std::memcpy(ptr, &count, sizeof(count));
        ptr += sizeof(count);

        for (txn_id_t txn_id : active_txns_) {
            std::memcpy(ptr, &txn_id, sizeof(txn_id));
            ptr += sizeof(txn_id);
        }
    }

    void deserialize_checkpoint(const char* ptr) {
        uint32_t count;
        std::memcpy(&count, ptr, sizeof(count));
        ptr += sizeof(count);

        active_txns_.resize(count);
        for (uint32_t i = 0; i < count; ++i) {
            std::memcpy(&active_txns_[i], ptr, sizeof(txn_id_t));
            ptr += sizeof(txn_id_t);
        }
    }

    // Header
    LogRecordHeader header_;

    // Data record fields
    oid_t table_oid_ = 0;
    RID rid_;
    std::vector<char> old_tuple_data_;
    std::vector<char> new_tuple_data_;

    // Checkpoint fields
    std::vector<txn_id_t> active_txns_;
};

}  // namespace entropy
