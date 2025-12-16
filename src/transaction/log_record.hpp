#pragma once

/**
 * @file log_record.hpp
 * @brief WAL record format
 */

#include <vector>

#include "common/types.hpp"

namespace entropy {

enum class LogRecordType : uint8_t {
    INVALID = 0,
    BEGIN,
    COMMIT,
    ABORT,
    INSERT,
    DELETE,
    UPDATE,
    CHECKPOINT,
};

class LogRecord {
public:
    LogRecord() = default;

    [[nodiscard]] lsn_t lsn() const noexcept { return lsn_; }
    [[nodiscard]] txn_id_t txn_id() const noexcept { return txn_id_; }
    [[nodiscard]] LogRecordType type() const noexcept { return type_; }

private:
    lsn_t lsn_ = INVALID_LSN;
    txn_id_t txn_id_ = INVALID_TXN_ID;
    lsn_t prev_lsn_ = INVALID_LSN;
    LogRecordType type_ = LogRecordType::INVALID;
};

}  // namespace entropy
