/**
 * @file log_record.cpp
 * @brief Log record serialization / deserialization
 */

#include "transaction/log_record.hpp"

namespace entropy {

bool LogRecord::try_deserialize(const char* data, uint32_t size, LogRecord& out) {
    if (data == nullptr || size < LOG_RECORD_HEADER_SIZE) {
        return false;
    }

    LogRecord record;
    std::memcpy(&record.header_, data, LOG_RECORD_HEADER_SIZE);

    if (record.header_.size != size) {
        return false;
    }

    const char* payload = data + LOG_RECORD_HEADER_SIZE;
    const uint32_t remaining = size - LOG_RECORD_HEADER_SIZE;

    bool ok = true;
    switch (record.header_.type) {
        case LogRecordType::BEGIN:
        case LogRecordType::COMMIT:
        case LogRecordType::ABORT:
        case LogRecordType::END_CHECKPOINT:
            ok = (remaining == 0);
            break;
        case LogRecordType::INSERT:
            ok = record.deserialize_insert(payload, remaining);
            break;
        case LogRecordType::DELETE:
            ok = record.deserialize_delete(payload, remaining);
            break;
        case LogRecordType::UPDATE:
            ok = record.deserialize_update(payload, remaining);
            break;
        case LogRecordType::CHECKPOINT:
            ok = record.deserialize_checkpoint(payload, remaining);
            break;
        case LogRecordType::INVALID:
        default:
            ok = false;
            break;
    }

    if (!ok) {
        return false;
    }

    out = std::move(record);
    return true;
}

}  // namespace entropy
