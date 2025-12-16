/**
 * @file wal.cpp
 * @brief WAL Manager stub
 */

#include "transaction/wal.hpp"

namespace entropy {

WALManager::WALManager([[maybe_unused]] const std::string& log_file)
    : log_file_(log_file) {}

lsn_t WALManager::append_log([[maybe_unused]] const LogRecord& record) {
    return next_lsn_++;  // Stub
}

Status WALManager::flush() {
    flushed_lsn_ = next_lsn_ - 1;
    return Status::Ok();  // Stub
}

}  // namespace entropy
