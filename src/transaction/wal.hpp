#pragma once

/**
 * @file wal.hpp
 * @brief Write-Ahead Log
 */

#include <memory>
#include <string>

#include "common/types.hpp"
#include "entropy/status.hpp"
#include "transaction/log_record.hpp"

namespace entropy {

class DiskManager;

class WALManager {
public:
    explicit WALManager(const std::string& log_file);
    ~WALManager() = default;

    [[nodiscard]] lsn_t append_log(const LogRecord& record);
    [[nodiscard]] Status flush();
    [[nodiscard]] lsn_t get_flushed_lsn() const noexcept { return flushed_lsn_; }

private:
    std::string log_file_;
    lsn_t next_lsn_ = 1;
    lsn_t flushed_lsn_ = 0;
};

}  // namespace entropy
