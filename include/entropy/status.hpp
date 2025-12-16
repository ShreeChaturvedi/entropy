#pragma once

/**
 * @file status.hpp
 * @brief Public status and error codes for Entropy Database Engine
 */

#include <string>
#include <string_view>

namespace entropy {

/**
 * @brief Status codes for database operations
 */
enum class StatusCode {
    kOk = 0,
    kError,
    kNotFound,
    kAlreadyExists,
    kInvalidArgument,
    kIOError,
    kCorruption,
    kNotSupported,
    kOutOfMemory,
    kBusy,
    kTimeout,
    kAborted,
    kInternal,
};

/**
 * @brief Status class for operation results
 *
 * Status encapsulates the result of an operation. It can indicate success
 * or failure, and in case of failure, provides an error code and message.
 */
class Status {
public:
    /**
     * @brief Create a success status
     */
    Status() noexcept : code_(StatusCode::kOk) {}

    /**
     * @brief Create a status with the given code
     */
    explicit Status(StatusCode code) noexcept : code_(code) {}

    /**
     * @brief Create a status with code and message
     */
    Status(StatusCode code, std::string message) noexcept
        : code_(code), message_(std::move(message)) {}

    // Factory methods for common statuses
    [[nodiscard]] static Status Ok() noexcept { return Status(); }
    [[nodiscard]] static Status Error(std::string msg = "") { return Status(StatusCode::kError, std::move(msg)); }
    [[nodiscard]] static Status NotFound(std::string msg = "") { return Status(StatusCode::kNotFound, std::move(msg)); }
    [[nodiscard]] static Status AlreadyExists(std::string msg = "") { return Status(StatusCode::kAlreadyExists, std::move(msg)); }
    [[nodiscard]] static Status InvalidArgument(std::string msg = "") { return Status(StatusCode::kInvalidArgument, std::move(msg)); }
    [[nodiscard]] static Status IOError(std::string msg = "") { return Status(StatusCode::kIOError, std::move(msg)); }
    [[nodiscard]] static Status Corruption(std::string msg = "") { return Status(StatusCode::kCorruption, std::move(msg)); }
    [[nodiscard]] static Status NotSupported(std::string msg = "") { return Status(StatusCode::kNotSupported, std::move(msg)); }
    [[nodiscard]] static Status OutOfMemory(std::string msg = "") { return Status(StatusCode::kOutOfMemory, std::move(msg)); }
    [[nodiscard]] static Status Busy(std::string msg = "") { return Status(StatusCode::kBusy, std::move(msg)); }
    [[nodiscard]] static Status Timeout(std::string msg = "") { return Status(StatusCode::kTimeout, std::move(msg)); }
    [[nodiscard]] static Status Aborted(std::string msg = "") { return Status(StatusCode::kAborted, std::move(msg)); }
    [[nodiscard]] static Status Internal(std::string msg = "") { return Status(StatusCode::kInternal, std::move(msg)); }

    // Query methods
    [[nodiscard]] bool ok() const noexcept { return code_ == StatusCode::kOk; }
    [[nodiscard]] bool is_error() const noexcept { return code_ != StatusCode::kOk; }
    [[nodiscard]] bool is_not_found() const noexcept { return code_ == StatusCode::kNotFound; }
    [[nodiscard]] bool is_io_error() const noexcept { return code_ == StatusCode::kIOError; }

    [[nodiscard]] StatusCode code() const noexcept { return code_; }
    [[nodiscard]] std::string_view message() const noexcept { return message_; }

    /**
     * @brief Get a human-readable string representation
     */
    [[nodiscard]] std::string to_string() const;

    // Implicit conversion to bool for convenience
    explicit operator bool() const noexcept { return ok(); }

private:
    StatusCode code_;
    std::string message_;
};

}  // namespace entropy
