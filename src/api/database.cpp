/**
 * @file database.cpp
 * @brief Database class implementation
 */

#include "entropy/database.hpp"

#include <utility>

#include "common/logger.hpp"

namespace entropy {

class DatabaseImpl {
public:
    explicit DatabaseImpl(const std::string& path,
                          [[maybe_unused]] const DatabaseOptions& options)
        : path_(path), is_open_(true) {
        Logger::init();
        LOG_INFO("Opening database: {}", path);
    }

    ~DatabaseImpl() {
        close();
    }

    Result execute([[maybe_unused]] std::string_view sql) {
        // TODO: Implement SQL execution
        return Result(Status::NotSupported("SQL execution not yet implemented"));
    }

    Status begin_transaction() {
        if (in_transaction_) {
            return Status::Error("Already in a transaction");
        }
        in_transaction_ = true;
        return Status::Ok();
    }

    Status commit() {
        if (!in_transaction_) {
            return Status::Error("No active transaction");
        }
        in_transaction_ = false;
        return Status::Ok();
    }

    Status rollback() {
        if (!in_transaction_) {
            return Status::Error("No active transaction");
        }
        in_transaction_ = false;
        return Status::Ok();
    }

    bool in_transaction() const noexcept { return in_transaction_; }

    void close() {
        if (is_open_) {
            LOG_INFO("Closing database: {}", path_);
            is_open_ = false;
        }
    }

    bool is_open() const noexcept { return is_open_; }
    std::string_view path() const noexcept { return path_; }

private:
    std::string path_;
    bool is_open_ = false;
    bool in_transaction_ = false;
};

Database::Database(const std::string& path, const DatabaseOptions& options)
    : impl_(std::make_unique<DatabaseImpl>(path, options)) {}

Database::~Database() = default;

Database::Database(Database&&) noexcept = default;
Database& Database::operator=(Database&&) noexcept = default;

Result Database::execute(std::string_view sql) {
    return impl_->execute(sql);
}

Status Database::begin_transaction() {
    return impl_->begin_transaction();
}

Status Database::commit() {
    return impl_->commit();
}

Status Database::rollback() {
    return impl_->rollback();
}

bool Database::in_transaction() const noexcept {
    return impl_->in_transaction();
}

void Database::close() {
    impl_->close();
}

bool Database::is_open() const noexcept {
    return impl_->is_open();
}

std::string_view Database::path() const noexcept {
    return impl_->path();
}

}  // namespace entropy
