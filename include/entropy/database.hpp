#pragma once

/**
 * @file database.hpp
 * @brief Database class - main entry point for Entropy
 */

#include <memory>
#include <string>
#include <string_view>

#include "entropy/result.hpp"
#include "entropy/status.hpp"

namespace entropy {

// Forward declarations
class DatabaseImpl;

/**
 * @brief Configuration options for opening a database
 */
struct DatabaseOptions {
    /// Size of the buffer pool in pages (default: 1024 = 4MB with 4KB pages)
    size_t buffer_pool_size = 1024;

    /// Page size in bytes (must be power of 2, default: 4096)
    size_t page_size = 4096;

    /// Enable write-ahead logging (default: true)
    bool enable_wal = true;

    /// Create the database if it doesn't exist (default: true)
    bool create_if_missing = true;

    /// Throw error if database already exists (default: false)
    bool error_if_exists = false;

    /// Enable strict mode for SQL parsing (default: false)
    bool strict_mode = false;
};

/**
 * @brief Main database class
 *
 * The Database class is the primary interface for interacting with Entropy.
 * It provides methods for executing SQL statements and managing transactions.
 *
 * Example usage:
 * @code
 * entropy::Database db("mydb.entropy");
 * db.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");
 * db.execute("INSERT INTO users VALUES (1, 'Alice')");
 * auto result = db.execute("SELECT * FROM users");
 * @endcode
 */
class Database {
public:
    /**
     * @brief Open or create a database
     * @param path Path to the database file
     * @param options Configuration options
     */
    explicit Database(const std::string& path, const DatabaseOptions& options = {});

    /**
     * @brief Destructor - closes the database
     */
    ~Database();

    // Non-copyable
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    // Movable
    Database(Database&&) noexcept;
    Database& operator=(Database&&) noexcept;

    /**
     * @brief Execute a SQL statement
     * @param sql The SQL statement to execute
     * @return Result containing query results or status
     */
    [[nodiscard]] Result execute(std::string_view sql);

    /**
     * @brief Begin a new transaction
     * @return Status indicating success or failure
     */
    [[nodiscard]] Status begin_transaction();

    /**
     * @brief Commit the current transaction
     * @return Status indicating success or failure
     */
    [[nodiscard]] Status commit();

    /**
     * @brief Rollback the current transaction
     * @return Status indicating success or failure
     */
    [[nodiscard]] Status rollback();

    /**
     * @brief Check if there's an active transaction
     */
    [[nodiscard]] bool in_transaction() const noexcept;

    /**
     * @brief Close the database
     */
    void close();

    /**
     * @brief Check if the database is open
     */
    [[nodiscard]] bool is_open() const noexcept;

    /**
     * @brief Get the path to the database file
     */
    [[nodiscard]] std::string_view path() const noexcept;

private:
    std::unique_ptr<DatabaseImpl> impl_;
};

}  // namespace entropy
