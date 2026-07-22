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
class Catalog;

/**
 * @brief Configuration options for opening a database
 */
struct DatabaseOptions {
    /// Size of the buffer pool in pages (default: 1024 = 4MB with 4KB pages)
    size_t buffer_pool_size = 1024;

    /// Page size in bytes. The storage engine is compiled for 4096-byte pages;
    /// any other value fails the open with a clear error (is_open() == false).
    size_t page_size = 4096;

    /// Enable write-ahead logging (default: true). When false, no WAL file is
    /// created or written: commits are not crash-durable and crash recovery is
    /// skipped on open.
    bool enable_wal = true;

    /// Create the database if it doesn't exist (default: true). When false and
    /// the file is missing, the open fails (is_open() == false).
    bool create_if_missing = true;

    /// Fail the open when the database file already exists (default: false)
    bool error_if_exists = false;

    /// Strict SQL validation (default: false). When true, INSERT literals
    /// whose kind does not match the target column's type family are rejected
    /// instead of silently coerced.
    bool strict_mode = false;
};

/**
 * @brief Main database class
 *
 * The Database class is the primary interface for interacting with Entropy.
 * It provides methods for executing SQL statements and managing transactions.
 *
 * Transactions run under Snapshot Isolation: reads see the database as of the
 * transaction's start, writers take row-level exclusive locks, and a write to
 * a row modified by a concurrently committed transaction fails with a
 * write-write conflict (kAborted) after rolling the transaction back. A
 * statement executed outside an explicit transaction runs in its own implicit
 * (autocommit) transaction.
 *
 * Explicit transactions are bound to the calling thread (a connection is a
 * thread): begin_transaction() opens a transaction for the current thread,
 * and only statements executed from that thread join it. Different threads
 * operate independent transactions against the same Database.
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
     * @brief Begin a new transaction on the calling thread
     * @return Status indicating success or failure
     */
    [[nodiscard]] Status begin_transaction();

    /**
     * @brief Commit the calling thread's transaction
     * @return Status indicating success or failure
     */
    [[nodiscard]] Status commit();

    /**
     * @brief Roll back the calling thread's transaction
     *
     * All of the transaction's writes are undone; a subsequent scan sees no
     * trace of them.
     *
     * @return Status indicating success or failure
     */
    [[nodiscard]] Status rollback();

    /**
     * @brief Check if the calling thread has an active transaction
     */
    [[nodiscard]] bool in_transaction() const noexcept;

    /**
     * @brief Close the database (flushes WAL and all dirty pages)
     * @return Ok on a clean shutdown; the first flush error otherwise. Not
     *         [[nodiscard]] so existing best-effort callers compile unchanged,
     *         but durability-sensitive callers should check it.
     */
    Status close();

    /**
     * @brief Check if the database is open
     */
    [[nodiscard]] bool is_open() const noexcept;

    /**
     * @brief Get the path to the database file
     */
    [[nodiscard]] std::string_view path() const noexcept;

    /// Test-only seam: exposes the internal Catalog so tests can create indexes
    /// (CREATE INDEX is not yet parseable). Not part of the stable public API.
    [[nodiscard]] Catalog* catalog_for_testing() noexcept;

private:
    std::unique_ptr<DatabaseImpl> impl_;
};

}  // namespace entropy
