#pragma once

#ifdef ENTROPY_BENCH_HAS_SQLITE

#include <sqlite3.h>

#include <string>

namespace entropy::bench {

class SqliteDb {
public:
    explicit SqliteDb(const std::string &path) {
        if (sqlite3_open(path.c_str(), &db_) != SQLITE_OK) {
            db_ = nullptr;
            return;
        }

        // Match Entropy's measured commit path, which is non-durable: its
        // Database::begin_transaction()/commit() are in-memory state flips that
        // neither append to the WAL nor fsync (rows reach disk only via
        // flush_all_pages() at teardown, after timing has stopped). SQLite
        // otherwise defaults to synchronous=FULL, paying a real fsync on every
        // commit, so without these pragmas the comparison would charge SQLite
        // for durability that Entropy never provides on this path.
        // synchronous=OFF drops the per-commit fsync; journal_mode=MEMORY keeps
        // the rollback journal off disk. This makes the insert benchmark an
        // apples-to-apples, no-durability-guarantee comparison on both sides.
        sqlite3_exec(db_, "PRAGMA journal_mode=MEMORY;", nullptr, nullptr,
                     nullptr);
        sqlite3_exec(db_, "PRAGMA synchronous=OFF;", nullptr, nullptr, nullptr);
    }

    ~SqliteDb() {
        if (db_ != nullptr) {
            sqlite3_close(db_);
            db_ = nullptr;
        }
    }

    bool ok() const noexcept { return db_ != nullptr; }

    bool exec(const std::string &sql, std::string *error) {
        if (db_ == nullptr) {
            if (error != nullptr) {
                *error = "sqlite3_open failed";
            }
            return false;
        }

        char *errmsg = nullptr;
        const int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &errmsg);
        if (rc != SQLITE_OK) {
            if (error != nullptr) {
                *error = errmsg != nullptr ? errmsg : "sqlite3_exec failed";
            }
            if (errmsg != nullptr) {
                sqlite3_free(errmsg);
            }
            return false;
        }

        return true;
    }

private:
    sqlite3 *db_ = nullptr;
};

}  // namespace entropy::bench

#endif
