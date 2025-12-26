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
        }
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
