/**
 * @file query_benchmark.cpp
 * @brief Benchmarks for query operations
 */

#include <benchmark/benchmark.h>

#include <cstdint>
#include <string>

#include <entropy/entropy.hpp>

#include "bench_utils.hpp"
#include "sqlite_utils.hpp"

namespace {

void SkipWithStatus(benchmark::State &state, const entropy::Status &status) {
    const std::string message = status.to_string();
    state.SkipWithError(message.c_str());
}

void SkipWithResult(benchmark::State &state, const entropy::Result &result) {
    const std::string message = result.status().to_string();
    state.SkipWithError(message.c_str());
}

static void BM_Entropy_PointSelect(benchmark::State &state) {
    const int64_t rows = state.range(0);

    entropy::bench::TempDbFile db_file("entropy_query");
    entropy::Database db(db_file.path());

    auto result = db.execute("CREATE TABLE bench (id INTEGER, value INTEGER)");
    if (!result.ok()) {
        SkipWithResult(state, result);
        return;
    }

    auto status = db.begin_transaction();
    if (!status.ok()) {
        SkipWithStatus(state, status);
        return;
    }

    for (int64_t i = 0; i < rows; ++i) {
        const std::string sql =
            "INSERT INTO bench VALUES (" + std::to_string(i) + ", " +
            std::to_string(i) + ")";
        result = db.execute(sql);
        if (!result.ok()) {
            SkipWithResult(state, result);
            return;
        }
    }

    status = db.commit();
    if (!status.ok()) {
        SkipWithStatus(state, status);
        return;
    }

    const int64_t target = rows > 0 ? rows / 2 : 0;
    const std::string query =
        "SELECT value FROM bench WHERE id = " + std::to_string(target);

    for (auto _ : state) {
        result = db.execute(query);
        if (!result.ok()) {
            SkipWithResult(state, result);
            return;
        }
        benchmark::DoNotOptimize(result.row_count());
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_Entropy_PointSelect)->Arg(1000)->Arg(10000);

#ifdef ENTROPY_BENCH_HAS_SQLITE
static void BM_Sqlite_PointSelect(benchmark::State &state) {
    const int64_t rows = state.range(0);

    entropy::bench::TempDbFile db_file("sqlite_query");
    entropy::bench::SqliteDb db(db_file.path());
    if (!db.ok()) {
        state.SkipWithError("sqlite3_open failed");
        return;
    }

    std::string error;
    if (!db.exec("CREATE TABLE bench (id INTEGER, value INTEGER);", &error)) {
        state.SkipWithError(error.c_str());
        return;
    }
    if (!db.exec("BEGIN;", &error)) {
        state.SkipWithError(error.c_str());
        return;
    }

    for (int64_t i = 0; i < rows; ++i) {
        const std::string sql =
            "INSERT INTO bench VALUES (" + std::to_string(i) + ", " +
            std::to_string(i) + ");";
        if (!db.exec(sql, &error)) {
            state.SkipWithError(error.c_str());
            return;
        }
    }

    if (!db.exec("COMMIT;", &error)) {
        state.SkipWithError(error.c_str());
        return;
    }

    const int64_t target = rows > 0 ? rows / 2 : 0;
    const std::string query =
        "SELECT value FROM bench WHERE id = " + std::to_string(target) + ";";

    for (auto _ : state) {
        if (!db.exec(query, &error)) {
            state.SkipWithError(error.c_str());
            return;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_Sqlite_PointSelect)->Arg(1000)->Arg(10000);
#endif

}  // namespace
