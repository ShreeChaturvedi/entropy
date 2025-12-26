/**
 * @file insert_benchmark.cpp
 * @brief Benchmarks for insert operations
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

static void BM_Entropy_InsertBatch(benchmark::State &state) {
    const int64_t rows = state.range(0);

    for (auto _ : state) {
        state.PauseTiming();
        {
            entropy::bench::TempDbFile db_file("entropy_insert");
            entropy::Database db(db_file.path());

            auto result =
                db.execute("CREATE TABLE bench (id INTEGER, value INTEGER)");
            if (!result.ok()) {
                SkipWithResult(state, result);
                return;
            }

            auto status = db.begin_transaction();
            if (!status.ok()) {
                SkipWithStatus(state, status);
                return;
            }
            state.ResumeTiming();

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

            state.PauseTiming();
        }

        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * rows);
}

BENCHMARK(BM_Entropy_InsertBatch)->Arg(1000)->Arg(10000);

#ifdef ENTROPY_BENCH_HAS_SQLITE
static void BM_Sqlite_InsertBatch(benchmark::State &state) {
    const int64_t rows = state.range(0);

    for (auto _ : state) {
        state.PauseTiming();
        {
            entropy::bench::TempDbFile db_file("sqlite_insert");
            entropy::bench::SqliteDb db(db_file.path());
            if (!db.ok()) {
                state.SkipWithError("sqlite3_open failed");
                return;
            }

            std::string error;
            if (!db.exec("CREATE TABLE bench (id INTEGER, value INTEGER);",
                         &error)) {
                state.SkipWithError(error.c_str());
                return;
            }
            if (!db.exec("BEGIN;", &error)) {
                state.SkipWithError(error.c_str());
                return;
            }
            state.ResumeTiming();

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

            state.PauseTiming();
        }

        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * rows);
}

BENCHMARK(BM_Sqlite_InsertBatch)->Arg(1000)->Arg(10000);
#endif

}  // namespace
