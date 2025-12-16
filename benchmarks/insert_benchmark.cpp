/**
 * @file insert_benchmark.cpp
 * @brief Benchmark for insert operations
 */

#include <benchmark/benchmark.h>

#include <entropy/entropy.hpp>

static void BM_Placeholder(benchmark::State& state) {
    for (auto _ : state) {
        // TODO: Implement insert benchmark
        benchmark::DoNotOptimize(0);
    }
}

BENCHMARK(BM_Placeholder);
