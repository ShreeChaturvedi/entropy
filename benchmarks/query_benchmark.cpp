/**
 * @file query_benchmark.cpp
 * @brief Benchmark for query operations
 */

#include <benchmark/benchmark.h>

#include <entropy/entropy.hpp>

static void BM_Placeholder(benchmark::State& state) {
    for (auto _ : state) {
        // TODO: Implement query benchmark
        benchmark::DoNotOptimize(0);
    }
}

BENCHMARK(BM_Placeholder);
