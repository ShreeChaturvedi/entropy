# ─────────────────────────────────────────────────────────────────────────────
# External Dependencies Configuration
# ─────────────────────────────────────────────────────────────────────────────

include(FetchContent)

# Set download directory for FetchContent
set(FETCHCONTENT_BASE_DIR "${CMAKE_BINARY_DIR}/_deps")

# ─────────────────────────────────────────────────────────────────────────────
# spdlog - Fast C++ logging library
# ─────────────────────────────────────────────────────────────────────────────

FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.12.0
    GIT_SHALLOW    TRUE
)

set(SPDLOG_BUILD_EXAMPLE OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SPDLOG_INSTALL OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(spdlog)

# ─────────────────────────────────────────────────────────────────────────────
# Google Test - Testing framework
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_BUILD_TESTS)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.14.0
        GIT_SHALLOW    TRUE
    )

    # For Windows: Prevent overriding the parent project's compiler/linker settings
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    set(BUILD_GMOCK ON CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)

    FetchContent_MakeAvailable(googletest)

    # Disable warnings for googletest
    if(TARGET gtest)
        entropy_disable_warnings(gtest)
    endif()
    if(TARGET gmock)
        entropy_disable_warnings(gmock)
    endif()
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Google Benchmark - Micro-benchmarking library
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_BUILD_BENCHMARKS)
    FetchContent_Declare(
        benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG        v1.8.3
        GIT_SHALLOW    TRUE
    )

    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_INSTALL_DOCS OFF CACHE BOOL "" FORCE)

    FetchContent_MakeAvailable(benchmark)

    if(TARGET benchmark)
        entropy_disable_warnings(benchmark)
    endif()
endif()

# ─────────────────────────────────────────────────────────────────────────────
# sql-parser (hsql) - SQL parsing library
# ─────────────────────────────────────────────────────────────────────────────

FetchContent_Declare(
    sqlparser
    GIT_REPOSITORY https://github.com/hyrise/sql-parser.git
    GIT_TAG        master
    GIT_SHALLOW    TRUE
)

# Note: hsql uses a different build approach, we'll handle it separately
# For now, we'll set up the declaration - implementation can be adjusted later

# ─────────────────────────────────────────────────────────────────────────────
# LZ4 - Fast compression library (optional)
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_ENABLE_LZ4)
    find_package(lz4 QUIET)

    if(NOT lz4_FOUND)
        FetchContent_Declare(
            lz4
            GIT_REPOSITORY https://github.com/lz4/lz4.git
            GIT_TAG        v1.9.4
            GIT_SHALLOW    TRUE
            SOURCE_SUBDIR  build/cmake
        )

        set(LZ4_BUILD_CLI OFF CACHE BOOL "" FORCE)
        set(LZ4_BUILD_LEGACY_LZ4C OFF CACHE BOOL "" FORCE)

        FetchContent_MakeAvailable(lz4)
    endif()

    set(entropy_lz4_target "")
    if(TARGET LZ4::LZ4)
        set(entropy_lz4_target LZ4::LZ4)
    elseif(TARGET lz4::lz4)
        set(entropy_lz4_target lz4::lz4)
    elseif(TARGET lz4_static)
        set(entropy_lz4_target lz4_static)
    elseif(TARGET lz4_shared)
        set(entropy_lz4_target lz4_shared)
    elseif(TARGET lz4)
        set(entropy_lz4_target lz4)
    endif()

    if(entropy_lz4_target)
        if(NOT TARGET entropy_lz4)
            add_library(entropy_lz4 INTERFACE)
        endif()
        target_link_libraries(entropy_lz4 INTERFACE ${entropy_lz4_target})
    else()
        message(FATAL_ERROR "LZ4 enabled but no CMake target was found.")
    endif()

    add_compile_definitions(ENTROPY_ENABLE_COMPRESSION)
endif()

# ─────────────────────────────────────────────────────────────────────────────
# SQLite3 - Optional (benchmark comparison)
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_BUILD_BENCHMARKS AND ENTROPY_BENCH_COMPARE_SQLITE)
    find_package(SQLite3 QUIET)

    if(SQLite3_FOUND)
        if(NOT TARGET entropy_sqlite)
            add_library(entropy_sqlite INTERFACE)
            target_include_directories(entropy_sqlite INTERFACE ${SQLite3_INCLUDE_DIRS})
            target_link_libraries(entropy_sqlite INTERFACE ${SQLite3_LIBRARIES})
            target_compile_definitions(entropy_sqlite INTERFACE ENTROPY_BENCH_HAS_SQLITE)
        endif()
    else()
        message(WARNING "SQLite3 not found; SQLite comparison benchmarks will be skipped.")
    endif()
endif()
