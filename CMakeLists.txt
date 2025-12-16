cmake_minimum_required(VERSION 3.20)

project(entropy
    VERSION 0.1.0
    DESCRIPTION "High-performance relational database engine"
    LANGUAGES CXX
)

# ─────────────────────────────────────────────────────────────────────────────
# Global Settings
# ─────────────────────────────────────────────────────────────────────────────

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Export compile commands for IDE support
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Default to Release build if not specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'Release' as none was specified.")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Options
# ─────────────────────────────────────────────────────────────────────────────

option(ENTROPY_BUILD_TESTS "Build unit tests" ON)
option(ENTROPY_BUILD_BENCHMARKS "Build performance benchmarks" OFF)
option(ENTROPY_BUILD_EXAMPLES "Build example programs" ON)
option(ENTROPY_ENABLE_LZ4 "Enable LZ4 page compression" OFF)
option(ENTROPY_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENTROPY_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

# ─────────────────────────────────────────────────────────────────────────────
# CMake Modules
# ─────────────────────────────────────────────────────────────────────────────

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(CompilerWarnings)
include(Dependencies)

# ─────────────────────────────────────────────────────────────────────────────
# Compiler Flags
# ─────────────────────────────────────────────────────────────────────────────

# Apply compiler warnings
entropy_set_compiler_warnings()

# Sanitizers
if(ENTROPY_ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

if(ENTROPY_ENABLE_TSAN)
    add_compile_options(-fsanitize=thread)
    add_link_options(-fsanitize=thread)
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Source Directories
# ─────────────────────────────────────────────────────────────────────────────

# Public headers
set(ENTROPY_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

# Add source directory
add_subdirectory(src)

# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_BUILD_TESTS)
    enable_testing()
    include(CTest)
    add_subdirectory(tests)
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Benchmarks
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Examples
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Installation
# ─────────────────────────────────────────────────────────────────────────────

include(GNUInstallDirs)

install(
    DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

message(STATUS "")
message(STATUS "Entropy Database Engine v${PROJECT_VERSION}")
message(STATUS "─────────────────────────────────────────────")
message(STATUS "Build type:        ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Compiler:      ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "C++ Standard:      ${CMAKE_CXX_STANDARD}")
message(STATUS "")
message(STATUS "Options:")
message(STATUS "  Tests:           ${ENTROPY_BUILD_TESTS}")
message(STATUS "  Benchmarks:      ${ENTROPY_BUILD_BENCHMARKS}")
message(STATUS "  Examples:        ${ENTROPY_BUILD_EXAMPLES}")
message(STATUS "  LZ4 Compression: ${ENTROPY_ENABLE_LZ4}")
message(STATUS "  AddressSanitizer:${ENTROPY_ENABLE_ASAN}")
message(STATUS "  ThreadSanitizer: ${ENTROPY_ENABLE_TSAN}")
message(STATUS "")
