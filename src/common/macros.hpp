#pragma once

/**
 * @file macros.hpp
 * @brief Utility macros for Entropy
 */

#include <cassert>
#include <cstdlib>
#include <iostream>

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Assertion Macros
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Assert that a condition is true (debug builds only)
 */
#ifdef NDEBUG
#define ENTROPY_ASSERT(condition, message) ((void)0)
#else
#define ENTROPY_ASSERT(condition, message)                                    \
    do {                                                                      \
        if (!(condition)) {                                                   \
            std::cerr << "Assertion failed: " << #condition << "\n"           \
                      << "Message: " << (message) << "\n"                     \
                      << "File: " << __FILE__ << "\n"                         \
                      << "Line: " << __LINE__ << std::endl;                   \
            std::abort();                                                     \
        }                                                                     \
    } while (false)
#endif

/**
 * @brief Assert that should always run (even in release)
 */
#define ENTROPY_CHECK(condition, message)                                     \
    do {                                                                      \
        if (!(condition)) {                                                   \
            std::cerr << "Check failed: " << #condition << "\n"               \
                      << "Message: " << (message) << "\n"                     \
                      << "File: " << __FILE__ << "\n"                         \
                      << "Line: " << __LINE__ << std::endl;                   \
            std::abort();                                                     \
        }                                                                     \
    } while (false)

/**
 * @brief Mark code as unreachable
 */
#define ENTROPY_UNREACHABLE()                                                 \
    do {                                                                      \
        std::cerr << "Unreachable code reached\n"                             \
                  << "File: " << __FILE__ << "\n"                             \
                  << "Line: " << __LINE__ << std::endl;                       \
        std::abort();                                                         \
    } while (false)

/**
 * @brief Mark code as not implemented
 */
#define ENTROPY_NOT_IMPLEMENTED()                                             \
    do {                                                                      \
        std::cerr << "Not implemented\n"                                      \
                  << "File: " << __FILE__ << "\n"                             \
                  << "Line: " << __LINE__ << std::endl;                       \
        std::abort();                                                         \
    } while (false)

// ─────────────────────────────────────────────────────────────────────────────
// Utility Macros
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Disable copy constructor and assignment
 */
#define ENTROPY_DISALLOW_COPY(ClassName)           \
    ClassName(const ClassName&) = delete;          \
    ClassName& operator=(const ClassName&) = delete

/**
 * @brief Disable move constructor and assignment
 */
#define ENTROPY_DISALLOW_MOVE(ClassName)           \
    ClassName(ClassName&&) = delete;               \
    ClassName& operator=(ClassName&&) = delete

/**
 * @brief Disable copy and move
 */
#define ENTROPY_DISALLOW_COPY_AND_MOVE(ClassName)  \
    ENTROPY_DISALLOW_COPY(ClassName);              \
    ENTROPY_DISALLOW_MOVE(ClassName)

/**
 * @brief Default move operations
 */
#define ENTROPY_DEFAULT_MOVE(ClassName)            \
    ClassName(ClassName&&) = default;              \
    ClassName& operator=(ClassName&&) = default

/**
 * @brief Prevent the compiler from inlining a function.
 *
 * Used where a function must remain a distinct stack frame regardless of the
 * optimizer's inlining decisions — e.g. so a ThreadSanitizer suppression that
 * matches on that frame's symbol keeps matching under -O2 (see
 * .config/tsan-suppressions.txt). Expands to nothing on unknown compilers, so
 * it never changes semantics, only frame layout.
 */
#if defined(_MSC_VER)
#define ENTROPY_NOINLINE __declspec(noinline)
#elif defined(__GNUC__) || defined(__clang__)
#define ENTROPY_NOINLINE __attribute__((noinline))
#else
#define ENTROPY_NOINLINE
#endif

/**
 * @brief Concatenate tokens
 */
#define ENTROPY_CONCAT_IMPL(a, b) a##b
#define ENTROPY_CONCAT(a, b) ENTROPY_CONCAT_IMPL(a, b)

/**
 * @brief Generate unique variable name
 */
#define ENTROPY_UNIQUE_NAME(prefix) ENTROPY_CONCAT(prefix, __LINE__)

}  // namespace entropy
