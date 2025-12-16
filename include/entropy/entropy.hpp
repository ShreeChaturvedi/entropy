#pragma once

/**
 * @file entropy.hpp
 * @brief Main include header for Entropy Database Engine
 *
 * Include this single header to access the public API of Entropy.
 */

#include "entropy/database.hpp"
#include "entropy/result.hpp"
#include "entropy/status.hpp"

namespace entropy {

/**
 * @brief Get the version string of Entropy
 * @return Version string in format "major.minor.patch"
 */
constexpr const char* version() noexcept {
    return "0.1.0";
}

/**
 * @brief Get the major version number
 */
constexpr int version_major() noexcept {
    return 0;
}

/**
 * @brief Get the minor version number
 */
constexpr int version_minor() noexcept {
    return 1;
}

/**
 * @brief Get the patch version number
 */
constexpr int version_patch() noexcept {
    return 0;
}

}  // namespace entropy
