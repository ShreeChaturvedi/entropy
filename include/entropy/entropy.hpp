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
#include "entropy/version.hpp"

namespace entropy {

/**
 * @brief Get the version string of Entropy
 * @return Version string in format "major.minor.patch"
 */
constexpr const char* version() noexcept {
    return kVersionString;
}

/**
 * @brief Get the major version number
 */
constexpr int version_major() noexcept {
    return kVersionMajor;
}

/**
 * @brief Get the minor version number
 */
constexpr int version_minor() noexcept {
    return kVersionMinor;
}

/**
 * @brief Get the patch version number
 */
constexpr int version_patch() noexcept {
    return kVersionPatch;
}

}  // namespace entropy
