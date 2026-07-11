#pragma once

/**
 * @file test_utils.hpp
 * @brief Common test utilities
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <random>
#include <string>

namespace entropy {
namespace test {

namespace detail {

// A process-globally-unique path component. A high-resolution clock reading, a
// process-lifetime counter, and a random draw together make collisions
// effectively impossible even across the separate processes a parallel ctest
// runner spawns — the same recipe the concurrent storage fixtures use. This
// matters because a colliding path would let one test open another's leftover
// on-disk state: an engine opens sidecar files next to its main file (`.wal`,
// `.catalog`), so a fresh handle landing on a stale path would replay foreign
// committed rows into a supposedly-empty database.
inline std::string unique_token(const std::string& prefix) {
    static std::atomic<uint64_t> counter{0};
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    std::random_device rd;
    return prefix + std::to_string(stamp) + "_" +
           std::to_string(counter.fetch_add(1)) + "_" + std::to_string(rd());
}

}  // namespace detail

/**
 * @brief RAII helper for a temporary test file path.
 *
 * The returned path lives inside a private directory this object owns, so every
 * sidecar the engine writes alongside it (`.wal`, `.catalog`, …) is contained
 * there and removed wholesale on destruction — no file is leaked and no later
 * test can inherit stale on-disk state through a path collision.
 */
class TempFile {
public:
    explicit TempFile(const std::string& prefix = "entropy_test_") {
        dir_ = std::filesystem::temp_directory_path() /
               (detail::unique_token(prefix) + ".d");
        std::error_code ec;
        std::filesystem::create_directories(dir_, ec);
        // A fixed leaf name inside the unique directory: uniqueness comes from
        // the directory, so the file (and its sidecars) can be reasoned about
        // by name while staying fully isolated.
        path_ = dir_ / "store";
    }

    ~TempFile() {
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    TempFile(const TempFile&) = delete;
    TempFile& operator=(const TempFile&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

    [[nodiscard]] std::string string() const {
        return path_.string();
    }

private:
    std::filesystem::path dir_;
    std::filesystem::path path_;
};

/**
 * @brief RAII helper for temporary test directory
 */
class TempDir {
public:
    explicit TempDir(const std::string& prefix = "entropy_test_") {
        path_ = std::filesystem::temp_directory_path() /
                detail::unique_token(prefix);
        std::error_code ec;
        std::filesystem::create_directories(path_, ec);
    }

    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

    [[nodiscard]] std::string string() const {
        return path_.string();
    }

private:
    std::filesystem::path path_;
};

}  // namespace test
}  // namespace entropy
