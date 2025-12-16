#pragma once

/**
 * @file test_utils.hpp
 * @brief Common test utilities
 */

#include <filesystem>
#include <random>
#include <string>

namespace entropy {
namespace test {

/**
 * @brief RAII helper for temporary test files
 */
class TempFile {
public:
    explicit TempFile(const std::string& prefix = "entropy_test_") {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 999999);

        path_ = std::filesystem::temp_directory_path() /
                (prefix + std::to_string(dis(gen)));
    }

    ~TempFile() {
        if (std::filesystem::exists(path_)) {
            std::filesystem::remove_all(path_);
        }
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

    [[nodiscard]] std::string string() const {
        return path_.string();
    }

private:
    std::filesystem::path path_;
};

/**
 * @brief RAII helper for temporary test directory
 */
class TempDir {
public:
    explicit TempDir(const std::string& prefix = "entropy_test_") {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 999999);

        path_ = std::filesystem::temp_directory_path() /
                (prefix + std::to_string(dis(gen)));
        std::filesystem::create_directories(path_);
    }

    ~TempDir() {
        if (std::filesystem::exists(path_)) {
            std::filesystem::remove_all(path_);
        }
    }

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
