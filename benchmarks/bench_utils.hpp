#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <random>
#include <sstream>
#include <string>
#include <string_view>

namespace entropy::bench {

inline std::string make_temp_path(std::string_view prefix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    std::mt19937_64 rng(static_cast<uint64_t>(now));
    std::uniform_int_distribution<uint64_t> dist;

    std::ostringstream name;
    name << prefix << "_" << std::hex << static_cast<uint64_t>(now) << dist(rng)
         << ".entropy";
    return (base / name.str()).string();
}

inline void remove_temp_files(const std::string &path) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
    std::filesystem::remove(path + ".wal", ec);
    std::filesystem::remove(path + ".catalog", ec);
    std::filesystem::remove(path + "-wal", ec);
    std::filesystem::remove(path + "-shm", ec);
    std::filesystem::remove(path + "-journal", ec);
}

class TempDbFile {
public:
    explicit TempDbFile(std::string_view prefix) : path_(make_temp_path(prefix)) {}

    ~TempDbFile() { remove_temp_files(path_); }

    const std::string &path() const noexcept { return path_; }

private:
    std::string path_;
};

}  // namespace entropy::bench
