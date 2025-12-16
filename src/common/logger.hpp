#pragma once

/**
 * @file logger.hpp
 * @brief Logging utilities for Entropy
 */

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <memory>
#include <string>

namespace entropy {

/**
 * @brief Logger wrapper for Entropy
 */
class Logger {
public:
    /**
     * @brief Initialize the logging system
     * @param name Logger name
     * @param level Log level (trace, debug, info, warn, error, critical)
     */
    static void init(const std::string& name = "entropy",
                     spdlog::level::level_enum level = spdlog::level::info);

    /**
     * @brief Get the logger instance
     */
    static std::shared_ptr<spdlog::logger>& get();

    /**
     * @brief Set the log level
     */
    static void set_level(spdlog::level::level_enum level);

    /**
     * @brief Shutdown the logging system
     */
    static void shutdown();

private:
    static std::shared_ptr<spdlog::logger> logger_;
};

// Convenience macros for logging
#define LOG_TRACE(...)    SPDLOG_LOGGER_TRACE(entropy::Logger::get(), __VA_ARGS__)
#define LOG_DEBUG(...)    SPDLOG_LOGGER_DEBUG(entropy::Logger::get(), __VA_ARGS__)
#define LOG_INFO(...)     SPDLOG_LOGGER_INFO(entropy::Logger::get(), __VA_ARGS__)
#define LOG_WARN(...)     SPDLOG_LOGGER_WARN(entropy::Logger::get(), __VA_ARGS__)
#define LOG_ERROR(...)    SPDLOG_LOGGER_ERROR(entropy::Logger::get(), __VA_ARGS__)
#define LOG_CRITICAL(...) SPDLOG_LOGGER_CRITICAL(entropy::Logger::get(), __VA_ARGS__)

}  // namespace entropy
