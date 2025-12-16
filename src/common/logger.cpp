/**
 * @file logger.cpp
 * @brief Logger implementation
 */

#include "common/logger.hpp"

namespace entropy {

std::shared_ptr<spdlog::logger> Logger::logger_ = nullptr;

void Logger::init(const std::string& name, spdlog::level::level_enum level) {
    if (logger_ == nullptr) {
        logger_ = spdlog::stdout_color_mt(name);
        logger_->set_level(level);
        logger_->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%s:%#] %v");
    }
}

std::shared_ptr<spdlog::logger>& Logger::get() {
    if (logger_ == nullptr) {
        init();
    }
    return logger_;
}

void Logger::set_level(spdlog::level::level_enum level) {
    if (logger_ != nullptr) {
        logger_->set_level(level);
    }
}

void Logger::shutdown() {
    if (logger_ != nullptr) {
        spdlog::drop(logger_->name());
        logger_.reset();
    }
}

}  // namespace entropy
