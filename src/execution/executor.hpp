#pragma once

/**
 * @file executor.hpp
 * @brief Executor interface
 */

#include <memory>
#include <optional>

#include "storage/tuple.hpp"

namespace entropy {

class ExecutorContext;

/**
 * @brief Base class for all executors
 */
class Executor {
public:
    explicit Executor(ExecutorContext* ctx) : ctx_(ctx) {}
    virtual ~Executor() = default;

    virtual void init() = 0;
    virtual std::optional<Tuple> next() = 0;

protected:
    ExecutorContext* ctx_;
};

}  // namespace entropy
