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
 * @brief True only when a predicate evaluated to boolean true.
 *
 * A NULL or non-boolean predicate value is treated as false (SQL three-valued
 * logic), so callers never invoke as_bool() on a non-bool variant and trigger
 * std::bad_variant_access. Shared by the executors that apply a WHERE/JOIN
 * predicate (filter, seq scan, nested loop join) so the guard stays consistent.
 */
inline bool predicate_is_true(const TupleValue& value) {
    return value.is_bool() && value.as_bool();
}

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
