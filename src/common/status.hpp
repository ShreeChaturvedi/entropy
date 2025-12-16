#pragma once

/**
 * @file status.hpp
 * @brief Internal status implementation
 *
 * This file re-exports the public status.hpp and adds internal utilities.
 */

#include "entropy/status.hpp"

namespace entropy {

/**
 * @brief Macro to return early if status is not OK
 */
#define ENTROPY_RETURN_IF_ERROR(expr)   \
    do {                                \
        auto _status = (expr);          \
        if (!_status.ok()) {            \
            return _status;             \
        }                               \
    } while (false)

/**
 * @brief Macro to assign or return error
 */
#define ENTROPY_ASSIGN_OR_RETURN(lhs, expr) \
    auto _result = (expr);                  \
    if (!_result.ok()) {                    \
        return _result.status();            \
    }                                       \
    lhs = std::move(_result.value())

}  // namespace entropy
