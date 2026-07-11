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
 * @brief Bind @p lhs to a successful Result's rows, or early-return its Status.
 *
 * Evaluates @p expr once (it must yield a Result). On failure the enclosing
 * function returns the Result's Status; on success @p lhs is assigned the
 * result rows. The enclosing function's return type must be constructible from
 * Status. Declares `_result` in the caller's scope, so use at most once per
 * scope.
 */
#define ENTROPY_ASSIGN_OR_RETURN(lhs, expr) \
    auto _result = (expr);                  \
    if (!_result.ok()) {                    \
        return _result.status();            \
    }                                       \
    lhs = _result.rows()

}  // namespace entropy
