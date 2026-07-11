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
 * @brief Resolve the version of @p heap_tuple visible to @p ctx's snapshot
 *
 * When @p ctx has a transaction and a version store, consults the store's
 * per-RID chain and returns the tuple the snapshot is entitled to see (the heap
 * bytes for the live head, or a retained before-image for an older snapshot),
 * or nullopt when the row is invisible (created after the snapshot, or deleted
 * from its view). With no context, no transaction, or no version store, returns
 * @p heap_tuple unchanged — the behaviour executor unit tests rely on when they
 * drive an executor directly against a table heap.
 */
[[nodiscard]] std::optional<Tuple> mvcc_visible(const ExecutorContext *ctx,
                                                const Tuple &heap_tuple);

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
