/**
 * @file executor.cpp
 * @brief Executor helpers shared across the execution engine
 */

#include "execution/executor.hpp"

#include "execution/executor_context.hpp"
#include "transaction/version_store.hpp"

namespace entropy {

std::optional<Tuple> mvcc_visible(const ExecutorContext *ctx,
                                  const Tuple &heap_tuple) {
  // No transaction context: yield the latest (heap) version unchanged. This is
  // the path executor unit tests take when constructing executors with a null
  // context, and the path any read takes when MVCC is not wired.
  if (ctx == nullptr || ctx->txn == nullptr || ctx->version_store == nullptr) {
    return heap_tuple;
  }

  auto visible = ctx->version_store->read_visible(heap_tuple.rid(),
                                                  heap_tuple.as_span(), ctx->txn);
  if (!visible.has_value()) {
    return std::nullopt; // invisible to this snapshot
  }
  // read_visible returns owned bytes (a before-image or a copy of the heap
  // bytes), so wrap them in a tuple carrying the same RID.
  return Tuple(std::move(*visible), heap_tuple.rid());
}

} // namespace entropy
