#pragma once

/**
 * @file executor_context.hpp
 * @brief Per-statement transaction context handed to executors
 *
 * ExecutorContext is the seam that injects transaction state into the execution
 * engine. It is created per statement by the Database API and threaded into the
 * leaf executors (scans do MVCC-visible reads; INSERT/UPDATE/DELETE acquire row
 * locks, record versions, and log to the WAL). A null context — or a context
 * whose @c txn is null — restores the pre-transactional behaviour used by the
 * executor unit tests that drive executors directly against a table heap.
 */

namespace entropy {

// Forward declarations — this header is a lightweight seam, so it pulls in no
// transaction/catalog headers. Executors that use the members include the
// concrete headers in their translation units.
class Transaction;
class TransactionManager;
class LockManager;
class VersionStore;
class Catalog;

/**
 * @brief Transaction handles an executor needs to enforce SI + WAL
 *
 * All pointers are non-owning; the Database owns the referenced objects and
 * keeps them alive for the duration of the statement. Any subset may be null:
 * executors guard each use, so a context with only @c txn set (no lock manager,
 * say) simply skips that concern.
 */
struct ExecutorContext {
  Transaction *txn = nullptr;              ///< The statement's transaction
  TransactionManager *txn_mgr = nullptr;   ///< For WAL logging of writes
  LockManager *lock_mgr = nullptr;         ///< For row-level exclusive locks
  VersionStore *version_store = nullptr;   ///< MVCC chains + conflict detection
  Catalog *catalog = nullptr;              ///< Metadata resolution
};

} // namespace entropy
