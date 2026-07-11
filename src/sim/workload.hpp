#pragma once

/**
 * @file workload.hpp
 * @brief Seeded transactional workload, its oracle, and the invariant checker.
 *
 * The workload drives begin/insert/update/delete/commit/abort against the real
 * TransactionManager + TableHeap + VersionStore stack, using high-entropy,
 * fixed-width payloads. Fixed width keeps updates in place (stable RIDs), and a
 * unique per-row prefix guarantees every row's bytes are distinct so recovery's
 * prefix-based undo-UPDATE residual (identical bytes reused at a freed RID)
 * cannot fire and be misattributed to the harness.
 *
 * The Workload interface is deliberately narrow so a post-#23 public-API driver
 * can replace RandomWorkload without touching the schedule engine.
 */

#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "catalog/schema.hpp"
#include "common/types.hpp"

namespace entropy {
class TransactionManager;
class TableHeap;
class VersionStore;
}  // namespace entropy

namespace entropy::sim {

/// Ground-truth committed state the harness expects to observe after recovery.
class Oracle {
public:
  void commit_insert(RID rid, std::vector<char> bytes) {
    deleted_.erase(rid);
    loser_.erase(rid);
    committed_[rid] = std::move(bytes);
  }
  void commit_update(RID rid, std::vector<char> bytes) {
    committed_[rid] = std::move(bytes);
  }
  void commit_delete(RID rid) {
    committed_.erase(rid);
    deleted_.insert(rid);
  }
  /// A RID written only by a not-yet-committed (loser) transaction.
  void note_loser(RID rid) {
    if (committed_.find(rid) == committed_.end()) {
      loser_.insert(rid);
    }
  }

  [[nodiscard]] const std::unordered_map<RID, std::vector<char>> &committed()
      const noexcept {
    return committed_;
  }
  [[nodiscard]] const std::unordered_set<RID> &deleted() const noexcept {
    return deleted_;
  }
  [[nodiscard]] const std::unordered_set<RID> &losers() const noexcept {
    return loser_;
  }

private:
  std::unordered_map<RID, std::vector<char>> committed_;
  std::unordered_set<RID> deleted_;  // committed-deleted RIDs
  std::unordered_set<RID> loser_;    // RIDs touched only by loser txns
};

/// The assembled engine stack a workload drives.
struct WorkloadContext {
  TransactionManager *tm = nullptr;
  TableHeap *heap = nullptr;
  VersionStore *version_store = nullptr;
  const Schema *schema = nullptr;
  oid_t table_oid = 1;
};

/// Abstract seeded workload. Slot a public-API driver in here later.
class Workload {
public:
  virtual ~Workload() = default;
  [[nodiscard]] virtual const char *name() const = 0;

  /// Run @p num_txns transactions against @p ctx, recording committed effects
  /// in @p oracle. When @p leave_in_flight is set, the final transaction is
  /// left open (a crash loser); its RIDs are noted as losers in the oracle.
  /// Returns the number of logical data operations performed.
  virtual size_t run(const WorkloadContext &ctx, std::mt19937_64 &rng,
                     Oracle &oracle, size_t num_txns, bool leave_in_flight) = 0;
};

/// Default randomized OLTP-ish workload.
class RandomWorkload : public Workload {
public:
  /// @param abort_ppk chance (parts per thousand) a transaction aborts instead
  ///        of committing. Kept 0 for schedules that must survive recovery:
  ///        recovery has no CLRs and never gates redo by page LSN, so it
  ///        resurrects rows from transactions aborted during normal operation
  ///        (tracked engine limitation; see the live-abort repro schedule).
  explicit RandomWorkload(uint32_t abort_ppk = 0) : abort_ppk_(abort_ppk) {}

  [[nodiscard]] const char *name() const override { return "random"; }
  size_t run(const WorkloadContext &ctx, std::mt19937_64 &rng, Oracle &oracle,
             size_t num_txns, bool leave_in_flight) override;

private:
  uint32_t abort_ppk_;
  uint64_t next_row_id_ = 1;  // monotonic, guarantees distinct row payloads
};

/// A schema shared across simulator runs: (id INTEGER, name VARCHAR(64)). The
/// workload always fills name to a constant width so tuples are fixed-size.
[[nodiscard]] Schema make_sim_schema();

// ── Invariant checking ─────────────────────────────────────────────────────

/// Check post-recovery invariants against the oracle, reading committed rows
/// back through @p recovered.heap's buffer pool. Appends the name of each
/// violated invariant to @p failures (empty vector means all invariants hold).
void check_invariants(const WorkloadContext &recovered, const Oracle &oracle,
                      std::vector<std::string> &failures);

}  // namespace entropy::sim
