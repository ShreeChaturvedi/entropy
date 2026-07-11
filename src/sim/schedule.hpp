#pragma once

/**
 * @file schedule.hpp
 * @brief Replayable crash schedules and the engine that runs them.
 *
 * A Schedule is {seed-independent knobs + fault probabilities}. Running it for
 * a seed assembles the full transaction stack on the simulated devices, drives
 * the workload, "crashes" (freezes the devices, then applies the scheduled
 * torn/lost damage), reopens, runs RecoveryManager::recover(), and checks the
 * post-recovery invariants against an oracle. The same seed always produces a
 * byte-identical fault sequence and identical outcome.
 */

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "sim/fault.hpp"

namespace entropy::sim {

/// One crash schedule. Presets are built by make_schedule().
struct Schedule {
  std::string name;
  std::string crash_point;  ///< human label for the JSONL "crash_point" field
  size_t num_txns = 12;
  bool leave_in_flight = true;         ///< leave a loser txn open at the crash
  bool flush_pages_before_crash = false;  ///< steal: write dirty pages (unsynced)
  bool sync_pages_after_workload = false;  ///< fsync the data image before crash
  bool skip_recovery = false;          ///< negative test: skip recover()
  uint32_t abort_ppk = 0;              ///< workload abort rate (see RandomWorkload)
  /// In-flight transaction insert count (see RandomWorkload). Sized past
  /// WAL_BUFFER_SIZE it forces mid-transaction overflow flushes so the loser's
  /// bytes genuinely reach the log device before the crash.
  size_t inflight_ops = 0;
  /// Arm SimLogStore sync failures at the start of the in-flight transaction:
  /// its overflow flushes then append without becoming durable, producing a
  /// real unsynced WAL tail for the crash to lose/tear.
  bool arm_wal_sync_failures = false;
  /// Steal the committed data pages the instant the in-flight loser begins,
  /// before its writes dirty them further and before arm_wal_sync_failures
  /// poisons the log. At that point the committed pages' WAL is already durable,
  /// so the WAL-before-page guard lets them reach disk unsynced; the crash then
  /// loses them and recovery redoes them from the durable log. This is the only
  /// point a page steal produces a genuine unsynced-committed-page loss once the
  /// same run also arms WAL-sync failures (which otherwise, correctly, blocks
  /// every steal because no page may precede its log to disk).
  bool steal_committed_at_inflight_begin = false;

  // ── Anti-vacuity contract, asserted by the schedule sweep test ────────────
  /// Every fault kind listed here must fire at least once across the sweep's
  /// seeds. A schedule whose advertised fault cannot fire fails its own test.
  std::vector<FaultKind> must_fire;
  /// The sweep asserts undo_ops > 0 for every seed (recovery's undo phase is
  /// genuinely exercised, not vacuously green).
  bool expect_undo = false;
  /// The sweep asserts at least one transaction aborted during normal operation
  /// for every seed, so an abort schedule can never regress into vacuous
  /// coverage (aborts silently stop firing).
  bool expect_aborts = false;
  /// The sweep asserts faults_injected == 0 for every seed (clean-baseline
  /// control schedules).
  bool expect_zero_faults = false;

  FaultConfig faults{};
};

/// Outcome of one seed under one schedule. Field set mirrors the JSONL schema.
struct RunResult {
  uint64_t seed = 0;
  std::string schedule;
  std::string crash_point;
  size_t faults_injected = 0;
  size_t ops = 0;
  size_t redo_ops = 0;  ///< recovery redo operations actually applied
  size_t undo_ops = 0;  ///< recovery undo operations actually applied
  size_t aborts = 0;    ///< transactions aborted during normal operation
  double recovery_ms = 0.0;  ///< wall time; excluded from JSONL by default
  bool recovery_ok = true;
  std::vector<std::string> invariants_failed;
  FaultLog faults;  ///< combined, ordered (disk then log) for determinism checks

  [[nodiscard]] bool passed() const {
    return recovery_ok && invariants_failed.empty();
  }
  /// "pass" (invariants held), "fail" (a checked invariant was violated), or
  /// "error" (recovery itself returned an error Status).
  [[nodiscard]] std::string outcome() const {
    if (!recovery_ok) {
      return "error";
    }
    return invariants_failed.empty() ? "pass" : "fail";
  }
  /// One stable JSONL line (schema documented in schedule.cpp). By default the
  /// line is fully deterministic: two runs of the same seed produce identical
  /// bytes. @p include_timing appends the wall-clock recovery_ms field, which
  /// is inherently non-reproducible and therefore opt-in.
  [[nodiscard]] std::string to_jsonl(bool include_timing = false) const;
};

/// Assemble the stack for @p seed under @p schedule, crash, recover, and check.
[[nodiscard]] RunResult run_schedule(uint64_t seed, const Schedule &schedule);

/// Look up a named preset schedule (nullopt if unknown).
[[nodiscard]] std::optional<Schedule> make_schedule(const std::string &name);

/// All preset schedule names, in a stable order.
[[nodiscard]] std::vector<std::string> schedule_names();

}  // namespace entropy::sim
