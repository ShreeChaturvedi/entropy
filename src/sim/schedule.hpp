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
  FaultConfig faults{};
};

/// Outcome of one seed under one schedule. Field set mirrors the JSONL schema.
struct RunResult {
  uint64_t seed = 0;
  std::string schedule;
  std::string crash_point;
  size_t faults_injected = 0;
  size_t ops = 0;
  double recovery_ms = 0.0;
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
  /// One stable JSONL line. The schema is fixed; a dashboard consumes it.
  [[nodiscard]] std::string to_jsonl() const;
};

/// Assemble the stack for @p seed under @p schedule, crash, recover, and check.
[[nodiscard]] RunResult run_schedule(uint64_t seed, const Schedule &schedule);

/// Look up a named preset schedule (nullopt if unknown).
[[nodiscard]] std::optional<Schedule> make_schedule(const std::string &name);

/// All preset schedule names, in a stable order.
[[nodiscard]] std::vector<std::string> schedule_names();

}  // namespace entropy::sim
