#pragma once

/**
 * @file data_loader.hpp
 * @brief Parses entropy-sim JSONL into structs the dashboard renders.
 *
 * The JSONL schema is stable (RunResult::to_jsonl in the simulator): one object
 * per line, fields in a fixed order. We parse the fields we render and expose a
 * few aggregates (pass/fail counts, recovery percentiles, per-kind fault
 * totals) so the dashboard does not re-derive them.
 */

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace entropy::tui {

/// One simulator run, mirrored from a JSONL line.
struct RunRecord {
  uint64_t seed = 0;
  std::string schedule;
  int faults_injected = 0;
  std::string crash_point;
  std::string outcome;  ///< "pass" | "fail" | "error"
  std::vector<std::string> invariants_failed;
  long ops = 0;
  long redo_ops = 0;
  long undo_ops = 0;
  double recovery_ms = 0.0;  ///< 0 when the run was written without --timing
  bool has_timing = false;

  [[nodiscard]] bool passed() const { return outcome == "pass"; }
};

/// A parsed dataset plus the aggregates the dashboard KPIs need.
struct DataSet {
  std::vector<RunRecord> runs;

  int total = 0;
  int passed = 0;
  int failed = 0;
  int errored = 0;

  int total_faults = 0;

  /// Distinct invariant names that were violated at least once.
  std::vector<std::string> distinct_invariants;
  /// Schedules present, in first-seen order.
  std::vector<std::string> schedules;

  /// Recovery-time percentiles over runs that carried timing (ms). Zero when no
  /// timed runs are present.
  double recovery_p50 = 0.0;
  double recovery_p99 = 0.0;
  double recovery_max = 0.0;

  /// Recovery-time samples, in seed order, for the timeline chart.
  std::vector<double> recovery_series;

  [[nodiscard]] bool empty() const { return runs.empty(); }
  [[nodiscard]] double pass_rate() const {
    return total > 0 ? static_cast<double>(passed) / static_cast<double>(total)
                     : 0.0;
  }
};

/// Parse a single JSONL file. Returns nullopt if the file cannot be opened.
/// Malformed lines are skipped rather than aborting the load.
[[nodiscard]] std::optional<DataSet> LoadJsonl(const std::string &path);

/// Load the bundled demo data (all tui/data/*.jsonl, merged) for a
/// deterministic offline render. Falls back to an empty set if the directory is
/// missing. Aggregates are computed over the merged runs.
[[nodiscard]] DataSet LoadDemoData();

/// Recompute all aggregate fields from `runs`. Called by the loaders; exposed
/// so a caller that assembles runs by hand can refresh them.
void Recompute(DataSet &data);

}  // namespace entropy::tui
