/**
 * @file main.cpp
 * @brief entropy-sim: deterministic crash-simulation CLI runner.
 *
 * Runs N consecutive seeds of a named crash schedule, prints a pass/fail
 * summary, and writes one JSONL line per run (schema documented in
 * sim/schedule.cpp) to a results file a dashboard can consume later.
 *
 * Usage:
 *   entropy-sim --seeds N [--schedule NAME] [--start-seed S] [--out FILE]
 *   entropy-sim --list
 */

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <string>

#include "sim/fault.hpp"
#include "sim/schedule.hpp"

namespace {

using entropy::sim::fault_kind_name;
using entropy::sim::FaultKind;
using entropy::sim::make_schedule;
using entropy::sim::run_schedule;
using entropy::sim::RunResult;
using entropy::sim::Schedule;
using entropy::sim::schedule_names;

void print_usage() {
  std::cout
      << "Usage: entropy-sim --seeds N [--schedule NAME] [--start-seed S] "
         "[--out FILE] [--timing]\n"
      << "       entropy-sim --list\n\n"
      << "  --seeds N        run N consecutive seeds (default 1)\n"
      << "  --schedule NAME  crash schedule to run (default \"mixed\")\n"
      << "  --start-seed S   first seed value (default 1)\n"
      << "  --out FILE       JSONL results file (default entropy-sim.jsonl)\n"
      << "  --timing         include wall-clock recovery_ms in the JSONL\n"
      << "                   (off by default so same-seed output is\n"
      << "                   byte-identical across runs)\n"
      << "  --list           list the available schedules and exit\n";
}

}  // namespace

int main(int argc, char **argv) {
  uint64_t num_seeds = 1;
  uint64_t start_seed = 1;
  std::string schedule_name = "mixed";
  std::string out_path = "entropy-sim.jsonl";
  bool include_timing = false;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto next = [&](const char *flag) -> std::string {
      if (i + 1 >= argc) {
        std::cerr << "error: " << flag << " requires a value\n";
        std::exit(2);
      }
      return argv[++i];
    };
    if (arg == "--seeds") {
      num_seeds = std::strtoull(next("--seeds").c_str(), nullptr, 10);
    } else if (arg == "--start-seed") {
      start_seed = std::strtoull(next("--start-seed").c_str(), nullptr, 10);
    } else if (arg == "--schedule") {
      schedule_name = next("--schedule");
    } else if (arg == "--out") {
      out_path = next("--out");
    } else if (arg == "--timing") {
      include_timing = true;
    } else if (arg == "--list") {
      std::cout << "Available schedules:\n";
      for (const auto &n : schedule_names()) {
        std::cout << "  " << n << '\n';
      }
      return 0;
    } else if (arg == "--help" || arg == "-h") {
      print_usage();
      return 0;
    } else {
      std::cerr << "error: unknown argument '" << arg << "'\n";
      print_usage();
      return 2;
    }
  }

  auto schedule = make_schedule(schedule_name);
  if (!schedule) {
    std::cerr << "error: unknown schedule '" << schedule_name
              << "' (try --list)\n";
    return 2;
  }

  std::ofstream out(out_path, std::ios::out | std::ios::trunc);
  if (!out) {
    std::cerr << "error: cannot open results file '" << out_path << "'\n";
    return 2;
  }

  uint64_t passed = 0;
  uint64_t failed = 0;
  uint64_t errored = 0;
  uint64_t undo_runs = 0;  // runs where recovery's undo phase applied work
  std::map<FaultKind, uint64_t> fired;  // per-kind fault totals for the sweep
  for (uint64_t k = 0; k < num_seeds; ++k) {
    const uint64_t seed = start_seed + k;
    RunResult result = run_schedule(seed, *schedule);
    out << result.to_jsonl(include_timing) << '\n';

    for (const auto &event : result.faults) {
      ++fired[event.kind];
    }
    if (result.undo_ops > 0) {
      ++undo_runs;
    }

    const std::string outcome = result.outcome();
    if (outcome == "pass") {
      ++passed;
    } else if (outcome == "error") {
      ++errored;
    } else {
      ++failed;
    }

    if (outcome != "pass") {
      std::cout << "  seed " << seed << " [" << schedule_name << "] -> "
                << outcome;
      for (const auto &inv : result.invariants_failed) {
        std::cout << ' ' << inv;
      }
      std::cout << '\n';
    }
  }

  std::cout << "entropy-sim: schedule=" << schedule_name
            << " seeds=" << num_seeds << " start=" << start_seed << "  "
            << passed << " passed, " << failed << " failed, " << errored
            << " errored\n";
  std::cout << "faults fired:";
  if (fired.empty()) {
    std::cout << " none";
  }
  for (const auto &[kind, count] : fired) {
    std::cout << ' ' << fault_kind_name(kind) << '=' << count;
  }
  std::cout << "  undo_exercised_runs=" << undo_runs << '\n';
  std::cout << "results written to " << out_path << '\n';

  return (failed == 0 && errored == 0) ? 0 : 1;
}
