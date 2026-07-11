#include "data_loader.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <string>

namespace entropy::tui {

namespace {

/// Read a "key":"value" string field. Returns nullopt when the key is absent.
[[nodiscard]] std::optional<std::string> field_string(const std::string &line,
                                                      const std::string &key) {
  const std::string needle = "\"" + key + "\":\"";
  const auto pos = line.find(needle);
  if (pos == std::string::npos) {
    return std::nullopt;
  }
  const auto start = pos + needle.size();
  const auto end = line.find('"', start);
  if (end == std::string::npos) {
    return std::nullopt;
  }
  return line.substr(start, end - start);
}

/// Read a "key":<number> field (int or float). Returns nullopt when absent.
[[nodiscard]] std::optional<double> field_number(const std::string &line,
                                                 const std::string &key) {
  const std::string needle = "\"" + key + "\":";
  const auto pos = line.find(needle);
  if (pos == std::string::npos) {
    return std::nullopt;
  }
  auto start = pos + needle.size();
  if (start < line.size() && line[start] == '"') {
    return std::nullopt;  // a string field, not a number
  }
  const auto end = line.find_first_of(",}", start);
  if (end == std::string::npos) {
    return std::nullopt;
  }
  try {
    return std::stod(line.substr(start, end - start));
  } catch (...) {
    return std::nullopt;
  }
}

/// Read a "key":[ "a", "b" ] string array (possibly empty).
[[nodiscard]] std::vector<std::string> field_string_array(
    const std::string &line, const std::string &key) {
  std::vector<std::string> out;
  const std::string needle = "\"" + key + "\":[";
  const auto pos = line.find(needle);
  if (pos == std::string::npos) {
    return out;
  }
  auto cursor = pos + needle.size();
  const auto close = line.find(']', cursor);
  if (close == std::string::npos) {
    return out;
  }
  while (cursor < close) {
    const auto q1 = line.find('"', cursor);
    if (q1 == std::string::npos || q1 >= close) {
      break;
    }
    const auto q2 = line.find('"', q1 + 1);
    if (q2 == std::string::npos || q2 > close) {
      break;
    }
    out.push_back(line.substr(q1 + 1, q2 - q1 - 1));
    cursor = q2 + 1;
  }
  return out;
}

[[nodiscard]] std::optional<RunRecord> parse_line(const std::string &line) {
  if (line.find("\"seed\"") == std::string::npos) {
    return std::nullopt;
  }
  RunRecord r;
  if (const auto v = field_number(line, "seed")) {
    r.seed = static_cast<uint64_t>(*v);
  }
  r.schedule = field_string(line, "schedule").value_or("");
  r.crash_point = field_string(line, "crash_point").value_or("");
  r.outcome = field_string(line, "outcome").value_or("");
  r.invariants_failed = field_string_array(line, "invariants_failed");
  r.faults_injected =
      static_cast<int>(field_number(line, "faults_injected").value_or(0.0));
  r.ops = static_cast<long>(field_number(line, "ops").value_or(0.0));
  r.redo_ops = static_cast<long>(field_number(line, "redo_ops").value_or(0.0));
  r.undo_ops = static_cast<long>(field_number(line, "undo_ops").value_or(0.0));
  if (const auto ms = field_number(line, "recovery_ms")) {
    r.recovery_ms = *ms;
    r.has_timing = true;
  }
  return r;
}

/// Linear-interpolated percentile over an unsorted copy.
[[nodiscard]] double percentile(std::vector<double> values, double p) {
  if (values.empty()) {
    return 0.0;
  }
  std::sort(values.begin(), values.end());
  if (values.size() == 1) {
    return values.front();
  }
  const double rank = std::clamp(p, 0.0, 100.0) / 100.0 *
                      static_cast<double>(values.size() - 1);
  const auto lo = static_cast<size_t>(std::floor(rank));
  const auto hi = static_cast<size_t>(std::ceil(rank));
  const double frac = rank - static_cast<double>(lo);
  return values[lo] + (values[hi] - values[lo]) * frac;
}

/// Append to a first-seen-ordered list if not already present.
void push_unique(std::vector<std::string> &seq, const std::string &value) {
  if (std::find(seq.begin(), seq.end(), value) == seq.end()) {
    seq.push_back(value);
  }
}

}  // namespace

void Recompute(DataSet &data) {
  data.total = static_cast<int>(data.runs.size());
  data.passed = 0;
  data.failed = 0;
  data.errored = 0;
  data.total_faults = 0;
  data.distinct_invariants.clear();
  data.schedules.clear();
  data.recovery_series.clear();

  std::vector<double> timed;
  for (const RunRecord &r : data.runs) {
    if (r.outcome == "pass") {
      ++data.passed;
    } else if (r.outcome == "fail") {
      ++data.failed;
    } else {
      ++data.errored;
    }
    data.total_faults += r.faults_injected;
    push_unique(data.schedules, r.schedule);
    for (const std::string &inv : r.invariants_failed) {
      push_unique(data.distinct_invariants, inv);
    }
    if (r.has_timing) {
      timed.push_back(r.recovery_ms);
      data.recovery_series.push_back(r.recovery_ms);
    }
  }

  data.recovery_p50 = percentile(timed, 50.0);
  data.recovery_p99 = percentile(timed, 99.0);
  data.recovery_max = timed.empty()
                          ? 0.0
                          : *std::max_element(timed.begin(), timed.end());
}

std::optional<DataSet> LoadJsonl(const std::string &path) {
  std::ifstream in(path);
  if (!in) {
    return std::nullopt;
  }
  DataSet data;
  std::string line;
  while (std::getline(in, line)) {
    if (auto rec = parse_line(line)) {
      data.runs.push_back(std::move(*rec));
    }
  }
  Recompute(data);
  return data;
}

DataSet LoadDemoData() {
#ifndef ENTROPY_TUI_DATA_DIR
#define ENTROPY_TUI_DATA_DIR "tui/data"
#endif
  // A fixed, ordered file list keeps the merged dataset deterministic across
  // machines: the mixed sweep first, then the focused schedules, then the
  // known-bug repro that supplies genuine failing seeds for the matrix.
  static const char *kFiles[] = {
      "demo.jsonl",
      "undo_durable_loser.jsonl",
      "torn_wal_tail.jsonl",
      "durable_survives_intact.jsonl",
      "torn_page_write.jsonl",
  };
  const std::string dir = ENTROPY_TUI_DATA_DIR;
  DataSet merged;
  for (const char *name : kFiles) {
    if (auto part = LoadJsonl(dir + "/" + name)) {
      merged.runs.insert(merged.runs.end(), part->runs.begin(),
                         part->runs.end());
    }
  }
  Recompute(merged);
  return merged;
}

}  // namespace entropy::tui
