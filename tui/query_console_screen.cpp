#include "query_console_screen.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <vector>

#include <ftxui/component/component.hpp>
#include <ftxui/component/event.hpp>
#include <ftxui/dom/elements.hpp>

#include <entropy/entropy.hpp>

#include "primitives.hpp"
#include "theme.hpp"

namespace entropy::tui {

using namespace ftxui;

namespace {

// ── Console-local syntax color (outside the status budget, used only to give
// the SQL editor its restrained four-role highlight). Muted so it reads as ink,
// not signal. ─────────────────────────────────────────────────────────────────
[[nodiscard]] Color kOperator() { return Color::RGB(190, 120, 182); }

// ── Small string helpers ─────────────────────────────────────────────────────

[[nodiscard]] std::string to_upper(std::string s) {
  for (char &c : s) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  return s;
}

[[nodiscard]] std::string to_lower(std::string s) {
  for (char &c : s) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return s;
}

[[nodiscard]] std::string trim(const std::string &s) {
  const auto a = s.find_first_not_of(" \t\r\n");
  if (a == std::string::npos) {
    return "";
  }
  const auto b = s.find_last_not_of(" \t\r\n");
  return s.substr(a, b - a + 1);
}

[[nodiscard]] bool is_word_char(char c) {
  return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

// The SQL keyword set. Doubles as the highlighter's keyword role and the
// autocomplete dictionary (joined with the schema's table/column names).
[[nodiscard]] const std::unordered_set<std::string> &keyword_set() {
  static const std::unordered_set<std::string> kw = {
      "SELECT",  "FROM",    "WHERE",  "INSERT", "INTO",   "VALUES", "UPDATE",
      "SET",     "DELETE",  "CREATE", "TABLE",  "INDEX",  "DROP",   "EXPLAIN",
      "ANALYZE", "ORDER",   "GROUP",  "BY",     "ASC",    "DESC",   "LIMIT",
      "OFFSET",  "JOIN",    "INNER",  "LEFT",   "RIGHT",  "OUTER",  "CROSS",
      "ON",      "AND",     "OR",     "NOT",    "IS",     "NULL",   "AS",
      "PRIMARY", "KEY",     "TRUE",   "FALSE",  "INT",    "INTEGER","BIGINT",
      "SMALLINT","BOOLEAN", "VARCHAR","TEXT",   "FLOAT",  "DOUBLE", "DECIMAL",
      "TIMESTAMP","DATE",   "CHAR"};
  return kw;
}

// ── The demo schema (also drives the sidebar and autocomplete) ───────────────

struct Column {
  std::string name;
  std::string glyph;  // type glyph: "#" numeric, "s" string, "b" bool
};
struct Table {
  std::string name;
  std::vector<Column> columns;
};

[[nodiscard]] const std::vector<Table> &schema() {
  static const std::vector<Table> tables = {
      {"runs",
       {{"seed", "#"},
        {"schedule", "s"},
        {"outcome", "s"},
        {"faults", "#"},
        {"recovery_ms", "#"}}},
      {"schedules",
       {{"name", "s"}, {"fault_kinds", "#"}, {"seeds", "#"}}},
  };
  return tables;
}

// ── Result display model (decoupled from Result's lifetime) ──────────────────

struct Grid {
  bool ok = false;
  bool is_query = false;
  std::vector<std::string> cols;
  std::vector<std::string> glyphs;             // per-column type glyph
  int outcome_col = -1;                         // index of an "outcome" column
  std::vector<std::vector<std::string>> rows;  // stringified cells
  std::string message;                          // error or "N rows affected"
};

// Display text for a cell. Doubles get a compact fixed precision so the grid
// reads like an instrument, not raw serialization ("12.40", not "12.400000").
[[nodiscard]] std::string cell_text(const Value &v) {
  if (v.is_double()) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%.2f", v.as_double());
    return buf;
  }
  return v.to_string();
}

[[nodiscard]] std::string value_glyph(const Value &v) {
  if (v.is_string()) {
    return "s";
  }
  if (v.is_bool()) {
    return "b";
  }
  if (v.is_int32() || v.is_int64() || v.is_double()) {
    return "#";
  }
  return "·";
}

[[nodiscard]] Grid build_grid(const Result &r) {
  Grid g;
  g.ok = r.ok();
  if (!r.ok()) {
    g.message = r.status().to_string();
    return g;
  }
  if (!r.is_query()) {
    g.message = std::to_string(r.affected_rows()) + " row(s) affected";
    return g;
  }

  g.is_query = true;
  g.cols = r.column_names();
  g.glyphs.assign(g.cols.size(), "·");
  for (size_t c = 0; c < g.cols.size(); ++c) {
    if (to_lower(g.cols[c]) == "outcome") {
      g.outcome_col = static_cast<int>(c);
    }
  }

  for (const auto &row : r.rows()) {
    std::vector<std::string> cells;
    cells.reserve(row.size());
    for (size_t c = 0; c < row.size(); ++c) {
      const Value &v = row[c];
      // First non-null value in a column fixes its type glyph.
      if (c < g.glyphs.size() && !v.is_null() && g.glyphs[c] == "·") {
        g.glyphs[c] = value_glyph(v);
      }
      cells.push_back(cell_text(v));
    }
    g.rows.push_back(std::move(cells));
  }
  return g;
}

// A single executed statement, remembered for the History tab.
struct HistoryEntry {
  std::string sql;
  bool ok = false;
  std::string note;  // "N rows" / "N affected" / error code
};

// ── The whole console's mutable state, kept alive by the component ───────────

enum class Focus { kEditor, kResults };
enum class Tab : int { kQuery = 0, kHistory = 1, kPlan = 2 };

struct Console {
  std::unique_ptr<Database> db;
  std::string db_note = "unavailable";
  std::string db_path;

  std::string sql;
  Focus focus = Focus::kEditor;
  Tab tab = Tab::kQuery;

  Grid grid;
  std::vector<std::string> plan;  // EXPLAIN output lines
  std::vector<HistoryEntry> history;

  int sel_row = 0;
  int sel_col = 0;

  ~Console() {
    if (db) {
      db->close();
    }
    if (!db_path.empty()) {
      std::error_code ec;
      std::filesystem::remove(db_path, ec);
      std::filesystem::remove(db_path + "-wal", ec);
      std::filesystem::remove(db_path + ".wal", ec);
    }
  }
};

// Recompute the EXPLAIN plan for the current statement (SELECT only).
void refresh_plan(Console &c) {
  c.plan.clear();
  if (!c.db) {
    return;
  }
  const std::string q = trim(c.sql);
  if (to_upper(q).rfind("SELECT", 0) != 0) {
    c.plan.push_back("EXPLAIN is available for SELECT statements.");
    return;
  }
  Result r = c.db->execute("EXPLAIN " + q);
  if (!r.ok()) {
    c.plan.push_back(r.status().to_string());
    return;
  }
  for (const auto &row : r.rows()) {
    std::string line;
    for (size_t i = 0; i < row.size(); ++i) {
      if (i) {
        line += "  ";
      }
      line += row[i].to_string();
    }
    c.plan.push_back(line);
  }
  if (c.plan.empty()) {
    c.plan.push_back("(no plan rows)");
  }
}

// Run the editor's statement against the live database and refresh every view.
void run_query(Console &c) {
  std::string q = trim(c.sql);
  if (!q.empty() && q.back() == ';') {
    q.pop_back();
    q = trim(q);
  }
  if (q.empty() || !c.db) {
    return;
  }

  Result r = c.db->execute(q);
  c.grid = build_grid(r);
  c.sel_row = 0;
  c.sel_col = 0;
  c.focus = Focus::kResults;
  c.tab = Tab::kQuery;

  HistoryEntry h;
  h.sql = q;
  h.ok = r.ok();
  if (!r.ok()) {
    h.note = std::string(r.status().message());
  } else if (r.is_query()) {
    h.note = std::to_string(r.row_count()) + " rows";
  } else {
    h.note = std::to_string(r.affected_rows()) + " affected";
  }
  c.history.push_back(std::move(h));

  refresh_plan(c);
}

// Open a temp database, seed the demo schema + rows, and run a first query so a
// captured still already shows a populated grid.
void open_and_seed(Console &c) {
  std::error_code ec;
  const auto dir = std::filesystem::temp_directory_path(ec);
  static int counter = 0;
  const auto token = std::chrono::steady_clock::now().time_since_epoch().count();
  c.db_path = (dir / ("entropy-console-" + std::to_string(token) + "-" +
                      std::to_string(counter++) + ".edb"))
                  .string();
  std::filesystem::remove(c.db_path, ec);
  std::filesystem::remove(c.db_path + "-wal", ec);

  try {
    c.db = std::make_unique<Database>(c.db_path);
  } catch (...) {
    c.db.reset();
    c.db_note = "unavailable";
    return;
  }

  const std::array<const char *, 4> ddl = {
      "CREATE TABLE runs (seed INTEGER, schedule VARCHAR(24), outcome "
      "VARCHAR(8), faults INTEGER, recovery_ms DOUBLE)",
      "CREATE TABLE schedules (name VARCHAR(24), fault_kinds INTEGER, seeds "
      "INTEGER)",
      "CREATE INDEX idx_runs_seed ON runs (seed)",
      "CREATE INDEX idx_runs_sched ON runs (schedule)",
  };
  for (const char *s : ddl) {
    (void)c.db->execute(s);  // index DDL may be a no-op; the grid tolerates it
  }

  const std::array<const char *, 14> runs = {
      "INSERT INTO runs VALUES (1042,'mixed','pass',3,12.40)",
      "INSERT INTO runs VALUES (1043,'mixed','pass',2,9.10)",
      "INSERT INTO runs VALUES (1044,'wal_stress','fail',7,48.75)",
      "INSERT INTO runs VALUES (1045,'wal_stress','pass',5,21.30)",
      "INSERT INTO runs VALUES (1046,'page_tears','pass',4,15.80)",
      "INSERT INTO runs VALUES (1047,'page_tears','fail',9,63.10)",
      "INSERT INTO runs VALUES (1048,'undo_heavy','pass',6,27.60)",
      "INSERT INTO runs VALUES (1049,'undo_heavy','pass',6,24.90)",
      "INSERT INTO runs VALUES (1050,'crash_storm','fail',12,88.20)",
      "INSERT INTO runs VALUES (1051,'crash_storm','pass',8,41.05)",
      "INSERT INTO runs VALUES (1052,'mixed','pass',1,6.75)",
      "INSERT INTO runs VALUES (1053,'wal_stress','pass',5,19.40)",
      "INSERT INTO runs VALUES (1054,'page_tears','pass',3,13.20)",
      "INSERT INTO runs VALUES (1055,'crash_storm','pass',10,52.60)",
  };
  for (const char *s : runs) {
    (void)c.db->execute(s);
  }
  const std::array<const char *, 5> scheds = {
      "INSERT INTO schedules VALUES ('mixed',5,64)",
      "INSERT INTO schedules VALUES ('wal_stress',3,48)",
      "INSERT INTO schedules VALUES ('page_tears',4,40)",
      "INSERT INTO schedules VALUES ('undo_heavy',2,32)",
      "INSERT INTO schedules VALUES ('crash_storm',6,72)",
  };
  for (const char *s : scheds) {
    (void)c.db->execute(s);
  }

  c.db_note = "connected · " + std::to_string(schema().size()) + " tables";
  c.sql = "SELECT seed, schedule, outcome, recovery_ms FROM runs ORDER BY "
          "recovery_ms DESC";
  run_query(c);
  c.focus = Focus::kResults;  // land on the populated grid for the still
}

// ── Rendering ────────────────────────────────────────────────────────────────

// Split a SQL line into colored tokens for the four-role highlight.
[[nodiscard]] Element highlight_line(const std::string &line) {
  static constexpr std::string_view kOpChars = "=<>!+-*/";
  Elements out;
  size_t i = 0;
  const size_t n = line.size();
  while (i < n) {
    const char ch = line[i];
    if (ch == '-' && i + 1 < n && line[i + 1] == '-') {  // comment to EOL
      out.push_back(text(line.substr(i)) | color(theme::kInk2()) | italic |
                    dim);
      break;
    }
    if (std::isspace(static_cast<unsigned char>(ch))) {
      size_t j = i;
      while (j < n && std::isspace(static_cast<unsigned char>(line[j]))) {
        ++j;
      }
      out.push_back(text(line.substr(i, j - i)));
      i = j;
    } else if (is_word_char(ch)) {
      size_t j = i;
      while (j < n && is_word_char(line[j])) {
        ++j;
      }
      const std::string word = line.substr(i, j - i);
      const bool is_kw = keyword_set().count(to_upper(word)) > 0;
      out.push_back(is_kw ? (text(word) | bold | color(theme::kInk0()))
                          : (text(word) | color(theme::kInk1())));
      i = j;
    } else if (ch == '\'') {  // string literal
      size_t j = i + 1;
      while (j < n && line[j] != '\'') {
        ++j;
      }
      if (j < n) {
        ++j;
      }
      out.push_back(text(line.substr(i, j - i)) | color(theme::kGreen()));
      i = j;
    } else if (kOpChars.find(ch) != std::string_view::npos) {
      size_t j = i;
      while (j < n && kOpChars.find(line[j]) != std::string_view::npos) {
        ++j;
      }
      out.push_back(text(line.substr(i, j - i)) | color(kOperator()));
      i = j;
    } else {  // punctuation: parens, commas, semicolons, dots
      out.push_back(text(std::string(1, ch)) | color(theme::kInk2()));
      ++i;
    }
  }
  if (out.empty()) {
    out.push_back(text(""));
  }
  return hbox(std::move(out));
}

[[nodiscard]] std::string pad_left(const std::string &s, size_t w) {
  return s.size() >= w ? s : std::string(w - s.size(), ' ') + s;
}

// The line-numbered SQL editor, with a block cursor when the editor has focus.
[[nodiscard]] Element render_editor(const Console &c) {
  std::vector<std::string> lines;
  size_t start = 0;
  while (true) {
    const size_t nl = c.sql.find('\n', start);
    if (nl == std::string::npos) {
      lines.push_back(c.sql.substr(start));
      break;
    }
    lines.push_back(c.sql.substr(start, nl - start));
    start = nl + 1;
  }

  Elements body;
  for (size_t i = 0; i < lines.size(); ++i) {
    Element code = highlight_line(lines[i]);
    if (i + 1 == lines.size() && c.focus == Focus::kEditor) {
      code = hbox({code, text("▏") | color(theme::kAmber())});
    }
    body.push_back(hbox({
        text(pad_left(std::to_string(i + 1), 3) + " ") | color(theme::kInk3()),
        text("│ ") | color(theme::kInk3()),
        code,
    }));
  }
  // Pad to a stable height so the panel does not jump as lines are added.
  for (size_t i = lines.size(); i < 4; ++i) {
    body.push_back(text(""));
  }

  return Panel("EDITOR", vbox(std::move(body)), c.focus == Focus::kEditor);
}

// A small autocomplete popup for the trailing identifier being typed.
[[nodiscard]] Element render_autocomplete(const Console &c) {
  if (c.focus != Focus::kEditor) {
    return text("");
  }
  size_t e = c.sql.size();
  while (e > 0 && is_word_char(c.sql[e - 1])) {
    --e;
  }
  const std::string frag = c.sql.substr(e);
  if (frag.size() < 1) {
    return text("");
  }
  const std::string up = to_upper(frag);

  std::vector<std::string> matches;
  for (const std::string &kw : keyword_set()) {
    if (kw.rfind(up, 0) == 0) {
      matches.push_back(kw);
    }
  }
  for (const Table &t : schema()) {
    if (to_upper(t.name).rfind(up, 0) == 0) {
      matches.push_back(t.name);
    }
    for (const Column &col : t.columns) {
      if (to_upper(col.name).rfind(up, 0) == 0) {
        matches.push_back(col.name);
      }
    }
  }
  if (matches.empty()) {
    return text("");
  }
  std::sort(matches.begin(), matches.end());
  matches.erase(std::unique(matches.begin(), matches.end()), matches.end());
  if (matches.size() > 6) {
    matches.resize(6);
  }

  Elements rows;
  for (size_t i = 0; i < matches.size(); ++i) {
    Element m = (i == 0)
                    ? (text(" " + matches[i] + " ") | color(theme::kAmber()) |
                       bgcolor(theme::kAmberDeep()))
                    : (text(" " + matches[i] + " ") | color(theme::kInk1()));
    rows.push_back(m);
  }
  return hbox({text("  "),
               vbox(std::move(rows)) | borderStyled(ROUNDED, theme::kInk3()) |
                   bgcolor(theme::kPanelHi())});
}

// The tab strip that heads the lower panel: Query / History / Plan.
[[nodiscard]] Element render_tabstrip(const Console &c,
                                      const std::string &annotation) {
  const std::array<std::pair<Tab, const char *>, 3> tabs = {
      {{Tab::kQuery, "Query"},
       {Tab::kHistory, "History"},
       {Tab::kPlan, "Plan"}}};
  Elements strip;
  for (const auto &[t, label] : tabs) {
    const bool on = (t == c.tab);
    strip.push_back(on ? (text(" " + std::string(label) + " ") | bold |
                          color(theme::kAmber()))
                       : (text(" " + std::string(label) + " ") |
                          color(theme::kInk2())));
    strip.push_back(text(" "));
  }
  strip.push_back(filler());
  if (!annotation.empty()) {
    strip.push_back(text(annotation) | color(theme::kInk2()));
  }
  return hbox(std::move(strip));
}

// The typed result grid for the Query tab.
[[nodiscard]] Element render_grid(const Console &c) {
  const Grid &g = c.grid;
  if (!g.ok) {
    return vbox({filler(),
                 hbox({filler(),
                       text(g.message.empty() ? "query failed" : g.message) |
                           color(theme::kRed()),
                       filler()}),
                 filler()});
  }
  if (!g.is_query) {
    return vbox({filler(),
                 hbox({filler(),
                       text(g.message) | color(theme::kGreen()), filler()}),
                 filler()});
  }
  if (g.cols.empty()) {
    return vbox({filler(),
                 hbox({filler(), text("(no columns)") | color(theme::kInk3()),
                       filler()}),
                 filler()});
  }

  const size_t ncol = g.cols.size();
  std::vector<size_t> width(ncol, 4);
  for (size_t c2 = 0; c2 < ncol; ++c2) {
    width[c2] = std::max(width[c2], g.cols[c2].size() + 2);  // glyph + space
    for (const auto &row : g.rows) {
      if (c2 < row.size()) {
        width[c2] = std::max(width[c2], row[c2].size());
      }
    }
    width[c2] = std::min<size_t>(width[c2], 28);
  }

  // Header: type glyph (dim) + column name (bold), each fixed-width.
  Elements header;
  for (size_t c2 = 0; c2 < ncol; ++c2) {
    header.push_back(hbox({text(g.glyphs[c2]) | color(theme::kInk3()),
                           text(" "),
                           text(g.cols[c2]) | bold | color(theme::kInk1())}) |
                     size(WIDTH, EQUAL, static_cast<int>(width[c2])));
    header.push_back(text("  "));
  }

  const int n = static_cast<int>(g.rows.size());
  constexpr int kMaxVisible = 16;
  int startr = 0;
  if (n > kMaxVisible) {
    startr = std::clamp(c.sel_row - kMaxVisible / 2, 0, n - kMaxVisible);
  }
  const int endr = std::min(n, startr + kMaxVisible);

  Elements body;
  for (int r = startr; r < endr; ++r) {
    const bool sel_row = (r == c.sel_row) && (c.focus == Focus::kResults);
    Elements cells;
    for (size_t c2 = 0; c2 < ncol; ++c2) {
      const std::string txt =
          c2 < g.rows[static_cast<size_t>(r)].size()
              ? g.rows[static_cast<size_t>(r)][c2]
              : "";
      Color fg = theme::kInk1();
      if (static_cast<int>(c2) == g.outcome_col) {
        fg = theme::status_color(theme::status_from_outcome(to_lower(txt)));
      }
      Element cell = text(txt) | color(fg) |
                     size(WIDTH, EQUAL, static_cast<int>(width[c2]));
      const bool sel_cell =
          sel_row && static_cast<int>(c2) == c.sel_col;
      if (sel_cell) {
        cell = cell | bold | color(theme::kInk0());
      }
      cells.push_back(std::move(cell));
      cells.push_back(text("  "));
    }
    Element row_el = hbox(std::move(cells));
    if (sel_row) {
      row_el = row_el | RowGlow(0);  // the one amber active-row band
    }
    body.push_back(std::move(row_el));
  }

  return vbox({
      hbox(std::move(header)),
      separatorLight() | color(theme::kInk3()),
      vbox(std::move(body)) | flex,
  });
}

[[nodiscard]] Element render_history(const Console &c) {
  if (c.history.empty()) {
    return vbox({filler(),
                 hbox({filler(), text("no statements yet") |
                                     color(theme::kInk3()),
                       filler()}),
                 filler()});
  }
  Elements rows;
  for (auto it = c.history.rbegin(); it != c.history.rend(); ++it) {
    const theme::Status st =
        it->ok ? theme::Status::kPass : theme::Status::kFail;
    rows.push_back(hbox({
        text(theme::status_glyph(st)) | color(theme::status_color(st)),
        text("  "),
        text(it->sql) | color(theme::kInk1()) | flex,
        text("  "),
        text(it->note) | color(theme::kInk2()),
    }));
  }
  return vbox(std::move(rows));
}

[[nodiscard]] Element render_plan(const Console &c) {
  if (c.plan.empty()) {
    return vbox({filler(),
                 hbox({filler(),
                       text("run a SELECT to see its plan") |
                           color(theme::kInk3()),
                       filler()}),
                 filler()});
  }
  Elements rows;
  for (const std::string &line : c.plan) {
    rows.push_back(text(line) | color(theme::kInk1()));
  }
  return vbox(std::move(rows));
}

// The schema sidebar. The table referenced by the current statement is lifted
// to the brightest ink (no amber: amber stays reserved for focus).
[[nodiscard]] Element render_sidebar(const Console &c) {
  const std::string up = to_upper(c.sql);
  std::string active;
  for (const Table &t : schema()) {
    const std::string tu = to_upper(t.name);
    const auto p = up.find(tu);
    if (p != std::string::npos) {
      const bool lb = (p == 0) || !is_word_char(up[p - 1]);
      const bool rb =
          (p + tu.size() >= up.size()) || !is_word_char(up[p + tu.size()]);
      if (lb && rb) {
        active = t.name;
        break;
      }
    }
  }
  if (active.empty() && !schema().empty()) {
    active = schema().front().name;
  }

  Elements items;
  for (const Table &t : schema()) {
    const bool on = (t.name == active);
    items.push_back(hbox({
        text(on ? "▸ " : "  ") | color(theme::kInk2()),
        on ? (text(t.name) | bold | color(theme::kInk0()))
           : (text(t.name) | color(theme::kInk1())),
    }));
    for (const Column &col : t.columns) {
      items.push_back(hbox({
          text("    "),
          text(col.glyph) | color(theme::kInk3()),
          text(" "),
          text(col.name) | color(on ? theme::kInk1() : theme::kInk2()),
      }));
    }
    items.push_back(text(""));
  }

  return Panel("SCHEMA", vbox(std::move(items)), /*focused=*/false);
}

[[nodiscard]] Element render_legend() {
  const auto key = [](const char *k) {
    return text(k) | color(theme::kInk1());
  };
  const auto lbl = [](const char *l) {
    return text(l) | color(theme::kInk3());
  };
  Element dot = text("   ") | color(theme::kInk3());
  return hbox({
      key(" ↵"), text(" "), lbl("run"), dot,
      key("↑↓←→"), text(" "), lbl("cells"), dot,
      key("F2/F3/F4"), text(" "), lbl("tabs"), dot,
      key("⌫"), text(" "), lbl("edit"), dot,
      key("Tab"), text(" "), lbl("dashboard"), dot,
      key("q"), text(" "), lbl("quit"),
      filler(),
  });
}

[[nodiscard]] Element render_console(const Console &c) {
  // Lower tabbed panel content.
  std::string annotation;
  Element content;
  switch (c.tab) {
    case Tab::kQuery:
      if (c.grid.ok && c.grid.is_query) {
        annotation = "(" + std::to_string(c.grid.rows.size()) + " rows)";
      }
      content = render_grid(c);
      break;
    case Tab::kHistory:
      annotation = "(" + std::to_string(c.history.size()) + ")";
      content = render_history(c);
      break;
    case Tab::kPlan:
      content = render_plan(c);
      break;
  }

  const Color lower_border =
      (c.focus == Focus::kResults) ? theme::kAmber() : theme::kInk3();
  Element lower = vbox({
                      render_tabstrip(c, annotation),
                      separatorLight() | color(theme::kInk3()),
                      content | flex,
                  }) |
                  borderStyled(ROUNDED, lower_border) |
                  bgcolor(theme::kPanel());

  Element main = vbox({
                     render_editor(c),
                     render_autocomplete(c),
                     lower | flex,
                 }) |
                 flex;

  Element header = hbox({
      text("QUERY CONSOLE") | bold | color(theme::kInk0()),
      text("   engine ") | color(theme::kInk3()),
      text(entropy::version()) | color(theme::kInk2()),
      filler(),
      text(c.db_note) | color(theme::kInk2()),
  });

  Element board = hbox({
      render_sidebar(c) | size(WIDTH, EQUAL, 26),
      text(" "),
      main | flex,
  });

  return vbox({
             header,
             separatorLight() | color(theme::kInk3()),
             board | flex,
             render_legend(),
         }) |
         theme::canvas_bg() | color(theme::kInk1());
}

}  // namespace

Component MakeQueryConsoleScreen() {
  auto state = std::make_shared<Console>();
  open_and_seed(*state);

  auto is_printable = [](const Event &e) -> bool {
    if (!e.is_character()) {
      return false;
    }
    const std::string &s = e.character();
    if (s.size() != 1) {
      return false;
    }
    const auto ch = static_cast<unsigned char>(s[0]);
    return ch >= 32 && ch < 127;
  };

  // The focus-aware Renderer overload reports Focusable()==true, which is what
  // lets this screen receive keyboard events once its tab is active (a bare
  // Renderer has no focusable child and would be skipped by the container).
  auto view = Renderer(
      [state](bool /*focused*/) { return render_console(*state); });

  return CatchEvent(view, [state, is_printable](const Event &e) -> bool {
    Console &c = *state;

    if (e == Event::Return) {
      run_query(c);
      return true;
    }
    if (e == Event::Backspace) {
      if (!c.sql.empty()) {
        c.sql.pop_back();
      }
      c.focus = Focus::kEditor;
      return true;
    }
    if (e == Event::F2) {
      c.tab = Tab::kQuery;
      return true;
    }
    if (e == Event::F3) {
      c.tab = Tab::kHistory;
      return true;
    }
    if (e == Event::F4) {
      c.tab = Tab::kPlan;
      refresh_plan(c);
      return true;
    }

    // Arrow keys navigate the result grid when there is one to walk.
    if (c.tab == Tab::kQuery && c.grid.ok && c.grid.is_query &&
        !c.grid.rows.empty()) {
      const int nrow = static_cast<int>(c.grid.rows.size());
      const int ncol = static_cast<int>(c.grid.cols.size());
      if (e == Event::ArrowUp) {
        c.focus = Focus::kResults;
        c.sel_row = std::max(0, c.sel_row - 1);
        return true;
      }
      if (e == Event::ArrowDown) {
        c.focus = Focus::kResults;
        c.sel_row = std::min(nrow - 1, c.sel_row + 1);
        return true;
      }
      if (e == Event::ArrowLeft) {
        c.focus = Focus::kResults;
        c.sel_col = std::max(0, c.sel_col - 1);
        return true;
      }
      if (e == Event::ArrowRight) {
        c.focus = Focus::kResults;
        c.sel_col = std::min(ncol - 1, c.sel_col + 1);
        return true;
      }
    }

    if (is_printable(e)) {
      c.sql += e.character();
      c.focus = Focus::kEditor;
      return true;
    }
    return false;
  });
}

}  // namespace entropy::tui
