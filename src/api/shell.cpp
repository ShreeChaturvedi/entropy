/**
 * @file shell.cpp
 * @brief Interactive SQL shell
 */

#include <iomanip>
#include <iostream>
#include <string>

#include "entropy/entropy.hpp"

namespace entropy {

void print_banner() {
  std::cout << "\n"
            << "  ENTROPY Database Engine v" << version() << "\n"
            << "  =====================================\n\n";
}

void print_help() {
  std::cout << "Entropy SQL Shell v" << version() << "\n"
            << "---------------------------------------------\n"
            << "Meta-Commands:\n"
            << "  .help              Show this help message\n"
            << "  .tables            List all tables\n"
            << "  .schema <TABLE>    Show table schema\n"
            << "  .clear             Clear the screen\n"
            << "  .quit              Exit the shell\n"
            << "\n"
            << "SQL Commands:\n"
            << "  SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE\n"
            << "  EXPLAIN SELECT     Show query execution plan\n"
            << "\n"
            << "Enter SQL statements ending with ;\n";
}

void print_table_header(const std::vector<std::string> &columns,
                        const std::vector<size_t> &widths) {
  std::cout << "+";
  for (size_t i = 0; i < columns.size(); ++i) {
    if (i > 0)
      std::cout << "+";
    std::cout << std::string(widths[i] + 2, '-');
  }
  std::cout << "+\n";

  std::cout << "|";
  for (size_t i = 0; i < columns.size(); ++i) {
    std::cout << " " << std::setw(static_cast<int>(widths[i])) << std::left
              << columns[i] << " |";
  }
  std::cout << "\n";

  std::cout << "+";
  for (size_t i = 0; i < columns.size(); ++i) {
    if (i > 0)
      std::cout << "+";
    std::cout << std::string(widths[i] + 2, '-');
  }
  std::cout << "+\n";
}

void print_table_footer(size_t num_columns, const std::vector<size_t> &widths) {
  std::cout << "+";
  for (size_t i = 0; i < num_columns; ++i) {
    if (i > 0)
      std::cout << "+";
    std::cout << std::string(widths[i] + 2, '-');
  }
  std::cout << "+\n";
}

void handle_tables([[maybe_unused]] Database &db) {
  std::cout << "(Use CREATE TABLE to create tables)\n";
  std::cout << "(Query tables with SELECT * FROM table_name)\n";
}

void handle_schema(Database &db, const std::string &table_name) {
  if (table_name.empty()) {
    std::cout << "Usage: .schema <TABLE_NAME>\n";
    return;
  }

  auto result = db.execute("SELECT * FROM " + table_name + " LIMIT 0;");
  if (!result.ok()) {
    std::cout << "Error: " << result.status().to_string() << "\n";
    return;
  }

  std::cout << "Table: " << table_name << "\n";
  std::cout << "Columns:\n";
  for (const auto &col : result.column_names()) {
    std::cout << "  " << col << "\n";
  }
}

void run_shell(Database &db) {
  print_banner();
  std::cout << "Type .help for usage hints.\n\n";

  std::string line;
  std::string query;

  while (true) {
    std::cout << (query.empty() ? "entropy> " : "      -> ");

    if (!std::getline(std::cin, line)) {
      break;
    }

    if (line.empty() && query.empty()) {
      continue;
    }

    if (query.empty() && !line.empty() && line[0] == '.') {
      if (line == ".quit" || line == ".exit" || line == ".q") {
        break;
      } else if (line == ".help" || line == ".h") {
        print_help();
      } else if (line == ".tables" || line == ".dt") {
        handle_tables(db);
      } else if (line.length() > 7 && line.substr(0, 7) == ".schema") {
        handle_schema(db, line.substr(8));
      } else if (line.length() > 2 && line.substr(0, 2) == ".d") {
        handle_schema(db, line.substr(3));
      } else if (line == ".clear" || line == ".c") {
        std::cout << "\033[2J\033[H";
      } else {
        std::cout << "Unknown command: " << line << "\n";
        std::cout << "Type .help for available commands.\n";
      }
      continue;
    }

    query += line;
    query += " ";

    if (!query.empty() && query.find(';') != std::string::npos) {
      auto result = db.execute(query);
      if (result.ok()) {
        if (result.has_rows()) {
          const auto &columns = result.column_names();

          std::vector<size_t> widths(columns.size());
          for (size_t i = 0; i < columns.size(); ++i) {
            widths[i] = columns[i].length();
          }
          for (const auto &row : result) {
            for (size_t i = 0; i < row.size(); ++i) {
              widths[i] = std::max(widths[i], row[i].to_string().length());
            }
          }

          print_table_header(columns, widths);
          for (const auto &row : result) {
            std::cout << "|";
            for (size_t i = 0; i < row.size(); ++i) {
              std::cout << " " << std::setw(static_cast<int>(widths[i]))
                        << std::left << row[i].to_string() << " |";
            }
            std::cout << "\n";
          }
          print_table_footer(columns.size(), widths);

          std::cout << result.row_count() << " row(s)\n";
        } else {
          std::cout << "OK (" << result.affected_rows()
                    << " row(s) affected)\n";
        }
      } else {
        std::cout << "Error: " << result.status().to_string() << "\n";
      }
      query.clear();
    }
  }

  std::cout << "\nGoodbye!\n";
}

} // namespace entropy

int main(int argc, char *argv[]) {
  std::string db_path = "entropy.db";

  if (argc > 1) {
    db_path = argv[1];
  }

  try {
    entropy::Database db(db_path);
    entropy::run_shell(db);
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
