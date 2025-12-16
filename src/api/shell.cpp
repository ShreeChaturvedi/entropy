/**
 * @file shell.cpp
 * @brief Interactive SQL shell
 */

#include <iostream>
#include <string>

#include "entropy/entropy.hpp"

namespace entropy {

void print_help() {
    std::cout << "Entropy SQL Shell\n"
              << "─────────────────────────────────────\n"
              << "Commands:\n"
              << "  .help     Show this help message\n"
              << "  .tables   List all tables\n"
              << "  .schema   Show table schema\n"
              << "  .quit     Exit the shell\n"
              << "\n"
              << "Enter SQL statements ending with ;\n";
}

void run_shell(Database& db) {
    std::cout << "Entropy v" << version() << " - SQL Shell\n";
    std::cout << "Type .help for usage hints.\n\n";

    std::string line;
    std::string query;

    while (true) {
        std::cout << (query.empty() ? "entropy> " : "      -> ");

        if (!std::getline(std::cin, line)) {
            break;
        }

        // Handle dot commands
        if (query.empty() && !line.empty() && line[0] == '.') {
            if (line == ".quit" || line == ".exit") {
                break;
            } else if (line == ".help") {
                print_help();
            } else if (line == ".tables") {
                std::cout << "(table listing not yet implemented)\n";
            } else {
                std::cout << "Unknown command: " << line << "\n";
            }
            continue;
        }

        query += line;
        query += " ";

        // Check if query is complete (ends with ;)
        if (!query.empty() && query.find(';') != std::string::npos) {
            auto result = db.execute(query);
            if (result.ok()) {
                if (result.has_rows()) {
                    // Print column headers
                    for (size_t i = 0; i < result.column_names().size(); ++i) {
                        if (i > 0) std::cout << " | ";
                        std::cout << result.column_names()[i];
                    }
                    std::cout << "\n";

                    // Print rows
                    for (const auto& row : result) {
                        for (size_t i = 0; i < row.size(); ++i) {
                            if (i > 0) std::cout << " | ";
                            std::cout << row[i].to_string();
                        }
                        std::cout << "\n";
                    }
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

    std::cout << "Goodbye!\n";
}

}  // namespace entropy

int main(int argc, char* argv[]) {
    std::string db_path = "entropy.db";

    if (argc > 1) {
        db_path = argv[1];
    }

    try {
        entropy::Database db(db_path);
        entropy::run_shell(db);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
