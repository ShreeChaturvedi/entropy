/**
 * @file basic_usage.cpp
 * @brief Basic usage example for Entropy database
 */

#include <iostream>

#include <entropy/entropy.hpp>

int main() {
    std::cout << "Entropy Database v" << entropy::version() << "\n\n";

    try {
        // Open or create a database
        entropy::Database db("example.entropy");

        std::cout << "Database opened: " << db.path() << "\n";

        // Execute some SQL (when implemented)
        auto result = db.execute("SELECT 1");

        if (result.ok()) {
            std::cout << "Query executed successfully\n";
        } else {
            std::cout << "Query error: " << result.status().to_string() << "\n";
        }

        // Transaction example
        std::cout << "\nTransaction example:\n";

        auto status = db.begin_transaction();
        std::cout << "  BEGIN: " << status.to_string() << "\n";

        status = db.commit();
        std::cout << "  COMMIT: " << status.to_string() << "\n";

        db.close();
        std::cout << "\nDatabase closed.\n";

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
