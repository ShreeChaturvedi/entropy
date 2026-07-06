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

        // Create a table, insert a row, and query it back
        auto create_result = db.execute("CREATE TABLE users (id INT, name VARCHAR(100))");
        if (!create_result.ok()) {
            std::cout << "CREATE TABLE error: " << create_result.status().to_string() << "\n";
            return 1;
        }

        auto insert_result = db.execute("INSERT INTO users VALUES (1, 'Alice')");
        if (!insert_result.ok()) {
            std::cout << "INSERT error: " << insert_result.status().to_string() << "\n";
            return 1;
        }

        auto result = db.execute("SELECT * FROM users");

        if (result.ok()) {
            std::cout << "Query executed successfully\n";
            for (const auto& row : result) {
                std::cout << "  id=" << row["id"].to_string()
                           << " name=" << row["name"].to_string() << "\n";
            }
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
