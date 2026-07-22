// Minimal downstream consumer: links against an installed Entropy via
// find_package(entropy) and exercises the public API surface only.
#include <iostream>

#include <entropy/entropy.hpp>

int main() {
    entropy::Database db("consumer_smoke.entropy");

    const auto created = db.execute("CREATE TABLE t (id INTEGER, v INTEGER)");
    const auto inserted = db.execute("INSERT INTO t VALUES (1, 42)");
    const auto selected = db.execute("SELECT v FROM t WHERE id = 1");

    std::cout << "entropy " << entropy::version() << " rows="
              << selected.row_count() << "\n";

    return (created.ok() && inserted.ok() && selected.ok()) ? 0 : 1;
}
