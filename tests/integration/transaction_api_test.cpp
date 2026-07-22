/**
 * @file transaction_api_test.cpp
 * @brief End-to-end transaction semantics through the public Database API
 *
 * Exercises the full stack (parser -> binder -> executors -> transaction
 * manager -> WAL/recovery) rather than any component in isolation: rollback
 * atomicity, write-write conflicts, snapshot isolation, DatabaseOptions
 * handling, and crash recovery across a reopen.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "entropy/entropy.hpp"
#include "test_utils.hpp"

namespace entropy {
namespace {

class TransactionApiTest : public ::testing::Test {
protected:
  void SetUp() override {
    // TempFile generates a unique random path per instantiation, so these
    // tests are safe under a parallel ctest runner.
    temp_file_ = std::make_unique<test::TempFile>("txn_api_test_");
    path_ = temp_file_->string();
  }

  void TearDown() override { temp_file_.reset(); }

  [[nodiscard]] std::string wal_path() const { return path_ + ".wal"; }

  static size_t count_rows(Database &db, const std::string &table) {
    auto result = db.execute("SELECT * FROM " + table);
    EXPECT_TRUE(result.ok()) << result.status().to_string();
    return result.row_count();
  }

  std::unique_ptr<test::TempFile> temp_file_;
  std::string path_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Rollback atomicity
// ─────────────────────────────────────────────────────────────────────────────

// INSERT + rollback leaves no visible row, in this process and after a clean
// reopen (the undo must reach the heap, not just in-memory bookkeeping).
TEST_F(TransactionApiTest, RollbackLeavesNoVisibleRow) {
  {
    Database db(path_);
    ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER, v VARCHAR(32))").ok());

    ASSERT_TRUE(db.begin_transaction().ok());
    auto ins = db.execute("INSERT INTO t VALUES (1, 'doomed')");
    ASSERT_TRUE(ins.ok()) << ins.status().to_string();
    ASSERT_TRUE(db.rollback().ok());

    EXPECT_EQ(count_rows(db, "t"), 0u) << "rolled-back row still visible";
    (void)db.close();
  }

  // The row must also be gone after a full reopen (recovery replays the WAL).
  Database reopened(path_);
  ASSERT_TRUE(reopened.is_open());
  EXPECT_EQ(count_rows(reopened, "t"), 0u)
      << "rolled-back row resurrected across reopen";
}

// Rollback undoes UPDATE and DELETE too, restoring the pre-transaction rows.
TEST_F(TransactionApiTest, RollbackRestoresUpdatesAndDeletes) {
  Database db(path_);
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER, v VARCHAR(32))").ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two')").ok());

  ASSERT_TRUE(db.begin_transaction().ok());
  ASSERT_TRUE(db.execute("UPDATE t SET v = 'changed' WHERE id = 1").ok());
  ASSERT_TRUE(db.execute("DELETE FROM t WHERE id = 2").ok());
  ASSERT_TRUE(db.rollback().ok());

  auto result = db.execute("SELECT v FROM t ORDER BY id");
  ASSERT_TRUE(result.ok()) << result.status().to_string();
  ASSERT_EQ(result.row_count(), 2u);
  EXPECT_EQ(result.rows()[0][0].as_string(), "one");
  EXPECT_EQ(result.rows()[1][0].as_string(), "two");
}

// A committed transaction's rows survive commit and reopen.
TEST_F(TransactionApiTest, CommitPersistsAcrossReopen) {
  {
    Database db(path_);
    ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER)").ok());
    ASSERT_TRUE(db.begin_transaction().ok());
    ASSERT_TRUE(db.execute("INSERT INTO t VALUES (7)").ok());
    ASSERT_TRUE(db.commit().ok());
    EXPECT_EQ(count_rows(db, "t"), 1u);
    (void)db.close();
  }

  Database reopened(path_);
  ASSERT_TRUE(reopened.is_open());
  EXPECT_EQ(count_rows(reopened, "t"), 1u);
}

// A transaction sees its own uncommitted writes.
TEST_F(TransactionApiTest, TransactionSeesOwnWrites) {
  Database db(path_);
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER)").ok());

  ASSERT_TRUE(db.begin_transaction().ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (42)").ok());
  EXPECT_EQ(count_rows(db, "t"), 1u) << "own uncommitted insert invisible";
  ASSERT_TRUE(db.rollback().ok());
  EXPECT_EQ(count_rows(db, "t"), 0u);
}

// A grow-update that cannot fit in place relocates the row (heap delete +
// insert). That path is logged as delete+insert, so both rollback and crash
// recovery must reproduce the move exactly.
TEST_F(TransactionApiTest, GrowUpdateRelocationRollsBackAndRecovers) {
  const std::string small(850, 'a');
  const std::string big(900, 'b'); // larger than page 0's leftover free space

  auto db1 = std::make_unique<Database>(path_);
  ASSERT_TRUE(db1->execute("CREATE TABLE t (id INTEGER, v VARCHAR(1000))").ok());
  // Four ~850-byte rows fill page 0 far enough that growing one to 900 bytes
  // cannot happen in place and must relocate to a fresh page.
  for (int i = 1; i <= 4; ++i) {
    ASSERT_TRUE(db1->execute("INSERT INTO t VALUES (" + std::to_string(i) +
                             ", '" + small + "')")
                    .ok());
  }

  // Rolled-back grow-update: the row must return to its original value.
  ASSERT_TRUE(db1->begin_transaction().ok());
  auto grown = db1->execute("UPDATE t SET v = '" + big + "' WHERE id = 1");
  ASSERT_TRUE(grown.ok()) << grown.status().to_string();
  ASSERT_EQ(grown.affected_rows(), 1u);
  ASSERT_TRUE(db1->rollback().ok());

  auto check = db1->execute("SELECT v FROM t WHERE id = 1");
  ASSERT_TRUE(check.ok());
  ASSERT_EQ(check.row_count(), 1u);
  EXPECT_EQ(check.rows()[0][0].as_string(), small)
      << "rolled-back grow-update left the relocated value";
  EXPECT_EQ(count_rows(*db1, "t"), 4u);

  // Committed grow-update, then crash (reopen without clean close): recovery
  // must replay the relocation.
  ASSERT_TRUE(db1->begin_transaction().ok());
  grown = db1->execute("UPDATE t SET v = '" + big + "' WHERE id = 2");
  ASSERT_TRUE(grown.ok()) << grown.status().to_string();
  ASSERT_TRUE(db1->commit().ok());

  {
    Database db2(path_);
    ASSERT_TRUE(db2.is_open());
    EXPECT_EQ(count_rows(db2, "t"), 4u) << "relocation duplicated or lost rows";
    auto recovered = db2.execute("SELECT v FROM t WHERE id = 2");
    ASSERT_TRUE(recovered.ok());
    ASSERT_EQ(recovered.row_count(), 1u);
    EXPECT_EQ(recovered.rows()[0][0].as_string(), big);
    (void)db2.close();
  }

  db1.reset();
}

// ─────────────────────────────────────────────────────────────────────────────
// Write-write conflicts
// ─────────────────────────────────────────────────────────────────────────────

// Two threads updating the same row concurrently: exactly one wins, the other
// gets a clean conflict error, and the surviving value is untorn (one of the
// two written values, matching the winner).
TEST_F(TransactionApiTest, ConcurrentUpdatesOneWinnerOneConflict) {
  Database db(path_);
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER, v INTEGER)").ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (1, 0)").ok());

  std::atomic<int> ok_count{0};
  std::atomic<int> conflict_count{0};
  std::atomic<int> begun_count{0};

  auto update_from_thread = [&](int value) {
    // Explicit per-thread transaction: begin, update, commit. Both threads
    // take their snapshots BEFORE either updates (the spin barrier below), so
    // the transactions genuinely overlap: the second updater blocks on the
    // row lock until the first commits, then hits the first-updater-wins
    // check and aborts. Without the barrier the OS could serialize the two
    // transactions entirely, which legitimately succeeds under SI.
    Status begun = db.begin_transaction();
    ASSERT_TRUE(begun.ok()) << begun.to_string();
    begun_count.fetch_add(1);
    while (begun_count.load() < 2) {
      std::this_thread::yield();
    }
    auto result = db.execute("UPDATE t SET v = " + std::to_string(value) +
                             " WHERE id = 1");
    if (result.ok()) {
      Status committed = db.commit();
      ASSERT_TRUE(committed.ok()) << committed.to_string();
      ok_count.fetch_add(1);
    } else {
      EXPECT_EQ(result.status().code(), StatusCode::kAborted)
          << result.status().to_string();
      // The conflict already aborted the transaction; the thread's explicit
      // transaction is closed.
      EXPECT_FALSE(db.in_transaction());
      conflict_count.fetch_add(1);
    }
  };

  std::thread t1(update_from_thread, 111);
  std::thread t2(update_from_thread, 222);
  t1.join();
  t2.join();

  EXPECT_EQ(ok_count.load(), 1) << "expected exactly one winning update";
  EXPECT_EQ(conflict_count.load(), 1) << "expected exactly one conflict";

  // No torn state: the row holds exactly the winner's value.
  auto result = db.execute("SELECT v FROM t WHERE id = 1");
  ASSERT_TRUE(result.ok()) << result.status().to_string();
  ASSERT_EQ(result.row_count(), 1u);
  const int32_t v = result.rows()[0][0].as_int32();
  EXPECT_TRUE(v == 111 || v == 222) << "torn value " << v;
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot isolation
// ─────────────────────────────────────────────────────────────────────────────

// A transaction's reads come from its snapshot: an insert committed after the
// snapshot was taken stays invisible until the reader's transaction ends; a
// transaction beginning after the commit sees it.
TEST_F(TransactionApiTest, SnapshotIsolationHidesLaterCommit) {
  Database db(path_);
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER)").ok());

  // Transaction A: snapshot taken now (main thread).
  ASSERT_TRUE(db.begin_transaction().ok());
  EXPECT_EQ(count_rows(db, "t"), 0u);

  // Transaction B: autocommit insert from another thread (transactions are
  // per-thread, so this is an independent transaction), committed before A
  // reads again.
  std::thread b([&db] {
    auto result = db.execute("INSERT INTO t VALUES (99)");
    ASSERT_TRUE(result.ok()) << result.status().to_string();
  });
  b.join();

  // A's snapshot predates B's commit: the row must stay invisible to A.
  EXPECT_EQ(count_rows(db, "t"), 0u)
      << "snapshot read saw a commit made after the snapshot";
  ASSERT_TRUE(db.commit().ok());

  // A new snapshot (autocommit SELECT) begins after B committed: row visible.
  EXPECT_EQ(count_rows(db, "t"), 1u);
}

// Snapshot reads return the before-image while a concurrent transaction has
// an uncommitted in-place update.
TEST_F(TransactionApiTest, SnapshotReadSeesBeforeImageOfUncommittedUpdate) {
  Database db(path_);
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER, v INTEGER)").ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (1, 10)").ok());

  // Writer (worker thread): uncommitted update held open.
  std::atomic<bool> updated{false};
  std::atomic<bool> reader_done{false};
  std::thread writer([&] {
    ASSERT_TRUE(db.begin_transaction().ok());
    auto result = db.execute("UPDATE t SET v = 20 WHERE id = 1");
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    updated.store(true);
    while (!reader_done.load()) {
      std::this_thread::yield();
    }
    ASSERT_TRUE(db.commit().ok());
  });

  while (!updated.load()) {
    std::this_thread::yield();
  }

  // Reader (main thread, autocommit): must see the committed before-image,
  // not the writer's uncommitted bytes.
  auto result = db.execute("SELECT v FROM t WHERE id = 1");
  ASSERT_TRUE(result.ok()) << result.status().to_string();
  ASSERT_EQ(result.row_count(), 1u);
  EXPECT_EQ(result.rows()[0][0].as_int32(), 10) << "dirty read of v=20";
  reader_done.store(true);
  writer.join();

  // After the writer commits, a fresh snapshot sees the new value.
  result = db.execute("SELECT v FROM t WHERE id = 1");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result.row_count(), 1u);
  EXPECT_EQ(result.rows()[0][0].as_int32(), 20);
}

// ─────────────────────────────────────────────────────────────────────────────
// DatabaseOptions
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(TransactionApiTest, UnsupportedPageSizeFailsLoudly) {
  DatabaseOptions options;
  options.page_size = 8192;
  Database db(path_, options);

  EXPECT_FALSE(db.is_open());
  auto result = db.execute("CREATE TABLE t (id INTEGER)");
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.status().message().find("page_size"), std::string_view::npos)
      << "error does not name the offending option: "
      << result.status().to_string();
}

TEST_F(TransactionApiTest, DisabledWalWritesNoWalFile) {
  DatabaseOptions options;
  options.enable_wal = false;
  {
    Database db(path_, options);
    ASSERT_TRUE(db.is_open());
    ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER)").ok());
    ASSERT_TRUE(db.execute("INSERT INTO t VALUES (1)").ok());
    EXPECT_EQ(count_rows(db, "t"), 1u);
    (void)db.close();
  }
  EXPECT_FALSE(std::filesystem::exists(wal_path()))
      << "enable_wal=false still produced a WAL file";
}

TEST_F(TransactionApiTest, ErrorIfExistsHonored) {
  { // Create the database file.
    Database db(path_);
    ASSERT_TRUE(db.is_open());
    (void)db.close();
  }

  DatabaseOptions options;
  options.error_if_exists = true;
  Database db(path_, options);
  EXPECT_FALSE(db.is_open());
}

TEST_F(TransactionApiTest, CreateIfMissingFalseFailsOnMissingFile) {
  DatabaseOptions options;
  options.create_if_missing = false;
  Database db(path_, options);
  EXPECT_FALSE(db.is_open());
}

TEST_F(TransactionApiTest, StrictModeRejectsMismatchedInsertTypes) {
  DatabaseOptions strict;
  strict.strict_mode = true;
  Database db(path_, strict);
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER, v VARCHAR(16))").ok());

  // Kind mismatches are rejected...
  auto result = db.execute("INSERT INTO t VALUES ('oops', 'x')");
  EXPECT_FALSE(result.ok()) << "string accepted into INTEGER in strict mode";
  result = db.execute("INSERT INTO t VALUES (1, 2)");
  EXPECT_FALSE(result.ok()) << "integer accepted into VARCHAR in strict mode";

  // ...while well-typed inserts and NULLs pass.
  result = db.execute("INSERT INTO t VALUES (1, 'ok')");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(count_rows(db, "t"), 1u);
}

TEST_F(TransactionApiTest, NonStrictModeCoercesInsertTypes) {
  Database db(path_); // strict_mode defaults to false
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER, v VARCHAR(16))").ok());
  auto result = db.execute("INSERT INTO t VALUES (1, 'ok')");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
}

// ─────────────────────────────────────────────────────────────────────────────
// Crash recovery through the public API
// ─────────────────────────────────────────────────────────────────────────────

// Commit a transaction, then reopen the database WITHOUT a clean close of the
// first handle (its dirty pages never flush — the WAL alone carries the
// committed rows). The committed rows must be recovered; an uncommitted
// transaction's row must be absent.
TEST_F(TransactionApiTest, CrashRecoveryKeepsCommittedDropsUncommitted) {
  // db1 is intentionally NOT destroyed before the reopen: destroying it would
  // run the clean-close flush, defeating the crash simulation. It is released
  // only after all assertions, at which point the test file is discarded.
  auto db1 = std::make_unique<Database>(path_);
  ASSERT_TRUE(db1->is_open());
  ASSERT_TRUE(db1->execute("CREATE TABLE t (id INTEGER, v VARCHAR(32))").ok());

  // Uncommitted transaction (main thread): its WAL records become durable
  // when the later commit below flushes the log, but no COMMIT follows them.
  ASSERT_TRUE(db1->begin_transaction().ok());
  ASSERT_TRUE(db1->execute("INSERT INTO t VALUES (2, 'uncommitted')").ok());

  // Committed autocommit insert from another thread (independent txn). Its
  // commit force-flushes the WAL, making both transactions' records durable.
  std::thread committer([&db1] {
    auto result = db1->execute("INSERT INTO t VALUES (1, 'committed')");
    ASSERT_TRUE(result.ok()) << result.status().to_string();
  });
  committer.join();

  // "Crash": reopen the same path while db1 still holds its dirty pages.
  // Recovery must replay the committed insert and roll back the loser.
  {
    Database db2(path_);
    ASSERT_TRUE(db2.is_open());
    auto result = db2.execute("SELECT v FROM t");
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    ASSERT_EQ(result.row_count(), 1u)
        << "recovery kept the wrong number of rows";
    EXPECT_EQ(result.rows()[0][0].as_string(), "committed");
    (void)db2.close();
  }

  // Release the "crashed" handle last (after assertions) so its file handles
  // close before TempFile cleanup (required on Windows).
  db1.reset();
}

} // namespace
} // namespace entropy
