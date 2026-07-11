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
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "common/config.hpp"
#include "entropy/entropy.hpp"
#include "storage/disk_manager.hpp"
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

  // Derive the WAL path exactly the way DatabaseImpl does, so this test can
  // never silently diverge from the path the engine actually writes.
  [[nodiscard]] std::string wal_path() const {
    return path_ + config::kWALFileExtension;
  }

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
      // The conflict aborted the transaction, but the binding stays in a
      // failed state until this thread acknowledges it with rollback —
      // statements must not silently fall back to autocommit.
      EXPECT_TRUE(db.in_transaction());
      auto rejected = db.execute("UPDATE t SET v = 999 WHERE id = 1");
      EXPECT_FALSE(rejected.ok())
          << "statement accepted on an aborted transaction";
      Status rolled_back = db.rollback();
      EXPECT_TRUE(rolled_back.ok()) << rolled_back.to_string();
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
// Snapshot isolation: deletes and relocations (ghost reads)
// ─────────────────────────────────────────────────────────────────────────────

// An uncommitted DELETE must not erase the row from a concurrent snapshot: the
// deleter frees the heap slot at execute time, but the retained before-image
// stays visible through the version chain until the delete commits — and a
// rollback makes it plainly visible again.
TEST_F(TransactionApiTest, UncommittedDeleteKeepsRowVisibleToOlderSnapshot) {
  Database db(path_);
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER, v INTEGER)").ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (1, 10)").ok());

  // Snapshot A (main thread) predates the delete.
  ASSERT_TRUE(db.begin_transaction().ok());
  EXPECT_EQ(count_rows(db, "t"), 1u);

  std::atomic<bool> deleted{false};
  std::atomic<bool> resume{false};
  std::thread deleter([&] {
    ASSERT_TRUE(db.begin_transaction().ok());
    auto result = db.execute("DELETE FROM t WHERE id = 1");
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    ASSERT_EQ(result.affected_rows(), 1u);
    deleted.store(true);
    while (!resume.load()) {
      std::this_thread::yield();
    }
    ASSERT_TRUE(db.rollback().ok());
  });

  while (!deleted.load()) {
    std::this_thread::yield();
  }

  // The delete is uncommitted: A must still see the row (this was the
  // reviewer's dirty-read-of-absence — a never-durable delete erased a
  // committed row from a concurrent snapshot).
  EXPECT_EQ(count_rows(db, "t"), 1u)
      << "uncommitted DELETE hid the row from an older snapshot";

  resume.store(true);
  deleter.join();

  // The delete rolled back: still visible to A, and to a fresh snapshot.
  EXPECT_EQ(count_rows(db, "t"), 1u);
  ASSERT_TRUE(db.commit().ok());
  EXPECT_EQ(count_rows(db, "t"), 1u);
}

// True SI: even after the deleter COMMITS, a still-open older snapshot keeps
// seeing the pre-image; only snapshots taken after the commit see it gone.
TEST_F(TransactionApiTest, CommittedDeleteKeepsRowVisibleToOpenOlderSnapshot) {
  Database db(path_);
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER, v INTEGER)").ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (1, 10)").ok());

  ASSERT_TRUE(db.begin_transaction().ok());
  EXPECT_EQ(count_rows(db, "t"), 1u);

  std::thread deleter([&] {
    auto result = db.execute("DELETE FROM t WHERE id = 1"); // autocommit
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    ASSERT_EQ(result.affected_rows(), 1u);
  });
  deleter.join();

  // The deleter committed, but A's snapshot predates it.
  EXPECT_EQ(count_rows(db, "t"), 1u)
      << "committed DELETE erased the row from an older open snapshot";
  ASSERT_TRUE(db.commit().ok());

  // A fresh snapshot sees the delete.
  EXPECT_EQ(count_rows(db, "t"), 0u);
}

// A grow-update's relocation must not make the old row vanish from an older
// snapshot through its delete leg — before or after the writer commits.
TEST_F(TransactionApiTest, RelocatedUpdateKeepsOldRowVisibleToOlderSnapshot) {
  const std::string small(850, 'a');
  const std::string big(900, 'b');

  Database db(path_);
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER, v VARCHAR(1000))").ok());
  for (int i = 1; i <= 4; ++i) {
    ASSERT_TRUE(db.execute("INSERT INTO t VALUES (" + std::to_string(i) +
                           ", '" + small + "')")
                    .ok());
  }

  // Snapshot A predates the update.
  ASSERT_TRUE(db.begin_transaction().ok());
  auto pre = db.execute("SELECT v FROM t WHERE id = 1");
  ASSERT_TRUE(pre.ok());
  ASSERT_EQ(pre.row_count(), 1u);

  std::atomic<bool> updated{false};
  std::atomic<bool> resume{false};
  std::thread writer([&] {
    ASSERT_TRUE(db.begin_transaction().ok());
    auto result = db.execute("UPDATE t SET v = '" + big + "' WHERE id = 1");
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    ASSERT_EQ(result.affected_rows(), 1u);
    updated.store(true);
    while (!resume.load()) {
      std::this_thread::yield();
    }
    ASSERT_TRUE(db.commit().ok());
  });

  while (!updated.load()) {
    std::this_thread::yield();
  }

  // Uncommitted relocation: A must still see the old value at the old row.
  auto mid = db.execute("SELECT v FROM t WHERE id = 1");
  ASSERT_TRUE(mid.ok()) << mid.status().to_string();
  ASSERT_EQ(mid.row_count(), 1u)
      << "relocated row vanished from an older snapshot";
  EXPECT_EQ(mid.rows()[0][0].as_string(), small);
  EXPECT_EQ(count_rows(db, "t"), 4u) << "relocation duplicated rows";

  resume.store(true);
  writer.join();

  // Writer committed; A's snapshot still predates it.
  auto post = db.execute("SELECT v FROM t WHERE id = 1");
  ASSERT_TRUE(post.ok());
  ASSERT_EQ(post.row_count(), 1u)
      << "committed relocation erased the row from an older open snapshot";
  EXPECT_EQ(post.rows()[0][0].as_string(), small);
  EXPECT_EQ(count_rows(db, "t"), 4u);
  ASSERT_TRUE(db.commit().ok());

  // A fresh snapshot sees the new value, exactly once.
  auto fresh = db.execute("SELECT v FROM t WHERE id = 1");
  ASSERT_TRUE(fresh.ok());
  ASSERT_EQ(fresh.row_count(), 1u);
  EXPECT_EQ(fresh.rows()[0][0].as_string(), big);
  EXPECT_EQ(count_rows(db, "t"), 4u);
}

// Hammer the insert publication window: a rolled-back INSERT must never be
// observed by a concurrent reader, not even transiently (the reviewer caught
// the unfixed window at ~26k reads: heap bytes readable before their version
// existed). WAL off so thousands of iterations stay fast; the publication
// mechanics under test are WAL-independent.
TEST_F(TransactionApiTest, RolledBackInsertsNeverVisibleToConcurrentReaders) {
  DatabaseOptions options;
  options.enable_wal = false;
  Database db(path_, options);
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER)").ok());

  constexpr int kIterations = 10000;
  std::atomic<bool> done{false};
  std::atomic<size_t> reads{0};
  std::atomic<size_t> dirty_reads{0};

  std::thread writer([&] {
    for (int i = 0; i < kIterations; ++i) {
      Status begun = db.begin_transaction();
      ASSERT_TRUE(begun.ok()) << begun.to_string();
      auto ins = db.execute("INSERT INTO t VALUES (" + std::to_string(i) + ")");
      ASSERT_TRUE(ins.ok()) << ins.status().to_string();
      Status rolled = db.rollback();
      ASSERT_TRUE(rolled.ok()) << rolled.to_string();
    }
    done.store(true);
  });

  // Several concurrent readers: their autocommit commits also run version GC,
  // which prunes the rollback tombstones between writer iterations — exactly
  // the state in which an unordered insert publication would be observable.
  auto read_loop = [&] {
    while (!done.load()) {
      auto result = db.execute("SELECT * FROM t");
      ASSERT_TRUE(result.ok()) << result.status().to_string();
      reads.fetch_add(1);
      if (result.row_count() > 0) {
        dirty_reads.fetch_add(1);
      }
    }
  };
  std::thread r1(read_loop);
  std::thread r2(read_loop);
  read_loop();
  r1.join();
  r2.join();
  writer.join();

  EXPECT_EQ(dirty_reads.load(), 0u)
      << "observed uncommitted/rolled-back inserts in " << reads.load()
      << " reads";
  EXPECT_EQ(count_rows(db, "t"), 0u);
}

// Rolling back a DELETE must restore its row even when a concurrent INSERT
// raced for the freed slot (crash-safety F1). The reviewer's interleaving: A
// deletes id=1, physically freeing slot (P,0) while still holding its X-lock;
// B inserts into the same page; A rolls back. If B were allowed to reuse the
// slot the deleter's rollback would find it occupied and skip the restore,
// silently losing A's committed row. The insert's free-slot search must skip a
// slot carrying an uncommitted delete, so B lands elsewhere and BOTH rows
// survive.
TEST_F(TransactionApiTest, RolledBackDeleteSurvivesConcurrentSlotReuse) {
  Database db(path_);
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER, v INTEGER)").ok());
  // id=1 is the committed row that lands at slot (P,0).
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (1, 10)").ok());

  std::atomic<bool> deleted{false};
  std::atomic<bool> resume{false};
  std::thread deleter([&] {
    ASSERT_TRUE(db.begin_transaction().ok());
    auto del = db.execute("DELETE FROM t WHERE id = 1");
    ASSERT_TRUE(del.ok()) << del.status().to_string();
    ASSERT_EQ(del.affected_rows(), 1u);
    deleted.store(true);
    // Hold the X-lock on the freed slot open across the concurrent insert.
    while (!resume.load()) {
      std::this_thread::yield();
    }
    ASSERT_TRUE(db.rollback().ok());
  });

  while (!deleted.load()) {
    std::this_thread::yield();
  }

  // Concurrent INSERT into the same page while A's delete is uncommitted. With
  // the fix it lands on a fresh slot (never (P,0)); without it, it reuses (P,0),
  // places its bytes, and blocks on A's row lock until the rollback below.
  std::atomic<bool> insert_started{false};
  std::thread inserter([&] {
    insert_started.store(true);
    auto ins = db.execute("INSERT INTO t VALUES (2, 20)"); // autocommit
    ASSERT_TRUE(ins.ok()) << ins.status().to_string();
  });

  // Let the insert place its bytes (and, on the buggy path, block on the lock)
  // before the deleter rolls back — that ordering is what triggered the loss.
  while (!insert_started.load()) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  resume.store(true);
  deleter.join();
  inserter.join();

  // Both rows must be present: id=1 restored by the rollback, id=2 from the
  // concurrent insert. Under the bug, id=1 is gone (its slot was reused).
  auto result = db.execute("SELECT id FROM t ORDER BY id");
  ASSERT_TRUE(result.ok()) << result.status().to_string();
  ASSERT_EQ(result.row_count(), 2u)
      << "rollback lost a committed row to a concurrent slot reuse";
  EXPECT_EQ(result.rows()[0][0].as_int32(), 1);
  EXPECT_EQ(result.rows()[1][0].as_int32(), 2);

  // And it survives a clean reopen (the restore reached the heap).
  (void)db.close();
  Database reopened(path_);
  ASSERT_TRUE(reopened.is_open());
  EXPECT_EQ(count_rows(reopened, "t"), 2u);
}

// ─────────────────────────────────────────────────────────────────────────────
// Failed-transaction semantics
// ─────────────────────────────────────────────────────────────────────────────

// A failed statement aborts the explicit transaction but keeps it BOUND in a
// failed state: later statements are rejected (never silently autocommitted),
// commit errors and clears, rollback clears. The reviewer's repro exploited
// the old unbind-on-error: follow-up statements autocommitted and the final
// rollback() no-opped, leaving exactly the wrong rows behind.
TEST_F(TransactionApiTest, FailedStatementPoisonsExplicitTransaction) {
  Database db(path_);
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER, v VARCHAR(16))").ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (1, 'keep')").ok());

  // Variant 1: commit on a failed transaction errors and clears.
  ASSERT_TRUE(db.begin_transaction().ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (2, 'mine')").ok());
  auto bad = db.execute("INSERT INTO missing VALUES (1)");
  ASSERT_FALSE(bad.ok());

  EXPECT_TRUE(db.in_transaction()) << "error silently unbound the transaction";
  auto rejected = db.execute("INSERT INTO t VALUES (3, 'no')");
  EXPECT_FALSE(rejected.ok()) << "statement ran on an aborted transaction";
  EXPECT_EQ(rejected.status().code(), StatusCode::kAborted);
  rejected = db.execute("SELECT * FROM t");
  EXPECT_FALSE(rejected.ok()) << "read ran on an aborted transaction";

  Status committed = db.commit();
  EXPECT_FALSE(committed.ok()) << "commit succeeded on an aborted transaction";
  EXPECT_FALSE(db.in_transaction());

  // Everything from the failed transaction is gone; only 'keep' remains.
  auto result = db.execute("SELECT v FROM t");
  ASSERT_TRUE(result.ok()) << result.status().to_string();
  ASSERT_EQ(result.row_count(), 1u);
  EXPECT_EQ(result.rows()[0][0].as_string(), "keep");

  // Variant 2: rollback acknowledges the failure cleanly.
  ASSERT_TRUE(db.begin_transaction().ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (4, 'gone')").ok());
  ASSERT_FALSE(db.execute("SELECT * FROM missing").ok());
  EXPECT_TRUE(db.in_transaction());
  EXPECT_TRUE(db.rollback().ok());
  EXPECT_FALSE(db.in_transaction());
  EXPECT_EQ(count_rows(db, "t"), 1u);
}

// ─────────────────────────────────────────────────────────────────────────────
// DDL page reuse vs. recovery
// ─────────────────────────────────────────────────────────────────────────────

// Dropping a table frees its pages for reuse, but its WAL records remain. A
// crash after a new table reused a freed page must not resurrect the dropped
// table's rows into it (the reviewer's repro: table b contained a's two
// dropped rows and b's own committed row was gone, because redo replayed a's
// inserts into the zeroed reused page and b's insert then collided). The
// post-drop checkpoint advances the redo anchor past the stale records.
TEST_F(TransactionApiTest, DroppedTablePageReuseSurvivesCrashRecovery) {
  auto db1 = std::make_unique<Database>(path_);
  ASSERT_TRUE(db1->execute("CREATE TABLE a (id INTEGER, v VARCHAR(32))").ok());
  ASSERT_TRUE(
      db1->execute("INSERT INTO a VALUES (1, 'a-one'), (2, 'a-two')").ok());
  ASSERT_TRUE(db1->execute("DROP TABLE a").ok());

  // b's first heap page reuses a's freed page.
  ASSERT_TRUE(db1->execute("CREATE TABLE b (id INTEGER, v VARCHAR(32))").ok());
  ASSERT_TRUE(db1->execute("INSERT INTO b VALUES (10, 'b-row')").ok());

  // Crash: reopen without a clean close of db1.
  {
    Database db2(path_);
    ASSERT_TRUE(db2.is_open());
    auto result = db2.execute("SELECT * FROM b");
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    ASSERT_EQ(result.row_count(), 1u)
        << "recovery resurrected dropped rows into the reused page";
    EXPECT_EQ(result.rows()[0][0].as_int32(), 10);
    EXPECT_EQ(result.rows()[0][1].as_string(), "b-row");
    (void)db2.close();
  }

  db1.reset();
}

// A file-backed disk manager that can be armed to fail every page write, used
// to force the post-drop checkpoint's page flush to fail.
class FaultyDiskManager : public FileDiskManager {
public:
  using FileDiskManager::FileDiskManager;
  Status write_page(page_id_t page_id, const char *page_data) override {
    if (fail_writes.load()) {
      return Status::IOError("injected write fault");
    }
    return FileDiskManager::write_page(page_id, page_data);
  }
  std::atomic<bool> fail_writes{false};
};

// A DROP whose post-drop checkpoint FAILS must not degrade to the original
// page-reuse corruption (crash-safety F2). The freed pages' ids are held back
// (leaked) rather than returned to the free list, so a later CREATE cannot
// reuse them and a crash cannot replay the dropped table's inserts into the new
// table's page. Without the fix the pages are freed before the checkpoint and
// reused behind the I/O fault, reproducing F1's corruption.
TEST_F(TransactionApiTest, PostDropCheckpointFailureDoesNotCorruptPageReuse) {
  auto disk = std::make_shared<FaultyDiskManager>(path_, /*create_if_missing=*/true,
                                                  /*error_if_exists=*/false);

  auto db1 = std::make_unique<Database>(path_, disk);
  ASSERT_TRUE(db1->is_open());
  ASSERT_TRUE(db1->execute("CREATE TABLE a (id INTEGER, v VARCHAR(32))").ok());
  ASSERT_TRUE(
      db1->execute("INSERT INTO a VALUES (1, 'a-one'), (2, 'a-two')").ok());

  // A second table with an unflushed dirty page, so the checkpoint's page flush
  // actually attempts a write the fault can reject (a's own pages are discarded
  // from the pool before the flush and would not be written).
  ASSERT_TRUE(db1->execute("CREATE TABLE keep (id INTEGER)").ok());
  ASSERT_TRUE(db1->execute("INSERT INTO keep VALUES (7)").ok());

  // Arm the fault and drop: the checkpoint fails, so the drop reports the
  // failure loudly and leaks a's pages instead of freeing them.
  disk->fail_writes.store(true);
  auto dropped = db1->execute("DROP TABLE a");
  EXPECT_FALSE(dropped.ok())
      << "a failing post-drop checkpoint must surface, not silently degrade";
  disk->fail_writes.store(false);

  // A new table must NOT reuse a's leaked pages.
  ASSERT_TRUE(db1->execute("CREATE TABLE b (id INTEGER, v VARCHAR(32))").ok());
  ASSERT_TRUE(db1->execute("INSERT INTO b VALUES (10, 'b-row')").ok());

  // Crash: reopen over the same file through a fresh (non-faulty) manager.
  {
    Database db2(path_);
    ASSERT_TRUE(db2.is_open());
    auto result = db2.execute("SELECT * FROM b");
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    ASSERT_EQ(result.row_count(), 1u)
        << "recovery resurrected dropped rows into a reused page";
    EXPECT_EQ(result.rows()[0][0].as_int32(), 10);
    EXPECT_EQ(result.rows()[0][1].as_string(), "b-row");
    (void)db2.close();
  }

  db1.reset();
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
