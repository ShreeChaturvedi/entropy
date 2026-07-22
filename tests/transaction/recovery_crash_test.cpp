/**
 * @file recovery_crash_test.cpp
 * @brief Component-level crash-recovery tests for the wave-2 transaction stack.
 *
 * Unlike the low-level recovery tests in transaction_test.cpp (which hand-write
 * individual LogRecords), these drive REAL transactions through the assembled
 * stack — TransactionManager + WALManager + TableHeap + BufferPoolManager — and
 * then simulate a crash by abandoning the in-memory state without a clean
 * shutdown, so only WAL-forced bytes are durable. Recovery is then run over the
 * same files through a fresh stack and every assertion is an OUTCOME: which
 * rows are present, that their bytes are byte-for-byte correct, that aborted or
 * never-committed rows are gone, and that a second recovery is idempotent.
 *
 * Crash model. BufferPoolManager flushes dirty pages in its destructor, so
 * destroying the writer would durably persist data pages and mask the crash.
 * Instead the writer stack is kept alive (parked in keep_alive_) while a second,
 * independent stack reopens the same files — the on-disk image a kill -9 would
 * have left, holding only the pages the engine explicitly forced (WAL on
 * commit/abort, and pages a test deliberately steals). All assertions read
 * through the recovery stack's own pool immediately after recover(), so they do
 * not depend on any later destructor ordering.
 *
 * NOTE. Crash points that involve a transaction which ABORTED cleanly before
 * the crash (and the double-crash that re-derives that state during recovery)
 * are intentionally not covered here: driving them through the real stack trips
 * a redo-gating defect filed as #75 (redo resurrects a row a committed abort had
 * already removed, because the forward write path never stamps page LSNs). Once
 * that is fixed those points can be added.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "catalog/schema.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/disk_manager.hpp"
#include "storage/page.hpp"
#include "storage/table_heap.hpp"
#include "storage/table_page.hpp"
#include "storage/tuple.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/mvcc.hpp"
#include "transaction/recovery.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction/version_store.hpp"
#include "transaction/wal.hpp"

// Process id for per-process-unique temp directories (guarded: no POSIX header
// leaks into the Windows CI build).
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

namespace entropy {
namespace {

[[nodiscard]] long long current_process_id() {
#ifdef _WIN32
  return static_cast<long long>(::_getpid());
#else
  return static_cast<long long>(::getpid());
#endif
}

[[nodiscard]] std::filesystem::path make_unique_test_dir(const std::string &tag) {
  static std::atomic<uint64_t> counter{0};
  const ::testing::TestInfo *info =
      ::testing::UnitTest::GetInstance()->current_test_info();
  std::string leaf = tag;
  leaf += '_';
  leaf += info ? info->test_suite_name() : "nosuite";
  leaf += '_';
  leaf += info ? info->name() : "notest";
  for (char &c : leaf) {
    if (c == '/' || c == '\\' || c == ':') {
      c = '_';
    }
  }
  leaf += "_p" + std::to_string(current_process_id());
  leaf += "_" + std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
  std::filesystem::path dir = std::filesystem::temp_directory_path() / leaf;
  std::filesystem::create_directories(dir);
  return dir;
}

constexpr oid_t kTableOid = 1;

// A fully wired writer: the transaction manager plus everything abort/commit
// need to log, undo, and force pages.
struct WriterStack {
  std::shared_ptr<DiskManager> disk;
  std::shared_ptr<BufferPoolManager> pool;
  std::shared_ptr<WALManager> wal;
  std::shared_ptr<MVCCManager> mvcc;
  std::shared_ptr<VersionStore> vstore;
  std::unique_ptr<LockManager> lock;
  std::shared_ptr<TableHeap> heap;
  std::unique_ptr<TransactionManager> tm;
};

// A recovery stack: just the page store and WAL the RecoveryManager drives.
struct RecoverStack {
  std::shared_ptr<DiskManager> disk;
  std::shared_ptr<BufferPoolManager> pool;
  std::shared_ptr<WALManager> wal;
};

class RecoveryCrashTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ = make_unique_test_dir("entropy_recovery_crash");
    db_file_ = (test_dir_ / "data.db").string();
    wal_file_ = (test_dir_ / "test.wal").string();
    schema_ = Schema({
        Column("id", TypeId::INTEGER),
        Column("name", TypeId::VARCHAR, 64),
    });
  }

  void TearDown() override {
    // Drop the parked stacks (their disk handles) before removing the files so
    // Windows, which refuses to delete open files, can clean up too.
    keep_alive_.clear();
    std::error_code ec;
    std::filesystem::remove_all(test_dir_, ec);
  }

  // Build a fully wired writer bound to this test's files.
  std::shared_ptr<WriterStack> make_writer() {
    auto s = std::make_shared<WriterStack>();
    s->disk = std::make_shared<FileDiskManager>(db_file_);
    s->pool = std::make_shared<BufferPoolManager>(16, s->disk);
    s->wal = std::make_shared<WALManager>(wal_file_);
    s->mvcc = std::make_shared<MVCCManager>();
    s->vstore = std::make_shared<VersionStore>(*s->mvcc);
    s->lock = std::make_unique<LockManager>(false, 100);
    s->heap = std::make_shared<TableHeap>(s->pool);
    s->tm = std::make_unique<TransactionManager>(s->wal);
    s->tm->set_lock_manager(s->lock.get());
    s->tm->set_mvcc(s->mvcc);
    s->tm->set_version_store(s->vstore);
    s->tm->set_buffer_pool(s->pool.get());
    TableHeap *heap = s->heap.get();
    s->tm->set_table_resolver(
        [heap](oid_t oid) -> TableHeap * { return oid == kTableOid ? heap : nullptr; });
    return s;
  }

  // Reopen the page store + WAL as a crash would leave them.
  std::shared_ptr<RecoverStack> make_recover_stack() {
    auto s = std::make_shared<RecoverStack>();
    s->disk = std::make_shared<FileDiskManager>(db_file_);
    s->pool = std::make_shared<BufferPoolManager>(16, s->disk);
    s->wal = std::make_shared<WALManager>(wal_file_);
    return s;
  }

  // Park a stack so its buffer pool never flushes on destruction (crash model).
  template <typename StackPtr> void crash_keeping_alive(StackPtr s) {
    keep_alive_.push_back(std::move(s));
  }

  Tuple make_tuple(int32_t id, const std::string &name) {
    return Tuple({TupleValue(id), TupleValue(name)}, schema_);
  }
  static std::vector<char> tuple_bytes(const Tuple &t) {
    return std::vector<char>(t.data(), t.data() + t.size());
  }
  static std::span<const char> span_of(const std::vector<char> &v) {
    return std::span<const char>(v.data(), v.size());
  }

  // Insert a tuple through the heap and log it against txn (the real write
  // path). Returns the RID the heap chose.
  RID insert_logged(WriterStack &s, Transaction *txn, const Tuple &t) {
    RID rid;
    EXPECT_TRUE(s.heap->insert_tuple(t, &rid).ok());
    (void)s.tm->log_insert(txn, kTableOid, rid, tuple_bytes(t));
    return rid;
  }

  // Read the raw record bytes stored at rid, or nullopt if the slot is empty.
  static std::optional<std::vector<char>> record_at(BufferPoolManager &pool,
                                                     const RID &rid) {
    Page *page = pool.fetch_page(rid.page_id);
    if (page == nullptr) {
      return std::nullopt;
    }
    TablePage table_page(page);
    std::span<const char> rec = table_page.get_record(rid.slot_id);
    std::optional<std::vector<char>> out;
    if (!rec.empty()) {
      out = std::vector<char>(rec.begin(), rec.end());
    }
    pool.unpin_page(rid.page_id, false);
    return out;
  }

  std::filesystem::path test_dir_;
  std::string db_file_;
  std::string wal_file_;
  Schema schema_;
  std::vector<std::shared_ptr<void>> keep_alive_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Crash point: after WAL COMMIT flush, before any data page reached disk.
// The committed rows live only in the WAL; recovery must redo them onto real
// pages, byte-for-byte, hand back a next txn id that cannot alias them, and a
// second recovery over the recovered image must change nothing (idempotence).
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(RecoveryCrashTest, CommittedInsertsRedoneAfterCrashBeforePageFlush) {
  std::vector<RID> rids;
  std::vector<std::vector<char>> expected;

  auto writer = make_writer();
  {
    Transaction *txn = writer->tm->begin();
    for (int i = 1; i <= 4; ++i) {
      Tuple t = make_tuple(i, "row" + std::to_string(i));
      rids.push_back(insert_logged(*writer, txn, t));
      expected.push_back(tuple_bytes(t));
    }
    writer->tm->commit(txn); // forces WAL; data pages stay dirty in the pool
  }
  // Crash: abandon the writer WITHOUT flushing its pages.
  crash_keeping_alive(writer);

  auto rec = make_recover_stack();
  RecoveryManager recovery(rec->pool, rec->wal, rec->disk);
  ASSERT_TRUE(recovery.recover().ok());

  // Every committed row is present at its exact RID and byte-correct.
  for (size_t i = 0; i < rids.size(); ++i) {
    auto got = record_at(*rec->pool, rids[i]);
    ASSERT_TRUE(got.has_value()) << "row " << i << " missing after redo";
    EXPECT_EQ(*got, expected[i]) << "row " << i << " not byte-correct";
  }
  // The committed transaction is not a loser, and its id cannot be reissued.
  EXPECT_TRUE(recovery.active_txn_table().empty());
  EXPECT_GT(recovery.next_txn_id(), 1u);

  // Second recovery over the same pool (redo stamped the page LSNs) is a genuine
  // no-op, and the rows are unchanged.
  RecoveryManager recovery2(rec->pool, rec->wal, rec->disk);
  ASSERT_TRUE(recovery2.recover().ok());
  EXPECT_EQ(recovery2.redo_count(), 0u);
  EXPECT_EQ(recovery2.undo_count(), 0u);
  for (size_t i = 0; i < rids.size(); ++i) {
    auto got = record_at(*rec->pool, rids[i]);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, expected[i]);
  }
  crash_keeping_alive(rec);
}

// ─────────────────────────────────────────────────────────────────────────────
// Crash point: mid-transaction, no COMMIT ever written. The rows were logged
// and may even sit dirty in the pool, but the transaction is a loser: recovery
// must undo it, leaving no trace.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(RecoveryCrashTest, UncommittedInsertsUndoneAfterCrash) {
  std::vector<RID> rids;

  auto writer = make_writer();
  {
    Transaction *txn = writer->tm->begin();
    for (int i = 1; i <= 3; ++i) {
      Tuple t = make_tuple(i, "ghost" + std::to_string(i));
      rids.push_back(insert_logged(*writer, txn, t));
    }
    // Force the WAL so the loser's records are durable (as a buffer-full flush
    // or checkpoint would); the data pages are then lost with the crash. Repeat
    // history re-creates the rows during redo, and undo must roll every one of
    // them back because the transaction never committed.
    ASSERT_TRUE(writer->wal->flush().ok());
    // No commit — crash.
  }
  crash_keeping_alive(writer);

  auto rec = make_recover_stack();
  RecoveryManager recovery(rec->pool, rec->wal, rec->disk);
  ASSERT_TRUE(recovery.recover().ok());

  // The loser was identified and every one of its rows is gone.
  EXPECT_FALSE(recovery.active_txn_table().empty());
  for (size_t i = 0; i < rids.size(); ++i) {
    EXPECT_FALSE(record_at(*rec->pool, rids[i]).has_value())
        << "uncommitted row " << i << " survived recovery";
  }
  crash_keeping_alive(rec);
}

// ─────────────────────────────────────────────────────────────────────────────
// One crash, two fates. Two transactions interleave on distinct rows: the one
// that committed survives byte-correct, the one still open at the crash is
// rolled back with no trace.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(RecoveryCrashTest, CommittedSurvivesAndConcurrentLoserIsRolledBack) {
  RID committed_rid;
  std::vector<char> committed_bytes;
  RID loser_rid;

  auto writer = make_writer();
  {
    // Two transactions running at the same time.
    Transaction *winner = writer->tm->begin();
    Transaction *loser = writer->tm->begin();

    Tuple w = make_tuple(1, "winner");
    committed_rid = insert_logged(*writer, winner, w);
    committed_bytes = tuple_bytes(w);

    Tuple g = make_tuple(2, "ghost");
    loser_rid = insert_logged(*writer, loser, g);

    // Winner commits (forcing the WAL up to its COMMIT, which also makes the
    // loser's earlier records durable). Loser never commits.
    writer->tm->commit(winner);
  }
  crash_keeping_alive(writer);

  auto rec = make_recover_stack();
  RecoveryManager recovery(rec->pool, rec->wal, rec->disk);
  ASSERT_TRUE(recovery.recover().ok());

  auto winner_row = record_at(*rec->pool, committed_rid);
  ASSERT_TRUE(winner_row.has_value()) << "committed row lost";
  EXPECT_EQ(*winner_row, committed_bytes);
  EXPECT_TRUE(recovery.committed_txns().count(1) > 0);

  EXPECT_FALSE(record_at(*rec->pool, loser_rid).has_value())
      << "concurrent loser's row survived recovery";
  EXPECT_TRUE(recovery.active_txn_table().count(2) > 0);
  crash_keeping_alive(rec);
}

// ─────────────────────────────────────────────────────────────────────────────
// Crash point: a committed UPDATE whose after-image never reached its page.
// Redo must restore the NEW bytes, not the pre-update base.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(RecoveryCrashTest, CommittedUpdateRedoneWithAfterImage) {
  RID rid;
  std::vector<char> v2_bytes;

  auto writer = make_writer();
  {
    // Base row, committed (WAL forced) but its page left dirty and lost.
    Transaction *t1 = writer->tm->begin();
    Tuple v1 = make_tuple(1, "v1");
    rid = insert_logged(*writer, t1, v1);
    writer->tm->commit(t1);

    // Update in place, committed but its page never reached disk either. v2's
    // name is the same length as v1's so the heap updates the slot in place
    // rather than relocating the tuple to a new RID.
    Transaction *t2 = writer->tm->begin();
    Tuple v2 = make_tuple(1, "vv");
    std::vector<char> v1_bytes = tuple_bytes(v1);
    v2_bytes = tuple_bytes(v2);
    ASSERT_TRUE(writer->heap->update_tuple(v2, rid).ok());
    (void)writer->tm->log_update(t2, kTableOid, rid, v1_bytes, v2_bytes);
    writer->tm->commit(t2);
  }
  crash_keeping_alive(writer);

  auto rec = make_recover_stack();
  RecoveryManager recovery(rec->pool, rec->wal, rec->disk);
  ASSERT_TRUE(recovery.recover().ok());

  auto got = record_at(*rec->pool, rid);
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, v2_bytes) << "redo must reinstate the committed after-image";
  crash_keeping_alive(rec);
}

} // namespace
} // namespace entropy
