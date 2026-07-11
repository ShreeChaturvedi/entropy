/**
 * @file txn_lock_integration_test.cpp
 * @brief Component tests where the LockManager, TransactionManager, and
 *        TableHeap interact: deadlock-victim data integrity and lock release on
 *        transaction end.
 *
 * These assert OUTCOMES a caller can observe — the value a survivor reads, and
 * whether a resource is lockable again — rather than internal counters. The
 * load-bearing property is ordering: a wait-die victim's write-set undo must
 * complete BEFORE its locks are released, so a survivor that then locks the row
 * can never read the victim's rolled-back bytes.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "catalog/schema.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/disk_manager.hpp"
#include "storage/table_heap.hpp"
#include "storage/tuple.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction/wal.hpp"

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
constexpr oid_t kAuxResource = 2; // a second lockable resource, no heap backing

class TxnLockIntegrationTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ = make_unique_test_dir("entropy_txnlock");
    db_file_ = (test_dir_ / "data.db").string();
    wal_file_ = (test_dir_ / "test.wal").string();

    disk_ = std::make_shared<FileDiskManager>(db_file_);
    pool_ = std::make_shared<BufferPoolManager>(16, disk_);
    heap_ = std::make_shared<TableHeap>(pool_);
    schema_ = Schema({
        Column("id", TypeId::INTEGER),
        Column("name", TypeId::VARCHAR, 64),
    });

    wal_ = std::make_shared<WALManager>(wal_file_);
    // Deadlock detection ON, long timeout: a resolution that completes quickly
    // proves detection broke the cycle, not the fallback timeout.
    lock_ = std::make_unique<LockManager>(true, /*timeout_ms=*/5000);
    tm_ = std::make_unique<TransactionManager>(wal_);
    tm_->set_lock_manager(lock_.get());
    tm_->set_buffer_pool(pool_.get());
    TableHeap *heap = heap_.get();
    tm_->set_table_resolver(
        [heap](oid_t oid) -> TableHeap * { return oid == kTableOid ? heap : nullptr; });
  }

  void TearDown() override {
    tm_.reset();
    lock_.reset();
    wal_.reset();
    heap_.reset();
    pool_.reset();
    disk_.reset();
    std::error_code ec;
    std::filesystem::remove_all(test_dir_, ec);
  }

  Tuple make_tuple(int32_t id, const std::string &name) {
    return Tuple({TupleValue(id), TupleValue(name)}, schema_);
  }
  static std::vector<char> tuple_bytes(const Tuple &t) {
    return std::vector<char>(t.data(), t.data() + t.size());
  }

  // Insert one committed row and return its RID.
  RID seed_committed_row(int32_t id, const std::string &name) {
    Transaction *setup = tm_->begin();
    Tuple t = make_tuple(id, name);
    RID rid;
    EXPECT_TRUE(heap_->insert_tuple(t, &rid).ok());
    (void)tm_->log_insert(setup, kTableOid, rid, tuple_bytes(t));
    tm_->commit(setup);
    return rid;
  }

  std::string read_name(const RID &rid) {
    Tuple out;
    EXPECT_TRUE(heap_->get_tuple(rid, &out).ok());
    return out.get_value(schema_, 1).as_string();
  }

  std::filesystem::path test_dir_;
  std::string db_file_;
  std::string wal_file_;
  std::shared_ptr<DiskManager> disk_;
  std::shared_ptr<BufferPoolManager> pool_;
  std::shared_ptr<TableHeap> heap_;
  Schema schema_;
  std::shared_ptr<WALManager> wal_;
  std::unique_ptr<LockManager> lock_;
  std::unique_ptr<TransactionManager> tm_;
};

// ─────────────────────────────────────────────────────────────────────────────
// A deadlock victim's write is fully undone before any survivor can lock the
// row, so the survivor reads the committed value, never the victim's dirty one.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(TxnLockIntegrationTest, DeadlockVictimWritesUndoneBeforeSurvivorLocks) {
  const RID rid = seed_committed_row(1, "v0");
  const std::vector<char> v0 = tuple_bytes(make_tuple(1, "v0"));

  // Older (smaller id) begins first, younger second.
  Transaction *older = tm_->begin();
  Transaction *younger = tm_->begin();
  ASSERT_LT(older->txn_id(), younger->txn_id());

  // Younger grabs the row (X) and dirties it in the heap + write set.
  ASSERT_TRUE(lock_->lock_row(younger, kTableOid, rid, LockMode::EXCLUSIVE).ok());
  Tuple v1 = make_tuple(1, "v1-dirty");
  ASSERT_TRUE(heap_->update_tuple(v1, rid).ok());
  (void)tm_->log_update(younger, kTableOid, rid, v0, tuple_bytes(v1));

  // Older grabs the auxiliary resource (X).
  ASSERT_TRUE(lock_->lock_table(older, kAuxResource, LockMode::EXCLUSIVE).ok());

  Status younger_status = Status::Ok();
  Status older_status = Status::Ok();
  std::string survivor_read;

  const auto start = std::chrono::steady_clock::now();

  // Younger reaches for the aux resource → cycle older<->younger. Wait-die
  // makes the younger the victim; its thread then finalizes the abort, which
  // undoes the write and only THEN releases the row lock.
  std::thread t_young([&] {
    younger_status = lock_->lock_table(younger, kAuxResource, LockMode::EXCLUSIVE);
    if (younger_status.code() == StatusCode::kAborted) {
      tm_->abort(younger); // undo v1 -> v0, then release locks
    }
  });

  // Older reaches for the row the younger holds. It blocks until the victim's
  // rollback releases the lock, then reads what must be the restored value.
  std::thread t_old([&] {
    older_status = lock_->lock_row(older, kTableOid, rid, LockMode::EXCLUSIVE);
    if (older_status.ok()) {
      survivor_read = read_name(rid);
    }
  });

  t_young.join();
  t_old.join();

  const auto elapsed = std::chrono::steady_clock::now() - start;

  // Wait-die outcome: younger dies, older wins.
  EXPECT_EQ(younger_status.code(), StatusCode::kAborted);
  EXPECT_TRUE(older_status.ok()) << older_status.message();
  EXPECT_GE(lock_->deadlock_count(), 1u);

  // The survivor observed the rolled-back value, proving undo preceded release.
  EXPECT_EQ(survivor_read, "v0")
      << "survivor read the victim's un-rolled-back write";

  // The heap itself is back to the committed value for everyone.
  EXPECT_EQ(read_name(rid), "v0");

  // Resolved by detection, not the 5s timeout.
  EXPECT_LT(elapsed, std::chrono::seconds(2));

  // Finalize the survivor through the manager: both transactions are now gone.
  tm_->commit(older);
  EXPECT_EQ(tm_->active_transaction_count(), 0u);
}

// ─────────────────────────────────────────────────────────────────────────────
// Committing a transaction releases every lock it held, so the resource is
// immediately acquirable by another transaction.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(TxnLockIntegrationTest, CommitReleasesAllLocks) {
  const RID rid = seed_committed_row(1, "row");

  Transaction *writer = tm_->begin();
  ASSERT_TRUE(lock_->lock_table(writer, kTableOid, LockMode::EXCLUSIVE).ok());
  ASSERT_TRUE(lock_->lock_row(writer, kTableOid, rid, LockMode::EXCLUSIVE).ok());
  ASSERT_GT(lock_->lock_table_size(), 0u);

  tm_->commit(writer);

  // Commit dropped every lock: the table drained.
  EXPECT_EQ(lock_->lock_table_size(), 0u);

  // A fresh transaction can take the exclusive locks with no contention.
  Transaction *next = tm_->begin();
  EXPECT_TRUE(lock_->lock_table(next, kTableOid, LockMode::EXCLUSIVE).ok());
  EXPECT_TRUE(lock_->lock_row(next, kTableOid, rid, LockMode::EXCLUSIVE).ok());
  tm_->commit(next);
  EXPECT_EQ(lock_->lock_table_size(), 0u);
}

// ─────────────────────────────────────────────────────────────────────────────
// Aborting a transaction likewise releases its locks (and undoes its writes),
// leaving the resource free and the row at its pre-transaction value.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(TxnLockIntegrationTest, AbortReleasesAllLocksAndRestoresRow) {
  const RID rid = seed_committed_row(1, "keep");
  const std::vector<char> v0 = tuple_bytes(make_tuple(1, "keep"));

  Transaction *writer = tm_->begin();
  ASSERT_TRUE(lock_->lock_row(writer, kTableOid, rid, LockMode::EXCLUSIVE).ok());
  Tuple v1 = make_tuple(1, "gone");
  ASSERT_TRUE(heap_->update_tuple(v1, rid).ok());
  (void)tm_->log_update(writer, kTableOid, rid, v0, tuple_bytes(v1));

  tm_->abort(writer);

  EXPECT_EQ(lock_->lock_table_size(), 0u);
  EXPECT_EQ(read_name(rid), "keep") << "abort must restore the row";

  // The row is lockable again by a new transaction.
  Transaction *next = tm_->begin();
  EXPECT_TRUE(lock_->lock_row(next, kTableOid, rid, LockMode::EXCLUSIVE).ok());
  tm_->commit(next);
  EXPECT_EQ(lock_->lock_table_size(), 0u);
}

} // namespace
} // namespace entropy
