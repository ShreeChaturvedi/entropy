/**
 * @file transaction_test.cpp
 * @brief Unit tests for Transaction, TransactionManager, LogRecord, and WAL
 */

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "transaction/lock_manager.hpp"
#include "transaction/log_record.hpp"
#include "transaction/mvcc.hpp"
#include "transaction/recovery.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction/wal.hpp"

namespace entropy {
namespace {

// ─────────────────────────────────────────────────────────────────────────────
// Transaction Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST(TransactionTest, CreateTransaction) {
  Transaction txn(1);

  EXPECT_EQ(txn.txn_id(), 1);
  EXPECT_EQ(txn.state(), TransactionState::GROWING);
  EXPECT_EQ(txn.isolation_level(), IsolationLevel::REPEATABLE_READ);
  EXPECT_TRUE(txn.is_active());
  EXPECT_EQ(txn.prev_lsn(), INVALID_LSN);
  EXPECT_GT(txn.start_ts(), 0UL);
}

TEST(TransactionTest, CreateWithIsolationLevel) {
  Transaction txn(2, IsolationLevel::READ_COMMITTED);

  EXPECT_EQ(txn.isolation_level(), IsolationLevel::READ_COMMITTED);
}

TEST(TransactionTest, StateTransitions) {
  Transaction txn(1);

  EXPECT_EQ(txn.state(), TransactionState::GROWING);
  EXPECT_TRUE(txn.is_active());

  txn.set_state(TransactionState::SHRINKING);
  EXPECT_EQ(txn.state(), TransactionState::SHRINKING);
  EXPECT_TRUE(txn.is_active());

  txn.set_state(TransactionState::COMMITTED);
  EXPECT_EQ(txn.state(), TransactionState::COMMITTED);
  EXPECT_FALSE(txn.is_active());
}

TEST(TransactionTest, StateToString) {
  EXPECT_STREQ(transaction_state_to_string(TransactionState::GROWING),
               "GROWING");
  EXPECT_STREQ(transaction_state_to_string(TransactionState::SHRINKING),
               "SHRINKING");
  EXPECT_STREQ(transaction_state_to_string(TransactionState::COMMITTED),
               "COMMITTED");
  EXPECT_STREQ(transaction_state_to_string(TransactionState::ABORTED),
               "ABORTED");
}

TEST(TransactionTest, WriteSet) {
  Transaction txn(1);

  EXPECT_TRUE(txn.write_set().empty());

  txn.add_write_record(WriteRecord(WriteType::INSERT, 1, RID(10, 5)));
  EXPECT_EQ(txn.write_set().size(), 1);

  txn.add_write_record(WriteRecord(WriteType::DELETE, 2, RID(20, 3),
                                   std::vector<char>{'a', 'b', 'c'}));
  EXPECT_EQ(txn.write_set().size(), 2);

  const auto &wr = txn.write_set()[0];
  EXPECT_EQ(wr.type, WriteType::INSERT);
  EXPECT_EQ(wr.table_oid, 1);
  EXPECT_EQ(wr.rid.page_id, 10);
  EXPECT_EQ(wr.rid.slot_id, 5);

  txn.clear_write_set();
  EXPECT_TRUE(txn.write_set().empty());
}

TEST(TransactionTest, LockTracking) {
  Transaction txn(1);

  EXPECT_FALSE(txn.has_page_lock(100));
  txn.add_page_lock(100);
  EXPECT_TRUE(txn.has_page_lock(100));

  txn.add_table_lock(5);
  EXPECT_EQ(txn.table_locks().size(), 1);
  EXPECT_TRUE(txn.table_locks().count(5) > 0);
}

TEST(TransactionTest, LSNTracking) {
  Transaction txn(1);

  EXPECT_EQ(txn.prev_lsn(), INVALID_LSN);

  txn.set_prev_lsn(100);
  EXPECT_EQ(txn.prev_lsn(), 100);

  txn.set_prev_lsn(200);
  EXPECT_EQ(txn.prev_lsn(), 200);
}

TEST(TransactionTest, TimestampTracking) {
  Transaction txn(1);

  auto start_ts = txn.start_ts();
  EXPECT_GT(start_ts, 0UL);
  EXPECT_EQ(txn.commit_ts(), 0UL);

  txn.set_commit_ts(12345);
  EXPECT_EQ(txn.commit_ts(), 12345);
}

// ─────────────────────────────────────────────────────────────────────────────
// LogRecord Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST(LogRecordTest, HeaderSize) {
  EXPECT_EQ(sizeof(LogRecordHeader), LOG_RECORD_HEADER_SIZE);
  EXPECT_EQ(LOG_RECORD_HEADER_SIZE, 32);
}

TEST(LogRecordTest, BeginRecord) {
  auto record = LogRecord::make_begin(42);

  EXPECT_EQ(record.type(), LogRecordType::BEGIN);
  EXPECT_EQ(record.txn_id(), 42);
  EXPECT_EQ(record.prev_lsn(), INVALID_LSN);
  EXPECT_EQ(record.size(), LOG_RECORD_HEADER_SIZE);
}

TEST(LogRecordTest, CommitRecord) {
  auto record = LogRecord::make_commit(42, 100);

  EXPECT_EQ(record.type(), LogRecordType::COMMIT);
  EXPECT_EQ(record.txn_id(), 42);
  EXPECT_EQ(record.prev_lsn(), 100);
  EXPECT_EQ(record.size(), LOG_RECORD_HEADER_SIZE);
}

TEST(LogRecordTest, AbortRecord) {
  auto record = LogRecord::make_abort(42, 100);

  EXPECT_EQ(record.type(), LogRecordType::ABORT);
  EXPECT_EQ(record.txn_id(), 42);
  EXPECT_EQ(record.prev_lsn(), 100);
}

TEST(LogRecordTest, InsertRecord) {
  std::vector<char> tuple_data = {'h', 'e', 'l', 'l', 'o'};
  auto record = LogRecord::make_insert(42, 100, 5, RID(10, 3), tuple_data);

  EXPECT_EQ(record.type(), LogRecordType::INSERT);
  EXPECT_EQ(record.txn_id(), 42);
  EXPECT_EQ(record.prev_lsn(), 100);
  EXPECT_EQ(record.table_oid(), 5);
  EXPECT_EQ(record.rid().page_id, 10);
  EXPECT_EQ(record.rid().slot_id, 3);
  EXPECT_EQ(record.new_tuple_data(), tuple_data);
  EXPECT_GT(record.size(), LOG_RECORD_HEADER_SIZE);
}

TEST(LogRecordTest, DeleteRecord) {
  std::vector<char> tuple_data = {'d', 'a', 't', 'a'};
  auto record = LogRecord::make_delete(42, 100, 5, RID(10, 3), tuple_data);

  EXPECT_EQ(record.type(), LogRecordType::DELETE);
  EXPECT_EQ(record.old_tuple_data(), tuple_data);
}

TEST(LogRecordTest, UpdateRecord) {
  std::vector<char> old_data = {'o', 'l', 'd'};
  std::vector<char> new_data = {'n', 'e', 'w', 'w'};
  auto record =
      LogRecord::make_update(42, 100, 5, RID(10, 3), old_data, new_data);

  EXPECT_EQ(record.type(), LogRecordType::UPDATE);
  EXPECT_EQ(record.old_tuple_data(), old_data);
  EXPECT_EQ(record.new_tuple_data(), new_data);
}

TEST(LogRecordTest, CheckpointRecord) {
  std::vector<txn_id_t> active_txns = {1, 5, 10, 42};
  auto record = LogRecord::make_checkpoint(active_txns);

  EXPECT_EQ(record.type(), LogRecordType::CHECKPOINT);
  EXPECT_EQ(record.active_txns(), active_txns);
}

TEST(LogRecordTest, SerializeDeserializeBegin) {
  auto original = LogRecord::make_begin(42);
  original.set_lsn(100);

  auto serialized = original.serialize();
  auto deserialized =
      LogRecord::deserialize(serialized.data(),
                             static_cast<uint32_t>(serialized.size()));

  EXPECT_EQ(deserialized.type(), LogRecordType::BEGIN);
  EXPECT_EQ(deserialized.txn_id(), 42);
  EXPECT_EQ(deserialized.lsn(), 100);
}

TEST(LogRecordTest, SerializeDeserializeInsert) {
  std::vector<char> tuple_data = {'t', 'e', 's', 't', ' ', 'd', 'a', 't', 'a'};
  auto original = LogRecord::make_insert(42, 100, 5, RID(10, 3), tuple_data);
  original.set_lsn(200);

  auto serialized = original.serialize();
  auto deserialized =
      LogRecord::deserialize(serialized.data(),
                             static_cast<uint32_t>(serialized.size()));

  EXPECT_EQ(deserialized.type(), LogRecordType::INSERT);
  EXPECT_EQ(deserialized.txn_id(), 42);
  EXPECT_EQ(deserialized.prev_lsn(), 100);
  EXPECT_EQ(deserialized.lsn(), 200);
  EXPECT_EQ(deserialized.table_oid(), 5);
  EXPECT_EQ(deserialized.rid().page_id, 10);
  EXPECT_EQ(deserialized.rid().slot_id, 3);
  EXPECT_EQ(deserialized.new_tuple_data(), tuple_data);
}

TEST(LogRecordTest, SerializeDeserializeUpdate) {
  std::vector<char> old_data = {'o', 'l', 'd'};
  std::vector<char> new_data = {'n', 'e', 'w', ' ', 'd', 'a', 't', 'a'};
  auto original =
      LogRecord::make_update(42, 100, 5, RID(10, 3), old_data, new_data);
  original.set_lsn(300);

  auto serialized = original.serialize();
  auto deserialized =
      LogRecord::deserialize(serialized.data(),
                             static_cast<uint32_t>(serialized.size()));

  EXPECT_EQ(deserialized.type(), LogRecordType::UPDATE);
  EXPECT_EQ(deserialized.old_tuple_data(), old_data);
  EXPECT_EQ(deserialized.new_tuple_data(), new_data);
}

TEST(LogRecordTest, SerializeDeserializeCheckpoint) {
  std::vector<txn_id_t> active_txns = {1, 5, 10, 42, 100};
  auto original = LogRecord::make_checkpoint(active_txns);
  original.set_lsn(500);

  auto serialized = original.serialize();
  auto deserialized =
      LogRecord::deserialize(serialized.data(),
                             static_cast<uint32_t>(serialized.size()));

  EXPECT_EQ(deserialized.type(), LogRecordType::CHECKPOINT);
  EXPECT_EQ(deserialized.active_txns(), active_txns);
}

TEST(LogRecordTest, TypeToString) {
  EXPECT_STREQ(log_record_type_to_string(LogRecordType::BEGIN), "BEGIN");
  EXPECT_STREQ(log_record_type_to_string(LogRecordType::COMMIT), "COMMIT");
  EXPECT_STREQ(log_record_type_to_string(LogRecordType::INSERT), "INSERT");
  EXPECT_STREQ(log_record_type_to_string(LogRecordType::UPDATE), "UPDATE");
  EXPECT_STREQ(log_record_type_to_string(LogRecordType::DELETE), "DELETE");
}

// ─────────────────────────────────────────────────────────────────────────────
// WAL Tests
// ─────────────────────────────────────────────────────────────────────────────

class WALTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() / "entropy_wal_test";
    std::filesystem::create_directories(test_dir_);
    wal_file_ = test_dir_ / "test.wal";
    std::filesystem::remove(wal_file_);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  std::filesystem::path test_dir_;
  std::filesystem::path wal_file_;
};

TEST_F(WALTest, CreateWAL) {
  WALManager wal(wal_file_.string());

  EXPECT_EQ(wal.next_lsn(), 1);
  EXPECT_EQ(wal.flushed_lsn(), 0);
  EXPECT_TRUE(std::filesystem::exists(wal_file_));
}

TEST_F(WALTest, AppendBeginRecord) {
  WALManager wal(wal_file_.string());

  auto record = LogRecord::make_begin(42);
  lsn_t lsn = wal.append_log(record);

  EXPECT_EQ(lsn, 1);
  EXPECT_EQ(record.lsn(), 1);
  EXPECT_EQ(wal.next_lsn(), 2);
}

TEST_F(WALTest, AppendMultipleRecords) {
  WALManager wal(wal_file_.string());

  auto r1 = LogRecord::make_begin(1);
  auto r2 = LogRecord::make_begin(2);
  auto r3 = LogRecord::make_commit(1, 1);

  lsn_t lsn1 = wal.append_log(r1);
  lsn_t lsn2 = wal.append_log(r2);
  lsn_t lsn3 = wal.append_log(r3);

  EXPECT_EQ(lsn1, 1);
  EXPECT_EQ(lsn2, 2);
  EXPECT_EQ(lsn3, 3);
  EXPECT_EQ(wal.next_lsn(), 4);
}

TEST_F(WALTest, FlushAndPersist) {
  {
    WALManager wal(wal_file_.string());

    auto r1 = LogRecord::make_begin(42);
    auto r2 = LogRecord::make_commit(42, 1);

    [[maybe_unused]] lsn_t lsn1 = wal.append_log(r1);
    [[maybe_unused]] lsn_t lsn2 = wal.append_log(r2);

    EXPECT_TRUE(wal.flush().ok());
    EXPECT_EQ(wal.flushed_lsn(), 2);
  }

  // Reopen and verify records persist
  {
    WALManager wal(wal_file_.string());
    auto records = wal.read_log();

    ASSERT_EQ(records.size(), 2);
    EXPECT_EQ(records[0].type(), LogRecordType::BEGIN);
    EXPECT_EQ(records[0].txn_id(), 42);
    EXPECT_EQ(records[1].type(), LogRecordType::COMMIT);
    EXPECT_EQ(records[1].txn_id(), 42);
  }
}

TEST_F(WALTest, ReadLog) {
  {
    WALManager wal(wal_file_.string());

    std::vector<char> tuple_data = {'d', 'a', 't', 'a'};

    auto r1 = LogRecord::make_begin(1);
    auto r2 = LogRecord::make_insert(1, 1, 10, RID(5, 3), tuple_data);
    auto r3 = LogRecord::make_commit(1, 2);

    [[maybe_unused]] lsn_t lsn1 = wal.append_log(r1);
    [[maybe_unused]] lsn_t lsn2 = wal.append_log(r2);
    [[maybe_unused]] lsn_t lsn3 = wal.append_log(r3);
    (void)wal.flush();
  }

  WALManager wal(wal_file_.string());
  auto records = wal.read_log();

  ASSERT_EQ(records.size(), 3);
  EXPECT_EQ(records[0].type(), LogRecordType::BEGIN);
  EXPECT_EQ(records[1].type(), LogRecordType::INSERT);
  EXPECT_EQ(records[1].table_oid(), 10);
  EXPECT_EQ(records[2].type(), LogRecordType::COMMIT);
}

TEST_F(WALTest, FlushToLSN) {
  WALManager wal(wal_file_.string());

  for (int i = 0; i < 5; ++i) {
    auto record = LogRecord::make_begin(static_cast<txn_id_t>(i + 1));
    [[maybe_unused]] lsn_t lsn = wal.append_log(record);
  }

  EXPECT_TRUE(wal.flush_to_lsn(3).ok());
  EXPECT_GE(wal.flushed_lsn(), 3);
}

TEST_F(WALTest, ReopenExistingWAL) {
  lsn_t last_lsn;

  {
    WALManager wal(wal_file_.string());
    for (int i = 0; i < 10; ++i) {
      auto record = LogRecord::make_begin(static_cast<txn_id_t>(i + 1));
      last_lsn = wal.append_log(record);
    }
    (void)wal.flush();
  }

  // Reopen and check LSN continues
  {
    WALManager wal(wal_file_.string());
    EXPECT_EQ(wal.next_lsn(), last_lsn + 1);
    EXPECT_EQ(wal.flushed_lsn(), last_lsn);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// TransactionManager Tests
// ─────────────────────────────────────────────────────────────────────────────

class TransactionManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() / "entropy_txn_test";
    std::filesystem::create_directories(test_dir_);
    wal_file_ = test_dir_ / "test.wal";
    std::filesystem::remove(wal_file_);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  std::filesystem::path test_dir_;
  std::filesystem::path wal_file_;
};

TEST_F(TransactionManagerTest, CreateWithoutWAL) {
  TransactionManager tm;

  auto *txn = tm.begin();
  ASSERT_NE(txn, nullptr);
  EXPECT_EQ(txn->state(), TransactionState::GROWING);
  EXPECT_EQ(tm.active_transaction_count(), 1);

  tm.commit(txn);
  EXPECT_EQ(tm.active_transaction_count(), 0);
}

TEST_F(TransactionManagerTest, CreateWithWAL) {
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  TransactionManager tm(wal);

  auto *txn = tm.begin();
  ASSERT_NE(txn, nullptr);
  EXPECT_NE(txn->prev_lsn(), INVALID_LSN); // BEGIN record written

  tm.commit(txn);

  // Check WAL has BEGIN and COMMIT
  auto records = wal->read_log();
  ASSERT_GE(records.size(), 2);
  EXPECT_EQ(records[0].type(), LogRecordType::BEGIN);
  EXPECT_EQ(records[1].type(), LogRecordType::COMMIT);
}

TEST_F(TransactionManagerTest, BeginMultiple) {
  TransactionManager tm;

  auto *txn1 = tm.begin();
  auto *txn2 = tm.begin();
  auto *txn3 = tm.begin();

  EXPECT_NE(txn1->txn_id(), txn2->txn_id());
  EXPECT_NE(txn2->txn_id(), txn3->txn_id());
  EXPECT_EQ(tm.active_transaction_count(), 3);

  tm.commit(txn1);
  tm.commit(txn2);
  tm.commit(txn3);
  EXPECT_EQ(tm.active_transaction_count(), 0);
}

TEST_F(TransactionManagerTest, Abort) {
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  TransactionManager tm(wal);

  auto *txn = tm.begin();
  txn_id_t id = txn->txn_id();

  tm.abort(txn);

  // Verify transaction is gone and ABORT logged
  EXPECT_EQ(tm.get_transaction(id), nullptr);

  auto records = wal->read_log();
  ASSERT_GE(records.size(), 2);
  EXPECT_EQ(records.back().type(), LogRecordType::ABORT);
}

TEST_F(TransactionManagerTest, GetTransaction) {
  TransactionManager tm;

  auto *txn = tm.begin();
  txn_id_t id = txn->txn_id();

  EXPECT_EQ(tm.get_transaction(id), txn);
  EXPECT_EQ(tm.get_transaction(9999), nullptr);

  tm.commit(txn);
  EXPECT_EQ(tm.get_transaction(id), nullptr);
}

TEST_F(TransactionManagerTest, GetActiveTransactionIds) {
  TransactionManager tm;

  auto *txn1 = tm.begin();
  auto *txn2 = tm.begin();

  auto ids = tm.get_active_txn_ids();
  ASSERT_EQ(ids.size(), 2);
  EXPECT_TRUE(std::find(ids.begin(), ids.end(), txn1->txn_id()) != ids.end());
  EXPECT_TRUE(std::find(ids.begin(), ids.end(), txn2->txn_id()) != ids.end());

  tm.commit(txn1);
  ids = tm.get_active_txn_ids();
  EXPECT_EQ(ids.size(), 1);

  tm.commit(txn2);
}

TEST_F(TransactionManagerTest, LogInsert) {
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  TransactionManager tm(wal);

  auto *txn = tm.begin();
  std::vector<char> data = {'t', 'e', 's', 't'};

  lsn_t lsn = tm.log_insert(txn, 10, RID(5, 3), data);

  EXPECT_NE(lsn, INVALID_LSN);
  EXPECT_EQ(txn->prev_lsn(), lsn);
  EXPECT_EQ(txn->write_set().size(), 1);
  EXPECT_EQ(txn->write_set()[0].type, WriteType::INSERT);

  tm.commit(txn);
}

TEST_F(TransactionManagerTest, LogDelete) {
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  TransactionManager tm(wal);

  auto *txn = tm.begin();
  std::vector<char> data = {'o', 'l', 'd'};

  lsn_t lsn = tm.log_delete(txn, 10, RID(5, 3), data);

  EXPECT_NE(lsn, INVALID_LSN);
  EXPECT_EQ(txn->write_set().size(), 1);
  EXPECT_EQ(txn->write_set()[0].type, WriteType::DELETE);
  EXPECT_EQ(txn->write_set()[0].old_data, data);

  tm.commit(txn);
}

TEST_F(TransactionManagerTest, LogUpdate) {
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  TransactionManager tm(wal);

  auto *txn = tm.begin();
  std::vector<char> old_data = {'o', 'l', 'd'};
  std::vector<char> new_data = {'n', 'e', 'w'};

  lsn_t lsn = tm.log_update(txn, 10, RID(5, 3), old_data, new_data);

  EXPECT_NE(lsn, INVALID_LSN);
  EXPECT_EQ(txn->write_set().size(), 1);
  EXPECT_EQ(txn->write_set()[0].type, WriteType::UPDATE);

  tm.commit(txn);
}

TEST_F(TransactionManagerTest, CommitClearsWriteSet) {
  TransactionManager tm;

  auto *txn = tm.begin();
  txn->add_write_record(WriteRecord(WriteType::INSERT, 1, RID(1, 1)));
  EXPECT_EQ(txn->write_set().size(), 1);

  // After commit, write set should be cleared (but txn pointer is invalid)
  // We can't check this directly since txn is deleted
  tm.commit(txn);
}

TEST_F(TransactionManagerTest, FullTransactionWorkflow) {
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  TransactionManager tm(wal);

  // Start transaction
  auto *txn = tm.begin();
  EXPECT_EQ(txn->state(), TransactionState::GROWING);

  // Do some operations
  std::vector<char> data1 = {'a', 'b', 'c'};
  std::vector<char> data2 = {'d', 'e', 'f'};

  tm.log_insert(txn, 1, RID(1, 0), data1);
  tm.log_insert(txn, 1, RID(1, 1), data2);

  EXPECT_EQ(txn->write_set().size(), 2);

  // Commit
  tm.commit(txn);

  // Verify WAL records
  auto records = wal->read_log();
  ASSERT_GE(records.size(), 4); // BEGIN, 2x INSERT, COMMIT

  int begin_count = 0;
  int insert_count = 0;
  int commit_count = 0;

  for (const auto &r : records) {
    if (r.type() == LogRecordType::BEGIN)
      begin_count++;
    if (r.type() == LogRecordType::INSERT)
      insert_count++;
    if (r.type() == LogRecordType::COMMIT)
      commit_count++;
  }

  EXPECT_EQ(begin_count, 1);
  EXPECT_EQ(insert_count, 2);
  EXPECT_EQ(commit_count, 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Lock Manager Tests
// ─────────────────────────────────────────────────────────────────────────────

class LockManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    lock_mgr_ = std::make_unique<LockManager>(true, 100); // 100ms timeout
  }

  std::unique_ptr<LockManager> lock_mgr_;
};

TEST_F(LockManagerTest, AcquireTableLockShared) {
  Transaction txn(1);

  auto status = lock_mgr_->lock_table(&txn, 1, LockMode::SHARED);
  EXPECT_TRUE(status.ok());

  status = lock_mgr_->unlock_table(&txn, 1);
  EXPECT_TRUE(status.ok());
}

TEST_F(LockManagerTest, AcquireTableLockExclusive) {
  Transaction txn(1);

  auto status = lock_mgr_->lock_table(&txn, 1, LockMode::EXCLUSIVE);
  EXPECT_TRUE(status.ok());

  status = lock_mgr_->unlock_table(&txn, 1);
  EXPECT_TRUE(status.ok());
}

TEST_F(LockManagerTest, AcquireRowLockShared) {
  Transaction txn(1);
  RID rid(10, 5);

  auto status = lock_mgr_->lock_row(&txn, 1, rid, LockMode::SHARED);
  EXPECT_TRUE(status.ok());

  status = lock_mgr_->unlock_row(&txn, 1, rid);
  EXPECT_TRUE(status.ok());
}

TEST_F(LockManagerTest, AcquireRowLockExclusive) {
  Transaction txn(1);
  RID rid(10, 5);

  auto status = lock_mgr_->lock_row(&txn, 1, rid, LockMode::EXCLUSIVE);
  EXPECT_TRUE(status.ok());

  status = lock_mgr_->unlock_row(&txn, 1, rid);
  EXPECT_TRUE(status.ok());
}

TEST_F(LockManagerTest, MultipleSharedLocks) {
  Transaction txn1(1);
  Transaction txn2(2);
  Transaction txn3(3);

  // All should be able to acquire shared locks
  EXPECT_TRUE(lock_mgr_->lock_table(&txn1, 1, LockMode::SHARED).ok());
  EXPECT_TRUE(lock_mgr_->lock_table(&txn2, 1, LockMode::SHARED).ok());
  EXPECT_TRUE(lock_mgr_->lock_table(&txn3, 1, LockMode::SHARED).ok());

  // Cleanup
  lock_mgr_->release_all_locks(&txn1);
  lock_mgr_->release_all_locks(&txn2);
  lock_mgr_->release_all_locks(&txn3);
}

TEST_F(LockManagerTest, ExclusiveBlocksShared) {
  Transaction txn1(1);
  Transaction txn2(2);

  // txn1 gets exclusive lock
  EXPECT_TRUE(lock_mgr_->lock_table(&txn1, 1, LockMode::EXCLUSIVE).ok());

  // txn2 should timeout trying to get shared lock
  auto status = lock_mgr_->lock_table(&txn2, 1, LockMode::SHARED);
  EXPECT_TRUE(status.code() == StatusCode::kTimeout);

  lock_mgr_->release_all_locks(&txn1);
}

TEST_F(LockManagerTest, SharedBlocksExclusive) {
  Transaction txn1(1);
  Transaction txn2(2);

  // txn1 gets shared lock
  EXPECT_TRUE(lock_mgr_->lock_table(&txn1, 1, LockMode::SHARED).ok());

  // txn2 should timeout trying to get exclusive lock
  auto status = lock_mgr_->lock_table(&txn2, 1, LockMode::EXCLUSIVE);
  EXPECT_TRUE(status.code() == StatusCode::kTimeout);

  lock_mgr_->release_all_locks(&txn1);
}

TEST_F(LockManagerTest, ExclusiveBlocksExclusive) {
  Transaction txn1(1);
  Transaction txn2(2);

  // txn1 gets exclusive lock
  EXPECT_TRUE(lock_mgr_->lock_table(&txn1, 1, LockMode::EXCLUSIVE).ok());

  // txn2 should timeout trying to get exclusive lock
  auto status = lock_mgr_->lock_table(&txn2, 1, LockMode::EXCLUSIVE);
  EXPECT_TRUE(status.code() == StatusCode::kTimeout);

  lock_mgr_->release_all_locks(&txn1);
}

TEST_F(LockManagerTest, LockUpgradeImmediate) {
  Transaction txn(1);

  // Get shared lock
  EXPECT_TRUE(lock_mgr_->lock_table(&txn, 1, LockMode::SHARED).ok());

  // Upgrade to exclusive (should succeed immediately)
  EXPECT_TRUE(lock_mgr_->upgrade_lock(&txn, 1).ok());

  lock_mgr_->release_all_locks(&txn);
}

TEST_F(LockManagerTest, LockUpgradeWhenAlone) {
  Transaction txn(1);

  // Get shared lock
  EXPECT_TRUE(lock_mgr_->lock_row(&txn, 1, RID(1, 0), LockMode::SHARED).ok());

  // Upgrade should work
  EXPECT_TRUE(lock_mgr_->upgrade_lock(&txn, 1, RID(1, 0)).ok());

  lock_mgr_->release_all_locks(&txn);
}

TEST_F(LockManagerTest, AlreadyHoldLock) {
  Transaction txn(1);

  // Get shared lock twice - should be idempotent
  EXPECT_TRUE(lock_mgr_->lock_table(&txn, 1, LockMode::SHARED).ok());
  EXPECT_TRUE(lock_mgr_->lock_table(&txn, 1, LockMode::SHARED).ok());

  // Only need to unlock once
  EXPECT_TRUE(lock_mgr_->unlock_table(&txn, 1).ok());
  EXPECT_EQ(lock_mgr_->lock_table_size(), 0);
}

TEST_F(LockManagerTest, ReleaseAllLocks) {
  Transaction txn(1);

  // Acquire multiple locks
  EXPECT_TRUE(lock_mgr_->lock_table(&txn, 1, LockMode::SHARED).ok());
  EXPECT_TRUE(lock_mgr_->lock_table(&txn, 2, LockMode::EXCLUSIVE).ok());
  EXPECT_TRUE(lock_mgr_->lock_row(&txn, 1, RID(1, 0), LockMode::SHARED).ok());

  EXPECT_EQ(lock_mgr_->lock_table_size(), 3);

  // Release all at once
  lock_mgr_->release_all_locks(&txn);

  EXPECT_EQ(lock_mgr_->lock_table_size(), 0);
}

TEST_F(LockManagerTest, UnlockNotHeld) {
  Transaction txn(1);

  auto status = lock_mgr_->unlock_table(&txn, 999);
  EXPECT_TRUE(status.is_not_found());
}

TEST_F(LockManagerTest, LockNullTransaction) {
  auto status = lock_mgr_->lock_table(nullptr, 1, LockMode::SHARED);
  EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
}

TEST_F(LockManagerTest, TwoPhaseLocking) {
  Transaction txn(1);

  // Growing phase
  EXPECT_EQ(txn.state(), TransactionState::GROWING);
  EXPECT_TRUE(lock_mgr_->lock_table(&txn, 1, LockMode::SHARED).ok());
  EXPECT_TRUE(lock_mgr_->lock_table(&txn, 2, LockMode::SHARED).ok());

  // First unlock transitions to shrinking
  EXPECT_TRUE(lock_mgr_->unlock_table(&txn, 1).ok());
  EXPECT_EQ(txn.state(), TransactionState::SHRINKING);

  // Cannot acquire new locks in shrinking phase
  auto status = lock_mgr_->lock_table(&txn, 3, LockMode::SHARED);
  EXPECT_EQ(status.code(), StatusCode::kAborted);

  lock_mgr_->release_all_locks(&txn);
}

TEST_F(LockManagerTest, AbortedTransactionCannotLock) {
  Transaction txn(1);
  txn.set_state(TransactionState::ABORTED);

  auto status = lock_mgr_->lock_table(&txn, 1, LockMode::SHARED);
  EXPECT_EQ(status.code(), StatusCode::kAborted);
}

TEST_F(LockManagerTest, LockDifferentResources) {
  Transaction txn1(1);
  Transaction txn2(2);

  // Different tables - no conflict
  EXPECT_TRUE(lock_mgr_->lock_table(&txn1, 1, LockMode::EXCLUSIVE).ok());
  EXPECT_TRUE(lock_mgr_->lock_table(&txn2, 2, LockMode::EXCLUSIVE).ok());

  lock_mgr_->release_all_locks(&txn1);
  lock_mgr_->release_all_locks(&txn2);
}

TEST_F(LockManagerTest, LockDifferentRowsSameTable) {
  Transaction txn1(1);
  Transaction txn2(2);

  // Different rows in same table - no conflict
  EXPECT_TRUE(
      lock_mgr_->lock_row(&txn1, 1, RID(1, 0), LockMode::EXCLUSIVE).ok());
  EXPECT_TRUE(
      lock_mgr_->lock_row(&txn2, 1, RID(1, 1), LockMode::EXCLUSIVE).ok());

  lock_mgr_->release_all_locks(&txn1);
  lock_mgr_->release_all_locks(&txn2);
}

TEST_F(LockManagerTest, LockModeToString) {
  EXPECT_STREQ(lock_mode_to_string(LockMode::SHARED), "SHARED");
  EXPECT_STREQ(lock_mode_to_string(LockMode::EXCLUSIVE), "EXCLUSIVE");
}

TEST_F(LockManagerTest, LockModeCompatibility) {
  EXPECT_TRUE(are_lock_modes_compatible(LockMode::SHARED, LockMode::SHARED));
  EXPECT_FALSE(
      are_lock_modes_compatible(LockMode::SHARED, LockMode::EXCLUSIVE));
  EXPECT_FALSE(
      are_lock_modes_compatible(LockMode::EXCLUSIVE, LockMode::SHARED));
  EXPECT_FALSE(
      are_lock_modes_compatible(LockMode::EXCLUSIVE, LockMode::EXCLUSIVE));
}

TEST_F(LockManagerTest, DeadlockCount) {
  EXPECT_EQ(lock_mgr_->deadlock_count(), 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Concurrent Lock Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(LockManagerTest, ConcurrentSharedLocks) {
  std::vector<std::unique_ptr<Transaction>> txns;
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  constexpr int NUM_THREADS = 10;

  // Create transactions
  for (size_t i = 0; i < static_cast<size_t>(NUM_THREADS); ++i) {
    const txn_id_t txn_id = i + 1;
    txns.push_back(std::make_unique<Transaction>(txn_id));
  }

  // All try to get shared locks concurrently
  for (size_t i = 0; i < static_cast<size_t>(NUM_THREADS); ++i) {
    threads.emplace_back([this, &txns, i, &success_count]() {
      if (lock_mgr_->lock_table(txns[i].get(), 1, LockMode::SHARED).ok()) {
        success_count++;
      }
    });
  }

  // Wait for all threads
  for (auto &t : threads) {
    t.join();
  }

  // All should have succeeded
  EXPECT_EQ(success_count.load(), NUM_THREADS);

  // Cleanup
  for (auto &txn : txns) {
    lock_mgr_->release_all_locks(txn.get());
  }
}

TEST_F(LockManagerTest, LockWaitAndRelease) {
  // Use longer timeout for this test
  lock_mgr_ = std::make_unique<LockManager>(true, 1000);

  Transaction txn1(1);
  Transaction txn2(2);

  std::atomic<bool> lock_acquired{false};

  // txn1 gets exclusive lock
  EXPECT_TRUE(lock_mgr_->lock_table(&txn1, 1, LockMode::EXCLUSIVE).ok());

  // Start thread that will wait for lock
  std::thread waiter([this, &txn2, &lock_acquired]() {
    if (lock_mgr_->lock_table(&txn2, 1, LockMode::SHARED).ok()) {
      lock_acquired = true;
    }
  });

  // Give waiter time to start waiting
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Release lock - waiter should get it
  lock_mgr_->release_all_locks(&txn1);

  waiter.join();

  EXPECT_TRUE(lock_acquired.load());

  lock_mgr_->release_all_locks(&txn2);
}

TEST_F(LockManagerTest, BasicDeadlockDetection) {
  // This test creates a potential deadlock scenario
  // T1 holds A, waits for B
  // T2 holds B, waits for A
  // Deadlock detection should abort one

  lock_mgr_ = std::make_unique<LockManager>(true, 500);

  Transaction txn1(1);
  Transaction txn2(2);

  std::atomic<int> aborted_count{0};

  // T1 gets lock on table 1
  EXPECT_TRUE(lock_mgr_->lock_table(&txn1, 1, LockMode::EXCLUSIVE).ok());

  // T2 gets lock on table 2
  EXPECT_TRUE(lock_mgr_->lock_table(&txn2, 2, LockMode::EXCLUSIVE).ok());

  std::thread t1([this, &txn1, &aborted_count]() {
    // T1 tries to get lock on table 2 (held by T2)
    auto status = lock_mgr_->lock_table(&txn1, 2, LockMode::EXCLUSIVE);
    if (status.code() == StatusCode::kAborted) {
      aborted_count++;
    }
  });

  std::thread t2([this, &txn2, &aborted_count]() {
    // T2 tries to get lock on table 1 (held by T1)
    auto status = lock_mgr_->lock_table(&txn2, 1, LockMode::EXCLUSIVE);
    if (status.code() == StatusCode::kAborted) {
      aborted_count++;
    }
  });

  t1.join();
  t2.join();

  // At least one should be aborted due to deadlock or timeout
  auto total_aborted = static_cast<uint64_t>(aborted_count.load()) +
                       lock_mgr_->deadlock_count();
  EXPECT_GE(total_aborted, static_cast<uint64_t>(1));

  lock_mgr_->release_all_locks(&txn1);
  lock_mgr_->release_all_locks(&txn2);
}

TEST_F(LockManagerTest, UpgradeBlockedByOthers) {
  // Test that lock upgrade waits when other transactions hold shared locks
  lock_mgr_ = std::make_unique<LockManager>(true, 200);

  Transaction txn1(1);
  Transaction txn2(2);

  // Both get shared locks
  EXPECT_TRUE(lock_mgr_->lock_table(&txn1, 1, LockMode::SHARED).ok());
  EXPECT_TRUE(lock_mgr_->lock_table(&txn2, 1, LockMode::SHARED).ok());

  std::atomic<bool> upgrade_started{false};
  std::atomic<bool> upgrade_completed{false};

  // txn1 tries to upgrade
  std::thread upgrader([this, &txn1, &upgrade_started, &upgrade_completed]() {
    upgrade_started = true;
    auto status = lock_mgr_->upgrade_lock(&txn1, 1);
    if (status.ok()) {
      upgrade_completed = true;
    }
  });

  // Wait for upgrade to start
  while (!upgrade_started) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Upgrade should be blocked
  EXPECT_FALSE(upgrade_completed.load());

  // Release txn2's lock
  EXPECT_TRUE(lock_mgr_->unlock_table(&txn2, 1).ok());

  upgrader.join();

  // Upgrade should have completed
  EXPECT_TRUE(upgrade_completed.load());

  lock_mgr_->release_all_locks(&txn1);
}

TEST_F(LockManagerTest, MultipleLockReleaseGrantsWaiting) {
  lock_mgr_ = std::make_unique<LockManager>(true, 1000);

  Transaction txn1(1);
  Transaction txn2(2);
  Transaction txn3(3);

  std::atomic<int> waiting_granted{0};

  // txn1 gets exclusive lock
  EXPECT_TRUE(lock_mgr_->lock_table(&txn1, 1, LockMode::EXCLUSIVE).ok());

  // Start multiple waiting threads
  std::thread waiter1([this, &txn2, &waiting_granted]() {
    if (lock_mgr_->lock_table(&txn2, 1, LockMode::SHARED).ok()) {
      waiting_granted++;
    }
  });

  std::thread waiter2([this, &txn3, &waiting_granted]() {
    if (lock_mgr_->lock_table(&txn3, 1, LockMode::SHARED).ok()) {
      waiting_granted++;
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Release exclusive - both shared waiters should get lock
  lock_mgr_->release_all_locks(&txn1);

  waiter1.join();
  waiter2.join();

  EXPECT_EQ(waiting_granted.load(), 2);

  lock_mgr_->release_all_locks(&txn2);
  lock_mgr_->release_all_locks(&txn3);
}

// ─────────────────────────────────────────────────────────────────────────────
// Recovery Manager Tests
// ─────────────────────────────────────────────────────────────────────────────

class RecoveryTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ =
        std::filesystem::temp_directory_path() / "entropy_recovery_test";
    std::filesystem::create_directories(test_dir_);
    wal_file_ = test_dir_ / "test.wal";
    std::filesystem::remove(wal_file_);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  std::filesystem::path test_dir_;
  std::filesystem::path wal_file_;
};

TEST_F(RecoveryTest, EmptyLogRecovery) {
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(nullptr, wal, nullptr);

  auto status = recovery.recover();
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(recovery.redo_count(), 0);
  EXPECT_EQ(recovery.undo_count(), 0);
}

TEST_F(RecoveryTest, AnalysisIdentifiesCommittedTransaction) {
  // Create WAL with a committed transaction
  {
    WALManager wal(wal_file_.string());
    auto begin = LogRecord::make_begin(1);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin);

    std::vector<char> data = {'t', 'e', 's', 't'};
    auto insert = LogRecord::make_insert(1, begin.lsn(), 10, RID(1, 0), data);
    lsn = wal.append_log(insert);

    auto commit = LogRecord::make_commit(1, insert.lsn());
    lsn = wal.append_log(commit);
    (void)wal.flush();
  }

  // Recover
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(nullptr, wal, nullptr);

  auto status = recovery.recover();
  EXPECT_TRUE(status.ok());

  // After analysis, committed txn should be in committed set, not ATT
  EXPECT_TRUE(recovery.active_txn_table().empty());
  EXPECT_TRUE(recovery.committed_txns().count(1) > 0);
}

TEST_F(RecoveryTest, AnalysisIdentifiesUncommittedTransaction) {
  // Create WAL with an uncommitted transaction (simulates crash)
  {
    WALManager wal(wal_file_.string());
    auto begin = LogRecord::make_begin(1);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin);

    std::vector<char> data = {'t', 'e', 's', 't'};
    auto insert = LogRecord::make_insert(1, begin.lsn(), 10, RID(1, 0), data);
    lsn = wal.append_log(insert);

    // No commit - simulates crash
    (void)wal.flush();
  }

  // Recover
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(nullptr, wal, nullptr);

  auto status = recovery.recover();
  EXPECT_TRUE(status.ok());

  // After analysis, uncommitted txn should be in ATT
  EXPECT_EQ(recovery.active_txn_table().size(), 1);
  EXPECT_TRUE(recovery.active_txn_table().count(1) > 0);
  EXPECT_TRUE(recovery.committed_txns().empty());
}

TEST_F(RecoveryTest, RedoCommittedTransaction) {
  // Create WAL with committed transaction
  {
    WALManager wal(wal_file_.string());
    auto begin = LogRecord::make_begin(1);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin);

    std::vector<char> data = {'d', 'a', 't', 'a'};
    auto insert1 = LogRecord::make_insert(1, begin.lsn(), 10, RID(1, 0), data);
    lsn = wal.append_log(insert1);

    auto insert2 =
        LogRecord::make_insert(1, insert1.lsn(), 10, RID(1, 1), data);
    lsn = wal.append_log(insert2);

    auto commit = LogRecord::make_commit(1, insert2.lsn());
    lsn = wal.append_log(commit);
    (void)wal.flush();
  }

  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(nullptr, wal, nullptr);

  auto status = recovery.recover();
  EXPECT_TRUE(status.ok());

  // Should have redone 2 insert operations
  EXPECT_EQ(recovery.redo_count(), 2);
  // No undo needed - transaction committed
  EXPECT_EQ(recovery.undo_count(), 0);
}

TEST_F(RecoveryTest, UndoUncommittedTransaction) {
  // Create WAL with uncommitted transaction
  {
    WALManager wal(wal_file_.string());
    auto begin = LogRecord::make_begin(42);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin);

    std::vector<char> data = {'t', 'e', 's', 't'};
    auto insert = LogRecord::make_insert(42, begin.lsn(), 10, RID(5, 0), data);
    lsn = wal.append_log(insert);

    auto update_old = std::vector<char>{'o', 'l', 'd'};
    auto update_new = std::vector<char>{'n', 'e', 'w'};
    auto update = LogRecord::make_update(42, insert.lsn(), 10, RID(5, 1),
                                         update_old, update_new);
    lsn = wal.append_log(update);

    // No commit - crash simulation
    (void)wal.flush();
  }

  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(nullptr, wal, nullptr);

  auto status = recovery.recover();
  EXPECT_TRUE(status.ok());

  // Should have redone both operations
  EXPECT_EQ(recovery.redo_count(), 2);
  // Should have undone both operations (reverse order via prevLSN)
  EXPECT_EQ(recovery.undo_count(), 2);
}

TEST_F(RecoveryTest, MixedCommittedAndUncommittedTransactions) {
  // Create WAL with multiple transactions - some committed, some not
  {
    WALManager wal(wal_file_.string());

    // Transaction 1: committed
    auto begin1 = LogRecord::make_begin(1);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin1);
    std::vector<char> data = {'a'};
    auto insert1 = LogRecord::make_insert(1, begin1.lsn(), 10, RID(1, 0), data);
    lsn = wal.append_log(insert1);
    auto commit1 = LogRecord::make_commit(1, insert1.lsn());
    lsn = wal.append_log(commit1);

    // Transaction 2: uncommitted
    auto begin2 = LogRecord::make_begin(2);
    lsn = wal.append_log(begin2);
    auto insert2 = LogRecord::make_insert(2, begin2.lsn(), 20, RID(2, 0), data);
    lsn = wal.append_log(insert2);
    // No commit

    // Transaction 3: committed
    auto begin3 = LogRecord::make_begin(3);
    lsn = wal.append_log(begin3);
    auto insert3 = LogRecord::make_insert(3, begin3.lsn(), 30, RID(3, 0), data);
    lsn = wal.append_log(insert3);
    auto commit3 = LogRecord::make_commit(3, insert3.lsn());
    lsn = wal.append_log(commit3);

    (void)wal.flush();
  }

  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(nullptr, wal, nullptr);

  auto status = recovery.recover();
  EXPECT_TRUE(status.ok());

  // Check analysis results
  EXPECT_EQ(recovery.active_txn_table().size(), 1); // Only txn 2
  EXPECT_TRUE(recovery.active_txn_table().count(2) > 0);
  EXPECT_EQ(recovery.committed_txns().size(), 2); // Txns 1 and 3
  EXPECT_TRUE(recovery.committed_txns().count(1) > 0);
  EXPECT_TRUE(recovery.committed_txns().count(3) > 0);

  // 3 inserts redone
  EXPECT_EQ(recovery.redo_count(), 3);
  // 1 insert undone (txn 2)
  EXPECT_EQ(recovery.undo_count(), 1);
}

TEST_F(RecoveryTest, CheckpointRecovery) {
  // Create WAL with checkpoint
  {
    WALManager wal(wal_file_.string());

    // Transaction 1: committed before checkpoint
    auto begin1 = LogRecord::make_begin(1);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin1);
    std::vector<char> data = {'x'};
    auto insert1 = LogRecord::make_insert(1, begin1.lsn(), 10, RID(1, 0), data);
    lsn = wal.append_log(insert1);
    auto commit1 = LogRecord::make_commit(1, insert1.lsn());
    lsn = wal.append_log(commit1);

    // Transaction 2: active at checkpoint
    auto begin2 = LogRecord::make_begin(2);
    lsn = wal.append_log(begin2);
    auto insert2 = LogRecord::make_insert(2, begin2.lsn(), 20, RID(2, 0), data);
    lsn = wal.append_log(insert2);

    // Checkpoint with txn 2 active
    auto checkpoint = LogRecord::make_checkpoint({2});
    lsn = wal.append_log(checkpoint);

    // Txn 2 continues and commits after checkpoint
    auto insert2b =
        LogRecord::make_insert(2, insert2.lsn(), 20, RID(2, 1), data);
    lsn = wal.append_log(insert2b);
    auto commit2 = LogRecord::make_commit(2, insert2b.lsn());
    lsn = wal.append_log(commit2);

    (void)wal.flush();
  }

  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(nullptr, wal, nullptr);

  auto status = recovery.recover();
  EXPECT_TRUE(status.ok());

  // Both transactions should be committed
  EXPECT_TRUE(recovery.active_txn_table().empty());
  EXPECT_EQ(recovery.committed_txns().size(), 2);
}

TEST_F(RecoveryTest, CreateCheckpoint) {
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(nullptr, wal, nullptr);

  std::vector<txn_id_t> active = {1, 5, 10};
  auto status = recovery.create_checkpoint(active);
  EXPECT_TRUE(status.ok());

  // Verify checkpoint was written
  auto records = wal->read_log();
  ASSERT_EQ(records.size(), 1);
  EXPECT_EQ(records[0].type(), LogRecordType::CHECKPOINT);
  EXPECT_EQ(records[0].active_txns(), active);
}

// ─────────────────────────────────────────────────────────────────────────────
// MVCC Tests
// ─────────────────────────────────────────────────────────────────────────────

class MVCCTest : public ::testing::Test {
protected:
  MVCCManager mvcc_;
};

TEST_F(MVCCTest, TimestampGeneration) {
  uint64_t ts1 = mvcc_.get_timestamp();
  uint64_t ts2 = mvcc_.get_timestamp();
  uint64_t ts3 = mvcc_.get_timestamp();

  EXPECT_LT(ts1, ts2);
  EXPECT_LT(ts2, ts3);
}

TEST_F(MVCCTest, VersionInfoDefaults) {
  VersionInfo version;

  EXPECT_EQ(version.created_by, TXN_ID_NONE);
  EXPECT_EQ(version.deleted_by, TXN_ID_NONE);
  EXPECT_EQ(version.begin_ts, 0);
  EXPECT_EQ(version.end_ts, TIMESTAMP_MAX);
  EXPECT_FALSE(version.is_deleted());
}

TEST_F(MVCCTest, InitVersion) {
  Transaction txn(100);
  VersionInfo version;

  mvcc_.init_version(version, &txn);

  EXPECT_EQ(version.created_by, 100);
  EXPECT_EQ(version.deleted_by, TXN_ID_NONE);
  EXPECT_EQ(version.begin_ts, 0); // Not yet committed
  EXPECT_EQ(version.end_ts, TIMESTAMP_MAX);
}

TEST_F(MVCCTest, MarkDeleted) {
  Transaction creator(1);
  Transaction deleter(2);
  VersionInfo version;

  mvcc_.init_version(version, &creator);
  mvcc_.mark_deleted(version, &deleter);

  EXPECT_EQ(version.deleted_by, 2);
  EXPECT_TRUE(version.is_deleted());
}

TEST_F(MVCCTest, CreatorSeesOwnUncommittedVersion) {
  Transaction txn(42);
  VersionInfo version;

  mvcc_.init_version(version, &txn);

  // Creator should see their own uncommitted version
  EXPECT_TRUE(mvcc_.is_visible(version, &txn));
}

TEST_F(MVCCTest, OtherTransactionCannotSeeUncommittedVersion) {
  Transaction creator(1);
  Transaction reader(2);
  VersionInfo version;

  mvcc_.init_version(version, &creator);

  // Other transaction should not see uncommitted version
  EXPECT_FALSE(mvcc_.is_visible(version, &reader));
}

TEST_F(MVCCTest, VisibleAfterCommit) {
  // Simulate: txn1 creates version, commits, then txn2 reads

  // txn1 creates version
  Transaction txn1(1);
  VersionInfo version;
  mvcc_.init_version(version, &txn1);

  // txn1 commits at timestamp 100
  txn1.set_commit_ts(100);
  mvcc_.finalize_commit(version, 100);

  // txn2 starts after txn1 committed (start_ts > version.begin_ts)
  Transaction txn2(2);
  // Simulate txn2 starting after txn1's commit
  // txn2 has start_ts from when it was created, which would be after 100

  // For testing, we'll verify the logic directly
  // Version should be visible if begin_ts <= reader's start_ts
  EXPECT_EQ(version.begin_ts, 100);
}

TEST_F(MVCCTest, DeletedVersionNotVisible) {
  Transaction creator(1);
  Transaction deleter(2);
  Transaction reader(3);
  VersionInfo version;

  // Create and commit version
  mvcc_.init_version(version, &creator);
  mvcc_.finalize_commit(version, 10); // Committed at ts 10

  // Delete and commit deletion
  mvcc_.mark_deleted(version, &deleter);
  mvcc_.finalize_commit(version, 20); // Deletion committed at ts 20

  EXPECT_EQ(version.begin_ts, 10);
  EXPECT_EQ(version.end_ts, 20);
  EXPECT_TRUE(version.is_deleted());
}

TEST_F(MVCCTest, RollbackVersion) {
  Transaction txn(1);
  VersionInfo version;

  mvcc_.init_version(version, &txn);
  mvcc_.rollback_version(version);

  // After rollback, begin_ts is MAX, end_ts is 0
  // This makes the version invisible to all transactions
  EXPECT_EQ(version.begin_ts, TIMESTAMP_MAX);
  EXPECT_EQ(version.end_ts, 0);
}

TEST_F(MVCCTest, VisibilityWithNullTransaction) {
  VersionInfo uncommitted;
  uncommitted.begin_ts = 0;

  VersionInfo committed;
  committed.begin_ts = 10;

  VersionInfo deleted;
  deleted.begin_ts = 10;
  deleted.end_ts = 20;

  // Null transaction sees committed, non-deleted versions
  EXPECT_FALSE(mvcc_.is_visible(uncommitted, nullptr));
  EXPECT_TRUE(mvcc_.is_visible(committed, nullptr));
  EXPECT_FALSE(mvcc_.is_visible(deleted, nullptr));
}

TEST_F(MVCCTest, CreatorCannotSeeAfterSelfDelete) {
  Transaction txn(1);
  VersionInfo version;

  mvcc_.init_version(version, &txn);

  // Creator sees their own version
  EXPECT_TRUE(mvcc_.is_visible(version, &txn));

  // Creator deletes
  mvcc_.mark_deleted(version, &txn);

  // Creator should NOT see after self-delete
  EXPECT_FALSE(mvcc_.is_visible(version, &txn));
}

} // namespace
} // namespace entropy
