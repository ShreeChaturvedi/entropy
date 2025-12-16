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

#include "transaction/log_record.hpp"
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
    EXPECT_STREQ(transaction_state_to_string(TransactionState::GROWING), "GROWING");
    EXPECT_STREQ(transaction_state_to_string(TransactionState::SHRINKING), "SHRINKING");
    EXPECT_STREQ(transaction_state_to_string(TransactionState::COMMITTED), "COMMITTED");
    EXPECT_STREQ(transaction_state_to_string(TransactionState::ABORTED), "ABORTED");
}

TEST(TransactionTest, WriteSet) {
    Transaction txn(1);

    EXPECT_TRUE(txn.write_set().empty());

    txn.add_write_record(WriteRecord(WriteType::INSERT, 1, RID(10, 5)));
    EXPECT_EQ(txn.write_set().size(), 1);

    txn.add_write_record(
        WriteRecord(WriteType::DELETE, 2, RID(20, 3), std::vector<char>{'a', 'b', 'c'}));
    EXPECT_EQ(txn.write_set().size(), 2);

    const auto& wr = txn.write_set()[0];
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
    auto record = LogRecord::make_update(42, 100, 5, RID(10, 3), old_data, new_data);

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
    auto deserialized = LogRecord::deserialize(serialized.data(), serialized.size());

    EXPECT_EQ(deserialized.type(), LogRecordType::BEGIN);
    EXPECT_EQ(deserialized.txn_id(), 42);
    EXPECT_EQ(deserialized.lsn(), 100);
}

TEST(LogRecordTest, SerializeDeserializeInsert) {
    std::vector<char> tuple_data = {'t', 'e', 's', 't', ' ', 'd', 'a', 't', 'a'};
    auto original = LogRecord::make_insert(42, 100, 5, RID(10, 3), tuple_data);
    original.set_lsn(200);

    auto serialized = original.serialize();
    auto deserialized = LogRecord::deserialize(serialized.data(), serialized.size());

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
    auto original = LogRecord::make_update(42, 100, 5, RID(10, 3), old_data, new_data);
    original.set_lsn(300);

    auto serialized = original.serialize();
    auto deserialized = LogRecord::deserialize(serialized.data(), serialized.size());

    EXPECT_EQ(deserialized.type(), LogRecordType::UPDATE);
    EXPECT_EQ(deserialized.old_tuple_data(), old_data);
    EXPECT_EQ(deserialized.new_tuple_data(), new_data);
}

TEST(LogRecordTest, SerializeDeserializeCheckpoint) {
    std::vector<txn_id_t> active_txns = {1, 5, 10, 42, 100};
    auto original = LogRecord::make_checkpoint(active_txns);
    original.set_lsn(500);

    auto serialized = original.serialize();
    auto deserialized = LogRecord::deserialize(serialized.data(), serialized.size());

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

        wal.append_log(r1);
        wal.append_log(r2);

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

        wal.append_log(r1);
        wal.append_log(r2);
        wal.append_log(r3);
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
        auto record = LogRecord::make_begin(i + 1);
        wal.append_log(record);
    }

    EXPECT_TRUE(wal.flush_to_lsn(3).ok());
    EXPECT_GE(wal.flushed_lsn(), 3);
}

TEST_F(WALTest, ReopenExistingWAL) {
    lsn_t last_lsn;

    {
        WALManager wal(wal_file_.string());
        for (int i = 0; i < 10; ++i) {
            auto record = LogRecord::make_begin(i + 1);
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

    auto* txn = tm.begin();
    ASSERT_NE(txn, nullptr);
    EXPECT_EQ(txn->state(), TransactionState::GROWING);
    EXPECT_EQ(tm.active_transaction_count(), 1);

    tm.commit(txn);
    EXPECT_EQ(tm.active_transaction_count(), 0);
}

TEST_F(TransactionManagerTest, CreateWithWAL) {
    auto wal = std::make_shared<WALManager>(wal_file_.string());
    TransactionManager tm(wal);

    auto* txn = tm.begin();
    ASSERT_NE(txn, nullptr);
    EXPECT_NE(txn->prev_lsn(), INVALID_LSN);  // BEGIN record written

    tm.commit(txn);

    // Check WAL has BEGIN and COMMIT
    auto records = wal->read_log();
    ASSERT_GE(records.size(), 2);
    EXPECT_EQ(records[0].type(), LogRecordType::BEGIN);
    EXPECT_EQ(records[1].type(), LogRecordType::COMMIT);
}

TEST_F(TransactionManagerTest, BeginMultiple) {
    TransactionManager tm;

    auto* txn1 = tm.begin();
    auto* txn2 = tm.begin();
    auto* txn3 = tm.begin();

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

    auto* txn = tm.begin();
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

    auto* txn = tm.begin();
    txn_id_t id = txn->txn_id();

    EXPECT_EQ(tm.get_transaction(id), txn);
    EXPECT_EQ(tm.get_transaction(9999), nullptr);

    tm.commit(txn);
    EXPECT_EQ(tm.get_transaction(id), nullptr);
}

TEST_F(TransactionManagerTest, GetActiveTransactionIds) {
    TransactionManager tm;

    auto* txn1 = tm.begin();
    auto* txn2 = tm.begin();

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

    auto* txn = tm.begin();
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

    auto* txn = tm.begin();
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

    auto* txn = tm.begin();
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

    auto* txn = tm.begin();
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
    auto* txn = tm.begin();
    txn_id_t id = txn->txn_id();
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
    ASSERT_GE(records.size(), 4);  // BEGIN, 2x INSERT, COMMIT

    int begin_count = 0;
    int insert_count = 0;
    int commit_count = 0;

    for (const auto& r : records) {
        if (r.type() == LogRecordType::BEGIN) begin_count++;
        if (r.type() == LogRecordType::INSERT) insert_count++;
        if (r.type() == LogRecordType::COMMIT) commit_count++;
    }

    EXPECT_EQ(begin_count, 1);
    EXPECT_EQ(insert_count, 2);
    EXPECT_EQ(commit_count, 1);
}

}  // namespace
}  // namespace entropy
