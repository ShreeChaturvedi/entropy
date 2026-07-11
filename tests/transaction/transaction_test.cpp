/**
 * @file transaction_test.cpp
 * @brief Unit tests for Transaction, TransactionManager, LogRecord, and WAL
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "catalog/schema.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/disk_manager.hpp"
#include "storage/table_heap.hpp"
#include "storage/tuple.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/log_record.hpp"
#include "transaction/mvcc.hpp"
#include "transaction/recovery.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction/version_store.hpp"
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

TEST_F(WALTest, FlushInvokesDurableSync) {
  int sync_count = 0;
  WALManager wal(wal_file_.string());
  wal.set_sync_hook_for_testing([&]() -> Status {
    ++sync_count;
    return Status::Ok();
  });

  auto record = LogRecord::make_begin(1);
  ASSERT_NE(wal.append_log(record), INVALID_LSN);
  ASSERT_TRUE(wal.flush().ok());

  EXPECT_GE(sync_count, 1);
}

TEST_F(WALTest, FlushPropagatesSyncFailure) {
  WALManager wal(wal_file_.string());
  wal.set_sync_hook_for_testing(
      []() -> Status { return Status::IOError("injected sync failure"); });

  auto record = LogRecord::make_begin(1);
  ASSERT_NE(wal.append_log(record), INVALID_LSN);

  Status status = wal.flush();
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.is_io_error());
}

TEST_F(WALTest, AppendLogReturnsInvalidLsnWhenFlushFails) {
  WALManager wal(wal_file_.string());
  wal.set_sync_hook_for_testing(
      []() -> Status { return Status::IOError("injected sync failure"); });

  // Fill the buffer so the next append must flush first.
  std::vector<char> big_tuple(WAL_BUFFER_SIZE / 2, 'x');
  auto first = LogRecord::make_insert(1, INVALID_LSN, 1, RID(1, 0), big_tuple);
  ASSERT_NE(wal.append_log(first), INVALID_LSN);

  auto second = LogRecord::make_insert(1, 1, 1, RID(1, 1), big_tuple);
  EXPECT_EQ(wal.append_log(second), INVALID_LSN);
}

TEST_F(WALTest, FlushRetryAfterSyncFailureDoesNotDuplicateRecords) {
  // A failed sync leaves the records' bytes already handed to the log file, so
  // a retry must only re-drive the sync. Re-appending would write duplicate
  // records (same LSNs twice) into the log.
  bool fail_next_sync = true;
  lsn_t last_lsn = INVALID_LSN;
  {
    WALManager wal(wal_file_.string());
    wal.set_sync_hook_for_testing([&]() -> Status {
      if (fail_next_sync) {
        fail_next_sync = false;
        return Status::IOError("injected transient sync failure");
      }
      return Status::Ok();
    });

    auto r1 = LogRecord::make_begin(1);
    ASSERT_NE(wal.append_log(r1), INVALID_LSN);
    auto r2 = LogRecord::make_begin(2);
    last_lsn = wal.append_log(r2);
    ASSERT_NE(last_lsn, INVALID_LSN);

    // First flush: bytes reach the file, the sync fails, nothing is durable.
    EXPECT_FALSE(wal.flush().ok());
    EXPECT_LT(wal.flushed_lsn(), last_lsn);

    // Retry: only the sync runs; the records are not appended a second time.
    ASSERT_TRUE(wal.flush().ok());
    EXPECT_EQ(wal.flushed_lsn(), last_lsn);
  }

  // The log contains each record exactly once.
  WALManager wal(wal_file_.string());
  auto records = wal.read_log();
  ASSERT_EQ(records.size(), 2u);
  EXPECT_EQ(records[0].lsn(), 1u);
  EXPECT_EQ(records[0].txn_id(), 1u);
  EXPECT_EQ(records[1].lsn(), 2u);
  EXPECT_EQ(records[1].txn_id(), 2u);
}

TEST_F(WALTest, BufferBoundaryCommitIsDurable) {
  // Regression for the flushed_lsn accounting bug: when a COMMIT append
  // overflows the buffer, the overflow flush writes only the *prior* records
  // but must NOT mark the (not-yet-buffered) COMMIT as durable. Otherwise the
  // subsequent flush_to_lsn(commit) short-circuits and never fsyncs the commit,
  // silently losing a committed transaction on crash.
  int sync_count = 0;
  lsn_t commit_lsn = INVALID_LSN;
  {
    WALManager wal(wal_file_.string());
    wal.set_sync_hook_for_testing([&]() -> Status {
      ++sync_count;
      return Status::Ok();
    });

    // COMMIT records carry no payload, so their on-disk size is just the header.
    const uint32_t commit_size = LOG_RECORD_HEADER_SIZE;
    ASSERT_LT(commit_size, WAL_BUFFER_SIZE);

    // Fill the buffer to within less than a commit record of full using a single
    // INSERT, so appending the COMMIT triggers an overflow flush.
    const uint32_t insert_overhead =
        LOG_RECORD_HEADER_SIZE + sizeof(oid_t) + sizeof(RID) + sizeof(uint32_t);
    const uint32_t target_offset = WAL_BUFFER_SIZE - (commit_size / 2);
    ASSERT_GT(target_offset, insert_overhead);
    std::vector<char> tuple(target_offset - insert_overhead, 'x');
    auto insert = LogRecord::make_insert(1, INVALID_LSN, 1, RID(1, 0), tuple);
    ASSERT_NE(wal.append_log(insert), INVALID_LSN);
    ASSERT_EQ(wal.buffer_offset(), target_offset);
    // Appending the commit must overflow the buffer.
    ASSERT_GT(wal.buffer_offset() + commit_size, WAL_BUFFER_SIZE);

    auto commit = LogRecord::make_commit(1, insert.lsn());
    commit_lsn = wal.append_log(commit);
    ASSERT_NE(commit_lsn, INVALID_LSN);

    // The overflow flush persisted only the INSERT, so the COMMIT must not yet
    // count as durable.
    ASSERT_LT(wal.flushed_lsn(), commit_lsn);
    const int syncs_before = sync_count;

    // Forcing the commit durable must actually perform a sync and advance the
    // flushed LSN past the commit.
    ASSERT_TRUE(wal.flush_to_lsn(commit_lsn).ok());
    EXPECT_GT(sync_count, syncs_before);
    EXPECT_GE(wal.flushed_lsn(), commit_lsn);
  }

  // And the commit record must actually be on disk after reopen.
  WALManager wal(wal_file_.string());
  auto records = wal.read_log();
  ASSERT_FALSE(records.empty());
  EXPECT_EQ(records.back().type(), LogRecordType::COMMIT);
}

TEST_F(WALTest, ReadLogRejectsOversizedHeaderSize) {
  // Craft an INSERT record that is fully self-consistent (its embedded
  // tuple_size exactly matches the declared payload), so try_deserialize would
  // ACCEPT it if handed the bytes. Its only defect is a declared size just past
  // WAL_MAX_RECORD_SIZE. The record is written to the file in full, so the sole
  // thing that can reject it in read_log is the size upper-bound guard --
  // deleting `header.size > WAL_MAX_RECORD_SIZE` must make this test fail.
  const uint32_t corrupt_size = WAL_MAX_RECORD_SIZE + 1;
  const uint32_t payload = corrupt_size - LOG_RECORD_HEADER_SIZE;
  const uint32_t fixed = sizeof(oid_t) + sizeof(RID) + sizeof(uint32_t);
  ASSERT_GT(payload, fixed);
  const uint32_t tuple_size = payload - fixed;

  std::vector<char> buffer(corrupt_size, 0);
  LogRecordHeader header{};
  header.lsn = 1;
  header.txn_id = 1;
  header.prev_lsn = INVALID_LSN;
  header.size = corrupt_size;
  header.type = LogRecordType::INSERT;
  std::memcpy(buffer.data(), &header, LOG_RECORD_HEADER_SIZE);

  char* ptr = buffer.data() + LOG_RECORD_HEADER_SIZE;
  oid_t table_oid = 7;
  RID rid(3, 1);
  std::memcpy(ptr, &table_oid, sizeof(table_oid));
  ptr += sizeof(table_oid);
  std::memcpy(ptr, &rid, sizeof(rid));
  ptr += sizeof(rid);
  std::memcpy(ptr, &tuple_size, sizeof(tuple_size));
  // Remaining tuple bytes stay zero-filled; they are valid payload.

  // Guard against the record being rejected for any reason other than size:
  // handed the full buffer, try_deserialize must accept it.
  {
    LogRecord probe;
    ASSERT_TRUE(LogRecord::try_deserialize(buffer.data(), corrupt_size, probe));
    EXPECT_EQ(probe.type(), LogRecordType::INSERT);
  }

  {
    std::ofstream out(wal_file_, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out.write(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    ASSERT_TRUE(out.good());
  }

  WALManager wal(wal_file_.string());
  auto records = wal.read_log();
  // Only the size upper bound rejects this otherwise-valid record.
  EXPECT_TRUE(records.empty());
}

TEST(LogRecordTest, DeserializeInsertRejectsOversizedTuple) {
  // Minimal INSERT payload with a huge embedded tuple_size.
  constexpr uint32_t kClaimedTupleSize = 0x0FFFFFFFu;
  constexpr uint32_t kPayload = 4 + 8 + 4;  // oid + rid + tuple_size (no data)
  constexpr uint32_t kSize = LOG_RECORD_HEADER_SIZE + kPayload;
  std::vector<char> buffer(kSize, 0);

  LogRecordHeader header{};
  header.lsn = 1;
  header.txn_id = 1;
  header.prev_lsn = INVALID_LSN;
  header.size = kSize;
  header.type = LogRecordType::INSERT;
  std::memcpy(buffer.data(), &header, LOG_RECORD_HEADER_SIZE);

  char* ptr = buffer.data() + LOG_RECORD_HEADER_SIZE;
  oid_t table_oid = 10;
  RID rid(1, 0);
  std::memcpy(ptr, &table_oid, sizeof(table_oid));
  ptr += sizeof(table_oid);
  std::memcpy(ptr, &rid, sizeof(rid));
  ptr += sizeof(rid);
  std::memcpy(ptr, &kClaimedTupleSize, sizeof(kClaimedTupleSize));

  LogRecord out;
  EXPECT_FALSE(LogRecord::try_deserialize(buffer.data(), kSize, out));
}

TEST(LogRecordTest, DeserializeLegacyCheckpointPayload) {
  // A pre-existing WAL may hold CHECKPOINT records written before begin_lsn /
  // next_txn_id existed (payload = count + ids only). Those must still parse,
  // with the new fields defaulted.
  const std::vector<txn_id_t> active_txns = {7, 9};
  const uint32_t payload =
      sizeof(uint32_t) + static_cast<uint32_t>(active_txns.size() * sizeof(txn_id_t));
  const uint32_t size = LOG_RECORD_HEADER_SIZE + payload;
  std::vector<char> buffer(size, 0);

  LogRecordHeader header{};
  header.lsn = 5;
  header.txn_id = INVALID_TXN_ID;
  header.prev_lsn = INVALID_LSN;
  header.size = size;
  header.type = LogRecordType::CHECKPOINT;
  std::memcpy(buffer.data(), &header, LOG_RECORD_HEADER_SIZE);

  char* ptr = buffer.data() + LOG_RECORD_HEADER_SIZE;
  const uint32_t count = static_cast<uint32_t>(active_txns.size());
  std::memcpy(ptr, &count, sizeof(count));
  ptr += sizeof(count);
  for (txn_id_t txn_id : active_txns) {
    std::memcpy(ptr, &txn_id, sizeof(txn_id));
    ptr += sizeof(txn_id);
  }

  LogRecord out;
  ASSERT_TRUE(LogRecord::try_deserialize(buffer.data(), size, out));
  EXPECT_EQ(out.type(), LogRecordType::CHECKPOINT);
  EXPECT_EQ(out.active_txns(), active_txns);
  EXPECT_EQ(out.begin_lsn(), INVALID_LSN);
  EXPECT_EQ(out.next_txn_id(), INVALID_TXN_ID);
}

TEST(LogRecordTest, DeserializeCheckpointRejectsOversizedCount) {
  constexpr uint32_t kClaimedCount = 0x0FFFFFFFu;
  constexpr uint32_t kPayload = 4;  // count only, no txn ids
  constexpr uint32_t kSize = LOG_RECORD_HEADER_SIZE + kPayload;
  std::vector<char> buffer(kSize, 0);

  LogRecordHeader header{};
  header.lsn = 1;
  header.txn_id = INVALID_TXN_ID;
  header.prev_lsn = INVALID_LSN;
  header.size = kSize;
  header.type = LogRecordType::CHECKPOINT;
  std::memcpy(buffer.data(), &header, LOG_RECORD_HEADER_SIZE);
  std::memcpy(buffer.data() + LOG_RECORD_HEADER_SIZE, &kClaimedCount, sizeof(kClaimedCount));

  LogRecord out;
  EXPECT_FALSE(LogRecord::try_deserialize(buffer.data(), kSize, out));
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
// Abort undo — atomicity (issue #13)
// ─────────────────────────────────────────────────────────────────────────────

class AbortUndoTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("entropy_abort_undo_" + std::to_string(rand()));
    std::filesystem::create_directories(test_dir_);
    db_file_ = test_dir_ / "data.db";
    wal_file_ = test_dir_ / "test.wal";

    disk_manager_ = std::make_shared<FileDiskManager>(db_file_.string());
    buffer_pool_ = std::make_shared<BufferPoolManager>(16, disk_manager_);
    table_heap_ = std::make_shared<TableHeap>(buffer_pool_);

    std::vector<Column> columns = {
        Column("id", TypeId::INTEGER),
        Column("name", TypeId::VARCHAR, 64),
    };
    schema_ = Schema(std::move(columns));

    wal_ = std::make_shared<WALManager>(wal_file_.string());
    lock_mgr_ = std::make_unique<LockManager>(false, 100);
    tm_ = std::make_unique<TransactionManager>(wal_);
    tm_->set_lock_manager(lock_mgr_.get());
    tm_->set_table_resolver([this](oid_t oid) -> TableHeap * {
      return oid == kTableOid ? table_heap_.get() : nullptr;
    });
  }

  void TearDown() override {
    tm_.reset();
    lock_mgr_.reset();
    wal_.reset();
    table_heap_.reset();
    buffer_pool_.reset();
    disk_manager_.reset();
    std::filesystem::remove_all(test_dir_);
  }

  Tuple make_tuple(int32_t id, const std::string &name) {
    return Tuple({TupleValue(id), TupleValue(name)}, schema_);
  }

  std::vector<char> tuple_bytes(const Tuple &t) {
    return std::vector<char>(t.data(), t.data() + t.size());
  }

  bool tuple_exists(const RID &rid) {
    Tuple out;
    return table_heap_->get_tuple(rid, &out).ok();
  }

  static constexpr oid_t kTableOid = 1;

  std::filesystem::path test_dir_;
  std::filesystem::path db_file_;
  std::filesystem::path wal_file_;
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_;
  std::shared_ptr<TableHeap> table_heap_;
  Schema schema_;
  std::shared_ptr<WALManager> wal_;
  std::unique_ptr<LockManager> lock_mgr_;
  std::unique_ptr<TransactionManager> tm_;
};

TEST_F(AbortUndoTest, AbortUndoesInsert) {
  auto *txn = tm_->begin();
  ASSERT_NE(txn, nullptr);

  Tuple inserted = make_tuple(1, "alice");
  RID rid;
  ASSERT_TRUE(table_heap_->insert_tuple(inserted, &rid).ok());
  ASSERT_TRUE(tuple_exists(rid));

  tm_->log_insert(txn, kTableOid, rid, tuple_bytes(inserted));
  ASSERT_EQ(txn->write_set().size(), 1u);

  tm_->abort(txn);

  EXPECT_FALSE(tuple_exists(rid))
      << "Aborted INSERT must remove the row (atomicity)";
  EXPECT_EQ(tm_->active_transaction_count(), 0u);
}

TEST_F(AbortUndoTest, AbortUndoesUpdate) {
  Tuple original = make_tuple(1, "alice");
  RID rid;
  ASSERT_TRUE(table_heap_->insert_tuple(original, &rid).ok());

  auto *txn = tm_->begin();
  Tuple updated = make_tuple(1, "bob");
  ASSERT_TRUE(table_heap_->update_tuple(updated, rid).ok());

  tm_->log_update(txn, kTableOid, rid, tuple_bytes(original),
                  tuple_bytes(updated));

  tm_->abort(txn);

  Tuple restored;
  ASSERT_TRUE(table_heap_->get_tuple(rid, &restored).ok());
  EXPECT_EQ(restored.size(), original.size());
  EXPECT_EQ(std::memcmp(restored.data(), original.data(), original.size()), 0)
      << "Aborted UPDATE must restore old tuple data";
}

TEST_F(AbortUndoTest, AbortUndoesDelete) {
  Tuple original = make_tuple(7, "carol");
  RID rid;
  ASSERT_TRUE(table_heap_->insert_tuple(original, &rid).ok());

  auto *txn = tm_->begin();
  ASSERT_TRUE(table_heap_->delete_tuple(rid).ok());
  ASSERT_FALSE(tuple_exists(rid));

  tm_->log_delete(txn, kTableOid, rid, tuple_bytes(original));

  tm_->abort(txn);

  Tuple restored;
  ASSERT_TRUE(table_heap_->get_tuple(rid, &restored).ok())
      << "Aborted DELETE must restore the row at the same RID";
  EXPECT_EQ(std::memcmp(restored.data(), original.data(), original.size()), 0);
  EXPECT_EQ(restored.rid(), rid);
}

TEST_F(AbortUndoTest, AbortUndoesWritesInReverseOrder) {
  // Baseline row that survives; txn inserts then updates it, then aborts.
  Tuple baseline = make_tuple(1, "keep");
  RID keep_rid;
  ASSERT_TRUE(table_heap_->insert_tuple(baseline, &keep_rid).ok());

  auto *txn = tm_->begin();

  Tuple inserted = make_tuple(2, "temp");
  RID insert_rid;
  ASSERT_TRUE(table_heap_->insert_tuple(inserted, &insert_rid).ok());
  tm_->log_insert(txn, kTableOid, insert_rid, tuple_bytes(inserted));

  Tuple updated = make_tuple(1, "changed");
  ASSERT_TRUE(table_heap_->update_tuple(updated, keep_rid).ok());
  tm_->log_update(txn, kTableOid, keep_rid, tuple_bytes(baseline),
                  tuple_bytes(updated));

  tm_->abort(txn);

  EXPECT_FALSE(tuple_exists(insert_rid));
  Tuple restored;
  ASSERT_TRUE(table_heap_->get_tuple(keep_rid, &restored).ok());
  EXPECT_EQ(std::memcmp(restored.data(), baseline.data(), baseline.size()), 0);
}

TEST_F(AbortUndoTest, AbortReleasesLocks) {
  auto *txn = tm_->begin();
  ASSERT_TRUE(lock_mgr_->lock_table(txn, kTableOid, LockMode::EXCLUSIVE).ok());
  EXPECT_EQ(lock_mgr_->lock_table_size(), 1u);

  Tuple inserted = make_tuple(3, "locked");
  RID rid;
  ASSERT_TRUE(table_heap_->insert_tuple(inserted, &rid).ok());
  tm_->log_insert(txn, kTableOid, rid, tuple_bytes(inserted));

  tm_->abort(txn);

  EXPECT_EQ(lock_mgr_->lock_table_size(), 0u)
      << "Abort must release all locks held by the transaction";
  EXPECT_FALSE(tuple_exists(rid));
}

TEST_F(AbortUndoTest, DeadlockVictimAbortFinalizesThroughManager) {
  // A wait-die deadlock victim is marked ABORTED by the lock manager before
  // its thread calls TransactionManager::abort. That call must still finalize
  // the abort: undo the write set, append the WAL ABORT record, and remove the
  // transaction from the active set. Deadlock detection on, generous timeout
  // so resolution provably comes from the victim path, not the timeout.
  lock_mgr_ = std::make_unique<LockManager>(true, 5000);
  tm_->set_lock_manager(lock_mgr_.get());

  // Two committed baseline rows.
  Tuple row_a = make_tuple(1, "alpha");
  Tuple row_b = make_tuple(2, "beta");
  RID rid_a;
  RID rid_b;
  ASSERT_TRUE(table_heap_->insert_tuple(row_a, &rid_a).ok());
  ASSERT_TRUE(table_heap_->insert_tuple(row_b, &rid_b).ok());

  auto *older = tm_->begin();
  auto *younger = tm_->begin();  // larger txn id -> wait-die victim
  const txn_id_t younger_id = younger->txn_id();
  ASSERT_LT(older->txn_id(), younger_id);

  // Each transaction locks and updates its own row (real logged writes).
  ASSERT_TRUE(
      lock_mgr_->lock_row(older, kTableOid, rid_a, LockMode::EXCLUSIVE).ok());
  ASSERT_TRUE(
      lock_mgr_->lock_row(younger, kTableOid, rid_b, LockMode::EXCLUSIVE).ok());

  Tuple updated_a = make_tuple(1, "alpha2");
  ASSERT_TRUE(table_heap_->update_tuple(updated_a, rid_a).ok());
  tm_->log_update(older, kTableOid, rid_a, tuple_bytes(row_a),
                  tuple_bytes(updated_a));

  Tuple updated_b = make_tuple(2, "beta2");
  ASSERT_TRUE(table_heap_->update_tuple(updated_b, rid_b).ok());
  tm_->log_update(younger, kTableOid, rid_b, tuple_bytes(row_b),
                  tuple_bytes(updated_b));

  // Cross-lock to deadlock. The younger transaction is the wait-die victim;
  // its thread receives Status::Aborted and calls TransactionManager::abort --
  // the handoff under test. The victim keeps its X lock on rid_b until that
  // abort finishes undo, so the survivor acquires rid_b only AFTER the
  // rollback: it must observe the restored row, then its own committed write
  // must be what remains.
  Status older_status = Status::Ok();
  Status younger_status = Status::Ok();
  std::vector<char> survivor_saw;  // rid_b bytes right after acquisition
  bool survivor_updated = false;

  Tuple gamma = make_tuple(2, "gamma");

  std::thread t_old([&] {
    older_status =
        lock_mgr_->lock_row(older, kTableOid, rid_b, LockMode::EXCLUSIVE);
    if (older_status.ok()) {
      // Read what the lock now protects, then overwrite it and log the write,
      // exactly as a real writer would.
      Tuple current;
      if (table_heap_->get_tuple(rid_b, &current).ok()) {
        survivor_saw.assign(current.data(), current.data() + current.size());
      }
      if (table_heap_->update_tuple(gamma, rid_b).ok()) {
        tm_->log_update(older, kTableOid, rid_b, survivor_saw,
                        tuple_bytes(gamma));
        survivor_updated = true;
      }
    }
  });
  std::thread t_young([&] {
    // Let the older transaction block first so the cycle closes here.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    younger_status =
        lock_mgr_->lock_row(younger, kTableOid, rid_a, LockMode::EXCLUSIVE);
    if (younger_status.code() == StatusCode::kAborted) {
      // A real caller may do arbitrary work between the failed lock call and
      // abort(); correctness must not depend on the abort being instantaneous.
      // The victim's X lock on rid_b protects this whole window.
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      tm_->abort(younger);
    }
  });
  t_old.join();
  t_young.join();

  ASSERT_EQ(younger_status.code(), StatusCode::kAborted);
  ASSERT_TRUE(older_status.ok())
      << "survivor must acquire the victim's lock: " << older_status.message();

  // The survivor's first read under the lock saw the victim's write already
  // undone (undo-then-release ordering), never the aborted "beta2". A shrink
  // via TablePage::update_record keeps the old slot length (internal
  // fragmentation), so compare the restored prefix; the varchar length prefix
  // inside those bytes distinguishes "beta" from "beta2".
  ASSERT_GE(survivor_saw.size(), row_b.size());
  EXPECT_EQ(std::memcmp(survivor_saw.data(), row_b.data(), row_b.size()), 0)
      << "Survivor must never observe the victim's not-yet-rolled-back data";
  ASSERT_TRUE(survivor_updated);

  // The victim left the active-transaction set.
  bool victim_active = false;
  for (txn_id_t id : tm_->get_active_txn_ids()) {
    if (id == younger_id) {
      victim_active = true;
    }
  }
  EXPECT_FALSE(victim_active)
      << "Victim must be removed from the active transaction set";

  // A WAL ABORT record was appended for the victim.
  bool saw_abort = false;
  for (const auto &r : wal_->read_log()) {
    if (r.type() == LogRecordType::ABORT && r.txn_id() == younger_id) {
      saw_abort = true;
    }
  }
  EXPECT_TRUE(saw_abort) << "WAL must contain an ABORT record for the victim";

  // Survivor commits; its committed write is what remains -- the victim's
  // rollback must not have destroyed it.
  tm_->commit(older);
  Tuple final_row;
  ASSERT_TRUE(table_heap_->get_tuple(rid_b, &final_row).ok());
  ASSERT_GE(final_row.size(), gamma.size());
  EXPECT_EQ(std::memcmp(final_row.data(), gamma.data(), gamma.size()), 0)
      << "Victim rollback must not clobber the survivor's committed write";

  EXPECT_EQ(tm_->active_transaction_count(), 0u);
  EXPECT_EQ(lock_mgr_->lock_table_size(), 0u);
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
  lock_mgr_ = std::make_unique<LockManager>(true, 1000);

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
    db_file_ = test_dir_ / "test.db";
    std::filesystem::remove(wal_file_);
    std::filesystem::remove(db_file_);
  }

  void TearDown() override {
    // Close the database file before removing the directory: Windows refuses
    // to delete files with open handles.
    disk_manager_.reset();
    std::error_code ec;
    std::filesystem::remove_all(test_dir_, ec);
  }

  // Build a page store backed by this test's database file.
  std::shared_ptr<BufferPoolManager> make_buffer_pool() {
    disk_manager_ = std::make_shared<FileDiskManager>(db_file_.string());
    return std::make_shared<BufferPoolManager>(16, disk_manager_);
  }

  static std::vector<char> bytes(const std::string &s) {
    return std::vector<char>(s.begin(), s.end());
  }

  // Read the record bytes stored at rid, or nullopt if the slot is empty.
  std::optional<std::vector<char>> record_at(BufferPoolManager &pool,
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

  // Snapshot the raw bytes of a page (for idempotency comparisons).
  std::vector<char> page_snapshot(BufferPoolManager &pool, page_id_t page_id) {
    Page *page = pool.fetch_page(page_id);
    EXPECT_NE(page, nullptr);
    std::vector<char> snap(page->data(), page->data() + Page::kPageSize);
    pool.unpin_page(page_id, false);
    return snap;
  }

  std::filesystem::path test_dir_;
  std::filesystem::path wal_file_;
  std::filesystem::path db_file_;
  std::shared_ptr<DiskManager> disk_manager_;
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
  // A committed transaction's inserts must be replayed onto real pages at their
  // exact RIDs.
  {
    WALManager wal(wal_file_.string());
    auto begin = LogRecord::make_begin(1);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin);

    auto insert1 =
        LogRecord::make_insert(1, begin.lsn(), 10, RID(0, 0), bytes("data0"));
    lsn = wal.append_log(insert1);
    auto insert2 =
        LogRecord::make_insert(1, insert1.lsn(), 10, RID(0, 1), bytes("data1"));
    lsn = wal.append_log(insert2);
    auto commit = LogRecord::make_commit(1, insert2.lsn());
    lsn = wal.append_log(commit);
    (void)wal.flush();
  }

  auto pool = make_buffer_pool();
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(pool, wal, disk_manager_);

  ASSERT_TRUE(recovery.recover().ok());

  // Both tuples are present at their exact RIDs after redo.
  auto r00 = record_at(*pool, RID(0, 0));
  ASSERT_TRUE(r00.has_value());
  EXPECT_EQ(*r00, bytes("data0"));
  auto r01 = record_at(*pool, RID(0, 1));
  ASSERT_TRUE(r01.has_value());
  EXPECT_EQ(*r01, bytes("data1"));

  EXPECT_TRUE(recovery.committed_txns().count(1) > 0);
  EXPECT_TRUE(recovery.active_txn_table().empty());
}

TEST_F(RecoveryTest, UndoUncommittedTransaction) {
  // An uncommitted insert+update on the same RID must be fully rolled back,
  // leaving no surviving tuple.
  {
    WALManager wal(wal_file_.string());
    auto begin = LogRecord::make_begin(42);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin);

    auto insert =
        LogRecord::make_insert(42, begin.lsn(), 10, RID(0, 0), bytes("orig"));
    lsn = wal.append_log(insert);
    auto update = LogRecord::make_update(42, insert.lsn(), 10, RID(0, 0),
                                         bytes("orig"), bytes("updated"));
    lsn = wal.append_log(update);
    // No commit - crash simulation.
    (void)wal.flush();
  }

  auto pool = make_buffer_pool();
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(pool, wal, disk_manager_);

  ASSERT_TRUE(recovery.recover().ok());

  // Loser transaction identified, and undo left no trace of its row.
  EXPECT_TRUE(recovery.active_txn_table().count(42) > 0);
  EXPECT_TRUE(recovery.committed_txns().empty());
  EXPECT_FALSE(record_at(*pool, RID(0, 0)).has_value());
}

TEST_F(RecoveryTest, MixedCommittedAndUncommittedTransactions) {
  // Two committed transactions survive; the uncommitted one is rolled back.
  {
    WALManager wal(wal_file_.string());

    // Transaction 1: committed insert on page 0.
    auto begin1 = LogRecord::make_begin(1);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin1);
    auto insert1 =
        LogRecord::make_insert(1, begin1.lsn(), 10, RID(0, 0), bytes("aaa"));
    lsn = wal.append_log(insert1);
    auto commit1 = LogRecord::make_commit(1, insert1.lsn());
    lsn = wal.append_log(commit1);

    // Transaction 2: uncommitted insert on page 1.
    auto begin2 = LogRecord::make_begin(2);
    lsn = wal.append_log(begin2);
    auto insert2 =
        LogRecord::make_insert(2, begin2.lsn(), 20, RID(1, 0), bytes("bbb"));
    lsn = wal.append_log(insert2);
    // No commit.

    // Transaction 3: committed insert on page 2.
    auto begin3 = LogRecord::make_begin(3);
    lsn = wal.append_log(begin3);
    auto insert3 =
        LogRecord::make_insert(3, begin3.lsn(), 30, RID(2, 0), bytes("ccc"));
    lsn = wal.append_log(insert3);
    auto commit3 = LogRecord::make_commit(3, insert3.lsn());
    lsn = wal.append_log(commit3);

    (void)wal.flush();
  }

  auto pool = make_buffer_pool();
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(pool, wal, disk_manager_);

  ASSERT_TRUE(recovery.recover().ok());

  // Analysis: only txn 2 is a loser; txns 1 and 3 committed.
  EXPECT_EQ(recovery.active_txn_table().size(), 1);
  EXPECT_TRUE(recovery.active_txn_table().count(2) > 0);
  EXPECT_EQ(recovery.committed_txns().size(), 2);
  EXPECT_TRUE(recovery.committed_txns().count(1) > 0);
  EXPECT_TRUE(recovery.committed_txns().count(3) > 0);

  // Committed rows survive; the uncommitted row is gone.
  auto r1 = record_at(*pool, RID(0, 0));
  ASSERT_TRUE(r1.has_value());
  EXPECT_EQ(*r1, bytes("aaa"));
  auto r3 = record_at(*pool, RID(2, 0));
  ASSERT_TRUE(r3.has_value());
  EXPECT_EQ(*r3, bytes("ccc"));
  EXPECT_FALSE(record_at(*pool, RID(1, 0)).has_value());
}

TEST_F(RecoveryTest, RedoIsIdempotentAcrossRepeatedRecovery) {
  // Two committed inserts. Recovering a second time over the already-recovered
  // page must apply nothing and leave the page byte-for-byte identical.
  {
    WALManager wal(wal_file_.string());
    auto begin = LogRecord::make_begin(1);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin);
    auto insert1 =
        LogRecord::make_insert(1, begin.lsn(), 10, RID(0, 0), bytes("aa"));
    lsn = wal.append_log(insert1);
    auto insert2 =
        LogRecord::make_insert(1, insert1.lsn(), 10, RID(0, 1), bytes("bb"));
    lsn = wal.append_log(insert2);
    auto commit = LogRecord::make_commit(1, insert2.lsn());
    lsn = wal.append_log(commit);
    (void)wal.flush();
  }

  auto pool = make_buffer_pool();
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(pool, wal, disk_manager_);

  // First recovery applies both inserts.
  ASSERT_TRUE(recovery.recover().ok());
  EXPECT_EQ(recovery.redo_count(), 2u);
  std::vector<char> after_first = page_snapshot(*pool, 0);

  // Second recovery applies nothing (page LSN already covers both records).
  ASSERT_TRUE(recovery.recover().ok());
  EXPECT_EQ(recovery.redo_count(), 0u);
  std::vector<char> after_second = page_snapshot(*pool, 0);

  EXPECT_EQ(after_first, after_second);
}

TEST_F(RecoveryTest, UndoOfStolenPageSurvivesSecondCrash) {
  // Crash-safety of undo without CLRs. Scenario: loser T inserts row R and the
  // page is stolen (flushed pre-crash with page_lsn = the INSERT's LSN), so
  // redo is LSN-gated and only undo removes R. If the ABORT became durable
  // while the compensated page was only dirty in the pool, a second crash
  // would leave R on disk forever: T is no longer a loser and redo stays
  // gated. Recovery must therefore flush the undone page BEFORE the ABORT.
  lsn_t insert_lsn = INVALID_LSN;
  {
    WALManager wal(wal_file_.string());
    auto begin = LogRecord::make_begin(7);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin);
    auto insert =
        LogRecord::make_insert(7, begin.lsn(), 10, RID(0, 0), bytes("loser-row"));
    insert_lsn = wal.append_log(insert);
    ASSERT_NE(insert_lsn, INVALID_LSN);
    // No commit - crash simulation.
    (void)wal.flush();
  }

  // Steal: the page holding the uncommitted row reached disk pre-crash with
  // its LSN advanced to the INSERT's LSN.
  {
    auto setup_pool = make_buffer_pool();
    Page *page = setup_pool->fetch_page(0);
    ASSERT_NE(page, nullptr);
    TablePage table_page(page);
    table_page.init();
    const std::vector<char> row = bytes("loser-row");
    auto slot = table_page.insert_record(row.data(), static_cast<uint16_t>(row.size()));
    ASSERT_TRUE(slot.has_value());
    ASSERT_EQ(*slot, 0u);
    page->set_lsn(insert_lsn);
    setup_pool->unpin_page(0, true);
    ASSERT_TRUE(setup_pool->flush_page(0));
  }

  // Crash 1 -> first recovery: redo is gated, undo removes R and must make the
  // compensation durable before the ABORT it appends.
  auto pool1 = make_buffer_pool();
  auto wal1 = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery1(pool1, wal1, disk_manager_);
  ASSERT_TRUE(recovery1.recover().ok());
  EXPECT_EQ(recovery1.redo_count(), 0u);
  EXPECT_EQ(recovery1.undo_count(), 1u);

  // Crash 2 before pool1 flushes anything further: read the on-disk state
  // through a fresh buffer pool while pool1 is still alive (its cached pages
  // never reach disk). The durable ABORT means T is no longer a loser, so
  // only the pre-ABORT page flush can have removed R.
  auto pool2 = make_buffer_pool();
  auto wal2 = std::make_shared<WALManager>(wal_file_.string());
  auto records = wal2->read_log();
  ASSERT_FALSE(records.empty());
  EXPECT_EQ(records.back().type(), LogRecordType::ABORT);

  RecoveryManager recovery2(pool2, wal2, disk_manager_);
  ASSERT_TRUE(recovery2.recover().ok());
  EXPECT_TRUE(recovery2.active_txn_table().empty());
  EXPECT_EQ(recovery2.redo_count(), 0u);
  EXPECT_EQ(recovery2.undo_count(), 0u);
  EXPECT_FALSE(record_at(*pool2, RID(0, 0)).has_value());
}

TEST_F(RecoveryTest, UndoSkipsSlotReusedByCommittedTransaction) {
  // Loser T1 inserts and deletes (0,0); committed T2 then reuses the slot.
  // A blind undo of T1's INSERT would delete T2's committed row; the
  // state-checked undo must leave it untouched.
  {
    WALManager wal(wal_file_.string());

    auto begin1 = LogRecord::make_begin(1);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin1);
    auto insert1 =
        LogRecord::make_insert(1, begin1.lsn(), 10, RID(0, 0), bytes("AAAA"));
    lsn = wal.append_log(insert1);
    auto delete1 =
        LogRecord::make_delete(1, insert1.lsn(), 10, RID(0, 0), bytes("AAAA"));
    lsn = wal.append_log(delete1);
    // No commit for T1 - loser.

    auto begin2 = LogRecord::make_begin(2);
    lsn = wal.append_log(begin2);
    auto insert2 =
        LogRecord::make_insert(2, begin2.lsn(), 10, RID(0, 0), bytes("BBBB"));
    lsn = wal.append_log(insert2);
    auto commit2 = LogRecord::make_commit(2, insert2.lsn());
    lsn = wal.append_log(commit2);

    (void)wal.flush();
  }

  auto pool = make_buffer_pool();
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(pool, wal, disk_manager_);

  ASSERT_TRUE(recovery.recover().ok());

  // T1 is the only loser, but both of its undo ops must skip: the slot is
  // occupied by T2's bytes (undo-DELETE needs an empty slot; undo-INSERT
  // needs the slot to still hold T1's bytes).
  EXPECT_TRUE(recovery.active_txn_table().count(1) > 0);
  EXPECT_TRUE(recovery.committed_txns().count(2) > 0);
  EXPECT_EQ(recovery.undo_count(), 0u);

  auto r = record_at(*pool, RID(0, 0));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(*r, bytes("BBBB"));
}

TEST_F(RecoveryTest, WarmPoolRetryFlushesCompensationBeforeAbort) {
  // A first recover() may apply compensations in-pool and then fail the page
  // flush transiently (error returned, no ABORT appended - correct). A retry
  // on the same warm pool applies nothing (the state-checked undo skips every
  // op), but it must STILL flush the loser's page before making any ABORT
  // durable, or the compensation dies with the pool while the durable ABORT
  // removes the transaction from every future loser set.
  lsn_t insert_lsn = INVALID_LSN;
  {
    WALManager wal(wal_file_.string());
    auto begin = LogRecord::make_begin(9);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin);
    auto insert =
        LogRecord::make_insert(9, begin.lsn(), 10, RID(0, 0), bytes("loser-row"));
    insert_lsn = wal.append_log(insert);
    ASSERT_NE(insert_lsn, INVALID_LSN);
    // No commit - crash simulation.
    (void)wal.flush();
  }

  // Disk: stolen page holding the uncommitted row (page_lsn = insert LSN).
  {
    auto setup_pool = make_buffer_pool();
    Page *page = setup_pool->fetch_page(0);
    ASSERT_NE(page, nullptr);
    TablePage table_page(page);
    table_page.init();
    const std::vector<char> row = bytes("loser-row");
    auto slot =
        table_page.insert_record(row.data(), static_cast<uint16_t>(row.size()));
    ASSERT_TRUE(slot.has_value());
    ASSERT_EQ(*slot, 0u);
    page->set_lsn(insert_lsn);
    setup_pool->unpin_page(0, true);
    ASSERT_TRUE(setup_pool->flush_page(0));
  }

  // Warm pool exactly as a failed first recovery leaves it: the compensation
  // applied in-pool (row deleted, page dirty) but never flushed to disk.
  auto pool = make_buffer_pool();
  {
    Page *page = pool->fetch_page(0);
    ASSERT_NE(page, nullptr);
    TablePage table_page(page);
    ASSERT_TRUE(table_page.delete_record(0));
    pool->unpin_page(0, true);
  }

  auto disk_row_present = [this]() {
    auto probe_disk = std::make_shared<FileDiskManager>(db_file_.string());
    BufferPoolManager probe_pool(4, probe_disk);
    Page *page = probe_pool.fetch_page(0);
    EXPECT_NE(page, nullptr);
    TablePage table_page(page);
    const bool present = !table_page.get_record(0).empty();
    probe_pool.unpin_page(0, false);
    return present;
  };
  ASSERT_TRUE(disk_row_present());

  // Ordering probe: the only WAL sync during this recover() is the one that
  // makes the ABORT durable; at that moment the compensation must already be
  // on disk.
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  bool abort_sync_seen = false;
  bool compensated_at_abort_sync = false;
  wal->set_sync_hook_for_testing([&]() -> Status {
    abort_sync_seen = true;
    compensated_at_abort_sync = !disk_row_present();
    return Status::Ok();
  });

  RecoveryManager recovery(pool, wal, disk_manager_);
  ASSERT_TRUE(recovery.recover().ok());

  // The retry applied nothing, yet the compensation reached disk before the
  // ABORT became durable.
  EXPECT_EQ(recovery.undo_count(), 0u);
  EXPECT_TRUE(abort_sync_seen);
  EXPECT_TRUE(compensated_at_abort_sync);
  EXPECT_FALSE(disk_row_present());
}

TEST_F(RecoveryTest, SeededManagersNeverReuseTxnId) {
  // #19: a transaction manager restarted from a WAL must consume the recovered
  // high-water mark so post-restart transactions can never alias recovered ids.
  {
    auto wal = std::make_shared<WALManager>(wal_file_.string());
    TransactionManager tm(wal);
    auto *t1 = tm.begin();
    auto *t2 = tm.begin();
    auto *t3 = tm.begin();
    EXPECT_GT(t3->txn_id(), t1->txn_id());
    tm.commit(t1);
    tm.commit(t2);
    tm.commit(t3);
  }

  // Recover the next-txn-id high-water mark from the same WAL.
  auto pool = make_buffer_pool();
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(pool, wal, disk_manager_);
  ASSERT_TRUE(recovery.recover().ok());
  const txn_id_t seed = recovery.next_txn_id();
  EXPECT_GE(seed, 4u); // strictly past the recovered ids 1..3

  // A fresh manager seeded from recovery must not reissue any recovered id.
  TransactionManager tm2(wal);
  tm2.seed_next_txn_id(seed);
  auto *t = tm2.begin();
  EXPECT_GE(t->txn_id(), seed);
  EXPECT_GT(t->txn_id(), 3u);
  tm2.commit(t);
}

TEST_F(RecoveryTest, UndoInsertSkipsLongerCommittedRowSharingPrefix) {
  // Loser T1 inserts "AB" at (0,0) and deletes it; committed T2 reuses the
  // slot with "ABxy". A prefix match would mistake T2's longer row for T1's
  // insert and destroy committed data; the exact-length check must skip it.
  {
    WALManager wal(wal_file_.string());

    auto begin1 = LogRecord::make_begin(1);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin1);
    auto insert1 =
        LogRecord::make_insert(1, begin1.lsn(), 10, RID(0, 0), bytes("AB"));
    lsn = wal.append_log(insert1);
    auto delete1 =
        LogRecord::make_delete(1, insert1.lsn(), 10, RID(0, 0), bytes("AB"));
    lsn = wal.append_log(delete1);
    // No commit for T1 - loser.

    auto begin2 = LogRecord::make_begin(2);
    lsn = wal.append_log(begin2);
    auto insert2 =
        LogRecord::make_insert(2, begin2.lsn(), 10, RID(0, 0), bytes("ABxy"));
    lsn = wal.append_log(insert2);
    auto commit2 = LogRecord::make_commit(2, insert2.lsn());
    lsn = wal.append_log(commit2);

    (void)wal.flush();
  }

  auto pool = make_buffer_pool();
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(pool, wal, disk_manager_);

  ASSERT_TRUE(recovery.recover().ok());
  EXPECT_EQ(recovery.undo_count(), 0u);

  auto r = record_at(*pool, RID(0, 0));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(*r, bytes("ABxy"));
}

TEST_F(RecoveryTest, UndoRemovesGrownLoserRow) {
  // Loser inserts a small row then grows it via UPDATE (the before-image is a
  // prefix of the after-image, the nastiest length case). Undo must restore
  // the before-image with its exact length and then remove the insert
  // entirely - the exact-length INSERT check may not strand the row.
  {
    WALManager wal(wal_file_.string());
    auto begin = LogRecord::make_begin(3);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin);
    auto insert =
        LogRecord::make_insert(3, begin.lsn(), 10, RID(0, 0), bytes("AB"));
    lsn = wal.append_log(insert);
    auto update = LogRecord::make_update(3, insert.lsn(), 10, RID(0, 0),
                                         bytes("AB"), bytes("ABCDEFGH"));
    lsn = wal.append_log(update);
    // No commit - crash simulation.
    (void)wal.flush();
  }

  auto pool = make_buffer_pool();
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(pool, wal, disk_manager_);

  ASSERT_TRUE(recovery.recover().ok());
  EXPECT_EQ(recovery.undo_count(), 2u);
  EXPECT_FALSE(record_at(*pool, RID(0, 0)).has_value());
}

TEST_F(RecoveryTest, RedoReinsertsIntoReusedSlot) {
  // Delete then re-insert at the same RID pre-crash: redo must reproduce the
  // slot reuse exactly (insert_record_at into the emptied slot), leaving the
  // re-inserted bytes at the original RID.
  {
    WALManager wal(wal_file_.string());
    auto begin = LogRecord::make_begin(1);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin);
    auto insert1 =
        LogRecord::make_insert(1, begin.lsn(), 10, RID(0, 0), bytes("first"));
    lsn = wal.append_log(insert1);
    auto del =
        LogRecord::make_delete(1, insert1.lsn(), 10, RID(0, 0), bytes("first"));
    lsn = wal.append_log(del);
    auto insert2 =
        LogRecord::make_insert(1, del.lsn(), 10, RID(0, 0), bytes("second"));
    lsn = wal.append_log(insert2);
    auto commit = LogRecord::make_commit(1, insert2.lsn());
    lsn = wal.append_log(commit);
    (void)wal.flush();
  }

  auto pool = make_buffer_pool();
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(pool, wal, disk_manager_);

  ASSERT_TRUE(recovery.recover().ok());
  EXPECT_EQ(recovery.redo_count(), 3u);

  auto r = record_at(*pool, RID(0, 0));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(*r, bytes("second"));
}

TEST_F(RecoveryTest, RedoAppliesGrownUpdate) {
  // An UPDATE whose tuple grew relocates the record within the page (the slot
  // is repointed at fresh bytes). Redo must reproduce the grown after-image at
  // the same RID.
  const std::vector<char> small = bytes("aa");
  const std::vector<char> grown = bytes("a-much-longer-tuple-value");
  {
    WALManager wal(wal_file_.string());
    auto begin = LogRecord::make_begin(1);
    [[maybe_unused]] lsn_t lsn = wal.append_log(begin);
    auto insert = LogRecord::make_insert(1, begin.lsn(), 10, RID(0, 0), small);
    lsn = wal.append_log(insert);
    auto update =
        LogRecord::make_update(1, insert.lsn(), 10, RID(0, 0), small, grown);
    lsn = wal.append_log(update);
    auto commit = LogRecord::make_commit(1, update.lsn());
    lsn = wal.append_log(commit);
    (void)wal.flush();
  }

  auto pool = make_buffer_pool();
  auto wal = std::make_shared<WALManager>(wal_file_.string());
  RecoveryManager recovery(pool, wal, disk_manager_);

  ASSERT_TRUE(recovery.recover().ok());
  EXPECT_EQ(recovery.redo_count(), 2u);

  auto r = record_at(*pool, RID(0, 0));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(*r, grown);
}

TEST_F(RecoveryTest, CheckpointAnchorSkipsPreCheckpointRedo) {
  // create_checkpoint flushes the WAL and all dirty pages, then records its
  // own LSN as begin_lsn. Recovery must not re-apply records older than the
  // anchor, while pre-checkpoint committed rows still survive (they are
  // already durable on their pages).
  auto pool = make_buffer_pool();
  auto wal = std::make_shared<WALManager>(wal_file_.string());

  // Committed txn 1, applied to page 0 (durable via the checkpoint's flush).
  auto begin1 = LogRecord::make_begin(1);
  [[maybe_unused]] lsn_t lsn = wal->append_log(begin1);
  auto insert1 =
      LogRecord::make_insert(1, begin1.lsn(), 10, RID(0, 0), bytes("pre"));
  lsn = wal->append_log(insert1);
  auto commit1 = LogRecord::make_commit(1, insert1.lsn());
  lsn = wal->append_log(commit1);
  {
    Page *page = pool->fetch_page(0);
    ASSERT_NE(page, nullptr);
    TablePage table_page(page);
    table_page.init();
    const std::vector<char> row = bytes("pre");
    auto slot =
        table_page.insert_record(row.data(), static_cast<uint16_t>(row.size()));
    ASSERT_TRUE(slot.has_value());
    ASSERT_EQ(*slot, 0u);
    page->set_lsn(insert1.lsn());
    pool->unpin_page(0, true);
  }

  RecoveryManager checkpointer(pool, wal, disk_manager_);
  ASSERT_TRUE(checkpointer.create_checkpoint({}).ok());

  // Committed txn 2 after the checkpoint, never applied to any page (crash
  // before its effects reached the buffer pool).
  auto begin2 = LogRecord::make_begin(2);
  lsn = wal->append_log(begin2);
  auto insert2 =
      LogRecord::make_insert(2, begin2.lsn(), 10, RID(1, 0), bytes("post"));
  lsn = wal->append_log(insert2);
  auto commit2 = LogRecord::make_commit(2, insert2.lsn());
  lsn = wal->append_log(commit2);
  ASSERT_TRUE(wal->flush().ok());

  // Crash: recover through fresh managers over the same files.
  auto pool2 = make_buffer_pool();
  auto wal2 = std::make_shared<WALManager>(wal_file_.string());

  // The persisted checkpoint carries a real anchor: its own LSN.
  auto records = wal2->read_log();
  auto checkpoint_it =
      std::find_if(records.begin(), records.end(), [](const LogRecord &r) {
        return r.type() == LogRecordType::CHECKPOINT;
      });
  ASSERT_NE(checkpoint_it, records.end());
  EXPECT_EQ(checkpoint_it->begin_lsn(), checkpoint_it->lsn());

  RecoveryManager recovery(pool2, wal2, disk_manager_);
  ASSERT_TRUE(recovery.recover().ok());

  // Only txn 2's insert is replayed; the pre-checkpoint insert is skipped by
  // the anchor, yet its committed row survives from the checkpoint's flush.
  EXPECT_EQ(recovery.redo_count(), 1u);
  auto pre = record_at(*pool2, RID(0, 0));
  ASSERT_TRUE(pre.has_value());
  EXPECT_EQ(*pre, bytes("pre"));
  auto post = record_at(*pool2, RID(1, 0));
  ASSERT_TRUE(post.has_value());
  EXPECT_EQ(*post, bytes("post"));
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
  mvcc_.finalize_commit(version, &txn1, 100);

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
  mvcc_.finalize_commit(version, &creator, 10); // Committed at ts 10

  // Delete and commit deletion
  mvcc_.mark_deleted(version, &deleter);
  mvcc_.finalize_commit(version, &deleter, 20); // Deletion committed at ts 20

  EXPECT_EQ(version.begin_ts, 10);
  EXPECT_EQ(version.end_ts, 20);
  EXPECT_TRUE(version.is_deleted());
}

TEST_F(MVCCTest, RollbackVersion) {
  Transaction txn(1);
  VersionInfo version;

  mvcc_.init_version(version, &txn);
  mvcc_.rollback_version(version, &txn);

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

// ─────────────────────────────────────────────────────────────────────────────
// WP5: TransactionManager <-> MVCC / VersionStore integration
//
// Exercises the wiring itself: begin()/commit() draw timestamps from the single
// logical clock, commit() finalizes version stamps, and abort() both flushes
// its undo compensation before the durable ABORT and drops uncommitted versions.
// ─────────────────────────────────────────────────────────────────────────────

class MVCCTxnIntegrationTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("entropy_wp5_" + std::to_string(rand()));
    std::filesystem::create_directories(test_dir_);
    db_file_ = test_dir_ / "data.db";
    wal_file_ = test_dir_ / "test.wal";

    disk_manager_ = std::make_shared<FileDiskManager>(db_file_.string());
    buffer_pool_ = std::make_shared<BufferPoolManager>(16, disk_manager_);
    table_heap_ = std::make_shared<TableHeap>(buffer_pool_);

    std::vector<Column> columns = {
        Column("id", TypeId::INTEGER),
        Column("name", TypeId::VARCHAR, 64),
    };
    schema_ = Schema(std::move(columns));

    wal_ = std::make_shared<WALManager>(wal_file_.string());
    lock_mgr_ = std::make_unique<LockManager>(false, 100);
    mvcc_ = std::make_shared<MVCCManager>();
    version_store_ = std::make_shared<VersionStore>(*mvcc_);

    tm_ = std::make_unique<TransactionManager>(wal_);
    tm_->set_lock_manager(lock_mgr_.get());
    tm_->set_mvcc(mvcc_);
    tm_->set_version_store(version_store_);
    tm_->set_buffer_pool(buffer_pool_.get());
    tm_->set_table_resolver([this](oid_t oid) -> TableHeap * {
      return oid == kTableOid ? table_heap_.get() : nullptr;
    });
  }

  void TearDown() override {
    tm_.reset();
    version_store_.reset();
    mvcc_.reset();
    lock_mgr_.reset();
    wal_.reset();
    table_heap_.reset();
    buffer_pool_.reset();
    disk_manager_.reset();
    std::error_code ec;
    std::filesystem::remove_all(test_dir_, ec);
  }

  Tuple make_tuple(int32_t id, const std::string &name) {
    return Tuple({TupleValue(id), TupleValue(name)}, schema_);
  }
  std::vector<char> tuple_bytes(const Tuple &t) {
    return std::vector<char>(t.data(), t.data() + t.size());
  }
  static std::span<const char> span_of(const std::vector<char> &v) {
    return std::span<const char>(v.data(), v.size());
  }
  bool tuple_exists(const RID &rid) {
    Tuple out;
    return table_heap_->get_tuple(rid, &out).ok();
  }

  static constexpr oid_t kTableOid = 1;

  std::filesystem::path test_dir_;
  std::filesystem::path db_file_;
  std::filesystem::path wal_file_;
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_;
  std::shared_ptr<TableHeap> table_heap_;
  Schema schema_;
  std::shared_ptr<WALManager> wal_;
  std::unique_ptr<LockManager> lock_mgr_;
  std::shared_ptr<MVCCManager> mvcc_;
  std::shared_ptr<VersionStore> version_store_;
  std::unique_ptr<TransactionManager> tm_;
};

// begin() seeds start_ts from the logical clock (not steady_clock), so a
// snapshot cannot see a version committed after it began. The review-demanded
// clock-unification test.
TEST_F(MVCCTxnIntegrationTest, SnapshotDoesNotSeeCommitAfterItStarted) {
  const RID rid(0, 0);
  const std::vector<char> heap = tuple_bytes(make_tuple(1, "v1"));

  // A takes its snapshot first. On a fresh clock the first stamp is small and
  // logical (== TIMESTAMP_PREHISTORY + 1), proving it came from get_timestamp()
  // rather than a steady_clock nanosecond count.
  auto *a = tm_->begin();
  EXPECT_EQ(a->start_ts(), TIMESTAMP_PREHISTORY + 1)
      << "begin() must seed start_ts from MVCCManager::get_timestamp()";

  // B begins strictly after A, writes rid, and commits.
  auto *b = tm_->begin();
  ASSERT_TRUE(version_store_->on_insert(b, rid).ok());
  tm_->commit(b); // commit_ts drawn from the same clock, > a's start_ts

  // Unified clock: b's begin_ts (its commit_ts) exceeds a's snapshot, so a
  // cannot see the row at all.
  auto seen = version_store_->read_visible(rid, span_of(heap), a);
  EXPECT_FALSE(seen.has_value())
      << "snapshot A must not observe a version committed after it began";

  // A reader whose snapshot starts after the commit does see it.
  auto *late = tm_->begin();
  auto late_seen = version_store_->read_visible(rid, span_of(heap), late);
  ASSERT_TRUE(late_seen.has_value());
  EXPECT_EQ(*late_seen, heap);

  tm_->commit(a);
  tm_->commit(late);
}

// First-updater-wins fires across the unified clock: A cannot overwrite a row
// that B committed after A's snapshot (the lost-update the review flagged).
TEST_F(MVCCTxnIntegrationTest, LostUpdateBecomesConflictAcrossUnifiedClock) {
  const RID rid(0, 0);
  const std::vector<char> v1 = tuple_bytes(make_tuple(1, "v1"));

  auto *creator = tm_->begin();
  ASSERT_TRUE(version_store_->on_insert(creator, rid).ok());
  tm_->commit(creator);

  auto *a = tm_->begin(); // A's snapshot

  auto *b = tm_->begin();
  ASSERT_TRUE(version_store_->on_update(b, rid, span_of(v1)).ok());
  tm_->commit(b); // committed strictly after A's snapshot

  // A's update must be rejected: B's committed version is newer than A saw.
  const Status s = version_store_->on_update(a, rid, span_of(v1));
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.code(), StatusCode::kAborted);

  tm_->abort(a);
}

// commit() finalizes version timestamps: a later snapshot reads the new value,
// an earlier snapshot keeps reading the retained before-image.
TEST_F(MVCCTxnIntegrationTest, CommitFinalizesTimestampsForSnapshotReaders) {
  const RID rid(0, 0);
  const std::vector<char> v1 = tuple_bytes(make_tuple(1, "v1"));
  const std::vector<char> v2 = tuple_bytes(make_tuple(1, "v2"));

  auto *creator = tm_->begin();
  ASSERT_TRUE(version_store_->on_insert(creator, rid).ok());
  tm_->commit(creator);

  auto *early = tm_->begin(); // snapshot while the heap holds v1

  auto *updater = tm_->begin();
  ASSERT_TRUE(version_store_->on_update(updater, rid, span_of(v1)).ok());
  tm_->commit(updater); // heap now conceptually holds v2

  auto *late = tm_->begin(); // snapshot after the update committed

  auto early_view = version_store_->read_visible(rid, span_of(v2), early);
  ASSERT_TRUE(early_view.has_value());
  EXPECT_EQ(*early_view, v1) << "earlier snapshot reads the before-image";

  auto late_view = version_store_->read_visible(rid, span_of(v2), late);
  ASSERT_TRUE(late_view.has_value());
  EXPECT_EQ(*late_view, v2) << "later snapshot reads the finalized new value";

  tm_->commit(early);
  tm_->commit(late);
}

// abort() undoes the heap mutation AND drops the transaction's uncommitted
// versions from the store.
TEST_F(MVCCTxnIntegrationTest, AbortRestoresHeapAndDropsUncommittedVersions) {
  auto *txn = tm_->begin();
  Tuple t = make_tuple(1, "alice");
  RID rid;
  ASSERT_TRUE(table_heap_->insert_tuple(t, &rid).ok());
  ASSERT_TRUE(version_store_->on_insert(txn, rid).ok());
  tm_->log_insert(txn, kTableOid, rid, tuple_bytes(t));

  ASSERT_TRUE(tuple_exists(rid));
  ASSERT_EQ(version_store_->chain_length(rid), 1u);

  tm_->abort(txn);

  EXPECT_FALSE(tuple_exists(rid)) << "aborted INSERT must be undone in the heap";
  EXPECT_EQ(version_store_->chain_length(rid), 0u)
      << "abort must drop the uncommitted version";
  EXPECT_EQ(tm_->active_transaction_count(), 0u);
}

// abort() must flush its undo compensation to disk BEFORE the WAL ABORT record
// is made durable (mirrors recovery's WarmPoolRetryFlushesCompensationBeforeAbort).
TEST_F(MVCCTxnIntegrationTest, AbortFlushesCompensationBeforeAbortRecord) {
  auto *txn = tm_->begin();
  Tuple t = make_tuple(1, "loser");
  RID rid;
  ASSERT_TRUE(table_heap_->insert_tuple(t, &rid).ok());
  tm_->log_insert(txn, kTableOid, rid, tuple_bytes(t));

  // Steal: force the uncommitted row to disk before the abort.
  ASSERT_TRUE(buffer_pool_->flush_page(rid.page_id));

  const page_id_t page_id = rid.page_id;
  const slot_id_t slot_id = rid.slot_id;
  auto disk_row_present = [this, page_id, slot_id]() {
    auto probe_disk = std::make_shared<FileDiskManager>(db_file_.string());
    BufferPoolManager probe_pool(4, probe_disk);
    Page *page = probe_pool.fetch_page(page_id);
    EXPECT_NE(page, nullptr);
    TablePage table_page(page);
    const bool present = !table_page.get_record(slot_id).empty();
    probe_pool.unpin_page(page_id, false);
    return present;
  };
  ASSERT_TRUE(disk_row_present());

  // Probe at the ABORT sync: the compensation must already be on disk.
  bool abort_sync_seen = false;
  bool compensated_at_abort_sync = false;
  wal_->set_sync_hook_for_testing([&]() -> Status {
    abort_sync_seen = true;
    compensated_at_abort_sync = !disk_row_present();
    return Status::Ok();
  });

  tm_->abort(txn);

  // Drop the hook so a teardown flush cannot call into freed stack captures.
  wal_->set_sync_hook_for_testing(nullptr);

  EXPECT_TRUE(abort_sync_seen);
  EXPECT_TRUE(compensated_at_abort_sync)
      << "undo compensation must reach disk before the ABORT record is synced";
  EXPECT_FALSE(disk_row_present());
}

} // namespace
} // namespace entropy
