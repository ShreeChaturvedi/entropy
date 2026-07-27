/**
 * @file wal_truncate_test.cpp
 * @brief Unit tests for LogStore prefix truncation and checkpoint reclaim.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "test_utils.hpp"
#include "sim/sim_log_store.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/disk_manager.hpp"
#include "storage/page.hpp"
#include "storage/table_page.hpp"
#include "transaction/log_record.hpp"
#include "transaction/recovery.hpp"
#include "transaction/wal.hpp"

namespace entropy {
namespace {

// ─────────────────────────────────────────────────────────────────────────────
// FileLogStore::truncate_prefix
// ─────────────────────────────────────────────────────────────────────────────

TEST(FileLogStoreTruncateTest, AppendSyncTruncateReadAllMatchesSuffix) {
  test::TempFile tmp("wal_trunc_file_");
  FileLogStore store(tmp.string());

  const std::string prefix = "PREFIX_BYTES_AAA";
  const std::string suffix = "SUFFIX_BYTES_BBB";
  ASSERT_TRUE(
      store.append(std::span<const char>(prefix.data(), prefix.size())).ok());
  ASSERT_TRUE(
      store.append(std::span<const char>(suffix.data(), suffix.size())).ok());
  ASSERT_TRUE(store.sync().ok());
  ASSERT_EQ(store.size(), prefix.size() + suffix.size());

  ASSERT_TRUE(store.truncate_prefix(prefix.size()).ok());
  EXPECT_EQ(store.size(), suffix.size());

  const auto bytes = store.read_all();
  EXPECT_EQ(std::string(bytes.begin(), bytes.end()), suffix);
}

TEST(FileLogStoreTruncateTest, OffsetZeroIsNoOp) {
  test::TempFile tmp("wal_trunc_zero_");
  FileLogStore store(tmp.string());
  const std::string data = "keep-me";
  ASSERT_TRUE(store.append(std::span<const char>(data.data(), data.size())).ok());
  ASSERT_TRUE(store.truncate_prefix(0).ok());
  EXPECT_EQ(store.size(), data.size());
  EXPECT_EQ(store.read_all().size(), data.size());
}

TEST(FileLogStoreTruncateTest, OffsetEqualsSizeEmpties) {
  test::TempFile tmp("wal_trunc_empty_");
  FileLogStore store(tmp.string());
  const std::string data = "gone";
  ASSERT_TRUE(store.append(std::span<const char>(data.data(), data.size())).ok());
  ASSERT_TRUE(store.truncate_prefix(store.size()).ok());
  EXPECT_EQ(store.size(), 0u);
  EXPECT_TRUE(store.read_all().empty());
}

TEST(FileLogStoreTruncateTest, OffsetBeyondSizeIsInvalid) {
  test::TempFile tmp("wal_trunc_oob_");
  FileLogStore store(tmp.string());
  const std::string data = "x";
  ASSERT_TRUE(store.append(std::span<const char>(data.data(), data.size())).ok());
  auto st = store.truncate_prefix(store.size() + 1);
  EXPECT_FALSE(st.ok());
  EXPECT_EQ(store.size(), 1u);
}

TEST(FileLogStoreTruncateTest, AppendAfterTruncateContinues) {
  test::TempFile tmp("wal_trunc_append_");
  FileLogStore store(tmp.string());
  const std::string a = "AAAA";
  const std::string b = "BBBB";
  const std::string c = "CCCC";
  ASSERT_TRUE(store.append(std::span<const char>(a.data(), a.size())).ok());
  ASSERT_TRUE(store.append(std::span<const char>(b.data(), b.size())).ok());
  ASSERT_TRUE(store.truncate_prefix(a.size()).ok());
  ASSERT_TRUE(store.append(std::span<const char>(c.data(), c.size())).ok());
  const auto bytes = store.read_all();
  EXPECT_EQ(std::string(bytes.begin(), bytes.end()), b + c);
}

// ─────────────────────────────────────────────────────────────────────────────
// SimLogStore::truncate_prefix
// ─────────────────────────────────────────────────────────────────────────────

TEST(SimLogStoreTruncateTest, TruncatePrefixAndDurableWatermark) {
  sim::FaultConfig cfg;
  sim::SimLogStore store(/*seed=*/7, cfg, nullptr);

  const std::string a = "AAAA";
  const std::string b = "BBBB";
  const std::string c = "CCCC";
  ASSERT_TRUE(store.append(std::span<const char>(a.data(), a.size())).ok());
  ASSERT_TRUE(store.sync().ok());
  ASSERT_TRUE(store.append(std::span<const char>(b.data(), b.size())).ok());
  ASSERT_TRUE(store.sync().ok());
  ASSERT_TRUE(store.append(std::span<const char>(c.data(), c.size())).ok());

  ASSERT_TRUE(store.truncate_prefix(a.size()).ok());
  EXPECT_EQ(store.size(), b.size() + c.size());
  const auto bytes = store.read_all();
  EXPECT_EQ(std::string(bytes.begin(), bytes.end()), b + c);

  // Crash should still respect the adjusted durable watermark (b durable, c not).
  auto survived = store.crash();
  // With default fates the whole tail may be kept or lost; durable prefix of
  // post-truncate store is at least "BBBB" when the tail is lost.
  ASSERT_GE(survived.size(), b.size());
  EXPECT_TRUE(std::equal(b.begin(), b.end(), survived.begin()));
}

TEST(SimLogStoreTruncateTest, CrashedStoreNoOps) {
  sim::FaultConfig cfg;
  cfg.wal_tail_lost_ppk = 1000;
  sim::SimLogStore store(/*seed=*/1, cfg, nullptr);
  const std::string a = "AAAA";
  const std::string b = "BBBB";
  ASSERT_TRUE(store.append(std::span<const char>(a.data(), a.size())).ok());
  ASSERT_TRUE(store.sync().ok());
  ASSERT_TRUE(store.append(std::span<const char>(b.data(), b.size())).ok());
  auto survived = store.crash();
  EXPECT_EQ(survived, std::vector<char>(a.begin(), a.end()));

  // After crash the live store is frozen; truncate is a silent no-op.
  EXPECT_TRUE(store.truncate_prefix(2).ok());
  EXPECT_EQ(store.size(), 0u);  // crash() moved bytes out
}

// ─────────────────────────────────────────────────────────────────────────────
// WALManager::truncate_before
// ─────────────────────────────────────────────────────────────────────────────

TEST(WALTruncateTest, TruncateBeforeKeepsSuffixRecords) {
  test::TempFile tmp("wal_mgr_trunc_");
  WALManager wal(tmp.string());

  auto r1 = LogRecord::make_begin(1);
  ASSERT_NE(wal.append_log(r1), INVALID_LSN);
  auto r2 = LogRecord::make_commit(1, r1.lsn());
  ASSERT_NE(wal.append_log(r2), INVALID_LSN);
  auto r3 = LogRecord::make_begin(2);
  ASSERT_NE(wal.append_log(r3), INVALID_LSN);
  auto r4 = LogRecord::make_commit(2, r3.lsn());
  ASSERT_NE(wal.append_log(r4), INVALID_LSN);
  ASSERT_TRUE(wal.flush().ok());

  const lsn_t cut = r3.lsn();  // keep r3 and r4
  ASSERT_TRUE(wal.truncate_before(cut).ok());

  auto records = wal.read_log();
  ASSERT_EQ(records.size(), 2u);
  EXPECT_EQ(records[0].lsn(), r3.lsn());
  EXPECT_EQ(records[0].type(), LogRecordType::BEGIN);
  EXPECT_EQ(records[1].lsn(), r4.lsn());
  EXPECT_EQ(records[1].type(), LogRecordType::COMMIT);
}

TEST(WALTruncateTest, TruncateBeforeInvalidIsNoOp) {
  test::TempFile tmp("wal_mgr_noop_");
  WALManager wal(tmp.string());
  auto r = LogRecord::make_begin(1);
  ASSERT_NE(wal.append_log(r), INVALID_LSN);
  ASSERT_TRUE(wal.flush().ok());
  ASSERT_TRUE(wal.truncate_before(INVALID_LSN).ok());
  EXPECT_EQ(wal.read_log().size(), 1u);
}

TEST(WALTruncateTest, TruncateBeforePastAllEmpties) {
  test::TempFile tmp("wal_mgr_empty_");
  WALManager wal(tmp.string());
  auto r = LogRecord::make_begin(1);
  ASSERT_NE(wal.append_log(r), INVALID_LSN);
  ASSERT_TRUE(wal.flush().ok());
  ASSERT_TRUE(wal.truncate_before(r.lsn() + 100).ok());
  EXPECT_TRUE(wal.read_log().empty());
}

TEST(WALTruncateTest, ReopenAfterTruncateSeesOnlySuffix) {
  test::TempFile tmp("wal_mgr_reopen_");
  lsn_t keep_lsn = INVALID_LSN;
  {
    WALManager wal(tmp.string());
    auto a = LogRecord::make_begin(1);
    ASSERT_NE(wal.append_log(a), INVALID_LSN);
    auto b = LogRecord::make_begin(2);
    ASSERT_NE(wal.append_log(b), INVALID_LSN);
    keep_lsn = b.lsn();
    ASSERT_TRUE(wal.flush().ok());
    ASSERT_TRUE(wal.truncate_before(keep_lsn).ok());
  }
  WALManager reopened(tmp.string());
  auto records = reopened.read_log();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].lsn(), keep_lsn);
  EXPECT_EQ(reopened.next_lsn(), keep_lsn + 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Checkpoint + truncate + recover
// ─────────────────────────────────────────────────────────────────────────────

class CheckpointTruncateRecoveryTest : public ::testing::Test {
protected:
  void SetUp() override {
    db_ = std::make_unique<test::TempFile>("ckpt_trunc_db_");
    wal_path_ = db_->path().string() + ".wal";
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove(wal_path_, ec);
  }

  static std::vector<char> bytes(const char *s) {
    return std::vector<char>(s, s + std::strlen(s));
  }

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

  std::unique_ptr<test::TempFile> db_;
  std::string wal_path_;
};

TEST_F(CheckpointTruncateRecoveryTest, RecoverAfterCheckpointTruncation) {
  // Pre-checkpoint committed data is forced to pages by create_checkpoint;
  // pre-checkpoint WAL records are then truncated. Recovery must still see
  // those rows from disk and replay only post-checkpoint work.
  auto disk = std::make_shared<FileDiskManager>(db_->string());
  auto pool = std::make_shared<BufferPoolManager>(16, disk);
  auto wal = std::make_shared<WALManager>(wal_path_);

  auto begin1 = LogRecord::make_begin(1);
  ASSERT_NE(wal->append_log(begin1), INVALID_LSN);
  auto insert1 =
      LogRecord::make_insert(1, begin1.lsn(), 10, RID(0, 0), bytes("pre"));
  ASSERT_NE(wal->append_log(insert1), INVALID_LSN);
  auto commit1 = LogRecord::make_commit(1, insert1.lsn());
  ASSERT_NE(wal->append_log(commit1), INVALID_LSN);
  {
    Page *page = pool->fetch_page(0);
    ASSERT_NE(page, nullptr);
    TablePage table_page(page);
    table_page.init();
    const auto row = bytes("pre");
    auto slot =
        table_page.insert_record(row.data(), static_cast<uint16_t>(row.size()));
    ASSERT_TRUE(slot.has_value());
    page->set_lsn(insert1.lsn());
    pool->unpin_page(0, true);
  }

  RecoveryManager checkpointer(pool, wal, disk);
  ASSERT_TRUE(checkpointer.create_checkpoint({}).ok());

  // Pre-checkpoint records must be gone; checkpoint remains.
  {
    auto records = wal->read_log();
    ASSERT_FALSE(records.empty());
    EXPECT_EQ(records.front().type(), LogRecordType::CHECKPOINT);
    for (const auto &r : records) {
      EXPECT_NE(r.type(), LogRecordType::BEGIN)
          << "pre-checkpoint BEGIN should have been truncated";
    }
  }

  auto begin2 = LogRecord::make_begin(2);
  ASSERT_NE(wal->append_log(begin2), INVALID_LSN);
  auto insert2 =
      LogRecord::make_insert(2, begin2.lsn(), 10, RID(1, 0), bytes("post"));
  ASSERT_NE(wal->append_log(insert2), INVALID_LSN);
  auto commit2 = LogRecord::make_commit(2, insert2.lsn());
  ASSERT_NE(wal->append_log(commit2), INVALID_LSN);
  ASSERT_TRUE(wal->flush().ok());

  // Crash: fresh stack over the same files (writer pool not destroyed, so
  // dirty pages that were not checkpoint-flushed stay non-durable — but
  // create_checkpoint already flushed; post rows were never applied).
  auto disk2 = std::make_shared<FileDiskManager>(db_->string());
  auto pool2 = std::make_shared<BufferPoolManager>(16, disk2);
  auto wal2 = std::make_shared<WALManager>(wal_path_);

  RecoveryManager recovery(pool2, wal2, disk2);
  ASSERT_TRUE(recovery.recover().ok());

  EXPECT_EQ(recovery.redo_count(), 1u);
  auto pre = record_at(*pool2, RID(0, 0));
  ASSERT_TRUE(pre.has_value());
  EXPECT_EQ(*pre, bytes("pre"));
  auto post = record_at(*pool2, RID(1, 0));
  ASSERT_TRUE(post.has_value());
  EXPECT_EQ(*post, bytes("post"));
}

TEST_F(CheckpointTruncateRecoveryTest, ActiveTxnsSkipTruncation) {
  auto disk = std::make_shared<FileDiskManager>(db_->string());
  auto pool = std::make_shared<BufferPoolManager>(16, disk);
  auto wal = std::make_shared<WALManager>(wal_path_);

  auto begin1 = LogRecord::make_begin(1);
  ASSERT_NE(wal->append_log(begin1), INVALID_LSN);
  auto insert1 =
      LogRecord::make_insert(1, begin1.lsn(), 10, RID(0, 0), bytes("active"));
  ASSERT_NE(wal->append_log(insert1), INVALID_LSN);
  {
    Page *page = pool->fetch_page(0);
    ASSERT_NE(page, nullptr);
    TablePage table_page(page);
    table_page.init();
    const auto row = bytes("active");
    (void)table_page.insert_record(row.data(),
                                   static_cast<uint16_t>(row.size()));
    page->set_lsn(insert1.lsn());
    pool->unpin_page(0, true);
  }

  RecoveryManager checkpointer(pool, wal, disk);
  // Active txn 1: must keep pre-checkpoint records for undo.
  ASSERT_TRUE(checkpointer.create_checkpoint({1}).ok());

  auto records = wal->read_log();
  bool saw_begin = false;
  bool saw_checkpoint = false;
  for (const auto &r : records) {
    if (r.type() == LogRecordType::BEGIN) {
      saw_begin = true;
    }
    if (r.type() == LogRecordType::CHECKPOINT) {
      saw_checkpoint = true;
    }
  }
  EXPECT_TRUE(saw_begin) << "active txn history must not be truncated";
  EXPECT_TRUE(saw_checkpoint);
}

}  // namespace
}  // namespace entropy
