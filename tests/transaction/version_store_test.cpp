/**
 * @file version_store_test.cpp
 * @brief Outcome-based tests for MVCC version chains (VersionStore) and the
 *        MVCCManager creator/deleter distinction (#25, #27).
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "common/types.hpp"
#include "transaction/mvcc.hpp"
#include "transaction/transaction.hpp"
#include "transaction/version_store.hpp"

namespace entropy {
namespace {

// steady_clock value in the same units Transaction uses for start_ts.
uint64_t steady_now() {
  return static_cast<uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
}

// Busy-wait until the steady clock is strictly past t, then return it. Lets a
// test construct a transaction whose start_ts is guaranteed to exceed a chosen
// commit timestamp, independent of clock resolution.
uint64_t wait_until_after(uint64_t t) {
  uint64_t now = steady_now();
  while (now <= t) {
    now = steady_now();
  }
  return now;
}

std::span<const char> span_of(const std::string &s) {
  return std::span<const char>(s.data(), s.size());
}

std::string bytes_of(std::span<const char> s) {
  return std::string(s.begin(), s.end());
}

// ─────────────────────────────────────────────────────────────────────────────
// VersionStore outcome tests
// ─────────────────────────────────────────────────────────────────────────────

class VersionStoreTest : public ::testing::Test {
protected:
  MVCCManager mvcc_;
  VersionStore store_{mvcc_};
  RID rid_{1, 0};
};

// #27: aborting a DELETE must not erase the committed row it targeted.
TEST_F(VersionStoreTest, AbortedDeleteLeavesCommittedRowReadable) {
  Transaction creator(1);
  ASSERT_TRUE(store_.on_insert(&creator, rid_).ok());
  store_.finalize(&creator, mvcc_.get_timestamp());

  const std::string heap = "ROW-V1";

  Transaction reader(2);
  auto before = store_.read_visible(rid_, span_of(heap), &reader);
  ASSERT_TRUE(before.has_value());
  EXPECT_EQ(bytes_of(*before), "ROW-V1");

  // A deleter removes the row, then aborts.
  Transaction deleter(3);
  ASSERT_TRUE(store_.on_delete(&deleter, rid_, span_of(heap)).ok());
  // While the delete is in flight, the deleter itself no longer sees the row.
  EXPECT_FALSE(store_.read_visible(rid_, span_of(heap), &deleter).has_value());
  store_.rollback(&deleter);

  // The committed row is readable again by everyone.
  auto after = store_.read_visible(rid_, span_of(heap), &reader);
  ASSERT_TRUE(after.has_value());
  EXPECT_EQ(bytes_of(*after), "ROW-V1");

  Transaction later_reader(4);
  auto later = store_.read_visible(rid_, span_of(heap), &later_reader);
  ASSERT_TRUE(later.has_value());
  EXPECT_EQ(bytes_of(*later), "ROW-V1");
}

// Two concurrent transactions updating the same RID: exactly one succeeds, the
// other gets a conflict Status.
TEST_F(VersionStoreTest, ConcurrentUpdateExactlyOneWinner) {
  Transaction creator(1);
  ASSERT_TRUE(store_.on_insert(&creator, rid_).ok());
  store_.finalize(&creator, mvcc_.get_timestamp());

  const std::string heap = "V1";
  Transaction a(2);
  Transaction b(3);

  const Status sa = store_.on_update(&a, rid_, span_of(heap));
  const Status sb = store_.on_update(&b, rid_, span_of(heap));

  EXPECT_TRUE(sa.ok());
  EXPECT_FALSE(sb.ok());
  EXPECT_EQ(sb.code(), StatusCode::kAborted);

  // A delete by the loser conflicts too.
  const Status sb_del = store_.on_delete(&b, rid_, span_of(heap));
  EXPECT_EQ(sb_del.code(), StatusCode::kAborted);
}

// First-updater-wins also rejects overwriting a version committed after the
// writer's snapshot (a lost update via an unseen committed version).
TEST_F(VersionStoreTest, UpdateConflictsWithCommittedVersionNewerThanSnapshot) {
  Transaction writer(1);
  Transaction other(2);
  ASSERT_TRUE(store_.on_insert(&other, rid_).ok());
  // Commit the other transaction's version strictly after writer's snapshot.
  store_.finalize(&other, writer.start_ts() + 1000);

  const std::string heap = "V1";
  const Status s = store_.on_update(&writer, rid_, span_of(heap));
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.code(), StatusCode::kAborted);
}

// A reader on an older snapshot reads the before-image while a newer committed
// update exists; a reader on a fresh snapshot sees the new version.
TEST_F(VersionStoreTest, OlderSnapshotReadsBeforeImage) {
  Transaction creator(1);
  ASSERT_TRUE(store_.on_insert(&creator, rid_).ok());

  Transaction reader(2);
  const uint64_t s_r = reader.start_ts();
  store_.finalize(&creator, s_r); // original visible to reader (begin_ts == s_r)

  const std::string heap_v1 = "V1";
  auto v1 = store_.read_visible(rid_, span_of(heap_v1), &reader);
  ASSERT_TRUE(v1.has_value());
  EXPECT_EQ(bytes_of(*v1), "V1");

  // A writer updates and commits strictly after the reader's snapshot.
  Transaction writer(3);
  ASSERT_TRUE(store_.on_update(&writer, rid_, span_of(heap_v1)).ok());
  const uint64_t s_w = s_r + 1;
  store_.finalize(&writer, s_w);

  const std::string heap_v2 = "V2"; // heap now holds the new bytes

  // Old snapshot still reads the before-image.
  auto old_view = store_.read_visible(rid_, span_of(heap_v2), &reader);
  ASSERT_TRUE(old_view.has_value());
  EXPECT_EQ(bytes_of(*old_view), "V1");

  // A snapshot taken after the update sees the new version.
  wait_until_after(s_w);
  Transaction fresh(4);
  auto new_view = store_.read_visible(rid_, span_of(heap_v2), &fresh);
  ASSERT_TRUE(new_view.has_value());
  EXPECT_EQ(bytes_of(*new_view), "V2");
}

// GC prunes a before-image only once no active snapshot can reach it.
TEST_F(VersionStoreTest, GcPrunesSupersededBeforeImages) {
  Transaction creator(1);
  ASSERT_TRUE(store_.on_insert(&creator, rid_).ok());
  store_.finalize(&creator, 10);

  const std::string heap = "V1";
  Transaction updater(2);
  ASSERT_TRUE(store_.on_update(&updater, rid_, span_of(heap)).ok());
  store_.finalize(&updater, 20);

  ASSERT_EQ(store_.chain_length(rid_), 2u);

  // An active snapshot as old as 15 still needs the before-image.
  store_.gc(15);
  EXPECT_EQ(store_.chain_length(rid_), 2u);

  // With the oldest snapshot at 25, the superseded version is unreachable.
  store_.gc(25);
  EXPECT_EQ(store_.chain_length(rid_), 1u);
}

// GC drops an entire chain once its committed delete is visible to everyone.
TEST_F(VersionStoreTest, GcDropsCommittedDeletedChain) {
  Transaction creator(1);
  ASSERT_TRUE(store_.on_insert(&creator, rid_).ok());
  store_.finalize(&creator, 10);

  const std::string heap = "V1";
  Transaction deleter(2);
  ASSERT_TRUE(store_.on_delete(&deleter, rid_, span_of(heap)).ok());
  store_.finalize(&deleter, 20);

  ASSERT_EQ(store_.chain_count(), 1u);

  // A snapshot at 15 predates the delete: the chain must be retained.
  store_.gc(15);
  EXPECT_EQ(store_.chain_count(), 1u);

  // Once every active snapshot is past the delete, the chain is dropped.
  store_.gc(25);
  EXPECT_EQ(store_.chain_count(), 0u);
}

// Rollback removes an uncommitted insert entirely.
TEST_F(VersionStoreTest, RollbackRemovesUncommittedInsert) {
  Transaction txn(1);
  ASSERT_TRUE(store_.on_insert(&txn, rid_).ok());
  ASSERT_EQ(store_.chain_length(rid_), 1u);

  store_.rollback(&txn);
  EXPECT_EQ(store_.chain_length(rid_), 0u);
}

// Rollback of an update drops the new version and restores the committed one.
TEST_F(VersionStoreTest, RollbackRevertsUncommittedUpdate) {
  Transaction creator(1);
  ASSERT_TRUE(store_.on_insert(&creator, rid_).ok());
  store_.finalize(&creator, 10);

  const std::string heap_v1 = "V1";
  Transaction updater(2);
  ASSERT_TRUE(store_.on_update(&updater, rid_, span_of(heap_v1)).ok());
  ASSERT_EQ(store_.chain_length(rid_), 2u);

  store_.rollback(&updater);
  EXPECT_EQ(store_.chain_length(rid_), 1u);

  Transaction reader(3);
  auto v = store_.read_visible(rid_, span_of(heap_v1), &reader);
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(bytes_of(*v), "V1");
}

// ─────────────────────────────────────────────────────────────────────────────
// Read-your-own-writes
// ─────────────────────────────────────────────────────────────────────────────

// A transaction sees its own uncommitted insert; a concurrent one does not.
TEST_F(VersionStoreTest, ReadYourOwnUncommittedInsert) {
  Transaction txn(5);
  ASSERT_TRUE(store_.on_insert(&txn, rid_).ok());

  const std::string heap = "MINE";
  auto own = store_.read_visible(rid_, span_of(heap), &txn);
  ASSERT_TRUE(own.has_value());
  EXPECT_EQ(bytes_of(*own), "MINE");

  Transaction other(6);
  EXPECT_FALSE(store_.read_visible(rid_, span_of(heap), &other).has_value())
      << "another transaction must not see an uncommitted insert";
}

// A transaction reads its own uncommitted update (the heap's new bytes), while a
// concurrent reader still sees the committed before-image.
TEST_F(VersionStoreTest, ReadYourOwnUncommittedUpdate) {
  Transaction creator(1);
  ASSERT_TRUE(store_.on_insert(&creator, rid_).ok());
  store_.finalize(&creator, 10);

  const std::string v1 = "V1";
  Transaction writer(2);
  ASSERT_TRUE(store_.on_update(&writer, rid_, span_of(v1)).ok());

  const std::string v2 = "V2"; // heap now holds the writer's new bytes
  auto own = store_.read_visible(rid_, span_of(v2), &writer);
  ASSERT_TRUE(own.has_value());
  EXPECT_EQ(bytes_of(*own), "V2") << "writer reads its own uncommitted update";

  Transaction reader(3);
  reader.set_start_ts(15); // snapshot after the base commit, before writer's
  auto seen = store_.read_visible(rid_, span_of(v2), &reader);
  ASSERT_TRUE(seen.has_value());
  EXPECT_EQ(bytes_of(*seen), "V1")
      << "a concurrent reader still sees the committed before-image";
}

// First-updater-wins holds regardless of arrival order: whichever transaction
// stages its write first wins, the other conflicts.
TEST_F(VersionStoreTest, FirstUpdaterWinsBothOrders) {
  auto seed_committed = [&](RID rid) {
    Transaction creator(1);
    ASSERT_TRUE(store_.on_insert(&creator, rid).ok());
    store_.finalize(&creator, 10);
  };
  const std::string heap = "V1";

  // Order 1: A updates first, then B.
  {
    RID rid{2, 0};
    seed_committed(rid);
    Transaction a(2), b(3);
    EXPECT_TRUE(store_.on_update(&a, rid, span_of(heap)).ok());
    EXPECT_EQ(store_.on_update(&b, rid, span_of(heap)).code(),
              StatusCode::kAborted);
  }
  // Order 2: B updates first, then A — the winner is simply whoever was first.
  {
    RID rid{3, 0};
    seed_committed(rid);
    Transaction a(2), b(3);
    EXPECT_TRUE(store_.on_update(&b, rid, span_of(heap)).ok());
    EXPECT_EQ(store_.on_update(&a, rid, span_of(heap)).code(),
              StatusCode::kAborted);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Concurrency: read_visible returns an OWNED copy, so it is safe against a
// concurrent rollback/gc that frees the underlying before-image. These stress
// tests must be race-free under ThreadSanitizer and must never return a torn or
// garbage buffer.
// ─────────────────────────────────────────────────────────────────────────────

// Readers hammer a RID while a single writer repeatedly stages and rolls back
// an update. Every read must return the committed base value, never a partial
// or freed buffer.
TEST_F(VersionStoreTest, ConcurrentReadVisibleAndRollbackStayConsistent) {
  Transaction creator(1);
  ASSERT_TRUE(store_.on_insert(&creator, rid_).ok());
  store_.finalize(&creator, 10); // base committed at ts 10
  const std::string base = "BASE-VALUE";

  std::atomic<bool> stop{false};
  std::atomic<int> bad{0};
  std::atomic<uint64_t> reads{0};

  std::vector<std::thread> readers;
  for (int i = 0; i < 3; ++i) {
    readers.emplace_back([&, i] {
      // Default start_ts is the steady-clock stamp, far past 10, so the base is
      // always visible.
      Transaction reader(static_cast<txn_id_t>(100 + i));
      while (!stop.load(std::memory_order_relaxed)) {
        auto v = store_.read_visible(rid_, span_of(base), &reader);
        if (!v.has_value() || bytes_of(*v) != base) {
          bad.fetch_add(1, std::memory_order_relaxed);
        }
        reads.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  std::thread writer([&] {
    Transaction w(2);
    for (int i = 0; i < 3000; ++i) {
      if (store_.on_update(&w, rid_, span_of(base)).ok()) {
        store_.rollback(&w); // frees the head + before-image the readers walk
      }
    }
    stop.store(true, std::memory_order_relaxed);
  });

  writer.join();
  for (auto &t : readers) {
    t.join();
  }

  EXPECT_EQ(bad.load(), 0) << "a reader observed a non-base / torn value";
  EXPECT_GT(reads.load(), 0u);
}

// Readers pinned to a snapshot that can only see a superseded before-image read
// it while gc concurrently prunes that version. Each read must return either the
// owned before-image bytes or nullopt — never garbage from a freed buffer.
TEST_F(VersionStoreTest, ConcurrentReadVisibleAndGcStayConsistent) {
  Transaction creator(1);
  ASSERT_TRUE(store_.on_insert(&creator, rid_).ok());
  store_.finalize(&creator, 10);

  const std::string v1 = "V1-before-image";
  Transaction updater(2);
  ASSERT_TRUE(store_.on_update(&updater, rid_, span_of(v1)).ok());
  store_.finalize(&updater, 20); // chain: base(before=V1) -> V2 head
  const std::string v2 = "V2-heap-value";

  std::atomic<bool> stop{false};
  std::atomic<int> bad{0};

  std::vector<std::thread> readers;
  for (int i = 0; i < 4; ++i) {
    readers.emplace_back([&] {
      Transaction reader(50);
      reader.set_start_ts(15); // sees the before-image, not the ts-20 head
      while (!stop.load(std::memory_order_relaxed)) {
        auto v = store_.read_visible(rid_, span_of(v2), &reader);
        // The pruning race can legitimately turn the answer into nullopt, but a
        // returned value must be the intact before-image, never torn bytes.
        if (v.has_value() && bytes_of(*v) != v1) {
          bad.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  // Churn gc across the boundary that prunes the superseded before-image.
  std::thread collector([&] {
    for (int i = 0; i < 5000; ++i) {
      store_.gc(i % 2 == 0 ? 15 : 25);
    }
    stop.store(true, std::memory_order_relaxed);
  });

  collector.join();
  for (auto &t : readers) {
    t.join();
  }

  EXPECT_EQ(bad.load(), 0) << "a reader observed a torn before-image";
}

// ─────────────────────────────────────────────────────────────────────────────
// MVCCManager creator/deleter distinction (#27)
// ─────────────────────────────────────────────────────────────────────────────

// Committing a creator must not stamp another transaction's pending delete.
TEST(MVCCManagerCreatorDeleterTest, FinalizeCommitStampsOnlyOwnFields) {
  MVCCManager mvcc;
  Transaction creator(1);
  Transaction deleter(2);
  VersionInfo v;
  mvcc.init_version(v, &creator);
  mvcc.mark_deleted(v, &deleter); // uncommitted delete by a different txn

  mvcc.finalize_commit(v, &creator, 100);
  EXPECT_EQ(v.begin_ts, 100u);
  EXPECT_EQ(v.end_ts, TIMESTAMP_MAX); // deleter has not committed
  EXPECT_EQ(v.deleted_by, 2u);

  // Later, the deleter commits and only then is end_ts stamped.
  mvcc.finalize_commit(v, &deleter, 200);
  EXPECT_EQ(v.begin_ts, 100u);
  EXPECT_EQ(v.end_ts, 200u);
}

// Aborting a delete leaves the underlying committed creation intact.
TEST(MVCCManagerCreatorDeleterTest, RollbackDeleteKeepsCommittedCreation) {
  MVCCManager mvcc;
  Transaction creator(1);
  Transaction deleter(2);
  VersionInfo v;
  mvcc.init_version(v, &creator);
  mvcc.finalize_commit(v, &creator, 10);
  mvcc.mark_deleted(v, &deleter);

  mvcc.rollback_version(v, &deleter);

  EXPECT_EQ(v.begin_ts, 10u);
  EXPECT_EQ(v.deleted_by, TXN_ID_NONE);
  EXPECT_EQ(v.end_ts, TIMESTAMP_MAX);
  EXPECT_FALSE(v.is_deleted());
}

} // namespace
} // namespace entropy
