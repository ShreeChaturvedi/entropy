/**
 * @file version_store_test.cpp
 * @brief Outcome-based tests for MVCC version chains (VersionStore) and the
 *        MVCCManager creator/deleter distinction (#25, #27).
 */

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <span>
#include <string>

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

// Rollback invalidates an uncommitted insert: the node is kept as a tombstone
// (so a reader racing the abort teardown still finds proof the bytes were
// never committed) but is invisible to every snapshot, and GC prunes it.
TEST_F(VersionStoreTest, RollbackRemovesUncommittedInsert) {
  Transaction txn(1);
  ASSERT_TRUE(store_.on_insert(&txn, rid_).ok());
  ASSERT_EQ(store_.chain_length(rid_), 1u);

  store_.rollback(&txn);

  const std::string heap_bytes = "ZOMBIE";
  Transaction reader(2);
  EXPECT_FALSE(store_.read_visible(rid_, span_of(heap_bytes), &reader)
                   .has_value())
      << "rolled-back insert still visible through its chain";

  store_.gc(100);
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

  // The committed version is what every reader sees again; the invalidated
  // update survives only as a tombstone until GC.
  Transaction reader(3);
  auto v = store_.read_visible(rid_, span_of(heap_v1), &reader);
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(bytes_of(*v), "V1");

  store_.gc(100);
  EXPECT_EQ(store_.chain_length(rid_), 1u) << "tombstone not pruned";
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
