/**
 * @file lock_manager_test.cpp
 * @brief Outcome-based concurrency tests for LockManager deadlock handling.
 *
 * These tests assert on observable behaviour (which transaction aborts, whether
 * survivors acquire, bounded wall-clock time) rather than on internal operation
 * counts. Every deadlock test uses a LONG lock timeout so that a resolution that
 * completes quickly proves the deadlock was broken by detection + victim release
 * (wait-die), not by the fallback wait timeout.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"

namespace entropy {
namespace {

using namespace std::chrono_literals;

// A timeout far larger than any legitimate resolution. If a test ever leans on
// the timeout instead of deadlock resolution it will blow past the wall-clock
// bound below and fail.
constexpr uint32_t kLongTimeoutMs = 5000;
constexpr auto kResolveBound = 1000ms;

// ─────────────────────────────────────────────────────────────────────────────
// Two-transaction deadlock: younger aborts, older survivor acquires fast.
// ─────────────────────────────────────────────────────────────────────────────

TEST(LockManagerDeadlockTest, TwoTxnDeadlockYoungerAbortsSurvivorAcquiresFast) {
  auto lock_mgr = std::make_unique<LockManager>(true, kLongTimeoutMs);

  Transaction older(1);    // smaller id => older => wins under wait-die
  Transaction younger(2);  // larger id  => younger => dies

  constexpr oid_t kResA = 1;
  constexpr oid_t kResB = 2;

  // Each transaction grabs one resource, then reaches for the other's.
  ASSERT_TRUE(lock_mgr->lock_table(&older, kResA, LockMode::EXCLUSIVE).ok());
  ASSERT_TRUE(lock_mgr->lock_table(&younger, kResB, LockMode::EXCLUSIVE).ok());

  Status older_status = Status::Ok();
  Status younger_status = Status::Ok();

  const auto start = std::chrono::steady_clock::now();

  std::thread t_old([&] {
    older_status = lock_mgr->lock_table(&older, kResB, LockMode::EXCLUSIVE);
  });
  std::thread t_young([&] {
    younger_status = lock_mgr->lock_table(&younger, kResA, LockMode::EXCLUSIVE);
  });
  t_old.join();
  t_young.join();

  const auto elapsed = std::chrono::steady_clock::now() - start;

  // Wait-die: the younger transaction is the victim, the older one wins.
  EXPECT_EQ(younger_status.code(), StatusCode::kAborted);
  EXPECT_TRUE(older_status.ok())
      << "older survivor should acquire, got: " << older_status.message();
  EXPECT_GE(lock_mgr->deadlock_count(), 1u);

  // Survivor acquired promptly rather than stalling to the 5s timeout.
  EXPECT_LT(elapsed, kResolveBound)
      << "resolution took "
      << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
      << "ms";

  lock_mgr->release_all_locks(&older);
  lock_mgr->release_all_locks(&younger);
}

// ─────────────────────────────────────────────────────────────────────────────
// N-transaction cycle (N >= 3) resolves quickly with no timeout stall.
// ─────────────────────────────────────────────────────────────────────────────

TEST(LockManagerDeadlockTest, NCycleDeadlockResolvesFast) {
  constexpr size_t kN = 5;
  auto lock_mgr = std::make_unique<LockManager>(true, kLongTimeoutMs);

  std::vector<std::unique_ptr<Transaction>> txns;
  for (size_t i = 0; i < kN; ++i) {
    txns.push_back(std::make_unique<Transaction>(i + 1));
  }

  // Resource i+1 is held by txn i. Txn i then reaches for resource of txn i+1,
  // closing a cycle T0 -> T1 -> ... -> T(N-1) -> T0.
  auto res_of = [](size_t i) { return static_cast<oid_t>(i + 1); };
  for (size_t i = 0; i < kN; ++i) {
    ASSERT_TRUE(
        lock_mgr->lock_table(txns[i].get(), res_of(i), LockMode::EXCLUSIVE).ok());
  }

  std::vector<StatusCode> results(kN, StatusCode::kOk);
  std::vector<std::thread> threads;

  const auto start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < kN; ++i) {
    threads.emplace_back([&, i] {
      const oid_t next = res_of((i + 1) % kN);
      results[i] =
          lock_mgr->lock_table(txns[i].get(), next, LockMode::EXCLUSIVE).code();
      // Once we hold (or fail) our second resource, drop everything so the
      // transaction blocked behind us can make progress and the chain unwinds.
      lock_mgr->release_all_locks(txns[i].get());
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  const auto elapsed = std::chrono::steady_clock::now() - start;

  int aborts = 0;
  int timeouts = 0;
  for (auto code : results) {
    if (code == StatusCode::kAborted) {
      ++aborts;
    } else if (code == StatusCode::kTimeout) {
      ++timeouts;
    }
  }

  EXPECT_EQ(timeouts, 0) << "no transaction should fall back to the timeout";
  EXPECT_GE(aborts, 1) << "cycle must be broken by aborting a victim";
  EXPECT_GE(lock_mgr->deadlock_count(), 1u);
  EXPECT_LT(elapsed, kResolveBound)
      << "N-cycle resolution took "
      << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
      << "ms";

  for (auto& txn : txns) {
    lock_mgr->release_all_locks(txn.get());
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Wait-die bounds starvation: the oldest transaction is never victimized.
// ─────────────────────────────────────────────────────────────────────────────

TEST(LockManagerDeadlockTest, WaitDieNeverVictimizesOldest) {
  auto lock_mgr = std::make_unique<LockManager>(true, kLongTimeoutMs);

  constexpr oid_t kResA = 1;
  constexpr oid_t kResB = 2;
  constexpr int kRounds = 25;

  for (int round = 0; round < kRounds; ++round) {
    // A fresh younger transaction each round; the old one keeps the same low id
    // and so is always the oldest live transaction in the conflict.
    Transaction older(1);
    Transaction younger(static_cast<txn_id_t>(1000 + round));

    ASSERT_TRUE(lock_mgr->lock_table(&older, kResA, LockMode::EXCLUSIVE).ok());
    ASSERT_TRUE(lock_mgr->lock_table(&younger, kResB, LockMode::EXCLUSIVE).ok());

    Status older_status = Status::Ok();
    Status younger_status = Status::Ok();

    std::thread t_old([&] {
      older_status = lock_mgr->lock_table(&older, kResB, LockMode::EXCLUSIVE);
    });
    std::thread t_young([&] {
      younger_status = lock_mgr->lock_table(&younger, kResA, LockMode::EXCLUSIVE);
    });
    t_old.join();
    t_young.join();

    // The old transaction wins every single round; only the younger is aborted.
    EXPECT_TRUE(older_status.ok())
        << "round " << round << ": oldest was victimized";
    EXPECT_EQ(younger_status.code(), StatusCode::kAborted)
        << "round " << round;

    lock_mgr->release_all_locks(&older);
    lock_mgr->release_all_locks(&younger);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Concurrent lock / abort stress. Primary purpose: exercise the cross-thread
// reads of Transaction::state_ and the lock-table paths so ThreadSanitizer can
// prove they are race-free. Also asserts the lock table drains cleanly.
// ─────────────────────────────────────────────────────────────────────────────

TEST(LockManagerDeadlockTest, ConcurrentLockAbortStress) {
  auto lock_mgr = std::make_unique<LockManager>(true, /*lock_timeout_ms=*/500);

  constexpr int kThreads = 8;
  constexpr int kItersPerThread = 200;
  constexpr oid_t kNumResources = 4;

  std::atomic<txn_id_t> next_id{1};
  std::vector<std::thread> threads;

  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(static_cast<uint32_t>(t) + 1);
      std::uniform_int_distribution<int> res_dist(1, kNumResources);

      for (int iter = 0; iter < kItersPerThread; ++iter) {
        Transaction txn(next_id.fetch_add(1, std::memory_order_relaxed));

        oid_t r1 = static_cast<oid_t>(res_dist(rng));
        oid_t r2 = static_cast<oid_t>(res_dist(rng));

        Status s1 = lock_mgr->lock_table(&txn, r1, LockMode::EXCLUSIVE);
        if (s1.ok() && r2 != r1) {
          Status s2 = lock_mgr->lock_table(&txn, r2, LockMode::EXCLUSIVE);
          (void)s2;
        }

        // Simulate a mix of commits and aborts. Setting ABORTED here is a
        // cross-thread-visible write to state_ that waiters on our locks may
        // read concurrently -- the atomic makes that well defined.
        if ((iter & 1) == 0) {
          txn.set_state(TransactionState::ABORTED);
        }
        lock_mgr->release_all_locks(&txn);
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  // Every transaction released; nothing should be left dangling.
  EXPECT_EQ(lock_mgr->lock_table_size(), 0u);
}

}  // namespace
}  // namespace entropy
