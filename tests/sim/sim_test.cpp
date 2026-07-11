/**
 * @file sim_test.cpp
 * @brief Tests for the deterministic crash-simulation harness.
 *
 * Coverage: device-level fault mechanics (torn/lost/durable) and their
 * determinism, exact WAL durable-prefix semantics, schedule replay determinism
 * (including byte-identical JSONL), a sweep of fixed-seed schedules that must
 * survive recovery with a per-schedule anti-vacuity guard (every advertised
 * fault kind fired; undo genuinely exercised where promised), and negative
 * tests proving the invariant checker actually detects violations.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "sim/fault.hpp"
#include "sim/schedule.hpp"
#include "sim/sim_disk_manager.hpp"
#include "sim/sim_log_store.hpp"
#include "storage/page.hpp"

namespace entropy::sim {
namespace {

constexpr size_t kPageBytes = DiskManager::page_size();

std::vector<char> filled_page(char value) {
  return std::vector<char>(kPageBytes, value);
}

// The device stamps a header checksum onto every non-empty page it stores, so
// the durable image of a written page is the filled bytes with that stamp. This
// mirrors what read-back / crash-survival returns.
std::vector<char> checksummed_page(char value) {
  std::vector<char> p(kPageBytes, value);
  stamp_page_checksum(p.data());
  return p;
}

// ── Device-level: SimDiskManager ────────────────────────────────────────────

TEST(SimDiskManagerTest, LostUnfsyncedWriteRevertsToDurable) {
  FaultConfig cfg;
  cfg.page_lost_ppk = 1000;  // any unfsynced write is lost
  FaultLog log;
  SimDiskManager dm(/*seed=*/1, cfg, &log);

  // First write + sync: durable content is 0xAA.
  ASSERT_TRUE(dm.write_page(0, filled_page('\xAA').data()).ok());
  dm.sync();
  // Second write, NOT synced: 0xBB.
  ASSERT_TRUE(dm.write_page(0, filled_page('\xBB').data()).ok());

  auto image = dm.crash();
  ASSERT_TRUE(image.pages.count(0));
  EXPECT_EQ(image.pages[0], checksummed_page('\xAA'))
      << "an unfsynced write must be lost, reverting to the durable content";
  ASSERT_EQ(log.size(), 1u);
  EXPECT_EQ(log[0].kind, FaultKind::kLostPageWrite);
}

TEST(SimDiskManagerTest, TornWriteIsPartialAndDeterministic) {
  FaultConfig cfg;
  cfg.page_lost_ppk = 0;
  cfg.page_torn_ppk = 1000;  // any unfsynced write tears
  FaultLog log_a;
  FaultLog log_b;
  SimDiskManager a(/*seed=*/42, cfg, &log_a);
  SimDiskManager b(/*seed=*/42, cfg, &log_b);

  // Never synced, so the torn base is a zeroed page.
  ASSERT_TRUE(a.write_page(0, filled_page('\xAA').data()).ok());
  ASSERT_TRUE(b.write_page(0, filled_page('\xAA').data()).ok());
  auto ia = a.crash();
  auto ib = b.crash();

  ASSERT_TRUE(ia.pages.count(0));
  const std::vector<char> &torn = ia.pages[0];
  // A torn page is neither the whole new write nor the (absent) durable base.
  EXPECT_NE(torn, filled_page('\xAA'));
  EXPECT_NE(torn, filled_page('\0'));
  const bool has_new = std::any_of(torn.begin(), torn.end(),
                                   [](char c) { return c == '\xAA'; });
  const bool has_base =
      std::any_of(torn.begin(), torn.end(), [](char c) { return c == '\0'; });
  EXPECT_TRUE(has_new && has_base) << "a torn write mixes new and base bytes";

  // Byte-identical across the same seed.
  EXPECT_EQ(ia.pages[0], ib.pages[0]);
  ASSERT_EQ(log_a.size(), 1u);
  EXPECT_EQ(log_a[0].kind, FaultKind::kTornPageWrite);
  EXPECT_EQ(log_a, log_b);
}

TEST(SimDiskManagerTest, SyncedWriteSurvivesCrashIntact) {
  FaultConfig cfg;
  cfg.page_lost_ppk = 1000;
  FaultLog log;
  SimDiskManager dm(/*seed=*/7, cfg, &log);
  ASSERT_TRUE(dm.write_page(3, filled_page('\xCD').data()).ok());
  dm.sync();  // fsynced -> durable, immune to crash damage

  auto image = dm.crash();
  ASSERT_TRUE(image.pages.count(3));
  EXPECT_EQ(image.pages[3], checksummed_page('\xCD'));
  EXPECT_TRUE(log.empty()) << "a fsynced write is not subject to any fault";
  EXPECT_EQ(dm.fsync_count(), 1u);
}

TEST(SimDiskManagerTest, TransientWriteErrorIsDeterministic) {
  FaultConfig cfg;
  cfg.transient_write_error_ppk = 1000;  // every write fails
  FaultLog log;
  SimDiskManager dm(/*seed=*/5, cfg, &log);
  Status s = dm.write_page(0, filled_page('\xAA').data());
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.is_io_error());
  ASSERT_EQ(log.size(), 1u);
  EXPECT_EQ(log[0].kind, FaultKind::kTransientWriteError);
}

// ── Device-level: SimLogStore ───────────────────────────────────────────────

std::vector<char> str_bytes(const std::string &s) {
  return std::vector<char>(s.begin(), s.end());
}

TEST(SimLogStoreTest, DurablePrefixSurvivesTailIsLost) {
  FaultConfig cfg;
  cfg.wal_tail_lost_ppk = 1000;
  FaultLog log;
  SimLogStore store(/*seed=*/1, cfg, &log);

  const auto a = str_bytes("AAAA");
  const auto b = str_bytes("BBBB");
  ASSERT_TRUE(store.append(std::span<const char>(a.data(), a.size())).ok());
  ASSERT_TRUE(store.sync().ok());  // "AAAA" is now durable
  ASSERT_TRUE(store.append(std::span<const char>(b.data(), b.size())).ok());

  auto survived = store.crash();
  EXPECT_EQ(survived, a) << "everything acked by a flush survives; the "
                            "unfsynced tail is lost";
  ASSERT_EQ(log.size(), 1u);
  EXPECT_EQ(log[0].kind, FaultKind::kLostWalTail);
}

TEST(SimLogStoreTest, TornTailKeepsAPrefixOfTheUnfsyncedBytes) {
  FaultConfig cfg;
  cfg.wal_tail_lost_ppk = 0;
  cfg.wal_tail_torn_ppk = 1000;
  FaultLog log_a;
  FaultLog log_b;
  SimLogStore a(/*seed=*/99, cfg, &log_a);
  SimLogStore b(/*seed=*/99, cfg, &log_b);

  const auto durable = str_bytes("DURABLE_");
  const auto tail = str_bytes("0123456789");
  for (SimLogStore *s : {&a, &b}) {
    ASSERT_TRUE(
        s->append(std::span<const char>(durable.data(), durable.size())).ok());
    ASSERT_TRUE(s->sync().ok());
    ASSERT_TRUE(s->append(std::span<const char>(tail.data(), tail.size())).ok());
  }
  auto sa = a.crash();
  auto sb = b.crash();

  // The durable prefix is always intact; the surviving length is in
  // [durable, durable+tail].
  ASSERT_GE(sa.size(), durable.size());
  ASSERT_LE(sa.size(), durable.size() + tail.size());
  EXPECT_TRUE(std::equal(durable.begin(), durable.end(), sa.begin()));
  EXPECT_EQ(sa, sb) << "same seed -> byte-identical torn tail";
  ASSERT_EQ(log_a.size(), 1u);
  EXPECT_EQ(log_a[0].kind, FaultKind::kTornWalTail);
  EXPECT_EQ(log_a, log_b);
}

// ── Schedule-level determinism ──────────────────────────────────────────────

TEST(ScheduleTest, SameSeedProducesIdenticalFaultSequenceAndOutcome) {
  auto sched = make_schedule("mixed");
  ASSERT_TRUE(sched.has_value());

  for (uint64_t seed : {1ull, 2ull, 17ull, 12345ull}) {
    RunResult r1 = run_schedule(seed, *sched);
    RunResult r2 = run_schedule(seed, *sched);

    EXPECT_EQ(r1.faults, r2.faults)
        << "seed " << seed << " must inject a byte-identical fault sequence";
    EXPECT_EQ(r1.faults_injected, r2.faults_injected);
    EXPECT_EQ(r1.ops, r2.ops);
    EXPECT_EQ(r1.redo_ops, r2.redo_ops);
    EXPECT_EQ(r1.undo_ops, r2.undo_ops);
    EXPECT_EQ(r1.outcome(), r2.outcome());
    EXPECT_EQ(r1.invariants_failed, r2.invariants_failed);
    // The default JSONL line (no wall-clock timing) is byte-identical across
    // replays — the report itself is reproducible, not just the outcome.
    EXPECT_EQ(r1.to_jsonl(), r2.to_jsonl());
  }
}

// ── Fixed-seed schedules that must survive recovery ─────────────────────────
//
// Beyond per-seed invariants, each sweep enforces the schedule's anti-vacuity
// contract: every fault kind the schedule advertises (must_fire) fired at
// least once across the sweep, undo-exercising schedules really drove
// recovery's undo phase, and the no-fault control really injected nothing. A
// schedule whose advertised fault cannot fire fails its own test.

class SchedulePassSweep
    : public ::testing::TestWithParam<std::string> {};

TEST_P(SchedulePassSweep, RecoversToACorrectStateAcrossSeeds) {
  auto sched = make_schedule(GetParam());
  ASSERT_TRUE(sched.has_value());

  std::set<FaultKind> fired;
  for (uint64_t seed = 1; seed <= 40; ++seed) {
    RunResult r = run_schedule(seed, *sched);
    EXPECT_TRUE(r.passed())
        << "schedule=" << GetParam() << " seed=" << seed
        << " outcome=" << r.outcome() << " failed=["
        << [&] {
             std::string s;
             for (const auto &f : r.invariants_failed) s += f + " ";
             return s;
           }()
        << "]";

    for (const FaultEvent &event : r.faults) {
      fired.insert(event.kind);
    }
    if (sched->expect_undo) {
      EXPECT_GT(r.undo_ops, 0u)
          << "schedule=" << GetParam() << " seed=" << seed
          << ": recovery's undo phase must be genuinely exercised";
    }
    if (sched->expect_aborts) {
      EXPECT_GT(r.aborts, 0u)
          << "schedule=" << GetParam() << " seed=" << seed
          << ": at least one transaction must abort during normal operation, "
             "or the abort-recovery coverage is vacuous";
    }
    if (sched->expect_zero_faults) {
      EXPECT_EQ(r.faults_injected, 0u)
          << "schedule=" << GetParam() << " seed=" << seed
          << ": the clean-baseline control must inject nothing";
    }
  }

  for (FaultKind kind : sched->must_fire) {
    EXPECT_TRUE(fired.contains(kind))
        << "schedule=" << GetParam() << " advertises "
        << fault_kind_name(kind)
        << " but it never fired across the sweep (vacuous coverage)";
  }
}

INSTANTIATE_TEST_SUITE_P(AllSchedules, SchedulePassSweep,
                         ::testing::ValuesIn(schedule_names()));

// ── Negative test: the invariant checker must be able to fail ───────────────

TEST(DetectionTest, SkippingRecoveryIsCaughtByTheInvariantChecker) {
  auto skip = make_schedule("skip_recovery");
  auto with_recovery = make_schedule("lost_page_write_after_commit");
  ASSERT_TRUE(skip.has_value());
  ASSERT_TRUE(with_recovery.has_value());

  // Same fault profile (steal + lost committed pages): recovery MUST reconstruct
  // the committed rows from the WAL, and skipping it MUST leave them missing.
  size_t skip_failures = 0;
  for (uint64_t seed = 1; seed <= 20; ++seed) {
    RunResult recovered = run_schedule(seed, *with_recovery);
    EXPECT_TRUE(recovered.passed())
        << "recovery must reconstruct committed rows, seed=" << seed;

    RunResult skipped = run_schedule(seed, *skip);
    if (!skipped.passed()) {
      ++skip_failures;
      EXPECT_FALSE(skipped.invariants_failed.empty());
    }
  }
  EXPECT_GT(skip_failures, 0u)
      << "a checker that never fails is worthless: skipping recovery over lost "
         "committed pages must trip the invariant checker for at least one seed";
}

// Page checksums now let recovery survive a torn data-page write (issue #86):
// the reopened disk detects the tear and recovery rebuilds the page from the
// WAL. The behavior is guarded permanently by the torn_page_write entry in the
// passing schedule sweep above (40/40 recover, with the tear required to fire).
// This focused test additionally pins that recovery never errors and always
// restores the committed rows across seeds.
TEST(DetectionTest, TornPageWritesAreDetectedAndRecovered) {
  auto sched = make_schedule("torn_page_write");
  ASSERT_TRUE(sched.has_value());

  bool torn_fired = false;
  for (uint64_t seed = 1; seed <= 40; ++seed) {
    RunResult r = run_schedule(seed, *sched);
    EXPECT_GT(r.faults_injected, 0u) << "seed " << seed;
    EXPECT_TRUE(r.recovery_ok)
        << "seed " << seed << ": recovery must not error on a torn page";
    EXPECT_TRUE(r.passed())
        << "seed " << seed
        << ": recovery must restore the committed rows despite the tear";
    for (const FaultEvent &event : r.faults) {
      if (event.kind == FaultKind::kTornPageWrite) {
        torn_fired = true;
      }
    }
  }
  EXPECT_TRUE(torn_fired)
      << "the torn-page fault must actually fire, or the coverage is vacuous";
}

}  // namespace
}  // namespace entropy::sim
