/**
 * @file schedule.cpp
 * @brief Schedule presets and the crash/recover/verify engine.
 *
 * ── JSONL output schema (STABLE — a dashboard consumes this; do not reorder or
 *    rename fields without versioning) ─────────────────────────────────────────
 *   {
 *     "seed":              <uint64>   seed that produced this run
 *     "schedule":          <string>   preset schedule name
 *     "faults_injected":   <uint>     number of fault events (disk + WAL)
 *     "crash_point":       <string>   where the crash was taken
 *     "outcome":           <string>   "pass" | "fail" | "error"
 *     "invariants_failed": [<string>] names of violated invariants (empty on pass)
 *     "ops":               <uint>     logical data operations the workload ran
 *     "redo_ops":          <uint>     recovery redo operations actually applied
 *     "undo_ops":          <uint>     recovery undo operations actually applied
 *     "recovery_ms":       <double>   OPTIONAL, only with --timing: wall time in
 *                                     RecoveryManager::recover(). Excluded by
 *                                     default so the line is byte-identical
 *                                     across replays of the same seed.
 *   }
 * One line per run. "outcome" is "error" when recover() returned a non-ok
 * Status, "fail" when an invariant was violated, "pass" otherwise. Every field
 * except the opt-in recovery_ms is deterministic for a given (seed, schedule).
 */

#include "sim/schedule.hpp"

#include <chrono>
#include <memory>
#include <random>
#include <sstream>

#include "sim/sim_disk_manager.hpp"
#include "sim/sim_log_store.hpp"
#include "sim/workload.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/table_heap.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/mvcc.hpp"
#include "transaction/recovery.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction/version_store.hpp"
#include "transaction/wal.hpp"

namespace entropy::sim {

namespace {

constexpr oid_t kTableOid = 1;
// Large enough that the small foundational workloads never evict, so the only
// disk writes are the deterministic pre-crash flush (steal) and no eviction
// noise perturbs the fault streams.
constexpr size_t kPoolSize = 128;

}  // namespace

std::string RunResult::to_jsonl(bool include_timing) const {
  std::ostringstream os;
  os << '{' << "\"seed\":" << seed << ",\"schedule\":\"" << schedule
     << "\",\"faults_injected\":" << faults_injected << ",\"crash_point\":\""
     << crash_point << "\",\"outcome\":\"" << outcome()
     << "\",\"invariants_failed\":[";
  for (size_t i = 0; i < invariants_failed.size(); ++i) {
    if (i != 0) {
      os << ',';
    }
    os << '"' << invariants_failed[i] << '"';
  }
  os << "],\"ops\":" << ops << ",\"redo_ops\":" << redo_ops
     << ",\"undo_ops\":" << undo_ops;
  if (include_timing) {
    os << ",\"recovery_ms\":" << recovery_ms;
  }
  os << '}';
  return os.str();
}

RunResult run_schedule(uint64_t seed, const Schedule &schedule) {
  RunResult result;
  result.seed = seed;
  result.schedule = schedule.name;
  result.crash_point = schedule.crash_point;

  FaultLog disk_faults;
  FaultLog log_faults;

  // Independent PRNG streams so workload and fault decisions never interfere.
  const uint64_t disk_seed = derive_seed(seed, kDiskSalt);
  const uint64_t log_seed = derive_seed(seed, kLogSalt);
  const uint64_t wl_seed = derive_seed(seed, kWorkloadSalt);

  auto sim_disk =
      std::make_shared<SimDiskManager>(disk_seed, schedule.faults, &disk_faults);
  auto sim_log =
      std::make_shared<SimLogStore>(log_seed, schedule.faults, &log_faults);

  Schema schema = make_sim_schema();
  Oracle oracle;

  SimDiskManager::PageImage disk_image;
  std::vector<char> log_bytes;

  // ── Live run: assemble the stack (MVCCTxnIntegrationTest recipe), drive the
  //    workload, then freeze the devices and apply crash damage. ───────────────
  {
    auto mvcc = std::make_shared<MVCCManager>();
    auto version_store = std::make_shared<VersionStore>(*mvcc);
    auto lock_mgr = std::make_unique<LockManager>(false, 100);
    // wal declared before pool so it outlives the pool destructor's flush.
    auto wal = std::make_shared<WALManager>(
        std::static_pointer_cast<LogStore>(sim_log));
    auto pool = std::make_shared<BufferPoolManager>(kPoolSize, sim_disk);
    auto heap = std::make_shared<TableHeap>(pool);
    auto tm = std::make_unique<TransactionManager>(wal);

    tm->set_lock_manager(lock_mgr.get());
    tm->set_mvcc(mvcc);
    tm->set_version_store(version_store);
    tm->set_buffer_pool(pool.get());
    tm->set_table_resolver([heap_ptr = heap.get()](oid_t oid) -> TableHeap * {
      return oid == kTableOid ? heap_ptr : nullptr;
    });
    // WAL-before-page (steal safety): the log must be durable up to a page's
    // LSN before that page reaches disk.
    pool->set_wal_flush_hook([wal_ptr = wal.get()](lsn_t lsn) {
      return wal_ptr->flush_to_lsn(lsn);
    });

    std::mt19937_64 wl_rng(wl_seed);
    WorkloadContext ctx{tm.get(), heap.get(), version_store.get(), &schema,
                        kTableOid, {}};
    if (schedule.arm_wal_sync_failures) {
      // From the in-flight transaction on, WAL syncs fail: overflow flushes
      // append record bytes that never become durable — the real unsynced
      // tail the crash resolves. The engine's retry path is written for
      // exactly this device state.
      ctx.on_inflight_begin = [log_ptr = sim_log.get()] {
        log_ptr->arm_sync_failures();
      };
    }
    RandomWorkload workload(schedule.abort_ppk, schedule.inflight_ops);
    result.ops = workload.run(ctx, wl_rng, oracle, schedule.num_txns,
                              schedule.leave_in_flight);

    // Steal: push dirty (committed and/or loser) pages to disk, unsynced.
    if (schedule.flush_pages_before_crash) {
      pool->flush_all_pages();
    }
    // Optional durable data baseline (never combined with an in-flight loser).
    if (schedule.sync_pages_after_workload) {
      sim_disk->sync();
    }

    // Freeze the devices and capture the damaged durable images. The disk and
    // log draw from independent PRNG streams, so this order is free; disk is
    // crashed first to match the disk-then-log order of the recorded fault log.
    // After this the stack destructors below run against crashed devices
    // (no-ops).
    disk_image = sim_disk->crash();
    log_bytes = sim_log->crash();
  }

  result.faults.reserve(disk_faults.size() + log_faults.size());
  result.faults.insert(result.faults.end(), disk_faults.begin(),
                       disk_faults.end());
  result.faults.insert(result.faults.end(), log_faults.begin(),
                       log_faults.end());
  result.faults_injected = result.faults.size();

  // ── Recovery run: reopen the crash images on fresh, fault-free devices. ─────
  std::shared_ptr<DiskManager> rdisk =
      SimDiskManager::reopen(std::move(disk_image));
  std::shared_ptr<LogStore> rlog = SimLogStore::reopen(std::move(log_bytes));

  auto rpool = std::make_shared<BufferPoolManager>(kPoolSize, rdisk);
  auto rwal = std::make_shared<WALManager>(rlog);
  auto rheap = std::make_shared<TableHeap>(rpool);

  if (!schedule.skip_recovery) {
    RecoveryManager recovery(rpool, rwal, rdisk);
    const auto start = std::chrono::steady_clock::now();
    Status status = recovery.recover();
    const auto end = std::chrono::steady_clock::now();
    result.recovery_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    result.redo_ops = recovery.redo_count();
    result.undo_ops = recovery.undo_count();
    result.recovery_ok = status.ok();
    if (!status.ok()) {
      result.invariants_failed.emplace_back("recovery_error");
    }
  }

  // ── Verify invariants against the oracle. ───────────────────────────────────
  WorkloadContext recovered{nullptr, rheap.get(), nullptr, &schema, kTableOid,
                            {}};
  check_invariants(recovered, oracle, result.invariants_failed);

  return result;
}

// ── Preset schedules ─────────────────────────────────────────────────────────

std::optional<Schedule> make_schedule(const std::string &name) {
  // Each preset sets only the knobs it deviates on; unset fields keep their
  // Schedule/FaultConfig defaults (num_txns=12, leave_in_flight=true, no flush,
  // page_lost=wal_tail_lost=1000, everything else 0/false).
  Schedule s;
  s.name = name;

  // Sizing note: a tiny insert's WAL record is ~79 bytes (32B header + 16B
  // oid/rid/size + ~31B tuple), so ~830 inserts overflow the 64KB WAL buffer.
  // kInflightOps forces one mid-transaction overflow flush with headroom.
  constexpr size_t kInflightOps = 1200;

  if (name == "torn_wal_tail") {
    // The in-flight loser writes enough to overflow the WAL buffer, and WAL
    // syncs fail from the moment it begins, so its overflow flush appends a
    // real ~64KB unsynced tail to the device. The crash then loses or tears
    // that tail; a torn cut lands mid-record, so read_log's partial-record
    // truncation is exercised end-to-end. The committed prefix (synced before
    // the loser began) always survives intact.
    s.crash_point = "in_flight_txn_wal_tail_torn";
    s.num_txns = 14;
    s.inflight_ops = kInflightOps;
    s.arm_wal_sync_failures = true;
    s.faults.wal_tail_lost_ppk = 500;
    s.faults.wal_tail_torn_ppk = 500;
    s.must_fire = {FaultKind::kLostWalTail, FaultKind::kTornWalTail};
    return s;
  }
  if (name == "lost_page_write_after_commit") {
    // Committed pages are stolen to disk (unsynced) then lost at the crash;
    // recovery must redo them from the durable WAL.
    s.crash_point = "after_commit_pages_flushed_unsynced";
    s.num_txns = 14;
    s.leave_in_flight = false;
    s.flush_pages_before_crash = true;
    s.must_fire = {FaultKind::kLostPageWrite};
    return s;
  }
  if (name == "crash_between_wal_and_page_flush") {
    // Commit forces the WAL durable; the page flush is issued but the crash
    // strikes before the pages fsync, so every page write is lost. Recovery
    // redoes all committed changes onto fresh pages, plus a small in-flight
    // loser whose records die in the WAL manager's buffer.
    s.crash_point = "between_wal_flush_and_page_flush";
    s.num_txns = 14;
    s.flush_pages_before_crash = true;
    s.must_fire = {FaultKind::kLostPageWrite};
    return s;
  }
  if (name == "undo_durable_loser") {
    // Exercises recovery's undo phase for real: the in-flight loser overflows
    // the WAL buffer and its overflow flush SUCCEEDS (no armed sync failures),
    // making ~830 of its records durable in the WAL with no COMMIT. Recovery's
    // redo repeats that history and the undo phase must then remove every one
    // of the loser's rows; the sweep asserts undo_ops > 0 on every seed.
    s.crash_point = "durable_loser_records_then_crash";
    s.num_txns = 14;
    s.inflight_ops = kInflightOps;
    s.flush_pages_before_crash = true;
    s.must_fire = {FaultKind::kLostPageWrite};
    s.expect_undo = true;
    return s;
  }
  if (name == "durable_survives_intact") {
    // Everything committed and fsynced before the crash: recovery over an
    // intact disk is a validating no-op. This is the no-fault control tier;
    // the sweep asserts zero fault events per seed.
    s.crash_point = "clean_fsynced_baseline";
    s.leave_in_flight = false;
    s.flush_pages_before_crash = true;
    s.sync_pages_after_workload = true;
    s.expect_zero_faults = true;
    return s;
  }
  if (name == "transient_write_errors") {
    // Page writes fail intermittently while flushing before the crash; a failed
    // write just leaves the page unflushed, recovered from the WAL.
    s.crash_point = "transient_errors_then_lost";
    s.num_txns = 14;
    s.flush_pages_before_crash = true;
    s.faults.transient_write_error_ppk = 300;
    s.must_fire = {FaultKind::kTransientWriteError, FaultKind::kLostPageWrite};
    return s;
  }
  if (name == "mixed") {
    // Broad default: steal + lost pages + a WAL-buffer-overflowing in-flight
    // loser whose appended-but-unsynced tail is lost or torn. Used for the
    // determinism check and wide seed sweeps.
    s.crash_point = "mixed_steal_lost_inflight";
    s.num_txns = 16;
    s.inflight_ops = kInflightOps;
    s.arm_wal_sync_failures = true;
    s.flush_pages_before_crash = true;
    s.faults.wal_tail_lost_ppk = 700;
    s.faults.wal_tail_torn_ppk = 300;
    s.must_fire = {FaultKind::kLostPageWrite, FaultKind::kLostWalTail,
                   FaultKind::kTornWalTail};
    return s;
  }
  if (name == "live_abort_repro") {
    // EXCLUDED from the passing set (schedule_names()) on purpose. Transactions
    // abort during normal operation; recovery then resurrects their rows,
    // because it writes no CLRs and never gates redo by page LSN (the write
    // path never stamps one, #75), so repeat-history re-applies an aborted
    // INSERT and the undo phase, which only rolls back in-flight losers, never
    // removes it. Reproduce: `entropy-sim --schedule live_abort_repro --seeds
    // 40` — seeds 1-40 all fail; at larger scale ~97% of seeds fail (a seed
    // whose random mix happens to abort no transaction passes). Tracked in
    // issues #75/#81; do not add to the passing sweep until fixed.
    s.crash_point = "live_abort_then_recover";
    s.num_txns = 14;
    s.leave_in_flight = false;
    s.flush_pages_before_crash = true;
    s.abort_ppk = 400;
    return s;
  }
  if (name == "torn_page_write") {
    // EXCLUDED from the passing set (schedule_names()) on purpose. Committed
    // pages are stolen to disk (unsynced) and every one is TORN at the crash: a
    // partial mix of new and pre-crash bytes. The engine has no page checksums,
    // so recovery cannot detect the tear; when the surviving fragment includes
    // the header + slot directory, redo cannot repair it either (the slot
    // claims the record is live, so insert-at refuses), and recover() reports
    // success while committed bytes are gone. Fails committed_present on
    // 220/500 measured seeds (the failing side of the tear is the one that
    // keeps the header). Tracked in issue #86; do not add to the passing sweep
    // until pages carry checksums. Reproduce:
    //   entropy-sim --schedule torn_page_write --seeds 200
    s.crash_point = "committed_pages_torn_unsynced";
    s.num_txns = 14;
    s.leave_in_flight = false;
    s.flush_pages_before_crash = true;
    s.faults.page_lost_ppk = 0;
    s.faults.page_torn_ppk = 1000;
    s.must_fire = {FaultKind::kTornPageWrite};
    return s;
  }
  if (name == "skip_recovery") {
    // Negative control: identical to lost_page_write_after_commit but recovery
    // is skipped, so committed rows that live only in the WAL are missing. The
    // invariant checker MUST report a failure.
    s.crash_point = "no_recovery_control";
    s.num_txns = 14;
    s.leave_in_flight = false;
    s.flush_pages_before_crash = true;
    s.skip_recovery = true;
    return s;
  }
  return std::nullopt;
}

std::vector<std::string> schedule_names() {
  return {
      "torn_wal_tail",
      "lost_page_write_after_commit",
      "crash_between_wal_and_page_flush",
      "undo_durable_loser",
      "durable_survives_intact",
      "transient_write_errors",
      "mixed",
  };
}

}  // namespace entropy::sim
