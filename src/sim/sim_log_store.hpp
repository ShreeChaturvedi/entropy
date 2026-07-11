#pragma once

/**
 * @file sim_log_store.hpp
 * @brief Fault-injecting, in-memory LogStore for crash simulation.
 *
 * SimLogStore implements the append-only durable byte-stream the WAL is written
 * through. Durability is exact: bytes are durable up to the last sync()
 * (fsync); everything appended after the last sync is "in flight" and, at a
 * simulated crash, is resolved by a seeded PRNG into {whole tail lost, torn
 * partial tail kept, tail fully durable}. WALManager::read_log then parses the
 * surviving bytes and naturally truncates any partially-surviving final record.
 */

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <span>
#include <vector>

#include "sim/fault.hpp"
#include "transaction/wal.hpp"

namespace entropy::sim {

class SimLogStore : public LogStore {
public:
  /// Live, fault-injecting store. @p log is not owned and may be null.
  SimLogStore(uint64_t seed, FaultConfig config, FaultLog *log);

  /// Reopen a durable byte image with fault injection disabled (recovery side).
  [[nodiscard]] static std::unique_ptr<SimLogStore> reopen(
      std::vector<char> durable_bytes);

  [[nodiscard]] Status append(std::span<const char> data) override;
  [[nodiscard]] Status sync() override;
  [[nodiscard]] std::vector<char> read_all() override;
  [[nodiscard]] uint64_t size() const override;
  void set_sync_hook_for_testing(std::function<Status()> hook) override;

  // ── Harness control ──────────────────────────────────────────────────────

  /// Freeze the store and return the surviving post-crash byte image. After
  /// this, append()/sync() are no-ops that draw no randomness.
  [[nodiscard]] std::vector<char> crash();

  /// From now on every sync() fails with an IOError while appends still land.
  /// This is the device state WALManager's retry path is written for: a failed
  /// sync leaves record bytes handed to the store but not durable, so the
  /// bytes appended after the last successful sync form a REAL unsynced tail
  /// at the crash. Schedules arm this at the start of their in-flight
  /// transaction so the crash lands between append and fsync. Deterministic:
  /// arming draws no randomness.
  void arm_sync_failures();

  /// Number of durable-sync boundaries crossed.
  [[nodiscard]] uint64_t fsync_count() const;

private:
  SimLogStore() = default;  // used by reopen()

  mutable std::mutex mutex_;
  std::vector<char> bytes_;    // everything appended so far
  uint64_t durable_size_ = 0;  // bytes durable as of the last sync()

  std::mt19937_64 rng_;
  FaultConfig config_;
  FaultLog *log_ = nullptr;
  bool crashed_ = false;
  bool syncs_fail_ = false;
  uint64_t fsync_count_ = 0;
};

}  // namespace entropy::sim
