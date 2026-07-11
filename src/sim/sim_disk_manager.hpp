#pragma once

/**
 * @file sim_disk_manager.hpp
 * @brief Fault-injecting, in-memory DiskManager for crash simulation.
 *
 * SimDiskManager implements the page-level DiskManager seam entirely in memory
 * and models a durability boundary: writes are visible immediately (like a page
 * cache) but only become crash-durable at a sync() (fsync). At a simulated
 * crash, every write issued since the last sync() is resolved by a seeded PRNG
 * into one of {lost, torn, durable}, and any write_page may fail transiently
 * mid-run. Reads always serve the current in-memory image, matching a real
 * page cache; a page id past the high-water mark reads back zeroed, exactly as
 * FileDiskManager returns a fresh page beyond EOF.
 *
 * The simulator drives two instances per run: a "live" one (fault injection on)
 * for the workload, then a fresh "reopen" one (fault injection off) built from
 * the post-crash image for recovery.
 */

#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <unordered_map>
#include <vector>

#include "sim/fault.hpp"
#include "storage/disk_manager.hpp"

namespace entropy::sim {

class SimDiskManager : public DiskManager {
public:
  /// A crash-time snapshot of the durable page image, handed to reopen().
  struct PageImage {
    std::unordered_map<page_id_t, std::vector<char>> pages;
    page_id_t num_pages = 0;
    std::vector<page_id_t> free_list;
  };

  /// Live, fault-injecting manager. @p log receives one entry per fault; it is
  /// not owned and must outlive this manager (may be null to discard events).
  SimDiskManager(uint64_t seed, FaultConfig config, FaultLog *log);

  /// Reopen a durable image with fault injection disabled (recovery side).
  [[nodiscard]] static std::unique_ptr<SimDiskManager> reopen(PageImage image);

  [[nodiscard]] Status read_page(page_id_t page_id, char *page_data) override;
  [[nodiscard]] Status write_page(page_id_t page_id,
                                  const char *page_data) override;
  [[nodiscard]] page_id_t allocate_page() override;
  void deallocate_page(page_id_t page_id) override;
  [[nodiscard]] page_id_t num_pages() const noexcept override;
  [[nodiscard]] bool is_open() const noexcept override { return true; }
  void sync() override;

  // ── Harness control ──────────────────────────────────────────────────────

  /// Freeze the device and compute the post-crash durable image. After this,
  /// all mutating calls are no-ops that draw no randomness, so a doomed
  /// process's destructor flushes cannot alter the captured image.
  [[nodiscard]] PageImage crash();

  /// Number of fsync boundaries crossed (durable-sync calls).
  [[nodiscard]] uint64_t fsync_count() const;

private:
  SimDiskManager() = default;  // used by reopen()

  mutable std::mutex mutex_;
  std::unordered_map<page_id_t, std::vector<char>> current_;  // live page cache
  std::unordered_map<page_id_t, std::vector<char>> durable_;  // as of last sync
  // Page ids written since the last sync. A sorted set both dedups and yields
  // the ascending crash-resolution order the PRNG draws depend on, for free.
  std::set<page_id_t> dirty_since_sync_;
  page_id_t num_pages_ = 0;
  std::vector<page_id_t> free_list_;

  std::mt19937_64 rng_;
  FaultConfig config_;
  FaultLog *log_ = nullptr;
  bool inject_ = false;
  bool crashed_ = false;
  uint64_t fsync_count_ = 0;
};

}  // namespace entropy::sim
