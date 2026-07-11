#pragma once

/**
 * @file fault.hpp
 * @brief Fault model shared by the simulated disk and log stores.
 *
 * Every fault decision in the crash simulator is drawn from a seeded
 * std::mt19937_64. Nothing here reads a wall clock or an unseeded source, so a
 * schedule replayed with the same seed injects a byte-identical fault sequence.
 *
 * Probabilities are expressed as integer parts-per-thousand (ppk) and compared
 * against `rng() % 1000`, so the thresholds are exact and portable rather than
 * floating-point.
 */

#include <cstdint>
#include <random>
#include <vector>

namespace entropy::sim {

/// Kinds of fault the simulator can inject, recorded in the event log.
enum class FaultKind : uint8_t {
  kTransientWriteError,  ///< write_page returned an IOError (write dropped)
  kLostPageWrite,        ///< an unfsynced page write vanished at crash
  kTornPageWrite,        ///< an unfsynced page write applied only partially
  kDurablePageKept,      ///< an unfsynced page write happened to survive intact
  kLostWalTail,          ///< the whole unfsynced WAL tail vanished at crash
  kTornWalTail,          ///< only a prefix of the unfsynced WAL tail survived
  kDurableWalKept,       ///< the unfsynced WAL tail happened to survive intact
};

[[nodiscard]] inline const char *fault_kind_name(FaultKind kind) {
  switch (kind) {
  case FaultKind::kTransientWriteError:
    return "transient_write_error";
  case FaultKind::kLostPageWrite:
    return "lost_page_write";
  case FaultKind::kTornPageWrite:
    return "torn_page_write";
  case FaultKind::kDurablePageKept:
    return "durable_page_kept";
  case FaultKind::kLostWalTail:
    return "lost_wal_tail";
  case FaultKind::kTornWalTail:
    return "torn_wal_tail";
  case FaultKind::kDurableWalKept:
    return "durable_wal_kept";
  }
  return "unknown";
}

/// One recorded fault. Two runs of the same seed produce equal event vectors.
struct FaultEvent {
  FaultKind kind;
  int64_t target;   ///< page id, or -1 for the WAL
  uint64_t detail;  ///< torn boundary / bytes kept (0 when not applicable)

  bool operator==(const FaultEvent &) const = default;
};

/// Ordered log of every fault injected in a single run.
using FaultLog = std::vector<FaultEvent>;

/**
 * @brief Per-schedule fault probabilities (parts per thousand).
 *
 * The page and WAL crash outcomes are mutually exclusive draws: the "lost" and
 * "torn" shares are taken first and any remainder means the unfsynced write
 * happened to reach durable storage intact.
 */
struct FaultConfig {
  /// Chance that any single write_page call fails with an IOError.
  uint32_t transient_write_error_ppk = 0;

  /// Fate of a page written but not fsynced before the crash.
  uint32_t page_lost_ppk = 1000;
  uint32_t page_torn_ppk = 0;

  /// Fate of the WAL bytes appended but not fsynced before the crash.
  uint32_t wal_tail_lost_ppk = 1000;
  uint32_t wal_tail_torn_ppk = 0;
};

/// The three mutually-exclusive fates of an unfsynced write at a crash.
enum class Fate : uint8_t { kDurable, kLost, kTorn };

/// A single ppk Bernoulli trial. Draws from @p rng only when @p ppk > 0, so a
/// disabled fault never perturbs the stream that would otherwise be consumed.
[[nodiscard]] inline bool bernoulli_ppk(std::mt19937_64 &rng,
                                        uint32_t ppk) noexcept {
  return ppk > 0 && (rng() % 1000) < ppk;
}

/// Draw an unfsynced write's fate from one roll: the lost share first, then the
/// torn share, and any remainder means it happened to reach durable storage.
/// Always consumes exactly one draw so the stream is layout-independent.
[[nodiscard]] inline Fate draw_fate(std::mt19937_64 &rng, uint32_t lost_ppk,
                                    uint32_t torn_ppk) noexcept {
  const uint32_t roll = static_cast<uint32_t>(rng() % 1000);
  if (roll < lost_ppk) {
    return Fate::kLost;
  }
  if (roll < lost_ppk + torn_ppk) {
    return Fate::kTorn;
  }
  return Fate::kDurable;
}

/// SplitMix64 mixing so independent PRNG streams can be derived from one seed
/// without their draw orders interfering. Deterministic and stateless.
[[nodiscard]] inline uint64_t derive_seed(uint64_t seed, uint64_t salt) noexcept {
  uint64_t z = seed + salt * 0x9E3779B97F4A7C15ull;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
  return z ^ (z >> 31);
}

/// Salts for the three independent streams a run draws from.
inline constexpr uint64_t kWorkloadSalt = 0x1;
inline constexpr uint64_t kDiskSalt = 0x2;
inline constexpr uint64_t kLogSalt = 0x3;

}  // namespace entropy::sim
