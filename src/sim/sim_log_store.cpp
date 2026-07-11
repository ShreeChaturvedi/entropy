/**
 * @file sim_log_store.cpp
 * @brief Implementation of the fault-injecting in-memory log store.
 */

#include "sim/sim_log_store.hpp"

namespace entropy::sim {

SimLogStore::SimLogStore(uint64_t seed, FaultConfig config, FaultLog *log)
    : rng_(seed), config_(config), log_(log) {}

std::unique_ptr<SimLogStore> SimLogStore::reopen(
    std::vector<char> durable_bytes) {
  std::unique_ptr<SimLogStore> store(new SimLogStore());
  store->durable_size_ = durable_bytes.size();
  store->bytes_ = std::move(durable_bytes);
  return store;
}

Status SimLogStore::append(std::span<const char> data) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (crashed_) {
    return Status::Ok();  // swallow doomed-process writes, draw no randomness
  }
  bytes_.insert(bytes_.end(), data.begin(), data.end());
  return Status::Ok();
}

Status SimLogStore::sync() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (crashed_) {
    return Status::Ok();
  }
  if (syncs_fail_) {
    // Appended bytes stay in the store but do NOT become durable: they are
    // the unsynced tail the crash will resolve.
    return Status::IOError("injected sync failure (armed)");
  }
  durable_size_ = bytes_.size();
  ++fsync_count_;
  return Status::Ok();
}

void SimLogStore::arm_sync_failures() {
  std::lock_guard<std::mutex> lock(mutex_);
  syncs_fail_ = true;
}

std::vector<char> SimLogStore::read_all() {
  std::lock_guard<std::mutex> lock(mutex_);
  return bytes_;
}

uint64_t SimLogStore::size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return bytes_.size();
}

void SimLogStore::set_sync_hook_for_testing(std::function<Status()> /*hook*/) {
  // The simulator models durability directly, so the WAL test sync hook is
  // intentionally ignored; sync() is the fsync boundary. The hook's contract
  // is "run INSTEAD of the real durable sync", which would bypass the sim's
  // exact durable-prefix accounting; sync-failure injection is provided as
  // first-class harness control instead (arm_sync_failures, next to crash()).
}

std::vector<char> SimLogStore::crash() {
  std::lock_guard<std::mutex> lock(mutex_);
  crashed_ = true;

  // Everything fsynced survives; the in-flight tail is fair game. bytes_ is
  // dead after this call (device frozen then destructed), so truncate-in-place
  // and move it out rather than copying the surviving prefix.
  const uint64_t tail_len = bytes_.size() - durable_size_;
  if (tail_len == 0) {
    return std::move(bytes_);  // nothing in flight
  }

  switch (draw_fate(rng_, config_.wal_tail_lost_ppk, config_.wal_tail_torn_ppk)) {
  case Fate::kLost:
    if (log_ != nullptr) {
      log_->push_back({FaultKind::kLostWalTail, -1, tail_len});
    }
    bytes_.resize(durable_size_);  // whole tail lost
    break;
  case Fate::kTorn: {
    const uint64_t keep = rng_() % (tail_len + 1);
    if (log_ != nullptr) {
      log_->push_back({FaultKind::kTornWalTail, -1, keep});
    }
    bytes_.resize(durable_size_ + keep);
    break;
  }
  case Fate::kDurable:
    if (log_ != nullptr) {
      log_->push_back({FaultKind::kDurableWalKept, -1, tail_len});
    }
    break;  // tail happened to reach disk: keep bytes_ whole
  }
  return std::move(bytes_);
}

uint64_t SimLogStore::fsync_count() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return fsync_count_;
}

}  // namespace entropy::sim
