/**
 * @file sim_disk_manager.cpp
 * @brief Implementation of the fault-injecting in-memory disk manager.
 */

#include "sim/sim_disk_manager.hpp"

#include <algorithm>
#include <cstring>

#include "storage/page.hpp"

namespace entropy::sim {

namespace {
constexpr size_t kPageBytes = DiskManager::page_size();
}  // namespace

SimDiskManager::SimDiskManager(uint64_t seed, FaultConfig config, FaultLog *log)
    : rng_(seed), config_(config), log_(log), inject_(true) {}

std::unique_ptr<SimDiskManager> SimDiskManager::reopen(PageImage image) {
  // A fresh, fault-free device serving the post-crash durable image. Recovery
  // reads and rewrites through current_; durable_ is unused on a reopened
  // device (no crash() is taken), so the image seeds current_ directly.
  std::unique_ptr<SimDiskManager> dm(new SimDiskManager());
  dm->current_ = std::move(image.pages);
  dm->num_pages_ = image.num_pages;
  dm->free_list_ = std::move(image.free_list);
  dm->inject_ = false;
  return dm;
}

Status SimDiskManager::read_page(page_id_t page_id, char *page_data) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (page_id < 0) {
    return Status::InvalidArgument("Invalid page ID");
  }
  auto it = current_.find(page_id);
  if (it == current_.end()) {
    // Beyond the written range (or lost at crash): a fresh, zeroed page. Not
    // stored data, so there is no checksum to verify.
    std::memset(page_data, 0, kPageBytes);
    return Status::Ok();
  }
  std::memcpy(page_data, it->second.data(), kPageBytes);
  // Detect a torn/partial write: the stored image no longer matches its stamped
  // checksum. Surfaced as Corruption so recovery rebuilds the page from the WAL
  // instead of trusting the half-persisted bytes.
  if (!verify_page_checksum(page_data)) {
    return Status::Corruption("torn/corrupt page: checksum mismatch");
  }
  return Status::Ok();
}

Status SimDiskManager::write_page(page_id_t page_id, const char *page_data) {
  std::lock_guard<std::mutex> lock(mutex_);
  // A crashed device swallows writes without drawing randomness, so a doomed
  // process's destructor flush cannot perturb the captured image or PRNG.
  if (crashed_) {
    return Status::Ok();
  }
  if (page_id < 0) {
    return Status::InvalidArgument("Invalid page ID");
  }

  if (inject_ && bernoulli_ppk(rng_, config_.transient_write_error_ppk)) {
    if (log_ != nullptr) {
      log_->push_back({FaultKind::kTransientWriteError, page_id, 0});
    }
    return Status::IOError("injected transient write error");
  }

  auto &slot = current_[page_id];
  slot.assign(page_data, page_data + kPageBytes);
  // Stamp the integrity checksum onto the stored image (never the caller's
  // buffer). A crash that later tears this page will break the stamp, so the
  // reopened device's read_page detects it. All-zero pages (fresh/deallocated)
  // are intentionally left unstamped and treated as valid empty pages on read.
  if (!page_is_all_zero(slot.data())) {
    stamp_page_checksum(slot.data());
  }
  dirty_since_sync_.insert(page_id);
  if (page_id >= num_pages_) {
    num_pages_ = page_id + 1;
  }
  return Status::Ok();
}

page_id_t SimDiskManager::allocate_page() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!free_list_.empty()) {
    page_id_t page_id = free_list_.back();
    free_list_.pop_back();
    return page_id;
  }
  return num_pages_++;
}

void SimDiskManager::deallocate_page(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (crashed_) {
    return;
  }
  if (page_id < 0 || page_id >= num_pages_) {
    return;
  }
  if (std::find(free_list_.begin(), free_list_.end(), page_id) !=
      free_list_.end()) {
    return;  // reject double free
  }
  // Zero the page so a reused id reads back fresh, mirroring FileDiskManager.
  current_[page_id].assign(kPageBytes, 0);
  dirty_since_sync_.insert(page_id);
  free_list_.push_back(page_id);
}

page_id_t SimDiskManager::num_pages() const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  return num_pages_;
}

void SimDiskManager::sync() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (crashed_) {
    return;
  }
  for (page_id_t page_id : dirty_since_sync_) {
    durable_[page_id] = current_[page_id];
  }
  dirty_since_sync_.clear();
  ++fsync_count_;
}

SimDiskManager::PageImage SimDiskManager::crash() {
  std::lock_guard<std::mutex> lock(mutex_);
  crashed_ = true;

  // The durable image is the crash baseline; move it in and mutate per fault.
  PageImage image;
  image.pages = std::move(durable_);
  image.num_pages = num_pages_;
  image.free_list = std::move(free_list_);

  // dirty_since_sync_ already iterates in ascending page-id order, pinning the
  // PRNG draw sequence independent of hash-map layout.
  for (page_id_t page_id : dirty_since_sync_) {
    switch (draw_fate(rng_, config_.page_lost_ppk, config_.page_torn_ppk)) {
    case Fate::kLost:
      // Reverts to the durable baseline image.pages already holds (absent when
      // the page was never fsynced) -- nothing to change.
      if (log_ != nullptr) {
        log_->push_back({FaultKind::kLostPageWrite, page_id, 0});
      }
      break;
    case Fate::kTorn: {
      // A prefix or suffix of the new bytes lands over the durable base.
      auto base_it = image.pages.find(page_id);
      std::vector<char> torn = (base_it != image.pages.end())
                                   ? base_it->second
                                   : std::vector<char>(kPageBytes, 0);
      const std::vector<char> &fresh = current_[page_id];
      const auto boundary =
          static_cast<std::ptrdiff_t>(1 + (rng_() % (kPageBytes - 1)));
      if ((rng_() & 1u) != 0u) {
        std::copy(fresh.begin(), fresh.begin() + boundary, torn.begin());
      } else {
        std::copy(fresh.begin() + boundary, fresh.end(), torn.begin() + boundary);
      }
      image.pages[page_id] = std::move(torn);
      if (log_ != nullptr) {
        log_->push_back({FaultKind::kTornPageWrite, page_id,
                         static_cast<uint64_t>(boundary)});
      }
      break;
    }
    case Fate::kDurable:
      image.pages[page_id] = current_[page_id];
      if (log_ != nullptr) {
        log_->push_back({FaultKind::kDurablePageKept, page_id, 0});
      }
      break;
    }
  }
  return image;
}

uint64_t SimDiskManager::fsync_count() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return fsync_count_;
}

}  // namespace entropy::sim
