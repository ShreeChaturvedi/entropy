#pragma once

/**
 * @file index.hpp
 * @brief Abstract secondary-index interface
 *
 * Index is the catalog/executor-facing surface for ordered key → RID lookup.
 * BPlusTree is the sole concrete implementation today; non-unique multi-match
 * and alternate structures land in later work streams.
 */

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "common/types.hpp"
#include "entropy/status.hpp"
#include "storage/b_plus_tree_page.hpp" // BPTreeKey

namespace entropy {

/**
 * @brief Type-erased index iterator for range / full scans
 *
 * Wraps any iterator that exposes key(), value(), pre-increment, is_end(), and
 * equality — today that is BPlusTreeIterator. IndexScan uses this without
 * knowing the concrete tree type.
 */
class IndexIterator {
public:
  IndexIterator() = default;

  template <typename Iter>
  explicit IndexIterator(Iter iter)
      : impl_(std::make_unique<Model<Iter>>(std::move(iter))) {}

  IndexIterator(const IndexIterator &other)
      : impl_(other.impl_ ? other.impl_->clone() : nullptr) {}

  IndexIterator &operator=(const IndexIterator &other) {
    if (this != &other) {
      impl_ = other.impl_ ? other.impl_->clone() : nullptr;
    }
    return *this;
  }

  IndexIterator(IndexIterator &&) noexcept = default;
  IndexIterator &operator=(IndexIterator &&) noexcept = default;

  [[nodiscard]] BPTreeKey key() const {
    return impl_ ? impl_->key() : BPTreeKey{};
  }

  [[nodiscard]] RID value() const {
    return impl_ ? impl_->value() : RID{};
  }

  [[nodiscard]] std::pair<BPTreeKey, RID> operator*() const {
    return {key(), value()};
  }

  IndexIterator &operator++() {
    if (impl_) {
      impl_->advance();
    }
    return *this;
  }

  IndexIterator operator++(int) {
    IndexIterator tmp = *this;
    ++(*this);
    return tmp;
  }

  [[nodiscard]] bool is_end() const {
    return impl_ == nullptr || impl_->is_end();
  }

  bool operator==(const IndexIterator &other) const {
    if (is_end() && other.is_end()) {
      return true;
    }
    if (is_end() || other.is_end()) {
      return false;
    }
    return impl_->equals(*other.impl_);
  }

  bool operator!=(const IndexIterator &other) const {
    return !(*this == other);
  }

private:
  struct Concept {
    virtual ~Concept() = default;
    virtual std::unique_ptr<Concept> clone() const = 0;
    virtual BPTreeKey key() const = 0;
    virtual RID value() const = 0;
    virtual void advance() = 0;
    virtual bool is_end() const = 0;
    virtual bool equals(const Concept &other) const = 0;
  };

  template <typename Iter>
  struct Model final : Concept {
    explicit Model(Iter it) : it_(std::move(it)) {}

    std::unique_ptr<Concept> clone() const override {
      return std::make_unique<Model>(it_);
    }

    BPTreeKey key() const override { return it_.key(); }

    RID value() const override { return it_.value(); }

    void advance() override { ++it_; }

    bool is_end() const override { return it_.is_end(); }

    bool equals(const Concept &other) const override {
      const auto *o = dynamic_cast<const Model *>(&other);
      return o != nullptr && it_ == o->it_;
    }

    Iter it_;
  };

  std::unique_ptr<Concept> impl_;
};

/**
 * @brief Abstract secondary index over BPTreeKey → RID
 */
class Index {
public:
  virtual ~Index() = default;

  /**
   * @brief Insert a key → RID mapping
   * @return Status::Ok() on success; AlreadyExists on unique-key conflict
   */
  [[nodiscard]] virtual Status insert(BPTreeKey key, const RID &rid) = 0;

  /**
   * @brief Remove a key → RID mapping
   *
   * @p rid is reserved for non-unique indexes (remove only the matching RID).
   * Unique implementations may ignore a RID mismatch when only one entry exists.
   */
  [[nodiscard]] virtual Status remove(BPTreeKey key, const RID &rid) = 0;

  /**
   * @brief Point lookup: first RID for @p key, if any
   */
  [[nodiscard]] virtual std::optional<RID> find(BPTreeKey key) = 0;

  /**
   * @brief All RIDs for @p key
   *
   * Until multi-match support lands, unique trees return 0 or 1 element
   * matching find().
   */
  [[nodiscard]] virtual std::vector<RID> find_all(BPTreeKey key) = 0;

  /**
   * @brief Whether this index enforces uniqueness on keys
   */
  [[nodiscard]] virtual bool is_unique() const = 0;

  /**
   * @brief Iterator to the first entry with key >= @p key
   */
  [[nodiscard]] virtual IndexIterator lower_bound(BPTreeKey key) = 0;

  /**
   * @brief Iterator to the smallest key
   */
  [[nodiscard]] virtual IndexIterator begin() = 0;

  /**
   * @brief Past-the-end iterator
   */
  [[nodiscard]] virtual IndexIterator end() = 0;
};

} // namespace entropy
