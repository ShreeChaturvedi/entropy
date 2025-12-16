/**
 * @file b_plus_tree.cpp
 * @brief B+ Tree implementation
 */

#include "storage/b_plus_tree.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// BPlusTree Implementation
// ─────────────────────────────────────────────────────────────────────────────

BPlusTree::BPlusTree(std::shared_ptr<BufferPoolManager> buffer_pool,
                     page_id_t root_page_id)
    : buffer_pool_(std::move(buffer_pool))
    , root_page_id_(root_page_id)
    , leaf_max_size_(BPTreeLeafPage::compute_max_size())
    , internal_max_size_(BPTreeInternalPage::compute_max_size()) {}

bool BPlusTree::is_empty() const {
    if (root_page_id_ == INVALID_PAGE_ID) {
        return true;
    }

    // Check if root page has any keys
    Page* page = buffer_pool_->fetch_page(root_page_id_);
    if (page == nullptr) {
        return true;
    }

    BPTreePage bp_page(page);
    uint32_t num_keys = bp_page.num_keys();
    buffer_pool_->unpin_page(root_page_id_, false);

    return num_keys == 0;
}

// ─────────────────────────────────────────────────────────────────────────────
// Find Operation
// ─────────────────────────────────────────────────────────────────────────────

page_id_t BPlusTree::find_leaf(KeyType key) {
    if (root_page_id_ == INVALID_PAGE_ID) {
        return INVALID_PAGE_ID;
    }

    page_id_t current_page_id = root_page_id_;

    while (true) {
        Page* page = buffer_pool_->fetch_page(current_page_id);
        if (page == nullptr) {
            return INVALID_PAGE_ID;
        }

        BPTreePage bp_page(page);

        if (bp_page.is_leaf()) {
            buffer_pool_->unpin_page(current_page_id, false);
            return current_page_id;
        }

        // Internal node - find the child to descend into
        BPTreeInternalPage internal(page);
        uint32_t child_idx = internal.find_child_index(key);
        page_id_t child_page_id = internal.child_at(child_idx);

        buffer_pool_->unpin_page(current_page_id, false);
        current_page_id = child_page_id;
    }
}

std::optional<BPlusTree::ValueType> BPlusTree::find(KeyType key) {
    page_id_t leaf_page_id = find_leaf(key);
    if (leaf_page_id == INVALID_PAGE_ID) {
        return std::nullopt;
    }

    Page* page = buffer_pool_->fetch_page(leaf_page_id);
    if (page == nullptr) {
        return std::nullopt;
    }

    BPTreeLeafPage leaf(page);
    auto result = leaf.find(key);

    buffer_pool_->unpin_page(leaf_page_id, false);
    return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// Insert Operation
// ─────────────────────────────────────────────────────────────────────────────

Page* BPlusTree::create_leaf_page(page_id_t* page_id) {
    Page* page = buffer_pool_->new_page(page_id);
    if (page == nullptr) {
        return nullptr;
    }

    BPTreeLeafPage leaf(page);
    leaf.init(leaf_max_size_);
    return page;
}

Page* BPlusTree::create_internal_page(page_id_t* page_id) {
    Page* page = buffer_pool_->new_page(page_id);
    if (page == nullptr) {
        return nullptr;
    }

    BPTreeInternalPage internal(page);
    internal.init(internal_max_size_);
    return page;
}

Status BPlusTree::insert(KeyType key, const ValueType& value) {
    // Case 1: Empty tree - create root as leaf
    if (root_page_id_ == INVALID_PAGE_ID) {
        page_id_t new_root_id;
        Page* root_page = create_leaf_page(&new_root_id);
        if (root_page == nullptr) {
            return Status::OutOfMemory("Failed to create root page");
        }

        BPTreeLeafPage leaf(root_page);
        if (!leaf.insert(key, value)) {
            buffer_pool_->unpin_page(new_root_id, false);
            return Status::AlreadyExists("Key already exists");
        }

        root_page_id_ = new_root_id;
        buffer_pool_->unpin_page(new_root_id, true);
        return Status::Ok();
    }

    // Case 2: Find the leaf where this key should go
    page_id_t leaf_page_id = find_leaf(key);
    if (leaf_page_id == INVALID_PAGE_ID) {
        return Status::Internal("Failed to find leaf for insertion");
    }

    Page* leaf_page = buffer_pool_->fetch_page(leaf_page_id);
    if (leaf_page == nullptr) {
        return Status::IOError("Failed to fetch leaf page");
    }

    // Try to insert into the leaf
    InsertResult result = insert_into_leaf(leaf_page, key, value);

    if (!result.status.ok()) {
        buffer_pool_->unpin_page(leaf_page_id, false);
        return result.status;
    }

    // If split occurred, propagate up
    if (result.did_split) {
        Status parent_status = insert_into_parent(leaf_page, result.split_key,
                                                   result.new_page_id);
        buffer_pool_->unpin_page(leaf_page_id, true);
        return parent_status;
    }

    buffer_pool_->unpin_page(leaf_page_id, true);
    return Status::Ok();
}

BPlusTree::InsertResult BPlusTree::insert_into_leaf(Page* leaf_page,
                                                     KeyType key,
                                                     const ValueType& value) {
    InsertResult result;
    BPTreeLeafPage leaf(leaf_page);

    // Check for duplicate
    if (leaf.find(key).has_value()) {
        result.status = Status::AlreadyExists("Key already exists");
        return result;
    }

    // Try direct insert
    if (!leaf.is_full()) {
        if (leaf.insert(key, value)) {
            result.status = Status::Ok();
            return result;
        }
        result.status = Status::Internal("Insert failed unexpectedly");
        return result;
    }

    // Leaf is full - need to split
    page_id_t new_leaf_id;
    Page* new_leaf_page = create_leaf_page(&new_leaf_id);
    if (new_leaf_page == nullptr) {
        result.status = Status::OutOfMemory("Failed to create new leaf for split");
        return result;
    }

    BPTreeLeafPage new_leaf(new_leaf_page);

    // Copy parent info
    new_leaf.set_parent_page_id(leaf.parent_page_id());

    // Split the leaf - move half to new leaf
    KeyType split_key = leaf.move_half_to(&new_leaf);

    // Update sibling pointers
    page_id_t old_next = leaf.next_leaf_id();
    leaf.set_next_leaf_id(new_leaf_id);
    new_leaf.set_prev_leaf_id(leaf.page_id());
    new_leaf.set_next_leaf_id(old_next);

    // Update the old next leaf's prev pointer
    if (old_next != INVALID_PAGE_ID) {
        Page* old_next_page = buffer_pool_->fetch_page(old_next);
        if (old_next_page != nullptr) {
            BPTreeLeafPage old_next_leaf(old_next_page);
            old_next_leaf.set_prev_leaf_id(new_leaf_id);
            buffer_pool_->unpin_page(old_next, true);
        }
    }

    // Insert the new key into the appropriate leaf
    if (key < split_key) {
        leaf.insert(key, value);
    } else {
        new_leaf.insert(key, value);
    }

    buffer_pool_->unpin_page(new_leaf_id, true);

    result.status = Status::Ok();
    result.did_split = true;
    result.split_key = split_key;
    result.new_page_id = new_leaf_id;
    return result;
}

Status BPlusTree::insert_into_parent(Page* old_page, KeyType key,
                                      page_id_t new_page_id) {
    BPTreePage bp_page(old_page);
    page_id_t old_page_id = bp_page.page_id();
    page_id_t parent_id = bp_page.parent_page_id();

    // Case 1: Old page is root - create new root
    if (parent_id == INVALID_PAGE_ID) {
        page_id_t new_root_id;
        Page* new_root_page = create_internal_page(&new_root_id);
        if (new_root_page == nullptr) {
            return Status::OutOfMemory("Failed to create new root");
        }

        BPTreeInternalPage new_root(new_root_page);
        new_root.set_child_at(0, old_page_id);
        new_root.insert_at(0, key, new_page_id);

        // Update children's parent pointers
        bp_page.set_parent_page_id(new_root_id);

        Page* new_child_page = buffer_pool_->fetch_page(new_page_id);
        if (new_child_page != nullptr) {
            BPTreePage new_child(new_child_page);
            new_child.set_parent_page_id(new_root_id);
            buffer_pool_->unpin_page(new_page_id, true);
        }

        root_page_id_ = new_root_id;
        buffer_pool_->unpin_page(new_root_id, true);
        return Status::Ok();
    }

    // Case 2: Insert into existing parent
    Page* parent_page = buffer_pool_->fetch_page(parent_id);
    if (parent_page == nullptr) {
        return Status::IOError("Failed to fetch parent page");
    }

    BPTreeInternalPage parent(parent_page);

    // Find where to insert in parent
    uint32_t idx = parent.find_child_index(key);

    // If parent has room, insert directly
    if (!parent.is_full()) {
        parent.insert_at(idx, key, new_page_id);

        // Update new child's parent pointer
        Page* new_child_page = buffer_pool_->fetch_page(new_page_id);
        if (new_child_page != nullptr) {
            BPTreePage new_child(new_child_page);
            new_child.set_parent_page_id(parent_id);
            buffer_pool_->unpin_page(new_page_id, true);
        }

        buffer_pool_->unpin_page(parent_id, true);
        return Status::Ok();
    }

    // Parent is full - need to split parent
    page_id_t new_internal_id;
    Page* new_internal_page = create_internal_page(&new_internal_id);
    if (new_internal_page == nullptr) {
        buffer_pool_->unpin_page(parent_id, false);
        return Status::OutOfMemory("Failed to create new internal node");
    }

    BPTreeInternalPage new_internal(new_internal_page);
    new_internal.set_parent_page_id(parent.parent_page_id());

    // We need to split the internal node
    // Strategy: collect all keys/children, split, and redistribute
    uint32_t total_keys = parent.num_keys() + 1;
    uint32_t mid = total_keys / 2;

    // Temporarily store all entries
    std::vector<KeyType> all_keys;
    std::vector<page_id_t> all_children;

    all_children.push_back(parent.child_at(0));
    for (uint32_t i = 0; i < parent.num_keys(); ++i) {
        if (i == idx) {
            all_keys.push_back(key);
            all_children.push_back(new_page_id);
        }
        all_keys.push_back(parent.key_at(i));
        all_children.push_back(parent.child_at(i + 1));
    }
    if (idx == parent.num_keys()) {
        all_keys.push_back(key);
        all_children.push_back(new_page_id);
    }

    // Key at mid goes up to parent's parent
    KeyType push_up_key = all_keys[mid];

    // Left side: keys 0..mid-1, children 0..mid
    parent.set_num_keys(0);
    parent.set_child_at(0, all_children[0]);
    for (uint32_t i = 0; i < mid; ++i) {
        parent.insert_at(i, all_keys[i], all_children[i + 1]);
    }

    // Right side: keys mid+1..end, children mid+1..end
    new_internal.set_child_at(0, all_children[mid + 1]);
    for (uint32_t i = mid + 1; i < all_keys.size(); ++i) {
        new_internal.insert_at(i - mid - 1, all_keys[i], all_children[i + 1]);
    }

    // Update parent pointers for children that moved to new_internal
    update_children_parent(new_internal_id, &new_internal);

    buffer_pool_->unpin_page(new_internal_id, true);

    // Recursively insert push_up_key into grandparent
    Status result = insert_into_parent(parent_page, push_up_key, new_internal_id);
    buffer_pool_->unpin_page(parent_id, true);
    return result;
}

void BPlusTree::update_children_parent(page_id_t parent_id,
                                        BPTreeInternalPage* internal) {
    for (uint32_t i = 0; i <= internal->num_keys(); ++i) {
        page_id_t child_id = internal->child_at(i);
        if (child_id != INVALID_PAGE_ID) {
            Page* child_page = buffer_pool_->fetch_page(child_id);
            if (child_page != nullptr) {
                BPTreePage child(child_page);
                child.set_parent_page_id(parent_id);
                buffer_pool_->unpin_page(child_id, true);
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Remove Operation (Simplified - marks as deleted without rebalancing)
// ─────────────────────────────────────────────────────────────────────────────

Status BPlusTree::remove(KeyType key) {
    page_id_t leaf_page_id = find_leaf(key);
    if (leaf_page_id == INVALID_PAGE_ID) {
        return Status::NotFound("Key not found");
    }

    Page* page = buffer_pool_->fetch_page(leaf_page_id);
    if (page == nullptr) {
        return Status::IOError("Failed to fetch leaf page");
    }

    BPTreeLeafPage leaf(page);
    bool removed = leaf.remove(key);

    buffer_pool_->unpin_page(leaf_page_id, removed);

    if (!removed) {
        return Status::NotFound("Key not found");
    }

    // Note: A complete implementation would handle underflow and rebalancing
    // For simplicity, we just remove the key without rebalancing

    return Status::Ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Range Scan
// ─────────────────────────────────────────────────────────────────────────────

std::vector<std::pair<BPlusTree::KeyType, BPlusTree::ValueType>>
BPlusTree::range_scan(KeyType start_key, KeyType end_key) {
    std::vector<std::pair<KeyType, ValueType>> result;

    if (start_key > end_key) {
        return result;
    }

    for (auto it = lower_bound(start_key); it != end(); ++it) {
        auto [key, value] = *it;
        if (key > end_key) {
            break;
        }
        result.emplace_back(key, value);
    }

    return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// Iterator Support
// ─────────────────────────────────────────────────────────────────────────────

BPlusTreeIterator BPlusTree::lower_bound(KeyType key) {
    page_id_t leaf_page_id = find_leaf(key);
    if (leaf_page_id == INVALID_PAGE_ID) {
        return end();
    }

    Page* page = buffer_pool_->fetch_page(leaf_page_id);
    if (page == nullptr) {
        return end();
    }

    BPTreeLeafPage leaf(page);
    bool found;
    uint32_t idx = leaf.find_key_index(key, &found);

    // If idx is past the end of this leaf, move to next leaf
    if (idx >= leaf.num_keys()) {
        page_id_t next_leaf = leaf.next_leaf_id();
        buffer_pool_->unpin_page(leaf_page_id, false);

        if (next_leaf == INVALID_PAGE_ID) {
            return end();
        }
        return BPlusTreeIterator(this, next_leaf, 0);
    }

    buffer_pool_->unpin_page(leaf_page_id, false);
    return BPlusTreeIterator(this, leaf_page_id, idx);
}

BPlusTreeIterator BPlusTree::begin() {
    if (root_page_id_ == INVALID_PAGE_ID) {
        return end();
    }

    // Find the leftmost leaf
    page_id_t current = root_page_id_;

    while (true) {
        Page* page = buffer_pool_->fetch_page(current);
        if (page == nullptr) {
            return end();
        }

        BPTreePage bp_page(page);

        if (bp_page.is_leaf()) {
            BPTreeLeafPage leaf(page);
            buffer_pool_->unpin_page(current, false);

            if (leaf.num_keys() == 0) {
                return end();
            }
            return BPlusTreeIterator(this, current, 0);
        }

        // Go to leftmost child
        BPTreeInternalPage internal(page);
        page_id_t next = internal.child_at(0);
        buffer_pool_->unpin_page(current, false);
        current = next;
    }
}

BPlusTreeIterator BPlusTree::end() {
    return BPlusTreeIterator();
}

// ─────────────────────────────────────────────────────────────────────────────
// BPlusTreeIterator Implementation
// ─────────────────────────────────────────────────────────────────────────────

BPlusTreeIterator::BPlusTreeIterator(BPlusTree* tree, page_id_t leaf_page_id,
                                     uint32_t index)
    : tree_(tree), leaf_page_id_(leaf_page_id), index_(index) {}

BPlusTreeIterator::KeyType BPlusTreeIterator::key() const {
    if (is_end()) return 0;

    Page* page = tree_->buffer_pool()->fetch_page(leaf_page_id_);
    if (page == nullptr) return 0;

    BPTreeLeafPage leaf(page);
    KeyType k = leaf.key_at(index_);

    tree_->buffer_pool()->unpin_page(leaf_page_id_, false);
    return k;
}

BPlusTreeIterator::ValueType BPlusTreeIterator::value() const {
    if (is_end()) return ValueType();

    Page* page = tree_->buffer_pool()->fetch_page(leaf_page_id_);
    if (page == nullptr) return ValueType();

    BPTreeLeafPage leaf(page);
    ValueType v = leaf.value_at(index_);

    tree_->buffer_pool()->unpin_page(leaf_page_id_, false);
    return v;
}

std::pair<BPlusTreeIterator::KeyType, BPlusTreeIterator::ValueType>
BPlusTreeIterator::operator*() const {
    if (is_end()) return {0, ValueType()};

    Page* page = tree_->buffer_pool()->fetch_page(leaf_page_id_);
    if (page == nullptr) return {0, ValueType()};

    BPTreeLeafPage leaf(page);
    auto result = std::make_pair(leaf.key_at(index_), leaf.value_at(index_));

    tree_->buffer_pool()->unpin_page(leaf_page_id_, false);
    return result;
}

BPlusTreeIterator& BPlusTreeIterator::operator++() {
    if (is_end()) return *this;

    Page* page = tree_->buffer_pool()->fetch_page(leaf_page_id_);
    if (page == nullptr) {
        leaf_page_id_ = INVALID_PAGE_ID;
        return *this;
    }

    BPTreeLeafPage leaf(page);

    ++index_;

    if (index_ >= leaf.num_keys()) {
        // Move to next leaf
        page_id_t next_leaf = leaf.next_leaf_id();
        tree_->buffer_pool()->unpin_page(leaf_page_id_, false);

        if (next_leaf == INVALID_PAGE_ID) {
            leaf_page_id_ = INVALID_PAGE_ID;
            index_ = 0;
        } else {
            leaf_page_id_ = next_leaf;
            index_ = 0;
        }
    } else {
        tree_->buffer_pool()->unpin_page(leaf_page_id_, false);
    }

    return *this;
}

BPlusTreeIterator BPlusTreeIterator::operator++(int) {
    BPlusTreeIterator tmp = *this;
    ++(*this);
    return tmp;
}

}  // namespace entropy
