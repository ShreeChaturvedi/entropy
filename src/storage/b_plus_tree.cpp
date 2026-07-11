/**
 * @file b_plus_tree.cpp
 * @brief B+ Tree implementation
 */

#include "storage/b_plus_tree.hpp"

#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Concurrency design (issue #5): latch crabbing
//
// Every Page carries a reader/writer latch (Page::rlatch/wlatch). The B+ tree
// uses hand-over-hand (crabbing) latch acquisition:
//
//   * Searches / scans (find, range_scan, iterator, is_empty) descend taking
//     READ latches root-to-leaf, releasing a parent as soon as the child is
//     latched. A leaf is read under its read latch, so a concurrent writer
//     mutating that page is excluded.
//
//   * insert / remove take WRITE latches top-down and RELEASE ancestors as soon
//     as the child is "safe" (an insert child that cannot split -- has a free
//     slot; a delete child that cannot underflow -- has more than min_size).
//     The retained ancestors are exactly the nodes a split/merge might touch, so
//     a structural change holds write latches on its whole affected path,
//     including the parent, while it runs -- which is what makes read crabbing
//     return correct answers across a split (the reader either passes the parent
//     before the split, seeing the pre-split child, or after, seeing the updated
//     parent that routes to the new sibling).
//
// Frame-reuse hazard: the buffer pool reuses a frame's Page object when it
// evicts/reallocates. The latch lives in the Page (not in data_) and Page::reset
// never touches it, so it survives reuse; and because a latched page is always
// kept pinned, the buffer pool refuses to evict or delete that frame, so a
// latched page can never be reused underneath its holder.
//
// Deadlock freedom: structural writers are serialized by write_mutex_, so the
// only latch-ordering interactions are reader-vs-writer. Both descend top-down
// (ancestors before descendants), and every sibling latch a writer takes is
// acquired left-to-right in the leaf chain (the merge/borrow-with-left cases
// briefly drop and re-take the current node's latch to preserve that order),
// matching the rightward direction of range scans. With a single global order
// (ancestor < descendant, left < right) no cycle can form.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Tracks the pages one structural write has pinned and write-latched.
 *
 * Writers are serialized by BPlusTree::write_mutex_, so exactly one WriteSet is
 * active at a time. Each page is pinned and write-latched exactly once for the
 * whole operation (repeated acquires return the already-held page);
 * release_all() (also run by the destructor, so latches are freed even if a
 * helper returns early) drops every latch and pin.
 */
class WriteSet {
public:
    explicit WriteSet(BufferPoolManager* bpm) : bpm_(bpm) {}
    WriteSet(const WriteSet&) = delete;
    WriteSet& operator=(const WriteSet&) = delete;
    ~WriteSet() { release_all(); }

    /// Pin + write-latch @p id the first time this op touches it; subsequent
    /// acquires return the held page without another buffer-pool round trip
    /// (the frame is pinned, so the pointer is stable).
    [[nodiscard]] Page* acquire(page_id_t id) {
        auto it = held_.find(id);
        if (it != held_.end()) {
            relatch_entry(it->second);
            return it->second.page;
        }
        Page* p = bpm_->fetch_page(id);
        if (p == nullptr) {
            return nullptr;
        }
        return track(id, p);
    }

    /// Adopt a freshly created page (already pinned once by new_page).
    Page* adopt(page_id_t id, Page* p) { return track(id, p); }

    /// Temporarily drop @p id's write latch (keeps pins) to re-take it later in
    /// a deadlock-free (left-to-right) order.
    void unlatch(page_id_t id) {
        auto it = held_.find(id);
        if (it != held_.end() && it->second.latched) {
            it->second.page->wunlatch();
            it->second.latched = false;
        }
    }
    void relatch(page_id_t id) {
        auto it = held_.find(id);
        if (it != held_.end()) {
            relatch_entry(it->second);
        }
    }

    /// Release @p id early (unlatch + drop this op's pins). Used to shed safe
    /// ancestors during the crabbing descent.
    void release(page_id_t id) {
        auto it = held_.find(id);
        if (it == held_.end()) {
            return;
        }
        drop(it->second, id);
        held_.erase(it);
    }

    /// Fully release @p id and delete its page from the pool.
    bool discard_and_delete(page_id_t id) {
        auto it = held_.find(id);
        if (it != held_.end()) {
            drop(it->second, id);
            held_.erase(it);
        }
        return bpm_->delete_page(id);
    }

    void release_all() {
        for (auto& [id, e] : held_) {
            drop(e, id);
        }
        held_.clear();
    }

private:
    struct Entry {
        Page* page = nullptr;
        bool latched = false;
    };

    /// Record @p p (which carries exactly one pin for this op) and latch it.
    Page* track(page_id_t id, Page* p) {
        Entry& e = held_[id];
        e.page = p;
        relatch_entry(e);
        return p;
    }

    static void relatch_entry(Entry& e) {
        if (!e.latched) {
            e.page->wlatch();
            e.latched = true;
        }
    }

    void drop(Entry& e, page_id_t id) {
        if (e.latched) {
            e.page->wunlatch();
            e.latched = false;
        }
        // Dirtiness is set by the BPTree page setters as they mutate, so an
        // unmodified (read-only) latched ancestor is not needlessly dirtied.
        bpm_->unpin_page(id, false);
    }

    BufferPoolManager* bpm_;
    std::unordered_map<page_id_t, Entry> held_;
};

// ─────────────────────────────────────────────────────────────────────────────
// BPlusTree Implementation
// ─────────────────────────────────────────────────────────────────────────────

void BPlusTree::change_root(page_id_t new_root_id) {
    root_page_id_.store(new_root_id, std::memory_order_relaxed);
    if (root_change_callback_) {
        root_change_callback_(new_root_id);
    }
}

BPlusTree::BPlusTree(std::shared_ptr<BufferPoolManager> buffer_pool,
                     page_id_t root_page_id)
    : buffer_pool_(std::move(buffer_pool))
    , root_page_id_(root_page_id)
    , leaf_max_size_(BPTreeLeafPage::compute_max_size())
    , internal_max_size_(BPTreeInternalPage::compute_max_size()) {}

BPlusTree::BPlusTree(std::shared_ptr<BufferPoolManager> buffer_pool,
                     page_id_t root_page_id, uint32_t leaf_max_size,
                     uint32_t internal_max_size)
    : buffer_pool_(std::move(buffer_pool))
    , root_page_id_(root_page_id)
    , leaf_max_size_(leaf_max_size)
    , internal_max_size_(internal_max_size) {}

Page* BPlusTree::latch_root_read() const {
    while (true) {
        const page_id_t root = root_page_id();
        if (root == INVALID_PAGE_ID) {
            return nullptr;
        }

        Page* page = buffer_pool_->fetch_page(root);
        if (page == nullptr) {
            return nullptr;
        }
        page->rlatch();
        // A writer can only move the root while holding the old root's write
        // latch, so if the id still matches we hold the current root stably;
        // otherwise the root moved between snapshot and latch -- retry.
        if (root_page_id() == root) {
            return page;
        }
        page->runlatch();
        buffer_pool_->unpin_page(root, false);
    }
}

bool BPlusTree::is_empty() const {
    Page* page = latch_root_read();
    if (page == nullptr) {
        return true;
    }

    uint32_t num_keys = BPTreePage(page).num_keys();
    page_id_t root_id = page->page_id();
    page->runlatch();
    buffer_pool_->unpin_page(root_id, false);
    return num_keys == 0;
}

// ─────────────────────────────────────────────────────────────────────────────
// Find Operation
// ─────────────────────────────────────────────────────────────────────────────

Page* BPlusTree::descend_to_leaf_read(KeyType key) {
    Page* page = latch_root_read();
    if (page == nullptr) {
        return nullptr;
    }

    // Hand-over-hand: hold the parent's read latch until the child's is in.
    while (true) {
        BPTreePage node(page);
        if (node.is_leaf()) {
            return page;  // caller runlatch()s and unpins
        }
        BPTreeInternalPage internal(page);
        page_id_t child_id = internal.child_at(internal.find_child_index(key));
        Page* child = buffer_pool_->fetch_page(child_id);
        if (child == nullptr) {
            page_id_t pid = page->page_id();
            page->runlatch();
            buffer_pool_->unpin_page(pid, false);
            return nullptr;
        }
        child->rlatch();
        page_id_t pid = page->page_id();
        page->runlatch();
        buffer_pool_->unpin_page(pid, false);
        page = child;
    }
}

std::optional<BPlusTree::ValueType> BPlusTree::find(KeyType key) {
    Page* leaf_page = descend_to_leaf_read(key);
    if (leaf_page == nullptr) {
        return std::nullopt;
    }

    BPTreeLeafPage leaf(leaf_page);
    auto result = leaf.find(key);

    page_id_t leaf_id = leaf_page->page_id();
    leaf_page->runlatch();
    buffer_pool_->unpin_page(leaf_id, false);
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

void BPlusTree::set_parent(WriteSet& ws, page_id_t child_id,
                           page_id_t parent_id) {
    if (child_id == INVALID_PAGE_ID) {
        return;
    }
    Page* child = ws.acquire(child_id);
    if (child != nullptr) {
        BPTreePage(child).set_parent_page_id(parent_id);
    }
}

void BPlusTree::reparent_children(WriteSet& ws, page_id_t parent_id,
                                  BPTreeInternalPage& internal) {
    for (uint32_t i = 0; i <= internal.num_keys(); ++i) {
        set_parent(ws, internal.child_at(i), parent_id);
    }
}

Status BPlusTree::insert(KeyType key, const ValueType& value) {
    std::lock_guard<std::mutex> wlock(write_mutex_);

    // Case 1: Empty tree - create the root as a leaf.
    if (root_page_id() == INVALID_PAGE_ID) {
        page_id_t new_root_id;
        Page* root_page = create_leaf_page(&new_root_id);
        if (root_page == nullptr) {
            return Status::OutOfMemory("Failed to create root page");
        }
        root_page->wlatch();
        BPTreeLeafPage leaf(root_page);
        bool inserted = leaf.insert(key, value);
        if (inserted) {
            change_root(new_root_id);
        }
        root_page->wunlatch();
        buffer_pool_->unpin_page(new_root_id, true);
        return inserted ? Status::Ok()
                        : Status::AlreadyExists("Key already exists");
    }

    // Case 2: Crab down with write latches, shedding safe ancestors.
    WriteSet ws(buffer_pool_.get());
    std::vector<page_id_t> path;  // retained root-first chain, all write-latched

    page_id_t cur_id = root_page_id();
    Page* cur = ws.acquire(cur_id);
    if (cur == nullptr) {
        return Status::IOError("Failed to fetch root page");
    }
    path.push_back(cur_id);

    while (!BPTreePage(cur).is_leaf()) {
        BPTreeInternalPage internal(cur);
        cur_id = internal.child_at(internal.find_child_index(key));
        cur = ws.acquire(cur_id);
        if (cur == nullptr) {
            return Status::IOError("Failed to fetch child page");
        }
        BPTreePage child_node(cur);
        // "Safe": the child has a free slot, so it cannot split and no key can
        // propagate above it -- release every retained ancestor.
        if (child_node.num_keys() < child_node.max_size()) {
            for (page_id_t aid : path) {
                ws.release(aid);
            }
            path.clear();
        }
        path.push_back(cur_id);
    }

    BPTreeLeafPage leaf(cur);
    if (leaf.find(key).has_value()) {
        return Status::AlreadyExists("Key already exists");
    }
    if (!leaf.is_full()) {
        leaf.insert(key, value);
        return Status::Ok();  // ws releases the (single) latched leaf
    }

    // Leaf is full - split it.
    page_id_t new_leaf_id;
    Page* new_leaf_page = create_leaf_page(&new_leaf_id);
    if (new_leaf_page == nullptr) {
        return Status::OutOfMemory("Failed to create new leaf for split");
    }
    ws.adopt(new_leaf_id, new_leaf_page);
    BPTreeLeafPage new_leaf(new_leaf_page);

    KeyType split_key = leaf.move_half_to(&new_leaf);

    page_id_t old_next = leaf.next_leaf_id();
    leaf.set_next_leaf_id(new_leaf_id);
    new_leaf.set_prev_leaf_id(leaf.page_id());
    new_leaf.set_next_leaf_id(old_next);
    if (old_next != INVALID_PAGE_ID) {
        // Right neighbour: acquired left-to-right, preserving the global order.
        Page* on = ws.acquire(old_next);
        if (on != nullptr) {
            BPTreeLeafPage(on).set_prev_leaf_id(new_leaf_id);
        }
    }

    if (key < split_key) {
        leaf.insert(key, value);
    } else {
        new_leaf.insert(key, value);
    }

    return insert_into_parents(ws, path, split_key, new_leaf_id);
}

Status BPlusTree::insert_into_parents(WriteSet& ws, std::vector<page_id_t>& path,
                                      KeyType up_key, page_id_t up_child) {
    // path.back() is the node that just split; (up_key, up_child) is the
    // separator and new right sibling to install in its parent.
    while (true) {
        page_id_t split_id = path.back();
        path.pop_back();

        if (path.empty()) {
            // The split node was the root - grow a new root above it.
            page_id_t new_root_id;
            Page* new_root_page = create_internal_page(&new_root_id);
            if (new_root_page == nullptr) {
                return Status::OutOfMemory("Failed to create new root");
            }
            ws.adopt(new_root_id, new_root_page);
            BPTreeInternalPage new_root(new_root_page);
            new_root.set_child_at(0, split_id);
            new_root.insert_at(0, up_key, up_child);
            set_parent(ws, split_id, new_root_id);
            set_parent(ws, up_child, new_root_id);
            change_root(new_root_id);
            return Status::Ok();
        }

        page_id_t parent_id = path.back();
        Page* parent_page = ws.acquire(parent_id);  // already latched (retained)
        if (parent_page == nullptr) {
            return Status::IOError("Failed to fetch parent page");
        }
        BPTreeInternalPage parent(parent_page);
        uint32_t idx = parent.find_child_index(up_key);
        set_parent(ws, up_child, parent_id);

        if (!parent.is_full()) {
            parent.insert_at(idx, up_key, up_child);
            return Status::Ok();
        }

        // Parent is full - split it, then continue up with its push-up key.
        page_id_t new_internal_id;
        Page* new_internal_page = create_internal_page(&new_internal_id);
        if (new_internal_page == nullptr) {
            return Status::OutOfMemory("Failed to create new internal node");
        }
        ws.adopt(new_internal_id, new_internal_page);
        BPTreeInternalPage new_internal(new_internal_page);

        // Gather existing entries plus the new (up_key, up_child) at `idx`.
        uint32_t total_keys = parent.num_keys() + 1;
        uint32_t mid = total_keys / 2;
        std::vector<KeyType> all_keys;
        std::vector<page_id_t> all_children;
        all_keys.reserve(total_keys);
        all_children.reserve(total_keys + 1);
        all_children.push_back(parent.child_at(0));
        for (uint32_t i = 0; i < parent.num_keys(); ++i) {
            if (i == idx) {
                all_keys.push_back(up_key);
                all_children.push_back(up_child);
            }
            all_keys.push_back(parent.key_at(i));
            all_children.push_back(parent.child_at(i + 1));
        }
        if (idx == parent.num_keys()) {
            all_keys.push_back(up_key);
            all_children.push_back(up_child);
        }

        KeyType push_up_key = all_keys[mid];

        parent.set_num_keys(0);
        parent.set_child_at(0, all_children[0]);
        for (uint32_t i = 0; i < mid; ++i) {
            parent.insert_at(i, all_keys[i], all_children[i + 1]);
        }

        new_internal.set_child_at(0, all_children[mid + 1]);
        for (uint32_t i = mid + 1; i < all_keys.size(); ++i) {
            new_internal.insert_at(i - mid - 1, all_keys[i], all_children[i + 1]);
        }

        // Children that moved into new_internal need their parent pointer fixed
        // (this also re-parents up_child if it landed on the right side).
        reparent_children(ws, new_internal_id, new_internal);

        up_key = push_up_key;
        up_child = new_internal_id;
        // path.back() is now `parent_id`; the next iteration pops it and inserts
        // push_up_key into ITS parent (or grows a new root).
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Remove Operation (with rebalancing)
// ─────────────────────────────────────────────────────────────────────────────

Status BPlusTree::remove(KeyType key) {
    std::lock_guard<std::mutex> wlock(write_mutex_);

    if (root_page_id() == INVALID_PAGE_ID) {
        return Status::NotFound("Key not found");
    }

    // Crab down with write latches, shedding ancestors once a child cannot
    // underflow (has more than min_size, so it stays >= min_size after a merge
    // removes one key/child).
    WriteSet ws(buffer_pool_.get());
    std::vector<page_id_t> path;

    page_id_t cur_id = root_page_id();
    Page* cur = ws.acquire(cur_id);
    if (cur == nullptr) {
        return Status::IOError("Failed to fetch root page");
    }
    path.push_back(cur_id);

    while (!BPTreePage(cur).is_leaf()) {
        BPTreeInternalPage internal(cur);
        cur_id = internal.child_at(internal.find_child_index(key));
        cur = ws.acquire(cur_id);
        if (cur == nullptr) {
            return Status::IOError("Failed to fetch child page");
        }
        BPTreePage child_node(cur);
        if (child_node.num_keys() > child_node.min_size()) {
            for (page_id_t aid : path) {
                ws.release(aid);
            }
            path.clear();
        }
        path.push_back(cur_id);
    }

    BPTreeLeafPage leaf(cur);
    if (!leaf.remove(key)) {
        return Status::NotFound("Key not found");
    }

    if (leaf.is_root()) {
        // Root leaf: dropping the last key empties the whole tree.
        if (leaf.is_empty()) {
            page_id_t leaf_id = leaf.page_id();
            ws.discard_and_delete(leaf_id);
            change_root(INVALID_PAGE_ID);
        }
        return Status::Ok();
    }

    if (!leaf.is_underflow()) {
        return Status::Ok();
    }

    // Underflow implies the leaf was not "safe" at descent, so its parent is
    // still in `path` (path.size() >= 2).
    return handle_leaf_underflow(ws, path);
}

Status BPlusTree::handle_leaf_underflow(WriteSet& ws,
                                        std::vector<page_id_t>& path) {
    page_id_t leaf_id = path.back();
    page_id_t parent_id = path[path.size() - 2];
    // Both pages are retained path members, already pinned + latched in ws;
    // acquire returns the held pages. The guard covers a broken invariant.
    Page* leaf_page = ws.acquire(leaf_id);
    Page* parent_page = ws.acquire(parent_id);
    if (leaf_page == nullptr || parent_page == nullptr) {
        return Status::Internal("Underflow path page not held");
    }
    BPTreeLeafPage leaf(leaf_page);
    BPTreeInternalPage parent(parent_page);
    uint32_t child_idx = parent.child_index_of(leaf_id);

    // Borrow from the right sibling (acquired left-to-right).
    if (child_idx < parent.num_keys()) {
        page_id_t right_id = parent.child_at(child_idx + 1);
        Page* right = ws.acquire(right_id);
        if (right != nullptr) {
            BPTreeLeafPage right_sibling(right);
            if (right_sibling.num_keys() > right_sibling.min_size()) {
                BPTreeKey new_key = leaf.borrow_from_right(&right_sibling);
                parent.set_key_at(child_idx, new_key);
                return Status::Ok();
            }
        }
    }

    // Borrow from the left sibling. Acquire the left sibling BEFORE re-taking the
    // leaf so every latch is taken left-to-right (deadlock-free vs right scans).
    if (child_idx > 0) {
        page_id_t left_id = parent.child_at(child_idx - 1);
        ws.unlatch(leaf_id);
        Page* left = ws.acquire(left_id);
        ws.relatch(leaf_id);
        if (left != nullptr) {
            BPTreeLeafPage left_sibling(left);
            if (left_sibling.num_keys() > left_sibling.min_size()) {
                BPTreeKey new_key = leaf.borrow_from_left(&left_sibling);
                parent.set_key_at(child_idx - 1, new_key);
                return Status::Ok();
            }
        }
    }

    // Merge with the right sibling (absorb it into this leaf).
    if (child_idx < parent.num_keys()) {
        page_id_t right_id = parent.child_at(child_idx + 1);
        Page* right = ws.acquire(right_id);
        if (right != nullptr) {
            BPTreeLeafPage right_sibling(right);
            leaf.merge_from_right(&right_sibling);
            parent.remove_at(child_idx);
            page_id_t far_right_id = leaf.next_leaf_id();  // was right's next
            if (far_right_id != INVALID_PAGE_ID) {
                Page* far = ws.acquire(far_right_id);
                if (far != nullptr) {
                    BPTreeLeafPage(far).set_prev_leaf_id(leaf.page_id());
                }
            }
            ws.discard_and_delete(right_id);
        }
    } else if (child_idx > 0) {
        // This leaf is the rightmost child: merge it into the left sibling.
        page_id_t left_id = parent.child_at(child_idx - 1);
        ws.unlatch(leaf_id);
        Page* left = ws.acquire(left_id);
        ws.relatch(leaf_id);
        if (left != nullptr) {
            BPTreeLeafPage left_sibling(left);
            left_sibling.merge_from_right(&leaf);
            parent.remove_at(child_idx - 1);
            page_id_t far_right_id = left_sibling.next_leaf_id();  // was our next
            if (far_right_id != INVALID_PAGE_ID) {
                Page* far = ws.acquire(far_right_id);
                if (far != nullptr) {
                    BPTreeLeafPage(far).set_prev_leaf_id(left_sibling.page_id());
                }
            }
            ws.discard_and_delete(leaf_id);
        }
    }

    path.pop_back();  // leaf resolved; the parent lost a key
    return handle_parent_after_merge(ws, path);
}

Status BPlusTree::handle_parent_after_merge(WriteSet& ws,
                                            std::vector<page_id_t>& path) {
    page_id_t node_id = path.back();
    Page* node_page = ws.acquire(node_id);
    if (node_page == nullptr) {
        return Status::Internal("Underflow path page not held");
    }
    BPTreeInternalPage node(node_page);

    if (node.is_root()) {
        // A root internal node with no keys has a single child, which becomes
        // the new root.
        if (node.num_keys() == 0) {
            page_id_t new_root = node.child_at(0);
            set_parent(ws, new_root, INVALID_PAGE_ID);
            ws.discard_and_delete(node_id);
            change_root(new_root);
        }
        return Status::Ok();
    }

    if (!node.is_underflow()) {
        return Status::Ok();
    }

    // As with leaves, an underflowing non-root node kept its parent in `path`.
    return handle_internal_underflow(ws, path);
}

Status BPlusTree::handle_internal_underflow(WriteSet& ws,
                                            std::vector<page_id_t>& path) {
    page_id_t node_id = path.back();
    page_id_t parent_id = path[path.size() - 2];
    Page* node_page = ws.acquire(node_id);
    Page* parent_page = ws.acquire(parent_id);
    if (node_page == nullptr || parent_page == nullptr) {
        return Status::Internal("Underflow path page not held");
    }
    BPTreeInternalPage node(node_page);
    BPTreeInternalPage parent(parent_page);
    uint32_t child_idx = parent.child_index_of(node_id);

    // Borrow from the right sibling.
    if (child_idx < parent.num_keys()) {
        page_id_t right_id = parent.child_at(child_idx + 1);
        Page* right = ws.acquire(right_id);
        if (right != nullptr) {
            BPTreeInternalPage right_sibling(right);
            if (right_sibling.num_keys() > right_sibling.min_size()) {
                BPTreeKey separator = parent.key_at(child_idx);
                BPTreeKey new_sep = node.borrow_from_right(&right_sibling, separator);
                parent.set_key_at(child_idx, new_sep);
                set_parent(ws, node.child_at(node.num_keys()), node.page_id());
                return Status::Ok();
            }
        }
    }

    // Borrow from the left sibling (reorder for left-to-right acquisition).
    if (child_idx > 0) {
        page_id_t left_id = parent.child_at(child_idx - 1);
        ws.unlatch(node_id);
        Page* left = ws.acquire(left_id);
        ws.relatch(node_id);
        if (left != nullptr) {
            BPTreeInternalPage left_sibling(left);
            if (left_sibling.num_keys() > left_sibling.min_size()) {
                BPTreeKey separator = parent.key_at(child_idx - 1);
                BPTreeKey new_sep = node.borrow_from_left(&left_sibling, separator);
                parent.set_key_at(child_idx - 1, new_sep);
                set_parent(ws, node.child_at(0), node.page_id());
                return Status::Ok();
            }
        }
    }

    // Merge with the right sibling (absorb it into this node).
    if (child_idx < parent.num_keys()) {
        page_id_t right_id = parent.child_at(child_idx + 1);
        Page* right = ws.acquire(right_id);
        if (right != nullptr) {
            BPTreeInternalPage right_sibling(right);
            BPTreeKey separator = parent.key_at(child_idx);
            node.merge_from_right(&right_sibling, separator);
            reparent_children(ws, node.page_id(), node);
            parent.remove_at(child_idx);
            ws.discard_and_delete(right_id);
        }
    } else if (child_idx > 0) {
        // Rightmost child: merge it into the left sibling (reorder).
        page_id_t left_id = parent.child_at(child_idx - 1);
        ws.unlatch(node_id);
        Page* left = ws.acquire(left_id);
        ws.relatch(node_id);
        if (left != nullptr) {
            BPTreeInternalPage left_sibling(left);
            BPTreeKey separator = parent.key_at(child_idx - 1);
            left_sibling.merge_from_right(&node, separator);
            reparent_children(ws, left_sibling.page_id(), left_sibling);
            parent.remove_at(child_idx - 1);
            ws.discard_and_delete(node_id);
        }
    }

    path.pop_back();  // node resolved; propagate any further parent underflow
    return handle_parent_after_merge(ws, path);
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

    // Read-crab to the leaf holding start_key, then walk the leaf chain
    // rightward hand-over-hand (latch the next leaf before releasing the
    // current one). Holding a read latch on the leaf being scanned excludes a
    // concurrent writer from mutating it, and the rightward-only coupling
    // matches the writer's left-to-right sibling latching, so the scan sees a
    // consistent chain without skips, duplicates, or torn reads.
    Page* leaf_page = descend_to_leaf_read(start_key);
    while (leaf_page != nullptr) {
        BPTreeLeafPage leaf(leaf_page);
        uint32_t n = leaf.num_keys();
        bool past_end = false;
        for (uint32_t i = 0; i < n; ++i) {
            KeyType k = leaf.key_at(i);
            if (k < start_key) {
                continue;  // only the first leaf can hold keys below start_key
            }
            if (k > end_key) {
                past_end = true;
                break;
            }
            result.emplace_back(k, leaf.value_at(i));
        }

        page_id_t cur_id = leaf_page->page_id();
        page_id_t next_id = leaf.next_leaf_id();
        if (past_end || next_id == INVALID_PAGE_ID) {
            leaf_page->runlatch();
            buffer_pool_->unpin_page(cur_id, false);
            break;
        }

        Page* next_page = buffer_pool_->fetch_page(next_id);
        if (next_page == nullptr) {
            leaf_page->runlatch();
            buffer_pool_->unpin_page(cur_id, false);
            break;
        }
        next_page->rlatch();
        leaf_page->runlatch();
        buffer_pool_->unpin_page(cur_id, false);
        leaf_page = next_page;
    }

    return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// Whole-tree Maintenance
// ─────────────────────────────────────────────────────────────────────────────

std::vector<page_id_t> BPlusTree::collect_tree_pages() {
    std::vector<page_id_t> pages;
    std::vector<page_id_t> pending;
    if (root_page_id() != INVALID_PAGE_ID) {
        pending.push_back(root_page_id());
    }

    // Iterative traversal; the visited set makes the walk terminate even on a
    // damaged tree (e.g. pages that never reached disk before a crash).
    std::unordered_set<page_id_t> visited;
    while (!pending.empty()) {
        page_id_t page_id = pending.back();
        pending.pop_back();
        if (page_id == INVALID_PAGE_ID || visited.contains(page_id)) {
            continue;
        }
        visited.insert(page_id);

        Page* page = buffer_pool_->fetch_page(page_id);
        if (page == nullptr) {
            continue;
        }
        BPTreePage node(page);
        if (node.is_internal()) {
            BPTreeInternalPage internal(page);
            for (uint32_t i = 0; i <= internal.num_keys(); ++i) {
                pending.push_back(internal.child_at(i));
            }
        } else if (!node.is_leaf()) {
            // Not an intact index page — not ours to touch.
            buffer_pool_->unpin_page(page_id, false);
            continue;
        }
        buffer_pool_->unpin_page(page_id, false);
        pages.push_back(page_id);
    }

    return pages;
}

Status BPlusTree::reclaim_all_pages() {
    std::lock_guard<std::mutex> wlock(write_mutex_);
    for (page_id_t page_id : collect_tree_pages()) {
        buffer_pool_->delete_page(page_id);
    }

    // Bypass change_root deliberately: the caller is destroying the tree and
    // persists the removal itself (firing the callback here could deadlock a
    // caller that already holds its own lock).
    root_page_id_.store(INVALID_PAGE_ID, std::memory_order_relaxed);
    return Status::Ok();
}

Status BPlusTree::flush_all_pages() {
    for (page_id_t page_id : collect_tree_pages()) {
        if (!buffer_pool_->flush_page(page_id)) {
            return Status::IOError("Failed to flush index page " +
                                   std::to_string(page_id));
        }
    }
    return Status::Ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Iterator Support
// ─────────────────────────────────────────────────────────────────────────────

BPlusTreeIterator BPlusTree::lower_bound(KeyType key) {
    Page* leaf_page = descend_to_leaf_read(key);
    if (leaf_page == nullptr) {
        return end();
    }

    BPTreeLeafPage leaf(leaf_page);
    bool found;
    uint32_t idx = leaf.find_key_index(key, &found);
    page_id_t leaf_id = leaf_page->page_id();
    page_id_t next_leaf = leaf.next_leaf_id();
    uint32_t nkeys = leaf.num_keys();
    leaf_page->runlatch();
    buffer_pool_->unpin_page(leaf_id, false);

    if (idx >= nkeys) {
        // Position falls past this leaf; start at the next one, if any.
        if (next_leaf == INVALID_PAGE_ID) {
            return end();
        }
        return BPlusTreeIterator(this, next_leaf, 0);
    }
    return BPlusTreeIterator(this, leaf_id, idx);
}

BPlusTreeIterator BPlusTree::begin() {
    Page* page = latch_root_read();
    if (page == nullptr) {
        return end();
    }

    // Descend to the leftmost leaf, hand-over-hand.
    while (true) {
        BPTreePage bp_page(page);
        if (bp_page.is_leaf()) {
            BPTreeLeafPage leaf(page);
            page_id_t leaf_id = page->page_id();
            uint32_t nkeys = leaf.num_keys();
            page->runlatch();
            buffer_pool_->unpin_page(leaf_id, false);
            if (nkeys == 0) {
                return end();
            }
            return BPlusTreeIterator(this, leaf_id, 0);
        }

        BPTreeInternalPage internal(page);
        page_id_t next = internal.child_at(0);
        Page* child = buffer_pool_->fetch_page(next);
        if (child == nullptr) {
            page_id_t pid = page->page_id();
            page->runlatch();
            buffer_pool_->unpin_page(pid, false);
            return end();
        }
        child->rlatch();
        page_id_t pid = page->page_id();
        page->runlatch();
        buffer_pool_->unpin_page(pid, false);
        page = child;
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

// Each accessor reads the positioned leaf under its READ latch, so it never
// races a concurrent writer or observes a torn page. The (page_id, index)
// position is resolved fresh each call; a concurrent split/merge between calls
// can still shift what that position denotes, so callers wanting a consistent
// bulk view should prefer range_scan.
BPlusTreeIterator::KeyType BPlusTreeIterator::key() const {
    if (is_end()) return 0;

    Page* page = tree_->buffer_pool()->fetch_page(leaf_page_id_);
    if (page == nullptr) return 0;
    page->rlatch();
    KeyType k = BPTreeLeafPage(page).key_at(index_);
    page->runlatch();
    tree_->buffer_pool()->unpin_page(leaf_page_id_, false);
    return k;
}

BPlusTreeIterator::ValueType BPlusTreeIterator::value() const {
    if (is_end()) return ValueType();

    Page* page = tree_->buffer_pool()->fetch_page(leaf_page_id_);
    if (page == nullptr) return ValueType();
    page->rlatch();
    ValueType v = BPTreeLeafPage(page).value_at(index_);
    page->runlatch();
    tree_->buffer_pool()->unpin_page(leaf_page_id_, false);
    return v;
}

std::pair<BPlusTreeIterator::KeyType, BPlusTreeIterator::ValueType>
BPlusTreeIterator::operator*() const {
    if (is_end()) return {0, ValueType()};

    Page* page = tree_->buffer_pool()->fetch_page(leaf_page_id_);
    if (page == nullptr) return {0, ValueType()};
    page->rlatch();
    BPTreeLeafPage leaf(page);
    auto result = std::make_pair(leaf.key_at(index_), leaf.value_at(index_));
    page->runlatch();
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
    page->rlatch();
    BPTreeLeafPage leaf(page);

    ++index_;
    page_id_t next_position = leaf_page_id_;
    uint32_t next_index = index_;
    if (index_ >= leaf.num_keys()) {
        // Advance to the next leaf (INVALID_PAGE_ID naturally becomes end()).
        next_position = leaf.next_leaf_id();
        next_index = 0;
    }

    page->runlatch();
    tree_->buffer_pool()->unpin_page(leaf_page_id_, false);
    leaf_page_id_ = next_position;
    index_ = next_index;
    return *this;
}

BPlusTreeIterator BPlusTreeIterator::operator++(int) {
    BPlusTreeIterator tmp = *this;
    ++(*this);
    return tmp;
}

}  // namespace entropy
