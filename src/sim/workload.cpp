/**
 * @file workload.cpp
 * @brief Randomized workload, oracle bookkeeping, and invariant checks.
 */

#include "sim/workload.hpp"

#include <algorithm>
#include <span>
#include <utility>

#include "storage/table_heap.hpp"
#include "storage/tuple.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction/version_store.hpp"

namespace entropy::sim {

namespace {

constexpr size_t kNameWidth = 24;  // constant -> fixed-size tuples, stable RIDs

std::vector<char> tuple_bytes(const Tuple &t) {
  return std::vector<char>(t.as_span().begin(), t.as_span().end());
}

/// Build a fixed-width, distinct-per-call payload. The 8-hex prefix of the
/// monotonic @p unique_id guarantees no two rows ever share bytes.
Tuple make_row(uint64_t unique_id, std::mt19937_64 &rng, const Schema &schema) {
  const auto id_val = static_cast<int32_t>(rng());
  static constexpr char kHex[] = "0123456789abcdef";
  static constexpr char kAlpha[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string name;
  name.reserve(kNameWidth);
  for (int i = 7; i >= 0; --i) {
    name.push_back(kHex[(unique_id >> (i * 4)) & 0xF]);
  }
  for (size_t i = 8; i < kNameWidth; ++i) {
    name.push_back(kAlpha[rng() % 64]);
  }
  return Tuple({TupleValue(id_val), TupleValue(name)}, schema);
}

/// Read the raw record bytes stored at @p rid, or empty when the slot holds no
/// record. TableHeap::get_tuple is exactly a physical slot read (fetch page,
/// get_record, copy) with no MVCC/schema interpretation, which is what the
/// post-recovery check wants.
std::vector<char> slot_bytes(TableHeap &heap, const RID &rid) {
  Tuple tuple;
  if (heap.get_tuple(rid, &tuple).ok()) {
    return tuple_bytes(tuple);
  }
  return {};
}

}  // namespace

Schema make_sim_schema() {
  std::vector<Column> columns = {
      Column("id", TypeId::INTEGER),
      Column("name", TypeId::VARCHAR, 64),
  };
  return Schema(std::move(columns));
}

size_t RandomWorkload::run(const WorkloadContext &ctx, std::mt19937_64 &rng,
                           Oracle &oracle, size_t num_txns,
                           bool leave_in_flight) {
  size_t ops = 0;
  std::vector<RID> live;  // committed RIDs available to update/delete

  // Pick a random already-committed row to update/delete. Returns the map entry
  // (rid + current bytes) or null if the chosen slot is no longer committed.
  // Only called when `live` is non-empty.
  auto pick_committed =
      [&]() -> const std::pair<const RID, std::vector<char>> * {
    const RID rid = live[rng() % live.size()];
    auto it = oracle.committed().find(rid);
    return it == oracle.committed().end() ? nullptr : &*it;
  };

  for (size_t t = 0; t < num_txns; ++t) {
    const bool in_flight = leave_in_flight && (t + 1 == num_txns);

    Transaction *txn = ctx.tm->begin();
    if (txn == nullptr) {
      continue;
    }

    const size_t op_count = 1 + (rng() % 4);
    // Per-transaction staging, kept in EXECUTION order: a DELETE and a
    // slot-reusing INSERT can hit the same RID, so the oracle must replay the
    // effects in the order they ran, not grouped by kind.
    enum class Kind { kInsert, kUpdate, kDelete };
    struct Effect {
      Kind kind;
      RID rid;
      std::vector<char> bytes;
    };
    std::vector<Effect> staged;

    for (size_t o = 0; o < op_count; ++o) {
      const uint32_t choice = static_cast<uint32_t>(rng() % 100);
      const bool can_modify = !live.empty();

      if (choice < 55 || !can_modify) {
        // INSERT
        Tuple row = make_row(next_row_id_++, rng, *ctx.schema);
        RID rid;
        if (!ctx.heap->insert_tuple(row, &rid).ok()) {
          continue;
        }
        std::vector<char> bytes = tuple_bytes(row);
        ctx.tm->log_insert(txn, ctx.table_oid, rid, bytes);
        if (ctx.version_store != nullptr) {
          (void)ctx.version_store->on_insert(txn, rid);
        }
        staged.push_back({Kind::kInsert, rid, std::move(bytes)});
        if (in_flight) {
          oracle.note_loser(rid);
        }
        ++ops;
      } else if (choice < 80) {
        // UPDATE a committed row in place (same width -> stable RID).
        const auto *entry = pick_committed();
        if (entry == nullptr) {
          continue;
        }
        const RID rid = entry->first;
        const std::vector<char> &old_bytes = entry->second;
        Tuple row = make_row(next_row_id_++, rng, *ctx.schema);
        if (!ctx.heap->update_tuple(row, rid).ok()) {
          continue;
        }
        std::vector<char> new_bytes = tuple_bytes(row);
        ctx.tm->log_update(txn, ctx.table_oid, rid, old_bytes, new_bytes);
        if (ctx.version_store != nullptr) {
          (void)ctx.version_store->on_update(txn, rid,
                                             std::span<const char>(old_bytes));
        }
        staged.push_back({Kind::kUpdate, rid, std::move(new_bytes)});
        ++ops;
      } else {
        // DELETE a committed row.
        const auto *entry = pick_committed();
        if (entry == nullptr) {
          continue;
        }
        const RID rid = entry->first;
        const std::vector<char> &old_bytes = entry->second;
        if (!ctx.heap->delete_tuple(rid).ok()) {
          continue;
        }
        ctx.tm->log_delete(txn, ctx.table_oid, rid, old_bytes);
        if (ctx.version_store != nullptr) {
          (void)ctx.version_store->on_delete(txn, rid,
                                             std::span<const char>(old_bytes));
        }
        staged.push_back({Kind::kDelete, rid, {}});
        ++ops;
      }
    }

    if (in_flight) {
      // Leave the transaction open: the process "crashes" mid-transaction.
      // Nothing is applied to the oracle; recovery must roll it all back.
      return ops;
    }

    // Commit vs abort (seeded). Abort discards the staged effects.
    const bool do_commit = (rng() % 1000) >= abort_ppk_;
    if (do_commit) {
      ctx.tm->commit(txn);
      for (Effect &e : staged) {
        switch (e.kind) {
        case Kind::kInsert:
          oracle.commit_insert(e.rid, std::move(e.bytes));
          if (std::find(live.begin(), live.end(), e.rid) == live.end()) {
            live.push_back(e.rid);
          }
          break;
        case Kind::kUpdate:
          oracle.commit_update(e.rid, std::move(e.bytes));
          break;
        case Kind::kDelete:
          oracle.commit_delete(e.rid);
          live.erase(std::remove(live.begin(), live.end(), e.rid), live.end());
          break;
        }
      }
    } else {
      ctx.tm->abort(txn);
      // Aborted effects vanish; oracle and `live` are unchanged.
    }
  }
  return ops;
}

void check_invariants(const WorkloadContext &recovered, const Oracle &oracle,
                      std::vector<std::string> &failures) {
  TableHeap &heap = *recovered.heap;

  // 1. Every committed row is present and byte-for-byte correct.
  bool committed_ok = true;
  for (const auto &[rid, expected] : oracle.committed()) {
    if (slot_bytes(heap, rid) != expected) {
      committed_ok = false;
      break;
    }
  }
  if (!committed_ok) {
    failures.emplace_back("committed_present");
  }

  // 2. No loser or committed-deleted RID leaves visible bytes behind. A RID
  //    that a later committed insert reused is excluded (checked positively
  //    above); everything else must read empty.
  bool clean = true;
  auto absent = [&](const RID &rid) {
    if (oracle.committed().find(rid) != oracle.committed().end()) {
      return true;  // reused by a committed row
    }
    return slot_bytes(heap, rid).empty();
  };
  for (const RID &rid : oracle.losers()) {
    if (!absent(rid)) {
      clean = false;
      break;
    }
  }
  if (clean) {
    for (const RID &rid : oracle.deleted()) {
      if (!absent(rid)) {
        clean = false;
        break;
      }
    }
  }
  if (!clean) {
    failures.emplace_back("no_uncommitted_visible");
  }
}

}  // namespace entropy::sim
