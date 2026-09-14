#pragma once

#include "pg_executor.hxx"
#include "wire_protocol.hxx"

#include "nuraft.hxx"

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace ariabc_pg {

/*
 * F1: per-Raft-entry completion tracker.
 *
 * Tracks how many items within each committed application entry have reached
 * a terminal PostgreSQL commit.  The contiguous durable applied prefix advances
 * only when every item of every earlier entry is terminal (no gaps allowed).
 *
 * All accesses are protected by pg_state_machine::tracker_mu_.
 */
struct entry_tracker_record {
    uint32_t total_items    = 0;  /* expected item count (from commit()) */
    uint32_t terminal_count = 0;  /* distinct ordinals that have reached PG commit */
    std::vector<bool> terminal_item; /* indexed by item_ordinal; true when that ordinal is done */

    /*
     * Mark ordinal @ord as terminal.  Returns true iff this was the first time
     * this ordinal was marked (i.e. we actually incremented terminal_count).
     * Silently ignores out-of-range ordinals to be safe.
     */
    bool mark_ordinal(uint32_t ord) {
        if (total_items == 0 || ord >= total_items) return false;
        if (static_cast<size_t>(ord) >= terminal_item.size()) {
            terminal_item.resize(total_items, false);
        }
        if (terminal_item[ord]) return false; /* already marked */
        terminal_item[ord] = true;
        ++terminal_count;
        return true;
    }

    bool is_complete() const {
        return total_items > 0 && terminal_count >= total_items;
    }
};

struct result_item_key {
    uint64_t result_token = 0;
    uint32_t ordinal = 0;

    result_item_key() = default;
    result_item_key(uint64_t token, uint32_t ord)
        : result_token(token), ordinal(ord) {}

    bool operator==(const result_item_key& other) const {
        return result_token == other.result_token && ordinal == other.ordinal;
    }
};

struct result_item_key_hash {
    size_t operator()(const result_item_key& key) const {
        return std::hash<uint64_t>{}(key.result_token) ^
               (std::hash<uint32_t>{}(key.ordinal) << 1);
    }
};

class pg_state_machine : public nuraft::state_machine {
public:
    pg_state_machine(int node_id, const db_options& db_opt, const kafka_options& k_opt);
    ~pg_state_machine();

    bool ensure_bcdb_initialized() { return executor_.ensure_bcdb_initialized(); }

    /*
     * P0 #14: Safe synchronous startup sequencing.
     *
     * Must be called AFTER durable Raft state manager is opened and validated,
     * and BEFORE the raft_launcher starts delivering commit()s.
     *
     * This function:
     *   1. Validates that PostgreSQL schema version and epoch anchor match the
     *      Raft identity (same epoch hex stored in durable Raft state).
     *   2. Queries raft_apply_item to find the conservative contiguous applied
     *      prefix (largest log_index N s.t. every item ordinal 0..K-1 is
     *      APPLIED_OK or APPLIED_ERROR, with no CLAIMED gaps).
     *   3. Calls seed_durable_prefix(N) to initialize the tracker before any
     *      Raft commit() is replayed.
     *
     * Returns the seeded prefix (0 if none found or safe mode is not enabled).
     * Throws std::runtime_error on any validation failure in safe mode.
     */
    uint64_t safe_sync_startup(uint64_t durable_log_start_index = 0);
    pg_executor_stats executor_stats() const { return executor_.stats(); }
    kafka_producer_stats kafka_stats() const { return executor_.kafka_stats(); }
    bool admission_control_blocked() const { return executor_.admission_control_blocked(); }
    bool wait_for_admission_drain(uint64_t max_wait_ns) {
        return executor_.wait_for_admission_drain(max_wait_ns);
    }

    // Bypass-Raft path: directly enqueue a request with a caller-provided
    // monotonic sequence number, skipping Raft log deserialization.
    void direct_enqueue(const std::string& req_id, const std::string& sql, uint64_t seq) {
        client_api_request_item item;
        item.req_id = req_id;
        item.sql = sql;
        item.has_assigned_det_seq = true;
        item.assigned_det_seq = seq;
        register_result_batch(seq, std::vector<client_api_request_item>{item});
        {
            std::lock_guard<std::mutex> lk(tracker_mu_);
            entry_tracker_record& rec = entry_tracker_[seq];
            if (rec.total_items == 0) {
                rec.total_items = 1;
                rec.terminal_item.assign(1, false);
            }
        }
        std::vector<std::string> req_ids{req_id};
        std::vector<std::string> sqls{sql};
        std::vector<uint64_t> assigned_det_seqs{seq};
        std::vector<uint8_t> assigned_det_seq_valid{1};
        executor_.enqueue_batch(req_ids, sqls, -1, seq, "", {}, assigned_det_seqs,
                                assigned_det_seq_valid);
    }
    void direct_enqueue_batch(const std::vector<client_api_request_item>& items, uint64_t first_seq) {
        if (items.empty()) return;
        const uint64_t last_seq = first_seq + static_cast<uint64_t>(items.size() - 1);
        register_result_batch(last_seq, items);
        {
            std::lock_guard<std::mutex> lk(tracker_mu_);
            entry_tracker_record& rec = entry_tracker_[last_seq];
            if (rec.total_items == 0) {
                rec.total_items = static_cast<uint32_t>(items.size());
                rec.terminal_item.assign(rec.total_items, false);
            }
        }
        std::vector<std::string> req_ids;
        std::vector<std::string> sqls;
        std::vector<uint64_t> assigned_det_seqs;
        std::vector<uint8_t> assigned_det_seq_valid;
        req_ids.reserve(items.size());
        sqls.reserve(items.size());
        assigned_det_seqs.reserve(items.size());
        assigned_det_seq_valid.reserve(items.size());
        for (size_t i = 0; i < items.size(); ++i) {
            const auto& item = items[i];
            req_ids.push_back(item.req_id);
            sqls.push_back(item.sql);
            assigned_det_seqs.push_back(first_seq + static_cast<uint64_t>(i));
            assigned_det_seq_valid.push_back(1);
        }
        executor_.enqueue_batch(req_ids, sqls, -1, last_seq, "", {}, assigned_det_seqs,
                                assigned_det_seq_valid);
    }

    nuraft::ptr<nuraft::buffer> commit(const nuraft::ulong log_idx,
                                       nuraft::buffer& data) override;

    void commit_config(const nuraft::ulong log_idx,
                       nuraft::ptr<nuraft::cluster_config>& new_conf) override;

    bool apply_snapshot(nuraft::snapshot& s) override;
    nuraft::ptr<nuraft::snapshot> last_snapshot() override;

    /*
     * F1: last_commit_index() returns the contiguous durable applied prefix:
     * the largest log_idx N such that every application entry <= N has had
     * all its items reach a top-level PostgreSQL commit.
     *
     * This differs from the Raft committed index; it trails until workers finish.
     */
    nuraft::ulong last_commit_index() override;

    void create_snapshot(nuraft::snapshot& s,
                         nuraft::async_result<bool>::handler_type& when_done) override;
    bool wait_for_result(uint64_t result_token,
                         int timeout_ms,
                         std::string* failure_reason = nullptr);
    bool wait_for_result_id(const std::string& req_id,
                            int timeout_ms,
                            std::string* failure_reason = nullptr);
    struct result_tracker_profile {
        size_t req_id_map_size_current = 0;
        size_t req_id_map_size_max = 0;
        size_t result_token_map_size_current = 0;
        size_t result_token_map_size_max = 0;
    };
    result_tracker_profile result_profile() const;

    /*
     * F1: Called by worker after its top-level PostgreSQL commit succeeds
     * (or after replay completes).  Marks the item_ordinal for log_idx as
     * terminal.  A given (log_idx, item_ordinal) pair is counted at most once;
     * duplicate calls are safe no-ops.  Advances durable_applied_prefix_ if
     * all items for log_idx are now terminal.
     */
    void note_item_applied(uint64_t log_idx, uint32_t item_ordinal);
    void note_item_failed(uint64_t log_idx, uint32_t item_ordinal, const std::string& reason);

    /*
     * F1: Seed the tracker with a known durable prefix (from ledger scan on
     * startup).  Must be called before NuRaft starts delivering commit()s.
     */
    void seed_durable_prefix(uint64_t prefix);

#ifdef BUILDING_UNIT_TESTS
    struct result_tracker_debug_counts {
        size_t pending_result_counts = 0;
        size_t result_token_by_req_id = 0;
        size_t completed_result_tokens = 0;
        size_t failed_result_tokens = 0;
        size_t outstanding_waiters = 0;
        size_t completed_at_ns = 0;
    };

    void test_register_result_batch(uint64_t result_token,
                                    const std::vector<client_api_request_item>& items) {
        register_result_batch(result_token, items);
    }
    void test_note_result_applied(uint64_t result_token) {
        note_result_applied(result_token);
    }
    void test_note_result_item_applied(uint64_t result_token, uint32_t ordinal) {
        note_result_item_applied(result_token, ordinal);
    }
    void test_note_result_failed(uint64_t result_token, const std::string& reason) {
        note_result_failed(result_token, reason);
    }
    void test_note_result_item_failed(uint64_t result_token,
                                      uint32_t ordinal,
                                      const std::string& reason) {
        note_result_item_failed(result_token, ordinal, reason);
    }
    void test_force_result_cleanup();
    result_tracker_debug_counts test_result_tracker_counts() const;
#endif

private:
    void register_result_batch(uint64_t result_token, size_t item_count);
    void register_result_batch(uint64_t result_token,
                               const std::vector<client_api_request_item>& items);
    void note_result_applied(uint64_t result_token);
    void note_result_item_applied(uint64_t result_token, uint32_t ordinal);
    void note_result_failed(uint64_t result_token, const std::string& reason);
    void note_result_item_failed(uint64_t result_token,
                                 uint32_t ordinal,
                                 const std::string& reason);
    void consume_result_waiter_locked(const std::string& req_id, uint64_t result_token);
    void consume_result_item_waiter_locked(const std::string& req_id,
                                           const result_item_key& key);
    uint64_t initialize_committed_det_seq_locked();
    uint64_t next_committed_det_seq_locked();
    void cleanup_result_tokens_locked(uint64_t now_ns, bool force);
    void maybe_cleanup_result_tokens_locked(uint64_t now_ns);
    void update_result_tracker_highwater_locked();

    /*
     * F1: try to advance durable_applied_prefix_ over any newly-complete entries.
     * Caller must hold tracker_mu_.
     */
    void maybe_advance_prefix_locked();

    pg_executor executor_;

    /*
     * last_committed_idx_ is the Raft-committed log index (set in commit()).
     * durable_applied_prefix_ is the contiguous applied index (set by workers).
     * NuRaft queries last_commit_index() → we return durable_applied_prefix_.
     */

    std::atomic<uint64_t> durable_applied_prefix_{0};

    /* F1: per-entry tracker map, ordered by log_idx. */
    std::mutex tracker_mu_;
    std::map<uint64_t, entry_tracker_record> entry_tracker_;

    nuraft::ptr<nuraft::snapshot> last_snapshot_;
    std::mutex last_snapshot_lock_;
    db_options db_opt_;
    int node_id_;

    mutable std::mutex result_mu_;
    std::condition_variable result_cv_;
    std::unordered_map<uint64_t, size_t> pending_result_counts_;
    std::unordered_map<std::string, uint64_t> result_token_by_req_id_;
    std::unordered_map<std::string, result_item_key> result_item_by_req_id_;
    std::unordered_map<uint64_t, std::vector<std::string>> req_ids_by_result_token_;
    std::unordered_set<uint64_t> completed_result_tokens_;
    std::unordered_map<uint64_t, std::unordered_set<uint32_t>> completed_result_ordinals_;
    std::unordered_map<uint64_t, std::string> failed_result_tokens_;
    std::unordered_map<uint64_t, std::unordered_map<uint32_t, std::string>> failed_result_ordinals_;
    std::unordered_map<uint64_t, uint32_t> outstanding_waiters_;
    std::unordered_map<uint64_t, uint64_t> completed_at_ns_;
    uint64_t last_result_cleanup_ns_ = 0;
    size_t req_id_map_size_max_ = 0;
    size_t result_token_map_size_max_ = 0;

    std::mutex committed_det_seq_mu_;
    bool committed_det_seq_initialized_ = false;
    uint64_t next_committed_det_seq_ = 0;
};

} // namespace ariabc_pg
