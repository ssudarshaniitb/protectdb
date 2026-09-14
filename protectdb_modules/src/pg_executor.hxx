#pragma once

#include "kafka_console.hxx"

#include <poll.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

struct pg_conn;
typedef struct pg_conn PGconn;

namespace ariabc_pg {

struct pg_executor_stats {
    uint64_t enqueued = 0;
    uint64_t dequeued = 0;
    uint64_t q_wait_ns = 0;
    uint64_t queue_delay_ns = 0;
    uint64_t queue_delay_dequeue_ns = 0;
    uint64_t queue_delay_exec_start_ns = 0;
    uint64_t backlog_cur = 0;
    uint64_t inflight_cur = 0;
    uint64_t inflight_max = 0;
    double inflight_avg = 0.0;
    uint64_t inflight_at_cap_ns = 0;
    uint64_t delayed_cur = 0;

    uint64_t conn_acquire_calls = 0;
    uint64_t conn_acquire_wait_ns = 0;

    uint64_t exec_calls = 0;
    uint64_t exec_ns = 0;
    uint64_t pg_query_ns = 0;
    uint64_t threaded_workers_configured = 0;
    uint64_t threaded_workers_created = 0;
    uint64_t owned_pg_connections = 0;
    uint64_t concurrent_pqexec_cur = 0;
    uint64_t concurrent_pqexec_max = 0;
    uint64_t overlapping_pqexec_intervals = 0;
    uint64_t result_format_ns = 0;
    uint64_t retryable_sqlstate_40001 = 0;
    uint64_t retryable_sqlstate_40P01 = 0;
    uint64_t retryable_sqlstate_57014 = 0;
    uint64_t retry_attempts_total = 0;
    uint64_t retry_exhausted_total = 0;

    uint64_t kafka_flush_calls = 0;
    uint64_t kafka_payload_bytes = 0;
    uint64_t kafka_build_payload_ns = 0;
    uint64_t kafka_send_ns = 0;
    uint64_t kafka_batch_records = 0;
    uint64_t kafka_batch_records_max = 0;
    uint64_t kafka_batch_dwell_ns = 0;
    uint64_t kafka_batch_dwell_max_ns = 0;
    uint64_t kafka_flush_reason_records = 0;
    uint64_t kafka_flush_reason_bytes = 0;
    uint64_t kafka_flush_reason_age = 0;
    uint64_t kafka_flush_reason_idle = 0;
    uint64_t kafka_flush_reason_final = 0;

    uint64_t kafka_batch_records_bin_1 = 0;
    uint64_t kafka_batch_records_bin_2_15 = 0;
    uint64_t kafka_batch_records_bin_16_63 = 0;
    uint64_t kafka_batch_records_bin_64_255 = 0;
    uint64_t kafka_batch_records_bin_256_plus = 0;

    uint64_t kafka_batch_bytes_bin_le_1k = 0;
    uint64_t kafka_batch_bytes_bin_1k_10k = 0;
    uint64_t kafka_batch_bytes_bin_10k_100k = 0;
    uint64_t kafka_batch_bytes_bin_100k_plus = 0;

    uint64_t kafka_batch_dwell_bin_le_1ms = 0;
    uint64_t kafka_batch_dwell_bin_1_5ms = 0;
    uint64_t kafka_batch_dwell_bin_5_20ms = 0;
    uint64_t kafka_batch_dwell_bin_20_100ms = 0;
    uint64_t kafka_batch_dwell_bin_100ms_plus = 0;

    uint64_t kafka_flush_backlog_bin_0 = 0;
    uint64_t kafka_flush_backlog_bin_1_15 = 0;
    uint64_t kafka_flush_backlog_bin_16_63 = 0;
    uint64_t kafka_flush_backlog_bin_64_255 = 0;
    uint64_t kafka_flush_backlog_bin_256_plus = 0;

    uint64_t kafka_flush_inflight_bin_0 = 0;
    uint64_t kafka_flush_inflight_bin_1_15 = 0;
    uint64_t kafka_flush_inflight_bin_16_63 = 0;
    uint64_t kafka_flush_inflight_bin_64_255 = 0;
    uint64_t kafka_flush_inflight_bin_256_plus = 0;

    uint64_t queue_depth_samples = 0;
    uint64_t queue_depth_sum = 0;
    uint64_t queue_depth_max = 0;
    uint64_t queue_depth_bin_le_16 = 0;
    uint64_t queue_depth_bin_17_64 = 0;
    uint64_t queue_depth_bin_65_256 = 0;
    uint64_t queue_depth_bin_gt_256 = 0;
    uint64_t queue_overload_enter = 0;
    uint64_t queue_overload_exit = 0;
    uint64_t queue_high_watermark = 0;
    uint64_t queue_low_watermark = 0;
    uint64_t queue_depth_cur = 0;
    int queue_overloaded = 0;
    int bcdb_init_enabled = 0;
    int bcdb_block_size = 0;
    int bcdb_init_arg_size_configured = 0;
    uint64_t det_block_batches = 0;
    uint64_t det_block_items = 0;
    uint64_t det_block_min = 0;
    uint64_t det_block_max = 0;
    uint64_t det_block_bin_1 = 0;
    uint64_t det_block_bin_2_15 = 0;
    uint64_t det_block_bin_16_63 = 0;
    uint64_t det_block_bin_64_127 = 0;
    uint64_t det_block_bin_128_plus = 0;
    uint64_t det_block_fallbacks = 0;
    uint64_t det_block_skipped_readonly = 0;
    uint64_t ready_det_results_max = 0;
    uint64_t ordered_emit_wait_ns = 0;
    uint64_t kafka_immediate_records = 0;
    uint64_t ordered_apply_wait_ns = 0;
    uint64_t ordered_apply_pending_max = 0;
    int det_raw_compat_mode = 0;
    int det_prefixed_direct_parallel = 0;
    int det_completion_only_success = 0;
    uint64_t det_raw_compat_activations = 0;
    std::string det_raw_compat_first_req_id;
    std::string det_raw_compat_first_sql_prefix;

    /* Block fastpath visibility (distinguish PG vs server vs gateway stalls). */
    uint64_t det_fastpath_blocks_submitted = 0;   /* blocks handed to PG */
    uint64_t det_fastpath_blocks_returned = 0;    /* blocks PG responded to */
    uint64_t det_fastpath_blocks_emitted = 0;     /* blocks emitted to Kafka */
    uint64_t det_fastpath_ready_blocks_max = 0;   /* peak in-flight ready blocks */
    uint64_t det_fastpath_last_submitted_block_id = 0;
    uint64_t det_fastpath_last_returned_block_id = 0;
    uint64_t det_fastpath_last_returned_block_seq = 0;
    uint64_t det_fastpath_last_emitted_seq = 0;
    uint64_t det_fastpath_submit_to_return_max_us = 0; /* max round-trip µs PG->response */
    uint64_t det_fastpath_send_failures = 0;
    uint64_t det_fastpath_requeues = 0;
    uint64_t det_fastpath_reconnect_failures = 0;

    // Telemetry fields for result flush and delivery pending
    uint64_t result_flush_count = 0;
    uint64_t result_flush_records_total = 0;
    uint64_t result_flush_records_max = 0;
    uint64_t result_flush_due_to_record_cap = 0;
    uint64_t result_flush_due_to_byte_cap = 0;
    uint64_t result_flush_due_to_age = 0;
    uint64_t result_flush_due_to_idle = 0;
    uint64_t result_flush_due_to_error = 0;
    uint64_t result_flush_due_to_shutdown = 0;
    uint64_t result_flush_while_delivery_pending_gt_8 = 0;
    uint64_t result_flush_while_delivery_pending_gt_32 = 0;
    uint64_t kafka_delivery_pending_current = 0;
    uint64_t kafka_delivery_pending_max = 0;
    uint64_t kafka_delivery_pending_over_8_events = 0;
    uint64_t kafka_delivery_pending_over_32_events = 0;
    int kafka_async_publisher_enabled = 0;
    uint64_t kafka_async_publisher_queue_max = 0;
};

struct db_options {
    std::string host = "localhost";
    std::string port;
    std::string dbname;
    std::string user = "postgres";
    std::string password;

    // Gateway db type (0=default, 1=det, 2=nondet).
    int db_type = 0;

    // 1=bcdb, 2=safedb, others=plain postgres.
    int safedb = 0;

    // "threaded": existing behavior (N worker threads for N connections).
    // "event": single-thread reactor (async libpq with poll()).
    std::string exec_mode = "threaded";

    int conn_pool_size = 20;
    int bcdb_init_block_size = 0;
    int max_retries = 10;
    int retry_backoff_ms = 100;

    std::string raft_apply_ledger_mode = "off";
    std::string raft_epoch_hex;
};

struct kafka_options {
    std::string bootstrap;
    std::string result_topic = "topic2";
    std::string result_sig_key;
};

class pg_executor {
public:
    static int configured_batch_delay_us();
    static int override_batch_delay_us();

    using completion_callback = std::function<void(uint64_t, uint32_t)>;
    using failure_callback = std::function<void(uint64_t, uint32_t, const std::string&)>;

    pg_executor(int node_id,
                const db_options& db_opt,
                const kafka_options& k_opt,
                completion_callback on_task_applied = completion_callback(),
                failure_callback on_task_failed = failure_callback());
    ~pg_executor();

    struct task {
        std::string req_id;
        std::string sql;
        int leader_node_hint = -1;
        uint64_t raft_log_idx = 0;
        bool has_assigned_det_seq = false;
        uint64_t assigned_det_seq = 0;
        uint64_t dispatch_seq = 0;
        uint64_t enqueue_ns = 0;
        uint64_t exec_begin_ns = 0; // set on first attempt start; preserved across retries (event mode)
        int attempt = 0;
        uint32_t raft_item_ordinal = 0;
        uint32_t raft_item_count = 0;
        std::string entry_digest;
        std::string item_digest;
    };

    struct ConfirmedResult {
        uint64_t raft_log_index = 0;
        uint32_t raft_item_ordinal = 0;
        std::string terminal_digest;
        std::string terminal_state;
        std::string failure_sqlstate;
        std::string failure_class;
        bool failure_retryable = false;
        std::string payload;
        int format_version = 1;
    };

    ConfirmedResult accept_safe_confirmed_result(const task& t, const std::string& raw_backend_result);

    void enqueue(const std::string& req_id,
                 const std::string& sql,
                 int leader_node_hint,
                 uint64_t raft_log_idx);
    void enqueue_batch(const std::vector<std::string>& req_ids,
                       const std::vector<std::string>& sqls,
                       int leader_node_hint,
                       uint64_t raft_log_idx,
                       const std::string& entry_digest_hex = "",
                       const std::vector<std::string>& item_digests_hex = {},
                       const std::vector<uint64_t>& assigned_det_seqs = {},
                       const std::vector<uint8_t>& assigned_det_seq_valid = {});

    bool verify_and_register_entry_manifest(uint64_t log_idx, const std::string& entry_digest_hex, const std::vector<std::string>& item_digests_hex);

    pg_executor_stats stats() const;

    kafka_producer_stats kafka_stats() const;

    void stop();

    bool admission_control_blocked() const;

    // Block until admission control is no longer engaged or `deadline_ns` from
    // now elapses (whichever comes first). Returns true if admission cleared
    // before the deadline. This replaces the 1ms sleep-poll on RPC threads and
    // lets the executor notify exactly when the queue drains.
    bool wait_for_admission_drain(uint64_t max_wait_ns);
    bool ensure_bcdb_initialized();
    int bcdb_block_size() const { return bcdb_block_size_; }

private:
    struct notice_state;
    static void notice_processor(void* arg, const char* message);

    enum class det_tx_state : uint8_t {
        QUEUED = 0,
        SIM_DONE = 1,
        APPLY_READY = 2,
        APPLIED = 3,
    };

    void worker_loop();
    void event_loop();

    PGconn* acquire_conn();
    void release_conn(PGconn* c);

    std::string exec_sql(PGconn* c, const std::string& sql, bool* is_error = nullptr);
    bool exec_det_block_batch(PGconn* c,
                              uint64_t tx_key_start,
                              const std::vector<task>& tasks,
                              std::vector<std::string>& out_results);
    bool initialize_bcdb();
    void ensure_bcdb_initialized_for_sql(const std::string& sql);
    uint64_t get_det_tx_seq(const task& t) const;
    bool get_det_tx_seq_valid(const task& t, uint64_t& out_seq) const;
    void det_mark_tx_state(uint64_t tx_seq, det_tx_state st);
    bool det_wait_for_apply_turn(uint64_t tx_seq);
    void det_finish_apply(uint64_t tx_seq);
    void notify_task_applied(uint64_t raft_log_idx, uint32_t item_ordinal);
    void notify_task_failed(uint64_t raft_log_idx, uint32_t item_ordinal, const std::string& reason);
    void mark_task_applied_ordered(uint64_t dispatch_seq, uint64_t raft_log_idx, uint32_t item_ordinal, uint64_t ready_ns);
    bool ensure_safe_ledger_terminal(PGconn* c, const task& t, const ConfirmedResult& confirmed);
    bool ensure_safe_nonterminal_failure(const task& t, const ConfirmedResult& confirmed);
    bool ensure_safe_outcome(PGconn* c, const task& t, const ConfirmedResult& confirmed);

    void record_det_block_batch(size_t size, bool fallback);
    bool wait_for_ordered_emit_turn(uint64_t dispatch_seq);
    void finish_ordered_emit(uint64_t dispatch_seq);

    enum class kafka_flush_reason : uint8_t {
        RECORDS,
        BYTES,
        AGE,
        IDLE,
        ERROR,
        FINAL,
    };

    struct kafka_result_record {
        std::string req_id;
        std::string result;
        uint64_t raft_log_idx = 0;
        int leader_hint = -1;
        std::string terminal_digest;
        uint64_t append_ns = 0;
        uint32_t raft_item_ordinal = 0;
        std::string terminal_state;
        int format_version = 1;
        uint64_t dispatch_seq = 0;
        uint64_t ready_ns = 0;
        size_t bytes = 0;
    };

    bool async_kafka_publisher_active() const;
    void enqueue_kafka_result(const task& t, const ConfirmedResult& confirmed);
    void kafka_publisher_loop();
    void publish_kafka_result_batch(std::vector<kafka_result_record>& batch,
                                    kafka_flush_reason reason);

    static bool is_event_mode(const std::string& mode);

    int node_id_;
    db_options db_opt_;
    std::string conninfo_;

    std::mutex pool_mu_;
    std::condition_variable pool_cv_;
    std::queue<PGconn*> pool_;
    std::vector<PGconn*> all_conns_;
    std::unordered_map<PGconn*, notice_state*> notice_state_by_conn_;
    std::vector<std::unique_ptr<notice_state>> notice_states_;

    kafka_options kafka_opt_;
    std::string result_sig_key_;
    kafka_console_producer kafka_prod_;
    bool kafka_enabled_;
    bool kafka_async_publisher_enabled_ = false;
    completion_callback on_task_applied_;
    failure_callback on_task_failed_;

    std::mutex kafka_pub_mu_;
    std::condition_variable kafka_pub_cv_;
    std::deque<kafka_result_record> kafka_pub_q_;
    std::thread kafka_pub_thread_;
    bool kafka_pub_stop_ = false;
    std::atomic<uint64_t> st_kafka_async_publisher_queue_max_{0};

    std::atomic<bool> stop_{false};
    std::mutex q_mu_;
    std::condition_variable q_cv_;
    std::queue<task> q_;
    std::map<uint64_t, task> det_preassigned_reorder_buf_;
    uint64_t next_det_preassigned_seq_ = 0;
    bool det_preassigned_seq_initialized_ = false;
    std::vector<std::thread> threads_;

    // Event-driven (reactor) mode.
    struct delayed_task {
        uint64_t deadline_ns = 0;
        task t;
    };
    struct delayed_cmp {
        bool operator()(const delayed_task& a, const delayed_task& b) const {
            // min-heap by deadline
            return a.deadline_ns > b.deadline_ns;
        }
    };
    std::priority_queue<delayed_task, std::vector<delayed_task>, delayed_cmp> delayed_;

    struct conn_state {
        PGconn* c = nullptr;
        int fd = -1;
        enum class state : uint8_t { IDLE = 0, SENDING = 1, READING = 2 } st = state::IDLE;
        task cur;
        bool has_task = false;
        bool has_det_block = false;
        uint64_t det_block_id = 0;
        uint64_t det_block_seq = 0;
        uint64_t det_block_tx_key_start = 0;
        std::vector<task> det_block_tasks;
        std::vector<uint8_t> det_block_backend_mask;
        uint64_t exec_start_ns = 0;
    };
    std::vector<conn_state> conns_;
    std::vector<std::queue<task>> conn_qs_;
    void push_task_ordered(task&& t);
    std::vector<pollfd> pfds_scratch_;
    std::thread event_thread_;
    bool event_mode_ = false;
    bool det_parallel_workers_ = false;
    bool det_raw_compat_mode_ = false;
    bool det_allow_raw_compat_ = false;
    bool det_prefixed_direct_parallel_ = false;
    bool det_completion_only_success_ = false;
    bool det_threaded_direct_no_preapply_wait_ = false;
    uint64_t det_next_block_id_ = 2;
    uint64_t det_block_tx_key_base_ = 0;
    uint64_t det_block_next_tx_key_ = 0;
    uint64_t det_block_next_backend_txid_ = 0;
    bool det_block_backend_txid_delta_set_ = false;
    int64_t det_block_backend_txid_delta_ = 0;
    PGconn* bcdb_ctrl_conn_ = nullptr;
    PGconn* manifest_conn_ = nullptr;
    std::mutex manifest_mu_;
    std::mutex bcdb_init_mu_;
    bool bcdb_init_done_ = false;
    bool bcdb_init_failed_ = false;
    int bcdb_block_size_ = 0;
    int det_block_parallel_ = 1;
    int det_block_pipeline_ = 1;
    size_t det_block_max_ = 2048;
    int wakeup_rfd_ = -1;
    int wakeup_wfd_ = -1;
    std::atomic<uint64_t> st_backlog_cur_{0};
    std::atomic<uint64_t> st_inflight_cur_{0};
    std::atomic<uint64_t> st_inflight_max_{0};
    std::atomic<uint64_t> st_inflight_area_ns_{0};
    std::atomic<uint64_t> st_inflight_time_ns_{0};
    std::atomic<uint64_t> st_inflight_at_cap_ns_{0};
    std::atomic<uint64_t> st_delayed_cur_{0};

    std::atomic<uint64_t> st_enqueued_{0};
    std::atomic<uint64_t> st_dequeued_{0};
    std::atomic<uint64_t> st_q_wait_ns_{0};
    std::atomic<uint64_t> st_queue_delay_ns_{0};
    std::atomic<uint64_t> st_queue_delay_dequeue_ns_{0};
    std::atomic<uint64_t> st_queue_delay_exec_start_ns_{0};
    std::atomic<uint64_t> st_conn_acquire_calls_{0};
    std::atomic<uint64_t> st_conn_acquire_wait_ns_{0};
    std::atomic<uint64_t> st_exec_calls_{0};
    std::atomic<uint64_t> st_exec_ns_{0};
    std::atomic<uint64_t> st_pg_query_ns_{0};
    std::atomic<uint64_t> st_threaded_workers_configured_{0};
    std::atomic<uint64_t> st_threaded_workers_created_{0};
    std::atomic<uint64_t> st_owned_pg_connections_{0};
    std::atomic<uint64_t> st_concurrent_pqexec_cur_{0};
    std::atomic<uint64_t> st_concurrent_pqexec_max_{0};
    std::atomic<uint64_t> st_overlapping_pqexec_intervals_{0};
    std::atomic<uint64_t> st_result_format_ns_{0};
    std::atomic<uint64_t> st_retryable_sqlstate_40001_{0};
    std::atomic<uint64_t> st_retryable_sqlstate_40P01_{0};
    std::atomic<uint64_t> st_retryable_sqlstate_57014_{0};
    std::atomic<uint64_t> st_retry_attempts_total_{0};
    std::atomic<uint64_t> st_retry_exhausted_total_{0};
    std::atomic<uint64_t> st_kafka_flush_calls_{0};
    std::atomic<uint64_t> st_kafka_payload_bytes_{0};
    std::atomic<uint64_t> st_kafka_build_payload_ns_{0};
    std::atomic<uint64_t> st_kafka_send_ns_{0};
    std::atomic<uint64_t> st_kafka_batch_records_{0};
    std::atomic<uint64_t> st_kafka_batch_records_max_{0};
    std::atomic<uint64_t> st_kafka_batch_dwell_ns_{0};
    std::atomic<uint64_t> st_kafka_batch_dwell_max_ns_{0};
    std::atomic<uint64_t> st_kafka_flush_reason_records_{0};
    std::atomic<uint64_t> st_kafka_flush_reason_bytes_{0};
    std::atomic<uint64_t> st_kafka_flush_reason_age_{0};
    std::atomic<uint64_t> st_kafka_flush_reason_idle_{0};
    std::atomic<uint64_t> st_kafka_flush_reason_final_{0};

    std::atomic<uint64_t> st_kafka_batch_records_bin_1_{0};
    std::atomic<uint64_t> st_kafka_batch_records_bin_2_15_{0};
    std::atomic<uint64_t> st_kafka_batch_records_bin_16_63_{0};
    std::atomic<uint64_t> st_kafka_batch_records_bin_64_255_{0};
    std::atomic<uint64_t> st_kafka_batch_records_bin_256_plus_{0};

    std::atomic<uint64_t> st_kafka_batch_bytes_bin_le_1k_{0};
    std::atomic<uint64_t> st_kafka_batch_bytes_bin_1k_10k_{0};
    std::atomic<uint64_t> st_kafka_batch_bytes_bin_10k_100k_{0};
    std::atomic<uint64_t> st_kafka_batch_bytes_bin_100k_plus_{0};

    std::atomic<uint64_t> st_kafka_batch_dwell_bin_le_1ms_{0};
    std::atomic<uint64_t> st_kafka_batch_dwell_bin_1_5ms_{0};
    std::atomic<uint64_t> st_kafka_batch_dwell_bin_5_20ms_{0};
    std::atomic<uint64_t> st_kafka_batch_dwell_bin_20_100ms_{0};
    std::atomic<uint64_t> st_kafka_batch_dwell_bin_100ms_plus_{0};

    std::atomic<uint64_t> st_kafka_flush_backlog_bin_0_{0};
    std::atomic<uint64_t> st_kafka_flush_backlog_bin_1_15_{0};
    std::atomic<uint64_t> st_kafka_flush_backlog_bin_16_63_{0};
    std::atomic<uint64_t> st_kafka_flush_backlog_bin_64_255_{0};
    std::atomic<uint64_t> st_kafka_flush_backlog_bin_256_plus_{0};

    std::atomic<uint64_t> st_kafka_flush_inflight_bin_0_{0};
    std::atomic<uint64_t> st_kafka_flush_inflight_bin_1_15_{0};
    std::atomic<uint64_t> st_kafka_flush_inflight_bin_16_63_{0};
    std::atomic<uint64_t> st_kafka_flush_inflight_bin_64_255_{0};
    std::atomic<uint64_t> st_kafka_flush_inflight_bin_256_plus_{0};

    size_t queue_high_wm_ = 0;
    size_t queue_low_wm_ = 0;
    std::atomic<uint64_t> st_queue_depth_samples_{0};
    std::atomic<uint64_t> st_queue_depth_sum_{0};
    std::atomic<uint64_t> st_queue_depth_max_{0};
    std::atomic<uint64_t> st_queue_depth_bin_le_16_{0};
    std::atomic<uint64_t> st_queue_depth_bin_17_64_{0};
    std::atomic<uint64_t> st_queue_depth_bin_65_256_{0};
    std::atomic<uint64_t> st_queue_depth_bin_gt_256_{0};
    std::atomic<uint64_t> st_queue_overload_enter_{0};
    std::atomic<uint64_t> st_queue_overload_exit_{0};
    std::atomic<uint64_t> st_det_block_batches_{0};
    std::atomic<uint64_t> st_det_block_items_{0};
    std::atomic<uint64_t> st_det_block_min_{0};
    std::atomic<uint64_t> st_det_block_max_{0};
    std::atomic<uint64_t> st_det_block_bin_1_{0};
    std::atomic<uint64_t> st_det_block_bin_2_15_{0};
    std::atomic<uint64_t> st_det_block_bin_16_63_{0};
    std::atomic<uint64_t> st_det_block_bin_64_127_{0};
    std::atomic<uint64_t> st_det_block_bin_128_plus_{0};
    std::atomic<uint64_t> st_det_block_fallbacks_{0};
    std::atomic<uint64_t> st_det_block_skipped_readonly_{0};
    std::atomic<uint64_t> st_ready_det_results_max_{0};
    std::atomic<uint64_t> st_ordered_emit_wait_ns_{0};
    std::atomic<uint64_t> st_kafka_immediate_records_{0};
    std::atomic<uint64_t> st_ordered_apply_wait_ns_{0};
    std::atomic<uint64_t> st_ordered_apply_pending_max_{0};
    std::atomic<uint64_t> st_det_raw_compat_activations_{0};
    std::atomic<uint64_t> st_queue_depth_cur_{0};
    std::atomic<bool> queue_overloaded_{false};
    std::mutex admission_mu_;
    std::condition_variable admission_cv_;
    std::atomic<bool> st_det_block_seen_{false};
    std::atomic<bool> st_det_block_fallback_seen_{false};
    std::atomic<uint64_t> next_dispatch_seq_{1};

    /* Block fastpath visibility counters. */
    std::atomic<uint64_t> st_fastpath_blocks_submitted_{0};
    std::atomic<uint64_t> st_fastpath_blocks_returned_{0};
    std::atomic<uint64_t> st_fastpath_blocks_emitted_{0};
    std::atomic<uint64_t> st_fastpath_ready_blocks_max_{0};
    std::atomic<uint64_t> st_fastpath_last_submitted_block_id_{0};
    std::atomic<uint64_t> st_fastpath_last_returned_block_id_{0};
    std::atomic<uint64_t> st_fastpath_last_returned_block_seq_{0};
    std::atomic<uint64_t> st_fastpath_last_emitted_seq_{0};
    std::atomic<uint64_t> st_fastpath_submit_to_return_max_us_{0};
    std::atomic<uint64_t> st_fastpath_send_failures_{0};
    std::atomic<uint64_t> st_fastpath_requeues_{0};
    std::atomic<uint64_t> st_fastpath_reconnect_failures_{0};

    // Atomic telemetry counters for result flush and delivery pending
    std::atomic<uint64_t> st_result_flush_count_{0};
    std::atomic<uint64_t> st_result_flush_records_total_{0};
    std::atomic<uint64_t> st_result_flush_records_max_{0};
    std::atomic<uint64_t> st_result_flush_due_to_record_cap_{0};
    std::atomic<uint64_t> st_result_flush_due_to_byte_cap_{0};
    std::atomic<uint64_t> st_result_flush_due_to_age_{0};
    std::atomic<uint64_t> st_result_flush_due_to_idle_{0};
    std::atomic<uint64_t> st_result_flush_due_to_error_{0};
    std::atomic<uint64_t> st_result_flush_due_to_shutdown_{0};
    std::atomic<uint64_t> st_result_flush_while_delivery_pending_gt_8_{0};
    std::atomic<uint64_t> st_result_flush_while_delivery_pending_gt_32_{0};
    std::atomic<uint64_t> st_kafka_delivery_pending_max_{0};
    std::atomic<uint64_t> st_kafka_delivery_pending_over_8_events_{0};
    std::atomic<uint64_t> st_kafka_delivery_pending_over_32_events_{0};

    std::mutex det_ordered_apply_mu_;
    uint64_t det_next_ordered_apply_seq_ = 1;
    std::map<uint64_t, std::pair<uint64_t, uint32_t>> det_ordered_apply_ready_; /* dispatch_seq → {raft_log_idx, item_ordinal} */
    std::mutex det_emit_mu_;
    std::condition_variable det_emit_cv_;
    uint64_t det_next_emit_seq_ = 1;
    std::mutex det_compat_mu_;
    std::condition_variable det_compat_cv_;
    bool det_compat_inflight_ = false;
    std::string det_raw_compat_first_req_id_;
    std::string det_raw_compat_first_sql_prefix_;
    std::mutex det_apply_mu_;
    std::condition_variable det_apply_cv_;
    uint64_t det_next_apply_seq_ = 1;
    bool det_apply_initialized_ = false;
    std::unordered_map<uint64_t, det_tx_state> det_tx_states_;

    bool is_det_prefixed_sql(const std::string& sql) const;
    void note_det_raw_compat_activation(const task& t);
    std::string det_unprefixed_sql_error(const task& t) const;
};

} // namespace ariabc_pg
