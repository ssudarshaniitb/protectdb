#include "pg_state_machine.hxx"

#include "ariabc_pg_util.hxx"
#include <openssl/sha.h>
#include <libpq-fe.h>

#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <cstring>
#include <sys/types.h>
#include <signal.h>

namespace ariabc_pg {

namespace {



inline uint64_t to_be64(uint64_t host_int) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return (((host_int & 0x00000000000000ffULL) << 56) |
            ((host_int & 0x000000000000ff00ULL) << 40) |
            ((host_int & 0x0000000000ff0000ULL) << 24) |
            ((host_int & 0x00000000ff000000ULL) << 8)  |
            ((host_int & 0x000000ff00000000ULL) >> 8)  |
            ((host_int & 0x0000ff0000000000ULL) >> 24) |
            ((host_int & 0x00ff000000000000ULL) >> 40) |
            ((host_int & 0xff00000000000000ULL) >> 56));
#else
    return host_int;
#endif
}

inline uint32_t to_be32(uint32_t host_int) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return (((host_int & 0x000000ffU) << 24) |
            ((host_int & 0x0000ff00U) << 8)  |
            ((host_int & 0x00ff0000U) >> 8)  |
            ((host_int & 0xff000000U) >> 24));
#else
    return host_int;
#endif
}

inline int32_t to_be32_s(int32_t host_int) {
    uint32_t val = to_be32(*reinterpret_cast<uint32_t*>(&host_int));
    return *reinterpret_cast<int32_t*>(&val);
}

bool debug_req_trace_enabled() {
    const char* env = std::getenv("ARIABC_DEBUG_REQ_TRACE");
    if (!env || !*env) return false;
    const std::string s(env);
    return !(s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
}

uint64_t debug_req_trace_limit() {
    const char* env = std::getenv("ARIABC_DEBUG_REQ_TRACE_LIMIT");
    if (!env || !*env) return 32;
    try {
        const unsigned long long n = std::stoull(env);
        return (n == 0ULL) ? 32ULL : static_cast<uint64_t>(n);
    } catch (...) {
        return 32ULL;
    }
}

std::atomic<uint64_t> g_debug_commit_trace_count{0};
std::atomic<bool> g_manifest_failpoint_fired{false};

uint64_t steady_now_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

uint64_t result_token_ttl_ns() {
    const char* env = std::getenv("ARIABC_RESULT_TOKEN_TTL_MS");
    uint64_t ttl_ms = 60000;
    if (env && env[0] != '\0') {
        try {
            ttl_ms = std::stoull(env);
        } catch (...) {
            ttl_ms = 60000;
        }
    }
    if (ttl_ms == 0) ttl_ms = 1;
    return ttl_ms * 1000000ULL;
}

uint32_t result_waiter_count(size_t item_count) {
    if (item_count > static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return std::numeric_limits<uint32_t>::max();
    }
    return static_cast<uint32_t>(item_count);
}

bool state_machine_failpoint_matches(const char* name,
                                     int node_id,
                                     uint64_t raft_log_idx,
                                     uint32_t item_ordinal) {
    const char* enabled = std::getenv(name);
    if (!enabled || enabled[0] == '\0') return false;

    const char* node_filter = std::getenv("ARIABC_FAILPOINT_NODE_ID");
    if (node_filter && node_filter[0] != '\0') {
        char* end = nullptr;
        const long wanted = std::strtol(node_filter, &end, 10);
        if (end == node_filter || *end != '\0' || wanted != node_id) {
            return false;
        }
    }

    const char* log_filter = std::getenv("ARIABC_FAILPOINT_RAFT_LOG_INDEX");
    if (log_filter && log_filter[0] != '\0') {
        char* end = nullptr;
        const unsigned long long wanted = std::strtoull(log_filter, &end, 10);
        if (end == log_filter || *end != '\0' || wanted != raft_log_idx) {
            return false;
        }
    }

    const char* min_log_filter = std::getenv("ARIABC_FAILPOINT_MIN_RAFT_LOG_INDEX");
    if (min_log_filter && min_log_filter[0] != '\0') {
        char* end = nullptr;
        const unsigned long long wanted = std::strtoull(min_log_filter, &end, 10);
        if (end == min_log_filter || *end != '\0' || wanted == 0ULL || raft_log_idx < wanted) {
            return false;
        }
    }

    const char* ordinal_filter = std::getenv("ARIABC_FAILPOINT_ITEM_ORDINAL");
    if (ordinal_filter && ordinal_filter[0] != '\0') {
        char* end = nullptr;
        const unsigned long wanted = std::strtoul(ordinal_filter, &end, 10);
        if (end == ordinal_filter || *end != '\0' ||
            wanted != static_cast<unsigned long>(item_ordinal)) {
            return false;
        }
    }

    bool expected = false;
    return g_manifest_failpoint_fired.compare_exchange_strong(
        expected, true, std::memory_order_acq_rel, std::memory_order_acquire);
}

void debug_trace_commit(const std::string& req_id, nuraft::ulong log_idx, const std::string& sql) {
    if (!debug_req_trace_enabled()) return;
    const uint64_t idx = g_debug_commit_trace_count.fetch_add(1, std::memory_order_relaxed);
    if (idx >= debug_req_trace_limit()) return;
    std::string sql_head = trim_copy(sql);
    if (sql_head.size() > 96) sql_head.resize(96);
    std::cerr << "REQ_TRACE commit"
              << " idx=" << idx
              << " req_id=" << req_id
              << " log_idx=" << log_idx
              << " sql=" << sql_head
              << std::endl;
}

void debug_trace_batch_commit(const raft_request_batch& batch,
                              nuraft::ulong log_idx) {
    if (!debug_req_trace_enabled()) return;
    const uint64_t idx = g_debug_commit_trace_count.fetch_add(1, std::memory_order_relaxed);
    if (idx >= debug_req_trace_limit()) return;
    const client_api_request_item* first =
        batch.items.empty() ? nullptr : &batch.items.front();
    std::string sql_head = first ? trim_copy(first->sql) : std::string();
    if (sql_head.size() > 96) sql_head.resize(96);
    std::cerr << "REQ_TRACE commit_batch"
              << " idx=" << idx
              << " items=" << batch.items.size()
              << " first_req_id=" << (first ? first->req_id : std::string("NA"))
              << " log_idx=" << log_idx
              << " sql=" << sql_head
              << std::endl;
}

std::string raft_ordering_policy() {
    const char* env = std::getenv("ARIABC_RAFT_ORDERING_POLICY");
    if (!env || !*env) return "preassigned";
    const std::string policy = trim_copy(env);
    return policy.empty() ? std::string("preassigned") : policy;
}

bool state_machine_leader_assigned_ordering_enabled() {
    return raft_ordering_policy() == "leader-assigned";
}

std::string format_det_seq8_state_machine(uint64_t seq) {
    std::ostringstream oss;
    oss.width(8);
    oss.fill('0');
    oss << seq;
    return oss.str();
}

bool parse_det_prefixed_sql_state_machine(const std::string& sql,
                                          uint64_t* out_seq,
                                          std::string* out_raw_sql) {
    const std::string t = trim_copy(sql);
    if (t.size() < 3) return false;
    if (!(t[0] == 's' || t[0] == 'S') ||
        !std::isspace(static_cast<unsigned char>(t[1]))) {
        return false;
    }

    size_t pos = 2;
    while (pos < t.size() && std::isspace(static_cast<unsigned char>(t[pos]))) ++pos;
    const size_t digits_begin = pos;
    while (pos < t.size() && std::isdigit(static_cast<unsigned char>(t[pos]))) ++pos;

    uint64_t seq = 0;
    if (pos > digits_begin && pos < t.size() &&
        std::isspace(static_cast<unsigned char>(t[pos]))) {
        try {
            seq = static_cast<uint64_t>(std::stoull(t.substr(digits_begin, pos - digits_begin)));
        } catch (...) {
            return false;
        }
        while (pos < t.size() && std::isspace(static_cast<unsigned char>(t[pos]))) ++pos;
    } else {
        pos = 2;
        while (pos < t.size() && std::isspace(static_cast<unsigned char>(t[pos]))) ++pos;
    }

    if (pos >= t.size()) return false;
    if (out_seq) *out_seq = seq;
    if (out_raw_sql) *out_raw_sql = t.substr(pos);
    return true;
}

bool rewrite_det_seq_in_sql_state_machine(const std::string& sql,
                                          uint64_t assigned_seq,
                                          std::string& out_sql) {
    uint64_t ignored_seq = 0;
    std::string raw_sql;
    if (!parse_det_prefixed_sql_state_machine(sql, &ignored_seq, &raw_sql)) {
        return false;
    }
    out_sql = "s " + format_det_seq8_state_machine(assigned_seq) + " " + raw_sql;
    return true;
}

void validate_assigned_det_seq_or_abort(const client_api_request_item& item,
                                        uint64_t log_idx,
                                        uint32_t ordinal) {
    if (!item.has_assigned_det_seq) return;

    uint64_t parsed_seq = 0;
    if (!parse_det_prefixed_sql_state_machine(item.sql, &parsed_seq, nullptr) ||
        parsed_seq != item.assigned_det_seq) {
        std::cerr << "ASSIGNED_DET_SEQ_MISMATCH"
                  << " log=" << log_idx
                  << " ord=" << ordinal
                  << " req_id=" << item.req_id
                  << " has_assigned=1"
                  << " assigned=" << item.assigned_det_seq
                  << " parsed=" << parsed_seq
                  << std::endl;
        std::abort();
    }
}

} // namespace

pg_state_machine::pg_state_machine(int node_id,
                                   const db_options& db_opt,
                                   const kafka_options& k_opt)
    : executor_(node_id,
                db_opt,
                k_opt,
                [this](uint64_t log_idx, uint32_t item_ordinal) {
                    note_item_applied(log_idx, item_ordinal);
                },
                [this](uint64_t log_idx, uint32_t item_ordinal, const std::string& reason) {
                    note_item_failed(log_idx, item_ordinal, reason);
                })
    , last_snapshot_(nullptr)
    , db_opt_(db_opt)
    , node_id_(node_id)
    {}

pg_state_machine::~pg_state_machine() {
    executor_.stop();
}

uint64_t pg_state_machine::initialize_committed_det_seq_locked() {
    if (committed_det_seq_initialized_) {
        return next_committed_det_seq_;
    }

    int64_t last_committed = -1;
    std::string conninfo = "host=" + db_opt_.host +
                           " port=" + db_opt_.port +
                           " dbname=" + db_opt_.dbname +
                           " user=" + db_opt_.user;
    if (!db_opt_.password.empty()) {
        conninfo += " password=" + db_opt_.password;
    }

    PGconn* c = PQconnectdb(conninfo.c_str());
    if (!c || PQstatus(c) != CONNECTION_OK) {
        const std::string err = c ? std::string(PQerrorMessage(c)) : std::string("null conn");
        if (c) PQfinish(c);
        throw std::runtime_error("LEADER_ASSIGNED_DET_SEQ_SEED_FAILED: connect: " + err);
    }

    PGresult* res = PQexec(c, "SELECT bcdb_last_committed_txid();");
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1) {
        const std::string err = res ? std::string(PQresultErrorMessage(res)) : std::string("null result");
        if (res) PQclear(res);
        PQfinish(c);
        throw std::runtime_error("LEADER_ASSIGNED_DET_SEQ_SEED_FAILED: query: " + err);
    }

    try {
        last_committed = static_cast<int64_t>(std::stoll(PQgetvalue(res, 0, 0)));
    } catch (...) {
        PQclear(res);
        PQfinish(c);
        throw std::runtime_error("LEADER_ASSIGNED_DET_SEQ_SEED_FAILED: invalid watermark");
    }

    PQclear(res);
    PQfinish(c);

    next_committed_det_seq_ =
        (last_committed < 0) ? 0 : static_cast<uint64_t>(last_committed) + 1ULL;
    committed_det_seq_initialized_ = true;
    std::cerr << "LEADER_ASSIGNED_DET_SEQ_SEED"
              << " node=" << node_id_
              << " last_committed=" << last_committed
              << " next=" << next_committed_det_seq_
              << std::endl;
    return next_committed_det_seq_;
}

uint64_t pg_state_machine::next_committed_det_seq_locked() {
    initialize_committed_det_seq_locked();
    return next_committed_det_seq_++;
}

nuraft::ptr<nuraft::buffer> pg_state_machine::commit(const nuraft::ulong log_idx,
                                                     nuraft::buffer& data)
{
    if (data.size() == 0) {
        std::cout << "[pg_state_machine::commit] no-op commit at log_idx=" << log_idx << std::endl;
        {
            std::lock_guard<std::mutex> lk(tracker_mu_);
            entry_tracker_record& rec = entry_tracker_[static_cast<uint64_t>(log_idx)];
            rec.total_items = 1;
            rec.terminal_item.assign(1, false);
        }
        note_item_applied(log_idx, 0);
        nuraft::ptr<nuraft::buffer> ack = nuraft::buffer::alloc(0);
        return ack;
    }

    std::string entry_digest_hex;
    if (db_opt_.raft_apply_ledger_mode == "safe") {
        unsigned char entry_hash[SHA256_DIGEST_LENGTH];
        SHA256(data.data(), data.size(), entry_hash);
        entry_digest_hex = to_hex_string(entry_hash, SHA256_DIGEST_LENGTH);
    }

    raft_request_batch batch;
    std::string parse_err;
    if (!parse_raft_request_log(data, batch, parse_err)) {
        if (db_opt_.raft_apply_ledger_mode == "safe") {
            std::cerr << "pg_state_machine commit parse FAILED at log " << log_idx
                      << " (safe mode): " << parse_err
                      << " — failing server closed (abort)" << std::endl;
            std::abort();
        }
        client_api_request_item item;
        item.req_id = "ERR";
        item.sql = std::string("/* commit parse failed: ") + parse_err + " */";
        batch.items.push_back(std::move(item));
        batch.leader_node_hint = -1;
        std::cerr << "pg_state_machine commit parse failed at log " << log_idx
                  << ": " << parse_err << std::endl;
    }

    if (batch.items.empty()) {
        if (db_opt_.raft_apply_ledger_mode == "safe") {
            std::cerr << "pg_state_machine commit parsed a zero-item batch at log " << log_idx
                      << " — failing server closed (abort)" << std::endl;
            std::abort();
        }
        // Non-safe mode: treat the empty batch as a synthetic single no-op item so the
        // tracker record advances correctly and we never call register_result_batch(0).
        std::cerr << "pg_state_machine commit parsed a zero-item batch at log " << log_idx
                  << " — treating as no-op" << std::endl;
        {
            std::lock_guard<std::mutex> lk(tracker_mu_);
            entry_tracker_record& rec = entry_tracker_[static_cast<uint64_t>(log_idx)];
            rec.total_items = 1;
            rec.terminal_item.assign(1, false);
        }
        note_item_applied(log_idx, 0);
        nuraft::ptr<nuraft::buffer> ack = nuraft::buffer::alloc(0);
        return ack;
    }

    const bool leader_assigned = state_machine_leader_assigned_ordering_enabled();
    if (leader_assigned && db_opt_.raft_apply_ledger_mode != "off") {
        std::cerr << "LEADER_ASSIGNED_UNSUPPORTED_LEDGER_MODE"
                  << " mode=" << db_opt_.raft_apply_ledger_mode
                  << " log=" << static_cast<uint64_t>(log_idx)
                  << std::endl;
        std::abort();
    }
    if (leader_assigned) {
        std::lock_guard<std::mutex> seq_lk(committed_det_seq_mu_);
        for (size_t i = 0; i < batch.items.size(); ++i) {
            client_api_request_item& item = batch.items[i];
            const uint64_t assigned_seq = next_committed_det_seq_locked();
            std::string rewritten_sql;
            if (!rewrite_det_seq_in_sql_state_machine(item.sql, assigned_seq, rewritten_sql)) {
                std::cerr << "LEADER_ASSIGNED_REWRITE_FAILED"
                          << " log=" << static_cast<uint64_t>(log_idx)
                          << " ord=" << i
                          << " req_id=" << item.req_id
                          << " assigned=" << assigned_seq
                          << std::endl;
                std::abort();
            }
            item.sql = std::move(rewritten_sql);
            item.has_assigned_det_seq = true;
            item.assigned_det_seq = assigned_seq;
        }
    }
    for (size_t i = 0; i < batch.items.size(); ++i) {
        validate_assigned_det_seq_or_abort(batch.items[i],
                                           static_cast<uint64_t>(log_idx),
                                           static_cast<uint32_t>(i));
    }

    if (batch.items.size() == 1) {
        const client_api_request_item& item = batch.items.front();
        debug_trace_commit(item.req_id, log_idx, item.sql);
    } else {
        debug_trace_batch_commit(batch, log_idx);
    }
    register_result_batch(static_cast<uint64_t>(log_idx), batch.items);

    // Safe-mode flow trace: SAFE_BLOCK_PARSED after successful parse.
    {
        const char* safe_trace_env = std::getenv("ARIABC_SAFE_TRACE");
        const bool safe_trace_on =
            (db_opt_.raft_apply_ledger_mode == "safe") ||
            (safe_trace_env && safe_trace_env[0] != '\0' &&
             std::string(safe_trace_env) != "0");
        if (safe_trace_on) {
            std::cerr << "SAFE_BLOCK_PARSED"
                      << " block=" << static_cast<uint64_t>(log_idx)
                      << " num_tx=" << batch.items.size()
                      << " worker_queues=" << executor_.bcdb_block_size()
                      << std::endl;
        }
    }
    std::vector<std::string> req_ids;
    std::vector<std::string> sqls;
    std::vector<uint64_t> assigned_det_seqs;
    std::vector<uint8_t> assigned_det_seq_valid;
    req_ids.reserve(batch.items.size());
    sqls.reserve(batch.items.size());
    assigned_det_seqs.reserve(batch.items.size());
    assigned_det_seq_valid.reserve(batch.items.size());
    for (const auto& item : batch.items) {
        req_ids.push_back(item.req_id);
        sqls.push_back(item.sql);
        assigned_det_seqs.push_back(item.assigned_det_seq);
        assigned_det_seq_valid.push_back(item.has_assigned_det_seq ? 1 : 0);
    }
    std::vector<std::string> item_digests_hex;
    if (db_opt_.raft_apply_ledger_mode == "safe") {
        std::vector<uint8_t> epoch_bytes = decode_hex_string(db_opt_.raft_epoch_hex);
        if (epoch_bytes.size() != 32) {
            epoch_bytes.assign(32, 0);
        }

        item_digests_hex.reserve(batch.items.size());
        for (size_t i = 0; i < batch.items.size(); ++i) {
            const auto& item = batch.items[i];
            SHA256_CTX ctx;
            SHA256_Init(&ctx);

            uint32_t proto_be = to_be32(1);
            SHA256_Update(&ctx, &proto_be, sizeof(proto_be));
            SHA256_Update(&ctx, epoch_bytes.data(), 32);

            uint64_t log_idx_be = to_be64(static_cast<uint64_t>(log_idx));
            SHA256_Update(&ctx, &log_idx_be, sizeof(log_idx_be));

            uint32_t ordinal_be = to_be32(static_cast<uint32_t>(i));
            SHA256_Update(&ctx, &ordinal_be, sizeof(ordinal_be));

            uint32_t count_be = to_be32(static_cast<uint32_t>(batch.items.size()));
            SHA256_Update(&ctx, &count_be, sizeof(count_be));

            int32_t leader_hint_be = to_be32_s(static_cast<int32_t>(batch.leader_node_hint));
            SHA256_Update(&ctx, &leader_hint_be, sizeof(leader_hint_be));

            uint32_t req_id_len_be = to_be32(static_cast<uint32_t>(item.req_id.size()));
            SHA256_Update(&ctx, &req_id_len_be, sizeof(req_id_len_be));
            if (!item.req_id.empty()) {
                SHA256_Update(&ctx, item.req_id.data(), item.req_id.size());
            }

            uint32_t sql_len_be = to_be32(static_cast<uint32_t>(item.sql.size()));
            SHA256_Update(&ctx, &sql_len_be, sizeof(sql_len_be));
            if (!item.sql.empty()) {
                SHA256_Update(&ctx, item.sql.data(), item.sql.size());
            }

            unsigned char item_hash[SHA256_DIGEST_LENGTH];
            SHA256_Final(item_hash, &ctx);
            item_digests_hex.push_back(to_hex_string(item_hash, SHA256_DIGEST_LENGTH));
        }
    }

    if (db_opt_.raft_apply_ledger_mode == "safe") {
        executor_.verify_and_register_entry_manifest(log_idx, entry_digest_hex, item_digests_hex);
        if (state_machine_failpoint_matches(
                "ARIABC_FAILPOINT_AFTER_MANIFEST_REGISTER_BEFORE_ENQUEUE",
                node_id_,
                static_cast<uint64_t>(log_idx),
                0)) {
            const char* cluster_id = std::getenv("ARIABC_RAFT_CLUSTER_ID");
            if (!cluster_id || cluster_id[0] == '\0') {
                cluster_id = "unknown_cluster";
            }
            const char* epoch_hex = std::getenv("ARIABC_RAFT_EPOCH_HEX");
            if (!epoch_hex || epoch_hex[0] == '\0') {
                epoch_hex = "unknown_epoch";
            }
            std::cerr << "SAFE_FAILPOINT_TRIGGERED"
                      << " name=ARIABC_FAILPOINT_AFTER_MANIFEST_REGISTER_BEFORE_ENQUEUE"
                      << " phase=after_manifest_register_before_enqueue"
                      << " node=" << node_id_
                      << " log=" << static_cast<uint64_t>(log_idx)
                      << " ordinal=0"
                      << " pid=" << ::getpid()
                      << " epoch=" << epoch_hex
                      << " cluster_id=" << cluster_id
                      << std::endl;
            ::kill(::getpid(), SIGKILL);
        }
    }

    {
        std::lock_guard<std::mutex> lk(tracker_mu_);
        entry_tracker_record& rec = entry_tracker_[static_cast<uint64_t>(log_idx)];
        if (rec.total_items == 0) {
            rec.total_items = static_cast<uint32_t>(batch.items.size());
            rec.terminal_item.assign(rec.total_items, false);
        }
    }

    // Safe-mode flow trace: SAFE_BLOCK_ENQUEUE_BEGIN before enqueue_batch, SAFE_BLOCK_ENQUEUE_DONE after.
    {
        const char* safe_trace_env = std::getenv("ARIABC_SAFE_TRACE");
        const bool safe_trace_on =
            (db_opt_.raft_apply_ledger_mode == "safe") ||
            (safe_trace_env && safe_trace_env[0] != '\0' &&
             std::string(safe_trace_env) != "0");
        if (safe_trace_on) {
            for (size_t i = 0; i < req_ids.size(); ++i) {
                std::cerr << "SAFE_BLOCK_ENQUEUE_BEGIN"
                          << " block=" << static_cast<uint64_t>(log_idx)
                          << " tx=" << req_ids[i]
                          << " log=" << static_cast<uint64_t>(log_idx)
                          << " ord=" << i
                          << std::endl;
            }
        }
    }
    executor_.enqueue_batch(req_ids,
                            sqls,
                            batch.leader_node_hint,
                            static_cast<uint64_t>(log_idx),
                            entry_digest_hex,
                            item_digests_hex,
                            assigned_det_seqs,
                            assigned_det_seq_valid);
    {
        const char* safe_trace_env = std::getenv("ARIABC_SAFE_TRACE");
        const bool safe_trace_on =
            (db_opt_.raft_apply_ledger_mode == "safe") ||
            (safe_trace_env && safe_trace_env[0] != '\0' &&
             std::string(safe_trace_env) != "0");
        if (safe_trace_on) {
            std::cerr << "SAFE_BLOCK_ENQUEUE_DONE"
                      << " block=" << static_cast<uint64_t>(log_idx)
                      << " num_tx=" << req_ids.size()
                      << std::endl;
        }
    }


    // ACK: {req_id, log_idx}
    const std::string ack_req_id =
        batch.items.empty() ? std::string("ERR") : batch.items.front().req_id;
    const size_t ack_sz = sizeof(int32_t) + ack_req_id.size() + sizeof(uint64_t);
    nuraft::ptr<nuraft::buffer> ack = nuraft::buffer::alloc(ack_sz);
    nuraft::buffer_serializer bs_ack(ack);
    bs_ack.put_str(ack_req_id);
    bs_ack.put_u64(log_idx);
    return ack;
}

void pg_state_machine::commit_config(const nuraft::ulong log_idx,
                                     nuraft::ptr<nuraft::cluster_config>& new_conf) {
    (void)new_conf;
    std::cout << "[pg_state_machine::commit_config] log_idx=" << log_idx << std::endl;
    {
        std::lock_guard<std::mutex> lk(tracker_mu_);
        entry_tracker_record& rec = entry_tracker_[static_cast<uint64_t>(log_idx)];
        rec.total_items = 1;
        rec.terminal_item.assign(1, false);
    }
    note_item_applied(log_idx, 0);
}

bool pg_state_machine::apply_snapshot(nuraft::snapshot& s) {
    // No-op logical snapshot (DB snapshot semantics are out of scope).
    // Keep the latest snapshot metadata for Raft.
    std::lock_guard<std::mutex> l(last_snapshot_lock_);
    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    last_snapshot_ = nuraft::snapshot::deserialize(*snp_buf);
    return true;
}

nuraft::ptr<nuraft::snapshot> pg_state_machine::last_snapshot() {
    std::lock_guard<std::mutex> l(last_snapshot_lock_);
    return last_snapshot_;
}

nuraft::ulong pg_state_machine::last_commit_index() {
    /*
     * F1: return the contiguous durable applied prefix (not the Raft committed
     * index).  NuRaft uses this to determine which log entries to re-deliver
     * on startup, so returning a trailing but correct value is safe.
     */
    const uint64_t durable = durable_applied_prefix_.load(std::memory_order_acquire);
    return static_cast<nuraft::ulong>(durable);
}

bool pg_state_machine::wait_for_result(uint64_t result_token,
                                       int timeout_ms,
                                       std::string* failure_reason) {
    if (result_token == 0) return false;
    if (timeout_ms <= 0) timeout_ms = 1;

    std::unique_lock<std::mutex> lk(result_mu_);
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    const bool ready = result_cv_.wait_until(lk, deadline, [&] {
        return completed_result_tokens_.find(result_token) != completed_result_tokens_.end() ||
               failed_result_tokens_.find(result_token) != failed_result_tokens_.end();
    });
    if (!ready) return false;
    auto failed = failed_result_tokens_.find(result_token);
    if (failed != failed_result_tokens_.end()) {
        if (failure_reason != nullptr) {
            *failure_reason = failed->second;
        }
        if (outstanding_waiters_.find(result_token) == outstanding_waiters_.end()) {
            failed_result_tokens_.erase(failed);
            completed_result_tokens_.erase(result_token);
            completed_at_ns_.erase(result_token);
        }
        return false;
    }
    if (failure_reason != nullptr) {
        failure_reason->clear();
    }
    if (outstanding_waiters_.find(result_token) == outstanding_waiters_.end()) {
        completed_result_tokens_.erase(result_token);
        completed_at_ns_.erase(result_token);
    }
    return true;
}

bool pg_state_machine::wait_for_result_id(const std::string& req_id,
                                          int timeout_ms,
                                          std::string* failure_reason) {
    if (req_id.empty()) return false;
    if (timeout_ms <= 0) timeout_ms = 1;

    std::unique_lock<std::mutex> lk(result_mu_);
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    uint64_t result_token = 0;
    result_item_key item_key;
    const bool ready = result_cv_.wait_until(lk, deadline, [&] {
        auto item_mapped = result_item_by_req_id_.find(req_id);
        if (item_mapped != result_item_by_req_id_.end()) {
            item_key = item_mapped->second;
            result_token = item_key.result_token;
            auto completed = completed_result_ordinals_.find(result_token);
            auto failed = failed_result_ordinals_.find(result_token);
            return (completed != completed_result_ordinals_.end() &&
                    completed->second.find(item_key.ordinal) != completed->second.end()) ||
                   (failed != failed_result_ordinals_.end() &&
                    failed->second.find(item_key.ordinal) != failed->second.end());
        } else {
            auto mapped = result_token_by_req_id_.find(req_id);
            if (mapped == result_token_by_req_id_.end()) {
                return false;
            }
            result_token = mapped->second;
            item_key = result_item_key{};
            return completed_result_tokens_.find(result_token) != completed_result_tokens_.end() ||
                   failed_result_tokens_.find(result_token) != failed_result_tokens_.end();
        }
    });
    if (!ready || result_token == 0) return false;

    auto token_failed = failed_result_tokens_.find(result_token);
    std::string item_failure_reason;
    bool has_item_failure = false;
    if (item_key.result_token != 0) {
        auto failed_ordinals = failed_result_ordinals_.find(result_token);
        if (failed_ordinals != failed_result_ordinals_.end()) {
            auto item_failed = failed_ordinals->second.find(item_key.ordinal);
            if (item_failed != failed_ordinals->second.end()) {
                has_item_failure = true;
                item_failure_reason = item_failed->second;
            }
        }
    }
    if (has_item_failure ||
        (item_key.result_token == 0 && token_failed != failed_result_tokens_.end())) {
        if (failure_reason != nullptr) {
            *failure_reason = has_item_failure
                ? item_failure_reason
                : token_failed->second;
        }
        if (item_key.result_token != 0) {
            consume_result_item_waiter_locked(req_id, item_key);
        } else {
            consume_result_waiter_locked(req_id, result_token);
        }
        return false;
    }
    if (failure_reason != nullptr) {
        failure_reason->clear();
    }
    if (item_key.result_token != 0) {
        consume_result_item_waiter_locked(req_id, item_key);
    } else {
        consume_result_waiter_locked(req_id, result_token);
    }
    return true;
}

pg_state_machine::result_tracker_profile pg_state_machine::result_profile() const {
    std::lock_guard<std::mutex> lk(result_mu_);
    result_tracker_profile p;
    p.req_id_map_size_current = result_token_by_req_id_.size();
    p.req_id_map_size_max = req_id_map_size_max_;
    p.result_token_map_size_current = req_ids_by_result_token_.size();
    p.result_token_map_size_max = result_token_map_size_max_;
    return p;
}

void pg_state_machine::register_result_batch(uint64_t result_token, size_t item_count) {
    if (result_token == 0 || item_count == 0) return;
    std::lock_guard<std::mutex> lk(result_mu_);
    maybe_cleanup_result_tokens_locked(steady_now_ns());
    auto old_req_ids = req_ids_by_result_token_.find(result_token);
    if (old_req_ids != req_ids_by_result_token_.end()) {
        for (const std::string& req_id : old_req_ids->second) {
            result_token_by_req_id_.erase(req_id);
            result_item_by_req_id_.erase(req_id);
        }
        req_ids_by_result_token_.erase(old_req_ids);
    }
    pending_result_counts_[result_token] = item_count;
    outstanding_waiters_[result_token] = result_waiter_count(item_count);
    completed_result_tokens_.erase(result_token);
    completed_result_ordinals_.erase(result_token);
    failed_result_ordinals_.erase(result_token);
    failed_result_tokens_.erase(result_token);
    completed_at_ns_.erase(result_token);
    update_result_tracker_highwater_locked();
}

void pg_state_machine::register_result_batch(
        uint64_t result_token,
        const std::vector<client_api_request_item>& items) {
    if (result_token == 0 || items.empty()) return;
    std::lock_guard<std::mutex> lk(result_mu_);
    maybe_cleanup_result_tokens_locked(steady_now_ns());
    auto old_req_ids = req_ids_by_result_token_.find(result_token);
    if (old_req_ids != req_ids_by_result_token_.end()) {
        for (const std::string& req_id : old_req_ids->second) {
            result_token_by_req_id_.erase(req_id);
            result_item_by_req_id_.erase(req_id);
        }
        req_ids_by_result_token_.erase(old_req_ids);
    }
    pending_result_counts_[result_token] = items.size();
    outstanding_waiters_[result_token] = result_waiter_count(items.size());
    completed_result_tokens_.erase(result_token);
    completed_result_ordinals_.erase(result_token);
    failed_result_ordinals_.erase(result_token);
    failed_result_tokens_.erase(result_token);
    completed_at_ns_.erase(result_token);
    std::vector<std::string>& req_ids = req_ids_by_result_token_[result_token];
    req_ids.reserve(items.size());
    for (uint32_t i = 0; i < items.size(); ++i) {
        const client_api_request_item& item = items[i];
        if (!item.req_id.empty()) {
            result_token_by_req_id_[item.req_id] = result_token;
            result_item_by_req_id_[item.req_id] = result_item_key{result_token, i};
            req_ids.push_back(item.req_id);
        }
    }
    update_result_tracker_highwater_locked();
    result_cv_.notify_all();
}

void pg_state_machine::note_result_applied(uint64_t result_token) {
    note_result_item_applied(result_token, 0);
}

void pg_state_machine::note_result_item_applied(uint64_t result_token, uint32_t ordinal) {
    if (result_token == 0) return;
    std::lock_guard<std::mutex> lk(result_mu_);
    auto failed_ordinals = failed_result_ordinals_.find(result_token);
    if (failed_ordinals != failed_result_ordinals_.end() &&
        failed_ordinals->second.find(ordinal) != failed_ordinals->second.end()) {
        result_cv_.notify_all();
        return;
    }
    const bool inserted = completed_result_ordinals_[result_token].insert(ordinal).second;
    if (!inserted) {
        result_cv_.notify_all();
        return;
    }
    auto it = pending_result_counts_.find(result_token);
    if (it == pending_result_counts_.end()) {
        result_cv_.notify_all();
        return;
    }
    if (it->second > 1) {
        --(it->second);
        result_cv_.notify_all();
        return;
    }
    pending_result_counts_.erase(it);
    if (failed_result_tokens_.find(result_token) == failed_result_tokens_.end()) {
        completed_result_tokens_.insert(result_token);
    }
    const uint64_t now_ns = steady_now_ns();
    completed_at_ns_[result_token] = now_ns;
    maybe_cleanup_result_tokens_locked(now_ns);
    result_cv_.notify_all();
}

void pg_state_machine::note_result_failed(uint64_t result_token, const std::string& reason) {
    if (result_token == 0) return;
    std::lock_guard<std::mutex> lk(result_mu_);
    const std::string stored_reason =
        reason.empty() ? "safe_protocol_failure_retryable" : reason;
    failed_result_tokens_[result_token] = stored_reason;
    completed_result_tokens_.erase(result_token);

    size_t item_count = 0;
    auto req_ids = req_ids_by_result_token_.find(result_token);
    if (req_ids != req_ids_by_result_token_.end()) {
        item_count = req_ids->second.size();
    }
    if (item_count == 0) {
        auto pending = pending_result_counts_.find(result_token);
        if (pending != pending_result_counts_.end()) {
            item_count = pending->second;
        }
    }
    if (item_count == 0) {
        item_count = 1;
    }

    auto& failed_ordinals = failed_result_ordinals_[result_token];
    for (uint32_t i = 0; i < item_count; ++i) {
        failed_ordinals[i] = stored_reason;
    }
    pending_result_counts_.erase(result_token);
    const uint64_t now_ns = steady_now_ns();
    completed_at_ns_[result_token] = now_ns;
    maybe_cleanup_result_tokens_locked(now_ns);
    result_cv_.notify_all();
}

void pg_state_machine::note_result_item_failed(uint64_t result_token,
                                               uint32_t ordinal,
                                               const std::string& reason) {
    if (result_token == 0) return;
    std::lock_guard<std::mutex> lk(result_mu_);
    auto completed = completed_result_ordinals_.find(result_token);
    if (completed != completed_result_ordinals_.end() &&
        completed->second.find(ordinal) != completed->second.end()) {
        result_cv_.notify_all();
        return;
    }
    std::string stored_reason =
        reason.empty() ? "safe_protocol_failure_retryable" : reason;
    auto& failed_ordinals = failed_result_ordinals_[result_token];
    if (failed_ordinals.find(ordinal) != failed_ordinals.end()) {
        result_cv_.notify_all();
        return;
    }
    failed_ordinals[ordinal] = stored_reason;
    failed_result_tokens_[result_token] = stored_reason;
    auto pending = pending_result_counts_.find(result_token);
    if (pending != pending_result_counts_.end()) {
        if (pending->second > 1) {
            --(pending->second);
        } else {
            pending_result_counts_.erase(pending);
        }
    }
    completed_result_tokens_.erase(result_token);
    const uint64_t now_ns = steady_now_ns();
    completed_at_ns_[result_token] = now_ns;
    maybe_cleanup_result_tokens_locked(now_ns);
    result_cv_.notify_all();
}

void pg_state_machine::consume_result_waiter_locked(const std::string& req_id,
                                                    uint64_t result_token) {
    result_token_by_req_id_.erase(req_id);
    result_item_by_req_id_.erase(req_id);

    auto waiter = outstanding_waiters_.find(result_token);
    if (waiter == outstanding_waiters_.end()) return;
    if (waiter->second > 1) {
        --(waiter->second);
        return;
    }

    pending_result_counts_.erase(result_token);
    completed_result_tokens_.erase(result_token);
    failed_result_tokens_.erase(result_token);
    completed_result_ordinals_.erase(result_token);
    failed_result_ordinals_.erase(result_token);
    outstanding_waiters_.erase(waiter);
    completed_at_ns_.erase(result_token);
    req_ids_by_result_token_.erase(result_token);
}

void pg_state_machine::consume_result_item_waiter_locked(const std::string& req_id,
                                                         const result_item_key& key) {
    consume_result_waiter_locked(req_id, key.result_token);
}

void pg_state_machine::update_result_tracker_highwater_locked() {
    req_id_map_size_max_ = std::max(req_id_map_size_max_, result_token_by_req_id_.size());
    result_token_map_size_max_ =
        std::max(result_token_map_size_max_, req_ids_by_result_token_.size());
}

void pg_state_machine::cleanup_result_tokens_locked(uint64_t now_ns, bool force) {
    const uint64_t ttl_ns = result_token_ttl_ns();
    for (auto it = completed_at_ns_.begin(); it != completed_at_ns_.end(); ) {
        const uint64_t result_token = it->first;
        const uint64_t completed_ns = it->second;
        if (pending_result_counts_.find(result_token) != pending_result_counts_.end()) {
            ++it;
            continue;
        }
        if (!force && now_ns < completed_ns + ttl_ns) {
            ++it;
            continue;
        }

        auto req_ids = req_ids_by_result_token_.find(result_token);
        if (req_ids != req_ids_by_result_token_.end()) {
            for (const std::string& req_id : req_ids->second) {
                result_token_by_req_id_.erase(req_id);
                result_item_by_req_id_.erase(req_id);
            }
            req_ids_by_result_token_.erase(req_ids);
        }
        completed_result_tokens_.erase(result_token);
        failed_result_tokens_.erase(result_token);
        completed_result_ordinals_.erase(result_token);
        failed_result_ordinals_.erase(result_token);
        outstanding_waiters_.erase(result_token);
        it = completed_at_ns_.erase(it);
    }
}

void pg_state_machine::maybe_cleanup_result_tokens_locked(uint64_t now_ns) {
    const uint64_t cleanup_interval_ns = 1000000000ULL;
    if (last_result_cleanup_ns_ == 0 ||
        now_ns - last_result_cleanup_ns_ >= cleanup_interval_ns ||
        completed_at_ns_.size() >= 65536) {
        cleanup_result_tokens_locked(now_ns, false);
        last_result_cleanup_ns_ = now_ns;
    }
}

#ifdef BUILDING_UNIT_TESTS
void pg_state_machine::test_force_result_cleanup() {
    std::lock_guard<std::mutex> lk(result_mu_);
    cleanup_result_tokens_locked(steady_now_ns(), false);
}

pg_state_machine::result_tracker_debug_counts
pg_state_machine::test_result_tracker_counts() const {
    std::lock_guard<std::mutex> lk(result_mu_);
    result_tracker_debug_counts counts;
    counts.pending_result_counts = pending_result_counts_.size();
    counts.result_token_by_req_id = result_token_by_req_id_.size();
    counts.completed_result_tokens = completed_result_tokens_.size();
    counts.failed_result_tokens = failed_result_tokens_.size();
    counts.outstanding_waiters = outstanding_waiters_.size();
    counts.completed_at_ns = completed_at_ns_.size();
    return counts;
}
#endif

void pg_state_machine::create_snapshot(nuraft::snapshot& s,
                                       nuraft::async_result<bool>::handler_type& when_done)
{
    // No-op snapshot: just persist snapshot metadata.
    {   std::lock_guard<std::mutex> l(last_snapshot_lock_);
        nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
        last_snapshot_ = nuraft::snapshot::deserialize(*snp_buf);
    }
    nuraft::ptr<std::exception> except(nullptr);
    bool ret = true;
    when_done(ret, except);
}

/* --------------------------------------------------------------------------
 * P0 #14: safe_sync_startup — synchronous pre-Raft validation and prefix seeding
 * -------------------------------------------------------------------------- */

namespace {
inline std::vector<uint8_t> parse_pg_bytea(const char* val, int len) {
    if (!val) return {};
    std::string s(val, len);
    if (s.size() >= 2 && s[0] == '\\' && s[1] == 'x') {
        return ariabc_pg::decode_hex_string(s.substr(2));
    }
    return std::vector<uint8_t>(s.begin(), s.end());
}
} // namespace

uint64_t pg_state_machine::safe_sync_startup(uint64_t durable_log_start_index) {
    if (db_opt_.raft_apply_ledger_mode != "safe") {
        // Not in safe mode — nothing to validate, return 0
        return 0;
    }

    const std::string& epoch_hex = db_opt_.raft_epoch_hex;
    if (epoch_hex.empty()) {
        throw std::runtime_error(
            "SAFE_STARTUP_FAILED: safe mode requires --raft-epoch-hex to be set");
    }
    if (durable_log_start_index > 1) {
        throw std::runtime_error(
            "SAFE_STARTUP_FAILED: safe v1 requires retained Raft logs; "
            "durable log begins at index=" +
            std::to_string(durable_log_start_index) +
            " but no verified all-entry checkpoint exists");
    }

    // Build conninfo from db_opt_ fields (same pattern as pg_executor)
    std::string conninfo = "host=" + db_opt_.host +
                           " port=" + db_opt_.port +
                           " dbname=" + db_opt_.dbname +
                           " user=" + db_opt_.user;
    if (!db_opt_.password.empty()) {
        conninfo += " password=" + db_opt_.password;
    }

    PGconn* c = PQconnectdb(conninfo.c_str());
    if (!c || PQstatus(c) != CONNECTION_OK) {
        const std::string err = c ? std::string(PQerrorMessage(c)) : std::string("null conn");
        if (c) PQfinish(c);
        throw std::runtime_error("SAFE_STARTUP_FAILED: cannot connect to PostgreSQL: " + err);
    }

    // Startup control statements must not inherit the workload's SERIALIZABLE
    // default: the singleton Merkle watermark is intentionally serialized by
    // row locking, and these checks are durable control-plane work.
    {
        PGresult* res = PQexec(c,
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED; "
            "SET synchronous_commit = on;");
        if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
            const std::string msg = PQerrorMessage(c);
            if (res) PQclear(res);
            PQfinish(c);
            throw std::runtime_error(
                "SAFE_STARTUP_FAILED: cannot configure control-plane session: " + msg);
        }
        PQclear(res);
    }

    // Step 1: Validate schema version (must be exactly 1 row, version=4)
    {
        PGresult* res = PQexec(c,
            "SELECT count(*), min(schema_version), max(schema_version) "
            "FROM ariabc_internal.raft_apply_schema_meta;");
        if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
            if (res) PQclear(res);
            PQfinish(c);
            throw std::runtime_error(
                "SAFE_STARTUP_FAILED: cannot query raft_apply_schema_meta");
        }
        const int cnt = std::stoi(PQgetvalue(res, 0, 0));
        const int minv = std::stoi(PQgetvalue(res, 0, 1));
        const int maxv = std::stoi(PQgetvalue(res, 0, 2));
        PQclear(res);
        if (cnt != 1 || minv != 4 || maxv != 4) {
            PQfinish(c);
            throw std::runtime_error(
                "SAFE_STARTUP_FAILED: schema version must be exactly one row with version=4 "
                "(got count=" + std::to_string(cnt) +
                " min=" + std::to_string(minv) +
                " max=" + std::to_string(maxv) + ")");
        }
    }

    // The server must never start safe mode with a legacy/invalid Merkle
    // format or a non-READY recovery state. Rebuild is idempotent and is a
    // no-op when no Merkle index exists yet; page apply is synchronous in the
    // transaction/ledger path, while the final status query is the hard gate.
    {
        PGresult* res = PQexec(c, "SELECT pg_catalog.merkle_rebuild_legacy_indexes();");
        if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
            const std::string msg = PQerrorMessage(c);
            if (res) PQclear(res);
            PQfinish(c);
            throw std::runtime_error(
                "SAFE_STARTUP_FAILED: Merkle legacy-index rebuild failed: " + msg);
        }
        PQclear(res);

        res = PQexec(c, "SELECT pg_catalog.merkle_recovery_status();");
        if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1) {
            const std::string msg = PQerrorMessage(c);
            if (res) PQclear(res);
            PQfinish(c);
            throw std::runtime_error(
                "SAFE_STARTUP_FAILED: cannot read Merkle recovery status: " + msg);
        }
        const std::string status_json = PQgetvalue(res, 0, 0);
        PQclear(res);
        if (status_json.find("\"state\":\"READY\"") == std::string::npos) {
            PQfinish(c);
            throw std::runtime_error(
                "SAFE_STARTUP_FAILED: Merkle recovery is not READY: " + status_json);
        }

        std::cout << "[safe_sync_startup] Merkle recovery READY: " << status_json << std::endl;
    }

    // Step 2: Validate epoch anchor (exactly one matching row)
    {
        const std::string q =
            "SELECT protocol_version FROM ariabc_internal.raft_apply_epoch "
            "WHERE epoch_id = decode('" + epoch_hex + "', 'hex');";
        PGresult* res = PQexec(c, q.c_str());
        if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
            if (res) PQclear(res);
            PQfinish(c);
            throw std::runtime_error(
                "SAFE_STARTUP_FAILED: cannot query raft_apply_epoch");
        }
        if (PQntuples(res) != 1) {
            const int n = PQntuples(res);
            PQclear(res);
            PQfinish(c);
            throw std::runtime_error(
                "SAFE_STARTUP_FAILED: epoch anchor not found or ambiguous in raft_apply_epoch "
                "(epoch_hex=" + epoch_hex +
                " rows_found=" + std::to_string(n) + ")");
        }
        const int proto_ver = std::stoi(PQgetvalue(res, 0, 0));
        PQclear(res);
        if (proto_ver != 1) {
            PQfinish(c);
            throw std::runtime_error(
                "SAFE_STARTUP_FAILED: epoch protocol version mismatch: expected 1, got " +
                std::to_string(proto_ver));
        }
        std::cout << "[safe_sync_startup] epoch anchor validated: " << epoch_hex << " (protocol_version=1)" << std::endl;
    }

    // Reject persistent CLAIMED rows
    {
        const std::string q =
            "SELECT count(*) FROM ariabc_internal.raft_apply_item "
            "WHERE epoch_id = decode('" + epoch_hex + "', 'hex') AND state = 1;";
        PGresult* res = PQexec(c, q.c_str());
        if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
            if (res) PQclear(res);
            PQfinish(c);
            throw std::runtime_error(
                "SAFE_STARTUP_FAILED: cannot query raft_apply_item for CLAIMED rows");
        }
        const int cnt = std::stoi(PQgetvalue(res, 0, 0));
        PQclear(res);
        if (cnt > 0) {
            PQfinish(c);
            throw std::runtime_error(
                "SAFE_STARTUP_FAILED: persistent_claimed_row (found " + std::to_string(cnt) +
                " CLAIMED rows for epoch_hex=" + epoch_hex + ")");
        }
    }

    // Reject invalid terminal metadata/digest rows
    {
        const std::string q =
            "SELECT raft_log_index, item_ordinal, state, "
            "       result_format_version, result_payload, "
            "       error_format_version, sqlstate_code, error_payload, "
            "       terminal_digest "
            "FROM ariabc_internal.raft_apply_item "
            "WHERE epoch_id = decode('" + epoch_hex + "', 'hex') AND state IN (2, 3);";
        PGresult* res = PQexec(c, q.c_str());
        if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
            if (res) PQclear(res);
            PQfinish(c);
            throw std::runtime_error(
                "SAFE_STARTUP_FAILED: cannot query raft_apply_item for terminal rows");
        }

        for (int row = 0; row < PQntuples(res); ++row) {
            const uint64_t log_idx = std::stoull(PQgetvalue(res, row, 0));
            const uint32_t ordinal = std::stoul(PQgetvalue(res, row, 1));
            const int state = std::stoi(PQgetvalue(res, row, 2));

            // Check terminal digest malformed
            if (PQgetisnull(res, row, 8)) {
                PQclear(res);
                PQfinish(c);
                throw std::runtime_error(
                    "SAFE_STARTUP_FAILED: terminal digest malformed (null terminal_digest at log=" +
                    std::to_string(log_idx) + " ord=" + std::to_string(ordinal) + ")");
            }

            const int db_digest_raw_len = PQgetlength(res, row, 8);
            std::vector<uint8_t> db_digest_bytes = parse_pg_bytea(PQgetvalue(res, row, 8), db_digest_raw_len);
            if (db_digest_bytes.size() != 32) {
                PQclear(res);
                PQfinish(c);
                throw std::runtime_error(
                    "SAFE_STARTUP_FAILED: terminal digest malformed (invalid digest length " +
                    std::to_string(db_digest_bytes.size()) + " at log=" + std::to_string(log_idx) +
                    " ord=" + std::to_string(ordinal) + ")");
            }

            // Recompute digest and check metadata
            int fmtver = 0;
            std::string sqlstate;
            std::vector<uint8_t> payload_bytes;

            if (state == 2) { // APPLIED_OK
                if (PQgetisnull(res, row, 3)) {
                    PQclear(res);
                    PQfinish(c);
                    throw std::runtime_error(
                        "SAFE_STARTUP_FAILED: terminal row metadata bad (null result_format_version at log=" +
                        std::to_string(log_idx) + " ord=" + std::to_string(ordinal) + ")");
                }
                fmtver = std::stoi(PQgetvalue(res, row, 3));
                if (fmtver != 1) {
                    PQclear(res);
                    PQfinish(c);
                    throw std::runtime_error(
                        "SAFE_STARTUP_FAILED: terminal row metadata bad (unsupported result_format_version=" +
                        std::to_string(fmtver) + " at log=" + std::to_string(log_idx) +
                        " ord=" + std::to_string(ordinal) + ")");
                }
                if (PQgetisnull(res, row, 4)) {
                    PQclear(res);
                    PQfinish(c);
                    throw std::runtime_error(
                        "SAFE_STARTUP_FAILED: terminal row metadata bad (null result_payload at log=" +
                        std::to_string(log_idx) + " ord=" + std::to_string(ordinal) + ")");
                }
                const int payload_raw_len = PQgetlength(res, row, 4);
                payload_bytes = parse_pg_bytea(PQgetvalue(res, row, 4), payload_raw_len);
            } else if (state == 3) { // APPLIED_ERROR
                if (PQgetisnull(res, row, 5)) {
                    PQclear(res);
                    PQfinish(c);
                    throw std::runtime_error(
                        "SAFE_STARTUP_FAILED: terminal row metadata bad (null error_format_version at log=" +
                        std::to_string(log_idx) + " ord=" + std::to_string(ordinal) + ")");
                }
                fmtver = std::stoi(PQgetvalue(res, row, 5));
                if (fmtver != 1) {
                    PQclear(res);
                    PQfinish(c);
                    throw std::runtime_error(
                        "SAFE_STARTUP_FAILED: terminal row metadata bad (unsupported error_format_version=" +
                        std::to_string(fmtver) + " at log=" + std::to_string(log_idx) +
                        " ord=" + std::to_string(ordinal) + ")");
                }
                if (PQgetisnull(res, row, 6)) {
                    PQclear(res);
                    PQfinish(c);
                    throw std::runtime_error(
                        "SAFE_STARTUP_FAILED: terminal row metadata bad (null sqlstate_code at log=" +
                        std::to_string(log_idx) + " ord=" + std::to_string(ordinal) + ")");
                }
                sqlstate = PQgetvalue(res, row, 6);
                if (sqlstate.size() != 5) {
                    PQclear(res);
                    PQfinish(c);
                    throw std::runtime_error(
                        "SAFE_STARTUP_FAILED: terminal row metadata bad (invalid sqlstate_code length=" +
                        std::to_string(sqlstate.size()) + " at log=" + std::to_string(log_idx) +
                        " ord=" + std::to_string(ordinal) + ")");
                }
                if (PQgetisnull(res, row, 7)) {
                    PQclear(res);
                    PQfinish(c);
                    throw std::runtime_error(
                        "SAFE_STARTUP_FAILED: terminal row metadata bad (null error_payload at log=" +
                        std::to_string(log_idx) + " ord=" + std::to_string(ordinal) + ")");
                }
                const int payload_raw_len = PQgetlength(res, row, 7);
                payload_bytes = parse_pg_bytea(PQgetvalue(res, row, 7), payload_raw_len);
            } else {
                PQclear(res);
                PQfinish(c);
                throw std::runtime_error(
                    "SAFE_STARTUP_FAILED: terminal row metadata bad (unknown state=" +
                    std::to_string(state) + " at log=" + std::to_string(log_idx) +
                    " ord=" + std::to_string(ordinal) + ")");
            }

            // Recompute terminal digest
            unsigned char computed_hash[SHA256_DIGEST_LENGTH];
            SHA256_CTX ctx;
            SHA256_Init(&ctx);

            const char* prefix = (state == 3) ? "ariabc-terminal-error-v1" : "ariabc-terminal-ok-v1";
            SHA256_Update(&ctx, prefix, strlen(prefix));

            uint32_t fmtver_be = to_be32(static_cast<uint32_t>(fmtver));
            SHA256_Update(&ctx, &fmtver_be, sizeof(fmtver_be));

            if (state == 3) {
                uint32_t sqlstate_len_be = to_be32(static_cast<uint32_t>(sqlstate.size()));
                SHA256_Update(&ctx, &sqlstate_len_be, sizeof(sqlstate_len_be));
                SHA256_Update(&ctx, sqlstate.data(), sqlstate.size());
            }

            uint32_t payload_len_be = to_be32(static_cast<uint32_t>(payload_bytes.size()));
            SHA256_Update(&ctx, &payload_len_be, sizeof(payload_len_be));
            if (!payload_bytes.empty()) {
                SHA256_Update(&ctx, payload_bytes.data(), payload_bytes.size());
            }

            SHA256_Final(computed_hash, &ctx);

            if (memcmp(db_digest_bytes.data(), computed_hash, SHA256_DIGEST_LENGTH) != 0) {
                PQclear(res);
                PQfinish(c);
                throw std::runtime_error(
                    "SAFE_STARTUP_FAILED: terminal row metadata bad (recomputed digest mismatch at log=" +
                    std::to_string(log_idx) + " ord=" + std::to_string(ordinal) + ")");
            }
        }
        PQclear(res);
    }

    PQfinish(c);

    std::cout << "[safe_sync_startup] validated ledger rows; safe v1 prefix=0" << std::endl;
    std::cout << "[safe_sync_startup] Raft redelivery required for terminal-result replay" << std::endl;
    return 0;
}

/* --------------------------------------------------------------------------
 * F1: Runtime entry tracker implementation
 * -------------------------------------------------------------------------- */


void pg_state_machine::seed_durable_prefix(uint64_t prefix) {
    std::lock_guard<std::mutex> lk(tracker_mu_);
    const uint64_t cur = durable_applied_prefix_.load(std::memory_order_relaxed);
    if (prefix > cur) {
        durable_applied_prefix_.store(prefix, std::memory_order_release);
    }
}

void pg_state_machine::note_item_applied(uint64_t log_idx, uint32_t item_ordinal) {
    if (log_idx == 0) return;
    bool marked = false;
    {
        std::lock_guard<std::mutex> lk(tracker_mu_);
        auto it = entry_tracker_.find(log_idx);
        if (it != entry_tracker_.end()) {
            if (item_ordinal >= it->second.total_items) {
                std::cerr << "pg_state_machine: FATAL: out-of-bounds ordinal " << item_ordinal
                          << " for log_idx " << log_idx << " (total_items: " << it->second.total_items << ")" << std::endl;
                abort();
            }
            /* mark_ordinal() is idempotent: duplicate calls for the same
             * (log_idx, item_ordinal) pair are safe no-ops. */
            marked = it->second.mark_ordinal(item_ordinal);
        } else {
            std::cerr << "pg_state_machine: FATAL: tracker record not found for log_idx " << log_idx << std::endl;
            abort();
        }
        maybe_advance_prefix_locked();
    }
    /* Also advance result_token waiter */
    if (marked) {
        note_result_item_applied(log_idx, item_ordinal);
    }
}

void pg_state_machine::note_item_failed(uint64_t log_idx,
                                        uint32_t item_ordinal,
                                        const std::string& reason) {
    if (log_idx == 0) return;
    std::cerr << "SAFE_TASK_FAILED"
              << " log=" << log_idx
              << " ord=" << item_ordinal
              << " reason=" << reason
              << std::endl;
    note_result_item_failed(log_idx, item_ordinal, reason);
}

void pg_state_machine::maybe_advance_prefix_locked() {
    /*
     * Walk the tracker map from (durable_applied_prefix_ + 1) forward,
     * erasing fully-complete entries and advancing the prefix.
     * Stop at the first entry that is not yet complete (gap preserved).
     */
    uint64_t cur = durable_applied_prefix_.load(std::memory_order_relaxed);
    while (true) {
        const uint64_t next = cur + 1;
        auto it = entry_tracker_.find(next);
        if (it == entry_tracker_.end()) break;
        if (!it->second.is_complete()) break;
        cur = next;
        entry_tracker_.erase(it);
    }
    if (cur > durable_applied_prefix_.load(std::memory_order_relaxed)) {
        durable_applied_prefix_.store(cur, std::memory_order_release);
    }
}

} // namespace ariabc_pg
