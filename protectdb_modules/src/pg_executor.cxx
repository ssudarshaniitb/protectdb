#include "pg_executor.hxx"

#include "ariabc_pg_util.hxx"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <climits>
#include <cstdlib>
#include <cerrno>
#include <fcntl.h>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <poll.h>
#include <sstream>
#include <stdexcept>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <signal.h>

#include <cassert>
#include <libpq-fe.h>

#ifndef SSL_LIBRARY_NOT_FOUND
#include <openssl/hmac.h>
#include <openssl/sha.h>
#endif

namespace ariabc_pg {
namespace {

constexpr const char* kBcdbMerkleTag = "BCDB_MERKLE_ROOTS:";
// Majority-confirmed throughput is latency-sensitive: keep batches moderate so
// reply visibility is not delayed too long.
constexpr size_t kKafkaBatchMaxBytes = 512 * 1024;
constexpr size_t kKafkaBatchMaxRecords = 256;
constexpr int kKafkaBatchMaxDelayMs = 1;
// Admission control watermarks are for the *queue depth* (ready+delayed), not
// queue+inflight. In deterministic mode, inflight work is expected; excessive
// queued work is what drives tail latency and straggler behavior.
constexpr size_t kQueueHighWatermarkMin = 32;
constexpr size_t kQueueLowWatermarkMin = 16;
constexpr size_t kQueueHighWatermarkFactor = 4;  // high = max(minHigh, 4*pool)
constexpr size_t kQueueLowWatermarkFactor = 2;   // low  = max(minLow,  2*pool)
// Session-restart heuristic for deterministic-apply ordering: a newly queued
// tx_seq more than this much below the current head means a fresh client
// session (e.g. workload starting at tx_seq=1 after a probe used 99000000)
// rather than late out-of-order arrival inside the same session.
constexpr uint64_t kDetApplyEpochGap = 1000000ULL;
constexpr size_t kDetQueueHighWatermarkMin = 24;
constexpr size_t kDetQueueLowWatermarkMin = 12;
constexpr size_t kDetQueueHighWatermarkFactor = 2;  // high = max(minHigh, 2*pool)
constexpr size_t kDetQueueLowWatermarkFactor = 1;   // low  = max(minLow,  1*pool)
constexpr uint64_t kDefaultDetPartialBlockMaxWaitNs = 2ULL * 1000ULL * 1000ULL;
constexpr uint64_t kDetExplicitTxidMaxSeq = 80000000ULL;
constexpr const char* kHashAlgo = "sha256";
constexpr uint8_t kHashAlgoId = 1;
constexpr const char* kDefaultResultSigKey = "ariabc-result-v2-dev-key";

bool debug_req_trace_enabled() {
    static const bool enabled = []() -> bool {
        const char* v = std::getenv("ARIABC_DEBUG_REQ_TRACE");
        if (!v) return false;
        const std::string s = trim_copy(v);
        return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
    }();
    return enabled;
}

void update_atomic_max(std::atomic<uint64_t>& target, uint64_t value) {
    uint64_t cur = target.load(std::memory_order_relaxed);
    while (value > cur &&
           !target.compare_exchange_weak(cur,
                                         value,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
    }
}

bool safe_trace_enabled(const std::string& ledger_mode) {
    if (ledger_mode == "safe") return true;
    const char* v = std::getenv("ARIABC_SAFE_TRACE");
    if (!v || !*v) return false;
    const std::string s = trim_copy(v);
    return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
}

bool kafka_async_result_publisher_env_enabled() {
    const char* v = std::getenv("ARIABC_KAFKA_ASYNC_RESULT_PUBLISHER");
    if (!v || !*v) return true;
    const std::string s = trim_copy(v);
    return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
}

size_t kafka_async_result_publisher_max_records() {
    const char* v = std::getenv("ARIABC_KAFKA_ASYNC_RESULT_BATCH_RECORDS");
    if (!v || !*v) return 512;
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(v, &end, 10);
    if (errno != 0 || end == v || *end != '\0' || parsed == 0) return 512;
    return static_cast<size_t>(std::min<unsigned long long>(parsed, 4096ULL));
}

size_t kafka_async_result_publisher_target_records() {
    const char* v = std::getenv("ARIABC_KAFKA_RESULT_TARGET_BATCH_RECORDS");
    if (!v || !*v) return 64;
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(v, &end, 10);
    if (errno != 0 || end == v || *end != '\0' || parsed == 0) return 64;
    return static_cast<size_t>(std::min<unsigned long long>(parsed, 4096ULL));
}

size_t kafka_async_result_publisher_max_bytes() {
    const char* v = std::getenv("ARIABC_KAFKA_ASYNC_RESULT_BATCH_BYTES");
    if (!v || !*v) return 1024 * 1024;
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(v, &end, 10);
    if (errno != 0 || end == v || *end != '\0' || parsed == 0) return 1024 * 1024;
    return static_cast<size_t>(std::min<unsigned long long>(parsed, 16ULL * 1024ULL * 1024ULL));
}

int kafka_async_result_publisher_delay_us() {
    const char* shared = std::getenv("ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US");
    if (shared && *shared) {
        char* end = nullptr;
        errno = 0;
        const long parsed = std::strtol(shared, &end, 10);
        if (errno == 0 && end != shared && *end == '\0' && parsed >= 0 && parsed <= 1000000) {
            return static_cast<int>(parsed);
        }
    }
    const char* v = std::getenv("ARIABC_KAFKA_ASYNC_RESULT_BATCH_DELAY_US");
    if (v && *v) {
        char* end = nullptr;
        errno = 0;
        const long parsed = std::strtol(v, &end, 10);
        if (errno == 0 && end != v && *end == '\0' && parsed >= 0 && parsed <= 1000000) {
            return static_cast<int>(parsed);
        }
    }
    return 50;
}

static const int kConfiguredDelayUs = []() -> int {
    const char* e = ::getenv("ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US");

    // Unset or empty means preserve adaptive legacy behavior.
    if (e == nullptr || e[0] == '\0')
        return -1;

    char* end = nullptr;
    errno = 0;
    const long v = std::strtol(e, &end, 10);

    // Accept 0 for immediate flush, and up to 1s.
    if (errno != 0 || end == e || *end != '\0' ||
        v < 0 || v > 1000000)
        return -1;

    return static_cast<int>(v);
}();

bool det_event_block_fastpath_enabled() {
    static const bool enabled = []() -> bool {
        const char* v = std::getenv("ARIABC_DET_EVENT_BLOCK_FASTPATH");
        if (!v) return false;
        const std::string s = trim_copy(v);
        return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
    }();
    return enabled;
}

bool det_allow_raw_compat_mode() {
    static const bool enabled = []() -> bool {
        const char* v = std::getenv("ARIABC_DET_ALLOW_RAW_COMPAT");
        if (!v) return false;
        const std::string s = trim_copy(v);
        return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
    }();
    return enabled;
}

bool det_prefixed_direct_parallel_enabled() {
    static const bool enabled = []() -> bool {
        const char* v = std::getenv("ARIABC_DET_PREFIXED_DIRECT_PARALLEL");
        if (!v) return false;
        const std::string s = trim_copy(v);
        return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
    }();
    return enabled;
}

bool det_completion_only_success_enabled() {
    static const bool enabled = []() -> bool {
        const char* v = std::getenv("ARIABC_DET_COMPLETION_ONLY_SUCCESS");
        if (!v) return false;
        const std::string s = trim_copy(v);
        return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
    }();
    return enabled;
}

bool det_threaded_direct_no_preapply_wait_enabled() {
    static const bool enabled = []() -> bool {
        const char* v = std::getenv("ARIABC_DET_THREADED_DIRECT_NO_PREAPPLY_WAIT");
        if (!v) return false;
        const std::string s = trim_copy(v);
        return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
    }();
    return enabled;
}

uint64_t det_partial_block_max_wait_ns() {
    static const uint64_t wait_ns = []() -> uint64_t {
        const char* v = std::getenv("ARIABC_DET_PARTIAL_BLOCK_MAX_WAIT_US");
        if (!v || !*v) return kDefaultDetPartialBlockMaxWaitNs;

        errno = 0;
        char* end = nullptr;
        const unsigned long long parsed = std::strtoull(v, &end, 10);
        if (end == v || errno != 0) return kDefaultDetPartialBlockMaxWaitNs;
        if (end && *end != '\0' && !trim_copy(end).empty()) {
            return kDefaultDetPartialBlockMaxWaitNs;
        }

        constexpr unsigned long long kMaxWaitUs = 1000000ULL;
        const unsigned long long capped = std::min(parsed, kMaxWaitUs);
        return static_cast<uint64_t>(capped) * 1000ULL;
    }();
    return wait_ns;
}

bool det_block_skip_readonly_enabled() {
    static const bool enabled = []() -> bool {
        const char* v = std::getenv("ARIABC_DET_BLOCK_SKIP_READONLY");
        if (!v) return false;
        const std::string s = trim_copy(v);
        return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
    }();
    return enabled;
}

std::string lower_copy(const std::string& in) {
    std::string out = in;
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return out;
}

static bool is_reset_barrier_sql(const std::string& sql) {
    const std::string t = lower_copy(trim_copy(sql));
    return t.find("bcdb_reset(") != std::string::npos;
}

static bool state_machine_preassigned_ordering_enabled() {
    const char* env = std::getenv("ARIABC_RAFT_ORDERING_POLICY");
    if (!env || !*env) return true;
    const std::string pol = trim_copy(env);
    return pol.empty() || pol == "preassigned";
}

bool sql_is_plain_select(const std::string& sql) {
    const std::string s = trim_copy(sql);
    if (s.size() < 6) return false;
    const char prefix[] = {'s', 'e', 'l', 'e', 'c', 't'};
    for (size_t i = 0; i < 6; ++i) {
        if (static_cast<char>(std::tolower(static_cast<unsigned char>(s[i]))) != prefix[i]) {
            return false;
        }
    }
    return s.size() == 6 ||
           std::isspace(static_cast<unsigned char>(s[6])) ||
           s[6] == '(';
}

uint64_t debug_req_trace_limit() {
    static const uint64_t limit = []() -> uint64_t {
        const char* v = std::getenv("ARIABC_DEBUG_REQ_TRACE_LIMIT");
        if (!v || !*v) return 32;
        try {
            const unsigned long long n = std::stoull(v);
            return (n == 0ULL) ? 32ULL : static_cast<uint64_t>(n);
        } catch (...) {
            return 32ULL;
        }
    }();
    return limit;
}

int full_result_replica_limit() {
    static const int limit = []() -> int {
        const char* v = std::getenv("ARIABC_FULL_RESULT_REPLICA_LIMIT");
        if (!v || !*v) return 0;
        char* end = nullptr;
        const long parsed = std::strtol(v, &end, 10);
        if (end == v) return 0;
        if (parsed < 0) return -1;
        return static_cast<int>(std::min<long>(parsed, 1024));
    }();
    return limit;
}

int result_publish_replica_limit() {
    static const int limit = []() -> int {
        const char* v = std::getenv("ARIABC_RESULT_PUBLISH_REPLICA_LIMIT");
        if (!v || !*v) return 0;
        char* end = nullptr;
        const long parsed = std::strtol(v, &end, 10);
        if (end == v || parsed <= 0) return 0;
        return static_cast<int>(std::min<long>(parsed, 1024));
    }();
    return limit;
}

bool should_publish_kafka_result(int node_id) {
    const int limit = result_publish_replica_limit();
    return limit <= 0 || node_id <= limit;
}

std::atomic<uint64_t> g_debug_exec_trace_count(0);
std::atomic<bool> g_safe_failpoint_fired(false);

void debug_trace_exec(const std::string& req_id,
                      uint64_t raft_log_idx,
                      const std::string& out) {
    if (!debug_req_trace_enabled()) return;
    const uint64_t idx = g_debug_exec_trace_count.fetch_add(1, std::memory_order_relaxed);
    if (idx >= debug_req_trace_limit()) return;
    std::string out_head = trim_copy(out);
    if (out_head.size() > 96) out_head.resize(96);
    std::cerr << "REQ_TRACE exec"
              << " idx=" << idx
              << " req_id=" << req_id
              << " raft_log_idx=" << raft_log_idx
              << " out=" << out_head
              << std::endl;
}

bool is_retryable_sqlstate(const char* sqlstate) {
    if (!sqlstate) return false;
    // 40001: serialization_failure, 40P01: deadlock_detected, 57014: query_canceled.
    return (strcmp(sqlstate, "40001") == 0) ||
           (strcmp(sqlstate, "40P01") == 0) ||
           (strcmp(sqlstate, "57014") == 0);
}

std::string format_result(PGresult* res) {
    if (!res) return "ERROR null_result";
    const ExecStatusType st = PQresultStatus(res);
    if (st == PGRES_TUPLES_OK) {
        const char* tag = PQcmdStatus(res);
        std::string out = tag ? std::string(tag) : std::string("SELECT");
        const int rows = PQntuples(res);
        const int cols = PQnfields(res);
        if (rows <= 0 || cols <= 0) return out;

        // Sort row indices by lexicographic value of the row text,
        // avoiding a per-row std::string copy and a post-sort re-append.
        // Row-text format is " <v0> <v1> ..." (space then value, per column).
        std::vector<int> order;
        order.reserve(static_cast<size_t>(rows));
        for (int r = 0; r < rows; ++r) order.push_back(r);

        auto cmp = [&](int a, int b) {
            for (int c = 0; c < cols; ++c) {
                const char* va = PQgetvalue(res, a, c);
                const char* vb = PQgetvalue(res, b, c);
                const int la = PQgetlength(res, a, c);
                const int lb = PQgetlength(res, b, c);
                // Each field is prefixed by ' ' in the canonical row text;
                // the leading-space is identical across rows so skip it.
                const int n = std::min(la, lb);
                const int d = memcmp(va ? va : "", vb ? vb : "", static_cast<size_t>(std::max(0, n)));
                if (d != 0) return d < 0;
                if (la != lb) return la < lb;
            }
            return false;
        };
        std::sort(order.begin(), order.end(), cmp);

        // Single pass: reserve exact byte size, then append once per field.
        size_t total = out.size();
        for (int r = 0; r < rows; ++r) {
            for (int c = 0; c < cols; ++c) {
                total += 1u + static_cast<size_t>(std::max(0, PQgetlength(res, r, c)));
            }
        }
        out.reserve(total);
        for (int r : order) {
            for (int c = 0; c < cols; ++c) {
                out.push_back(' ');
                const char* v = PQgetvalue(res, r, c);
                const int n = PQgetlength(res, r, c);
                if (v && n > 0) out.append(v, static_cast<size_t>(n));
            }
        }
        return out;
    }
    if (st == PGRES_COMMAND_OK) {
        return PQcmdStatus(res) ? PQcmdStatus(res) : "OK";
    }
    const char* msg = PQresultErrorMessage(res);
    std::string out = "ERROR ";
    if (msg && *msg) out.append(trim_copy(msg));
    else out.append("unknown");
    return out;
}

bool parse_req_num(const std::string& req_id, uint64_t& out_req_num) {
    out_req_num = 0;
    const size_t dash = req_id.rfind('-');
    if (dash == std::string::npos || dash + 1 >= req_id.size()) return false;
    try {
        out_req_num = static_cast<uint64_t>(std::stoull(req_id.substr(dash + 1)));
        return true;
    } catch (...) {
        return false;
    }
}

bool parse_det_prefixed_sql_parts(const std::string& sql,
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
    size_t digits_begin = pos;
    while (pos < t.size() && std::isdigit(static_cast<unsigned char>(t[pos]))) ++pos;

    uint64_t seq = 0;
    if (pos > digits_begin && pos < t.size() && std::isspace(static_cast<unsigned char>(t[pos]))) {
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

std::string maybe_strip_det_prefix_for_compat(const std::string& sql,
                                              bool det_raw_compat_mode,
                                              int db_type) {
    if (!det_raw_compat_mode || db_type != 1) {
        return sql;
    }
    uint64_t seq = 0;
    std::string raw_sql;
    if (!parse_det_prefixed_sql_parts(sql, &seq, &raw_sql)) {
        return sql;
    }
    if (raw_sql.empty()) {
        return sql;
    }
    return raw_sql;
}

std::string det_sql_prefix_for_profile(const std::string& sql) {
    std::string head = trim_copy(sql);
    if (head.size() > 64) {
        head.resize(64);
    }
    return head;
}

void json_escape_append(std::string& out, const std::string& in) {
    static const char kHex[] = "0123456789abcdef";
    out.reserve(out.size() + in.size() + 8);
    for (unsigned char ch : in) {
        switch (ch) {
            case '\\': out.append("\\\\", 2); break;
            case '"':  out.append("\\\"", 2); break;
            case '\b': out.append("\\b", 2); break;
            case '\f': out.append("\\f", 2); break;
            case '\n': out.append("\\n", 2); break;
            case '\r': out.append("\\r", 2); break;
            case '\t': out.append("\\t", 2); break;
            default:
                if (ch < 0x20) {
                    char buf[6] = {'\\','u','0','0', kHex[(ch >> 4) & 0xF], kHex[ch & 0xF]};
                    out.append(buf, 6);
                } else {
                    out.push_back(static_cast<char>(ch));
                }
                break;
        }
    }
}

std::string json_escape(const std::string& in) {
    std::string out;
    json_escape_append(out, in);
    return out;
}

std::string sql_escape_literal(const std::string& in) {
    std::string out;
    out.reserve(in.size() + 16);
    for (char ch : in) {
        if (ch == '\'') out += "''";
        else out.push_back(ch);
    }
    return out;
}

bool is_lower_hex_64(const std::string& in) {
    if (in.size() != 64) return false;
    for (char ch : in) {
        if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'))) {
            return false;
        }
    }
    return true;
}

void log_safe_metadata_submit(const std::vector<pg_executor::task>& tasks) {
    for (const auto& t : tasks) {
        if (t.raft_log_idx == 0) continue;
        std::cerr << "SAFE_METADATA_SUBMIT"
                  << " req=" << t.req_id
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal << "/" << t.raft_item_count
                  << " entry_digest_prefix=" << t.entry_digest.substr(0, std::min<size_t>(8, t.entry_digest.size()))
                  << " item_digest_prefix=" << t.item_digest.substr(0, std::min<size_t>(8, t.item_digest.size()))
                  << std::endl;
    }
}

std::string build_bcdb_block_submit_results_sql(
    uint64_t block_id,
    const std::vector<std::pair<std::string, std::string>>& txs,
    const std::vector<pg_executor::task>& tasks,
    const std::vector<int64_t>* explicit_txids = nullptr,
    const std::string& raft_epoch_hex = "",
    bool safe_mode = false)
{
    // Estimate JSON size: 32 (bid prefix) + per-tx overhead + hash/sql bodies.
    size_t est = 64;
    const bool use_explicit_txids =
        explicit_txids != nullptr && explicit_txids->size() == txs.size();
    if (safe_mode) {
        if (tasks.size() != txs.size()) {
            throw std::runtime_error("SAFE_METADATA_ALIGNMENT_FAILED: task/tx count mismatch");
        }
        if (!is_lower_hex_64(raft_epoch_hex)) {
            throw std::runtime_error("SAFE_METADATA_MISSING: invalid or missing raft epoch");
        }
    }
    for (size_t i = 0; i < txs.size(); ++i) {
        est += 256 + txs[i].first.size() + 2 * txs[i].second.size();
    }

    std::string json;
    json.reserve(est);
    json.append("{\"bid\":");
    json.append(std::to_string(block_id));
    json.append(",\"txs\":[");
    for (size_t i = 0; i < txs.size(); ++i) {
        if (i) json.push_back(',');
        json.append("{\"hash\":\"");
        json_escape_append(json, txs[i].first);
        json.append("\",\"sql\":\"");
        json_escape_append(json, txs[i].second);

        // Safe mode must never degrade to a metadata-less backend submission.
        // raft_log_index is emitted as a decimal string to preserve exact
        // uint64 precision above 2^53 when parsed with strtoull on the backend.
        if (safe_mode) {
            const auto& t = tasks[i];
            if (t.raft_log_idx == 0 ||
                t.raft_item_count == 0 ||
                t.raft_item_ordinal >= t.raft_item_count ||
                !is_lower_hex_64(t.entry_digest) ||
                !is_lower_hex_64(t.item_digest)) {
                std::ostringstream oss;
                oss << "SAFE_METADATA_MISSING: refusing metadata-less backend submission"
                    << " req=" << t.req_id
                    << " log=" << t.raft_log_idx
                    << " ord=" << t.raft_item_ordinal << "/" << t.raft_item_count
                    << " entry_digest_len=" << t.entry_digest.size()
                    << " item_digest_len=" << t.item_digest.size();
                throw std::runtime_error(oss.str());
            }
            json.append("\",\"raft_ledger_required\":true");
            json.append(",\"raft_log_index\":\"");
            json.append(std::to_string(t.raft_log_idx));
            json.append("\",\"raft_item_ordinal\":");
            json.append(std::to_string(t.raft_item_ordinal));
            json.append(",\"raft_item_count\":");
            json.append(std::to_string(t.raft_item_count));
            json.append(",\"raft_epoch_id\":\"");
            json_escape_append(json, raft_epoch_hex);
            json.append("\",\"entry_digest\":\"");
            json_escape_append(json, t.entry_digest);
            json.append("\",\"item_digest\":\"");
            json_escape_append(json, t.item_digest);
        } else if (!raft_epoch_hex.empty() && i < tasks.size()) {
            const auto& t = tasks[i];
            if (!t.entry_digest.empty() && !t.item_digest.empty() &&
                t.entry_digest.size() == 64 && t.item_digest.size() == 64 &&
                t.raft_log_idx != 0) {
                json.append("\",\"raft_log_index\":\"");
                json.append(std::to_string(t.raft_log_idx));
                json.append("\",\"raft_item_ordinal\":");
                json.append(std::to_string(t.raft_item_ordinal));
                json.append(",\"raft_item_count\":");
                json.append(std::to_string(t.raft_item_count));
                json.append(",\"raft_epoch_id\":\"");
                json_escape_append(json, raft_epoch_hex);
                json.append("\",\"entry_digest\":\"");
                json_escape_append(json, t.entry_digest);
                json.append("\",\"item_digest\":\"");
                json_escape_append(json, t.item_digest);
            }
        }

        if (use_explicit_txids) {
            json.append("\",\"txid\":");
            json.append(std::to_string((*explicit_txids)[i]));
            json.append("}");
        } else {
            json.append("\"}");
        }
    }
    json.append("]}");

    std::string sql;
    sql.reserve(json.size() * 2 + 48);
    sql.append("SELECT bcdb_block_submit_results('");
    // Inline sql_escape_literal to avoid an extra allocation+copy.
    for (char ch : json) {
        if (ch == '\'') sql.append("''", 2);
        else sql.push_back(ch);
    }
    sql.append("');");
    return sql;
}

bool decode_hex_char(char ch, uint8_t& out) {
    if (ch >= '0' && ch <= '9') {
        out = static_cast<uint8_t>(ch - '0');
        return true;
    }
    if (ch >= 'a' && ch <= 'f') {
        out = static_cast<uint8_t>(10 + (ch - 'a'));
        return true;
    }
    if (ch >= 'A' && ch <= 'F') {
        out = static_cast<uint8_t>(10 + (ch - 'A'));
        return true;
    }
    return false;
}

bool hex_decode_string(const std::string& in, std::string& out) {
    if ((in.size() % 2) != 0) return false;
    out.clear();
    out.reserve(in.size() / 2);
    for (size_t i = 0; i < in.size(); i += 2) {
        uint8_t hi = 0;
        uint8_t lo = 0;
        if (!decode_hex_char(in[i], hi) || !decode_hex_char(in[i + 1], lo)) {
            out.clear();
            return false;
        }
        out.push_back(static_cast<char>((hi << 4) | lo));
    }
    return true;
}

bool parse_bcdb_block_results_text(
    const std::string& payload,
    std::unordered_map<std::string, std::string>& out_by_hash)
{
    out_by_hash.clear();
    size_t pos = 0;
    while (pos < payload.size()) {
        const size_t nl = payload.find('\n', pos);
        const size_t end = (nl == std::string::npos) ? payload.size() : nl;
        if (end > pos) {
            const size_t tab = payload.find('\t', pos);
            if (tab == std::string::npos || tab >= end) return false;
            const std::string hash = payload.substr(pos, tab - pos);
            const std::string hex = payload.substr(tab + 1, end - (tab + 1));
            std::string decoded;
            if (!hex_decode_string(hex, decoded)) return false;
            out_by_hash.emplace(hash, std::move(decoded));
        }
        pos = (nl == std::string::npos) ? payload.size() : (nl + 1);
    }
    return true;
}

bool parse_confirmed_result_strict(const std::string& raw_result, pg_executor::ConfirmedResult* out) {
    if (out) *out = pg_executor::ConfirmedResult();

    const std::string commit_header = "[BCDB_RAFT_COMMIT_CONFIRMED]\n";
    const std::string failure_header = "[BCDB_RAFT_FAILURE_NOTICE]\n";
    const bool is_commit = raw_result.compare(0, commit_header.size(), commit_header) == 0;
    const bool is_failure = raw_result.compare(0, failure_header.size(), failure_header) == 0;
    const std::string header = is_commit ? commit_header : failure_header;
    if (!is_commit && !is_failure) {
        return false;
    }

    const size_t payload_marker_len = std::string("\n[PAYLOAD]\n").size();
    size_t payload_pos = raw_result.find("\n[PAYLOAD]\n", header.size());
    if (payload_pos == std::string::npos) {
        return false;
    }

    std::string metadata = raw_result.substr(header.size(), payload_pos - header.size() + 1);
    std::string payload = raw_result.substr(payload_pos + payload_marker_len);

    std::vector<std::string> lines;
    size_t start = 0;
    while (true) {
        size_t next_nl = metadata.find('\n', start);
        if (next_nl == std::string::npos) break;
        std::string line = metadata.substr(start, next_nl - start);
        while (!line.empty() && std::isspace((unsigned char)line.back())) line.pop_back();
        while (!line.empty() && std::isspace((unsigned char)line.front())) line.erase(0, 1);
        if (!line.empty()) {
            lines.push_back(line);
        }
        start = next_nl + 1;
    }

    std::map<std::string, std::string> meta;

    for (const auto& line : lines) {
        size_t eq = line.find('=');
        if (eq == std::string::npos) {
            return false;
        }
        std::string key = line.substr(0, eq);
        std::string val = line.substr(eq + 1);
        if (!meta.emplace(key, val).second) return false;
    }

    auto is_uint = [](const std::string& s) -> bool {
        if (s.empty()) return false;
        for (char c : s) if (!std::isdigit((unsigned char)c)) return false;
        return true;
    };
    auto is_hex64 = [](const std::string& s) -> bool {
        if (s.size() != 64) return false;
        for (char c : s) {
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
        }
        return true;
    };
    auto get_required = [&](const std::string& key, std::string& out_val) -> bool {
        auto it = meta.find(key);
        if (it == meta.end()) return false;
        out_val = it->second;
        return true;
    };
    auto parse_common_identity = [&](uint64_t& log_idx, uint32_t& ordinal) -> bool {
        std::string v;
        if (!get_required("raft_log_index", v) || !is_uint(v)) return false;
        try {
            log_idx = std::stoull(v);
        } catch (...) {
            return false;
        }
        if (!get_required("raft_item_ordinal", v) || !is_uint(v)) return false;
        try {
            unsigned long temp = std::stoul(v);
            if (temp > 0xFFFFFFFFUL) return false;
            ordinal = static_cast<uint32_t>(temp);
        } catch (...) {
            return false;
        }
        return true;
    };
    auto parse_payload_kv = [&](std::map<std::string, std::string>& kv) -> bool {
        size_t p = 0;
        while (p < payload.size()) {
            const size_t nl = payload.find('\n', p);
            const size_t end = (nl == std::string::npos) ? payload.size() : nl;
            if (end > p) {
                std::string line = payload.substr(p, end - p);
                const size_t eq = line.find('=');
                if (eq == std::string::npos) return false;
                if (!kv.emplace(line.substr(0, eq), line.substr(eq + 1)).second) return false;
            }
            if (nl == std::string::npos) break;
            p = nl + 1;
        }
        return true;
    };

    uint64_t log_idx = 0;
    uint32_t ordinal = 0;
    if (!parse_common_identity(log_idx, ordinal)) return false;

    if (is_commit) {
        std::string confirmed;
        std::string digest;
        std::string state;
        std::string fmt;
        if (meta.size() != 6) return false;
        if (!get_required("postgres_commit_confirmed", confirmed) || confirmed != "1") return false;
        if (!get_required("terminal_digest", digest) || !is_hex64(digest)) return false;
        if (!get_required("terminal_state", state)) return false;
        if (state != "OK" && state != "ERROR") return false;
        if (!get_required("terminal_format_version", fmt) || fmt != "1") return false;

        if (out) {
            out->raft_log_index = log_idx;
            out->raft_item_ordinal = ordinal;
            out->terminal_digest = digest;
            out->terminal_state = state;
            out->payload = payload;
            out->format_version = 1;
        }
        return true;
    }

    std::string digest;
    std::string state;
    std::string fmt;
    std::string notice_committed;
    std::string pg_confirmed;
    if (meta.size() != 7) return false;
    if (meta.find("terminal_digest") != meta.end() ||
        meta.find("terminal_state") != meta.end() ||
        meta.find("terminal_format_version") != meta.end()) return false;
    if (!get_required("failure_digest", digest) || !is_hex64(digest)) return false;
    if (!get_required("outcome_state", state) || state != "NONTERMINAL_FAILURE") return false;
    if (!get_required("failure_format_version", fmt) || fmt != "1") return false;
    if (!get_required("failure_notice_committed", notice_committed) || notice_committed != "1") return false;
    if (!get_required("postgres_commit_confirmed", pg_confirmed) || pg_confirmed != "0") return false;

    std::map<std::string, std::string> payload_kv;
    if (!parse_payload_kv(payload_kv) || payload_kv.size() != 3) return false;
    auto p_sqlstate = payload_kv.find("sqlstate");
    auto p_class = payload_kv.find("failure_class");
    auto p_retryable = payload_kv.find("retryable");
    if (p_sqlstate == payload_kv.end() || p_class == payload_kv.end() ||
        p_retryable == payload_kv.end()) return false;
    if (p_sqlstate->second.size() != 5) return false;
    for (char c : p_sqlstate->second) {
        if (!((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z'))) return false;
    }
    if (p_class->second.empty()) return false;
    if (p_retryable->second != "0" && p_retryable->second != "1") return false;

    if (out) {
        out->raft_log_index = log_idx;
        out->raft_item_ordinal = ordinal;
        out->terminal_digest = digest;
        out->terminal_state = "NONTERMINAL_FAILURE";
        out->failure_sqlstate = p_sqlstate->second;
        out->failure_class = p_class->second;
        out->failure_retryable = (p_retryable->second == "1");
        out->payload = payload;
        out->format_version = 1;
    }
    return true;
}

bool parse_confirmed_result(const std::string& raw_result,
                            uint64_t* out_log_idx,
                            uint32_t* out_ordinal,
                            std::string* out_terminal_digest,
                            std::string* out_payload) {
    pg_executor::ConfirmedResult res;
    if (!parse_confirmed_result_strict(raw_result, &res)) {
        if (out_payload) *out_payload = raw_result;
        return false;
    }
    if (out_log_idx) *out_log_idx = res.raft_log_index;
    if (out_ordinal) *out_ordinal = res.raft_item_ordinal;
    if (out_terminal_digest) *out_terminal_digest = res.terminal_digest;
    if (out_payload) *out_payload = res.payload;
    return true;
}

std::string hex_encode_bytes(const std::string& in) {
    static const char* kHex = "0123456789abcdef";
    std::string out;
    out.reserve(in.size() * 2);
    for (unsigned char ch : in) {
        out.push_back(kHex[ch >> 4]);
        out.push_back(kHex[ch & 0x0f]);
    }
    return out;
}

bool extract_terminal_sqlstate(const std::string& payload, std::string* out) {
    const std::string key = "sqlstate=";
    const size_t pos = payload.find(key);
    if (pos == std::string::npos || pos + key.size() + 5 > payload.size()) {
        if (out) *out = "XX000";
        return true;
    }
    const std::string state = payload.substr(pos + key.size(), 5);
    for (char c : state) {
        if (!((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z'))) {
            return false;
        }
    }
    if (out) *out = state;
    return true;
}

void append_u16_le(std::string& out, uint16_t v) {
    out.push_back(static_cast<char>(v & 0xFFu));
    out.push_back(static_cast<char>((v >> 8) & 0xFFu));
}

void append_u8(std::string& out, uint8_t v) {
    out.push_back(static_cast<char>(v));
}

void append_u32_le(std::string& out, uint32_t v) {
    out.push_back(static_cast<char>(v & 0xFFu));
    out.push_back(static_cast<char>((v >> 8) & 0xFFu));
    out.push_back(static_cast<char>((v >> 16) & 0xFFu));
    out.push_back(static_cast<char>((v >> 24) & 0xFFu));
}

void append_u64_le(std::string& out, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<char>((v >> (8 * i)) & 0xFFu));
    }
}

std::string hex_encode(const unsigned char* data, size_t len) {
    static const char* kHex = "0123456789abcdef";
    std::string out;
    out.resize(len * 2);
    for (size_t i = 0; i < len; ++i) {
        const unsigned char b = data[i];
        out[2 * i] = kHex[(b >> 4) & 0xF];
        out[2 * i + 1] = kHex[b & 0xF];
    }
    return out;
}

[[maybe_unused]] std::string fnv1a64_hex(const std::string& in) {
    uint64_t h = 1469598103934665603ULL;
    for (unsigned char c : in) {
        h ^= static_cast<uint64_t>(c);
        h *= 1099511628211ULL;
    }
    std::ostringstream oss;
    oss << std::hex << std::setw(16) << std::setfill('0') << h;
    return oss.str();
}

std::string canonical_result_hash(const std::string& result) {
#ifndef SSL_LIBRARY_NOT_FOUND
    unsigned char md[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(result.data()),
           static_cast<size_t>(result.size()),
           md);
    return hex_encode(md, SHA256_DIGEST_LENGTH);
#else
    return fnv1a64_hex(result);
#endif
}

std::string sign_payload(const std::string& key, const std::string& payload) {
#ifndef SSL_LIBRARY_NOT_FOUND
    unsigned int out_len = 0;
    unsigned char mac[EVP_MAX_MD_SIZE];
    unsigned char* out = HMAC(EVP_sha256(),
                              reinterpret_cast<const unsigned char*>(key.data()),
                              static_cast<int>(key.size()),
                              reinterpret_cast<const unsigned char*>(payload.data()),
                              static_cast<int>(payload.size()),
                              mac,
                              &out_len);
    if (!out || out_len == 0) return "";
    return hex_encode(mac, static_cast<size_t>(out_len));
#else
    return fnv1a64_hex(key + "|" + payload);
#endif
}

uint64_t now_epoch_ms() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

uint64_t now_steady_ns() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

std::string make_sig_payload(uint64_t req_num,
                             uint64_t raft_log_idx,
                             const std::string& req_id,
                             int node_id,
                             int leader_node_id,
                             const std::string& result_hash,
                             uint64_t timestamp_ms,
                             bool has_full_result)
{
    std::ostringstream oss;
    oss << req_num << "|"
        << raft_log_idx << "|"
        << req_id << "|"
        << node_id << "|"
        << leader_node_id << "|"
        << result_hash << "|"
        << kHashAlgo << "|"
        << timestamp_ms << "|"
        << (has_full_result ? 1 : 0);
    return oss.str();
}

std::string make_sig_payload_v4(uint64_t req_num,
                                 uint64_t raft_log_idx,
                                 uint32_t raft_item_ordinal,
                                 const std::string& req_id,
                                 int node_id,
                                 int leader_node_id,
                                 const std::string& epoch_hex,
                                 const std::string& terminal_state,
                                 const std::string& terminal_digest,
                                 int format_version,
                                 uint64_t timestamp_ms,
                                 bool has_full_result)
{
    std::ostringstream oss;
    oss << req_num << "|"
        << raft_log_idx << "|"
        << raft_item_ordinal << "|"
        << req_id << "|"
        << node_id << "|"
        << leader_node_id << "|"
        << epoch_hex << "|"
        << terminal_state << "|"
        << terminal_digest << "|"
        << format_version << "|"
        << timestamp_ms << "|"
        << (has_full_result ? 1 : 0);
    return oss.str();
}

std::string build_bin_batch_payload_v2(const std::vector<std::string>& req_ids,
                                       const std::vector<std::string>& results,
                                       const std::vector<uint64_t>& raft_log_idxs,
                                       const std::vector<int>& leader_node_hints,
                                       const std::vector<std::string>& terminal_digests,
                                       const std::vector<uint32_t>& raft_item_ordinals,
                                       const std::vector<std::string>& terminal_states,
                                       const std::vector<int>& format_versions,
                                       uint16_t node_id,
                                       const std::string& sig_key,
                                       const std::string& raft_epoch_hex,
                                       bool safe_ledger_mode)
{
    std::string out;
    out.reserve(8 + req_ids.size() * 128);

    // Encode to B4 only if safe ledger mode is active.
    if (safe_ledger_mode) {
        if (safe_ledger_mode && raft_epoch_hex.empty()) {
            std::cerr << "build_bin_batch_payload_v2: FATAL: epoch is empty in safe mode" << std::endl;
            std::abort();
        }
        // B4 format
        out.push_back('B');
        out.push_back('4');
        append_u16_le(out, static_cast<uint16_t>(req_ids.size()));
        append_u32_le(out, 0u); // reserved

        std::vector<uint8_t> epoch_bytes = decode_hex_string(raft_epoch_hex);
        if (epoch_bytes.size() != 32) {
            if (safe_ledger_mode) {
                std::cerr << "build_bin_batch_payload_v2: FATAL: invalid epoch length for epoch=" << raft_epoch_hex << std::endl;
                std::abort();
            }
            epoch_bytes.assign(32, 0);
        }

        for (size_t i = 0; i < req_ids.size(); ++i) {
            uint64_t req_num = 0;
            if (!parse_req_num(req_ids[i], req_num)) {
                if (safe_ledger_mode) {
                    std::cerr << "build_bin_batch_payload_v2: FATAL: cannot parse req_num from req_id='" << req_ids[i] << "' in safe mode" << std::endl;
                    std::abort();
                }
                req_num = static_cast<uint64_t>(std::hash<std::string>{}(req_ids[i]));
            }

            const int leader_node_id = (i < leader_node_hints.size()) ? leader_node_hints[i] : -1;
            const uint64_t raft_log_idx = (i < raft_log_idxs.size()) ? raft_log_idxs[i] : 0;
            const uint32_t raft_item_ordinal = (i < raft_item_ordinals.size()) ? raft_item_ordinals[i] : 0;
            const std::string& term_state = (i < terminal_states.size()) ? terminal_states[i] : "OK";
            const int format_ver = (i < format_versions.size()) ? format_versions[i] : 1;

            if (term_state != "OK" && term_state != "ERROR" && term_state != "NONTERMINAL_FAILURE") {
                std::cerr << "build_bin_batch_payload_v2: FATAL: invalid terminal_state='"
                          << term_state << "' in safe mode for req_id="
                          << req_ids[i] << " log_idx=" << raft_log_idx
                          << " ordinal=" << raft_item_ordinal << std::endl;
                std::abort();
            }
            if (format_ver != 1) {
                std::cerr << "build_bin_batch_payload_v2: FATAL: unsupported terminal_format_version="
                          << format_ver << " in safe mode for req_id="
                          << req_ids[i] << " log_idx=" << raft_log_idx
                          << " ordinal=" << raft_item_ordinal << std::endl;
                std::abort();
            }

            const int full_result_limit = full_result_replica_limit();
            const bool include_full_result =
                (full_result_limit == 0 ||
                 (full_result_limit > 0 && static_cast<int>(node_id) <= full_result_limit));
            const uint64_t ts_ms = now_epoch_ms();

            std::string term_digest_hex;
            if (i < terminal_digests.size() && terminal_digests[i].size() == 64) {
                term_digest_hex = terminal_digests[i];
            } else {
                if (safe_ledger_mode) {
                    std::cerr << "build_bin_batch_payload_v2: FATAL: missing 64-hex terminal digest in safe mode for req_id="
                              << req_ids[i] << " log_idx=" << raft_log_idx << std::endl;
                    std::abort();
                }
                term_digest_hex = canonical_result_hash(results[i]);
            }
            std::vector<uint8_t> digest_bytes = decode_hex_string(term_digest_hex);
            if (digest_bytes.size() != 32) {
                if (safe_ledger_mode) {
                    std::cerr << "build_bin_batch_payload_v2: FATAL: invalid terminal digest size for term_digest_hex=" << term_digest_hex << std::endl;
                    std::abort();
                }
                digest_bytes.assign(32, 0);
            }

            const std::string sig_payload = make_sig_payload_v4(
                req_num,
                raft_log_idx,
                raft_item_ordinal,
                req_ids[i],
                static_cast<int>(node_id),
                leader_node_id,
                raft_epoch_hex,
                term_state,
                term_digest_hex,
                format_ver,
                ts_ms,
                include_full_result
            );
            const std::string sig = sign_payload(sig_key, sig_payload);
            const uint8_t flags = include_full_result ? 0x1u : 0x0u;

            // fixed part of record
            append_u64_le(out, req_num);
            append_u64_le(out, raft_log_idx);
            append_u32_le(out, raft_item_ordinal);
            append_u16_le(out, node_id);
            append_u16_le(out, static_cast<uint16_t>(leader_node_id > 0 ? leader_node_id : 0));
            append_u64_le(out, ts_ms);
            append_u8(out, flags);
            append_u8(out, kHashAlgoId);
            uint8_t state_code = 1u;
            if (term_state == "ERROR") {
                state_code = 2u;
            } else if (term_state == "NONTERMINAL_FAILURE") {
                state_code = 3u;
            }
            append_u8(out, state_code);
            append_u32_le(out, static_cast<uint32_t>(format_ver));
            append_u16_le(out, static_cast<uint16_t>(req_ids[i].size()));
            append_u16_le(out, static_cast<uint16_t>(sig.size()));
            append_u32_le(out, static_cast<uint32_t>(include_full_result ? results[i].size() : 0));

            // variable part of record
            out.append(reinterpret_cast<const char*>(epoch_bytes.data()), 32);
            out.append(reinterpret_cast<const char*>(digest_bytes.data()), 32);
            out.append(req_ids[i]);
            out.append(sig);
            if (include_full_result) {
                out.append(results[i]);
            }
        }
        return out;
    }

    // B3 format fallback
    out.push_back('B');
    out.push_back('3');
    append_u16_le(out, static_cast<uint16_t>(req_ids.size()));
    append_u32_le(out, 0u); // reserved

    for (size_t i = 0; i < req_ids.size(); ++i) {
        uint64_t req_num = 0;
        if (!parse_req_num(req_ids[i], req_num)) {
            if (safe_ledger_mode) {
                std::cerr << "build_bin_batch_payload: FATAL: cannot parse req_num from req_id='" << req_ids[i] << "' in safe mode" << std::endl;
                std::abort();
            }
            // Fallback hash keeps the pipeline alive for unexpected req ids in legacy mode.
            req_num = static_cast<uint64_t>(std::hash<std::string>{}(req_ids[i]));
        }

        const int leader_node_id = (i < leader_node_hints.size()) ? leader_node_hints[i] : -1;
        const uint64_t raft_log_idx = (i < raft_log_idxs.size()) ? raft_log_idxs[i] : 0;
        const int full_result_limit = full_result_replica_limit();
        const bool include_full_result =
            (full_result_limit == 0 ||
             (full_result_limit > 0 && static_cast<int>(node_id) <= full_result_limit));
        const uint64_t ts_ms = now_epoch_ms();
        std::string result_hash;
        if (i < terminal_digests.size() && terminal_digests[i].size() == 64) {
            result_hash = terminal_digests[i];
        } else {
            result_hash = canonical_result_hash(results[i]);
        }
        const std::string sig_payload = make_sig_payload(req_num,
                                                         raft_log_idx,
                                                         req_ids[i],
                                                         static_cast<int>(node_id),
                                                         leader_node_id,
                                                         result_hash,
                                                         ts_ms,
                                                         include_full_result);
        const std::string sig = sign_payload(sig_key, sig_payload);
        const uint8_t flags = include_full_result ? 0x1u : 0x0u;

        append_u64_le(out, req_num);
        append_u64_le(out, raft_log_idx);
        append_u16_le(out, node_id);
        append_u16_le(out, static_cast<uint16_t>(leader_node_id > 0 ? leader_node_id : 0));
        append_u64_le(out, ts_ms);
        append_u8(out, flags);
        append_u8(out, kHashAlgoId);
        append_u16_le(out, static_cast<uint16_t>(req_ids[i].size()));
        append_u16_le(out, static_cast<uint16_t>(result_hash.size()));
        append_u16_le(out, static_cast<uint16_t>(sig.size()));
        append_u32_le(out, static_cast<uint32_t>(include_full_result ? results[i].size() : 0));
        out.append(req_ids[i]);
        out.append(result_hash);
        out.append(sig);
        if (include_full_result) {
            out.append(results[i]);
        }
    }
    return out;
}

static std::string hex_preview(const std::string& input)
{
    static const char digits[] = "0123456789abcdef";
    const std::size_t limit = input.size() > 128 ? 128 : input.size();

    std::string out;
    out.reserve(limit * 2 + 3);

    for (std::size_t i = 0; i < limit; ++i)
    {
        const unsigned char c =
            static_cast<unsigned char>(input[i]);

        out.push_back(digits[(c >> 4) & 0x0f]);
        out.push_back(digits[c & 0x0f]);
    }

    if (input.size() > limit)
        out += "...";

    return out;
}

bool safe_failpoint_matches(const char* name,
                            int node_id,
                            uint64_t raft_log_idx,
                            uint32_t item_ordinal)
{
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
    return g_safe_failpoint_fired.compare_exchange_strong(
        expected, true, std::memory_order_acq_rel, std::memory_order_acquire);
}

void trigger_safe_failpoint(const char* name,
                            int node_id,
                            uint64_t raft_log_idx,
                            uint32_t item_ordinal)
{
    if (!safe_failpoint_matches(name, node_id, raft_log_idx, item_ordinal)) {
        return;
    }

    const char* cluster_id = std::getenv("ARIABC_RAFT_CLUSTER_ID");
    if (!cluster_id || cluster_id[0] == '\0') {
        cluster_id = "unknown_cluster";
    }
    const char* epoch_hex = std::getenv("ARIABC_RAFT_EPOCH_HEX");
    if (!epoch_hex || epoch_hex[0] == '\0') {
        epoch_hex = "unknown_epoch";
    }
    const char* phase = "";
    if (::strcmp(name, "ARIABC_FAILPOINT_AFTER_RESULT_RING_BEFORE_KAFKA_PUBLISH") == 0) {
        phase = "after_result_ring_before_kafka_publish";
    } else if (::strcmp(name, "ARIABC_FAILPOINT_AFTER_KAFKA_PUBLISH_BEFORE_APPLIED_MARK") == 0) {
        phase = "after_kafka_publish_before_applied_mark";
    }

    std::cerr << "SAFE_FAILPOINT_TRIGGERED name=" << name
              << " phase=" << phase
              << " node=" << node_id
              << " log=" << raft_log_idx
              << " ordinal=" << item_ordinal
              << " pid=" << ::getpid()
              << " epoch=" << epoch_hex
              << " cluster_id=" << cluster_id
              << std::endl;
    ::kill(::getpid(), SIGKILL);
}

} // namespace

pg_executor::ConfirmedResult pg_executor::accept_safe_confirmed_result(const pg_executor::task& t, const std::string& raw_backend_result) {
    if (t.raft_log_idx == 0) {
        ConfirmedResult res;
        res.raft_log_index = 0;
        res.raft_item_ordinal = t.raft_item_ordinal;
        res.terminal_digest = "";
        res.terminal_state = "OK";
        res.payload = raw_backend_result;
        res.format_version = 1;
        return res;
    }

    if (db_opt_.raft_apply_ledger_mode != "safe") {
        ConfirmedResult res;
        res.raft_log_index = t.raft_log_idx;
        res.raft_item_ordinal = t.raft_item_ordinal;
        res.terminal_digest = "";
        res.terminal_state = "OK";
        res.payload = raw_backend_result;
        res.format_version = 1;
        return res;
    }

    ConfirmedResult res;
    if (!parse_confirmed_result_strict(raw_backend_result, &res)) {
        std::cerr << "SAFE_PROTOCOL_FAILURE"
                  << " req_id=" << t.req_id
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal
                  << " reason=parse_failed"
                  << " bytes=" << raw_backend_result.size()
                  << " raw_hex=" << hex_preview(raw_backend_result)
                  << std::endl;
        // Fail closed: surface a deterministic failure without crashing all
        // three server processes.  The task is marked with an invalid index
        // so the caller can detect and skip Kafka publish.
        res.raft_log_index = static_cast<uint64_t>(-1);
        res.raft_item_ordinal = t.raft_item_ordinal;
        res.terminal_digest = std::string(64, '0');
        res.terminal_state = "SAFE_PROTOCOL_FAILURE";
        res.payload = "[SAFE_PROTOCOL_FAILURE] parse_confirmed_result_strict returned false";
        res.format_version = 1;
        return res;
    }

    if (res.raft_log_index != t.raft_log_idx || res.raft_item_ordinal != t.raft_item_ordinal) {
        std::cerr << "SAFE_PROTOCOL_FAILURE"
                  << " req_id=" << t.req_id
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal
                  << " reason=index_ordinal_mismatch"
                  << " got_log=" << res.raft_log_index
                  << " got_ord=" << res.raft_item_ordinal
                  << " bytes=" << raw_backend_result.size()
                  << " raw_hex=" << hex_preview(raw_backend_result)
                  << std::endl;
        res.raft_log_index = static_cast<uint64_t>(-1);
        res.raft_item_ordinal = t.raft_item_ordinal;
        res.terminal_digest = std::string(64, '0');
        res.terminal_state = "SAFE_PROTOCOL_FAILURE";
        res.payload = "[SAFE_PROTOCOL_FAILURE] index/ordinal mismatch";
        res.format_version = 1;
        return res;
    }

    if (res.terminal_digest.size() != 64) {
        std::cerr << "SAFE_PROTOCOL_FAILURE"
                  << " req_id=" << t.req_id
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal
                  << " reason=bad_digest_size"
                  << " digest_len=" << res.terminal_digest.size()
                  << " bytes=" << raw_backend_result.size()
                  << " raw_hex=" << hex_preview(raw_backend_result)
                  << std::endl;
        res.raft_log_index = static_cast<uint64_t>(-1);
        res.raft_item_ordinal = t.raft_item_ordinal;
        res.terminal_digest = std::string(64, '0');
        res.terminal_state = "SAFE_PROTOCOL_FAILURE";
        res.payload = "[SAFE_PROTOCOL_FAILURE] invalid terminal digest size";
        res.format_version = 1;
        return res;
    }

    return res;
}

bool pg_executor::ensure_safe_ledger_terminal(PGconn* c,
                                              const pg_executor::task& t,
                                              const pg_executor::ConfirmedResult& confirmed) {
    if (db_opt_.raft_apply_ledger_mode != "safe" || t.raft_log_idx == 0) {
        return true;
    }
    if (!c || db_opt_.raft_epoch_hex.size() != 64 ||
        confirmed.terminal_digest.size() != 64 ||
        confirmed.raft_log_index != t.raft_log_idx ||
        confirmed.raft_item_ordinal != t.raft_item_ordinal ||
        (confirmed.terminal_state != "OK" && confirmed.terminal_state != "ERROR")) {
        std::cerr << "SAFE_LEDGER_DURABLE_VERIFY_FAILED"
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal
                  << " reason=invalid_confirmed_metadata"
                  << std::endl;
        return false;
    }

    std::string sqlstate = "XX000";
    if (confirmed.terminal_state == "ERROR" &&
        !extract_terminal_sqlstate(confirmed.payload, &sqlstate)) {
        std::cerr << "SAFE_LEDGER_DURABLE_VERIFY_FAILED"
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal
                  << " reason=invalid_error_sqlstate"
                  << std::endl;
        return false;
    }

    const std::string log_s = std::to_string(confirmed.raft_log_index);
    const std::string ord_s = std::to_string(confirmed.raft_item_ordinal);
    const bool is_error = confirmed.terminal_state == "ERROR";

    const char* select_params[3] = {
        db_opt_.raft_epoch_hex.c_str(),
        log_s.c_str(),
        ord_s.c_str()
    };
    PGresult* sel = PQexecParams(
        c,
        "SELECT state, encode(terminal_digest, 'hex'), committed_at IS NOT NULL"
        "  FROM ariabc_internal.raft_apply_item"
        " WHERE epoch_id = decode($1, 'hex')"
        "   AND raft_log_index = $2::bigint"
        "   AND item_ordinal = $3::integer",
        3,
        nullptr,
        select_params,
        nullptr,
        nullptr,
        0);
    if (!sel || PQresultStatus(sel) != PGRES_TUPLES_OK || PQntuples(sel) != 1) {
        std::cerr << "SAFE_LEDGER_DURABLE_VERIFY_FAILED"
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal
                  << " reason=select_failed"
                  << " pgerror=" << (sel ? PQresultErrorMessage(sel) : PQerrorMessage(c))
                  << std::endl;
        if (sel) PQclear(sel);
        return false;
    }
    const int state = std::atoi(PQgetvalue(sel, 0, 0));
    const std::string digest = PQgetisnull(sel, 0, 1) ? "" : PQgetvalue(sel, 0, 1);
    const bool committed = !PQgetisnull(sel, 0, 2) &&
                           strcmp(PQgetvalue(sel, 0, 2), "t") == 0;
    PQclear(sel);

    const int expected_state = is_error ? 3 : 2;
    if (state != expected_state || digest != confirmed.terminal_digest || !committed) {
        std::cerr << "SAFE_LEDGER_DURABLE_VERIFY_FAILED"
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal
                  << " reason=nonmatching_existing"
                  << " state=" << state
                  << " expected_state=" << expected_state
                  << " committed=" << (committed ? 1 : 0)
                  << std::endl;
        return false;
    }

    return true;
}

bool pg_executor::ensure_safe_nonterminal_failure(const pg_executor::task& t,
                                                  const pg_executor::ConfirmedResult& confirmed) {
    if (db_opt_.raft_apply_ledger_mode != "safe" || t.raft_log_idx == 0) {
        return true;
    }
    if (db_opt_.raft_epoch_hex.size() != 64 ||
        confirmed.terminal_digest.size() != 64 ||
        confirmed.terminal_state != "NONTERMINAL_FAILURE" ||
        confirmed.failure_sqlstate.size() != 5 ||
        confirmed.failure_class.empty() ||
        confirmed.format_version != 1 ||
        confirmed.raft_log_index != t.raft_log_idx ||
        confirmed.raft_item_ordinal != t.raft_item_ordinal) {
        std::cerr << "SAFE_LEDGER_NONTERMINAL_VERIFY_FAILED"
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal
                  << " reason=invalid_failure_metadata"
                  << std::endl;
        return false;
    }

    PGconn* fresh = PQconnectdb(conninfo_.c_str());
    if (!fresh || PQstatus(fresh) != CONNECTION_OK) {
        std::cerr << "SAFE_LEDGER_NONTERMINAL_VERIFY_FAILED"
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal
                  << " reason=fresh_connect_failed"
                  << " pgerror=" << (fresh ? PQerrorMessage(fresh) : "PQconnectdb returned null")
                  << std::endl;
        if (fresh) PQfinish(fresh);
        return false;
    }

    const std::string log_s = std::to_string(confirmed.raft_log_index);
    const std::string ord_s = std::to_string(confirmed.raft_item_ordinal);
    const char* params[3] = {
        db_opt_.raft_epoch_hex.c_str(),
        log_s.c_str(),
        ord_s.c_str()
    };
    PGresult* sel = PQexecParams(
        fresh,
        "SELECT state,"
        "       encode(failure_digest, 'hex'),"
        "       failure_sqlstate,"
        "       failure_class,"
        "       failure_retryable,"
        "       failure_format_version,"
        "       failure_recorded_at IS NOT NULL,"
        "       sqlstate_code IS NULL,"
        "       terminal_digest IS NULL,"
        "       result_payload IS NULL,"
        "       error_payload IS NULL,"
        "       committed_at IS NULL,"
        "       result_format_version IS NULL,"
        "       error_format_version IS NULL"
        "  FROM ariabc_internal.raft_apply_item"
        " WHERE epoch_id = decode($1, 'hex')"
        "   AND raft_log_index = $2::bigint"
        "   AND item_ordinal = $3::integer",
        3,
        nullptr,
        params,
        nullptr,
        nullptr,
        0);
    if (!sel || PQresultStatus(sel) != PGRES_TUPLES_OK || PQntuples(sel) != 1) {
        std::cerr << "SAFE_LEDGER_NONTERMINAL_VERIFY_FAILED"
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal
                  << " reason=select_failed"
                  << " pgerror=" << (sel ? PQresultErrorMessage(sel) : PQerrorMessage(fresh))
                  << std::endl;
        if (sel) PQclear(sel);
        PQfinish(fresh);
        return false;
    }

    const int state = std::atoi(PQgetvalue(sel, 0, 0));
    const std::string digest = PQgetisnull(sel, 0, 1) ? "" : PQgetvalue(sel, 0, 1);
    const std::string sqlstate = PQgetisnull(sel, 0, 2) ? "" : PQgetvalue(sel, 0, 2);
    const std::string failure_class = PQgetisnull(sel, 0, 3) ? "" : PQgetvalue(sel, 0, 3);
    const bool retryable = !PQgetisnull(sel, 0, 4) && PQgetvalue(sel, 0, 4)[0] == 't';
    const int fmtver = PQgetisnull(sel, 0, 5) ? 0 : std::atoi(PQgetvalue(sel, 0, 5));
    const bool recorded = !PQgetisnull(sel, 0, 6) && PQgetvalue(sel, 0, 6)[0] == 't';
    const bool sqlstate_code_null = !PQgetisnull(sel, 0, 7) && PQgetvalue(sel, 0, 7)[0] == 't';
    const bool terminal_null = !PQgetisnull(sel, 0, 8) && PQgetvalue(sel, 0, 8)[0] == 't';
    const bool result_null = !PQgetisnull(sel, 0, 9) && PQgetvalue(sel, 0, 9)[0] == 't';
    const bool error_null = !PQgetisnull(sel, 0, 10) && PQgetvalue(sel, 0, 10)[0] == 't';
    const bool committed_null = !PQgetisnull(sel, 0, 11) && PQgetvalue(sel, 0, 11)[0] == 't';
    const bool result_fmt_null = !PQgetisnull(sel, 0, 12) && PQgetvalue(sel, 0, 12)[0] == 't';
    const bool error_fmt_null = !PQgetisnull(sel, 0, 13) && PQgetvalue(sel, 0, 13)[0] == 't';
    PQclear(sel);
    PQfinish(fresh);

    if (state != 4 ||
        digest != confirmed.terminal_digest ||
        sqlstate != confirmed.failure_sqlstate ||
        failure_class != confirmed.failure_class ||
        retryable != confirmed.failure_retryable ||
        fmtver != 1 ||
        !recorded ||
        !sqlstate_code_null ||
        !terminal_null ||
        !result_null ||
        !error_null ||
        !committed_null ||
        !result_fmt_null ||
        !error_fmt_null) {
        std::cerr << "SAFE_LEDGER_NONTERMINAL_VERIFY_FAILED"
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal
                  << " reason=nonmatching_state4"
                  << " state=" << state
                  << " sqlstate=" << sqlstate
                  << " failure_class=" << failure_class
                  << std::endl;
        return false;
    }

    std::cerr << "SAFE_VERIFY_NONTERMINAL_FAILURE"
              << " epoch=" << db_opt_.raft_epoch_hex
              << " log=" << t.raft_log_idx
              << " ord=" << t.raft_item_ordinal
              << " sqlstate=" << sqlstate
              << " failure_digest=" << digest
              << std::endl;
    std::cerr << "SAFE_VERIFY_NONTERMINAL_FAILURE_FRESH_CONN"
              << " log=" << t.raft_log_idx
              << " ord=" << t.raft_item_ordinal
              << " sqlstate=" << sqlstate
              << " failure_class=" << failure_class
              << " retryable=" << (retryable ? 1 : 0)
              << " sqlstate_code_null=" << (sqlstate_code_null ? 1 : 0)
              << std::endl;
    return true;
}

bool pg_executor::ensure_safe_outcome(PGconn* c,
                                      const pg_executor::task& t,
                                      const pg_executor::ConfirmedResult& confirmed) {
    if (confirmed.terminal_state == "OK" || confirmed.terminal_state == "ERROR") {
        return ensure_safe_ledger_terminal(c, t, confirmed);
    }
    if (confirmed.terminal_state == "NONTERMINAL_FAILURE") {
        return ensure_safe_nonterminal_failure(t, confirmed);
    }
    std::cerr << "SAFE_LEDGER_DURABLE_VERIFY_FAILED"
              << " log=" << t.raft_log_idx
              << " ord=" << t.raft_item_ordinal
              << " reason=unknown_outcome_state"
              << std::endl;
    return false;
}

namespace {

bool safe_external_probe_enabled() {
    static const bool enabled = []() -> bool {
        const char* v = std::getenv("ARIABC_SAFE_EXTERNAL_PROBE");
        if (!v) return false;
        const std::string s = trim_copy(v);
        return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
    }();
    return enabled;
}

void log_safe_verify_submit_conn(PGconn* c, uint64_t log_idx, uint32_t ord) {
    const int pq_status = c ? static_cast<int>(PQstatus(c)) : -1;
    const int tx_status = c ? static_cast<int>(PQtransactionStatus(c)) : -1;
    const int backend_pid = c ? PQbackendPID(c) : -1;
    std::cerr << "SAFE_VERIFY_SUBMIT_CONN"
              << " log=" << log_idx
              << " ord=" << ord
              << " pq_status=" << pq_status
              << " tx_status=" << tx_status
              << " backend_pid=" << backend_pid
              << std::endl;
}

void probe_safe_ledger_terminal_visibility(PGconn* submit_conn,
                                           const std::string& conninfo,
                                           const std::string& epoch_hex,
                                           const pg_executor::task& t) {
    if (!safe_external_probe_enabled()) return;

    log_safe_verify_submit_conn(submit_conn, t.raft_log_idx, t.raft_item_ordinal);

    std::string probe_conninfo = conninfo;
    if (probe_conninfo.find("application_name=") == std::string::npos) {
        probe_conninfo += " application_name=ariabc_safe_external_probe";
    }

    PGconn* fresh = PQconnectdb(probe_conninfo.c_str());
    int pq_status = fresh ? static_cast<int>(PQstatus(fresh)) : -1;
    int tx_status = fresh ? static_cast<int>(PQtransactionStatus(fresh)) : -1;
    int backend_pid = fresh ? PQbackendPID(fresh) : -1;
    int observed_state = -1;
    int digest_present = 0;
    int committed = 0;
    bool query_ok = false;
    std::string error_detail;

    if (fresh && PQstatus(fresh) == CONNECTION_OK &&
        PQtransactionStatus(fresh) == PQTRANS_IDLE) {
        const std::string log_s = std::to_string(t.raft_log_idx);
        const std::string ord_s = std::to_string(t.raft_item_ordinal);
        const char* params[3] = {
            epoch_hex.c_str(),
            log_s.c_str(),
            ord_s.c_str()
        };
        PGresult* sel = PQexecParams(
            fresh,
            "SELECT state, encode(terminal_digest, 'hex'), committed_at IS NOT NULL"
            "  FROM ariabc_internal.raft_apply_item"
            " WHERE epoch_id = decode($1, 'hex')"
            "   AND raft_log_index = $2::bigint"
            "   AND item_ordinal = $3::integer",
            3,
            nullptr,
            params,
            nullptr,
            nullptr,
            0);

        if (sel && PQresultStatus(sel) == PGRES_TUPLES_OK && PQntuples(sel) == 1) {
            query_ok = true;
            if (!PQgetisnull(sel, 0, 0)) {
                observed_state = std::atoi(PQgetvalue(sel, 0, 0));
            }
            digest_present = !PQgetisnull(sel, 0, 1) && PQgetvalue(sel, 0, 1)[0] != '\0';
            committed = (!PQgetisnull(sel, 0, 2) && PQgetvalue(sel, 0, 2)[0] == 't') ? 1 : 0;
        } else {
            error_detail = sel ? PQresultErrorMessage(sel) : PQerrorMessage(fresh);
        }
        if (sel) PQclear(sel);
    } else {
        error_detail = fresh ? PQerrorMessage(fresh) : "PQconnectdb returned null";
    }

    if (!query_ok) {
        std::cerr << "SAFE_VERIFY_FRESH_CONN_ERROR"
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal
                  << " pq_status=" << pq_status
                  << " tx_status=" << tx_status
                  << " backend_pid=" << backend_pid
                  << " error=" << trim_copy(error_detail)
                  << std::endl;
    }

    std::cerr << "SAFE_VERIFY_FRESH_CONN"
              << " log=" << t.raft_log_idx
              << " ord=" << t.raft_item_ordinal
              << " pq_status=" << pq_status
              << " tx_status=" << tx_status
              << " backend_pid=" << backend_pid
              << " state=" << observed_state
              << " digest_present=" << digest_present
              << " committed=" << committed
              << std::endl;

    if (fresh) PQfinish(fresh);
}

void probe_safe_ledger_nonterminal_failure_visibility(PGconn* submit_conn,
                                                       const std::string& conninfo,
                                                       const std::string& epoch_hex,
                                                       const pg_executor::task& t,
                                                       const pg_executor::ConfirmedResult& confirmed) {
    if (!safe_external_probe_enabled()) return;

    log_safe_verify_submit_conn(submit_conn, t.raft_log_idx, t.raft_item_ordinal);

    std::string probe_conninfo = conninfo;
    if (probe_conninfo.find("application_name=") == std::string::npos) {
        probe_conninfo += " application_name=ariabc_safe_external_probe";
    }

    PGconn* fresh = PQconnectdb(probe_conninfo.c_str());
    int pq_status = fresh ? static_cast<int>(PQstatus(fresh)) : -1;
    int tx_status = fresh ? static_cast<int>(PQtransactionStatus(fresh)) : -1;
    int backend_pid = fresh ? PQbackendPID(fresh) : -1;
    int observed_state = -1;
    int digest_present = 0;
    int committed = 0;
    int sqlstate_null = 0;
    bool query_ok = false;
    std::string error_detail;

    if (fresh && PQstatus(fresh) == CONNECTION_OK &&
        PQtransactionStatus(fresh) == PQTRANS_IDLE) {
        const std::string log_s = std::to_string(t.raft_log_idx);
        const std::string ord_s = std::to_string(t.raft_item_ordinal);
        const char* params[3] = {
            epoch_hex.c_str(),
            log_s.c_str(),
            ord_s.c_str()
        };
        PGresult* sel = PQexecParams(
            fresh,
            "SELECT state, encode(failure_digest, 'hex'), committed_at IS NOT NULL, sqlstate_code IS NULL"
            "  FROM ariabc_internal.raft_apply_item"
            " WHERE epoch_id = decode($1, 'hex')"
            "   AND raft_log_index = $2::bigint"
            "   AND item_ordinal = $3::integer",
            3,
            nullptr,
            params,
            nullptr,
            nullptr,
            0);

        if (sel && PQresultStatus(sel) == PGRES_TUPLES_OK && PQntuples(sel) == 1) {
            query_ok = true;
            if (!PQgetisnull(sel, 0, 0)) {
                observed_state = std::atoi(PQgetvalue(sel, 0, 0));
            }
            digest_present = !PQgetisnull(sel, 0, 1) && PQgetvalue(sel, 0, 1)[0] != '\0';
            committed = (!PQgetisnull(sel, 0, 2) && PQgetvalue(sel, 0, 2)[0] == 't') ? 1 : 0;
            sqlstate_null = (!PQgetisnull(sel, 0, 3) && PQgetvalue(sel, 0, 3)[0] == 't') ? 1 : 0;
        } else {
            error_detail = sel ? PQresultErrorMessage(sel) : PQerrorMessage(fresh);
        }
        if (sel) PQclear(sel);
    } else {
        error_detail = fresh ? PQerrorMessage(fresh) : "PQconnectdb returned null";
    }

    if (!query_ok) {
        std::cerr << "SAFE_VERIFY_NONTERMINAL_FAILURE_FRESH_CONN_ERROR"
                  << " log=" << t.raft_log_idx
                  << " ord=" << t.raft_item_ordinal
                  << " pq_status=" << pq_status
                  << " tx_status=" << tx_status
                  << " backend_pid=" << backend_pid
                  << " error=" << trim_copy(error_detail)
                  << std::endl;
    }

    std::cerr << "SAFE_VERIFY_NONTERMINAL_FAILURE_FRESH_CONN"
              << " log=" << t.raft_log_idx
              << " ord=" << t.raft_item_ordinal
              << " pq_status=" << pq_status
              << " tx_status=" << tx_status
              << " backend_pid=" << backend_pid
              << " state=" << observed_state
              << " digest_present=" << digest_present
              << " committed=" << committed
              << " sqlstate_null=" << sqlstate_null
              << " failure_class=" << confirmed.failure_class
              << " sqlstate=" << confirmed.failure_sqlstate
              << std::endl;

    if (fresh) PQfinish(fresh);
}

void probe_safe_ledger_outcome_visibility(PGconn* submit_conn,
                                          const std::string& conninfo,
                                          const std::string& epoch_hex,
                                          const pg_executor::task& t,
                                          const pg_executor::ConfirmedResult& confirmed) {
    if (confirmed.terminal_state == "NONTERMINAL_FAILURE") {
        probe_safe_ledger_nonterminal_failure_visibility(submit_conn, conninfo, epoch_hex, t, confirmed);
        return;
    }
    probe_safe_ledger_terminal_visibility(submit_conn, conninfo, epoch_hex, t);
}

void update_atomic_max_u64(std::atomic<uint64_t>& target, uint64_t value) {
    uint64_t cur = target.load(std::memory_order_relaxed);
    while (value > cur &&
           !target.compare_exchange_weak(cur,
                                         value,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
    }
}

} // namespace

int pg_executor::configured_batch_delay_us() {
    return kConfiguredDelayUs;
}

int pg_executor::override_batch_delay_us() {
    return kConfiguredDelayUs > 0 ? kConfiguredDelayUs : -1;
}

struct pg_executor::notice_state {
    std::string last_merkle_roots;
};

void pg_executor::notice_processor(void* arg, const char* message) {
    if (!arg || !message) return;
    notice_state* st = static_cast<notice_state*>(arg);
    const char* p = ::strstr(message, kBcdbMerkleTag);
    if (!p) return;
    std::string payload = trim_copy(p + ::strlen(kBcdbMerkleTag));
    if (!payload.empty()) st->last_merkle_roots = payload;
}

bool pg_executor::is_event_mode(const std::string& mode) {
    const std::string m = trim_copy(mode);
    return (m == "event" || m == "reactor" || m == "async");
}

bool pg_executor::is_det_prefixed_sql(const std::string& sql) const {
    const std::string t = trim_copy(sql);
    if (t.size() < 2) return false;
    // Deterministic/safedb parser-path commands are shaped as:
    //   "s <SQL>" or "s <8digit-seq> <SQL>"
    // If this prefix is absent in safedb deterministic mode, executing many
    // queued requests concurrently can reorder conflicting writes and break
    // Merkle consistency.
    return (t[0] == 's' || t[0] == 'S') && std::isspace(static_cast<unsigned char>(t[1]));
}

void pg_executor::note_det_raw_compat_activation(const task& t) {
    const uint64_t prev =
        st_det_raw_compat_activations_.fetch_add(1, std::memory_order_relaxed);
    det_raw_compat_mode_ = true;
    if (prev == 0) {
        det_raw_compat_first_req_id_ = t.req_id;
        det_raw_compat_first_sql_prefix_ = det_sql_prefix_for_profile(t.sql);
    }
}

std::string pg_executor::det_unprefixed_sql_error(const task& t) const {
    std::ostringstream oss;
    oss << "ERROR det_prefixed_sql_required req_id=" << t.req_id;
    const std::string prefix = det_sql_prefix_for_profile(t.sql);
    if (!prefix.empty()) {
        oss << " sql=\"" << prefix << "\"";
    }
    return oss.str();
}

bool pg_executor::initialize_bcdb() {
    if (db_opt_.db_type != 1) return true;
    if (bcdb_init_done_) return true;
    if (bcdb_init_failed_) return false;

    std::lock_guard<std::mutex> lk(bcdb_init_mu_);
    if (bcdb_init_done_) return true;
    if (bcdb_init_failed_) return false;

    PGconn* c = PQconnectdb(conninfo_.c_str());
    if (!c || PQstatus(c) != CONNECTION_OK) {
        std::cerr << "bcdb_init skipped on node " << node_id_
                  << ": connect failed: "
                  << trim_copy(c ? PQerrorMessage(c) : std::string("null conn"))
                  << std::endl;
        if (c) PQfinish(c);
        bcdb_init_failed_ = true;
        return false;
    }

    if (db_opt_.raft_apply_ledger_mode == "safe") {
        // 1. Check SHA-256 provider in PostgreSQL
        {
            PGresult* res = PQexec(c, "SELECT sha256('test'::bytea);");
            if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
                std::cerr << "Safe mode validation failed: missing or non-functional SHA-256 provider in database" << std::endl;
                if (res) PQclear(res);
                PQfinish(c);
                bcdb_init_failed_ = true;
                return false;
            }
            PQclear(res);
        }

        // 2. Check schema version
        int schema_version = -1;
        {
            PGresult* res = PQexec(c, "SELECT schema_version FROM ariabc_internal.raft_apply_schema_meta;");
            if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
                std::cerr << "Safe mode validation failed: missing schema version (metadata table not found or empty)" << std::endl;
                if (res) PQclear(res);
                PQfinish(c);
                bcdb_init_failed_ = true;
                return false;
            }
            if (PQntuples(res) != 1) {
                std::cerr << "Safe mode validation failed: schema version count is not exactly 1" << std::endl;
                PQclear(res);
                PQfinish(c);
                bcdb_init_failed_ = true;
                return false;
            }
            schema_version = std::stoi(PQgetvalue(res, 0, 0));
            PQclear(res);
        }
        if (schema_version != 4) {
            std::cerr << "Safe mode validation failed: unsupported schema version " << schema_version << " (expected 4)" << std::endl;
            PQfinish(c);
            bcdb_init_failed_ = true;
            return false;
        }

        // 3. Check epoch anchor
        {
            const char* epoch_params[1] = { db_opt_.raft_epoch_hex.c_str() };
            PGresult* res = PQexecParams(c,
                                         "SELECT 1 FROM ariabc_internal.raft_apply_epoch WHERE epoch_id = decode($1, 'hex') FOR UPDATE;",
                                         1,
                                         nullptr,
                                         epoch_params,
                                         nullptr,
                                         nullptr,
                                         0);
            if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
                std::cerr << "Safe mode validation failed: missing epoch anchor table or query failed: "
                          << (res ? PQerrorMessage(c) : "null result") << std::endl;
                if (res) PQclear(res);
                PQfinish(c);
                bcdb_init_failed_ = true;
                return false;
            }
            if (PQntuples(res) != 1) {
                std::cerr << "Safe mode validation failed: epoch mismatch or epoch count is not exactly 1 for epoch "
                          << db_opt_.raft_epoch_hex << std::endl;
                PQclear(res);
                PQfinish(c);
                bcdb_init_failed_ = true;
                return false;
            }
            PQclear(res);
        }

        // Enforce the Merkle recovery gate before workers are exposed.  The
        // The synchronous DML and safe-ledger paths own Merkle page apply;
        // startup only rebuilds old-format indexes and validates READY state.
        {
            PGresult* res = PQexec(c, "SELECT pg_catalog.merkle_rebuild_legacy_indexes();");
            if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
                std::cerr << "Safe mode validation failed: Merkle legacy-index rebuild failed: "
                          << (res ? PQerrorMessage(c) : "null result") << std::endl;
                if (res) PQclear(res);
                PQfinish(c);
                bcdb_init_failed_ = true;
                return false;
            }
            PQclear(res);

            res = PQexec(c, "SELECT pg_catalog.merkle_recovery_status();");
            if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1) {
                std::cerr << "Safe mode validation failed: cannot read Merkle recovery status"
                          << std::endl;
                if (res) PQclear(res);
                PQfinish(c);
                bcdb_init_failed_ = true;
                return false;
            }
            const std::string status_json = PQgetvalue(res, 0, 0);
            PQclear(res);
            if (status_json.find("\"state\":\"READY\"") == std::string::npos) {
                std::cerr << "Safe mode validation failed: Merkle recovery is not READY: "
                          << status_json << std::endl;
                PQfinish(c);
                bcdb_init_failed_ = true;
                return false;
            }

        }
    }

    const int configured_block_size =
        (db_opt_.bcdb_init_block_size > 0)
            ? db_opt_.bcdb_init_block_size
            : db_opt_.conn_pool_size;
    const int worker_queue_count = std::max(1, configured_block_size);
    std::cerr << "BCDB_INIT_CONFIG"
              << " pool_size=" << db_opt_.conn_pool_size
              << " bcdb_init_arg_size=" << worker_queue_count
              << " requested_worker_count=" << worker_queue_count
              << " effective_worker_queues=" << worker_queue_count
              << std::endl;
    std::ostringstream q;
    q << "SELECT bcdb_init(True, " << worker_queue_count << ");";
    PGresult* res = PQexec(c, q.str().c_str());
    const ExecStatusType st = res ? PQresultStatus(res) : PGRES_FATAL_ERROR;
    if (!(st == PGRES_TUPLES_OK || st == PGRES_COMMAND_OK)) {
        std::cerr << "bcdb_init skipped on node " << node_id_
                  << ": " << trim_copy(res ? PQresultErrorMessage(res) : "null result")
                  << std::endl;
        if (res) PQclear(res);
        PQfinish(c);
        bcdb_init_failed_ = true;
        return false;
    }

    if (res) PQclear(res);
    bcdb_ctrl_conn_ = c;
    bcdb_init_done_ = true;
    bcdb_block_size_ = worker_queue_count;
    std::cerr << "bcdb_init enabled on node " << node_id_
              << " with worker_queue_count=" << worker_queue_count
              << std::endl;
    return true;
}

bool pg_executor::ensure_bcdb_initialized() {
    return initialize_bcdb();
}

void pg_executor::ensure_bcdb_initialized_for_sql(const std::string& sql) {
    if (db_opt_.db_type != 1) return;
    if (!is_det_prefixed_sql(sql)) return;
    (void)initialize_bcdb();
}

pg_executor::pg_executor(int node_id,
                         const db_options& db_opt,
                         const kafka_options& k_opt,
                         completion_callback on_task_applied,
                         failure_callback on_task_failed)
    : node_id_(node_id)
    , db_opt_(db_opt)
    , kafka_opt_(k_opt)
    , result_sig_key_(k_opt.result_sig_key)
    , kafka_enabled_(false)
    , on_task_applied_(std::move(on_task_applied))
    , on_task_failed_(std::move(on_task_failed))
{
    event_mode_ = is_event_mode(db_opt_.exec_mode);
    if (db_opt_.db_type == 1) {
        // Parallel deterministic workers are on by default in threaded mode:
        // ordering is enforced by the PG serial gate (worker.c:bcdb_wait_for_serial_slot),
        // so multiple client-side worker threads dispatching in parallel stay
        // deterministic while fully utilising the connection pool. Users can
        // force the legacy single-worker block-batch fast path with
        // ARIABC_DET_PARALLEL_WORKERS=0.
        det_parallel_workers_ = !event_mode_;
        const char* v = ::getenv("ARIABC_DET_PARALLEL_WORKERS");
        if (v && *v) {
            const std::string s = trim_copy(v);
            det_parallel_workers_ =
                !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
        }
    }
    det_allow_raw_compat_ = det_allow_raw_compat_mode();
    det_prefixed_direct_parallel_ =
        (db_opt_.db_type == 1) && det_prefixed_direct_parallel_enabled();
    det_completion_only_success_ =
        (db_opt_.db_type == 1) && det_completion_only_success_enabled();
    det_threaded_direct_no_preapply_wait_ =
        (db_opt_.db_type == 1) &&
        !event_mode_ &&
        db_opt_.raft_apply_ledger_mode != "safe" &&
        det_threaded_direct_no_preapply_wait_enabled();
    if (det_prefixed_direct_parallel_ && det_event_block_fastpath_enabled()) {
        std::cerr << "ARIABC_DET_PREFIXED_DIRECT_PARALLEL requested on node "
                  << node_id_
                  << " but ARIABC_DET_EVENT_BLOCK_FASTPATH is enabled; "
                     "block-submit fast path remains active"
                  << std::endl;
    }
    if (det_threaded_direct_no_preapply_wait_) {
        std::cerr << "ARIABC_DET_THREADED_DIRECT_NO_PREAPPLY_WAIT enabled on node "
                  << node_id_
                  << ": worker threads enter PQexec concurrently and publish results in dispatch order"
                  << std::endl;
    }
    if (event_mode_ && det_parallel_workers_) {
        std::cerr << "event-mode deterministic server execution does not support parallel worker coordination; "
                     "forcing ARIABC_DET_PARALLEL_WORKERS=0 on node "
                  << node_id_ << std::endl;
        det_parallel_workers_ = false;
    }
    if (db_opt_.db_type == 1) {
        const char* vp = ::getenv("ARIABC_DET_BLOCK_PARALLEL");
        if (vp && *vp) {
            char* end = nullptr;
            const long parsed = std::strtol(vp, &end, 10);
            if (end != vp && parsed > 0) {
                /*
                 * Event-mode deterministic blocks can be submitted on several
                 * PG connections at once.  The backend serial gate preserves
                 * tx order; the event loop emits completed block results in
                 * submission order.
                 */
                det_block_parallel_ = static_cast<int>(std::min<long>(parsed, 256));
            }
        }
        const char* v = ::getenv("ARIABC_DET_BLOCK_PIPELINE");
        if (v && *v) {
            char* end = nullptr;
            const long parsed = std::strtol(v, &end, 10);
            if (end != v && parsed > 0) {
                /*
                 * Allow one backend submit to cover multiple ordered BCDB
                 * worker waves. The runtime cap is still bounded below by the
                 * result ring size used by the trusted 4-node runner.
                 */
                det_block_pipeline_ = static_cast<int>(std::min<long>(parsed, 64));
            }
        }
        v = ::getenv("ARIABC_DET_BLOCK_MAX");
        if (v && *v) {
            char* end = nullptr;
            const long parsed = std::strtol(v, &end, 10);
            if (end != v && parsed > 0) {
                det_block_max_ = static_cast<size_t>(std::min<long>(parsed, 8192));
            }
        }
    }

    // Assign a per-process-instance base so BCDB tx_pool keys are unique
    // across ariabc_pg_server restarts sharing the same PostgreSQL instance.
    {
        static std::atomic<uint64_t> s_block_tx_key_session{0};
        det_block_tx_key_base_ =
            s_block_tx_key_session.fetch_add(100000000ULL, std::memory_order_relaxed);
    }

    if (result_sig_key_.empty()) {
        const char* env_key = ::getenv("ARIABC_RESULT_SIG_KEY");
        if (env_key && *env_key) {
            result_sig_key_ = env_key;
        } else {
            result_sig_key_ = kDefaultResultSigKey;
        }
    }

    // Kafka producer (optional).
    if (!kafka_opt_.bootstrap.empty()) {
        std::string err;
        if (!kafka_prod_.start(kafka_opt_.bootstrap,
                               kafka_opt_.result_topic,
                               kafka_producer_profile::result_fast,
                               err)) {
            std::cerr << "Kafka disabled: " << err << std::endl;
            kafka_enabled_ = false;
        } else {
            kafka_enabled_ = true;
        }
    }
    kafka_async_publisher_enabled_ =
        kafka_enabled_ &&
        should_publish_kafka_result(node_id_) &&
        db_opt_.raft_apply_ledger_mode != "safe" &&
        kafka_async_result_publisher_env_enabled();
    if (kafka_async_publisher_enabled_) {
        kafka_pub_thread_ = std::thread([this] { kafka_publisher_loop(); });
        std::cerr << "Kafka async result publisher enabled on node "
                  << node_id_
                  << " records="
                  << kafka_async_result_publisher_max_records()
                  << " target_records="
                  << kafka_async_result_publisher_target_records()
                  << " delay_us="
                  << kafka_async_result_publisher_delay_us()
                  << std::endl;
    }

    // DB connections.
    if (db_opt_.dbname.empty() || db_opt_.port.empty()) {
        throw std::runtime_error("missing --dbName/--dbPort");
    }
    if (db_opt_.conn_pool_size <= 0) {
        throw std::runtime_error("invalid --dbConnPoolSize");
    }

    // Hysteresis watermarks for admission control. Deterministic mode uses
    // lower queue targets to reduce overload latch oscillation and long-tail
    // append stalls under serialized execution.
    //
    // Env overrides:
    //   BCDB_DET_QUEUE_HIGH_WM, BCDB_DET_QUEUE_LOW_WM: hard override the
    //   computed values when set to a positive integer. Used by the
    //   single-node gateway-direct profile so the gateway can pipeline more
    //   in-flight work into a single backend without tripping admission
    //   control stalls (which cap throughput at the formula's 2*pool=512).
    const bool det_mode = (db_opt_.db_type == 1);
    const size_t high_min = det_mode ? kDetQueueHighWatermarkMin : kQueueHighWatermarkMin;
    const size_t low_min = det_mode ? kDetQueueLowWatermarkMin : kQueueLowWatermarkMin;
    const size_t high_factor = det_mode ? kDetQueueHighWatermarkFactor : kQueueHighWatermarkFactor;
    const size_t low_factor = det_mode ? kDetQueueLowWatermarkFactor : kQueueLowWatermarkFactor;
    queue_high_wm_ = std::max<size_t>(
        high_min,
        static_cast<size_t>(db_opt_.conn_pool_size) * high_factor);
    queue_low_wm_ = std::max<size_t>(
        low_min,
        static_cast<size_t>(db_opt_.conn_pool_size) * low_factor);
    if (det_mode) {
        if (const char* v = std::getenv("BCDB_DET_QUEUE_HIGH_WM")) {
            errno = 0;
            char* end = nullptr;
            const unsigned long long override_high = std::strtoull(v, &end, 10);
            if (errno == 0 && end && *end == '\0' && override_high > 0) {
                queue_high_wm_ = static_cast<size_t>(override_high);
            }
        }
        if (const char* v = std::getenv("BCDB_DET_QUEUE_LOW_WM")) {
            errno = 0;
            char* end = nullptr;
            const unsigned long long override_low = std::strtoull(v, &end, 10);
            if (errno == 0 && end && *end == '\0' && override_low > 0) {
                queue_low_wm_ = static_cast<size_t>(override_low);
            }
        }
    }
    if (queue_low_wm_ > queue_high_wm_) {
        queue_low_wm_ = queue_high_wm_;
    }

    if (event_mode_) {
        int pfd[2];
        if (::pipe(pfd) != 0) {
            throw std::runtime_error(std::string("pipe failed: ") + ::strerror(errno));
        }
        wakeup_rfd_ = pfd[0];
        wakeup_wfd_ = pfd[1];
        // Nonblocking wakeups: best-effort; ignore errors.
        (void)::fcntl(wakeup_rfd_, F_SETFL, ::fcntl(wakeup_rfd_, F_GETFL, 0) | O_NONBLOCK);
        (void)::fcntl(wakeup_wfd_, F_SETFL, ::fcntl(wakeup_wfd_, F_GETFL, 0) | O_NONBLOCK);
        (void)::fcntl(wakeup_rfd_, F_SETFD, ::fcntl(wakeup_rfd_, F_GETFD, 0) | FD_CLOEXEC);
        (void)::fcntl(wakeup_wfd_, F_SETFD, ::fcntl(wakeup_wfd_, F_GETFD, 0) | FD_CLOEXEC);
    }

    conninfo_ = "host=" + db_opt_.host +
                " port=" + db_opt_.port +
                " dbname=" + db_opt_.dbname +
                " user=" + db_opt_.user;
    if (!db_opt_.password.empty()) {
        conninfo_ += " password=" + db_opt_.password;
    }

    for (int i = 0; i < db_opt_.conn_pool_size; ++i) {
        PGconn* c = nullptr;
#ifdef BUILDING_UNIT_TESTS
        if (db_opt_.dbname == "dummy_test") {
            // Bypass actual connection for unit tests
        } else {
#endif
        c = PQconnectdb(conninfo_.c_str());
        if (!c || PQstatus(c) != CONNECTION_OK) {
            std::string msg = c ? PQerrorMessage(c) : "null conn";
            if (c) PQfinish(c);
            throw std::runtime_error("PQconnectdb failed: " + trim_copy(msg));
        }

        /* These pooled sessions execute deterministic BCDB work.  Preserve
         * the server's SERIALIZABLE default: BCDB's worker snapshot path
         * requires it.  Only make the durability contract explicit here. */
        {
            PGresult* session_res = PQexec(
                c,
                "SET synchronous_commit = on;");
            if (!session_res || PQresultStatus(session_res) != PGRES_COMMAND_OK) {
                std::string msg = PQerrorMessage(c);
                if (session_res) PQclear(session_res);
                PQfinish(c);
                throw std::runtime_error("failed to configure control-plane session: " +
                                         trim_copy(msg));
            }
            PQclear(session_res);
        }
#ifdef BUILDING_UNIT_TESTS
        }
#endif

        if (event_mode_) {
            if (PQsetnonblocking(c, 1) != 0) {
                std::string msg = PQerrorMessage(c);
                PQfinish(c);
                throw std::runtime_error("PQsetnonblocking failed: " + trim_copy(msg));
            }
        }

        // Capture BCDB merkle roots emitted via NOTICE (for deterministic result strings).
        std::unique_ptr<notice_state> ns(new notice_state());
        notice_state* ns_ptr = ns.get();
        if (c) {
            PQsetNoticeProcessor(c, &pg_executor::notice_processor, ns_ptr);
        }
        notice_state_by_conn_.emplace(c, ns_ptr);
        notice_states_.push_back(std::move(ns));

        // NOTE:
        // We intentionally do NOT run bcdb_init() here.
        //
        // In our deterministic pipeline, the gateway shapes requests as
        // "s <8digit(seq)> <SQL>" (see Postgres tcop/postgres.c), which routes
        // execution through safedb_txdt() on the server side.
        //
        // Calling bcdb_init(True, N) is extremely heavy: it spawns N additional
        // libpq "worker" backends per calling session. If we do that for each
        // pooled connection (N sessions), the total number of connections grows
        // ~O(N^2) and quickly exhausts Postgres max_connections at N>=12.
        //
        // If a future setup requires bcdb_init, it should be performed once,
        // outside the per-connection pool, with an explicitly chosen block size.

        all_conns_.push_back(c);
        st_owned_pg_connections_.store(static_cast<uint64_t>(all_conns_.size()),
                                       std::memory_order_relaxed);
        if (!event_mode_) {
            pool_.push(c);
        }
    }

    if (event_mode_) {
        conns_.reserve(all_conns_.size());
        conn_qs_.resize(all_conns_.size());
        for (PGconn* c : all_conns_) {
            conn_state st;
            st.c = c;
            st.fd = PQsocket(c);
            st.st = conn_state::state::IDLE;
            conns_.push_back(std::move(st));
        }
        st_inflight_cur_.store(0, std::memory_order_relaxed);
        st_delayed_cur_.store(0, std::memory_order_relaxed);
        event_thread_ = std::thread([this] { event_loop(); });
    } else {
        // Worker threads. Deterministic parallel workers are opt-in.
        const int n_threads =
            (db_opt_.db_type == 1 && !det_parallel_workers_)
                ? 1
                : db_opt_.conn_pool_size;
        st_threaded_workers_configured_.store(static_cast<uint64_t>(n_threads),
                                              std::memory_order_relaxed);
        threads_.reserve(n_threads);
        for (int i = 0; i < n_threads; ++i) {
            threads_.emplace_back([this] { worker_loop(); });
            st_threaded_workers_created_.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

pg_executor::~pg_executor() {
    stop();
}

void pg_executor::push_task_ordered(task&& t) {
    if (!conn_qs_.empty()) {
        uint64_t task_seq = 0;
        size_t target_conn = 0;
        if (get_det_tx_seq_valid(t, task_seq)) {
            target_conn = task_seq % conn_qs_.size();
        } else {
            target_conn = std::hash<std::string>{}(t.req_id) % conn_qs_.size();
        }
        conn_qs_[target_conn].push(std::move(t));
    } else {
        q_.push(std::move(t));
    }
}

void pg_executor::enqueue(const std::string& req_id,
                          const std::string& sql,
                          int leader_node_hint,
                          uint64_t raft_log_idx) {
    std::vector<std::string> req_ids;
    std::vector<std::string> sqls;
    req_ids.push_back(req_id);
    sqls.push_back(sql);
    enqueue_batch(req_ids, sqls, leader_node_hint, raft_log_idx);
}

void pg_executor::enqueue_batch(const std::vector<std::string>& req_ids,
                                const std::vector<std::string>& sqls,
                                int leader_node_hint,
                                uint64_t raft_log_idx,
                                const std::string& entry_digest_hex,
                                const std::vector<std::string>& item_digests_hex,
                                const std::vector<uint64_t>& assigned_det_seqs,
                                const std::vector<uint8_t>& assigned_det_seq_valid) {
    if (stop_.load()) return;
    if (req_ids.empty() || req_ids.size() != sqls.size()) return;

    for (const std::string& sql : sqls) {
        ensure_bcdb_initialized_for_sql(sql);
    }

    st_enqueued_.fetch_add(static_cast<uint64_t>(req_ids.size()), std::memory_order_relaxed);
    {
        const uint64_t now_ns = now_steady_ns();
        std::lock_guard<std::mutex> lk(q_mu_);
        const bool preassigned_mode = state_machine_preassigned_ordering_enabled();
        for (size_t i = 0; i < req_ids.size(); ++i) {
            task t;
            t.req_id = req_ids[i];
            t.sql = sqls[i];
            t.leader_node_hint = leader_node_hint;
            t.raft_log_idx = raft_log_idx;
            if (i < assigned_det_seqs.size() &&
                i < assigned_det_seq_valid.size() &&
                assigned_det_seq_valid[i] != 0) {
                t.has_assigned_det_seq = true;
                t.assigned_det_seq = assigned_det_seqs[i];
            }
            t.enqueue_ns = now_ns;
            t.exec_begin_ns = 0;
            t.attempt = 0;
            t.raft_item_ordinal = static_cast<uint32_t>(i);
            t.raft_item_count = static_cast<uint32_t>(req_ids.size());
            t.entry_digest = entry_digest_hex;
            if (i < item_digests_hex.size()) {
                t.item_digest = item_digests_hex[i];
            }

            uint64_t seq = 0;
            const bool has_seq = get_det_tx_seq_valid(t, seq);
            if (has_seq && is_det_prefixed_sql(t.sql) && !is_reset_barrier_sql(t.sql)) {
                if (!det_preassigned_seq_initialized_ || seq < next_det_preassigned_seq_) {
                    next_det_preassigned_seq_ = seq;
                    det_preassigned_seq_initialized_ = true;
                    det_preassigned_reorder_buf_.clear();
                }
                det_preassigned_reorder_buf_[seq] = std::move(t);
                while (!det_preassigned_reorder_buf_.empty() &&
                       det_preassigned_reorder_buf_.begin()->first == next_det_preassigned_seq_) {
                    push_task_ordered(std::move(det_preassigned_reorder_buf_.begin()->second));
                    det_preassigned_reorder_buf_.erase(det_preassigned_reorder_buf_.begin());
                    ++next_det_preassigned_seq_;
                }
            } else {
                if (!det_preassigned_reorder_buf_.empty()) {
                    while (!det_preassigned_reorder_buf_.empty()) {
                        push_task_ordered(std::move(det_preassigned_reorder_buf_.begin()->second));
                        det_preassigned_reorder_buf_.erase(det_preassigned_reorder_buf_.begin());
                    }
                    det_preassigned_seq_initialized_ = false;
                }
                push_task_ordered(std::move(t));
            }

            size_t total_conn_qs = 0;
            for (const auto& cq : conn_qs_) {
                total_conn_qs += cq.size();
            }
            const size_t depth = q_.size() + total_conn_qs;
            st_queue_depth_cur_.store(static_cast<uint64_t>(depth), std::memory_order_relaxed);
            st_queue_depth_samples_.fetch_add(1, std::memory_order_relaxed);
            st_queue_depth_sum_.fetch_add(static_cast<uint64_t>(depth), std::memory_order_relaxed);
            if (depth <= 16) {
                st_queue_depth_bin_le_16_.fetch_add(1, std::memory_order_relaxed);
            } else if (depth <= 64) {
                st_queue_depth_bin_17_64_.fetch_add(1, std::memory_order_relaxed);
            } else if (depth <= 256) {
                st_queue_depth_bin_65_256_.fetch_add(1, std::memory_order_relaxed);
            } else {
                st_queue_depth_bin_gt_256_.fetch_add(1, std::memory_order_relaxed);
            }
            uint64_t cur_max = st_queue_depth_max_.load(std::memory_order_relaxed);
            while (depth > cur_max &&
                   !st_queue_depth_max_.compare_exchange_weak(
                       cur_max,
                       static_cast<uint64_t>(depth),
                       std::memory_order_relaxed,
                       std::memory_order_relaxed)) {
            }
        }
        const size_t delayed = delayed_.size();
        const size_t inflight = static_cast<size_t>(st_inflight_cur_.load(std::memory_order_relaxed));
        size_t total_conn_qs = 0;
        for (const auto& cq : conn_qs_) {
            total_conn_qs += cq.size();
        }
        const size_t queued = q_.size() + total_conn_qs + delayed;
        const size_t backlog = queued + inflight;
        st_backlog_cur_.store(static_cast<uint64_t>(backlog), std::memory_order_relaxed);
        st_delayed_cur_.store(static_cast<uint64_t>(delayed), std::memory_order_relaxed);
        // NOTE:
        // Do not toggle `queue_overloaded_` here.
        //
        // `enqueue()` runs on the Raft state-machine thread and can observe
        // short bursts where `q_` spikes before the executor thread dispatches
        // work to idle connections. If we flip the overload latch on such a
        // transient spike, the server may reject client submissions with
        // NOT_ACCEPTED_BUSY until the queue drains all the way below the low
        // watermark, causing large throughput variance.
        //
        // Instead, overload state is updated by the consumer loops (worker or
        // event reactor) based on their post-dispatch view.
    }
    q_cv_.notify_one();
    if (event_mode_ && wakeup_wfd_ >= 0) {
        const uint8_t b = 1;
        (void)::write(wakeup_wfd_, &b, 1);
    }
}

bool pg_executor::verify_and_register_entry_manifest(uint64_t log_idx, const std::string& entry_digest_hex, const std::vector<std::string>& item_digests_hex) {
    if (db_opt_.raft_apply_ledger_mode != "safe") return true;

    if (log_idx == 0) {
        std::cerr << "verify_and_register_entry_manifest: FATAL: log_idx is 0" << std::endl;
        std::abort();
    }
    if (item_digests_hex.empty()) {
        std::cerr << "verify_and_register_entry_manifest: FATAL: empty item list" << std::endl;
        std::abort();
    }
    if (entry_digest_hex.size() != 64) {
        std::cerr << "verify_and_register_entry_manifest: FATAL: invalid entry_digest_hex length " << entry_digest_hex.size() << std::endl;
        std::abort();
    }
    std::lock_guard<std::mutex> lk(manifest_mu_);
    for (const auto& item_digest : item_digests_hex) {
        if (item_digest.size() != 64) {
            std::cerr << "verify_and_register_entry_manifest: FATAL: invalid item_digest_hex length " << item_digest.size() << std::endl;
            std::abort();
        }
    }

    if (!manifest_conn_) {
        manifest_conn_ = PQconnectdb(conninfo_.c_str());
        if (PQstatus(manifest_conn_) != CONNECTION_OK) {
            std::cerr << "verify_and_register_entry_manifest: FATAL: failed to connect: "
                      << PQerrorMessage(manifest_conn_) << std::endl;
            PQfinish(manifest_conn_);
            manifest_conn_ = nullptr;
            std::abort();
        }

        PGresult* res = PQexec(manifest_conn_, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED; SET synchronous_commit = on;");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::cerr << "verify_and_register_entry_manifest: FATAL: failed to set connection characteristics: "
                      << PQerrorMessage(manifest_conn_) << std::endl;
            std::abort();
        }
        PQclear(res);
    }
    PGconn* c = manifest_conn_;

    const std::string epoch_hex = db_opt_.raft_epoch_hex;
    const std::string log_idx_s = std::to_string(log_idx);
    const std::string exp_items_s = std::to_string(item_digests_hex.size());
    const size_t expected_items = item_digests_hex.size();

    PGresult* res = PQexec(c, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "verify_and_register_entry_manifest: FATAL BEGIN failed: " << PQerrorMessage(c) << std::endl;
        std::abort();
    }
    PQclear(res);

    const char* ins_params[4] = {
        epoch_hex.c_str(),
        log_idx_s.c_str(),
        entry_digest_hex.c_str(),
        exp_items_s.c_str()
    };
    const char* ins_sql =
        "WITH locked AS ("
        "  SELECT next_seq"
        "    FROM ariabc_internal.merkle_apply_counter"
        "   WHERE singleton"
        "   FOR UPDATE"
        "), inserted AS ("
        "  INSERT INTO ariabc_internal.raft_apply_entry"
        "    (epoch_id, raft_log_index, entry_digest, expected_items, merkle_apply_seq_base)"
        "  SELECT decode($1,'hex'), $2::bigint, decode($3,'hex'), $4::integer, next_seq + 1"
        "    FROM locked"
        "  ON CONFLICT (epoch_id, raft_log_index) DO NOTHING"
        "  RETURNING 1"
        ")"
        " UPDATE ariabc_internal.merkle_apply_counter"
        "    SET next_seq = next_seq + $4::bigint"
        "  WHERE singleton"
        "    AND EXISTS (SELECT 1 FROM inserted)";
    res = PQexecParams(c, ins_sql, 4, nullptr, ins_params, nullptr, nullptr, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "verify_and_register_entry_manifest: FATAL INSERT failed: " << PQerrorMessage(c) << std::endl;
        std::abort();
    }
    PQclear(res);

    const char* sel_params[2] = { epoch_hex.c_str(), log_idx_s.c_str() };
    const char* sel_sql =
        "SELECT encode(entry_digest, 'hex'), expected_items, merkle_apply_seq_base"
        "  FROM ariabc_internal.raft_apply_entry"
        " WHERE epoch_id = decode($1, 'hex')"
        "   AND raft_log_index = $2::bigint FOR UPDATE";
    res = PQexecParams(c, sel_sql, 2, nullptr, sel_params, nullptr, nullptr, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1) {
        std::cerr << "verify_and_register_entry_manifest: FATAL SELECT parent failed: " << PQerrorMessage(c) << std::endl;
        std::abort();
    }
    std::string db_digest = PQgetvalue(res, 0, 0);
    int db_expected = std::atoi(PQgetvalue(res, 0, 1));
    uint64_t db_merkle_seq_base = std::strtoull(PQgetvalue(res, 0, 2), nullptr, 10);
    PQclear(res);
    if (db_digest != entry_digest_hex || db_expected != static_cast<int>(expected_items) ||
        db_merkle_seq_base == 0) {
        std::cerr << "FATAL: log corruption or split-brain at log_idx=" << log_idx
                  << " incoming digest=" << entry_digest_hex << " count=" << expected_items
                  << " db digest=" << db_digest << " db count=" << db_expected
                  << " merkle_seq_base=" << db_merkle_seq_base << std::endl;
        std::abort();
    }

    for (size_t i = 0; i < item_digests_hex.size(); ++i) {
        const std::string ord_s = std::to_string(i);
        const char* ins_item_params[4] = {
            epoch_hex.c_str(),
            log_idx_s.c_str(),
            ord_s.c_str(),
            item_digests_hex[i].c_str()
        };
        const char* ins_item_sql =
            "INSERT INTO ariabc_internal.raft_apply_entry_item"
            " (epoch_id, raft_log_index, item_ordinal, item_digest)"
            " VALUES (decode($1,'hex'), $2::bigint, $3::integer, decode($4,'hex'))"
            " ON CONFLICT (epoch_id, raft_log_index, item_ordinal) DO NOTHING";
        res = PQexecParams(c, ins_item_sql, 4, nullptr, ins_item_params, nullptr, nullptr, 0);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::cerr << "verify_and_register_entry_manifest: FATAL ITEM INSERT failed: " << PQerrorMessage(c) << std::endl;
            std::abort();
        }
        PQclear(res);
    }

    const char* sel_item_sql =
        "SELECT item_ordinal, encode(item_digest, 'hex')"
        "  FROM ariabc_internal.raft_apply_entry_item"
        " WHERE epoch_id = decode($1, 'hex')"
        "   AND raft_log_index = $2::bigint"
        " ORDER BY item_ordinal ASC";
    res = PQexecParams(c, sel_item_sql, 2, nullptr, sel_params, nullptr, nullptr, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "verify_and_register_entry_manifest: FATAL SELECT items failed: " << PQerrorMessage(c) << std::endl;
        std::abort();
    }

    int num_items = PQntuples(res);
    if (num_items != static_cast<int>(expected_items)) {
        std::cerr << "verify_and_register_entry_manifest: FATAL count mismatch, expected " << expected_items << " got " << num_items << std::endl;
        std::abort();
    }
    for (int i = 0; i < num_items; ++i) {
        int db_ordinal = std::atoi(PQgetvalue(res, i, 0));
        std::string db_item_digest = PQgetvalue(res, i, 1);
        if (db_ordinal != i) {
            std::cerr << "verify_and_register_entry_manifest: FATAL ordinal mismatch, expected " << i << " got " << db_ordinal << std::endl;
            std::abort();
        }
        if (db_item_digest != item_digests_hex[i]) {
            std::cerr << "verify_and_register_entry_manifest: FATAL item digest mismatch at ordinal " << i << std::endl;
            std::abort();
        }
    }
    PQclear(res);

    res = PQexec(c, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "verify_and_register_entry_manifest: FATAL COMMIT failed: " << PQerrorMessage(c) << std::endl;
        std::abort();
    }
    PQclear(res);

    return true;
}

pg_executor_stats pg_executor::stats() const {
    pg_executor_stats out;
    out.enqueued = st_enqueued_.load(std::memory_order_relaxed);
    out.dequeued = st_dequeued_.load(std::memory_order_relaxed);
    out.q_wait_ns = st_q_wait_ns_.load(std::memory_order_relaxed);
    out.queue_delay_ns = st_queue_delay_ns_.load(std::memory_order_relaxed);
    out.queue_delay_dequeue_ns = st_queue_delay_dequeue_ns_.load(std::memory_order_relaxed);
    out.queue_delay_exec_start_ns = st_queue_delay_exec_start_ns_.load(std::memory_order_relaxed);
    out.backlog_cur = st_backlog_cur_.load(std::memory_order_relaxed);
    out.inflight_cur = st_inflight_cur_.load(std::memory_order_relaxed);
    out.inflight_max = st_inflight_max_.load(std::memory_order_relaxed);
    out.inflight_at_cap_ns = st_inflight_at_cap_ns_.load(std::memory_order_relaxed);
    {
        const uint64_t time_ns = st_inflight_time_ns_.load(std::memory_order_relaxed);
        const uint64_t area_ns = st_inflight_area_ns_.load(std::memory_order_relaxed);
        out.inflight_avg =
            (time_ns > 0) ? (static_cast<double>(area_ns) / static_cast<double>(time_ns)) : 0.0;
    }
    out.delayed_cur = st_delayed_cur_.load(std::memory_order_relaxed);
    out.conn_acquire_calls = st_conn_acquire_calls_.load(std::memory_order_relaxed);
    out.conn_acquire_wait_ns = st_conn_acquire_wait_ns_.load(std::memory_order_relaxed);
    out.exec_calls = st_exec_calls_.load(std::memory_order_relaxed);
    out.exec_ns = st_exec_ns_.load(std::memory_order_relaxed);
    out.pg_query_ns = st_pg_query_ns_.load(std::memory_order_relaxed);
    out.threaded_workers_configured = st_threaded_workers_configured_.load(std::memory_order_relaxed);
    out.threaded_workers_created = st_threaded_workers_created_.load(std::memory_order_relaxed);
    out.owned_pg_connections = st_owned_pg_connections_.load(std::memory_order_relaxed);
    out.concurrent_pqexec_cur = st_concurrent_pqexec_cur_.load(std::memory_order_relaxed);
    out.concurrent_pqexec_max = st_concurrent_pqexec_max_.load(std::memory_order_relaxed);
    out.overlapping_pqexec_intervals = st_overlapping_pqexec_intervals_.load(std::memory_order_relaxed);
    out.result_format_ns = st_result_format_ns_.load(std::memory_order_relaxed);
    out.retryable_sqlstate_40001 = st_retryable_sqlstate_40001_.load(std::memory_order_relaxed);
    out.retryable_sqlstate_40P01 = st_retryable_sqlstate_40P01_.load(std::memory_order_relaxed);
    out.retryable_sqlstate_57014 = st_retryable_sqlstate_57014_.load(std::memory_order_relaxed);
    out.retry_attempts_total = st_retry_attempts_total_.load(std::memory_order_relaxed);
    out.retry_exhausted_total = st_retry_exhausted_total_.load(std::memory_order_relaxed);
    out.kafka_flush_calls = st_kafka_flush_calls_.load(std::memory_order_relaxed);
    out.kafka_payload_bytes = st_kafka_payload_bytes_.load(std::memory_order_relaxed);
    out.kafka_build_payload_ns = st_kafka_build_payload_ns_.load(std::memory_order_relaxed);
    out.kafka_send_ns = st_kafka_send_ns_.load(std::memory_order_relaxed);
    out.kafka_batch_records = st_kafka_batch_records_.load(std::memory_order_relaxed);
    out.kafka_batch_records_max = st_kafka_batch_records_max_.load(std::memory_order_relaxed);
    out.kafka_batch_dwell_ns = st_kafka_batch_dwell_ns_.load(std::memory_order_relaxed);
    out.kafka_batch_dwell_max_ns = st_kafka_batch_dwell_max_ns_.load(std::memory_order_relaxed);
    out.kafka_flush_reason_records = st_kafka_flush_reason_records_.load(std::memory_order_relaxed);
    out.kafka_flush_reason_bytes = st_kafka_flush_reason_bytes_.load(std::memory_order_relaxed);
    out.kafka_flush_reason_age = st_kafka_flush_reason_age_.load(std::memory_order_relaxed);
    out.kafka_flush_reason_idle = st_kafka_flush_reason_idle_.load(std::memory_order_relaxed);
    out.kafka_flush_reason_final = st_kafka_flush_reason_final_.load(std::memory_order_relaxed);
    out.queue_depth_samples = st_queue_depth_samples_.load(std::memory_order_relaxed);
    out.queue_depth_sum = st_queue_depth_sum_.load(std::memory_order_relaxed);
    out.queue_depth_max = st_queue_depth_max_.load(std::memory_order_relaxed);
    out.queue_depth_bin_le_16 = st_queue_depth_bin_le_16_.load(std::memory_order_relaxed);
    out.queue_depth_bin_17_64 = st_queue_depth_bin_17_64_.load(std::memory_order_relaxed);
    out.queue_depth_bin_65_256 = st_queue_depth_bin_65_256_.load(std::memory_order_relaxed);
    out.queue_depth_bin_gt_256 = st_queue_depth_bin_gt_256_.load(std::memory_order_relaxed);

    out.kafka_batch_records_bin_1 = st_kafka_batch_records_bin_1_.load(std::memory_order_relaxed);
    out.kafka_batch_records_bin_2_15 = st_kafka_batch_records_bin_2_15_.load(std::memory_order_relaxed);
    out.kafka_batch_records_bin_16_63 = st_kafka_batch_records_bin_16_63_.load(std::memory_order_relaxed);
    out.kafka_batch_records_bin_64_255 = st_kafka_batch_records_bin_64_255_.load(std::memory_order_relaxed);
    out.kafka_batch_records_bin_256_plus = st_kafka_batch_records_bin_256_plus_.load(std::memory_order_relaxed);

    out.kafka_batch_bytes_bin_le_1k = st_kafka_batch_bytes_bin_le_1k_.load(std::memory_order_relaxed);
    out.kafka_batch_bytes_bin_1k_10k = st_kafka_batch_bytes_bin_1k_10k_.load(std::memory_order_relaxed);
    out.kafka_batch_bytes_bin_10k_100k = st_kafka_batch_bytes_bin_10k_100k_.load(std::memory_order_relaxed);
    out.kafka_batch_bytes_bin_100k_plus = st_kafka_batch_bytes_bin_100k_plus_.load(std::memory_order_relaxed);

    out.kafka_batch_dwell_bin_le_1ms = st_kafka_batch_dwell_bin_le_1ms_.load(std::memory_order_relaxed);
    out.kafka_batch_dwell_bin_1_5ms = st_kafka_batch_dwell_bin_1_5ms_.load(std::memory_order_relaxed);
    out.kafka_batch_dwell_bin_5_20ms = st_kafka_batch_dwell_bin_5_20ms_.load(std::memory_order_relaxed);
    out.kafka_batch_dwell_bin_20_100ms = st_kafka_batch_dwell_bin_20_100ms_.load(std::memory_order_relaxed);
    out.kafka_batch_dwell_bin_100ms_plus = st_kafka_batch_dwell_bin_100ms_plus_.load(std::memory_order_relaxed);

    out.kafka_flush_backlog_bin_0 = st_kafka_flush_backlog_bin_0_.load(std::memory_order_relaxed);
    out.kafka_flush_backlog_bin_1_15 = st_kafka_flush_backlog_bin_1_15_.load(std::memory_order_relaxed);
    out.kafka_flush_backlog_bin_16_63 = st_kafka_flush_backlog_bin_16_63_.load(std::memory_order_relaxed);
    out.kafka_flush_backlog_bin_64_255 = st_kafka_flush_backlog_bin_64_255_.load(std::memory_order_relaxed);
    out.kafka_flush_backlog_bin_256_plus = st_kafka_flush_backlog_bin_256_plus_.load(std::memory_order_relaxed);

    out.kafka_flush_inflight_bin_0 = st_kafka_flush_inflight_bin_0_.load(std::memory_order_relaxed);
    out.kafka_flush_inflight_bin_1_15 = st_kafka_flush_inflight_bin_1_15_.load(std::memory_order_relaxed);
    out.kafka_flush_inflight_bin_16_63 = st_kafka_flush_inflight_bin_16_63_.load(std::memory_order_relaxed);
    out.kafka_flush_inflight_bin_64_255 = st_kafka_flush_inflight_bin_64_255_.load(std::memory_order_relaxed);
    out.kafka_flush_inflight_bin_256_plus = st_kafka_flush_inflight_bin_256_plus_.load(std::memory_order_relaxed);

    out.queue_overload_enter = st_queue_overload_enter_.load(std::memory_order_relaxed);
    out.queue_overload_exit = st_queue_overload_exit_.load(std::memory_order_relaxed);
    out.queue_high_watermark = static_cast<uint64_t>(queue_high_wm_);
    out.queue_low_watermark = static_cast<uint64_t>(queue_low_wm_);
    out.queue_depth_cur = st_queue_depth_cur_.load(std::memory_order_relaxed);
    out.queue_overloaded = queue_overloaded_.load(std::memory_order_relaxed) ? 1 : 0;
    out.bcdb_init_enabled = bcdb_init_done_ ? 1 : 0;
    out.bcdb_block_size = bcdb_block_size_;
    out.bcdb_init_arg_size_configured =
        (db_opt_.bcdb_init_block_size > 0)
            ? db_opt_.bcdb_init_block_size
            : db_opt_.conn_pool_size;
    out.det_block_batches = st_det_block_batches_.load(std::memory_order_relaxed);
    out.det_block_items = st_det_block_items_.load(std::memory_order_relaxed);
    out.det_block_min = st_det_block_min_.load(std::memory_order_relaxed);
    out.det_block_max = st_det_block_max_.load(std::memory_order_relaxed);
    out.det_block_bin_1 = st_det_block_bin_1_.load(std::memory_order_relaxed);
    out.det_block_bin_2_15 = st_det_block_bin_2_15_.load(std::memory_order_relaxed);
    out.det_block_bin_16_63 = st_det_block_bin_16_63_.load(std::memory_order_relaxed);
    out.det_block_bin_64_127 = st_det_block_bin_64_127_.load(std::memory_order_relaxed);
    out.det_block_bin_128_plus = st_det_block_bin_128_plus_.load(std::memory_order_relaxed);
    out.det_block_fallbacks = st_det_block_fallbacks_.load(std::memory_order_relaxed);
    out.det_block_skipped_readonly = st_det_block_skipped_readonly_.load(std::memory_order_relaxed);
    out.ready_det_results_max = st_ready_det_results_max_.load(std::memory_order_relaxed);
    out.ordered_emit_wait_ns = st_ordered_emit_wait_ns_.load(std::memory_order_relaxed);
    out.kafka_immediate_records = st_kafka_immediate_records_.load(std::memory_order_relaxed);
    out.ordered_apply_wait_ns = st_ordered_apply_wait_ns_.load(std::memory_order_relaxed);
    out.ordered_apply_pending_max = st_ordered_apply_pending_max_.load(std::memory_order_relaxed);
    out.det_raw_compat_mode = det_raw_compat_mode_ ? 1 : 0;
    out.det_prefixed_direct_parallel = det_prefixed_direct_parallel_ ? 1 : 0;
    out.det_completion_only_success = det_completion_only_success_ ? 1 : 0;
    out.det_raw_compat_activations =
        st_det_raw_compat_activations_.load(std::memory_order_relaxed);
    out.det_raw_compat_first_req_id = det_raw_compat_first_req_id_;
    out.det_raw_compat_first_sql_prefix = det_raw_compat_first_sql_prefix_;
    out.det_fastpath_blocks_submitted = st_fastpath_blocks_submitted_.load(std::memory_order_relaxed);
    out.det_fastpath_blocks_returned  = st_fastpath_blocks_returned_.load(std::memory_order_relaxed);
    out.det_fastpath_blocks_emitted   = st_fastpath_blocks_emitted_.load(std::memory_order_relaxed);
    out.det_fastpath_ready_blocks_max = st_fastpath_ready_blocks_max_.load(std::memory_order_relaxed);
    out.det_fastpath_last_submitted_block_id = st_fastpath_last_submitted_block_id_.load(std::memory_order_relaxed);
    out.det_fastpath_last_returned_block_id  = st_fastpath_last_returned_block_id_.load(std::memory_order_relaxed);
    out.det_fastpath_last_returned_block_seq = st_fastpath_last_returned_block_seq_.load(std::memory_order_relaxed);
    out.det_fastpath_last_emitted_seq = st_fastpath_last_emitted_seq_.load(std::memory_order_relaxed);
    out.det_fastpath_submit_to_return_max_us = st_fastpath_submit_to_return_max_us_.load(std::memory_order_relaxed);
    out.det_fastpath_send_failures = st_fastpath_send_failures_.load(std::memory_order_relaxed);
    out.det_fastpath_requeues = st_fastpath_requeues_.load(std::memory_order_relaxed);
    out.det_fastpath_reconnect_failures = st_fastpath_reconnect_failures_.load(std::memory_order_relaxed);

    const kafka_producer_stats kstats = kafka_prod_.stats();
    out.kafka_delivery_pending_current = kstats.delivery_pending_current;
    out.kafka_delivery_pending_max = std::max(st_kafka_delivery_pending_max_.load(std::memory_order_relaxed), kstats.delivery_pending_max);

    out.result_flush_count = st_result_flush_count_.load(std::memory_order_relaxed);
    out.result_flush_records_total = st_result_flush_records_total_.load(std::memory_order_relaxed);
    out.result_flush_records_max = st_result_flush_records_max_.load(std::memory_order_relaxed);
    out.result_flush_due_to_record_cap = st_result_flush_due_to_record_cap_.load(std::memory_order_relaxed);
    out.result_flush_due_to_byte_cap = st_result_flush_due_to_byte_cap_.load(std::memory_order_relaxed);
    out.result_flush_due_to_age = st_result_flush_due_to_age_.load(std::memory_order_relaxed);
    out.result_flush_due_to_idle = st_result_flush_due_to_idle_.load(std::memory_order_relaxed);
    out.result_flush_due_to_error = st_result_flush_due_to_error_.load(std::memory_order_relaxed);
    out.result_flush_due_to_shutdown = st_result_flush_due_to_shutdown_.load(std::memory_order_relaxed);
    out.result_flush_while_delivery_pending_gt_8 = st_result_flush_while_delivery_pending_gt_8_.load(std::memory_order_relaxed);
    out.result_flush_while_delivery_pending_gt_32 = st_result_flush_while_delivery_pending_gt_32_.load(std::memory_order_relaxed);
    out.kafka_delivery_pending_over_8_events = kstats.pending_crossed_above_8;
    out.kafka_delivery_pending_over_32_events = kstats.pending_crossed_above_32;
    out.kafka_async_publisher_enabled = kafka_async_publisher_enabled_ ? 1 : 0;
    out.kafka_async_publisher_queue_max =
        st_kafka_async_publisher_queue_max_.load(std::memory_order_relaxed);

    return out;
}

kafka_producer_stats pg_executor::kafka_stats() const {
    return kafka_prod_.stats();
}

bool pg_executor::admission_control_blocked() const {
    return queue_overloaded_.load(std::memory_order_relaxed);
}

bool pg_executor::wait_for_admission_drain(uint64_t max_wait_ns) {
    if (!queue_overloaded_.load(std::memory_order_acquire)) return true;
    if (stop_.load(std::memory_order_relaxed)) return false;
    std::unique_lock<std::mutex> lk(admission_mu_);
    // Re-check under the lock to avoid lost-wakeup.
    if (!queue_overloaded_.load(std::memory_order_acquire)) return true;
    if (stop_.load(std::memory_order_relaxed)) return false;
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::nanoseconds(max_wait_ns);
    admission_cv_.wait_until(lk, deadline, [this] {
        return !queue_overloaded_.load(std::memory_order_acquire) ||
               stop_.load(std::memory_order_relaxed);
    });
    return !queue_overloaded_.load(std::memory_order_acquire);
}

bool pg_executor::wait_for_ordered_emit_turn(uint64_t dispatch_seq) {
    if (!(db_opt_.db_type == 1 && !event_mode_ && det_parallel_workers_) ||
        det_threaded_direct_no_preapply_wait_ ||
        dispatch_seq == 0) {
        return true;
    }
    std::unique_lock<std::mutex> lk(det_emit_mu_);
    det_emit_cv_.wait(lk, [&] {
        return stop_.load(std::memory_order_relaxed) || dispatch_seq == det_next_emit_seq_;
    });
    return !stop_.load(std::memory_order_relaxed);
}

void pg_executor::finish_ordered_emit(uint64_t dispatch_seq) {
    if (!(db_opt_.db_type == 1 && !event_mode_ && det_parallel_workers_) ||
        det_threaded_direct_no_preapply_wait_ ||
        dispatch_seq == 0) {
        return;
    }
    {
        std::lock_guard<std::mutex> lk(det_emit_mu_);
        if (dispatch_seq >= det_next_emit_seq_) {
            det_next_emit_seq_ = dispatch_seq + 1;
        }
    }
    det_emit_cv_.notify_all();
}

bool pg_executor::async_kafka_publisher_active() const {
    return kafka_async_publisher_enabled_;
}

void pg_executor::enqueue_kafka_result(const task& t, const ConfirmedResult& confirmed) {
    if (!async_kafka_publisher_active()) return;

    kafka_result_record rec;
    rec.req_id = t.req_id;
    rec.result = confirmed.payload;
    rec.raft_log_idx = t.raft_log_idx;
    rec.leader_hint = t.leader_node_hint;
    rec.terminal_digest = confirmed.terminal_digest;
    rec.append_ns = now_steady_ns();
    rec.raft_item_ordinal = t.raft_item_ordinal;
    rec.terminal_state = confirmed.terminal_state;
    rec.format_version = confirmed.format_version;
    rec.dispatch_seq = t.dispatch_seq;
    rec.ready_ns = rec.append_ns;
    rec.bytes = t.req_id.size() + confirmed.payload.size() + 32;

    {
        std::lock_guard<std::mutex> lk(kafka_pub_mu_);
        kafka_pub_q_.push_back(std::move(rec));
        update_atomic_max(st_kafka_async_publisher_queue_max_,
                          static_cast<uint64_t>(kafka_pub_q_.size()));
    }
    kafka_pub_cv_.notify_one();
}

void pg_executor::publish_kafka_result_batch(std::vector<kafka_result_record>& batch,
                                             kafka_flush_reason reason) {
    if (!kafka_enabled_ || batch.empty()) return;

    st_kafka_flush_calls_.fetch_add(1, std::memory_order_relaxed);
    const uint64_t flush_ns = now_steady_ns();
    const uint64_t batch_records = static_cast<uint64_t>(batch.size());
    st_kafka_batch_records_.fetch_add(batch_records, std::memory_order_relaxed);
    update_atomic_max(st_kafka_batch_records_max_, batch_records);

    std::vector<std::string> batch_req_ids;
    std::vector<std::string> batch_results;
    std::vector<uint64_t> batch_raft_log_idxs;
    std::vector<int> batch_leader_hints;
    std::vector<std::string> batch_terminal_digests;
    std::vector<uint32_t> batch_raft_item_ordinals;
    std::vector<std::string> batch_terminal_states;
    std::vector<int> batch_format_versions;
    batch_req_ids.reserve(batch.size());
    batch_results.reserve(batch.size());
    batch_raft_log_idxs.reserve(batch.size());
    batch_leader_hints.reserve(batch.size());
    batch_terminal_digests.reserve(batch.size());
    batch_raft_item_ordinals.reserve(batch.size());
    batch_terminal_states.reserve(batch.size());
    batch_format_versions.reserve(batch.size());

    uint64_t total_dwell_ns = 0;
    size_t batch_bytes = 0;
    for (const auto& rec : batch) {
        batch_req_ids.push_back(rec.req_id);
        batch_results.push_back(rec.result);
        batch_raft_log_idxs.push_back(rec.raft_log_idx);
        batch_leader_hints.push_back(rec.leader_hint);
        batch_terminal_digests.push_back(rec.terminal_digest);
        batch_raft_item_ordinals.push_back(rec.raft_item_ordinal);
        batch_terminal_states.push_back(rec.terminal_state);
        batch_format_versions.push_back(rec.format_version);
        batch_bytes += rec.bytes;
        if (flush_ns >= rec.append_ns) {
            const uint64_t dwell_ns = flush_ns - rec.append_ns;
            st_kafka_batch_dwell_ns_.fetch_add(dwell_ns, std::memory_order_relaxed);
            update_atomic_max(st_kafka_batch_dwell_max_ns_, dwell_ns);
            total_dwell_ns += dwell_ns;
        }
    }

    const uint64_t backlog = st_backlog_cur_.load(std::memory_order_relaxed);
    if (backlog == 0) st_kafka_flush_backlog_bin_0_.fetch_add(1, std::memory_order_relaxed);
    else if (backlog <= 15) st_kafka_flush_backlog_bin_1_15_.fetch_add(1, std::memory_order_relaxed);
    else if (backlog <= 63) st_kafka_flush_backlog_bin_16_63_.fetch_add(1, std::memory_order_relaxed);
    else if (backlog <= 255) st_kafka_flush_backlog_bin_64_255_.fetch_add(1, std::memory_order_relaxed);
    else st_kafka_flush_backlog_bin_256_plus_.fetch_add(1, std::memory_order_relaxed);

    const uint64_t inflight = st_inflight_cur_.load(std::memory_order_relaxed);
    if (inflight == 0) st_kafka_flush_inflight_bin_0_.fetch_add(1, std::memory_order_relaxed);
    else if (inflight <= 15) st_kafka_flush_inflight_bin_1_15_.fetch_add(1, std::memory_order_relaxed);
    else if (inflight <= 63) st_kafka_flush_inflight_bin_16_63_.fetch_add(1, std::memory_order_relaxed);
    else if (inflight <= 255) st_kafka_flush_inflight_bin_64_255_.fetch_add(1, std::memory_order_relaxed);
    else st_kafka_flush_inflight_bin_256_plus_.fetch_add(1, std::memory_order_relaxed);

    if (batch_records == 1) st_kafka_batch_records_bin_1_.fetch_add(1, std::memory_order_relaxed);
    else if (batch_records <= 15) st_kafka_batch_records_bin_2_15_.fetch_add(1, std::memory_order_relaxed);
    else if (batch_records <= 63) st_kafka_batch_records_bin_16_63_.fetch_add(1, std::memory_order_relaxed);
    else if (batch_records <= 255) st_kafka_batch_records_bin_64_255_.fetch_add(1, std::memory_order_relaxed);
    else st_kafka_batch_records_bin_256_plus_.fetch_add(1, std::memory_order_relaxed);

    if (batch_bytes <= 1024) st_kafka_batch_bytes_bin_le_1k_.fetch_add(1, std::memory_order_relaxed);
    else if (batch_bytes <= 10240) st_kafka_batch_bytes_bin_1k_10k_.fetch_add(1, std::memory_order_relaxed);
    else if (batch_bytes <= 102400) st_kafka_batch_bytes_bin_10k_100k_.fetch_add(1, std::memory_order_relaxed);
    else st_kafka_batch_bytes_bin_100k_plus_.fetch_add(1, std::memory_order_relaxed);

    uint64_t mean_dwell_ns = batch_records > 0 ? (total_dwell_ns / batch_records) : 0;
    if (mean_dwell_ns <= 1000000ULL) st_kafka_batch_dwell_bin_le_1ms_.fetch_add(1, std::memory_order_relaxed);
    else if (mean_dwell_ns <= 5000000ULL) st_kafka_batch_dwell_bin_1_5ms_.fetch_add(1, std::memory_order_relaxed);
    else if (mean_dwell_ns <= 20000000ULL) st_kafka_batch_dwell_bin_5_20ms_.fetch_add(1, std::memory_order_relaxed);
    else if (mean_dwell_ns <= 100000000ULL) st_kafka_batch_dwell_bin_20_100ms_.fetch_add(1, std::memory_order_relaxed);
    else st_kafka_batch_dwell_bin_100ms_plus_.fetch_add(1, std::memory_order_relaxed);

    st_result_flush_count_.fetch_add(1, std::memory_order_relaxed);
    st_result_flush_records_total_.fetch_add(batch_records, std::memory_order_relaxed);
    update_atomic_max(st_result_flush_records_max_, batch_records);

    switch (reason) {
        case kafka_flush_reason::RECORDS:
            st_kafka_flush_reason_records_.fetch_add(1, std::memory_order_relaxed);
            st_result_flush_due_to_record_cap_.fetch_add(1, std::memory_order_relaxed);
            break;
        case kafka_flush_reason::BYTES:
            st_kafka_flush_reason_bytes_.fetch_add(1, std::memory_order_relaxed);
            st_result_flush_due_to_byte_cap_.fetch_add(1, std::memory_order_relaxed);
            break;
        case kafka_flush_reason::AGE:
            st_kafka_flush_reason_age_.fetch_add(1, std::memory_order_relaxed);
            st_result_flush_due_to_age_.fetch_add(1, std::memory_order_relaxed);
            break;
        case kafka_flush_reason::IDLE:
            st_kafka_flush_reason_idle_.fetch_add(1, std::memory_order_relaxed);
            st_result_flush_due_to_idle_.fetch_add(1, std::memory_order_relaxed);
            break;
        case kafka_flush_reason::ERROR:
            st_result_flush_due_to_error_.fetch_add(1, std::memory_order_relaxed);
            break;
        case kafka_flush_reason::FINAL:
            st_kafka_flush_reason_final_.fetch_add(1, std::memory_order_relaxed);
            st_result_flush_due_to_shutdown_.fetch_add(1, std::memory_order_relaxed);
            break;
    }

    {
        const uint64_t dp = kafka_prod_.delivery_pending_relaxed();
        update_atomic_max(st_kafka_delivery_pending_max_, dp);
        if (dp > 8) {
            st_result_flush_while_delivery_pending_gt_8_.fetch_add(1, std::memory_order_relaxed);
        }
        if (dp > 32) {
            st_result_flush_while_delivery_pending_gt_32_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    std::string err;
    const auto b0 = std::chrono::steady_clock::now();
    const std::string payload = build_bin_batch_payload_v2(
        batch_req_ids,
        batch_results,
        batch_raft_log_idxs,
        batch_leader_hints,
        batch_terminal_digests,
        batch_raft_item_ordinals,
        batch_terminal_states,
        batch_format_versions,
        static_cast<uint16_t>(node_id_),
        result_sig_key_,
        db_opt_.raft_epoch_hex,
        false);
    const auto b1 = std::chrono::steady_clock::now();
    st_kafka_build_payload_ns_.fetch_add(
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(b1 - b0).count()),
        std::memory_order_relaxed);
    st_kafka_payload_bytes_.fetch_add(static_cast<uint64_t>(payload.size()),
                                      std::memory_order_relaxed);

    const auto s0 = std::chrono::steady_clock::now();
    const bool kafka_send_ok = kafka_prod_.send_payload(payload, batch_req_ids.front(), err);
    if (!kafka_send_ok) {
        std::cerr << "Kafka async result send failed: " << err << std::endl;
    }
    const auto s1 = std::chrono::steady_clock::now();
    st_kafka_send_ns_.fetch_add(
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count()),
        std::memory_order_relaxed);
}

void pg_executor::kafka_publisher_loop() {
    const size_t max_records = kafka_async_result_publisher_max_records();
    const size_t target_records = kafka_async_result_publisher_target_records();
    const size_t max_bytes = kafka_async_result_publisher_max_bytes();
    const int max_delay_us = kafka_async_result_publisher_delay_us();
    std::vector<kafka_result_record> batch;
    batch.reserve(max_records);

    for (;;) {
        batch.clear();
        size_t batch_bytes = 0;
        kafka_flush_reason reason = kafka_flush_reason::AGE;
        auto batch_start = std::chrono::steady_clock::now();

        {
            std::unique_lock<std::mutex> lk(kafka_pub_mu_);
            kafka_pub_cv_.wait(lk, [this] {
                return kafka_pub_stop_ || !kafka_pub_q_.empty();
            });
            if (kafka_pub_q_.empty() && kafka_pub_stop_) {
                break;
            }
            if (kafka_pub_q_.empty()) {
                continue;
            }

            batch_start = std::chrono::steady_clock::now();
            for (;;) {
                while (!kafka_pub_q_.empty() &&
                       batch.size() < max_records &&
                       batch_bytes < max_bytes) {
                    batch_bytes += kafka_pub_q_.front().bytes;
                    batch.push_back(std::move(kafka_pub_q_.front()));
                    kafka_pub_q_.pop_front();
                }
                if (batch.size() >= max_records) {
                    reason = kafka_flush_reason::RECORDS;
                    break;
                }
                if (batch_bytes >= max_bytes) {
                    reason = kafka_flush_reason::BYTES;
                    break;
                }
                if (kafka_pub_stop_) {
                    reason = kafka_flush_reason::FINAL;
                    break;
                }
                if (batch.size() >= target_records) {
                    reason = kafka_flush_reason::RECORDS;
                    break;
                }

                // Check pipeline state: if no active transactions are in-flight
                // or queued in PostgreSQL, and nothing is left in kafka_pub_q_,
                // all work for the current workload run is finished! Flush the
                // tail batch immediately without lingering.
                const uint64_t active_work =
                    st_inflight_cur_.load(std::memory_order_relaxed) +
                    st_backlog_cur_.load(std::memory_order_relaxed);
                if (active_work == 0 && kafka_pub_q_.empty()) {
                    reason = kafka_flush_reason::IDLE;
                    break;
                }

                const int linger_us = (max_delay_us > 0) ? max_delay_us : 2000;
                const auto deadline =
                    batch_start + std::chrono::microseconds(linger_us);
                const bool woke = kafka_pub_cv_.wait_until(lk, deadline, [this] {
                    return kafka_pub_stop_ || !kafka_pub_q_.empty();
                });
                if (woke) {
                    continue;
                }
                reason = kafka_flush_reason::AGE;
                break;
            }
        }

        if (!batch.empty()) {
            publish_kafka_result_batch(batch, reason);
        }
    }
}

uint64_t pg_executor::get_det_tx_seq(const task& t) const {
    if (t.has_assigned_det_seq) {
        return t.assigned_det_seq;
    }
    uint64_t seq = 0;
    if (parse_det_prefixed_sql_parts(t.sql, &seq, nullptr)) {
        return seq;
    }
    uint64_t tx_seq = 0;
    if (parse_req_num(t.req_id, tx_seq)) {
        return tx_seq;
    }
    return 0;
}

bool pg_executor::get_det_tx_seq_valid(const task& t, uint64_t& out_seq) const {
    if (t.has_assigned_det_seq) {
        out_seq = t.assigned_det_seq;
        return true;
    }
    if (parse_det_prefixed_sql_parts(t.sql, &out_seq, nullptr)) {
        return true;
    }
    if (parse_req_num(t.req_id, out_seq)) {
        return true;
    }
    out_seq = 0;
    return false;
}

void pg_executor::det_mark_tx_state(uint64_t tx_seq, det_tx_state st) {
    if (tx_seq == 0) return;
    std::lock_guard<std::mutex> lk(det_apply_mu_);
    if (st == det_tx_state::QUEUED) {
        if (!det_apply_initialized_) {
            det_next_apply_seq_ = tx_seq;
            det_apply_initialized_ = true;
        } else if (tx_seq < det_next_apply_seq_) {
            // Session-restart detection. Tasks are enqueued in Raft commit
            // order and popped in FIFO order, so within a single client
            // session tx_seqs arrive here strictly monotonically. A newly
            // queued tx_seq below the current head therefore means a new
            // client/session started a fresh deterministic numbering after a
            // previous session ran (e.g. the bcdb_init probe uses
            // detStartSeq=99000000; the subsequent workload restarts at 1, or
            // back-to-back gateway invocations both start at detStartSeq=1).
            // Rewind so the new session's requests aren't blocked forever
            // waiting for a det_next_apply_seq_ they will never reach.
            det_next_apply_seq_ = tx_seq;
            det_tx_states_.clear();
            det_apply_cv_.notify_all();
        }
    }
    det_tx_states_[tx_seq] = st;
}

bool pg_executor::det_wait_for_apply_turn(uint64_t tx_seq) {
    if (tx_seq == 0) return true;
    std::unique_lock<std::mutex> lk(det_apply_mu_);
    det_apply_cv_.wait(lk, [&] {
        return stop_.load(std::memory_order_relaxed) || tx_seq == det_next_apply_seq_;
    });
    return !stop_.load(std::memory_order_relaxed);
}

void pg_executor::det_finish_apply(uint64_t tx_seq) {
    if (tx_seq == 0) return;
    {
        std::lock_guard<std::mutex> lk(det_apply_mu_);
        det_tx_states_[tx_seq] = det_tx_state::APPLIED;
        if (tx_seq == det_next_apply_seq_) {
            ++det_next_apply_seq_;
        }
        auto it = det_tx_states_.find(tx_seq);
        if (it != det_tx_states_.end()) {
            det_tx_states_.erase(it);
        }
    }
    det_apply_cv_.notify_all();
}

void pg_executor::notify_task_applied(uint64_t raft_log_idx, uint32_t item_ordinal) {
    if (raft_log_idx == 0) return;
    if (on_task_applied_) {
        on_task_applied_(raft_log_idx, item_ordinal);
    }
}

void pg_executor::notify_task_failed(uint64_t raft_log_idx,
                                     uint32_t item_ordinal,
                                     const std::string& reason) {
    if (raft_log_idx == 0) return;
    if (on_task_failed_) {
        on_task_failed_(raft_log_idx, item_ordinal, reason);
    }
}



void pg_executor::mark_task_applied_ordered(uint64_t dispatch_seq,
                                            uint64_t raft_log_idx,
                                            uint32_t item_ordinal,
                                            uint64_t ready_ns) {
    if (raft_log_idx == 0) return;
    if (dispatch_seq == 0) {
        notify_task_applied(raft_log_idx, item_ordinal);
        return;
    }

    std::vector<std::pair<uint64_t, uint32_t>> ready_to_notify;
    {
        std::lock_guard<std::mutex> lk(det_ordered_apply_mu_);
        if (dispatch_seq < det_next_ordered_apply_seq_) {
            return;
        }
        det_ordered_apply_ready_[dispatch_seq] = {raft_log_idx, item_ordinal};
        const uint64_t pending = static_cast<uint64_t>(det_ordered_apply_ready_.size());
        uint64_t cur_max = st_ordered_apply_pending_max_.load(std::memory_order_relaxed);
        while (pending > cur_max &&
               !st_ordered_apply_pending_max_.compare_exchange_weak(
                   cur_max, pending, std::memory_order_relaxed)) {
        }

        for (;;) {
            auto it = det_ordered_apply_ready_.find(det_next_ordered_apply_seq_);
            if (it == det_ordered_apply_ready_.end()) break;
            ready_to_notify.push_back(it->second);
            det_ordered_apply_ready_.erase(it);
            ++det_next_ordered_apply_seq_;
        }
    }

    if (!ready_to_notify.empty() && ready_ns != 0) {
        const uint64_t now_ns = now_steady_ns();
        if (now_ns >= ready_ns) {
            st_ordered_apply_wait_ns_.fetch_add(now_ns - ready_ns,
                                                std::memory_order_relaxed);
        }
    }
    for (const auto& p : ready_to_notify) {
        notify_task_applied(p.first, p.second);
    }
}

void pg_executor::record_det_block_batch(size_t size, bool fallback) {
    if (size == 0) return;

    const uint64_t n = static_cast<uint64_t>(size);
    st_det_block_batches_.fetch_add(1, std::memory_order_relaxed);
    st_det_block_items_.fetch_add(n, std::memory_order_relaxed);
    if (fallback) {
        st_det_block_fallbacks_.fetch_add(1, std::memory_order_relaxed);
    }

    uint64_t cur_min = st_det_block_min_.load(std::memory_order_relaxed);
    while ((cur_min == 0 || n < cur_min) &&
           !st_det_block_min_.compare_exchange_weak(
               cur_min, n, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }

    uint64_t cur_max = st_det_block_max_.load(std::memory_order_relaxed);
    while (n > cur_max &&
           !st_det_block_max_.compare_exchange_weak(
               cur_max, n, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }

    if (n == 1) {
        st_det_block_bin_1_.fetch_add(1, std::memory_order_relaxed);
    } else if (n < 16) {
        st_det_block_bin_2_15_.fetch_add(1, std::memory_order_relaxed);
    } else if (n < 64) {
        st_det_block_bin_16_63_.fetch_add(1, std::memory_order_relaxed);
    } else if (n < 128) {
        st_det_block_bin_64_127_.fetch_add(1, std::memory_order_relaxed);
    } else {
        st_det_block_bin_128_plus_.fetch_add(1, std::memory_order_relaxed);
    }
}

void pg_executor::stop() {
    bool expected = false;
    if (!stop_.compare_exchange_strong(expected, true)) {
        // already stopped
    }
    q_cv_.notify_all();
    pool_cv_.notify_all();
    det_emit_cv_.notify_all();
    det_compat_cv_.notify_all();
    det_apply_cv_.notify_all();
    admission_cv_.notify_all();
    if (event_mode_ && wakeup_wfd_ >= 0) {
        const uint8_t b = 1;
        (void)::write(wakeup_wfd_, &b, 1);
    }
    if (event_thread_.joinable()) {
        event_thread_.join();
    }
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();

    {
        std::lock_guard<std::mutex> lk(kafka_pub_mu_);
        kafka_pub_stop_ = true;
    }
    kafka_pub_cv_.notify_all();
    if (kafka_pub_thread_.joinable()) {
        kafka_pub_thread_.join();
    }

    kafka_prod_.stop();

    if (bcdb_ctrl_conn_) {
        PQfinish(bcdb_ctrl_conn_);
        bcdb_ctrl_conn_ = nullptr;
    }
    {
        std::lock_guard<std::mutex> lk(manifest_mu_);
        if (manifest_conn_) {
            PQfinish(manifest_conn_);
            manifest_conn_ = nullptr;
        }
    }

    if (wakeup_rfd_ >= 0) {
        ::close(wakeup_rfd_);
        wakeup_rfd_ = -1;
    }
    if (wakeup_wfd_ >= 0) {
        ::close(wakeup_wfd_);
        wakeup_wfd_ = -1;
    }

    for (PGconn* c : all_conns_) {
        if (c) PQfinish(c);
    }
    all_conns_.clear();
    while (!pool_.empty()) pool_.pop();
}

PGconn* pg_executor::acquire_conn() {
    std::unique_lock<std::mutex> lk(pool_mu_);
    st_conn_acquire_calls_.fetch_add(1, std::memory_order_relaxed);
    const auto w0 = std::chrono::steady_clock::now();
    pool_cv_.wait(lk, [this] { return stop_.load() || !pool_.empty(); });
    const auto w1 = std::chrono::steady_clock::now();
    st_conn_acquire_wait_ns_.fetch_add(
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count()),
        std::memory_order_relaxed);
    if (stop_.load()) return nullptr;
    PGconn* c = pool_.front();
    pool_.pop();
    return c;
}

void pg_executor::release_conn(PGconn* c) {
    if (!c) return;
    {
        std::lock_guard<std::mutex> lk(pool_mu_);
        pool_.push(c);
    }
    pool_cv_.notify_one();
}

std::string pg_executor::exec_sql(PGconn* c, const std::string& sql, bool* is_error) {
    if (is_error) *is_error = false;
    if (!c) {
        if (is_error) *is_error = true;
        return "ERROR no_connection";
    }

    st_exec_calls_.fetch_add(1, std::memory_order_relaxed);
    const auto t0 = std::chrono::steady_clock::now();

    notice_state* ns = nullptr;
    auto it = notice_state_by_conn_.find(c);
    if (it != notice_state_by_conn_.end()) ns = it->second;

    // Best-effort reconnect if needed.
    if (PQstatus(c) != CONNECTION_OK) {
        PQreset(c);
        if (ns) {
            PQsetNoticeProcessor(c, &pg_executor::notice_processor, ns);
        }
    }

    for (int attempt = 0; attempt <= db_opt_.max_retries; ++attempt) {
        if (ns) ns->last_merkle_roots.clear();
        const auto q0 = std::chrono::steady_clock::now();
        const uint64_t concurrent_now =
            st_concurrent_pqexec_cur_.fetch_add(1, std::memory_order_relaxed) + 1;
        update_atomic_max_u64(st_concurrent_pqexec_max_, concurrent_now);
        if (concurrent_now > 1) {
            st_overlapping_pqexec_intervals_.fetch_add(1, std::memory_order_relaxed);
        }
        PGresult* res = PQexec(c, sql.c_str());
        st_concurrent_pqexec_cur_.fetch_sub(1, std::memory_order_relaxed);
        const auto q1 = std::chrono::steady_clock::now();
        st_pg_query_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(q1 - q0).count()),
            std::memory_order_relaxed);
        const ExecStatusType st = res ? PQresultStatus(res) : PGRES_FATAL_ERROR;

        if (st == PGRES_TUPLES_OK || st == PGRES_COMMAND_OK) {
            const auto f0 = std::chrono::steady_clock::now();
            std::string out = format_result(res);
            const auto f1 = std::chrono::steady_clock::now();
            st_result_format_ns_.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(f1 - f0).count()),
                std::memory_order_relaxed);
            if (ns && !ns->last_merkle_roots.empty()) {
                // BCDB emits Merkle roots via NOTICE. Keep those for offline
                // verification/profiling, but do not fold them into the
                // majority-voted SQL result string or identical reads can hash
                // differently across replicas.
                ns->last_merkle_roots.clear();
            }
            PQclear(res);

            const auto t1 = std::chrono::steady_clock::now();
            st_exec_ns_.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()),
                std::memory_order_relaxed);
            return out;
        }

        const char* sqlstate = res ? PQresultErrorField(res, PG_DIAG_SQLSTATE) : nullptr;
        const bool retryable = is_retryable_sqlstate(sqlstate);
        const std::string err_msg = res ? trim_copy(PQresultErrorMessage(res)) : "null result";
        if (res) PQclear(res);

        if (!retryable) {
            const auto t1 = std::chrono::steady_clock::now();
            st_exec_ns_.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()),
                std::memory_order_relaxed);
            if (is_error) *is_error = true;
            return "ERROR " + err_msg;
        }
        if (retryable && sqlstate) {
            if (strcmp(sqlstate, "40001") == 0) {
                st_retryable_sqlstate_40001_.fetch_add(1, std::memory_order_relaxed);
            } else if (strcmp(sqlstate, "40P01") == 0) {
                st_retryable_sqlstate_40P01_.fetch_add(1, std::memory_order_relaxed);
            } else if (strcmp(sqlstate, "57014") == 0) {
                st_retryable_sqlstate_57014_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        if (attempt == db_opt_.max_retries) {
            st_retry_exhausted_total_.fetch_add(1, std::memory_order_relaxed);
            const auto t1 = std::chrono::steady_clock::now();
            st_exec_ns_.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()),
                std::memory_order_relaxed);
            if (is_error) *is_error = true;
            return "ERROR(retry_exhausted) " + err_msg;
        }

        st_retry_attempts_total_.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::milliseconds(db_opt_.retry_backoff_ms));
    }

    {
        const auto t1 = std::chrono::steady_clock::now();
        st_exec_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()),
            std::memory_order_relaxed);
    }
    if (is_error) *is_error = true;
    return "ERROR unexpected";
}

bool pg_executor::exec_det_block_batch(PGconn* c,
                                       uint64_t tx_key_start,
                                       const std::vector<task>& tasks,
                                       std::vector<std::string>& out_results) {
    out_results.clear();
    if (!c || tasks.empty()) return false;

    std::vector<std::pair<std::string, std::string>> txs;
    txs.reserve(tasks.size());

    for (size_t i = 0; i < tasks.size(); ++i) {
        uint64_t seq = 0;
        std::string raw_sql;
        if (!parse_det_prefixed_sql_parts(tasks[i].sql, &seq, &raw_sql)) {
            return false;
        }
        // Use a session-unique monotonically allocated key so tx_pool entries
        // from previous server sessions sharing the same PostgreSQL instance
        // don't collide. This used to be block_id * 1024 + slot, which silently
        // capped safe block sizes at 1024 because larger blocks overlapped the
        // next block's key range.
        const std::string key = std::to_string(tx_key_start + static_cast<uint64_t>(i));
        txs.emplace_back(key, std::move(raw_sql));
    }

    const bool safe_mode = (db_opt_.raft_apply_ledger_mode == "safe");
    const std::string sql = build_bcdb_block_submit_results_sql(
        det_next_block_id_++,
        txs,
        tasks,
        nullptr,
        db_opt_.raft_epoch_hex,
        safe_mode);
    if (safe_mode) {
        log_safe_metadata_submit(tasks);
    }

    notice_state* ns = nullptr;
    auto it = notice_state_by_conn_.find(c);
    if (it != notice_state_by_conn_.end()) ns = it->second;

    if (PQstatus(c) != CONNECTION_OK) {
        PQreset(c);
        if (ns) {
            PQsetNoticeProcessor(c, &pg_executor::notice_processor, ns);
        }
    }
    if (ns) ns->last_merkle_roots.clear();

    const auto q0 = std::chrono::steady_clock::now();
    PGresult* res = PQexec(c, sql.c_str());
    const auto q1 = std::chrono::steady_clock::now();
    st_pg_query_ns_.fetch_add(
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(q1 - q0).count()),
        std::memory_order_relaxed);

    const ExecStatusType st = res ? PQresultStatus(res) : PGRES_FATAL_ERROR;
    if (!(st == PGRES_TUPLES_OK || st == PGRES_COMMAND_OK) ||
        !res || PQntuples(res) < 1 || PQnfields(res) < 1) {
        if (res) PQclear(res);
        return false;
    }

    const auto f0 = std::chrono::steady_clock::now();
    const char* val = PQgetvalue(res, 0, 0);
    std::unordered_map<std::string, std::string> result_by_hash;
    const bool parsed = parse_bcdb_block_results_text(val ? std::string(val) : std::string(),
                                                      result_by_hash);
    const auto f1 = std::chrono::steady_clock::now();
    st_result_format_ns_.fetch_add(
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(f1 - f0).count()),
        std::memory_order_relaxed);
    if (ns && !ns->last_merkle_roots.empty()) {
        ns->last_merkle_roots.clear();
    }
    PQclear(res);
    if (!parsed) return false;

    out_results.reserve(tasks.size());
    for (size_t i = 0; i < tasks.size(); ++i) {
        const std::string key = std::to_string(tx_key_start + static_cast<uint64_t>(i));
        auto rit = result_by_hash.find(key);
        if (rit == result_by_hash.end()) return false;
        out_results.push_back(rit->second);
    }
    return (out_results.size() == tasks.size());
}

void pg_executor::worker_loop() {
    std::vector<std::string> batch_req_ids;
    std::vector<std::string> batch_results;
    std::vector<uint64_t> batch_raft_log_idxs;
    std::vector<int> batch_leader_hints;
    std::vector<std::string> batch_terminal_digests;
    std::vector<uint64_t> batch_append_ns;
    std::vector<uint32_t> batch_raft_item_ordinals;
    std::vector<std::string> batch_terminal_states;
    std::vector<int> batch_format_versions;
    std::vector<uint64_t> batch_dispatch_seqs;
    std::vector<uint64_t> batch_ready_ns;
    size_t batch_bytes = 0;
    auto batch_start = std::chrono::steady_clock::now();

    enum event_flush_reason {
        FLUSH_REASON_RECORDS,
        FLUSH_REASON_BYTES,
        FLUSH_REASON_AGE,
        FLUSH_REASON_IDLE,
        FLUSH_REASON_ERROR,
        FLUSH_REASON_FINAL
    };

    auto update_atomic_max = [](std::atomic<uint64_t>& target, uint64_t value) {
        uint64_t cur = target.load(std::memory_order_relaxed);
        while (value > cur &&
               !target.compare_exchange_weak(cur,
                                             value,
                                             std::memory_order_relaxed,
                                             std::memory_order_relaxed)) {
        }
    };

    auto flush_batch = [&](event_flush_reason reason = FLUSH_REASON_FINAL) {
        if (!kafka_enabled_ || batch_req_ids.empty()) {
            batch_req_ids.clear();
            batch_results.clear();
            batch_raft_log_idxs.clear();
            batch_leader_hints.clear();
            batch_terminal_digests.clear();
            batch_append_ns.clear();
            batch_raft_item_ordinals.clear();
            batch_terminal_states.clear();
            batch_format_versions.clear();
            batch_dispatch_seqs.clear();
            batch_ready_ns.clear();
            batch_bytes = 0;
            batch_start = std::chrono::steady_clock::now();
            return;
        }

        st_kafka_flush_calls_.fetch_add(1, std::memory_order_relaxed);
        const uint64_t flush_ns = now_steady_ns();
        const uint64_t batch_records = static_cast<uint64_t>(batch_req_ids.size());
        st_kafka_batch_records_.fetch_add(batch_records, std::memory_order_relaxed);
        update_atomic_max(st_kafka_batch_records_max_, batch_records);
        
        uint64_t total_dwell_ns = 0;
        for (uint64_t append_ns : batch_append_ns) {
            if (flush_ns >= append_ns) {
                const uint64_t dwell_ns = flush_ns - append_ns;
                st_kafka_batch_dwell_ns_.fetch_add(dwell_ns, std::memory_order_relaxed);
                update_atomic_max(st_kafka_batch_dwell_max_ns_, dwell_ns);
                total_dwell_ns += dwell_ns;
            }
        }
        
        const uint64_t backlog = st_backlog_cur_.load(std::memory_order_relaxed);
        if (backlog == 0) st_kafka_flush_backlog_bin_0_.fetch_add(1, std::memory_order_relaxed);
        else if (backlog <= 15) st_kafka_flush_backlog_bin_1_15_.fetch_add(1, std::memory_order_relaxed);
        else if (backlog <= 63) st_kafka_flush_backlog_bin_16_63_.fetch_add(1, std::memory_order_relaxed);
        else if (backlog <= 255) st_kafka_flush_backlog_bin_64_255_.fetch_add(1, std::memory_order_relaxed);
        else st_kafka_flush_backlog_bin_256_plus_.fetch_add(1, std::memory_order_relaxed);

        const uint64_t inflight = st_inflight_cur_.load(std::memory_order_relaxed);
        if (inflight == 0) st_kafka_flush_inflight_bin_0_.fetch_add(1, std::memory_order_relaxed);
        else if (inflight <= 15) st_kafka_flush_inflight_bin_1_15_.fetch_add(1, std::memory_order_relaxed);
        else if (inflight <= 63) st_kafka_flush_inflight_bin_16_63_.fetch_add(1, std::memory_order_relaxed);
        else if (inflight <= 255) st_kafka_flush_inflight_bin_64_255_.fetch_add(1, std::memory_order_relaxed);
        else st_kafka_flush_inflight_bin_256_plus_.fetch_add(1, std::memory_order_relaxed);

        if (batch_records == 1) st_kafka_batch_records_bin_1_.fetch_add(1, std::memory_order_relaxed);
        else if (batch_records <= 15) st_kafka_batch_records_bin_2_15_.fetch_add(1, std::memory_order_relaxed);
        else if (batch_records <= 63) st_kafka_batch_records_bin_16_63_.fetch_add(1, std::memory_order_relaxed);
        else if (batch_records <= 255) st_kafka_batch_records_bin_64_255_.fetch_add(1, std::memory_order_relaxed);
        else st_kafka_batch_records_bin_256_plus_.fetch_add(1, std::memory_order_relaxed);

        if (batch_bytes <= 1024) st_kafka_batch_bytes_bin_le_1k_.fetch_add(1, std::memory_order_relaxed);
        else if (batch_bytes <= 10240) st_kafka_batch_bytes_bin_1k_10k_.fetch_add(1, std::memory_order_relaxed);
        else if (batch_bytes <= 102400) st_kafka_batch_bytes_bin_10k_100k_.fetch_add(1, std::memory_order_relaxed);
        else st_kafka_batch_bytes_bin_100k_plus_.fetch_add(1, std::memory_order_relaxed);

        uint64_t mean_dwell_ns = batch_records > 0 ? (total_dwell_ns / batch_records) : 0;
        if (mean_dwell_ns <= 1000000ULL) st_kafka_batch_dwell_bin_le_1ms_.fetch_add(1, std::memory_order_relaxed);
        else if (mean_dwell_ns <= 5000000ULL) st_kafka_batch_dwell_bin_1_5ms_.fetch_add(1, std::memory_order_relaxed);
        else if (mean_dwell_ns <= 20000000ULL) st_kafka_batch_dwell_bin_5_20ms_.fetch_add(1, std::memory_order_relaxed);
        else if (mean_dwell_ns <= 100000000ULL) st_kafka_batch_dwell_bin_20_100ms_.fetch_add(1, std::memory_order_relaxed);
        else st_kafka_batch_dwell_bin_100ms_plus_.fetch_add(1, std::memory_order_relaxed);
        st_result_flush_count_.fetch_add(1, std::memory_order_relaxed);
        st_result_flush_records_total_.fetch_add(batch_records, std::memory_order_relaxed);
        update_atomic_max(st_result_flush_records_max_, batch_records);

        switch (reason) {
            case FLUSH_REASON_RECORDS:
                st_kafka_flush_reason_records_.fetch_add(1, std::memory_order_relaxed);
                st_result_flush_due_to_record_cap_.fetch_add(1, std::memory_order_relaxed);
                break;
            case FLUSH_REASON_BYTES:
                st_kafka_flush_reason_bytes_.fetch_add(1, std::memory_order_relaxed);
                st_result_flush_due_to_byte_cap_.fetch_add(1, std::memory_order_relaxed);
                break;
            case FLUSH_REASON_AGE:
                st_kafka_flush_reason_age_.fetch_add(1, std::memory_order_relaxed);
                st_result_flush_due_to_age_.fetch_add(1, std::memory_order_relaxed);
                break;
            case FLUSH_REASON_IDLE:
                st_kafka_flush_reason_idle_.fetch_add(1, std::memory_order_relaxed);
                st_result_flush_due_to_idle_.fetch_add(1, std::memory_order_relaxed);
                break;
            case FLUSH_REASON_ERROR:
                st_result_flush_due_to_error_.fetch_add(1, std::memory_order_relaxed);
                break;
            case FLUSH_REASON_FINAL:
                st_kafka_flush_reason_final_.fetch_add(1, std::memory_order_relaxed);
                st_result_flush_due_to_shutdown_.fetch_add(1, std::memory_order_relaxed);
                break;
        }

        {
            const uint64_t dp = kafka_prod_.delivery_pending_relaxed();
            update_atomic_max(st_kafka_delivery_pending_max_, dp);
            if (dp > 8) {
                st_result_flush_while_delivery_pending_gt_8_.fetch_add(1, std::memory_order_relaxed);
            }
            if (dp > 32) {
                st_result_flush_while_delivery_pending_gt_32_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        std::string err;
        // True batching: build ONE multi-record payload for the whole batch,
        // send ONCE. The consumer (gateway) already decodes 'B3' payloads
        // with N>1 records via parse_kafka_payload_records.
        const auto b0 = std::chrono::steady_clock::now();
        const std::string payload = build_bin_batch_payload_v2(
            batch_req_ids,
            batch_results,
            batch_raft_log_idxs,
            batch_leader_hints,
            batch_terminal_digests,
            batch_raft_item_ordinals,
            batch_terminal_states,
            batch_format_versions,
            static_cast<uint16_t>(node_id_),
            result_sig_key_,
            db_opt_.raft_epoch_hex,
            db_opt_.raft_apply_ledger_mode == "safe");
        const auto b1 = std::chrono::steady_clock::now();
        st_kafka_build_payload_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(b1 - b0).count()),
            std::memory_order_relaxed);
        st_kafka_payload_bytes_.fetch_add(static_cast<uint64_t>(payload.size()),
                                          std::memory_order_relaxed);

        const auto s0 = std::chrono::steady_clock::now();
        const bool safe_trace_on = safe_trace_enabled(db_opt_.raft_apply_ledger_mode);
        if (safe_trace_on) {
            trigger_safe_failpoint("ARIABC_FAILPOINT_AFTER_RESULT_RING_BEFORE_KAFKA_PUBLISH",
                                   node_id_,
                                   batch_raft_log_idxs.empty() ? 0 : batch_raft_log_idxs.front(),
                                   batch_raft_item_ordinals.empty() ? 0 : batch_raft_item_ordinals.front());
            std::cerr << "SAFE_KAFKA_PUBLISH_BEGIN"
                      << " records=" << batch_req_ids.size()
                      << " first_log=" << (batch_raft_log_idxs.empty() ? 0 : batch_raft_log_idxs.front())
                      << " first_ord=" << (batch_raft_item_ordinals.empty() ? 0 : batch_raft_item_ordinals.front())
                      << " bytes=" << payload.size()
                      << std::endl;
        }
        const bool kafka_send_ok = kafka_prod_.send_payload(payload, batch_req_ids.front(), err);
        if (!kafka_send_ok) {
            std::cerr << "Kafka send failed: " << err << std::endl;
        } else if (safe_trace_on) {
            std::cerr << "SAFE_KAFKA_PUBLISH_SENT"
                      << " records=" << batch_req_ids.size()
                      << " first_log=" << (batch_raft_log_idxs.empty() ? 0 : batch_raft_log_idxs.front())
                      << " first_ord=" << (batch_raft_item_ordinals.empty() ? 0 : batch_raft_item_ordinals.front())
                      << std::endl;
        }
        bool kafka_delivered_ok = kafka_send_ok;
        if (kafka_send_ok && db_opt_.raft_apply_ledger_mode == "safe") {
            kafka_delivered_ok = kafka_prod_.wait_for_delivery(5000, err);
            if (!kafka_delivered_ok) {
                std::cerr << "Kafka delivery confirmation failed: " << err << std::endl;
            } else if (safe_trace_on) {
                std::cerr << "SAFE_KAFKA_PUBLISH_DELIVERED"
                          << " records=" << batch_req_ids.size()
                          << " first_log=" << (batch_raft_log_idxs.empty() ? 0 : batch_raft_log_idxs.front())
                          << " first_ord=" << (batch_raft_item_ordinals.empty() ? 0 : batch_raft_item_ordinals.front())
                          << std::endl;
            }
        }
        if (kafka_delivered_ok && db_opt_.raft_apply_ledger_mode == "safe") {
            if (safe_trace_on) {
                trigger_safe_failpoint("ARIABC_FAILPOINT_AFTER_KAFKA_PUBLISH_BEFORE_APPLIED_MARK",
                                       node_id_,
                                       batch_raft_log_idxs.empty() ? 0 : batch_raft_log_idxs.front(),
                                       batch_raft_item_ordinals.empty() ? 0 : batch_raft_item_ordinals.front());
            }
            for (size_t i = 0; i < batch_raft_log_idxs.size(); ++i) {
                const uint64_t dispatch_seq =
                    (i < batch_dispatch_seqs.size()) ? batch_dispatch_seqs[i] : 0;
                const uint64_t ready_ns =
                    (i < batch_ready_ns.size()) ? batch_ready_ns[i] : 0;
                if (dispatch_seq == 0) {
                    notify_task_applied(batch_raft_log_idxs[i],
                                        batch_raft_item_ordinals[i]);
                } else {
                    mark_task_applied_ordered(dispatch_seq,
                                              batch_raft_log_idxs[i],
                                              batch_raft_item_ordinals[i],
                                              ready_ns);
                }
            }
        }
        const auto s1 = std::chrono::steady_clock::now();
        st_kafka_send_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count()),
            std::memory_order_relaxed);
        batch_req_ids.clear();
        batch_results.clear();
        batch_raft_log_idxs.clear();
        batch_leader_hints.clear();
        batch_terminal_digests.clear();
        batch_append_ns.clear();
        batch_raft_item_ordinals.clear();
        batch_terminal_states.clear();
        batch_format_versions.clear();
        batch_dispatch_seqs.clear();
        batch_ready_ns.clear();
        batch_bytes = 0;
        batch_start = std::chrono::steady_clock::now();
    };

    while (!stop_.load()) {
        task t;
        size_t q_depth_after_pop = 0;
        {
            std::unique_lock<std::mutex> lk(q_mu_);
            const auto w0 = std::chrono::steady_clock::now();
            q_cv_.wait(lk, [this] { return stop_.load() || !q_.empty(); });
            const auto w1 = std::chrono::steady_clock::now();
            st_q_wait_ns_.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count()),
                std::memory_order_relaxed);
            if (stop_.load()) break;
            const size_t q_depth_before_pop = q_.size();
            t = q_.front();
            q_.pop();
            q_depth_after_pop = q_.size();
            st_queue_depth_cur_.store(static_cast<uint64_t>(q_depth_after_pop), std::memory_order_relaxed);
            st_queue_depth_samples_.fetch_add(1, std::memory_order_relaxed);
            st_queue_depth_sum_.fetch_add(static_cast<uint64_t>(q_depth_after_pop), std::memory_order_relaxed);
            if (q_depth_after_pop <= 16) {
                st_queue_depth_bin_le_16_.fetch_add(1, std::memory_order_relaxed);
            } else if (q_depth_after_pop <= 64) {
                st_queue_depth_bin_17_64_.fetch_add(1, std::memory_order_relaxed);
            } else if (q_depth_after_pop <= 256) {
                st_queue_depth_bin_65_256_.fetch_add(1, std::memory_order_relaxed);
            } else {
                st_queue_depth_bin_gt_256_.fetch_add(1, std::memory_order_relaxed);
            }
            uint64_t cur_max = st_queue_depth_max_.load(std::memory_order_relaxed);
            while (q_depth_after_pop > cur_max &&
                   !st_queue_depth_max_.compare_exchange_weak(
                       cur_max,
                       static_cast<uint64_t>(q_depth_after_pop),
                       std::memory_order_relaxed,
                       std::memory_order_relaxed)) {
            }
            if (queue_overloaded_.load(std::memory_order_relaxed) &&
                q_depth_after_pop <= queue_low_wm_) {
                bool expected = true;
                if (queue_overloaded_.compare_exchange_strong(
                        expected, false, std::memory_order_release, std::memory_order_relaxed)) {
                    st_queue_overload_exit_.fetch_add(1, std::memory_order_relaxed);
                    admission_cv_.notify_all();
                }
            } else if (!queue_overloaded_.load(std::memory_order_relaxed) &&
                       q_depth_before_pop >
                           (queue_high_wm_ + ((db_opt_.db_type == 1)
                                                  ? 0
                                                  : std::max<size_t>(1, static_cast<size_t>(db_opt_.conn_pool_size))))) {
                bool expected = false;
                if (queue_overloaded_.compare_exchange_strong(
                        expected, true, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    st_queue_overload_enter_.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }

        st_dequeued_.fetch_add(1, std::memory_order_relaxed);
        t.dispatch_seq = next_dispatch_seq_.fetch_add(1, std::memory_order_relaxed);

        // Safe-mode flow trace: emit SAFE_WORKER_DEQUEUE for every item when
        // ledger mode is 'safe' or ARIABC_SAFE_TRACE=1 is set.
        if (db_opt_.raft_apply_ledger_mode == "safe" || ::getenv("ARIABC_SAFE_TRACE")) {
            const char* safe_trace_env = ::getenv("ARIABC_SAFE_TRACE");
            const bool safe_trace_on =
                (db_opt_.raft_apply_ledger_mode == "safe") ||
                (safe_trace_env && safe_trace_env[0] != '\0' &&
                 std::string(safe_trace_env) != "0");
            if (safe_trace_on) {
                std::cerr << "SAFE_WORKER_DEQUEUE"
                          << " tx=" << t.req_id
                          << " block=" << t.raft_log_idx
                          << " log=" << t.raft_log_idx
                          << " ord=" << t.raft_item_ordinal
                          << std::endl;
            }
        }

        const bool det_unprefixed_det_sql =
            det_parallel_workers_ &&
            (db_opt_.db_type == 1) && !is_det_prefixed_sql(t.sql);
        if (det_unprefixed_det_sql && !det_allow_raw_compat_) {
            const std::string result = det_unprefixed_sql_error(t);
            if (!wait_for_ordered_emit_turn(t.dispatch_seq)) {
                break;
            }
            if (kafka_enabled_) {
                if (should_publish_kafka_result(node_id_)) {
                    if (db_opt_.raft_apply_ledger_mode != "safe") {
                        if (t.dispatch_seq == 0) {
                            notify_task_applied(t.raft_log_idx, t.raft_item_ordinal);
                        } else {
                            mark_task_applied_ordered(t.dispatch_seq,
                                                      t.raft_log_idx,
                                                      t.raft_item_ordinal,
                                                      now_steady_ns());
                        }
                    }
                    if (async_kafka_publisher_active()) {
                        ConfirmedResult conf;
                        conf.payload = result;
                        conf.terminal_state = "ERROR";
                        conf.format_version = 1;
                        enqueue_kafka_result(t, conf);
                    } else {
                        debug_trace_exec(t.req_id, t.raft_log_idx, result);
                        if (batch_req_ids.empty()) {
                            batch_start = std::chrono::steady_clock::now();
                        }
                        batch_req_ids.push_back(t.req_id);
                        batch_results.push_back(result);
                        batch_raft_log_idxs.push_back(t.raft_log_idx);
                        batch_leader_hints.push_back(t.leader_node_hint);
                        batch_terminal_digests.push_back(""); // non-det loop doesn't have terminal digest yet
                        batch_append_ns.push_back(now_steady_ns());
                        batch_raft_item_ordinals.push_back(t.raft_item_ordinal);
                        batch_terminal_states.push_back("ERROR");
                        batch_format_versions.push_back(1);
                        batch_bytes += t.req_id.size() + result.size() + 32;
                    }
                }
            } else {
                std::cout << (t.req_id + "  " + std::to_string(node_id_) + "  " + result)
                          << std::endl;
            }
            finish_ordered_emit(t.dispatch_seq);
            continue;
        }

        const bool det_raw_compat = det_unprefixed_det_sql && det_allow_raw_compat_;
        if (det_raw_compat) {
            note_det_raw_compat_activation(t);
        }

        bool det_raw_compat_serial_turn = false;
        if (det_raw_compat) {
            // Raw deterministic compatibility mode must execute strictly in
            // dispatch order. Waiting for the ordered turn *before* execution
            // prevents out-of-order apply and avoids slot+emit deadlocks.
            if (!wait_for_ordered_emit_turn(t.dispatch_seq)) {
                break;
            }
            det_raw_compat_serial_turn = true;
        }

        const bool det_prefixed_parallel =
            det_parallel_workers_ &&
            (db_opt_.db_type == 1) && is_det_prefixed_sql(t.sql);
        const bool det_prefixed_requires_apply_turn =
            det_prefixed_parallel && !det_threaded_direct_no_preapply_wait_;
        uint64_t det_tx_seq = 0;
        if (det_prefixed_requires_apply_turn) {
            det_tx_seq = get_det_tx_seq(t);
            det_mark_tx_state(det_tx_seq, det_tx_state::QUEUED);

            // Phase 2 Stage A (simulation): parse deterministic envelope.
            uint64_t det_sql_seq = 0;
            std::string det_raw_sql;
            (void)parse_det_prefixed_sql_parts(t.sql, &det_sql_seq, &det_raw_sql);
            det_mark_tx_state(det_tx_seq, det_tx_state::SIM_DONE);

            if (!det_wait_for_apply_turn(det_tx_seq)) {
                break;
            }
            det_mark_tx_state(det_tx_seq, det_tx_state::APPLY_READY);
        }

        if (t.enqueue_ns != 0) {
            const auto now = std::chrono::steady_clock::now().time_since_epoch();
            const uint64_t now_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
            if (now_ns >= t.enqueue_ns) {
                const uint64_t delay_ns = now_ns - t.enqueue_ns;
                st_queue_delay_ns_.fetch_add(delay_ns, std::memory_order_relaxed);
                st_queue_delay_dequeue_ns_.fetch_add(delay_ns, std::memory_order_relaxed);
            }
        }

        PGconn* c = acquire_conn();
        const std::string sql_exec =
            maybe_strip_det_prefix_for_compat(t.sql, det_raw_compat_mode_, db_opt_.db_type);
        bool is_error = false;
        const std::string result = exec_sql(c, sql_exec, &is_error);
        static const bool s_trace_backend_reply = (::getenv("ARIABC_TRACE_BACKEND_REPLY") != nullptr);
        if (s_trace_backend_reply) {
            std::cerr << "SAFE_BACKEND_REPLY"
                      << " log=" << t.raft_log_idx
                      << " ord=" << t.raft_item_ordinal
                      << " bytes=" << result.size()
                      << std::endl;
        }
        if (det_prefixed_requires_apply_turn) {
            det_finish_apply(det_tx_seq);
        }
        ConfirmedResult confirmed = accept_safe_confirmed_result(t, result);
        // Fail closed: do not publish Kafka and do not advance Raft applied tracker
        if (confirmed.raft_log_index == static_cast<uint64_t>(-1)) {
            std::cerr << "SAFE_PROTOCOL_FAILURE_NOT_APPLIED"
                      << " log=" << t.raft_log_idx
                      << " ord=" << t.raft_item_ordinal
                      << std::endl;
            notify_task_failed(t.raft_log_idx,
                               t.raft_item_ordinal,
                               "safe_protocol_failure_retryable");
            finish_ordered_emit(t.dispatch_seq);
            release_conn(c);
            continue;
        }
        // Safe-mode flow trace: SAFE_WORKER_LEDGER_BEGIN before durability check.
        {
            const char* safe_trace_env = ::getenv("ARIABC_SAFE_TRACE");
            const bool safe_trace_on =
                (db_opt_.raft_apply_ledger_mode == "safe") ||
                (safe_trace_env && safe_trace_env[0] != '\0' &&
                 std::string(safe_trace_env) != "0");
            if (safe_trace_on) {
                std::cerr << "SAFE_WORKER_LEDGER_BEGIN"
                          << " tx=" << t.req_id
                          << " log=" << t.raft_log_idx
                          << " ord=" << t.raft_item_ordinal
                          << std::endl;
            }
        }
        const bool durable_ok = ensure_safe_outcome(c, t, confirmed);
        probe_safe_ledger_outcome_visibility(c,
                                             conninfo_,
                                             db_opt_.raft_epoch_hex,
                                             t,
                                             confirmed);
        if (!durable_ok) {
            notify_task_failed(t.raft_log_idx,
                               t.raft_item_ordinal,
                               "safe_ledger_terminal_not_durable");
            finish_ordered_emit(t.dispatch_seq);
            release_conn(c);
            continue;
        }
        // Safe-mode flow trace: SAFE_WORKER_TOPLEVEL_COMMIT after durability confirmed.
        {
            const char* safe_trace_env = ::getenv("ARIABC_SAFE_TRACE");
            const bool safe_trace_on =
                (db_opt_.raft_apply_ledger_mode == "safe") ||
                (safe_trace_env && safe_trace_env[0] != '\0' &&
                 std::string(safe_trace_env) != "0");
            if (safe_trace_on) {
                std::cerr << "SAFE_WORKER_TOPLEVEL_COMMIT"
                          << " tx=" << t.req_id
                          << " log=" << t.raft_log_idx
                          << " ord=" << t.raft_item_ordinal
                          << std::endl;
            }
        }
        release_conn(c);
        if (!det_raw_compat_serial_turn) {
            if (!wait_for_ordered_emit_turn(t.dispatch_seq)) {
                break;
            }
        }

        /*
         * Non-safe threaded direct completion waits for the top-level
         * PostgreSQL commit, not for Kafka producer batching. Safe mode keeps
         * the stronger existing order in flush_batch(): publish first, then
         * mark the Raft item applied.
         */
        if (kafka_enabled_ &&
            should_publish_kafka_result(node_id_) &&
            db_opt_.raft_apply_ledger_mode != "safe") {
            if (t.dispatch_seq == 0) {
                notify_task_applied(t.raft_log_idx, t.raft_item_ordinal);
            } else {
                mark_task_applied_ordered(t.dispatch_seq,
                                          t.raft_log_idx,
                                          t.raft_item_ordinal,
                                          now_steady_ns());
            }
        }

        if (kafka_enabled_) {
            if (should_publish_kafka_result(node_id_)) {
                if (async_kafka_publisher_active()) {
                    enqueue_kafka_result(t, confirmed);
                } else {
                    debug_trace_exec(t.req_id, t.raft_log_idx, confirmed.payload);
                    if (batch_req_ids.empty()) {
                        batch_start = std::chrono::steady_clock::now();
                    }
                    batch_req_ids.push_back(t.req_id);
                    batch_results.push_back(confirmed.payload);
                    batch_raft_log_idxs.push_back(t.raft_log_idx);
                    batch_leader_hints.push_back(t.leader_node_hint);
                    batch_terminal_digests.push_back(confirmed.terminal_digest);
                    batch_append_ns.push_back(now_steady_ns());
                    batch_raft_item_ordinals.push_back(t.raft_item_ordinal);
                    batch_terminal_states.push_back(confirmed.terminal_state);
                    batch_format_versions.push_back(confirmed.format_version);
                    batch_dispatch_seqs.push_back(t.dispatch_seq);
                    batch_ready_ns.push_back(now_steady_ns());
                    batch_bytes += t.req_id.size() + confirmed.payload.size() + 32;
                    if (confirmed.terminal_state == "ERROR") {
                        flush_batch(FLUSH_REASON_ERROR);
                    } else {
                        const size_t max_records = (q_depth_after_pop >= 64) ? 512 : kKafkaBatchMaxRecords;
                        const size_t max_bytes = (q_depth_after_pop >= 64) ? (1024 * 1024) : kKafkaBatchMaxBytes;
                        int max_delay_us = -1;
                        if (kConfiguredDelayUs >= 0) {
                            max_delay_us = kConfiguredDelayUs;
                        } else if (q_depth_after_pop < 16) {
                            max_delay_us = kKafkaBatchMaxDelayMs * 1000;
                        }
                        const auto age_us = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - batch_start).count();
                        if (batch_req_ids.size() >= max_records ||
                            batch_bytes >= max_bytes ||
                            (max_delay_us >= 0 && age_us >= max_delay_us)) {
                            flush_batch(batch_req_ids.size() >= max_records
                                            ? FLUSH_REASON_RECORDS
                                            : (batch_bytes >= max_bytes
                                                   ? FLUSH_REASON_BYTES
                                                   : FLUSH_REASON_AGE));
                        }
                    }
                }
            }
        } else {
            const std::string msg =
                t.req_id + "  " + std::to_string(node_id_) + "  " + result;
            std::cout << msg << std::endl;
            if (t.dispatch_seq == 0) {
                notify_task_applied(t.raft_log_idx, t.raft_item_ordinal);
            } else {
                mark_task_applied_ordered(t.dispatch_seq,
                                          t.raft_log_idx,
                                          t.raft_item_ordinal,
                                          now_steady_ns());
            }
        }
        if (kafka_enabled_ && !should_publish_kafka_result(node_id_)) {
            if (t.dispatch_seq == 0) {
                notify_task_applied(t.raft_log_idx, t.raft_item_ordinal);
            } else {
                mark_task_applied_ordered(t.dispatch_seq,
                                          t.raft_log_idx,
                                          t.raft_item_ordinal,
                                          now_steady_ns());
            }
        }
        finish_ordered_emit(t.dispatch_seq);

        {
            std::lock_guard<std::mutex> lk(q_mu_);
            if (q_.empty()) {
                flush_batch(FLUSH_REASON_IDLE);
            }
        }
    }

    flush_batch(FLUSH_REASON_FINAL);
}

void pg_executor::event_loop() {
    std::vector<std::string> batch_req_ids;
    std::vector<std::string> batch_results;
    std::vector<uint64_t> batch_raft_log_idxs;
    std::vector<int> batch_leader_hints;
    std::vector<std::string> batch_terminal_digests;
    std::vector<uint64_t> batch_append_ns;
    std::vector<uint32_t> batch_raft_item_ordinals;
    std::vector<std::string> batch_terminal_states;
    std::vector<int> batch_format_versions;
    std::vector<uint64_t> batch_dispatch_seqs;
    std::vector<uint64_t> batch_ready_ns;
    size_t batch_bytes = 0;
    auto batch_start = std::chrono::steady_clock::now();

    enum event_flush_reason {
        FLUSH_REASON_RECORDS,
        FLUSH_REASON_BYTES,
        FLUSH_REASON_AGE,
        FLUSH_REASON_IDLE,
        FLUSH_REASON_ERROR,
        FLUSH_REASON_FINAL
    };

    auto update_atomic_max = [](std::atomic<uint64_t>& target, uint64_t value) {
        uint64_t cur = target.load(std::memory_order_relaxed);
        while (value > cur &&
               !target.compare_exchange_weak(cur,
                                             value,
                                             std::memory_order_relaxed,
                                             std::memory_order_relaxed)) {
        }
    };

    auto flush_batch = [&](event_flush_reason reason = FLUSH_REASON_FINAL) {
        if (!kafka_enabled_ || batch_req_ids.empty()) {
            batch_req_ids.clear();
            batch_results.clear();
            batch_raft_log_idxs.clear();
            batch_leader_hints.clear();
            batch_terminal_digests.clear();
            batch_append_ns.clear();
            batch_raft_item_ordinals.clear();
            batch_terminal_states.clear();
            batch_format_versions.clear();
            batch_dispatch_seqs.clear();
            batch_ready_ns.clear();
            batch_bytes = 0;
            batch_start = std::chrono::steady_clock::now();
            return;
        }

        st_kafka_flush_calls_.fetch_add(1, std::memory_order_relaxed);
        const uint64_t flush_ns = now_steady_ns();
        const uint64_t batch_records = static_cast<uint64_t>(batch_req_ids.size());
        st_kafka_batch_records_.fetch_add(batch_records, std::memory_order_relaxed);
        update_atomic_max(st_kafka_batch_records_max_, batch_records);
        
        uint64_t total_dwell_ns = 0;
        for (uint64_t append_ns : batch_append_ns) {
            if (flush_ns >= append_ns) {
                const uint64_t dwell_ns = flush_ns - append_ns;
                st_kafka_batch_dwell_ns_.fetch_add(dwell_ns, std::memory_order_relaxed);
                update_atomic_max(st_kafka_batch_dwell_max_ns_, dwell_ns);
                total_dwell_ns += dwell_ns;
            }
        }

        const uint64_t backlog = st_backlog_cur_.load(std::memory_order_relaxed);
        if (backlog == 0) st_kafka_flush_backlog_bin_0_.fetch_add(1, std::memory_order_relaxed);
        else if (backlog <= 15) st_kafka_flush_backlog_bin_1_15_.fetch_add(1, std::memory_order_relaxed);
        else if (backlog <= 63) st_kafka_flush_backlog_bin_16_63_.fetch_add(1, std::memory_order_relaxed);
        else if (backlog <= 255) st_kafka_flush_backlog_bin_64_255_.fetch_add(1, std::memory_order_relaxed);
        else st_kafka_flush_backlog_bin_256_plus_.fetch_add(1, std::memory_order_relaxed);

        const uint64_t inflight = st_inflight_cur_.load(std::memory_order_relaxed);
        if (inflight == 0) st_kafka_flush_inflight_bin_0_.fetch_add(1, std::memory_order_relaxed);
        else if (inflight <= 15) st_kafka_flush_inflight_bin_1_15_.fetch_add(1, std::memory_order_relaxed);
        else if (inflight <= 63) st_kafka_flush_inflight_bin_16_63_.fetch_add(1, std::memory_order_relaxed);
        else if (inflight <= 255) st_kafka_flush_inflight_bin_64_255_.fetch_add(1, std::memory_order_relaxed);
        else st_kafka_flush_inflight_bin_256_plus_.fetch_add(1, std::memory_order_relaxed);

        if (batch_records == 1) st_kafka_batch_records_bin_1_.fetch_add(1, std::memory_order_relaxed);
        else if (batch_records <= 15) st_kafka_batch_records_bin_2_15_.fetch_add(1, std::memory_order_relaxed);
        else if (batch_records <= 63) st_kafka_batch_records_bin_16_63_.fetch_add(1, std::memory_order_relaxed);
        else if (batch_records <= 255) st_kafka_batch_records_bin_64_255_.fetch_add(1, std::memory_order_relaxed);
        else st_kafka_batch_records_bin_256_plus_.fetch_add(1, std::memory_order_relaxed);

        if (batch_bytes <= 1024) st_kafka_batch_bytes_bin_le_1k_.fetch_add(1, std::memory_order_relaxed);
        else if (batch_bytes <= 10240) st_kafka_batch_bytes_bin_1k_10k_.fetch_add(1, std::memory_order_relaxed);
        else if (batch_bytes <= 102400) st_kafka_batch_bytes_bin_10k_100k_.fetch_add(1, std::memory_order_relaxed);
        else st_kafka_batch_bytes_bin_100k_plus_.fetch_add(1, std::memory_order_relaxed);

        uint64_t mean_dwell_ns = batch_records > 0 ? (total_dwell_ns / batch_records) : 0;
        if (mean_dwell_ns <= 1000000ULL) st_kafka_batch_dwell_bin_le_1ms_.fetch_add(1, std::memory_order_relaxed);
        else if (mean_dwell_ns <= 5000000ULL) st_kafka_batch_dwell_bin_1_5ms_.fetch_add(1, std::memory_order_relaxed);
        else if (mean_dwell_ns <= 20000000ULL) st_kafka_batch_dwell_bin_5_20ms_.fetch_add(1, std::memory_order_relaxed);
        else if (mean_dwell_ns <= 100000000ULL) st_kafka_batch_dwell_bin_20_100ms_.fetch_add(1, std::memory_order_relaxed);
        else st_kafka_batch_dwell_bin_100ms_plus_.fetch_add(1, std::memory_order_relaxed);
        st_result_flush_count_.fetch_add(1, std::memory_order_relaxed);
        st_result_flush_records_total_.fetch_add(batch_records, std::memory_order_relaxed);
        update_atomic_max(st_result_flush_records_max_, batch_records);

        switch (reason) {
            case FLUSH_REASON_RECORDS:
                st_kafka_flush_reason_records_.fetch_add(1, std::memory_order_relaxed);
                st_result_flush_due_to_record_cap_.fetch_add(1, std::memory_order_relaxed);
                break;
            case FLUSH_REASON_BYTES:
                st_kafka_flush_reason_bytes_.fetch_add(1, std::memory_order_relaxed);
                st_result_flush_due_to_byte_cap_.fetch_add(1, std::memory_order_relaxed);
                break;
            case FLUSH_REASON_AGE:
                st_kafka_flush_reason_age_.fetch_add(1, std::memory_order_relaxed);
                st_result_flush_due_to_age_.fetch_add(1, std::memory_order_relaxed);
                break;
            case FLUSH_REASON_IDLE:
                st_kafka_flush_reason_idle_.fetch_add(1, std::memory_order_relaxed);
                st_result_flush_due_to_idle_.fetch_add(1, std::memory_order_relaxed);
                break;
            case FLUSH_REASON_ERROR:
                st_result_flush_due_to_error_.fetch_add(1, std::memory_order_relaxed);
                break;
            case FLUSH_REASON_FINAL:
                st_kafka_flush_reason_final_.fetch_add(1, std::memory_order_relaxed);
                st_result_flush_due_to_shutdown_.fetch_add(1, std::memory_order_relaxed);
                break;
        }

        {
            const uint64_t dp = kafka_prod_.delivery_pending_relaxed();
            update_atomic_max(st_kafka_delivery_pending_max_, dp);
            if (dp > 8) {
                st_result_flush_while_delivery_pending_gt_8_.fetch_add(1, std::memory_order_relaxed);
            }
            if (dp > 32) {
                st_result_flush_while_delivery_pending_gt_32_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        std::string err;
        // True batching: build ONE multi-record payload, send ONCE.
        const auto b0 = std::chrono::steady_clock::now();
        const std::string payload = build_bin_batch_payload_v2(
            batch_req_ids,
            batch_results,
            batch_raft_log_idxs,
            batch_leader_hints,
            batch_terminal_digests,
            batch_raft_item_ordinals,
            batch_terminal_states,
            batch_format_versions,
            static_cast<uint16_t>(node_id_),
            result_sig_key_,
            db_opt_.raft_epoch_hex,
            db_opt_.raft_apply_ledger_mode == "safe");
        const auto b1 = std::chrono::steady_clock::now();
        st_kafka_build_payload_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(b1 - b0).count()),
            std::memory_order_relaxed);
        st_kafka_payload_bytes_.fetch_add(static_cast<uint64_t>(payload.size()),
                                          std::memory_order_relaxed);

        const auto s0 = std::chrono::steady_clock::now();
        const bool safe_trace_on = safe_trace_enabled(db_opt_.raft_apply_ledger_mode);
        if (safe_trace_on) {
            trigger_safe_failpoint("ARIABC_FAILPOINT_AFTER_RESULT_RING_BEFORE_KAFKA_PUBLISH",
                                   node_id_,
                                   batch_raft_log_idxs.empty() ? 0 : batch_raft_log_idxs.front(),
                                   batch_raft_item_ordinals.empty() ? 0 : batch_raft_item_ordinals.front());
            std::cerr << "SAFE_KAFKA_PUBLISH_BEGIN"
                      << " records=" << batch_req_ids.size()
                      << " first_log=" << (batch_raft_log_idxs.empty() ? 0 : batch_raft_log_idxs.front())
                      << " first_ord=" << (batch_raft_item_ordinals.empty() ? 0 : batch_raft_item_ordinals.front())
                      << " bytes=" << payload.size()
                      << std::endl;
        }
        const bool kafka_send_ok = kafka_prod_.send_payload(payload, batch_req_ids.front(), err);
        if (!kafka_send_ok) {
            std::cerr << "Kafka send failed: " << err << std::endl;
        } else if (safe_trace_on) {
            std::cerr << "SAFE_KAFKA_PUBLISH_SENT"
                      << " records=" << batch_req_ids.size()
                      << " first_log=" << (batch_raft_log_idxs.empty() ? 0 : batch_raft_log_idxs.front())
                      << " first_ord=" << (batch_raft_item_ordinals.empty() ? 0 : batch_raft_item_ordinals.front())
                      << std::endl;
        }
        bool kafka_delivered_ok = kafka_send_ok;
        if (kafka_send_ok && db_opt_.raft_apply_ledger_mode == "safe") {
            kafka_delivered_ok = kafka_prod_.wait_for_delivery(5000, err);
            if (!kafka_delivered_ok) {
                std::cerr << "Kafka delivery confirmation failed: " << err << std::endl;
            } else if (safe_trace_on) {
                std::cerr << "SAFE_KAFKA_PUBLISH_DELIVERED"
                          << " records=" << batch_req_ids.size()
                          << " first_log=" << (batch_raft_log_idxs.empty() ? 0 : batch_raft_log_idxs.front())
                          << " first_ord=" << (batch_raft_item_ordinals.empty() ? 0 : batch_raft_item_ordinals.front())
                          << std::endl;
            }
        }
        if (kafka_delivered_ok) {
            if (safe_trace_on) {
                trigger_safe_failpoint("ARIABC_FAILPOINT_AFTER_KAFKA_PUBLISH_BEFORE_APPLIED_MARK",
                                       node_id_,
                                       batch_raft_log_idxs.empty() ? 0 : batch_raft_log_idxs.front(),
                                       batch_raft_item_ordinals.empty() ? 0 : batch_raft_item_ordinals.front());
            }
            for (size_t i = 0; i < batch_raft_log_idxs.size(); ++i) {
                const uint64_t dispatch_seq =
                    (i < batch_dispatch_seqs.size()) ? batch_dispatch_seqs[i] : 0;
                const uint64_t ready_ns =
                    (i < batch_ready_ns.size()) ? batch_ready_ns[i] : 0;
                if (dispatch_seq == 0) {
                    notify_task_applied(batch_raft_log_idxs[i],
                                        batch_raft_item_ordinals[i]);
                } else {
                    mark_task_applied_ordered(dispatch_seq,
                                              batch_raft_log_idxs[i],
                                              batch_raft_item_ordinals[i],
                                              ready_ns);
                }
            }
        }
        const auto s1 = std::chrono::steady_clock::now();
        st_kafka_send_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count()),
            std::memory_order_relaxed);
        batch_req_ids.clear();
        batch_results.clear();
        batch_raft_log_idxs.clear();
        batch_leader_hints.clear();
        batch_terminal_digests.clear();
        batch_append_ns.clear();
        batch_raft_item_ordinals.clear();
        batch_terminal_states.clear();
        batch_format_versions.clear();
        batch_dispatch_seqs.clear();
        batch_ready_ns.clear();
        batch_bytes = 0;
        batch_start = std::chrono::steady_clock::now();
    };

    struct ready_det_block {
        std::vector<task> tasks;
        std::vector<std::string> results;
        std::vector<bool> errors;
        uint64_t ready_ns = 0;
    };
    std::map<uint64_t, ready_det_block> ready_det_blocks;
    uint64_t next_det_block_submit_seq = 0;
    uint64_t next_det_block_emit_seq = 0;
    uint64_t last_inflight_sample_ns = now_steady_ns();
    size_t last_inflight_level = 0;

    auto emit_det_result = [&](const task& done_task, const std::string& out, bool is_error = false, const std::string& terminal_digest = "", const std::string& terminal_state = "", int format_version = 1) {
        if (kafka_enabled_) {
            if (should_publish_kafka_result(node_id_)) {
                if (async_kafka_publisher_active()) {
                    ConfirmedResult conf;
                    conf.payload = out;
                    conf.terminal_digest = terminal_digest;
                    conf.terminal_state =
                        terminal_state.empty() ? (is_error ? "ERROR" : "OK") : terminal_state;
                    conf.format_version = format_version;
                    enqueue_kafka_result(done_task, conf);
                    if (done_task.dispatch_seq == 0) {
                        notify_task_applied(done_task.raft_log_idx,
                                            done_task.raft_item_ordinal);
                    } else {
                        mark_task_applied_ordered(done_task.dispatch_seq,
                                                  done_task.raft_log_idx,
                                                  done_task.raft_item_ordinal,
                                                  now_steady_ns());
                    }
                } else {
                    debug_trace_exec(done_task.req_id, done_task.raft_log_idx, out);
                    if (batch_req_ids.empty()) batch_start = std::chrono::steady_clock::now();
                    batch_req_ids.push_back(done_task.req_id);
                    batch_results.push_back(out);
                    batch_raft_log_idxs.push_back(done_task.raft_log_idx);
                    batch_leader_hints.push_back(done_task.leader_node_hint);
                    batch_terminal_digests.push_back(terminal_digest);
                    batch_append_ns.push_back(now_steady_ns());
                    batch_raft_item_ordinals.push_back(done_task.raft_item_ordinal);
                    batch_terminal_states.push_back(terminal_state.empty() ? (is_error ? "ERROR" : "OK") : terminal_state);
                    batch_format_versions.push_back(format_version);
                    batch_dispatch_seqs.push_back(done_task.dispatch_seq);
                    batch_ready_ns.push_back(now_steady_ns());
                    batch_bytes += done_task.req_id.size() + out.size() + 32;
                    st_kafka_immediate_records_.fetch_add(1, std::memory_order_relaxed);
                    if (is_error) {
                        flush_batch(FLUSH_REASON_ERROR);
                    }
                }
            }
        } else {
            std::cout << (done_task.req_id + "  " + std::to_string(node_id_) + "  " + out)
                      << std::endl;
            if (done_task.dispatch_seq == 0) {
                notify_task_applied(done_task.raft_log_idx, done_task.raft_item_ordinal);
            } else {
                mark_task_applied_ordered(done_task.dispatch_seq,
                                          done_task.raft_log_idx,
                                          done_task.raft_item_ordinal,
                                          now_steady_ns());
            }
        }
        if (kafka_enabled_ && !should_publish_kafka_result(node_id_)) {
            if (done_task.dispatch_seq == 0) {
                notify_task_applied(done_task.raft_log_idx, done_task.raft_item_ordinal);
            } else {
                mark_task_applied_ordered(done_task.dispatch_seq,
                                          done_task.raft_log_idx,
                                          done_task.raft_item_ordinal,
                                          now_steady_ns());
            }
        }
    };

    auto drain_ready_det_blocks = [&]() {
        for (;;) {
            auto it = ready_det_blocks.find(next_det_block_emit_seq);
            if (it == ready_det_blocks.end()) break;

            ready_det_block ready = std::move(it->second);
            ready_det_blocks.erase(it);
            if (ready.ready_ns != 0) {
                const uint64_t now_ns = now_steady_ns();
                if (now_ns >= ready.ready_ns) {
                    st_ordered_emit_wait_ns_.fetch_add(now_ns - ready.ready_ns,
                                                       std::memory_order_relaxed);
                }
            }
            const size_t n = std::min(ready.tasks.size(), ready.results.size());
            for (size_t i = 0; i < n; ++i) {
                const bool is_err = (i < ready.errors.size()) ? ready.errors[i] : false;

                static const bool s_trace_backend_reply = (::getenv("ARIABC_TRACE_BACKEND_REPLY") != nullptr);
                if (s_trace_backend_reply) {
                    std::cerr << "SAFE_BACKEND_REPLY"
                              << " log=" << ready.tasks[i].raft_log_idx
                              << " ord=" << ready.tasks[i].raft_item_ordinal
                              << " bytes=" << ready.results[i].size()
                              << std::endl;
                }
                ConfirmedResult conf = accept_safe_confirmed_result(ready.tasks[i], ready.results[i]);
                // Fail closed: do not publish Kafka and do not advance Raft applied tracker
                if (conf.raft_log_index == static_cast<uint64_t>(-1)) {
                    std::cerr << "SAFE_PROTOCOL_FAILURE_NOT_APPLIED"
                              << " log=" << ready.tasks[i].raft_log_idx
                              << " ord=" << ready.tasks[i].raft_item_ordinal
                              << " outcome=safe_protocol_failure_retryable"
                              << std::endl;
                    notify_task_failed(ready.tasks[i].raft_log_idx,
                                       ready.tasks[i].raft_item_ordinal,
                                       "safe_protocol_failure_retryable");
                    continue;
                }
                emit_det_result(
                    ready.tasks[i],
                    conf.payload,
                    conf.terminal_state == "ERROR",
                    conf.terminal_digest,
                    conf.terminal_state,
                    conf.format_version
                );
                if (db_opt_.raft_apply_ledger_mode == "safe" &&
                    kafka_enabled_ &&
                    should_publish_kafka_result(node_id_) &&
                    !batch_req_ids.empty()) {
                    flush_batch(FLUSH_REASON_RECORDS);
                }
            }
            if (kafka_enabled_) {
                int max_delay_us = -1;
                if (kConfiguredDelayUs >= 0) {
                    max_delay_us = kConfiguredDelayUs;
                } else {
                    // No backlog context here directly, default to 1ms
                    max_delay_us = kKafkaBatchMaxDelayMs * 1000;
                }
                const auto age_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - batch_start).count();
                if (batch_req_ids.size() >= kKafkaBatchMaxRecords ||
                    batch_bytes >= kKafkaBatchMaxBytes ||
                    (max_delay_us >= 0 && age_us >= max_delay_us)) {
                    flush_batch(batch_req_ids.size() >= kKafkaBatchMaxRecords
                                    ? FLUSH_REASON_RECORDS
                                    : (batch_bytes >= kKafkaBatchMaxBytes
                                           ? FLUSH_REASON_BYTES
                                           : FLUSH_REASON_AGE));
                }
            }
            ++next_det_block_emit_seq;
            /* Fastpath visibility: block emitted from server. */
            st_fastpath_blocks_emitted_.fetch_add(1, std::memory_order_relaxed);
            st_fastpath_last_emitted_seq_.store(next_det_block_emit_seq - 1,
                                                std::memory_order_relaxed);
            {
                const uint64_t ready_depth = static_cast<uint64_t>(ready_det_blocks.size());
                uint64_t cur_max = st_fastpath_ready_blocks_max_.load(std::memory_order_relaxed);
                while (ready_depth > cur_max &&
                       !st_fastpath_ready_blocks_max_.compare_exchange_weak(
                           cur_max, ready_depth, std::memory_order_relaxed));
            }
        }
    };

    auto mark_det_result_ready = [&](PGconn* durable_conn, task done_task, const std::string& out, bool is_error = false) {
        const uint64_t ready_ns = now_steady_ns();
        static const bool s_trace_backend_reply = (::getenv("ARIABC_TRACE_BACKEND_REPLY") != nullptr);
        if (s_trace_backend_reply) {
            std::cerr << "SAFE_BACKEND_REPLY"
                      << " log=" << done_task.raft_log_idx
                      << " ord=" << done_task.raft_item_ordinal
                      << " bytes=" << out.size()
                      << std::endl;
        }
        ConfirmedResult conf = accept_safe_confirmed_result(done_task, out);
        // Fail closed: do not publish Kafka and do not advance Raft applied tracker
        if (conf.raft_log_index == static_cast<uint64_t>(-1)) {
            std::cerr << "SAFE_PROTOCOL_FAILURE_NOT_APPLIED"
                      << " log=" << done_task.raft_log_idx
                      << " ord=" << done_task.raft_item_ordinal
                      << " outcome=safe_protocol_failure_retryable"
                      << std::endl;
            notify_task_failed(done_task.raft_log_idx,
                               done_task.raft_item_ordinal,
                               "safe_protocol_failure_retryable");
            return;
        }
        const bool durable_ok = ensure_safe_outcome(durable_conn, done_task, conf);
        probe_safe_ledger_outcome_visibility(durable_conn,
                                             conninfo_,
                                             db_opt_.raft_epoch_hex,
                                             done_task,
                                             conf);
        if (!durable_ok) {
            notify_task_failed(done_task.raft_log_idx,
                               done_task.raft_item_ordinal,
                               "safe_ledger_terminal_not_durable");
            return;
        }
        emit_det_result(
            done_task,
            conf.payload,
            conf.terminal_state == "ERROR",
            conf.terminal_digest,
            conf.terminal_state,
            conf.format_version
        );
        if (db_opt_.raft_apply_ledger_mode == "safe" &&
            kafka_enabled_ &&
            should_publish_kafka_result(node_id_) &&
            !batch_req_ids.empty()) {
            flush_batch(FLUSH_REASON_RECORDS);
        }
    };

    auto update_overload_state = [&](size_t backlog, size_t queued) {
        st_backlog_cur_.store(static_cast<uint64_t>(backlog), std::memory_order_relaxed);
        // Allow dispatch jitter margin before entering overload in non-det
        // mode. Deterministic mode uses zero margin to react directly to
        // queued depth and avoid prolonged append stalls.
        const size_t enter_margin =
            (db_opt_.db_type == 1)
                ? 0
                : std::max<size_t>(1, static_cast<size_t>(db_opt_.conn_pool_size));
        const size_t enter_wm = queue_high_wm_ + enter_margin;
        if (queue_overloaded_.load(std::memory_order_relaxed)) {
            if (queued <= queue_low_wm_) {
                bool expected = true;
                if (queue_overloaded_.compare_exchange_strong(
                        expected, false, std::memory_order_release, std::memory_order_relaxed)) {
                    st_queue_overload_exit_.fetch_add(1, std::memory_order_relaxed);
                    admission_cv_.notify_all();
                }
            }
        } else {
            if (queued > enter_wm) {
                bool expected = false;
                if (queue_overloaded_.compare_exchange_strong(
                        expected, true, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    st_queue_overload_enter_.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    };

    while (!stop_.load()) {
        // Move due retries into the ready queue.
        const uint64_t now_ns = now_steady_ns();
        uint64_t next_det_partial_block_ready_ns = 0;
        {
            std::lock_guard<std::mutex> lk(q_mu_);
            while (!delayed_.empty() && delayed_.top().deadline_ns <= now_ns) {
                q_.push(delayed_.top().t);
                delayed_.pop();
            }
            if (!det_preassigned_reorder_buf_.empty()) {
                const uint64_t oldest_enq_ns = det_preassigned_reorder_buf_.begin()->second.enqueue_ns;
                if (oldest_enq_ns != 0 && now_ns >= oldest_enq_ns + 5000000ULL) {
                    next_det_preassigned_seq_ = det_preassigned_reorder_buf_.begin()->first;
                    while (!det_preassigned_reorder_buf_.empty() &&
                           det_preassigned_reorder_buf_.begin()->first == next_det_preassigned_seq_) {
                        push_task_ordered(std::move(det_preassigned_reorder_buf_.begin()->second));
                        det_preassigned_reorder_buf_.erase(det_preassigned_reorder_buf_.begin());
                        ++next_det_preassigned_seq_;
                    }
                }
            }
            st_delayed_cur_.store(static_cast<uint64_t>(delayed_.size()), std::memory_order_relaxed);
        }

        // Deterministic BCDB block fast path: submit ordered parser-mode
        // blocks on several PG connections, then emit completed block results
        // in submission order.  This preserves the global result stream while
        // allowing later blocks to parse/simulate while earlier blocks finish.
        if (!stop_.load() &&
            db_opt_.db_type == 1 &&
            !det_parallel_workers_ &&
            det_event_block_fastpath_enabled() &&
            bcdb_init_done_ &&
            !conns_.empty()) {
            const size_t fixed_chunk_size = []() -> size_t {
                const char* v = std::getenv("ARIABC_DET_BLOCK_CHUNK_SIZE");
                if (v && *v) {
                    size_t s = std::strtoul(v, nullptr, 10);
                    if (s > 0) return s;
                }
                const char* pol = std::getenv("ARIABC_RAFT_ORDERING_POLICY");
                if (!pol || !*pol || std::string(pol) == "preassigned") {
                    return 64; // Default fixed chunk size for preassigned determinism across thread counts
                }
                return 0;
            }();

            const size_t base_block_cap = bcdb_init_done_
                ? static_cast<size_t>(bcdb_block_size_)
                : std::max<size_t>(1, static_cast<size_t>(db_opt_.conn_pool_size));
            const size_t block_cap = (fixed_chunk_size > 0)
                ? std::min<size_t>(det_block_max_, fixed_chunk_size)
                : std::min<size_t>(det_block_max_,
                                   base_block_cap *
                                   static_cast<size_t>(std::max(1, det_block_pipeline_)));
            int det_blocks_inflight = 0;

            for (const auto& cs : conns_) {
                if (cs.has_det_block && cs.st != conn_state::state::IDLE) {
                    ++det_blocks_inflight;
                }
            }

            for (auto& cs : conns_) {
                if (det_blocks_inflight >= det_block_parallel_) break;
                if (block_cap == 0 || cs.st != conn_state::state::IDLE) continue;

                std::vector<task> det_batch;
                size_t depth_after_pop = 0;
                {
                    std::lock_guard<std::mutex> lk(q_mu_);
                    if (delayed_.empty() && !q_.empty() && is_det_prefixed_sql(q_.front().sql)) {
                        std::queue<task> restored;
                        std::vector<task> candidate;

                        uint64_t start_seq = 0;
                        bool has_start_seq = get_det_tx_seq_valid(q_.front(), start_seq);

                        size_t desired_block_size = block_cap;
                        if (has_start_seq && block_cap > 0) {
                            const size_t offset_in_block = static_cast<size_t>(start_seq % block_cap);
                            desired_block_size = block_cap - offset_in_block;
                        }
                        if (desired_block_size == 0) desired_block_size = block_cap;
                        desired_block_size = std::min<size_t>(block_cap, desired_block_size);

                        candidate.reserve(desired_block_size);
                        uint64_t expected_next_seq = start_seq;

                        while (!q_.empty() && candidate.size() < desired_block_size) {
                            const task& front_task = q_.front();
                            if (!is_det_prefixed_sql(front_task.sql)) {
                                break;
                            }
                            if (lower_copy(trim_copy(front_task.sql)).find("bcdb_reset(") != std::string::npos) {
                                if (candidate.empty()) {
                                    candidate.push_back(std::move(q_.front()));
                                    q_.pop();
                                }
                                break;
                            }

                            /*
                             * In Kafka-only/bypass mode raft_log_idx is 0, so
                             * each replica must keep the gateway batch boundary:
                             * merging adjacent local queue batches would let
                             * replica scheduling choose different BCDB block
                             * boundaries.  In raft-kafka mode every item has a
                             * non-zero Raft log index and the global req_id
                             * order below is authoritative, so a logical BCDB
                             * block may span several committed Raft batches.
                             */
                            if (!candidate.empty() &&
                                front_task.raft_log_idx != candidate.front().raft_log_idx &&
                                (candidate.front().raft_log_idx == 0 ||
                                 front_task.raft_log_idx == 0)) {
                                break;
                            }

                            uint64_t cur_seq = 0;
                            if (has_start_seq) {
                                if (!get_det_tx_seq_valid(front_task, cur_seq) || cur_seq != expected_next_seq) {
                                    break;
                                }
                                expected_next_seq = cur_seq + 1;
                            }

                            candidate.push_back(std::move(q_.front()));
                            q_.pop();
                        }

                        bool eligible = !candidate.empty();
                        const bool is_single_barrier =
                            (candidate.size() == 1 &&
                             lower_copy(trim_copy(candidate.front().sql)).find("bcdb_reset(") != std::string::npos);

                        uint64_t partial_wait_ns = det_partial_block_max_wait_ns();
                        if (state_machine_preassigned_ordering_enabled()) {
                            partial_wait_ns = 50ULL * 1000ULL * 1000ULL; // 50ms wait to ensure full 64-tx block alignment
                        } else if (partial_wait_ns == 0 && (fixed_chunk_size > 0)) {
                            partial_wait_ns = 2ULL * 1000ULL * 1000ULL; // 2ms default for aligned determinism
                        }

                        if (eligible && !is_single_barrier && candidate.size() < desired_block_size && partial_wait_ns > 0) {
                            const uint64_t oldest_enqueue_ns = candidate.front().enqueue_ns;
                            if (oldest_enqueue_ns != 0) {
                                const uint64_t ready_ns = oldest_enqueue_ns + partial_wait_ns;
                                if (now_ns < ready_ns) {
                                    if (next_det_partial_block_ready_ns == 0 ||
                                        ready_ns < next_det_partial_block_ready_ns) {
                                        next_det_partial_block_ready_ns = ready_ns;
                                    }
                                    eligible = false;
                                }
                            }
                        }

                        if (eligible) {
                            det_batch = std::move(candidate);
                            depth_after_pop = q_.size();
                            st_queue_depth_cur_.store(static_cast<uint64_t>(depth_after_pop),
                                                      std::memory_order_relaxed);
                            st_queue_depth_samples_.fetch_add(1, std::memory_order_relaxed);
                            st_queue_depth_sum_.fetch_add(static_cast<uint64_t>(depth_after_pop),
                                                          std::memory_order_relaxed);
                        } else {
                            for (auto& t : candidate) restored.push(std::move(t));
                            while (!q_.empty()) {
                                restored.push(std::move(q_.front()));
                                q_.pop();
                            }
                            q_.swap(restored);
                        }
                    }
                }
                if (det_batch.empty()) break;

                const uint64_t exec_begin_ns = now_steady_ns();
                std::vector<std::pair<std::string, std::string>> txs;
                std::vector<uint64_t> tx_det_seqs;
                std::vector<uint8_t> backend_mask;
                const uint64_t tx_key_start = det_block_tx_key_base_ + det_block_next_tx_key_;
                bool build_ok = true;
                uint64_t skipped_readonly = 0;
                txs.reserve(det_batch.size());
                tx_det_seqs.reserve(det_batch.size());
                backend_mask.reserve(det_batch.size());
                for (size_t i = 0; i < det_batch.size(); ++i) {
                    uint64_t seq = 0;
                    std::string raw_sql;
                    if (!parse_det_prefixed_sql_parts(det_batch[i].sql, &seq, &raw_sql)) {
                        build_ok = false;
                        break;
                    }
                    if (det_block_skip_readonly_enabled() && sql_is_plain_select(raw_sql)) {
                        backend_mask.push_back(0);
                        ++skipped_readonly;
                        continue;
                    }
                    backend_mask.push_back(1);
                    const std::string key = std::to_string(tx_key_start + static_cast<uint64_t>(i));
                    txs.emplace_back(key, std::move(raw_sql));
                    tx_det_seqs.push_back(seq);
                }
                det_block_next_tx_key_ += static_cast<uint64_t>(det_batch.size());
                if (skipped_readonly > 0) {
                    st_det_block_skipped_readonly_.fetch_add(skipped_readonly,
                                                             std::memory_order_relaxed);
                }

                for (auto& t : det_batch) {
                    if (t.exec_begin_ns == 0) t.exec_begin_ns = exec_begin_ns;
                    st_dequeued_.fetch_add(1, std::memory_order_relaxed);
                    st_exec_calls_.fetch_add(1, std::memory_order_relaxed);
                    if (t.enqueue_ns != 0 && exec_begin_ns >= t.enqueue_ns) {
                        const uint64_t delay_ns = exec_begin_ns - t.enqueue_ns;
                        st_queue_delay_ns_.fetch_add(delay_ns, std::memory_order_relaxed);
                        st_queue_delay_exec_start_ns_.fetch_add(delay_ns, std::memory_order_relaxed);
                    }
                }

                const uint64_t block_seq = next_det_block_submit_seq++;
                if (!build_ok) {
                    ready_det_block ready;
                    ready.tasks = std::move(det_batch);
                    ready.results.assign(ready.tasks.size(), "ERROR det_block_build_failed");
                    ready.ready_ns = now_steady_ns();
                    record_det_block_batch(ready.tasks.size(), true);
                    ready_det_blocks.emplace(block_seq, std::move(ready));
                    continue;
                }
                if (txs.empty()) {
                    ready_det_block ready;
                    ready.tasks = std::move(det_batch);
                    ready.results.assign(ready.tasks.size(), "");
                    ready.ready_ns = now_steady_ns();
                    record_det_block_batch(ready.tasks.size(), false);
                    ready_det_blocks.emplace(block_seq, std::move(ready));
                    continue;
                }

                std::vector<int64_t> explicit_backend_txids;
                if (db_opt_.raft_apply_ledger_mode != "safe" &&
                    skipped_readonly == 0 && tx_det_seqs.size() == txs.size()) {
                    bool can_use_explicit_txids = true;
                    if (!det_block_backend_txid_delta_set_) {
                        if (tx_det_seqs.empty() ||
                            tx_det_seqs.front() > kDetExplicitTxidMaxSeq) {
                            can_use_explicit_txids = false;
                        } else {
                            det_block_backend_txid_delta_ =
                                static_cast<int64_t>(det_block_next_backend_txid_) -
                                static_cast<int64_t>(tx_det_seqs.front());
                            det_block_backend_txid_delta_set_ = true;
                        }
                    }
                    if (can_use_explicit_txids && det_block_backend_txid_delta_set_) {
                        explicit_backend_txids.reserve(tx_det_seqs.size());
                        for (uint64_t det_seq : tx_det_seqs) {
                            if (det_seq > kDetExplicitTxidMaxSeq) {
                                can_use_explicit_txids = false;
                                break;
                            }
                            const int64_t txid =
                                static_cast<int64_t>(det_seq) + det_block_backend_txid_delta_;
                            if (txid < 0 || txid > INT32_MAX) {
                                can_use_explicit_txids = false;
                                break;
                            }
                            explicit_backend_txids.push_back(txid);
                        }
                    }
                    if (!can_use_explicit_txids) {
                        explicit_backend_txids.clear();
                    }
                }
                uint64_t candidate_backend_txid = det_block_next_backend_txid_;
                if (!explicit_backend_txids.empty()) {
                    const uint64_t next_backend =
                        static_cast<uint64_t>(explicit_backend_txids.back()) + 1ULL;
                    if (next_backend > candidate_backend_txid) {
                        candidate_backend_txid = next_backend;
                    }
                } else {
                    candidate_backend_txid += static_cast<uint64_t>(txs.size());
                }

                std::vector<task> tasks_to_submit;
                tasks_to_submit.reserve(txs.size());
                for (size_t i = 0; i < det_batch.size(); ++i) {
                    if (backend_mask[i] == 1) {
                        tasks_to_submit.push_back(det_batch[i]);
                    }
                }

                const uint64_t submitted_block_id = det_next_block_id_;
                const bool safe_mode = (db_opt_.raft_apply_ledger_mode == "safe");
                const std::string sql = build_bcdb_block_submit_results_sql(
                    submitted_block_id,
                    txs,
                    tasks_to_submit,
                    explicit_backend_txids.empty() ? nullptr : &explicit_backend_txids,
                    db_opt_.raft_epoch_hex,
                    safe_mode);
                if (PQstatus(cs.c) != CONNECTION_OK) {
                    PQreset(cs.c);
                    auto it = notice_state_by_conn_.find(cs.c);
                    if (it != notice_state_by_conn_.end()) {
                        PQsetNoticeProcessor(cs.c, &pg_executor::notice_processor, it->second);
                    }
                }
                auto it = notice_state_by_conn_.find(cs.c);
                notice_state* ns = (it != notice_state_by_conn_.end()) ? it->second : nullptr;
                if (ns) ns->last_merkle_roots.clear();

                bool send_ok = false;
                const char* fail_env = std::getenv("ARIABC_TEST_FAIL_DET_BLOCK_SEND_ONCE");
                static bool force_fail =
                    fail_env != nullptr &&
                    fail_env[0] != '\0' &&
                    strcmp(fail_env, "0") != 0;
                if (force_fail) {
                    force_fail = false;
                } else {
                    if (safe_mode) {
                        log_safe_metadata_submit(tasks_to_submit);
                    }
                    send_ok = (PQsendQuery(cs.c, sql.c_str()) == 1);
                }

                if (!send_ok) {
                    st_fastpath_send_failures_.fetch_add(1, std::memory_order_relaxed);
                    st_fastpath_requeues_.fetch_add(det_batch.size(), std::memory_order_relaxed);
                    std::queue<task> restored;
                    for (auto& t : det_batch) {
                        st_dequeued_.fetch_sub(1, std::memory_order_relaxed);
                        st_exec_calls_.fetch_sub(1, std::memory_order_relaxed);
                        if (t.enqueue_ns != 0 && exec_begin_ns >= t.enqueue_ns) {
                            const uint64_t delay_ns = exec_begin_ns - t.enqueue_ns;
                            st_queue_delay_ns_.fetch_sub(delay_ns, std::memory_order_relaxed);
                            st_queue_delay_exec_start_ns_.fetch_sub(delay_ns, std::memory_order_relaxed);
                        }
                        t.exec_begin_ns = 0;
                        restored.push(std::move(t));
                    }
                    {
                        std::lock_guard<std::mutex> lk(q_mu_);
                        while (!q_.empty()) {
                            restored.push(std::move(q_.front()));
                            q_.pop();
                        }
                        q_.swap(restored);

                        const uint64_t depth = static_cast<uint64_t>(q_.size());
                        st_queue_depth_cur_.store(depth, std::memory_order_relaxed);
                        st_queue_depth_samples_.fetch_add(1, std::memory_order_relaxed);
                        st_queue_depth_sum_.fetch_add(depth, std::memory_order_relaxed);
                    }

                    assert(det_block_next_tx_key_ >= det_batch.size());
                    det_block_next_tx_key_ -= static_cast<uint64_t>(det_batch.size());
                    next_det_block_submit_seq = block_seq;

                    PQreset(cs.c);
                    if (PQstatus(cs.c) != CONNECTION_OK) {
                        st_fastpath_reconnect_failures_.fetch_add(1, std::memory_order_relaxed);
                    }
                    auto it2 = notice_state_by_conn_.find(cs.c);
                    if (it2 != notice_state_by_conn_.end()) {
                        PQsetNoticeProcessor(cs.c, &pg_executor::notice_processor, it2->second);
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    continue;
                }

                // Successful send: commit the reservation
                det_block_next_backend_txid_ = candidate_backend_txid;
                det_next_block_id_ = submitted_block_id + 1;

                cs.cur = task{};
                cs.has_task = false;
                cs.has_det_block = true;
                cs.det_block_id = submitted_block_id;
                cs.det_block_seq = block_seq;
                cs.det_block_tx_key_start = tx_key_start;
                cs.det_block_tasks = std::move(det_batch);
                cs.det_block_backend_mask = std::move(backend_mask);
                cs.exec_start_ns = exec_begin_ns;
                cs.st = conn_state::state::SENDING;
                ++det_blocks_inflight;

                /* Fastpath visibility: count submission, record last block id. */
                st_fastpath_blocks_submitted_.fetch_add(1, std::memory_order_relaxed);
                st_fastpath_last_submitted_block_id_.store(
                    submitted_block_id, std::memory_order_relaxed);
            }
        }
        drain_ready_det_blocks();

        // Assign ready work to idle connections.
        size_t inflight = 0;
        const bool preassigned_exec = state_machine_preassigned_ordering_enabled();
        const bool cap_det_event_inflight =
            db_opt_.db_type == 1 && !det_event_block_fastpath_enabled() && !preassigned_exec;
        const bool strict_det_per_tx_event =
            cap_det_event_inflight &&
            !det_allow_raw_compat_ &&
            !det_prefixed_direct_parallel_;
        const size_t det_event_exec_limit = cap_det_event_inflight
            ? (strict_det_per_tx_event
                   ? 1
                   : std::min<size_t>(
                         static_cast<size_t>(std::max(1, det_block_parallel_)),
                         conns_.empty() ? 1 : conns_.size()))
            : conns_.size();
        for (size_t conn_idx = 0; conn_idx < conns_.size(); ++conn_idx) {
            auto& cs = conns_[conn_idx];
            if (cs.st != conn_state::state::IDLE) {
                ++inflight;
                continue;
            }
            if (cap_det_event_inflight && inflight >= det_event_exec_limit) {
                break;
            }

            task t;
            bool have = false;
            size_t depth_after_pop = 0;
            {
                std::lock_guard<std::mutex> lk(q_mu_);
                if (!conn_qs_.empty() && conn_idx < conn_qs_.size() && !conn_qs_[conn_idx].empty()) {
                    t = std::move(conn_qs_[conn_idx].front());
                    conn_qs_[conn_idx].pop();
                    have = true;
                    size_t total_conn_qs = 0;
                    for (const auto& cq : conn_qs_) {
                        total_conn_qs += cq.size();
                    }
                    depth_after_pop = q_.size() + total_conn_qs;
                    st_queue_depth_cur_.store(static_cast<uint64_t>(depth_after_pop),
                                              std::memory_order_relaxed);
                    st_queue_depth_samples_.fetch_add(1, std::memory_order_relaxed);
                    st_queue_depth_sum_.fetch_add(static_cast<uint64_t>(depth_after_pop),
                                                  std::memory_order_relaxed);
                } else if (!q_.empty()) {
                    const task& front = q_.front();
                    if (db_opt_.db_type == 1 &&
                        !is_det_prefixed_sql(front.sql) &&
                        !det_allow_raw_compat_) {
                        task bad = std::move(q_.front());
                        q_.pop();
                        depth_after_pop = q_.size();
                        have = true;
                        st_queue_depth_cur_.store(static_cast<uint64_t>(depth_after_pop),
                                                  std::memory_order_relaxed);
                        st_queue_depth_samples_.fetch_add(1, std::memory_order_relaxed);
                        st_queue_depth_sum_.fetch_add(static_cast<uint64_t>(depth_after_pop),
                                                      std::memory_order_relaxed);
                        t = std::move(bad);
                    } else if (!det_raw_compat_mode_ &&
                               db_opt_.db_type == 1 &&
                               !is_det_prefixed_sql(front.sql) &&
                               det_allow_raw_compat_) {
                        note_det_raw_compat_activation(front);
                    }
                    if (have) {
                        // Raw deterministic SQL reached a strict prefixed-SQL
                        // executor. Surface the error instead of silently
                        // switching the whole run into compatibility mode.
                    } else if (!det_parallel_workers_ &&
                        det_event_block_fastpath_enabled() &&
                        db_opt_.db_type == 1 &&
                        bcdb_init_done_ &&
                        is_det_prefixed_sql(front.sql)) {
                        // Deterministic parser-mode throughput path is handled by the
                        // block submit fast path above. Always block det-prefixed BCDB
                        // queries from leaking to the multi-connection async path,
                        // regardless of det_raw_compat_mode_ — concurrent async
                        // connections cause non-deterministic snapshot reads.
                        break;
                    }
                    if (det_raw_compat_mode_ && inflight > 0) {
                        // Raw deterministic SQL compatibility mode.
                        // For BCDB (db_type==1): always enforce single-connection
                        // serialization. Concurrent async connections produce
                        // different per-node MVCC snapshots due to OS-scheduling
                        // variance, causing divergent results after restore even
                        // though the BCDB serial gate orders commits correctly.
                        if (db_opt_.db_type == 1) break;
                        // For non-BCDB installs lockstep is opt-in via
                        // ARIABC_DET_COMPAT_LOCKSTEP=1 (serial gate sufficient).
                        static const bool k_compat_lockstep = [] {
                            const char* v = ::getenv("ARIABC_DET_COMPAT_LOCKSTEP");
                            if (!v || !*v) return false;
                            const std::string s = trim_copy(v);
                            return !(s == "0" || s == "false" || s == "FALSE" ||
                                     s == "no" || s == "NO");
                        }();
                        if (k_compat_lockstep) break;
                    }
                    if (!have) {
                        t = std::move(q_.front());
                        q_.pop();
                        depth_after_pop = q_.size();
                        have = true;
                        st_queue_depth_cur_.store(static_cast<uint64_t>(depth_after_pop), std::memory_order_relaxed);
                        st_queue_depth_samples_.fetch_add(1, std::memory_order_relaxed);
                        st_queue_depth_sum_.fetch_add(static_cast<uint64_t>(depth_after_pop), std::memory_order_relaxed);
                    }
                }
            }
            if (!have) continue;
            if (t.dispatch_seq == 0) {
                t.dispatch_seq = next_dispatch_seq_.fetch_add(1, std::memory_order_relaxed);
            }

            const bool first_attempt = (t.attempt == 0 && t.exec_begin_ns == 0);
            if (t.exec_begin_ns == 0) {
                t.exec_begin_ns = now_ns;
            }
            if (first_attempt) {
                st_dequeued_.fetch_add(1, std::memory_order_relaxed);
                st_exec_calls_.fetch_add(1, std::memory_order_relaxed);
                if (t.enqueue_ns != 0 && now_ns >= t.enqueue_ns) {
                    const uint64_t delay_ns = now_ns - t.enqueue_ns;
                    st_queue_delay_ns_.fetch_add(delay_ns, std::memory_order_relaxed);
                    st_queue_delay_exec_start_ns_.fetch_add(delay_ns, std::memory_order_relaxed);
                }
            }

            // Best-effort reconnect if needed.
            if (PQstatus(cs.c) != CONNECTION_OK) {
                PQreset(cs.c);
                auto it = notice_state_by_conn_.find(cs.c);
                if (it != notice_state_by_conn_.end()) {
                    PQsetNoticeProcessor(cs.c, &pg_executor::notice_processor, it->second);
                }
            }

            // Start async send.
            auto it = notice_state_by_conn_.find(cs.c);
            notice_state* ns = (it != notice_state_by_conn_.end()) ? it->second : nullptr;
            if (ns) ns->last_merkle_roots.clear();

            if (db_opt_.db_type == 1 &&
                !is_det_prefixed_sql(t.sql) &&
                !det_allow_raw_compat_) {
                const std::string out = det_unprefixed_sql_error(t);
                mark_det_result_ready(cs.c, std::move(t), out);
                continue;
            }

            const std::string sql_exec =
                maybe_strip_det_prefix_for_compat(t.sql, det_raw_compat_mode_, db_opt_.db_type);
            if (PQsendQuery(cs.c, sql_exec.c_str()) != 1) {
                std::string msg = trim_copy(PQerrorMessage(cs.c));
                const std::string out = "ERROR " + (msg.empty() ? std::string("send_failed") : msg);
                if (t.exec_begin_ns != 0 && now_ns >= t.exec_begin_ns) {
                    st_exec_ns_.fetch_add(now_ns - t.exec_begin_ns, std::memory_order_relaxed);
                }
                mark_det_result_ready(cs.c, std::move(t), out);
                continue;
            }

            cs.cur = std::move(t);
            cs.has_task = true;
            cs.exec_start_ns = now_ns;
            cs.st = conn_state::state::SENDING;
            ++inflight;
        }

        st_inflight_cur_.store(static_cast<uint64_t>(inflight), std::memory_order_relaxed);
        {
            const uint64_t sample_ns = now_steady_ns();
            if (sample_ns >= last_inflight_sample_ns) {
                const uint64_t delta_ns = sample_ns - last_inflight_sample_ns;
                st_inflight_area_ns_.fetch_add(
                    static_cast<uint64_t>(last_inflight_level) * delta_ns,
                    std::memory_order_relaxed);
                st_inflight_time_ns_.fetch_add(delta_ns, std::memory_order_relaxed);
                if (last_inflight_level >= det_event_exec_limit && det_event_exec_limit > 0) {
                    st_inflight_at_cap_ns_.fetch_add(delta_ns, std::memory_order_relaxed);
                }
            }
            last_inflight_sample_ns = sample_ns;
            last_inflight_level = inflight;
        }
        uint64_t cur_inflight_max = st_inflight_max_.load(std::memory_order_relaxed);
        while (inflight > cur_inflight_max &&
               !st_inflight_max_.compare_exchange_weak(
                   cur_inflight_max,
                   static_cast<uint64_t>(inflight),
                   std::memory_order_relaxed,
                   std::memory_order_relaxed)) {
        }

        // Backlog is for observability; overload control uses queued depth.
        size_t backlog = 0;
        size_t queued = 0;
        {
            std::lock_guard<std::mutex> lk(q_mu_);
            queued = q_.size() + delayed_.size();
            backlog = queued + inflight;
            st_queue_depth_cur_.store(static_cast<uint64_t>(q_.size()), std::memory_order_relaxed);
        }
        update_overload_state(backlog, queued);

        // Batch flushing policy: adapt to backlog and age.
        if (kafka_enabled_ && !batch_req_ids.empty()) {
            int max_delay_us = -1;
            if (kConfiguredDelayUs >= 0) {
                max_delay_us = kConfiguredDelayUs;
            } else if (backlog < 16) {
                max_delay_us = kKafkaBatchMaxDelayMs * 1000;
            }
            const auto age_us = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - batch_start).count();
            const size_t max_records = (backlog >= 64) ? 512 : kKafkaBatchMaxRecords;
            const size_t max_bytes = (backlog >= 64) ? (1024 * 1024) : kKafkaBatchMaxBytes;
            if (batch_req_ids.size() >= max_records ||
                batch_bytes >= max_bytes ||
                (max_delay_us >= 0 && age_us >= max_delay_us)) {
                flush_batch(batch_req_ids.size() >= max_records
                                ? FLUSH_REASON_RECORDS
                                : (batch_bytes >= max_bytes
                                       ? FLUSH_REASON_BYTES
                                       : FLUSH_REASON_AGE));
            }
        }

        // Prepare poll set (reuse scratch buffer to avoid per-iter alloc).
        std::vector<pollfd>& pfds = pfds_scratch_;
        pfds.clear();
        if (pfds.capacity() < 1 + conns_.size()) {
            pfds.reserve(1 + conns_.size());
        }
        if (wakeup_rfd_ >= 0) {
            pollfd p;
            p.fd = wakeup_rfd_;
            p.events = POLLIN;
            p.revents = 0;
            pfds.push_back(p);
        }
        for (const auto& cs : conns_) {
            if (cs.fd < 0) continue;
            short ev = 0;
            if (cs.st == conn_state::state::SENDING) ev |= POLLOUT;
            if (cs.st == conn_state::state::READING) ev |= POLLIN;
            if (!ev) continue;
            pollfd p;
            p.fd = cs.fd;
            p.events = ev;
            p.revents = 0;
            pfds.push_back(p);
        }

        // Timeout: wake for next retry deadline or batch delay.
        int timeout_ms = 50;
        {
            std::lock_guard<std::mutex> lk(q_mu_);
            if (!delayed_.empty()) {
                const uint64_t dl_ns = delayed_.top().deadline_ns;
                if (dl_ns <= now_ns) {
                    timeout_ms = 0;
                } else {
                    const uint64_t delta_ns = dl_ns - now_ns;
                    const uint64_t delta_ms = delta_ns / 1000000ULL;
                    timeout_ms = static_cast<int>(std::min<uint64_t>(delta_ms, 50ULL));
                }
            }
        }
        if (next_det_partial_block_ready_ns != 0) {
            if (next_det_partial_block_ready_ns <= now_ns) {
                timeout_ms = 0;
            } else {
                const uint64_t delta_ns = next_det_partial_block_ready_ns - now_ns;
                const uint64_t delta_ms = (delta_ns + 999999ULL) / 1000000ULL;
                timeout_ms = std::min(
                    timeout_ms,
                    static_cast<int>(std::max<uint64_t>(1ULL, std::min<uint64_t>(delta_ms, 50ULL))));
            }
        }
        if (kafka_enabled_ && !batch_req_ids.empty()) {
            int max_delay_us = -1;
            if (kConfiguredDelayUs >= 0) {
                max_delay_us = kConfiguredDelayUs;
            } else if (backlog < 16) {
                max_delay_us = kKafkaBatchMaxDelayMs * 1000;
            }
            const auto age_us = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - batch_start).count();
            if (max_delay_us >= 0) {
                const int remaining_us = std::max(0, max_delay_us - static_cast<int>(age_us));
                const int remaining_ms = std::max<int>(1, (remaining_us + 999) / 1000);
                timeout_ms = std::min(timeout_ms, remaining_ms);
            }
        }

        int rc = 0;
        if (!pfds.empty()) {
            rc = ::poll(pfds.data(), pfds.size(), timeout_ms);
        } else {
            // No sockets in flight; just sleep a bit.
            std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
            rc = 0;
        }
        if (rc < 0) {
            if (errno == EINTR) continue;
        }

        // Drain wakeups.
        if (!pfds.empty() && pfds[0].fd == wakeup_rfd_ && (pfds[0].revents & POLLIN)) {
            uint8_t buf[64];
            while (::read(wakeup_rfd_, buf, sizeof(buf)) > 0) {
            }
        }

        // Drive connections.
        for (auto& cs : conns_) {
            if (cs.fd < 0) continue;
            short re = 0;
            for (const auto& p : pfds) {
                if (p.fd == cs.fd) {
                    re = p.revents;
                    break;
                }
            }
            if (!re) continue;

            if (cs.st == conn_state::state::SENDING && (re & POLLOUT)) {
                const int fr = PQflush(cs.c);
                if (fr == 0) {
                    cs.st = conn_state::state::READING;
                } else if (fr < 0) {
                    cs.st = conn_state::state::IDLE;
                    cs.has_task = false;
                    cs.has_det_block = false;
                    cs.det_block_tasks.clear();
                    cs.det_block_backend_mask.clear();
                }
            }

            if (cs.st == conn_state::state::READING && (re & POLLIN)) {
                if (PQconsumeInput(cs.c) != 1) {
                    cs.st = conn_state::state::IDLE;
                    cs.has_task = false;
                    cs.has_det_block = false;
                    cs.det_block_tasks.clear();
                    cs.det_block_backend_mask.clear();
                    continue;
                }
                if (PQisBusy(cs.c)) continue;

                // Drain all results; keep the last (PQexec-like behavior).
                PGresult* last = nullptr;
                PGresult* r = nullptr;
                while ((r = PQgetResult(cs.c)) != nullptr) {
                    if (last) PQclear(last);
                    last = r;
                }

                const uint64_t query_end_ns = now_steady_ns();
                if (cs.exec_start_ns != 0 && query_end_ns >= cs.exec_start_ns) {
                    st_pg_query_ns_.fetch_add(query_end_ns - cs.exec_start_ns, std::memory_order_relaxed);
                }

                if (cs.has_det_block) {
                    ready_det_block ready;
                    ready.tasks = std::move(cs.det_block_tasks);
                    std::vector<uint8_t> backend_mask = std::move(cs.det_block_backend_mask);
                    if (backend_mask.size() != ready.tasks.size()) {
                        backend_mask.assign(ready.tasks.size(), 1);
                    }
                    bool batch_ok = false;
                    const ExecStatusType st = last ? PQresultStatus(last) : PGRES_FATAL_ERROR;

                    if ((st == PGRES_TUPLES_OK || st == PGRES_COMMAND_OK) &&
                        last && PQntuples(last) >= 1 && PQnfields(last) >= 1) {
                        const auto f0 = std::chrono::steady_clock::now();
                        const char* val = PQgetvalue(last, 0, 0);
                        std::unordered_map<std::string, std::string> result_by_hash;
                        batch_ok = parse_bcdb_block_results_text(
                            val ? std::string(val) : std::string(),
                            result_by_hash);
                        if (batch_ok) {
                            ready.results.assign(ready.tasks.size(), "");
                            ready.errors.assign(ready.tasks.size(), false);
                            for (size_t i = 0; i < ready.tasks.size(); ++i) {
                                if (backend_mask[i] == 0) {
                                    continue;
                                }
                                const std::string key =
                                    std::to_string(cs.det_block_tx_key_start +
                                                   static_cast<uint64_t>(i));
                                auto it = result_by_hash.find(key);
                                if (it == result_by_hash.end()) {
                                    batch_ok = false;
                                    break;
                                }
                                ready.results[i] = it->second;
                            }
                        }
                        const auto f1 = std::chrono::steady_clock::now();
                        st_result_format_ns_.fetch_add(
                            static_cast<uint64_t>(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(f1 - f0).count()),
                            std::memory_order_relaxed);
                    }

                    record_det_block_batch(ready.tasks.size(), !batch_ok);
                    ready.ready_ns = now_steady_ns();
                    if (batch_ok) {
                        bool expected = false;
                        if (st_det_block_seen_.compare_exchange_strong(expected, true)) {
                            std::cerr << "det_block_batch active on node " << node_id_
                                      << " size=" << ready.tasks.size()
                                      << " parallel=" << det_block_parallel_ << std::endl;
                        }
                    } else {
                        bool expected = false;
                        if (st_det_block_fallback_seen_.compare_exchange_strong(expected, true)) {
                            std::cerr << "det_block_batch fallback on node " << node_id_
                                      << " size=" << ready.tasks.size()
                                      << " parallel=" << det_block_parallel_ << std::endl;
                        }
                        ready.results.clear();
                        ready.results.reserve(ready.tasks.size());
                        ready.errors.clear();
                        ready.errors.reserve(ready.tasks.size());
                        for (size_t i = 0; i < ready.tasks.size(); ++i) {
                            if (backend_mask[i] == 0) {
                                ready.results.push_back("");
                                ready.errors.push_back(false);
                            } else {
                                bool is_err = false;
                                ready.results.push_back(exec_sql(cs.c, ready.tasks[i].sql, &is_err));
                                ready.errors.push_back(is_err);
                            }
                        }
                    }

                    const uint64_t finish_ns = now_steady_ns();
                    if (cs.exec_start_ns != 0 && finish_ns >= cs.exec_start_ns) {
                        st_exec_ns_.fetch_add(finish_ns - cs.exec_start_ns, std::memory_order_relaxed);
                    }
                    if (last) PQclear(last);

                    const uint64_t start_ns = cs.exec_start_ns;
                    const uint64_t block_seq = cs.det_block_seq;
                    const uint64_t det_block_id = cs.det_block_id;
                    if (batch_ok && db_opt_.raft_apply_ledger_mode == "safe") {
                        const size_t verify_n = std::min(ready.tasks.size(), ready.results.size());
                        for (size_t i = 0; i < verify_n; ++i) {
                            if (i < backend_mask.size() && backend_mask[i] == 0) {
                                continue;
                            }
                            ConfirmedResult conf = accept_safe_confirmed_result(ready.tasks[i], ready.results[i]);
                            bool durable_ok = false;
                            if (conf.raft_log_index != static_cast<uint64_t>(-1)) {
                                durable_ok = ensure_safe_outcome(cs.c, ready.tasks[i], conf);
                                probe_safe_ledger_outcome_visibility(cs.c,
                                                                     conninfo_,
                                                                     db_opt_.raft_epoch_hex,
                                                                     ready.tasks[i],
                                                                     conf);
                            }
                            if (conf.raft_log_index == static_cast<uint64_t>(-1) ||
                                !durable_ok) {
                                notify_task_failed(ready.tasks[i].raft_log_idx,
                                                   ready.tasks[i].raft_item_ordinal,
                                                   "safe_ledger_terminal_not_durable");
                                ready.results[i] = "[SAFE_PROTOCOL_FAILURE] durable terminal verify failed";
                                batch_ok = false;
                                break;
                            }
                        }
                    }
                    cs.st = conn_state::state::IDLE;
                    cs.has_task = false;
                    cs.has_det_block = false;
                    cs.det_block_id = 0;
                    cs.det_block_seq = 0;
                    cs.det_block_tx_key_start = 0;
                    cs.det_block_backend_mask.clear();
                    cs.exec_start_ns = 0;

                    ready_det_blocks.emplace(block_seq, std::move(ready));
                    {
                        const uint64_t ready_depth = static_cast<uint64_t>(ready_det_blocks.size());
                        uint64_t cur_max = st_fastpath_ready_blocks_max_.load(std::memory_order_relaxed);
                        while (ready_depth > cur_max &&
                               !st_fastpath_ready_blocks_max_.compare_exchange_weak(
                                   cur_max, ready_depth, std::memory_order_relaxed));
                    }

                    /* Fastpath visibility: block returned from PG. */
                    st_fastpath_blocks_returned_.fetch_add(1, std::memory_order_relaxed);
                    st_fastpath_last_returned_block_id_.store(det_block_id, std::memory_order_relaxed);
                    st_fastpath_last_returned_block_seq_.store(block_seq, std::memory_order_relaxed);
                    if (finish_ns > start_ns && start_ns != 0)
                    {
                        const uint64_t rtt_us = (finish_ns - start_ns) / 1000ULL;
                        uint64_t cur_max = st_fastpath_submit_to_return_max_us_.load(
                            std::memory_order_relaxed);
                        while (rtt_us > cur_max &&
                               !st_fastpath_submit_to_return_max_us_.compare_exchange_weak(
                                   cur_max, rtt_us, std::memory_order_relaxed));
                    }
                    drain_ready_det_blocks();
                    continue;
                }

                std::string out;
                bool retry = false;
                std::string err_msg;
                const ExecStatusType st = last ? PQresultStatus(last) : PGRES_FATAL_ERROR;
                const bool is_error = !(st == PGRES_TUPLES_OK || st == PGRES_COMMAND_OK);
                if (!is_error) {
                    const auto f0 = std::chrono::steady_clock::now();
                    if (det_completion_only_success_ &&
                        db_opt_.db_type == 1 &&
                        is_det_prefixed_sql(cs.cur.sql)) {
                        out.clear();
                    } else {
                        out = format_result(last);
                    }
                    const auto f1 = std::chrono::steady_clock::now();
                    st_result_format_ns_.fetch_add(
                        static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(f1 - f0).count()),
                        std::memory_order_relaxed);
                    auto it = notice_state_by_conn_.find(cs.c);
                    notice_state* ns = (it != notice_state_by_conn_.end()) ? it->second : nullptr;
                    if (ns && !ns->last_merkle_roots.empty()) {
                        ns->last_merkle_roots.clear();
                    }
                } else {
                    const char* sqlstate = last ? PQresultErrorField(last, PG_DIAG_SQLSTATE) : nullptr;
                    retry = is_retryable_sqlstate(sqlstate);
                    err_msg = last ? trim_copy(PQresultErrorMessage(last)) : "null result";
                    if (retry && sqlstate) {
                        if (strcmp(sqlstate, "40001") == 0) {
                            st_retryable_sqlstate_40001_.fetch_add(1, std::memory_order_relaxed);
                        } else if (strcmp(sqlstate, "40P01") == 0) {
                            st_retryable_sqlstate_40P01_.fetch_add(1, std::memory_order_relaxed);
                        } else if (strcmp(sqlstate, "57014") == 0) {
                            st_retryable_sqlstate_57014_.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                    if (!retry) {
                        out = "ERROR " + err_msg;
                    }
                }
                if (last) PQclear(last);

                task done_task = std::move(cs.cur);
                const int attempt = done_task.attempt;

                cs.st = conn_state::state::IDLE;
                cs.has_task = false;
                cs.exec_start_ns = 0;

                if (retry) {
                    if (attempt >= db_opt_.max_retries) {
                        st_retry_exhausted_total_.fetch_add(1, std::memory_order_relaxed);
                        out = "ERROR(retry_exhausted) " + err_msg;
                    } else {
                        st_retry_attempts_total_.fetch_add(1, std::memory_order_relaxed);
                        done_task.attempt = attempt + 1;
                        delayed_task dt;
                        dt.deadline_ns = now_steady_ns() +
                            static_cast<uint64_t>(std::max(0, db_opt_.retry_backoff_ms)) * 1000000ULL;
                        dt.t = std::move(done_task);
                        {
                            std::lock_guard<std::mutex> lk(q_mu_);
                            delayed_.push(std::move(dt));
                            st_delayed_cur_.store(static_cast<uint64_t>(delayed_.size()), std::memory_order_relaxed);
                        }
                        if (wakeup_wfd_ >= 0) {
                            const uint8_t b = 1;
                            (void)::write(wakeup_wfd_, &b, 1);
                        }
                        continue;
                    }
                }

                const uint64_t finish_ns = now_steady_ns();
                if (done_task.exec_begin_ns != 0 && finish_ns >= done_task.exec_begin_ns) {
                    st_exec_ns_.fetch_add(finish_ns - done_task.exec_begin_ns, std::memory_order_relaxed);
                }
                mark_det_result_ready(cs.c, std::move(done_task), out, is_error);
            }
        }

        // If we are idle, flush.
        bool idle = true;
        for (const auto& cs : conns_) {
            if (cs.st != conn_state::state::IDLE) {
                idle = false;
                break;
            }
        }
        if (idle) {
            std::lock_guard<std::mutex> lk(q_mu_);
            bool all_conn_qs_empty = true;
            for (const auto& cq : conn_qs_) {
                if (!cq.empty()) { all_conn_qs_empty = false; break; }
            }
            if (q_.empty() && delayed_.empty() && all_conn_qs_empty) {
                flush_batch(FLUSH_REASON_IDLE);
            }
        }
    }

    {
        const uint64_t sample_ns = now_steady_ns();
        if (sample_ns >= last_inflight_sample_ns) {
            const uint64_t delta_ns = sample_ns - last_inflight_sample_ns;
            st_inflight_area_ns_.fetch_add(
                static_cast<uint64_t>(last_inflight_level) * delta_ns,
                std::memory_order_relaxed);
            st_inflight_time_ns_.fetch_add(delta_ns, std::memory_order_relaxed);
            const bool strict_det_per_tx_event =
                db_opt_.db_type == 1 &&
                !det_event_block_fastpath_enabled() &&
                !det_allow_raw_compat_ &&
                !det_prefixed_direct_parallel_;
            const size_t cap = det_event_block_fastpath_enabled()
                ? conns_.size()
                : (strict_det_per_tx_event
                       ? 1
                       : std::min<size_t>(
                             static_cast<size_t>(std::max(1, det_block_parallel_)),
                             conns_.empty() ? 1 : conns_.size()));
            if (last_inflight_level >= cap && cap > 0) {
                st_inflight_at_cap_ns_.fetch_add(delta_ns, std::memory_order_relaxed);
            }
        }
    }
    flush_batch(FLUSH_REASON_FINAL);
}

} // namespace ariabc_pg

#ifdef BUILDING_UNIT_TESTS
namespace ariabc_pg {
std::string test_build_bin_batch_payload_v2(const std::vector<std::string>& req_ids,
                                            const std::vector<std::string>& results,
                                            const std::vector<uint64_t>& raft_log_idxs,
                                            const std::vector<int>& leader_node_hints,
                                            const std::vector<std::string>& terminal_digests,
                                            const std::vector<uint32_t>& raft_item_ordinals,
                                            const std::vector<std::string>& terminal_states,
                                            const std::vector<int>& format_versions,
                                            uint16_t node_id,
                                            const std::string& sig_key,
                                            const std::string& raft_epoch_hex,
                                            bool safe_ledger_mode) {
    return build_bin_batch_payload_v2(req_ids, results, raft_log_idxs, leader_node_hints, terminal_digests, raft_item_ordinals, terminal_states, format_versions, node_id, sig_key, raft_epoch_hex, safe_ledger_mode);
}
}
#endif
