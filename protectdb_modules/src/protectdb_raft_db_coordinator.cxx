#include "pg_state_machine.hxx"
#include "wire_protocol.hxx"

#include "ariabc_pg_util.hxx"
#include "in_memory_state_mgr.hxx"
#include "durable_state_mgr.hxx"
#include <openssl/sha.h>
#include "durable_log_store.hxx"
#include "logger_wrapper.hxx"

#include "nuraft.hxx"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cctype>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace ariabc_pg {

int preferred_leader_id_from_env() {
    const char* v = std::getenv("ARIABC_PREFERRED_LEADER_ID");
    if (!v || !*v) return 0;
    char* end = nullptr;
    const long parsed = std::strtol(v, &end, 10);
    if (end == v || parsed <= 0 || parsed > 1000000) return 0;
    return static_cast<int>(parsed);
}

int preferred_leader_transfer_wait_ms_from_env() {
    const char* v = std::getenv("ARIABC_PREFERRED_LEADER_TRANSFER_WAIT_MS");
    if (!v || !*v) return 1000;
    char* end = nullptr;
    const long parsed = std::strtol(v, &end, 10);
    if (end == v || parsed < 0 || parsed > 60000) return 1000;
    return static_cast<int>(parsed);
}

struct server_profile_stats {
    std::atomic<uint64_t> client_read_frames{0};
    std::atomic<uint64_t> client_read_ns{0};
    std::atomic<uint64_t> client_write_frames{0};
    std::atomic<uint64_t> client_write_ns{0};

    std::atomic<uint64_t> append_calls{0};
    std::atomic<uint64_t> append_ns{0};
    std::atomic<uint64_t> append_not_accepted{0};
    std::atomic<uint64_t> append_not_accepted_busy{0};
    std::atomic<uint64_t> append_stall_calls{0};
    std::atomic<uint64_t> append_stall_ns{0};
    std::atomic<uint64_t> append_stall_max_ns{0};
    std::atomic<uint64_t> append_stall_admission{0};
    std::atomic<uint64_t> append_stall_queue_depth_sum{0};
    std::atomic<uint64_t> append_stall_queue_depth_max{0};

    std::atomic<uint64_t> orderer_arrivals{0};
    std::atomic<uint64_t> orderer_drains{0};
    std::atomic<uint64_t> orderer_pending_depth_max{0};
    std::atomic<uint64_t> orderer_gap_wait_count{0};
    std::atomic<uint64_t> orderer_gap_wait_ns{0};
    std::atomic<uint64_t> append_vector_calls{0};
    std::atomic<uint64_t> append_vector_entries_total{0};
    std::atomic<uint64_t> append_vector_entries_max{0};
    std::atomic<uint64_t> orderer_flush_reason_target{0};
    std::atomic<uint64_t> orderer_flush_reason_linger{0};
    std::atomic<uint64_t> orderer_flush_reason_idle{0};
    std::atomic<uint64_t> orderer_leader_assigned_requests{0};
    std::atomic<uint64_t> orderer_leader_assigned_items{0};
};

server_profile_stats g_prof;
std::atomic<bool> g_stop{false};
int g_listen_fd = -1;
std::atomic<uint64_t> g_debug_server_trace_count{0};

void atomic_max_u64(std::atomic<uint64_t>& target, uint64_t value) {
    uint64_t cur = target.load(std::memory_order_relaxed);
    while (value > cur &&
           !target.compare_exchange_weak(cur,
                                         value,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
    }
}

void note_append_stall(uint64_t stall_ns, uint64_t queue_depth) {
    g_prof.append_stall_calls.fetch_add(1, std::memory_order_relaxed);
    g_prof.append_stall_ns.fetch_add(stall_ns, std::memory_order_relaxed);
    g_prof.append_stall_admission.fetch_add(1, std::memory_order_relaxed);
    g_prof.append_stall_queue_depth_sum.fetch_add(queue_depth, std::memory_order_relaxed);
    atomic_max_u64(g_prof.append_stall_max_ns, stall_ns);
    atomic_max_u64(g_prof.append_stall_queue_depth_max, queue_depth);
}

void on_term(int /*signum*/) {
    g_stop.store(true);
    if (g_listen_fd >= 0) {
        ::close(g_listen_fd);
        g_listen_fd = -1;
    }
}

bool profile_enabled() {
    const char* env = ::getenv("ARIABC_PROFILE");
    return env && *env && std::string(env) != "0";
}

bool starts_with(const std::string& s, const std::string& prefix) {
    return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

std::string profile_token(const std::string& s) {
    if (s.empty()) return "-";
    std::string out;
    out.reserve(s.size());
    for (unsigned char ch : s) {
        if (std::isspace(ch)) {
            out.push_back('_');
        } else {
            out.push_back(static_cast<char>(ch));
        }
    }
    return out;
}

bool parse_det_seq_from_sql(const std::string& sql, uint64_t& out_seq) {
    out_seq = 0;
    const std::string t = trim_copy(sql);
    if (t.size() < 3 || (t[0] != 's' && t[0] != 'S') ||
        !std::isspace(static_cast<unsigned char>(t[1]))) {
        return false;
    }
    size_t i = 2;
    while (i < t.size() && std::isspace(static_cast<unsigned char>(t[i]))) ++i;
    if (i >= t.size() || !std::isdigit(static_cast<unsigned char>(t[i]))) {
        return false;
    }
    uint64_t seq = 0;
    while (i < t.size() && std::isdigit(static_cast<unsigned char>(t[i]))) {
        seq = (seq * 10ULL) + static_cast<uint64_t>(t[i] - '0');
        ++i;
    }
    out_seq = seq;
    return true;
}

bool parse_det_range_from_request(const client_api_request& req,
                                  uint64_t& out_first,
                                  uint64_t& out_last) {
    out_first = 0;
    out_last = 0;
    if (req.is_batch()) {
        if (req.batch_items.empty()) return false;
        for (size_t i = 0; i < req.batch_items.size(); ++i) {
            uint64_t seq = 0;
            if (!parse_det_seq_from_sql(req.batch_items[i].sql, seq)) return false;
            if (i == 0) {
                out_first = seq;
            } else if (seq != out_first + static_cast<uint64_t>(i)) {
                return false;
            }
        }
        out_last = out_first + static_cast<uint64_t>(req.batch_items.size() - 1);
        return true;
    }

    if (!parse_det_seq_from_sql(req.sql, out_first)) return false;
    out_last = out_first;
    return true;
}

bool parse_wait_commit_cmd(const std::string& sql,
                           uint64_t& out_target_idx,
                           int& out_timeout_ms,
                           bool& out_wait_result_mode) {
    out_target_idx = 0;
    out_timeout_ms = 30000;
    out_wait_result_mode = false;
    const std::string ctrl_prefix = "__ARIABC_CTRL_WAIT_COMMIT_INDEX";
    const std::string wait_result_prefix = "WAIT_RESULT";
    std::string rest;
    if (starts_with(sql, ctrl_prefix)) {
        rest = sql.substr(ctrl_prefix.size());
    } else if (starts_with(sql, wait_result_prefix)) {
        rest = sql.substr(wait_result_prefix.size());
        out_wait_result_mode = true;
    } else {
        return false;
    }

    std::istringstream iss(rest);
    long long target = 0;
    long long timeout = 30000;
    if (!(iss >> target)) return false;
    if (iss >> timeout) {
        // optional timeout parsed
    }
    if (target < 0) return false;
    if (timeout <= 0) timeout = 1;
    out_target_idx = static_cast<uint64_t>(target);
    out_timeout_ms = static_cast<int>(timeout);
    return true;
}

bool parse_wait_result_id_cmd(const std::string& sql,
                              std::string& out_req_id,
                              int& out_timeout_ms) {
    out_req_id.clear();
    out_timeout_ms = 30000;
    const std::string prefix = "WAIT_RESULT_ID";
    if (!starts_with(sql, prefix)) {
        return false;
    }
    std::istringstream iss(sql.substr(prefix.size()));
    long long timeout = 30000;
    if (!(iss >> out_req_id)) return false;
    if (iss >> timeout) {
        // optional timeout parsed
    }
    if (out_req_id.empty()) return false;
    if (timeout <= 0) timeout = 1;
    out_timeout_ms = static_cast<int>(timeout);
    return true;
}

bool debug_req_trace_enabled() {
    const char* env = ::getenv("ARIABC_DEBUG_REQ_TRACE");
    if (!env || !*env) return false;
    const std::string s(env);
    return !(s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
}

bool env_flag_enabled(const char* name, bool default_value) {
    const char* env = ::getenv(name);
    if (!env || !*env) return default_value;
    const std::string s(env);
    return !(s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
}

bool raft_ordered_fanout_enabled() {
    return env_flag_enabled("ARIABC_RAFT_ORDERED_FANOUT", false);
}

bool raft_ordered_batch_append_enabled() {
    return env_flag_enabled("ARIABC_RAFT_ORDERED_BATCH_APPEND", false);
}

bool raft_ordered_coalesce_log_enabled() {
    return env_flag_enabled("ARIABC_RAFT_ORDERED_COALESCE_LOG", false);
}

uint64_t env_u64(const char* name, uint64_t default_value) {
    const char* env = ::getenv(name);
    if (!env || !*env) return default_value;
    try {
        return static_cast<uint64_t>(std::stoull(env));
    } catch (...) {
        return default_value;
    }
}

uint64_t raft_ordered_batch_target_entries() {
    return env_u64("ARIABC_RAFT_ORDERED_BATCH_TARGET_ENTRIES", 1);
}

uint64_t raft_ordered_batch_linger_us() {
    return env_u64("ARIABC_RAFT_ORDERED_BATCH_LINGER_US", 0);
}

std::string raft_ordering_policy() {
    const char* env = ::getenv("ARIABC_RAFT_ORDERING_POLICY");
    if (!env || !*env) return "preassigned";
    const std::string policy = trim_copy(env);
    if (policy.empty()) return "preassigned";
    return policy;
}

bool raft_leader_assigned_ordering_enabled() {
    return raft_ordering_policy() == "leader-assigned";
}

uint64_t det_order_start_seq() {
    const char* env = ::getenv("ARIABC_DET_ORDER_START_SEQ");
    if (!env || !*env) return 0;
    try {
        return static_cast<uint64_t>(std::stoull(env));
    } catch (...) {
        return 0;
    }
}

uint64_t debug_req_trace_limit() {
    const char* env = ::getenv("ARIABC_DEBUG_REQ_TRACE_LIMIT");
    if (!env || !*env) return 32;
    try {
        const unsigned long long n = std::stoull(env);
        return (n == 0ULL) ? 32ULL : static_cast<uint64_t>(n);
    } catch (...) {
        return 32ULL;
    }
}

void debug_trace_server_request(const client_api_request& req) {
    if (!debug_req_trace_enabled()) return;
    const uint64_t idx = g_debug_server_trace_count.fetch_add(1, std::memory_order_relaxed);
    if (idx >= debug_req_trace_limit()) return;
    const std::string req_id = req.is_batch() ? req.batch_items.front().req_id : req.req_id;
    const std::string sql = req.is_batch() ? req.batch_items.front().sql : req.sql;
    std::string sql_head = ariabc_pg::trim_copy(sql);
    if (sql_head.size() > 96) sql_head.resize(96);
    std::cerr << "REQ_TRACE server"
              << " idx=" << idx
              << " req_id=" << req_id
              << " batch_items=" << req.item_count()
              << " sql=" << sql_head
              << std::endl;
}

void debug_trace_server_append(const client_api_request& req,
                               bool accepted,
                               int result_code,
                               int leader_id) {
    if (!debug_req_trace_enabled()) return;
    const uint64_t idx = g_debug_server_trace_count.fetch_add(1, std::memory_order_relaxed);
    if (idx >= debug_req_trace_limit()) return;
    std::cerr << "REQ_TRACE append"
              << " idx=" << idx
              << " req_id=" << (req.is_batch() ? req.batch_items.front().req_id : req.req_id)
              << " batch_items=" << req.item_count()
              << " accepted=" << (accepted ? 1 : 0)
              << " code=" << result_code
              << " leader=" << leader_id
              << std::endl;
}

struct server_options {
    int id = 0;
    std::string raft_endpoint;
    int client_port = 0;
    std::string raft_members;

    // When true: skip Raft entirely and directly enqueue to pg_executor.
    bool bypass_raft = false;

    // Raft storage configuration
    std::string raft_storage_mode = "durable";
    std::string raft_storage_dir = "./raft_storage";
    std::string raft_cluster_id = "ariabc_cluster";

    // Commit B1: safe-mode ledger gate: "off" or "safe"
    std::string raft_apply_ledger_mode = "off";

    // Commit B2: cluster epoch as 64-char lowercase hex (32 bytes)
    std::string raft_epoch_hex;

    db_options db;
    kafka_options kafka;
};

void usage(const char* argv0) {
    std::cout
        << "Usage:\n"
        << "  " << argv0 << " \\\n"
        << "    --id <int> --raftEndpoint <host:port> --clientPort <port> \\\n"
        << "    --raftMembers <id=host:port,id=host:port,...> \\\n"
        << "    --dbName <name> --dbPort <port> [--dbHost <host>] [--dbUser <user>] [--dbPass <pass>] \\\n"
        << "    [--dbType <0|1|2>] [--safedb <0|1|2>] [--dbConnPoolSize <N>] [--bcdbInitBlockSize <legacy-init-arg>] [--pgExecMode threaded|event] \\\n"
        << "    [--kafkaBootstrap <host:port>] [--resultTopic <t>] [--resultSigKey <k>] \\\n"
        << "    [--bypassRaft 0|1]  # skip Raft, direct-enqueue to executor (kafka-only profile)\n"
        << "    [--raft-storage-mode <in_memory|durable>]\n"
        << "    [--raft-storage-dir <path>]\n"
        << "    [--raft-apply-ledger off|safe]   # B1: safe-mode ledger gate\n"
        << "    [--raft-epoch-hex <64-hex-chars>] # B2: cluster epoch identifier\n";
}

bool parse_args(int argc, char** argv, server_options& opt, std::string& err) {
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto need = [&](const char* flag) -> std::string {
            if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + flag);
            return std::string(argv[++i]);
        };

        try {
            if (a == "--help" || a == "-h") {
                usage(argv[0]);
                exit(0);
            } else if (a == "--id") {
                opt.id = std::stoi(need("--id"));
            } else if (a == "--raftEndpoint") {
                opt.raft_endpoint = need("--raftEndpoint");
            } else if (a == "--clientPort") {
                opt.client_port = std::stoi(need("--clientPort"));
            } else if (a == "--raftMembers") {
                opt.raft_members = need("--raftMembers");
            } else if (a == "--safedb") {
                opt.db.safedb = std::stoi(need("--safedb"));
            } else if (a == "--dbType") {
                opt.db.db_type = std::stoi(need("--dbType"));
            } else if (a == "--dbName") {
                opt.db.dbname = need("--dbName");
            } else if (a == "--dbHost") {
                opt.db.host = need("--dbHost");
            } else if (a == "--dbPort") {
                opt.db.port = need("--dbPort");
            } else if (a == "--dbUser") {
                opt.db.user = need("--dbUser");
            } else if (a == "--dbPass") {
                opt.db.password = need("--dbPass");
            } else if (a == "--dbConnPoolSize") {
                opt.db.conn_pool_size = std::stoi(need("--dbConnPoolSize"));
            } else if (a == "--bcdbInitBlockSize") {
                opt.db.bcdb_init_block_size = std::stoi(need("--bcdbInitBlockSize"));
            } else if (a == "--pgExecMode") {
                opt.db.exec_mode = need("--pgExecMode");
            } else if (a == "--kafkaBootstrap") {
                opt.kafka.bootstrap = need("--kafkaBootstrap");
            } else if (a == "--resultTopic") {
                opt.kafka.result_topic = need("--resultTopic");
            } else if (a == "--resultSigKey") {
                opt.kafka.result_sig_key = need("--resultSigKey");
            } else if (a == "--bypassRaft") {
                opt.bypass_raft = (std::stoi(need("--bypassRaft")) != 0);
            } else if (a == "--raft-storage-mode") {
                opt.raft_storage_mode = need("--raft-storage-mode");
            } else if (a == "--raft-storage-dir") {
                opt.raft_storage_dir = need("--raft-storage-dir");
            } else if (a == "--raft-cluster-id") {
                opt.raft_cluster_id = need("--raft-cluster-id");
            } else if (a == "--raft-apply-ledger") {
                opt.raft_apply_ledger_mode = need("--raft-apply-ledger");
            } else if (a == "--raft-epoch-hex") {
                opt.raft_epoch_hex = need("--raft-epoch-hex");
            } else {
                throw std::runtime_error("unknown flag: " + a);
            }
        } catch (const std::exception& e) {
            err = e.what();
            return false;
        }
    }
    if (opt.id <= 0) {
        err = "invalid/missing --id";
        return false;
    }
    if (!opt.bypass_raft && opt.raft_endpoint.empty()) {
        err = "missing --raftEndpoint";
        return false;
    }
    if (opt.client_port <= 0 || opt.client_port > 65535) {
        err = "invalid/missing --clientPort";
        return false;
    }
    if (opt.db.dbname.empty() || opt.db.port.empty()) {
        err = "missing --dbName/--dbPort";
        return false;
    }
    if (opt.db.db_type < 0 || opt.db.db_type > 2) {
        err = "invalid --dbType";
        return false;
    }
    if (opt.db.conn_pool_size <= 0) {
        err = "invalid --dbConnPoolSize";
        return false;
    }
    if (opt.db.bcdb_init_block_size < 0) {
        err = "invalid --bcdbInitBlockSize";
        return false;
    }
    {
        const std::string m = ariabc_pg::trim_copy(opt.db.exec_mode);
        if (!m.empty() &&
            m != "threaded" && m != "event" && m != "reactor" && m != "async") {
            err = "invalid --pgExecMode (expected threaded|event)";
            return false;
        }
    }
    // Validate storage mode: must be exactly "durable" or "in_memory".
    // An unrecognised value (e.g. a typo) must never silently fall back to
    // in-memory, because the caller would believe they launched a durable node.
    {
        const std::string& sm = opt.raft_storage_mode;
        if (sm != "durable" && sm != "in_memory") {
            err = "invalid --raft-storage-mode '" + sm + "' (expected 'durable' or 'in_memory')";
            return false;
        }
        if (sm == "durable") {
            if (opt.raft_storage_dir.empty()) {
                err = "--raft-storage-dir is required when --raft-storage-mode=durable";
                return false;
            }
            if (opt.raft_cluster_id.empty()) {
                err = "--raft-cluster-id is required when --raft-storage-mode=durable";
                return false;
            }
        }
    }
    // B1: validate --raft-apply-ledger
    {
        const std::string& lm = opt.raft_apply_ledger_mode;
        if (lm != "off" && lm != "safe") {
            err = "invalid --raft-apply-ledger '" + lm + "' (expected 'off' or 'safe')";
            return false;
        }
        // B2: safe mode requires a valid 64-char lowercase hex epoch
        if (lm == "safe") {
            const std::string& hex = opt.raft_epoch_hex;
            if (hex.size() != 64) {
                err = "--raft-epoch-hex must be exactly 64 lowercase hex chars when --raft-apply-ledger=safe";
                return false;
            }
            for (char c : hex) {
                if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                    err = "--raft-epoch-hex contains non-lowercase-hex character '" + std::string(1, c) + "'";
                    return false;
                }
            }
        }
    }
    opt.db.raft_apply_ledger_mode = opt.raft_apply_ledger_mode;
    opt.db.raft_epoch_hex = opt.raft_epoch_hex;
    return true;
}

struct raft_member {
    int id = 0;
    std::string endpoint;
};

std::vector<raft_member> parse_raft_members(const server_options& opt) {
    std::vector<raft_member> out;
    if (opt.raft_members.empty()) {
        raft_member me;
        me.id = opt.id;
        me.endpoint = opt.raft_endpoint;
        out.push_back(me);
        return out;
    }

    const std::vector<std::string> parts = split_csv_trim(opt.raft_members);
    for (const auto& p : parts) {
        const std::vector<std::string> kv = split_char(p, '=');
        if (kv.size() != 2) {
            throw std::runtime_error("invalid --raftMembers entry: " + p);
        }
        const std::string id_s = trim_copy(kv[0]);
        const std::string ep_s = trim_copy(kv[1]);
        const int id = std::stoi(id_s);
        if (id <= 0) throw std::runtime_error("invalid member id: " + id_s);
        const host_port hp = parse_host_port(ep_s);
        raft_member m;
        m.id = id;
        m.endpoint = hp.host + ":" + std::to_string(hp.port);
        out.push_back(m);
    }
    bool found_me = false;
    for (const auto& m : out) {
        if (m.id == opt.id) {
            found_me = true;
            if (m.endpoint != opt.raft_endpoint) {
                throw std::runtime_error("my endpoint mismatch: --raftEndpoint=" + opt.raft_endpoint +
                                         " but --raftMembers says " + m.endpoint);
            }
        }
    }
    if (!found_me) {
        throw std::runtime_error("my id not present in --raftMembers");
    }
    return out;
}

int listen_tcp(int port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error(std::string("socket failed: ") + ::strerror(errno));
    }
    int on = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    sockaddr_in addr;
    ::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        const std::string msg = std::string("bind failed: ") + ::strerror(errno);
        ::close(fd);
        throw std::runtime_error(msg);
    }
    if (::listen(fd, 128) != 0) {
        const std::string msg = std::string("listen failed: ") + ::strerror(errno);
        ::close(fd);
        throw std::runtime_error(msg);
    }
    return fd;
}

void wait_for_admission_drain(pg_state_machine* psm) {
    if (!psm) return;

    const auto s0 = std::chrono::steady_clock::now();
    bool stalled = false;
    uint64_t waits = 0;
    while (psm->admission_control_blocked() && !g_stop.load(std::memory_order_relaxed)) {
        stalled = true;
        // Cap a single wait so we re-check g_stop periodically.
        (void)psm->wait_for_admission_drain(50ULL * 1000ULL * 1000ULL); // 50ms
        ++waits;
        if (waits == 20 || (waits > 20 && waits % 100 == 0)) {
            const pg_executor_stats exec = psm->executor_stats();
            std::cerr << "SAFE_RAFT_APPEND_ADMISSION_WAIT"
                      << " waits=" << waits
                      << " queue_depth=" << exec.queue_depth_cur
                      << " backlog=" << exec.backlog_cur
                      << " inflight=" << exec.inflight_cur
                      << std::endl;
        }
    }
    if (stalled) {
        const auto s1 = std::chrono::steady_clock::now();
        const pg_executor_stats exec = psm->executor_stats();
        note_append_stall(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count()),
            exec.queue_depth_cur);
    }
}

void append_request_to_raft(nuraft::ptr<nuraft::raft_server> raft,
                            pg_state_machine* psm,
                            const client_api_request& req,
                            client_api_response& resp) {
    resp = client_api_response();
    if (!raft) {
        resp.status = 1;
        resp.msg = "NO_RAFT_SERVER";
        return;
    }

    wait_for_admission_drain(psm);

    std::string err;
    const int leader_hint = raft->get_leader();
    nuraft::ptr<nuraft::buffer> log =
        build_raft_request_log(req, leader_hint, err);
    if (!log) {
        resp.status = 2;
        resp.msg = err.empty() ? std::string("LOG_BUILD_FAILED") : err;
        return;
    }

    g_prof.append_calls.fetch_add(1, std::memory_order_relaxed);
    g_prof.append_vector_calls.fetch_add(1, std::memory_order_relaxed);
    g_prof.append_vector_entries_total.fetch_add(1, std::memory_order_relaxed);
    atomic_max_u64(g_prof.append_vector_entries_max, 1);
    const auto a0 = std::chrono::steady_clock::now();
    nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> r =
        raft->append_entries({log});
    const auto a1 = std::chrono::steady_clock::now();
    g_prof.append_ns.fetch_add(
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(a1 - a0).count()),
        std::memory_order_relaxed);

    const bool accepted = r && r->get_accepted();
    const int result_code = r ? static_cast<int>(r->get_result_code()) : -1;
    debug_trace_server_append(req, accepted, result_code, raft->get_leader());
    if (psm) {
        std::cerr << "SAFE_RAFT_APPEND_RESULT"
                  << " accepted=" << (accepted ? 1 : 0)
                  << " code=" << result_code
                  << " leader=" << raft->get_leader()
                  << " last_log_idx=" << raft->get_last_log_idx()
                  << " items=" << req.item_count()
                  << std::endl;
    }

    if (!accepted) {
        g_prof.append_not_accepted.fetch_add(1, std::memory_order_relaxed);
        resp.status = 1;
        resp.msg =
            "NOT_ACCEPTED code=" + std::to_string(result_code) +
            " accepted=0 leader=" + std::to_string(raft->get_leader()) +
            " leader_id=" + std::to_string(raft->get_leader()) +
            " raft_log_idx=0";
        return;
    }

    const uint64_t raft_log_idx_hint =
        static_cast<uint64_t>(raft->get_last_log_idx());
    resp.status = 0;
    resp.msg = "ACCEPTED accepted=1 leader=" + std::to_string(raft->get_leader()) +
               " leader_id=" + std::to_string(raft->get_leader()) +
               " raft_log_idx=" + std::to_string(raft_log_idx_hint) +
               " batch_items=" + std::to_string(req.item_count());
}

struct raft_ordered_entry {
    client_api_request req;
    uint64_t first_seq = 0;
    uint64_t last_seq = 0;
    client_api_response resp;
    bool done = false;
};

client_api_request coalesce_raft_ordered_entries(
    const std::vector<std::shared_ptr<raft_ordered_entry>>& entries) {
    client_api_request out;
    size_t item_count = 0;
    for (const std::shared_ptr<raft_ordered_entry>& entry : entries) {
        item_count += entry->req.item_count();
    }
    out.batch_items.reserve(item_count);
    for (const std::shared_ptr<raft_ordered_entry>& entry : entries) {
        if (entry->req.is_batch()) {
            out.batch_items.insert(out.batch_items.end(),
                                   entry->req.batch_items.begin(),
                                   entry->req.batch_items.end());
        } else {
            client_api_request_item item;
            item.req_id = entry->req.req_id;
            item.sql = entry->req.sql;
            item.has_assigned_det_seq = entry->req.has_assigned_det_seq;
            item.assigned_det_seq = entry->req.assigned_det_seq;
            out.batch_items.push_back(std::move(item));
        }
    }
    return out;
}

void append_requests_to_raft_batch(
    nuraft::ptr<nuraft::raft_server> raft,
    pg_state_machine* psm,
    const std::vector<std::shared_ptr<raft_ordered_entry>>& entries) {
    if (entries.empty()) {
        return;
    }
    if (entries.size() == 1) {
        append_request_to_raft(raft, psm, entries.front()->req, entries.front()->resp);
        return;
    }
    if (raft_ordered_coalesce_log_enabled()) {
        client_api_request coalesced = coalesce_raft_ordered_entries(entries);
        client_api_response batch_resp;
        append_request_to_raft(raft, psm, coalesced, batch_resp);
        std::cerr << "SAFE_RAFT_ORDERER_COALESCE_LOG"
                  << " status=" << static_cast<int>(batch_resp.status)
                  << " entries=" << entries.size()
                  << " items=" << coalesced.item_count()
                  << " first_seq=" << entries.front()->first_seq
                  << " last_seq=" << entries.back()->last_seq
                  << std::endl;
        for (std::shared_ptr<raft_ordered_entry> entry : entries) {
            entry->resp = batch_resp;
            if (entry->resp.status == 0) {
                entry->resp.msg +=
                    " raft_coalesced_entries=" + std::to_string(entries.size()) +
                    " raft_coalesced_items=" + std::to_string(coalesced.item_count());
            }
        }
        return;
    }

    for (std::shared_ptr<raft_ordered_entry> entry : entries) {
        entry->resp = client_api_response();
    }
    if (!raft) {
        for (std::shared_ptr<raft_ordered_entry> entry : entries) {
            entry->resp.status = 1;
            entry->resp.msg = "NO_RAFT_SERVER";
        }
        return;
    }

    wait_for_admission_drain(psm);

    std::vector<nuraft::ptr<nuraft::buffer>> logs;
    logs.reserve(entries.size());
    const int leader_hint = raft->get_leader();
    for (std::shared_ptr<raft_ordered_entry> entry : entries) {
        std::string err;
        nuraft::ptr<nuraft::buffer> log =
            build_raft_request_log(entry->req, leader_hint, err);
        if (!log) {
            entry->resp.status = 2;
            entry->resp.msg = err.empty() ? std::string("LOG_BUILD_FAILED") : err;
            for (std::shared_ptr<raft_ordered_entry> blocked : entries) {
                if (blocked.get() != entry.get()) {
                    blocked->resp.status = 1;
                    blocked->resp.msg = "RAFT_BATCH_BUILD_BLOCKED first_seq=" +
                                        std::to_string(blocked->first_seq) +
                                        " blocked_by=" +
                                        std::to_string(entry->first_seq);
                }
            }
            return;
        }
        logs.push_back(log);
    }

    g_prof.append_calls.fetch_add(static_cast<uint64_t>(logs.size()),
                                  std::memory_order_relaxed);
    g_prof.append_vector_calls.fetch_add(1, std::memory_order_relaxed);
    g_prof.append_vector_entries_total.fetch_add(static_cast<uint64_t>(logs.size()),
                                                 std::memory_order_relaxed);
    atomic_max_u64(g_prof.append_vector_entries_max,
                   static_cast<uint64_t>(logs.size()));
    const auto a0 = std::chrono::steady_clock::now();
    nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> r =
        raft->append_entries(logs);
    const auto a1 = std::chrono::steady_clock::now();
    g_prof.append_ns.fetch_add(
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(a1 - a0).count()),
        std::memory_order_relaxed);

    const bool accepted = r && r->get_accepted();
    const int result_code = r ? static_cast<int>(r->get_result_code()) : -1;
    const uint64_t raft_log_idx_hint =
        static_cast<uint64_t>(raft->get_last_log_idx());
    std::cerr << "SAFE_RAFT_APPEND_BATCH_RESULT"
              << " accepted=" << (accepted ? 1 : 0)
              << " code=" << result_code
              << " leader=" << raft->get_leader()
              << " last_log_idx=" << raft_log_idx_hint
              << " entries=" << entries.size()
              << " first_seq=" << entries.front()->first_seq
              << " last_seq=" << entries.back()->last_seq
              << std::endl;

    if (!accepted) {
        g_prof.append_not_accepted.fetch_add(static_cast<uint64_t>(entries.size()),
                                             std::memory_order_relaxed);
        for (std::shared_ptr<raft_ordered_entry> entry : entries) {
            entry->resp.status = 1;
            entry->resp.msg =
                "NOT_ACCEPTED code=" + std::to_string(result_code) +
                " accepted=0 leader=" + std::to_string(raft->get_leader()) +
                " leader_id=" + std::to_string(raft->get_leader()) +
                " raft_log_idx=0";
        }
        return;
    }

    for (std::shared_ptr<raft_ordered_entry> entry : entries) {
        entry->resp.status = 0;
        entry->resp.msg =
            "ACCEPTED accepted=1 leader=" + std::to_string(raft->get_leader()) +
            " leader_id=" + std::to_string(raft->get_leader()) +
            " raft_log_idx=" + std::to_string(raft_log_idx_hint) +
            " batch_items=" + std::to_string(entry->req.item_count()) +
            " raft_append_batch_entries=" + std::to_string(entries.size());
    }
}

void maybe_wait_for_terminal_result(pg_state_machine* psm,
                                    const client_api_request& req,
                                    client_api_response& resp,
                                    const char* completion_source) {
    if (!psm || !req.wait_for_terminal || resp.status != 0) {
        return;
    }
    if (req.is_batch()) {
        resp.status = 1;
        resp.msg = "FUSED_WAIT_UNSUPPORTED_BATCH";
        return;
    }
    if (req.req_id.empty()) {
        resp.status = 1;
        resp.msg = "FUSED_WAIT_MISSING_REQ_ID";
        return;
    }

    uint32_t timeout_ms_u32 = req.terminal_timeout_ms;
    if (timeout_ms_u32 == 0) {
        timeout_ms_u32 = 30000;
    }
    if (timeout_ms_u32 > 600000) {
        timeout_ms_u32 = 600000;
    }

    std::string failure_reason;
    if (psm->wait_for_result_id(req.req_id,
                                static_cast<int>(timeout_ms_u32),
                                &failure_reason)) {
        resp.status = 0;
        resp.msg = "WAIT_RESULT_ID_OK state=COMPLETED completion_source=" +
                   std::string(completion_source) +
                   " req_id=" + req.req_id;
    } else if (!failure_reason.empty()) {
        resp.status = 1;
        resp.msg = "WAIT_RESULT_ID_FAILED req_id=" + req.req_id +
                   " reason=" + failure_reason;
    } else {
        const uint64_t cur = static_cast<uint64_t>(psm->last_commit_index());
        resp.status = 1;
        resp.msg = "WAIT_RESULT_ID_TIMEOUT cur=" + std::to_string(cur) +
                   " req_id=" + req.req_id;
    }
}

struct raft_orderer {
    std::mutex mu;
    std::condition_variable cv;
    bool initialized = false;
    bool drained_any = false;
    uint64_t next_seq = 1;
    uint64_t next_assigned_seq = 1;
    std::map<uint64_t, std::shared_ptr<raft_ordered_entry>> pending;
    // leader-assigned dedup: maps req_id -> first_seq for in-flight requests.
    // Prevents retried gateway submissions from getting a new first_seq and
    // being committed twice to the Raft log with different raft_log_idx.
    std::unordered_map<std::string, uint64_t> req_id_to_seq;
};

bool append_raft_ordered(nuraft::ptr<nuraft::raft_server> raft,
                         pg_state_machine* psm,
                         const client_api_request& req,
                         raft_orderer& orderer,
                         client_api_response& out_resp) {
    if (!raft_ordered_fanout_enabled()) {
        return false;
    }

    std::shared_ptr<raft_ordered_entry> entry(new raft_ordered_entry());
    const bool leader_assigned = raft_leader_assigned_ordering_enabled();

    std::unique_lock<std::mutex> lk(orderer.mu);
    const uint64_t order_start_seq = det_order_start_seq();
    uint64_t first_seq = 0;
    uint64_t last_seq = 0;
    if (leader_assigned) {
        if (!orderer.initialized) {
            orderer.initialized = true;
            orderer.next_seq = order_start_seq;
            orderer.next_assigned_seq = order_start_seq;
        }
        // Idempotent assignment: if this req_id is already in-flight (a gateway
        // retry), reuse its original first_seq so it lands in the same Raft
        // log slot. Without this, retries get a new first_seq, are committed
        // twice, and produce duplicate_identity_conflict in the vote store.
        std::string primary_req_id = req.req_id;
        if (primary_req_id.empty() && req.is_batch() && !req.batch_items.empty()) {
            primary_req_id = req.batch_items.front().req_id;
        }
        if (!primary_req_id.empty()) {
            auto it_dedup = orderer.req_id_to_seq.find(primary_req_id);
            if (it_dedup != orderer.req_id_to_seq.end()) {
                out_resp.status = 1;
                out_resp.msg = "LEADER_ASSIGNED_INFLIGHT_DUPLICATE req_id=" + primary_req_id +
                               " existing_first_seq=" + std::to_string(it_dedup->second);
                return true;
            }
        }
        first_seq = orderer.next_assigned_seq;
        last_seq = first_seq + static_cast<uint64_t>(req.item_count() - 1);
        entry->req = req;
        orderer.next_assigned_seq = last_seq + 1;
        if (!primary_req_id.empty()) {
            orderer.req_id_to_seq[primary_req_id] = first_seq;
        }
        g_prof.orderer_leader_assigned_requests.fetch_add(1, std::memory_order_relaxed);
        g_prof.orderer_leader_assigned_items.fetch_add(entry->req.item_count(),
                                                       std::memory_order_relaxed);
    } else {
        if (!parse_det_range_from_request(req, first_seq, last_seq)) {
            return false;
        }
        entry->req = req;

        if (!orderer.initialized) {
            orderer.initialized = true;
            // Preflight probes can use high DET ids before the workload restarts.
            // Do not let an early fanout lane make a later batch the epoch start.
            orderer.next_seq = (first_seq >= 90000000ULL) ? first_seq : order_start_seq;
        } else if (orderer.pending.empty() &&
                   first_seq < orderer.next_seq &&
                   !orderer.drained_any) {
            orderer.next_seq = first_seq;
        } else if (orderer.pending.empty() &&
                   first_seq < orderer.next_seq &&
                   orderer.next_seq - first_seq > 1000000ULL) {
            orderer.next_seq = order_start_seq;
        }
    }
    entry->first_seq = first_seq;
    entry->last_seq = last_seq;

    if (first_seq < orderer.next_seq) {
        out_resp.status = 1;
        out_resp.msg = "STALE_RAFT_DET_SEQ first_seq=" + std::to_string(first_seq) +
                       " next_seq=" + std::to_string(orderer.next_seq);
        return true;
    }
    if (orderer.pending.find(first_seq) != orderer.pending.end()) {
        out_resp.status = 1;
        out_resp.msg = "DUPLICATE_RAFT_DET_SEQ first_seq=" + std::to_string(first_seq);
        return true;
    }
    orderer.pending.emplace(first_seq, entry);
    g_prof.orderer_arrivals.fetch_add(1, std::memory_order_relaxed);
    atomic_max_u64(g_prof.orderer_pending_depth_max,
                   static_cast<uint64_t>(orderer.pending.size()));
    std::cerr << "SAFE_RAFT_ORDERER_ENQUEUE"
              << " first_seq=" << first_seq
              << " last_seq=" << last_seq
              << " next_seq=" << orderer.next_seq
              << " pending=" << orderer.pending.size()
              << std::endl;

    auto fail_pending_after = [&](uint64_t blocker_seq) {
        for (auto& kv : orderer.pending) {
            std::shared_ptr<raft_ordered_entry> blocked = kv.second;
            blocked->resp.status = 1;
            blocked->resp.msg = "RAFT_ORDERER_BLOCKED first_seq=" +
                                std::to_string(blocked->first_seq) +
                                " blocked_by=" + std::to_string(blocker_seq);
            blocked->done = true;
            if (leader_assigned) {
                const std::string& rid = blocked->req.req_id;
                if (!rid.empty()) {
                    orderer.req_id_to_seq.erase(rid);
                }
            }
        }
        orderer.pending.clear();
        orderer.cv.notify_all();
    };

    auto drain_ready = [&]() {
        while (!g_stop.load(std::memory_order_relaxed)) {
            auto it = orderer.pending.find(orderer.next_seq);
            if (it == orderer.pending.end()) break;
            std::vector<std::shared_ptr<raft_ordered_entry>> ready_batch;
            ready_batch.push_back(it->second);
            orderer.pending.erase(it);
            bool hit_target = false;
            bool hit_linger = false;
            if (raft_ordered_batch_append_enabled()) {
                auto collect_contiguous = [&]() {
                    uint64_t want_seq = ready_batch.back()->last_seq + 1;
                    while (!g_stop.load(std::memory_order_relaxed)) {
                        auto next_it = orderer.pending.find(want_seq);
                        if (next_it == orderer.pending.end()) {
                            break;
                        }
                        ready_batch.push_back(next_it->second);
                        orderer.pending.erase(next_it);
                        want_seq = ready_batch.back()->last_seq + 1;
                    }
                };
                collect_contiguous();

                const uint64_t target_entries =
                    std::max<uint64_t>(1, raft_ordered_batch_target_entries());
                const uint64_t linger_us = raft_ordered_batch_linger_us();
                hit_target = ready_batch.size() >= target_entries;
                if (target_entries > 1 &&
                    linger_us > 0 &&
                    ready_batch.size() < target_entries) {
                    const auto deadline =
                        std::chrono::steady_clock::now() +
                        std::chrono::microseconds(linger_us);
                    while (!g_stop.load(std::memory_order_relaxed) &&
                           ready_batch.size() < target_entries) {
                        const uint64_t want_seq = ready_batch.back()->last_seq + 1;
                        if (orderer.pending.find(want_seq) != orderer.pending.end()) {
                            collect_contiguous();
                            hit_target = ready_batch.size() >= target_entries;
                            continue;
                        }
                        if (orderer.cv.wait_until(lk, deadline) == std::cv_status::timeout) {
                            collect_contiguous();
                            hit_target = ready_batch.size() >= target_entries;
                            hit_linger = !hit_target;
                            break;
                        }
                    }
                }
            }
            if (hit_target) {
                g_prof.orderer_flush_reason_target.fetch_add(1, std::memory_order_relaxed);
            } else if (hit_linger) {
                g_prof.orderer_flush_reason_linger.fetch_add(1, std::memory_order_relaxed);
            } else {
                g_prof.orderer_flush_reason_idle.fetch_add(1, std::memory_order_relaxed);
            }

            std::cerr << "SAFE_RAFT_ORDERER_DRAIN"
                      << " first_seq=" << ready_batch.front()->first_seq
                      << " last_seq=" << ready_batch.back()->last_seq
                      << " next_seq=" << orderer.next_seq
                      << " entries=" << ready_batch.size()
                      << std::endl;
            g_prof.orderer_drains.fetch_add(1, std::memory_order_relaxed);
            orderer.next_seq = ready_batch.back()->last_seq + 1;
            std::cerr << "SAFE_RAFT_ORDERER_ADVANCE"
                      << " next_seq=" << orderer.next_seq
                      << std::endl;
            orderer.drained_any = true;
            orderer.cv.notify_all();

            lk.unlock();
            append_requests_to_raft_batch(raft, psm, ready_batch);
            lk.lock();

            bool failed = false;
            uint64_t blocker_seq = ready_batch.front()->first_seq;
            for (std::shared_ptr<raft_ordered_entry> ready : ready_batch) {
                ready->done = true;
                // Remove from dedup map once the entry is committed and complete
                // so that post-commit retries are treated as fresh requests.
                if (leader_assigned) {
                    const std::string& rid = ready->req.req_id;
                    if (!rid.empty()) {
                        orderer.req_id_to_seq.erase(rid);
                    }
                }
                if (ready->resp.status != 0 && !failed) {
                    failed = true;
                    blocker_seq = ready->first_seq;
                }
            }
            if (failed) {
                fail_pending_after(blocker_seq);
                orderer.cv.notify_all();
                break;
            }
            orderer.cv.notify_all();
        }
    };

    drain_ready();
    bool gap_wait = false;
    std::chrono::steady_clock::time_point gap_wait_start;
    if (!leader_assigned &&
        !entry->done &&
        orderer.pending.find(orderer.next_seq) == orderer.pending.end()) {
        gap_wait = true;
        gap_wait_start = std::chrono::steady_clock::now();
        g_prof.orderer_gap_wait_count.fetch_add(1, std::memory_order_relaxed);
    }
    orderer.cv.wait(lk, [&] {
        return entry->done || g_stop.load(std::memory_order_relaxed);
    });
    if (gap_wait) {
        const auto gap_wait_end = std::chrono::steady_clock::now();
        g_prof.orderer_gap_wait_ns.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    gap_wait_end - gap_wait_start).count()),
            std::memory_order_relaxed);
    }
    if (!entry->done) {
        out_resp.status = 1;
        out_resp.msg = "RAFT_ORDERER_STOPPED first_seq=" + std::to_string(first_seq);
    } else {
        out_resp = entry->resp;
    }
    return true;
}

void handle_client_fd(int fd,
                      nuraft::ptr<nuraft::raft_server> raft,
                      nuraft::ptr<nuraft::state_machine> sm,
                      std::shared_ptr<raft_orderer> orderer) {
    // Low-latency request/response: avoid Nagle delays on small frames.
    {
        int one = 1;
        (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
    while (true) {
        client_api_request req;
        std::string err;
        const auto r0 = std::chrono::steady_clock::now();
        const bool ok_read = read_request_frame(fd, req, err);
        const auto r1 = std::chrono::steady_clock::now();
        if (!ok_read) {
            break;
        }
        debug_trace_server_request(req);
        g_prof.client_read_frames.fetch_add(1, std::memory_order_relaxed);
        g_prof.client_read_ns.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(r1 - r0).count()),
            std::memory_order_relaxed);

        pg_state_machine* psm = sm ? dynamic_cast<pg_state_machine*>(sm.get()) : nullptr;

        // Local control-plane commands used by gateway barriers.
        if (req.sql == "__ARIABC_CTRL_GET_LEADER") {
            client_api_response resp;
            resp.status = 0;
            resp.msg = std::to_string(raft ? raft->get_leader() : -1);
            const bool ok_write = write_response_frame(fd, resp, err);
            if (!ok_write) break;
            continue;
        }
        if (req.sql == "__ARIABC_CTRL_IS_LEADER") {
            client_api_response resp;
            resp.status = 0;
            resp.msg = (raft && raft->is_leader()) ? "1" : "0";
            const bool ok_write = write_response_frame(fd, resp, err);
            if (!ok_write) break;
            continue;
        }
        if (psm && req.sql == "__ARIABC_CTRL_GET_COMMIT_INDEX") {
            client_api_response resp;
            resp.status = 0;
            resp.msg = std::to_string(static_cast<uint64_t>(psm->last_commit_index()));
            const bool ok_write = write_response_frame(fd, resp, err);
            if (!ok_write) break;
            continue;
        }
        if (psm && starts_with(req.sql, "WAIT_RESULT_ID")) {
            std::string wait_req_id;
            int timeout_ms = 30000;
            client_api_response resp;
            if (!parse_wait_result_id_cmd(req.sql, wait_req_id, timeout_ms)) {
                resp.status = 1;
                resp.msg = "INVALID_WAIT_RESULT_ID_COMMAND";
            } else {
                std::string failure_reason;
                if (psm->wait_for_result_id(wait_req_id, timeout_ms, &failure_reason)) {
                    resp.status = 0;
                    resp.msg = "WAIT_RESULT_ID_OK state=COMPLETED completion_source=apply_complete req_id=" +
                               wait_req_id;
                } else if (!failure_reason.empty()) {
                    resp.status = 1;
                    resp.msg = "WAIT_RESULT_ID_FAILED req_id=" + wait_req_id +
                               " reason=" + failure_reason;
                } else {
                    const uint64_t cur = static_cast<uint64_t>(psm->last_commit_index());
                    resp.status = 1;
                    resp.msg = "WAIT_RESULT_ID_TIMEOUT cur=" + std::to_string(cur) +
                               " req_id=" + wait_req_id;
                }
            }
            const bool ok_write = write_response_frame(fd, resp, err);
            if (!ok_write) break;
            continue;
        }
        if (psm && (starts_with(req.sql, "__ARIABC_CTRL_WAIT_COMMIT_INDEX") ||
                    starts_with(req.sql, "WAIT_RESULT"))) {
            uint64_t target_idx = 0;
            int timeout_ms = 30000;
            bool wait_result_mode = false;
            client_api_response resp;
            if (!parse_wait_commit_cmd(req.sql, target_idx, timeout_ms, wait_result_mode)) {
                resp.status = 1;
                resp.msg = "INVALID_WAIT_COMMIT_COMMAND";
            } else {
                if (wait_result_mode) {
                    std::string failure_reason;
                    if (psm->wait_for_result(target_idx, timeout_ms, &failure_reason)) {
                        resp.status = 0;
                        resp.msg = "WAIT_RESULT_OK completion_source=apply_complete raft_log_idx=" +
                                   std::to_string(target_idx) +
                                   " result_payload=NA result_hash=NA";
                    } else if (!failure_reason.empty()) {
                        resp.status = 1;
                        resp.msg = "WAIT_RESULT_FAILED cur=" + std::to_string(static_cast<uint64_t>(psm->last_commit_index())) +
                                   " target=" + std::to_string(target_idx) +
                                   " reason=" + failure_reason;
                    } else {
                        const uint64_t cur = static_cast<uint64_t>(psm->last_commit_index());
                        resp.status = 1;
                        resp.msg = "WAIT_RESULT_TIMEOUT cur=" + std::to_string(cur) +
                                   " target=" + std::to_string(target_idx);
                    }
                    const bool ok_write = write_response_frame(fd, resp, err);
                    if (!ok_write) break;
                    continue;
                }
                const auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
                bool ok = false;
                while (!g_stop.load(std::memory_order_relaxed)) {
                    const uint64_t cur = static_cast<uint64_t>(psm->last_commit_index());
                    if (cur >= target_idx) {
                        resp.status = 0;
                        resp.msg = std::to_string(cur);
                        ok = true;
                        break;
                    }
                    if (std::chrono::steady_clock::now() >= deadline) {
                        resp.status = 1;
                        resp.msg = "WAIT_COMMIT_TIMEOUT cur=" + std::to_string(cur) +
                                   " target=" + std::to_string(target_idx);
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                if (!ok && resp.msg.empty()) {
                    const uint64_t cur = static_cast<uint64_t>(psm->last_commit_index());
                    resp.status = 1;
                    resp.msg = "WAIT_COMMIT_ABORTED cur=" + std::to_string(cur) +
                               " target=" + std::to_string(target_idx);
                }
            }
            const bool ok_write = write_response_frame(fd, resp, err);
            if (!ok_write) break;
            continue;
        }

        client_api_response resp;
        if (!orderer || !append_raft_ordered(raft, psm, req, *orderer, resp)) {
            append_request_to_raft(raft, psm, req, resp);
        }
        maybe_wait_for_terminal_result(psm, req, resp, "fused_apply_complete");

        const auto w0 = std::chrono::steady_clock::now();
        const bool ok_write = write_response_frame(fd, resp, err);
        const auto w1 = std::chrono::steady_clock::now();
        g_prof.client_write_frames.fetch_add(1, std::memory_order_relaxed);
        g_prof.client_write_ns.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count()),
            std::memory_order_relaxed);
        if (!ok_write) {
            break;
        }
    }
    ::close(fd);
}

struct direct_ordered_entry {
    client_api_request req;
    uint64_t first_seq = 0;
    uint64_t last_seq = 0;
    client_api_response resp;
    bool done = false;
};

struct direct_orderer {
    std::mutex mu;
    std::condition_variable cv;
    bool initialized = false;
    bool drained_any = false;
    uint64_t next_seq = 1;
    std::map<uint64_t, std::shared_ptr<direct_ordered_entry>> pending;
};

client_api_response make_direct_accept_response(uint64_t last_seq, size_t item_count) {
    client_api_response resp;
    resp.status = 0;
    resp.msg = "ACCEPTED_DIRECT accepted=1 leader=-1 leader_id=-1 raft_log_idx=" +
               std::to_string(last_seq) +
               " seq=" + std::to_string(last_seq) +
               " batch_items=" + std::to_string(item_count);
    return resp;
}

void direct_enqueue_to_state_machine(pg_state_machine* psm,
                                     const client_api_request& req,
                                     uint64_t first_seq) {
    if (req.is_batch()) {
        psm->direct_enqueue_batch(req.batch_items, first_seq);
    } else {
        psm->direct_enqueue(req.req_id, req.sql, first_seq);
    }
}

bool direct_enqueue_ordered(pg_state_machine* psm,
                            const client_api_request& req,
                            direct_orderer& orderer,
                            client_api_response& out_resp) {
    uint64_t first_seq = 0;
    uint64_t last_seq = 0;
    if (!parse_det_range_from_request(req, first_seq, last_seq)) {
        return false;
    }

    std::shared_ptr<direct_ordered_entry> entry(new direct_ordered_entry());
    entry->req = req;
    entry->first_seq = first_seq;
    entry->last_seq = last_seq;

    std::unique_lock<std::mutex> lk(orderer.mu);
    const uint64_t order_start_seq = det_order_start_seq();
    std::cerr << "[ORDERER] first_seq=" << first_seq << " last_seq=" << last_seq
              << " order_start_seq=" << order_start_seq << " init=" << orderer.initialized
              << " next_seq=" << orderer.next_seq << std::endl;
    if (!orderer.initialized) {
        orderer.initialized = true;
        if (first_seq >= 90000000ULL) {
            orderer.next_seq = first_seq;
        } else if (order_start_seq > 0) {
            orderer.next_seq = order_start_seq;
        } else {
            orderer.next_seq = first_seq;
        }
        std::cerr << "[ORDERER] initialized next_seq to " << orderer.next_seq << std::endl;
    } else if (orderer.pending.empty() &&
               first_seq < orderer.next_seq &&
               !orderer.drained_any) {
        orderer.next_seq = first_seq;
        std::cerr << "[ORDERER] reset next_seq to " << orderer.next_seq << std::endl;
    } else if (orderer.pending.empty() &&
               first_seq < orderer.next_seq &&
               orderer.next_seq - first_seq > 1000000ULL) {
        orderer.next_seq = order_start_seq;
        std::cerr << "[ORDERER] reset epoch next_seq to " << orderer.next_seq << std::endl;
    }

    if (orderer.pending.find(first_seq) != orderer.pending.end()) {
        out_resp.status = 1;
        out_resp.msg = "DUPLICATE_DIRECT_DET_SEQ first_seq=" + std::to_string(first_seq);
        std::cerr << "[ORDERER] DUPLICATE_DIRECT_DET_SEQ: " << first_seq << std::endl;
        return true;
    }
    orderer.pending.emplace(first_seq, entry);

    auto drain_ready = [&]() {
        while (!g_stop.load(std::memory_order_relaxed)) {
            auto it = orderer.pending.find(orderer.next_seq);
            if (it == orderer.pending.end()) {
                std::cerr << "[ORDERER] drain stopped: next_seq=" << orderer.next_seq
                          << " not in pending (size=" << orderer.pending.size() << ")" << std::endl;
                break;
            }
            std::shared_ptr<direct_ordered_entry> ready = it->second;
            orderer.pending.erase(it);

            std::cerr << "[ORDERER] draining ready first_seq=" << ready->first_seq << std::endl;
            lk.unlock();
            direct_enqueue_to_state_machine(psm, ready->req, ready->first_seq);
            lk.lock();

            ready->resp = make_direct_accept_response(ready->last_seq, ready->req.item_count());
            ready->done = true;
            orderer.next_seq = ready->last_seq + 1;
            orderer.drained_any = true;
            orderer.cv.notify_all();
        }
    };

    drain_ready();
    orderer.cv.wait(lk, [&] {
        return entry->done || g_stop.load(std::memory_order_relaxed);
    });
    if (!entry->done) {
        out_resp.status = 1;
        out_resp.msg = "DIRECT_ORDERER_STOPPED first_seq=" + std::to_string(first_seq);
    } else {
        out_resp = entry->resp;
    }
    return true;
}

// Bypass-Raft handler: directly enqueues requests into pg_executor without
// going through NuRaft. Used for the kafka-only-no-raft configuration where
// ordering is provided by the gateway broadcasting in the same sequence to all
// replicas. Kafka result collection in the gateway enforces distributed agreement.
void handle_client_fd_direct(int fd,
                              pg_state_machine* psm,
                              std::atomic<uint64_t>& seq_counter,
                              direct_orderer& orderer) {
    std::cerr << "[DIRECT] client connected fd=" << fd << std::endl;
    {
        int one = 1;
        (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
    while (true) {
        client_api_request req;
        std::string err;
        const auto r0 = std::chrono::steady_clock::now();
        const bool ok_read = read_request_frame(fd, req, err);
        const auto r1 = std::chrono::steady_clock::now();
        if (!ok_read) {
            std::cerr << "[DIRECT] read_request_frame failed fd=" << fd << " err=" << err << std::endl;
            break;
        }

        std::cerr << "[DIRECT] req fd=" << fd << " req_id=" << req.req_id << " sql=" << req.sql.substr(0, 40)
                  << " batch_items=" << req.batch_items.size() << std::endl;

        debug_trace_server_request(req);
        g_prof.client_read_frames.fetch_add(1, std::memory_order_relaxed);
        g_prof.client_read_ns.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(r1 - r0).count()),
            std::memory_order_relaxed);

        if (!psm) {
            client_api_response resp;
            resp.status = 1;
            resp.msg = "BYPASS_RAFT_NO_PSM";
            write_response_frame(fd, resp, err);
            break;
        }

        if (req.sql == "__ARIABC_CTRL_GET_LEADER") {
            client_api_response resp;
            resp.status = 0;
            resp.msg = "1";
            const bool ok_write = write_response_frame(fd, resp, err);
            if (!ok_write) break;
            continue;
        }
        if (req.sql == "__ARIABC_CTRL_IS_LEADER") {
            client_api_response resp;
            resp.status = 0;
            resp.msg = "1";
            const bool ok_write = write_response_frame(fd, resp, err);
            if (!ok_write) break;
            continue;
        }
        // Control-plane: return seq counter as proxy for commit index.
        if (req.sql == "__ARIABC_CTRL_GET_COMMIT_INDEX") {
            client_api_response resp;
            resp.status = 0;
            resp.msg = std::to_string(seq_counter.load(std::memory_order_relaxed));
            const bool ok_write = write_response_frame(fd, resp, err);
            if (!ok_write) break;
            continue;
        }
        if (starts_with(req.sql, "WAIT_RESULT_ID")) {
            std::string wait_req_id;
            int timeout_ms = 30000;
            client_api_response resp;
            if (!parse_wait_result_id_cmd(req.sql, wait_req_id, timeout_ms)) {
                resp.status = 1;
                resp.msg = "INVALID_WAIT_RESULT_ID_COMMAND";
            } else {
                std::string failure_reason;
                if (psm->wait_for_result_id(wait_req_id, timeout_ms, &failure_reason)) {
                    resp.status = 0;
                    resp.msg = "WAIT_RESULT_ID_OK state=COMPLETED completion_source=direct_apply req_id=" +
                               wait_req_id;
                } else if (!failure_reason.empty()) {
                    resp.status = 1;
                    resp.msg = "WAIT_RESULT_ID_FAILED req_id=" + wait_req_id +
                               " reason=" + failure_reason;
                } else {
                    const uint64_t cur = seq_counter.load(std::memory_order_relaxed);
                    resp.status = 1;
                    resp.msg = "WAIT_RESULT_ID_TIMEOUT cur=" + std::to_string(cur) +
                               " req_id=" + wait_req_id;
                }
            }
            const bool ok_write = write_response_frame(fd, resp, err);
            if (!ok_write) break;
            continue;
        }
        if (starts_with(req.sql, "__ARIABC_CTRL_WAIT_COMMIT_INDEX") || starts_with(req.sql, "WAIT_RESULT")) {
            uint64_t target_idx = 0;
            int timeout_ms = 30000;
            bool wait_result_mode = false;
            client_api_response resp;
            if (!parse_wait_commit_cmd(req.sql, target_idx, timeout_ms, wait_result_mode)) {
                resp.status = 1;
                resp.msg = "INVALID_WAIT_COMMIT_COMMAND";
            } else if (wait_result_mode) {
                std::string failure_reason;
                if (psm->wait_for_result(target_idx, timeout_ms, &failure_reason)) {
                    resp.status = 0;
                    resp.msg = "WAIT_RESULT_OK completion_source=direct_apply raft_log_idx=" +
                               std::to_string(target_idx) +
                               " result_payload=NA result_hash=NA";
                } else if (!failure_reason.empty()) {
                    resp.status = 1;
                    resp.msg = "WAIT_RESULT_FAILED cur=" + std::to_string(seq_counter.load(std::memory_order_relaxed)) +
                               " target=" + std::to_string(target_idx) +
                               " reason=" + failure_reason;
                } else {
                    const uint64_t cur = seq_counter.load(std::memory_order_relaxed);
                    resp.status = 1;
                    resp.msg = "WAIT_RESULT_TIMEOUT cur=" + std::to_string(cur) +
                               " target=" + std::to_string(target_idx);
                }
            } else {
                resp.status = 0;
                resp.msg = std::to_string(seq_counter.load(std::memory_order_relaxed));
            }
            const bool ok_write = write_response_frame(fd, resp, err);
            if (!ok_write) break;
            continue;
        }

        // Admission control (direct path): condvar-backed wait instead of sleep polling.
        {
            const auto s0 = std::chrono::steady_clock::now();
            bool stalled = false;
            while (psm->admission_control_blocked() && !g_stop.load(std::memory_order_relaxed)) {
                stalled = true;
                (void)psm->wait_for_admission_drain(50ULL * 1000ULL * 1000ULL); // 50ms
            }
            if (stalled) {
                const auto s1 = std::chrono::steady_clock::now();
                const pg_executor_stats exec = psm->executor_stats();
                note_append_stall(
                    static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count()),
                    exec.queue_depth_cur);
            }
        }

        client_api_response resp;
        if (!direct_enqueue_ordered(psm, req, orderer, resp)) {
            // Non-deterministic fallback for direct test probes that do not
            // carry the "s NNNNNNNN" prefix.
            const uint64_t seq = seq_counter.fetch_add(
                static_cast<uint64_t>(req.item_count()),
                std::memory_order_relaxed);
            direct_enqueue_to_state_machine(psm, req, seq);
            const uint64_t last_seq = seq + static_cast<uint64_t>(req.item_count() - 1);
            resp = make_direct_accept_response(last_seq, req.item_count());
        }
        g_prof.append_calls.fetch_add(1, std::memory_order_relaxed);

        const auto w0 = std::chrono::steady_clock::now();
        const bool ok_write = write_response_frame(fd, resp, err);
        const auto w1 = std::chrono::steady_clock::now();
        g_prof.client_write_frames.fetch_add(1, std::memory_order_relaxed);
        g_prof.client_write_ns.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count()),
            std::memory_order_relaxed);
        if (!ok_write) break;
    }
    ::close(fd);
}

void dump_profile(nuraft::ptr<nuraft::raft_server> raft,
                  nuraft::ptr<nuraft::state_machine> sm,
                  nuraft::ptr<nuraft::state_mgr> smgr = nullptr)
{
    if (!profile_enabled()) return;

    pg_executor_stats exec;
    kafka_producer_stats kprod;
    pg_state_machine::result_tracker_profile result_prof;
    if (sm) {
        // Safe in this example: we construct this concrete state machine.
        pg_state_machine* psm = dynamic_cast<pg_state_machine*>(sm.get());
        if (psm) {
            exec = psm->executor_stats();
            kprod = psm->kafka_stats();
            result_prof = psm->result_profile();
        }
    }

    const uint64_t read_frames = g_prof.client_read_frames.load(std::memory_order_relaxed);
    const uint64_t write_frames = g_prof.client_write_frames.load(std::memory_order_relaxed);
    const uint64_t append_calls = g_prof.append_calls.load(std::memory_order_relaxed);

        std::cout
        << "PROFILE_SERVER "
        << "read_frames=" << read_frames
        << " read_ms=" << (g_prof.client_read_ns.load(std::memory_order_relaxed) / 1000000.0)
        << " write_frames=" << write_frames
        << " write_ms=" << (g_prof.client_write_ns.load(std::memory_order_relaxed) / 1000000.0)
        << " append_calls=" << append_calls
        << " append_ms=" << (g_prof.append_ns.load(std::memory_order_relaxed) / 1000000.0)
        << " append_not_accepted=" << g_prof.append_not_accepted.load(std::memory_order_relaxed)
        << " append_not_accepted_busy=" << g_prof.append_not_accepted_busy.load(std::memory_order_relaxed)
        << " append_stall_calls=" << g_prof.append_stall_calls.load(std::memory_order_relaxed)
        << " append_stall_ms=" << (g_prof.append_stall_ns.load(std::memory_order_relaxed) / 1000000.0)
        << " append_stall_max_ms=" << (g_prof.append_stall_max_ns.load(std::memory_order_relaxed) / 1000000.0)
        << " append_stall_reason_admission=" << g_prof.append_stall_admission.load(std::memory_order_relaxed)
        << " req_id_map_size_current=" << result_prof.req_id_map_size_current
        << " req_id_map_size_max=" << result_prof.req_id_map_size_max
        << " result_token_map_size_current=" << result_prof.result_token_map_size_current
        << " result_token_map_size_max=" << result_prof.result_token_map_size_max
        << " append_stall_queue_depth_avg="
        << ((g_prof.append_stall_calls.load(std::memory_order_relaxed) > 0)
                ? (static_cast<double>(g_prof.append_stall_queue_depth_sum.load(std::memory_order_relaxed)) /
                   static_cast<double>(g_prof.append_stall_calls.load(std::memory_order_relaxed)))
                : 0.0)
        << " append_stall_queue_depth_max=" << g_prof.append_stall_queue_depth_max.load(std::memory_order_relaxed)
        << " raft_leader=" << (raft ? raft->get_leader() : -1)
        << " exec_calls=" << exec.exec_calls
        << " exec_ms=" << (exec.exec_ns / 1000000.0)
        << " pg_query_ms=" << (exec.pg_query_ns / 1000000.0)
        << " configured_server_workers=" << exec.threaded_workers_configured
        << " created_server_workers=" << exec.threaded_workers_created
        << " unique_owned_pg_connections=" << exec.owned_pg_connections
        << " concurrent_pqexec_cur=" << exec.concurrent_pqexec_cur
        << " max_concurrent_PQexec=" << exec.concurrent_pqexec_max
        << " overlapping_PQexec_intervals=" << exec.overlapping_pqexec_intervals
        << " result_format_ms=" << (exec.result_format_ns / 1000000.0)
        << " retryable_sqlstate_40001=" << exec.retryable_sqlstate_40001
        << " retryable_sqlstate_40P01=" << exec.retryable_sqlstate_40P01
        << " retryable_sqlstate_57014=" << exec.retryable_sqlstate_57014
        << " retry_attempts_total=" << exec.retry_attempts_total
        << " retry_exhausted_total=" << exec.retry_exhausted_total
        << " enqueue_to_pickup_us=" << (exec.queue_delay_dequeue_ns / 1000.0)
        << " pickup_to_PQexec_start_us=" << (exec.queue_delay_exec_start_ns / 1000.0)
        << " PQexec_us=" << (exec.pg_query_ns / 1000.0)
        << " PQexec_to_completion_notify_us=" << (exec.result_format_ns / 1000.0)
        << " q_wait_ms=" << (exec.q_wait_ns / 1000000.0)
        << " queue_delay_ms=" << (exec.queue_delay_ns / 1000000.0)
        << " queue_delay_dequeue_ms=" << (exec.queue_delay_dequeue_ns / 1000000.0)
        << " queue_delay_exec_start_ms=" << (exec.queue_delay_exec_start_ns / 1000000.0)
        << " backlog_cur=" << exec.backlog_cur
        << " inflight_cur=" << exec.inflight_cur
        << " inflight_max=" << exec.inflight_max
        << " inflight_avg=" << exec.inflight_avg
        << " inflight_at_cap_ms=" << (exec.inflight_at_cap_ns / 1000000.0)
        << " delayed_cur=" << exec.delayed_cur
        << " queue_depth_cur=" << exec.queue_depth_cur
        << " queue_depth_max=" << exec.queue_depth_max
        << " queue_depth_samples=" << exec.queue_depth_samples
        << " queue_depth_avg="
        << ((exec.queue_depth_samples > 0)
                ? (static_cast<double>(exec.queue_depth_sum) / static_cast<double>(exec.queue_depth_samples))
                : 0.0)
        << " queue_depth_bin_le_16=" << exec.queue_depth_bin_le_16
        << " queue_depth_bin_17_64=" << exec.queue_depth_bin_17_64
        << " queue_depth_bin_65_256=" << exec.queue_depth_bin_65_256
        << " queue_depth_bin_gt_256=" << exec.queue_depth_bin_gt_256
        << " queue_high_wm=" << exec.queue_high_watermark
        << " queue_low_wm=" << exec.queue_low_watermark
        << " queue_overloaded=" << exec.queue_overloaded
        << " queue_overload_enter=" << exec.queue_overload_enter
        << " queue_overload_exit=" << exec.queue_overload_exit
        << " bcdb_init_enabled=" << exec.bcdb_init_enabled
        << " bcdb_block_size=" << exec.bcdb_block_size
        << " bcdb_init_arg_size_configured=" << exec.bcdb_init_arg_size_configured
        << " det_block_batches=" << exec.det_block_batches
        << " det_block_items=" << exec.det_block_items
        << " det_block_avg="
        << ((exec.det_block_batches > 0)
                ? (static_cast<double>(exec.det_block_items) / static_cast<double>(exec.det_block_batches))
                : 0.0)
        << " det_block_min=" << exec.det_block_min
        << " det_block_max=" << exec.det_block_max
        << " det_block_bin_1=" << exec.det_block_bin_1
        << " det_block_bin_2_15=" << exec.det_block_bin_2_15
        << " det_block_bin_16_63=" << exec.det_block_bin_16_63
        << " det_block_bin_64_127=" << exec.det_block_bin_64_127
        << " det_block_bin_128_plus=" << exec.det_block_bin_128_plus
        << " det_block_fallbacks=" << exec.det_block_fallbacks
        << " det_block_skipped_readonly=" << exec.det_block_skipped_readonly
        << " ready_det_results_max=" << exec.ready_det_results_max
        << " ordered_emit_wait_ms=" << (exec.ordered_emit_wait_ns / 1000000.0)
        << " kafka_immediate_records=" << exec.kafka_immediate_records
        << " ordered_apply_wait_ms=" << (exec.ordered_apply_wait_ns / 1000000.0)
        << " ordered_apply_pending_max=" << exec.ordered_apply_pending_max
        << " det_raw_compat_mode=" << exec.det_raw_compat_mode
        << " det_prefixed_direct_parallel=" << exec.det_prefixed_direct_parallel
        << " det_completion_only_success=" << exec.det_completion_only_success
        << " det_raw_compat_activations=" << exec.det_raw_compat_activations
        << " det_raw_compat_first_req_id=" << profile_token(exec.det_raw_compat_first_req_id)
        << " det_raw_compat_first_sql_prefix=" << profile_token(exec.det_raw_compat_first_sql_prefix)
        << " conn_wait_ms=" << (exec.conn_acquire_wait_ns / 1000000.0)
        << " kafka_flush_calls=" << exec.kafka_flush_calls
        << " kafka_payload_kb=" << (exec.kafka_payload_bytes / 1024.0)
        << " kafka_batch_records=" << exec.kafka_batch_records
        << " kafka_batch_records_avg="
        << ((exec.kafka_flush_calls > 0)
                ? (static_cast<double>(exec.kafka_batch_records) /
                   static_cast<double>(exec.kafka_flush_calls))
                : 0.0)
        << " kafka_batch_records_max=" << exec.kafka_batch_records_max
        << " kafka_batch_dwell_ms_avg="
        << ((exec.kafka_batch_records > 0)
                ? ((exec.kafka_batch_dwell_ns / 1000000.0) /
                   static_cast<double>(exec.kafka_batch_records))
                : 0.0)
        << " kafka_batch_dwell_ms_max=" << (exec.kafka_batch_dwell_max_ns / 1000000.0)
        << " kafka_flush_reason_records=" << exec.kafka_flush_reason_records
        << " kafka_flush_reason_bytes=" << exec.kafka_flush_reason_bytes
        << " kafka_flush_reason_age=" << exec.kafka_flush_reason_age
        << " kafka_flush_reason_idle=" << exec.kafka_flush_reason_idle
        << " kafka_flush_reason_final=" << exec.kafka_flush_reason_final
        << " kafka_batch_records_bin_1=" << exec.kafka_batch_records_bin_1
        << " kafka_batch_records_bin_2_15=" << exec.kafka_batch_records_bin_2_15
        << " kafka_batch_records_bin_16_63=" << exec.kafka_batch_records_bin_16_63
        << " kafka_batch_records_bin_64_255=" << exec.kafka_batch_records_bin_64_255
        << " kafka_batch_records_bin_256_plus=" << exec.kafka_batch_records_bin_256_plus
        << " kafka_batch_bytes_bin_le_1k=" << exec.kafka_batch_bytes_bin_le_1k
        << " kafka_batch_bytes_bin_1k_10k=" << exec.kafka_batch_bytes_bin_1k_10k
        << " kafka_batch_bytes_bin_10k_100k=" << exec.kafka_batch_bytes_bin_10k_100k
        << " kafka_batch_bytes_bin_100k_plus=" << exec.kafka_batch_bytes_bin_100k_plus
        << " kafka_batch_dwell_bin_le_1ms=" << exec.kafka_batch_dwell_bin_le_1ms
        << " kafka_batch_dwell_bin_1_5ms=" << exec.kafka_batch_dwell_bin_1_5ms
        << " kafka_batch_dwell_bin_5_20ms=" << exec.kafka_batch_dwell_bin_5_20ms
        << " kafka_batch_dwell_bin_20_100ms=" << exec.kafka_batch_dwell_bin_20_100ms
        << " kafka_batch_dwell_bin_100ms_plus=" << exec.kafka_batch_dwell_bin_100ms_plus
        << " kafka_flush_backlog_bin_0=" << exec.kafka_flush_backlog_bin_0
        << " kafka_flush_backlog_bin_1_15=" << exec.kafka_flush_backlog_bin_1_15
        << " kafka_flush_backlog_bin_16_63=" << exec.kafka_flush_backlog_bin_16_63
        << " kafka_flush_backlog_bin_64_255=" << exec.kafka_flush_backlog_bin_64_255
        << " kafka_flush_backlog_bin_256_plus=" << exec.kafka_flush_backlog_bin_256_plus
        << " kafka_flush_inflight_bin_0=" << exec.kafka_flush_inflight_bin_0
        << " kafka_flush_inflight_bin_1_15=" << exec.kafka_flush_inflight_bin_1_15
        << " kafka_flush_inflight_bin_16_63=" << exec.kafka_flush_inflight_bin_16_63
        << " kafka_flush_inflight_bin_64_255=" << exec.kafka_flush_inflight_bin_64_255
        << " kafka_flush_inflight_bin_256_plus=" << exec.kafka_flush_inflight_bin_256_plus
        << " kafka_build_ms=" << (exec.kafka_build_payload_ns / 1000000.0)
        << " kafka_send_ms=" << (exec.kafka_send_ns / 1000000.0)
        << " kafka_send_calls=" << kprod.send_calls
        << " kafka_send_ok=" << kprod.send_ok
        << " kafka_producev_ms=" << (kprod.producev_ns / 1000000.0)
        << " kafka_delivery_calls=" << kprod.delivery_calls
        << " kafka_delivery_errors=" << kprod.delivery_errors
        << " kafka_delivery_ms=" << (kprod.delivery_ns / 1000000.0)
        << " kafka_delivery_ms_avg="
        << ((kprod.delivery_calls > 0)
                ? ((kprod.delivery_ns / 1000000.0) /
                   static_cast<double>(kprod.delivery_calls))
                : 0.0)
        << " kafka_delivery_ms_max=" << (kprod.delivery_max_ns / 1000000.0)
        << " kafka_poll_ms=" << (kprod.poll_ns / 1000000.0)
        << " kafka_backoff_ms=" << (kprod.backoff_sleep_ns / 1000000.0)
        << " kafka_flush_ms=" << (kprod.flush_ns / 1000000.0)
        << " kafka_callback_poll_calls=" << kprod.producer_callback_poll_calls
        << " kafka_callback_poll_ms=" << (kprod.producer_callback_poll_ns / 1000000.0)
        << " kafka_producer_delivery_pending_max=" << kprod.delivery_pending_max
        << " kafka_result_batch_delay_override_us=" << ariabc_pg::pg_executor::override_batch_delay_us()
        << " kafka_worker_low_backlog_delay_us=1000"
        << " kafka_worker_high_backlog_delay_us=-1"
        << " kafka_event_loop_default_delay_us=1000"
        << " kafka_event_loop_high_backlog_delay_us=-1"
        << " det_fastpath_blocks_submitted=" << exec.det_fastpath_blocks_submitted
        << " det_fastpath_blocks_returned=" << exec.det_fastpath_blocks_returned
        << " det_fastpath_blocks_emitted=" << exec.det_fastpath_blocks_emitted
        << " det_fastpath_ready_blocks_max=" << exec.det_fastpath_ready_blocks_max
        << " det_fastpath_last_submitted_block_id=" << exec.det_fastpath_last_submitted_block_id
        << " det_fastpath_last_returned_block_id=" << exec.det_fastpath_last_returned_block_id
        << " det_fastpath_last_returned_block_seq=" << exec.det_fastpath_last_returned_block_seq
        << " det_fastpath_last_emitted_seq=" << exec.det_fastpath_last_emitted_seq
        << " det_fastpath_submit_to_return_max_us=" << exec.det_fastpath_submit_to_return_max_us
        << " det_fastpath_send_failures=" << exec.det_fastpath_send_failures
        << " det_fastpath_requeues=" << exec.det_fastpath_requeues
        << " det_fastpath_reconnect_failures=" << exec.det_fastpath_reconnect_failures
        << " result_flush_count=" << exec.result_flush_count
        << " result_flush_records_total=" << exec.result_flush_records_total
        << " result_flush_records_max=" << exec.result_flush_records_max
        << " result_flush_due_to_record_cap=" << exec.result_flush_due_to_record_cap
        << " result_flush_due_to_byte_cap=" << exec.result_flush_due_to_byte_cap
        << " result_flush_due_to_age=" << exec.result_flush_due_to_age
        << " result_flush_due_to_idle=" << exec.result_flush_due_to_idle
        << " result_flush_due_to_error=" << exec.result_flush_due_to_error
        << " result_flush_due_to_shutdown=" << exec.result_flush_due_to_shutdown
        << " result_flush_while_delivery_pending_gt_8=" << exec.result_flush_while_delivery_pending_gt_8
        << " result_flush_while_delivery_pending_gt_32=" << exec.result_flush_while_delivery_pending_gt_32
        << " kafka_delivery_pending_current=" << exec.kafka_delivery_pending_current
        << " kafka_delivery_pending_max=" << exec.kafka_delivery_pending_max
        << " kafka_delivery_pending_over_8_events=" << exec.kafka_delivery_pending_over_8_events
        << " kafka_delivery_pending_over_32_events=" << exec.kafka_delivery_pending_over_32_events
        << " kafka_async_publisher_enabled=" << exec.kafka_async_publisher_enabled
        << " kafka_async_publisher_queue_max=" << exec.kafka_async_publisher_queue_max
        << std::endl;

    const uint64_t append_vector_calls =
        g_prof.append_vector_calls.load(std::memory_order_relaxed);
    const uint64_t append_vector_entries_total =
        g_prof.append_vector_entries_total.load(std::memory_order_relaxed);
    std::cout
        << "PROFILE_RAFT_ORDERER "
        << "arrivals=" << g_prof.orderer_arrivals.load(std::memory_order_relaxed)
        << " policy=" << profile_token(raft_ordering_policy())
        << " leader_assigned_requests=" << g_prof.orderer_leader_assigned_requests.load(std::memory_order_relaxed)
        << " leader_assigned_items=" << g_prof.orderer_leader_assigned_items.load(std::memory_order_relaxed)
        << " ordered_drains=" << g_prof.orderer_drains.load(std::memory_order_relaxed)
        << " pending_depth_max=" << g_prof.orderer_pending_depth_max.load(std::memory_order_relaxed)
        << " gap_wait_count=" << g_prof.orderer_gap_wait_count.load(std::memory_order_relaxed)
        << " gap_wait_ms=" << (g_prof.orderer_gap_wait_ns.load(std::memory_order_relaxed) / 1000000.0)
        << " append_vector_calls=" << append_vector_calls
        << " append_vector_entries_total=" << append_vector_entries_total
        << " append_vector_entries_max=" << g_prof.append_vector_entries_max.load(std::memory_order_relaxed)
        << " append_vector_entries_avg="
        << ((append_vector_calls > 0)
                ? (static_cast<double>(append_vector_entries_total) /
                   static_cast<double>(append_vector_calls))
                : 0.0)
        << " batch_target_entries=" << raft_ordered_batch_target_entries()
        << " batch_linger_us=" << raft_ordered_batch_linger_us()
        << " flush_reason_target=" << g_prof.orderer_flush_reason_target.load(std::memory_order_relaxed)
        << " flush_reason_linger=" << g_prof.orderer_flush_reason_linger.load(std::memory_order_relaxed)
        << " flush_reason_idle=" << g_prof.orderer_flush_reason_idle.load(std::memory_order_relaxed)
        << std::endl;

    if (smgr) {
        auto d_smgr = std::dynamic_pointer_cast<ariabc_raft::durable_state_mgr>(smgr);
        if (d_smgr) {
            auto lstore = std::dynamic_pointer_cast<ariabc_raft::durable_log_store>(d_smgr->load_log_store());
            if (lstore) {
                const auto& p = lstore->profile();
                const auto latency = lstore->latency_profile();
                const uint64_t fdatasync_calls = p.fdatasync_calls.load();
                const uint64_t append_batches = p.append_batches.load();
                const uint64_t append_batch_entries_total = p.append_batch_entries_total.load();
                const uint64_t bytes_appended = p.bytes_appended.load();
                std::cout << "PROFILE_RAFT_STORAGE "
                          << "append_calls=" << p.append_calls.load()
                          << " append_batches=" << append_batches
                          << " bytes_appended=" << bytes_appended
                          << " fdatasync_calls=" << fdatasync_calls
                          << " fdatasync_total_ms=" << (p.fdatasync_total_ns.load() / 1000000.0)
                          << " fdatasync_max_ms=" << (p.fdatasync_max_ns.load() / 1000000.0)
                          << " fdatasync_p50_ms=" << (latency.fdatasync_p50_ns / 1000000.0)
                          << " fdatasync_p95_ms=" << (latency.fdatasync_p95_ns / 1000000.0)
                          << " fdatasync_p99_ms=" << (latency.fdatasync_p99_ns / 1000000.0)
                          << " entries_per_fsync_avg="
                          << ((fdatasync_calls > 0)
                                  ? (static_cast<double>(append_batch_entries_total) /
                                     static_cast<double>(fdatasync_calls))
                                  : 0.0)
                          << " bytes_per_fsync_avg="
                          << ((fdatasync_calls > 0)
                                  ? (static_cast<double>(bytes_appended) /
                                     static_cast<double>(fdatasync_calls))
                                  : 0.0)
                          << " append_write_ms=" << (p.append_write_total_ns.load() / 1000000.0)
                          << " append_write_max_ms=" << (p.append_write_max_ns.load() / 1000000.0)
                          << " append_write_p50_ms=" << (latency.append_write_p50_ns / 1000000.0)
                          << " append_write_p95_ms=" << (latency.append_write_p95_ns / 1000000.0)
                          << " append_write_p99_ms=" << (latency.append_write_p99_ns / 1000000.0)
                          << " append_fsync_ms=" << (p.fdatasync_total_ns.load() / 1000000.0)
                          << " directory_fsync_ms=" << (p.directory_fsync_total_ns.load() / 1000000.0)
                          << " directory_fsync_max_ms=" << (p.directory_fsync_max_ns.load() / 1000000.0)
                          << " directory_fsync_p50_ms=" << (latency.directory_fsync_p50_ns / 1000000.0)
                          << " directory_fsync_p95_ms=" << (latency.directory_fsync_p95_ns / 1000000.0)
                          << " directory_fsync_p99_ms=" << (latency.directory_fsync_p99_ns / 1000000.0)
                          << " append_batch_entries_max=" << p.append_batch_entries_max.load()
                          << " append_batch_entries_total=" << append_batch_entries_total
                          << " segment_fdatasync_calls=" << p.segment_fdatasync_calls.load()
                          << " directory_fsync_calls=" << p.directory_fsync_calls.load()
                          << " segment_rollovers=" << p.segment_rollovers.load()
                          << " truncate_records_written=" << p.truncate_records_written.load()
                          << " tail_repairs=" << p.tail_repairs.load()
                          << " recovery_entries_loaded=" << p.recovery_entries_loaded.load()
                          << " async_flush_enabled=" << (lstore->async_flush_enabled() ? 1 : 0)
                          << " async_flush_jobs=" << p.async_flush_jobs.load()
                          << " async_flush_coalesced_jobs=" << p.async_flush_coalesced_jobs.load()
                          << " async_flush_notifications=" << p.async_flush_notifications.load()
                          << " async_flush_queue_max=" << p.async_flush_queue_max.load()
                          << " async_flush_waits=" << p.async_flush_waits.load()
                          << " last_durable_index=" << lstore->last_durable_index()
                          << std::endl;
            }
        }
    }
}

} // namespace ariabc_pg

int main(int argc, char** argv) {
    using namespace nuraft;
    using ariabc_pg::server_options;

    server_options opt;
    std::string err;
    if (!ariabc_pg::parse_args(argc, argv, opt, err)) {
        std::cerr << "Argument error: " << err << std::endl;
        ariabc_pg::usage(argv[0]);
        return 1;
    }

    // B1: Verify SHA-256 is functional at program startup.
    {
        unsigned char hash[SHA256_DIGEST_LENGTH];
        const char* test_data = "ariabc_test_hash";
        SHA256(reinterpret_cast<const unsigned char*>(test_data), strlen(test_data), hash);
        std::cout << "SHA-256 test: ";
        for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
            printf("%02x", hash[i]);
        }
        std::cout << std::endl;
    }

    // B1: Recovered durable storage + off: reject startup.
    if (opt.raft_apply_ledger_mode == "off" && !opt.raft_storage_dir.empty()) {
        struct stat st;
        std::string identity_p = opt.raft_storage_dir + "/identity.bin";
        if (::stat(identity_p.c_str(), &st) == 0) {
            std::cerr << "Startup rejected: Recovered durable storage found at "
                      << opt.raft_storage_dir << " but --raft-apply-ledger is set to 'off'."
                      << std::endl;
            return 1;
        }
    }

    // Ignore SIGPIPE so a broken client connection doesn't kill the server.
    ::signal(SIGPIPE, SIG_IGN);
    ::signal(SIGTERM, ariabc_pg::on_term);
    ::signal(SIGINT, ariabc_pg::on_term);

    // State machine (always needed: owns pg_executor + Kafka publisher).
    ptr<state_machine> sm =
        cs_new<ariabc_pg::pg_state_machine>(opt.id, opt.db, opt.kafka);

    if (opt.db.db_type == 1 && opt.db.raft_apply_ledger_mode != "safe") {
        ariabc_pg::pg_state_machine* psm =
            dynamic_cast<ariabc_pg::pg_state_machine*>(sm.get());
        if (psm) {
            psm->ensure_bcdb_initialized();
        }
    }

    if (opt.bypass_raft) {
        // ---- Bypass-Raft mode (kafka-only-no-raft profile) ----
        // Skip NuRaft entirely. The gateway broadcasts transactions to all
        // replicas in the same order; each replica executes deterministically
        // and publishes results to Kafka. The gateway collects Kafka results
        // and waits for threshold agreement.
        std::cout << "ariabc_pg_server ready (bypass-raft): id=" << opt.id
                  << " clientPort=" << opt.client_port
                  << std::endl;

        std::atomic<uint64_t> direct_seq{1};
        ariabc_pg::direct_orderer direct_order;
        ariabc_pg::pg_state_machine* psm =
            dynamic_cast<ariabc_pg::pg_state_machine*>(sm.get());

        const int listen_fd = ariabc_pg::listen_tcp(opt.client_port);
        ariabc_pg::g_listen_fd = listen_fd;
        while (!ariabc_pg::g_stop.load()) {
            sockaddr_in cli;
            socklen_t len = sizeof(cli);
            const int fd = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&cli), &len);
            if (fd < 0) {
                if (errno == EINTR) continue;
                if (ariabc_pg::g_stop.load()) break;
                std::cerr << "accept failed: " << ::strerror(errno) << std::endl;
                continue;
            }
            std::thread th([fd, psm, &direct_seq, &direct_order] {
                ariabc_pg::handle_client_fd_direct(fd, psm, direct_seq, direct_order);
            });
            th.detach();
        }

        ariabc_pg::dump_profile(nullptr, sm);
        return 0;
    }

    // ---- Normal Raft mode ----
    std::vector<ariabc_pg::raft_member> members;
    try {
        members = ariabc_pg::parse_raft_members(opt);
    } catch (const std::exception& e) {
        std::cerr << "Raft members error: " << e.what() << std::endl;
        return 1;
    }

    // Logger.
    const std::string log_file = "./ariabc_pg_srv" + std::to_string(opt.id) + ".log";
    ptr<logger> raft_logger = cs_new<logger_wrapper>(log_file, 4);

    // State manager + initial config.
    ptr<state_mgr> smgr;
    const int preferred_leader_id = ariabc_pg::preferred_leader_id_from_env();
    const int preferred_leader_transfer_wait_ms =
        ariabc_pg::preferred_leader_transfer_wait_ms_from_env();

    try {
        if (opt.raft_storage_mode == "durable") {
            ariabc_raft::durable_state_mgr_config ds_cfg;
            ds_cfg.storage_dir = opt.raft_storage_dir;
            ds_cfg.node_id = opt.id;
            ds_cfg.endpoint = opt.raft_endpoint;
            ds_cfg.cluster_id = opt.raft_cluster_id;
            ds_cfg.raft_epoch_hex = opt.raft_epoch_hex;

            std::cout << "[Raft Storage] Reopening durable storage at " << opt.raft_storage_dir << std::endl;
            auto start_recover = std::chrono::high_resolution_clock::now();
            auto d_smgr = cs_new<ariabc_raft::durable_state_mgr>(ds_cfg);
            auto end_recover = std::chrono::high_resolution_clock::now();
            double ms = std::chrono::duration<double, std::milli>(end_recover - start_recover).count();

            smgr = d_smgr;

            if (!d_smgr->is_recovered()) {
                char* strict_preserve_env = ::getenv("ARIABC_STRICT_PRESERVE");
                if (strict_preserve_env && std::string(strict_preserve_env) == "1") {
                    throw std::runtime_error("ARIABC_STRICT_PRESERVE_FAILED: Storage directory is not recovered, but strict preserve mode is enabled.");
                }
                std::cout << "[Raft Storage] Fresh storage initialized. Initializing fresh cluster." << std::endl;
                ptr<cluster_config> conf = cs_new<cluster_config>();
                for (const auto& m : members) {
                    const int priority = (preferred_leader_id > 0 && m.id == preferred_leader_id) ? 100 : 1;
                    conf->get_servers().push_back(cs_new<srv_config>(m.id,
                                                                     0,
                                                                     m.endpoint,
                                                                     "",
                                                                     false,
                                                                     priority));
                }
                d_smgr->initialize_fresh(*conf);
            } else {
                // Verify stored membership matches CLI --raftMembers
                auto saved_conf = d_smgr->load_config();
                if (!saved_conf) {
                    throw std::runtime_error("RAFT_STORAGE_MEMBERSHIP_MISMATCH: recovered config is null");
                }
                auto& saved_servers = saved_conf->get_servers();
                if (saved_servers.size() != members.size()) {
                    throw std::runtime_error("RAFT_STORAGE_MEMBERSHIP_MISMATCH: stored config has " +
                                             std::to_string(saved_servers.size()) + " members, but CLI specifies " +
                                             std::to_string(members.size()));
                }
                for (const auto& m : members) {
                    bool found = false;
                    for (const auto& s : saved_servers) {
                        if (s->get_id() == m.id) {
                            if (s->get_endpoint() != m.endpoint) {
                                throw std::runtime_error("RAFT_STORAGE_MEMBERSHIP_MISMATCH: member ID " +
                                                         std::to_string(m.id) + " has stored endpoint " +
                                                         s->get_endpoint() + " but CLI specifies " + m.endpoint);
                            }
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw std::runtime_error("RAFT_STORAGE_MEMBERSHIP_MISMATCH: member ID " +
                                                 std::to_string(m.id) + " present in CLI but not in stored config");
                    }
                }

                auto lstore = std::dynamic_pointer_cast<ariabc_raft::durable_log_store>(d_smgr->load_log_store());
                if (lstore) {
                    std::cout << "[Raft Storage] Recovery scan complete in " << ms << " ms. "
                              << "Start index: " << lstore->start_index()
                              << ", Next index: " << lstore->next_slot()
                              << ", Last entry term: " << lstore->last_entry()->get_term()
                              << ", Last durable index: " << lstore->last_durable_index()
                              << std::endl;
                }
            }
        } else {
            std::cout << "WARNING: RAFT STORAGE MODE = in_memory; Raft state is not crash durable." << std::endl;
            smgr = cs_new<inmem_state_mgr>(opt.id, opt.raft_endpoint);
            ptr<cluster_config> conf = cs_new<cluster_config>();
            for (const auto& m : members) {
                const int priority = (preferred_leader_id > 0 && m.id == preferred_leader_id) ? 100 : 1;
                conf->get_servers().push_back(cs_new<srv_config>(m.id,
                                                                 0,
                                                                 m.endpoint,
                                                                 "",
                                                                 false,
                                                                 priority));
            }
            smgr->save_config(*conf);
        }
    } catch (const std::exception& e) {
        std::cerr << "RAFT_STORAGE_FATAL: " << e.what() << std::endl;
        return 1;
    }

    // ASIO options.
    asio_service::options asio_opt;
    asio_opt.thread_pool_size_ = 8;

    // Raft parameters.
    raft_params params;
#if defined(WIN32) || defined(_WIN32)
    params.heart_beat_interval_ = 1000;
    params.election_timeout_lower_bound_ = 2000;
    params.election_timeout_upper_bound_ = 4000;
#else
    // More forgiving defaults for busy local dev machines:
    // - Avoid spurious elections under CPU load.
    // - Avoid yielding leadership due to brief scheduling stalls.
    params.heart_beat_interval_ = 250;
    params.election_timeout_lower_bound_ = 5000;
    params.election_timeout_upper_bound_ = 10000;
#endif
    params.reserved_log_items_ = 5;
    params.snapshot_distance_ = 0; // disable snapshots/compaction (DB snapshot semantics out of scope)
    // Blocking append_entries should tolerate transient load spikes.
    params.client_req_timeout_ = 60000;
    // 0 => default is 20x heartbeat, which can be too aggressive under load.
    params.leadership_expiry_ = 120000;
    if (preferred_leader_id > 0) {
        params.leadership_transfer_min_wait_time_ = preferred_leader_transfer_wait_ms;
    }
    /*
     * Safe ledger mode must not ACK a client request from NuRaft's async
     * placeholder result.  In async_handler mode append_entries() can return
     * RESULT_NOT_EXIST_YET before the log is durably stored/committed; the
     * gateway then waits forever on a raft_log_idx that was only observed from
     * leader-local state.  Blocking mode preserves the safe contract: ACCEPTED
     * means the leader committed the entry and the durable store flushed it.
     */
    params.return_method_ =
        (opt.db.db_type == 1 && opt.db.raft_apply_ledger_mode == "safe")
            ? raft_params::blocking
            : raft_params::async_handler;
    params.auto_forwarding_ = true;
    const bool durable_async_flush =
        (opt.raft_storage_mode == "durable") &&
        ariabc_pg::env_flag_enabled("ARIABC_RAFT_DURABLE_ASYNC_FLUSH", true);
    if (opt.raft_storage_mode == "durable") {
        params.parallel_log_appending_ = durable_async_flush;
    }
    const uint64_t configured_stream_gap =
        ariabc_pg::env_u64("ARIABC_RAFT_STREAM_GAP",
                           durable_async_flush ? 512 : 0);
    if (configured_stream_gap > 0) {
        params.max_log_gap_in_stream_ =
            static_cast<int32>(std::min<uint64_t>(configured_stream_gap, 1000000ULL));
        params.max_bytes_in_flight_in_stream_ =
            static_cast<int64_t>(ariabc_pg::env_u64("ARIABC_RAFT_STREAM_BYTES", 0));
        asio_opt.streaming_mode_ = true;
    }

    // P0 #14: In safe mode, run synchronous startup validation and prefix recovery
    // AFTER durable Raft state manager is open and validated,
    // BEFORE Raft server starts delivering commit()s.
    if (opt.db.db_type == 1 && opt.db.raft_apply_ledger_mode == "safe") {
        ariabc_pg::pg_state_machine* psm =
            dynamic_cast<ariabc_pg::pg_state_machine*>(sm.get());
        if (!psm) {
            std::cerr << "SAFE_STARTUP_FAILED: sm is not a pg_state_machine" << std::endl;
            return 1;
        }

        // Determine the start index from durable log store (if available)
        uint64_t durable_log_start = 0;
        if (opt.raft_storage_mode == "durable") {
            auto d_smgr = std::dynamic_pointer_cast<ariabc_raft::durable_state_mgr>(smgr);
            if (d_smgr) {
                auto lstore = std::dynamic_pointer_cast<ariabc_raft::durable_log_store>(
                    d_smgr->load_log_store());
                if (lstore) {
                    durable_log_start = lstore->start_index();
                }
            }
        }

        try {
            const uint64_t seeded = psm->safe_sync_startup(durable_log_start);
            std::cout << "[safe startup] prefix seeded at " << seeded
                      << ", durable_log_start=" << durable_log_start << std::endl;
            if (!psm->ensure_bcdb_initialized()) {
                std::cerr << "SAFE_STARTUP_FAILED: bcdb_init failed\n";
                return 1;
            }
        } catch (const std::exception& e) {
            std::cerr << "FATAL: " << e.what() << std::endl;
            return 1;
        }
    }

    // Initialize Raft server listening on raftEndpoint port.
    const ariabc_pg::host_port raft_hp = ariabc_pg::parse_host_port(opt.raft_endpoint);
    raft_launcher launcher;
    ptr<raft_server> raft = launcher.init(sm, smgr, raft_logger,
                                          raft_hp.port, asio_opt, params);
    if (!raft) {
        std::cerr << "Failed to init raft server (see log: " << log_file << ")" << std::endl;
        return 1;
    }
    if (durable_async_flush) {
        auto d_smgr = std::dynamic_pointer_cast<ariabc_raft::durable_state_mgr>(smgr);
        if (d_smgr) {
            auto lstore = std::dynamic_pointer_cast<ariabc_raft::durable_log_store>(
                d_smgr->load_log_store());
            if (lstore) {
                lstore->enable_async_flush(raft.get());
            }
        }
    }

    // Wait for initialization.
    // With longer election timeouts (for stability under load), initialization can
    // legitimately take >5s on local dev machines. Use a wall-clock deadline.
    const auto init_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < init_deadline) {
        if (raft->is_initialized()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (!raft->is_initialized()) {
        std::cerr << "Raft initialization timeout" << std::endl;
        return 1;
    }

    std::cout << "ariabc_pg_server ready: id=" << opt.id
              << " raft=" << opt.raft_endpoint
              << " clientPort=" << opt.client_port
              << " members=" << members.size()
              << " preferredLeaderId=" << preferred_leader_id
              << " leaderTransferWaitMs=" << (preferred_leader_id > 0 ? preferred_leader_transfer_wait_ms : 0)
              << std::endl;

    if (preferred_leader_id == opt.id) {
        ptr<raft_server> preferred_raft = raft;
        std::thread([preferred_raft, preferred_leader_id] {
            for (int attempt = 1; attempt <= 40 && !ariabc_pg::g_stop.load(); ++attempt) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                const int leader = preferred_raft->get_leader();
                if (preferred_raft->is_leader()) {
                    std::cerr << "preferred_leader active id=" << preferred_leader_id
                              << " attempt=" << attempt << std::endl;
                    return;
                }
                if (leader > 0 && leader != preferred_leader_id) {
                    const bool requested = preferred_raft->request_leadership();
                    std::cerr << "preferred_leader request id=" << preferred_leader_id
                              << " current_leader=" << leader
                              << " attempt=" << attempt
                              << " requested=" << (requested ? 1 : 0)
                              << std::endl;
                }
            }
        }).detach();
    }

    const int listen_fd = ariabc_pg::listen_tcp(opt.client_port);
    ariabc_pg::g_listen_fd = listen_fd;
    std::shared_ptr<ariabc_pg::raft_orderer> raft_order(new ariabc_pg::raft_orderer());
    while (!ariabc_pg::g_stop.load()) {
        sockaddr_in cli;
        socklen_t len = sizeof(cli);
        const int fd = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&cli), &len);
        if (fd < 0) {
            if (errno == EINTR) continue;
            if (ariabc_pg::g_stop.load()) break;
            std::cerr << "accept failed: " << ::strerror(errno) << std::endl;
            continue;
        }
        std::thread th([fd, raft, sm, raft_order] {
            ariabc_pg::handle_client_fd(fd, raft, sm, raft_order);
        });
        th.detach();
    }

    ariabc_pg::dump_profile(raft, sm, smgr);

    return 0;
}
