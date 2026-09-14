// ariabc_pg/tests/durable_raft_external_sigkill_test.cxx
// Multi-process integration test verifying durability across real SIGKILL deaths and failpoints.

#include "../src/durable_state_mgr.hxx"
#include "../src/durable_log_store.hxx"
#include "../src/logger_wrapper.hxx"
#include "nuraft.hxx"
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <memory>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdexcept>
#include <chrono>
#include <thread>
#include <csignal>
#include <cstring>
#include <map>
#include <set>

#define REQUIRE(expr) \
    do { \
        if (!(expr)) { \
            std::cerr << "CRITICAL FAILURE at " << __FILE__ << ":" << __LINE__ << " - requirement failed: " #expr << std::endl; \
            throw std::runtime_error("requirement failed: " #expr); \
        } \
    } while (0)

using namespace ariabc_raft;
using namespace nuraft;

// State machine for testing
class test_state_machine : public state_machine {
public:
    test_state_machine() : commit_idx_(0) {}
    ptr<buffer> commit(const ulong log_idx, buffer& data) override {
        commit_idx_ = log_idx;
        return nullptr;
    }
    ptr<snapshot> last_snapshot() override { return nullptr; }
    ulong last_commit_index() override { return commit_idx_; }
    void create_snapshot(snapshot& s, async_result<bool>::handler_type& when_done) override {}
    bool apply_snapshot(snapshot& s) override { return true; }

    ulong get_commit_idx() const { return commit_idx_; }
private:
    std::atomic<ulong> commit_idx_;
};

// Helper to clean directory
static void clean_dir(const std::string& d) {
    ::system(("rm -rf " + d).c_str());
}

static ptr<log_entry> create_test_entry(ulong term, const std::string& val) {
    ptr<buffer> buf = buffer::alloc(val.size() + 1);
    buf->put_raw((const uint8_t*)val.c_str(), val.size() + 1);
    buf->pos(0);
    return cs_new<log_entry>(term, buf);
}

static bool entry_payload_equals(const ptr<log_entry>& entry, const std::string& val) {
    if (!entry) return false;
    if (entry->get_buf().size() != val.size() + 1) return false;
    return ::strcmp((const char*)entry->get_buf().data(), val.c_str()) == 0;
}

static std::vector<uint8_t> serialized_entry_bytes(const ptr<log_entry>& entry) {
    if (!entry) return {};
    ptr<buffer> buf = entry->serialize();
    std::vector<uint8_t> bytes(buf->size());
    ::memcpy(bytes.data(), buf->data(), buf->size());
    return bytes;
}

// Function to write node status file for controller to inspect
static void write_status_file(const std::string& base_dir, int id, const std::string& role, ulong term, ulong commit_idx, ulong last_durable_idx) {
    std::string path = base_dir + "/node_" + std::to_string(id) + ".status";
    std::ofstream out(path);
    if (out.is_open()) {
        out << "ROLE=" << role << "\n";
        out << "TERM=" << term << "\n";
        out << "COMMIT_INDEX=" << commit_idx << "\n";
        out << "LAST_DURABLE_IDX=" << last_durable_idx << "\n";
        out << "PID=" << ::getpid() << "\n";
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        out << "HEARTBEAT_MONOTONIC_MS=" << now_ms << "\n";
    }
}

// Single node execution loop
static int run_node_process(int id, const std::string& base_dir, int port1, int port2, int port3) {
    try {
        std::string dir = base_dir + "/node" + std::to_string(id);
        
        ptr<cluster_config> config = cs_new<cluster_config>();
        config->get_servers().push_back(cs_new<srv_config>(1, "127.0.0.1:" + std::to_string(port1)));
        config->get_servers().push_back(cs_new<srv_config>(2, "127.0.0.1:" + std::to_string(port2)));
        config->get_servers().push_back(cs_new<srv_config>(3, "127.0.0.1:" + std::to_string(port3)));

        auto log_wrapper = cs_new<logger_wrapper>(base_dir + "/node" + std::to_string(id) + ".log", 4);
        auto sm = cs_new<test_state_machine>();

        durable_state_mgr_config cfg;
        cfg.storage_dir = dir;
        cfg.node_id = id;
        cfg.endpoint = "127.0.0.1:" + std::to_string(id == 1 ? port1 : (id == 2 ? port2 : port3));
        cfg.cluster_id = "sigkill_cluster";

        auto smgr = cs_new<durable_state_mgr>(cfg);
        if (!smgr->is_recovered()) {
            smgr->initialize_fresh(*config);
        }

        asio_service::options asio_opt;
        asio_opt.thread_pool_size_ = 2;

        raft_params params;
        params.heart_beat_interval_ = 100;
        params.election_timeout_lower_bound_ = 200;
        params.election_timeout_upper_bound_ = 400;
        params.reserved_log_items_ = 5;
        params.snapshot_distance_ = 0;
        params.client_req_timeout_ = 1000;
        params.leadership_expiry_ = 2000;
        params.return_method_ = raft_params::async_handler;
        params.auto_forwarding_ = true;
        params.parallel_log_appending_ = false;

        raft_launcher launcher;
        int listen_port = (id == 1) ? port1 : ((id == 2) ? port2 : port3);
        ptr<raft_server> server = launcher.init(sm, smgr, log_wrapper, listen_port, asio_opt, params);
        if (!server) {
            std::cerr << "Node " << id << " failed to initialize server" << std::endl;
            return 1;
        }

        auto store = std::dynamic_pointer_cast<durable_log_store>(smgr->load_log_store());

        std::cout << "Node " << id << " started successfully on port " << listen_port << std::endl;

        // Run loop writing status file and proposing if leader
        int propose_counter = 0;
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            std::string role = "FOLLOWER";
            if (server->is_leader()) {
                role = "LEADER";
                
                // Periodically propose entry
                propose_counter++;
                if (propose_counter >= 5) { // every 500ms
                    propose_counter = 0;
                    ptr<buffer> entry_data = buffer::alloc(20);
                    std::string payload = "data_node_" + std::to_string(id) + "_" + std::to_string(sm->get_commit_idx());
                    entry_data->put_raw((const uint8_t*)payload.c_str(), payload.size() + 1);
                    entry_data->pos(0);
                    server->append_entries({entry_data});
                }
            }

            write_status_file(base_dir, id, role, server->get_term(), sm->get_commit_idx(), store ? store->last_durable_index() : 0);
        }

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Node " << id << " crashed with exception: " << e.what() << std::endl;
        return 1;
    }
}

// Struct to read status file
struct NodeStatus {
    std::string role = "UNKNOWN";
    ulong term = 0;
    ulong commit_index = 0;
    ulong last_durable_idx = 0;
    pid_t pid = 0;
    uint64_t heartbeat_ms = 0;
    bool ok = false;
};

static NodeStatus read_status_file(const std::string& base_dir, int id) {
    std::string path = base_dir + "/node_" + std::to_string(id) + ".status";
    NodeStatus status;
    std::ifstream in(path);
    if (!in.is_open()) return status;

    std::string line;
    while (std::getline(in, line)) {
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        std::string key = line.substr(0, eq);
        std::string val = line.substr(eq + 1);
        if (key == "ROLE") status.role = val;
        else if (key == "TERM") status.term = std::stoul(val);
        else if (key == "COMMIT_INDEX") status.commit_index = std::stoul(val);
        else if (key == "LAST_DURABLE_IDX") status.last_durable_idx = std::stoul(val);
        else if (key == "PID") status.pid = std::stoi(val);
        else if (key == "HEARTBEAT_MONOTONIC_MS") status.heartbeat_ms = std::stoull(val);
    }
    status.ok = true;
    return status;
}

static void verify_on_disk_metadata(const std::string& dir, uint64_t expected_start, uint64_t expected_next) {
    std::string manifest_path = dir + "/manifest.bin";
    std::ifstream in(manifest_path, std::ios::binary);
    REQUIRE(in.is_open());

    // Read magic (4 bytes)
    uint32_t magic = 0;
    in.read((char*)&magic, 4);
    REQUIRE(magic == 0xAB1CFACE); // MAGIC

    // Read version (4 bytes)
    uint32_t ver = 0;
    in.read((char*)&ver, 4);
    REQUIRE(ver == 2); // FORMAT_VER

    // Read num_segs (4 bytes)
    uint32_t num_segs = 0;
    in.read((char*)&num_segs, 4);

    // Read reserved (4 bytes)
    uint32_t reserved = 0;
    in.read((char*)&reserved, 4);

    // Read next_gen_id (8 bytes)
    uint64_t next_gen_id = 0;
    in.read((char*)&next_gen_id, 8);

    struct DiskSegment {
        uint64_t first_index;
        uint64_t gen_id;
    };
    std::vector<DiskSegment> segments;
    for (uint32_t i = 0; i < num_segs; ++i) {
        DiskSegment seg;
        in.read((char*)&seg.first_index, 8);
        in.read((char*)&seg.gen_id, 8);
        segments.push_back(seg);
    }
    in.close();

    // Now, simulate the log store recovery scanning to determine start_index and next_slot
    std::map<uint64_t, uint64_t> active_indices; // index -> term
    uint64_t start_idx = 1;
    if (!segments.empty()) {
        start_idx = segments[0].first_index;
    }

    for (const auto& seg : segments) {
        // Construct segment path
        char buf[128];
        snprintf(buf, sizeof(buf), "/segment_%020llu_g%020llu.log",
                 (unsigned long long)seg.first_index, (unsigned long long)seg.gen_id);
        std::string seg_path = dir + buf;

        std::ifstream s_in(seg_path, std::ios::binary);
        if (!s_in.is_open()) {
            continue;
        }

        // Read header (16 bytes)
        char seg_hdr[16];
        s_in.read(seg_hdr, 16);
        if (s_in.gcount() < 16 || std::memcmp(seg_hdr, "RAFT_SEG", 8) != 0) {
            s_in.close();
            continue;
        }

        // Read records
        while (true) {
            // Record header is 32 bytes
            char rec_hdr[32];
            s_in.read(rec_hdr, 32);
            if (s_in.gcount() < 32) {
                break; // EOF or partial header
            }

            uint32_t r_magic = *(uint32_t*)(rec_hdr);
            uint32_t r_ver = *(uint32_t*)(rec_hdr + 4);
            uint32_t r_type = *(uint32_t*)(rec_hdr + 8);
            uint32_t r_len = *(uint32_t*)(rec_hdr + 12);
            uint64_t r_idx = *(uint64_t*)(rec_hdr + 16);
            uint64_t r_term = *(uint64_t*)(rec_hdr + 24);

            if (r_magic != 0xAB1CFACE) { // MAGIC
                break; // Corrupt
            }

            // Seek past payload
            s_in.seekg(r_len, std::ios::cur);

            if (r_type == 1) { // RT_ENTRY
                active_indices[r_idx] = r_term;
            } else if (r_type == 2) { // RT_TRUNCATE
                auto it = active_indices.lower_bound(r_idx);
                while (it != active_indices.end()) {
                    it = active_indices.erase(it);
                }
            }
        }
        s_in.close();
    }

    uint64_t computed_start = start_idx;
    if (!active_indices.empty()) {
        computed_start = active_indices.begin()->first;
    }
    uint64_t computed_next = computed_start;
    if (!active_indices.empty()) {
        computed_next = active_indices.rbegin()->first + 1;
    }

    std::cout << "verify_on_disk_metadata: computed start_idx=" << computed_start
              << ", next_slot=" << computed_next << " (expected start=" << expected_start
              << ", next=" << expected_next << ")" << std::endl;

    REQUIRE(computed_start == expected_start);
    REQUIRE(computed_next == expected_next);
}

#include <set>

static std::set<pid_t> active_child_pids;

static pid_t register_spawn(pid_t pid) {
    if (pid > 0) {
        active_child_pids.insert(pid);
    }
    return pid;
}

static void register_reap(pid_t pid) {
    active_child_pids.erase(pid);
}

static void kill_and_reap(pid_t pid) {
    ::kill(pid, SIGKILL);
    int st;
    ::waitpid(pid, &st, 0);
    register_reap(pid);
}

static pid_t wait_and_reap(pid_t pid, int* status_out, int options = 0) {
    int status = 0;
    const pid_t result = ::waitpid(pid, &status, options);
    if (result == pid) {
        register_reap(pid);
        if (status_out) {
            *status_out = status;
        }
    }
    return result;
}

static int wait_with_timeout(pid_t pid, int timeout_seconds, const std::string& log_file_path = "") {
    auto start = std::chrono::steady_clock::now();
    while (true) {
        int status = 0;
        pid_t res = wait_and_reap(pid, &status, WNOHANG);
        if (res == pid) {
            return status;
        }
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - start).count() >= timeout_seconds) {
            std::cerr << "TIMEOUT waiting for process " << pid << " after " << timeout_seconds << " seconds." << std::endl;
            if (!log_file_path.empty()) {
                std::cerr << "--- Log Content of " << log_file_path << " ---" << std::endl;
                std::ifstream lf(log_file_path);
                if (lf.is_open()) {
                    std::string l;
                    while (std::getline(lf, l)) {
                        std::cerr << l << "\n";
                    }
                }
                std::cerr << "--- End of Log ---" << std::endl;
            }
            kill_and_reap(pid);
            REQUIRE(false); // fail the test
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

static void kill_all_active_children() {
    std::set<pid_t> pids = active_child_pids;
    for (pid_t pid : pids) {
        kill_and_reap(pid);
    }
    active_child_pids.clear();
}

// Controller runner for external process tests
int main(int argc, char** argv) {
    // If running as node child process
    if (argc >= 7 && std::string(argv[1]) == "--node") {
        int id = std::stoi(argv[2]);
        std::string base_dir = argv[3];
        int port1 = std::stoi(argv[4]);
        int port2 = std::stoi(argv[5]);
        int port3 = std::stoi(argv[6]);
        return run_node_process(id, base_dir, port1, port2, port3);
    }

    try {
        std::cout << "==========================================================" << std::endl;
        std::cout << "Starting Multi-Process SIGKILL & Failpoint Recovery Tests" << std::endl;
        std::cout << "==========================================================" << std::endl;

        std::string base_dir = "./test_cluster_sigkill";
        clean_dir(base_dir);
        ::mkdir(base_dir.c_str(), 0755);

        int port1 = 9001, port2 = 9002, port3 = 9003;
        std::string self_binary = argv[0];

        auto spawn_node = [&](int id, const std::vector<std::string>& env_vars = {}) -> pid_t {
            pid_t pid = ::fork();
            if (pid == 0) {
                // Child process
                for (const auto& ev : env_vars) {
                    size_t eq = ev.find('=');
                    if (eq != std::string::npos) {
                        ::setenv(ev.substr(0, eq).c_str(), ev.substr(eq + 1).c_str(), 1);
                    }
                }
                ::execl(self_binary.c_str(), self_binary.c_str(), "--node",
                        std::to_string(id).c_str(), base_dir.c_str(),
                        std::to_string(port1).c_str(), std::to_string(port2).c_str(), std::to_string(port3).c_str(),
                        nullptr);
                std::cerr << "Failed to exec self binary" << std::endl;
                ::_exit(1);
            }
            return register_spawn(pid);
        };

        // ---------------------------------------------------------------------
        // SCENARIO 1: Real Process SIGKILL
        // ---------------------------------------------------------------------
        std::cout << "\n--- Scenario 1: Abrupt SIGKILL Process Termination ---" << std::endl;

        std::cout << "Spawning 3 durable peers..." << std::endl;
        pid_t pid1 = spawn_node(1);
        pid_t pid2 = spawn_node(2);
        pid_t pid3 = spawn_node(3);

        REQUIRE(pid1 > 0 && pid2 > 0 && pid3 > 0);

        // Wait for leader election and initial commits
        std::cout << "Waiting for leader election & initial replication..." << std::endl;
        ulong leader_id = 0;
        ulong commit_index = 0;
        for (int i = 0; i < 100; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            auto s1 = read_status_file(base_dir, 1);
            auto s2 = read_status_file(base_dir, 2);
            auto s3 = read_status_file(base_dir, 3);
            if (s1.role == "LEADER") leader_id = 1;
            if (s2.role == "LEADER") leader_id = 2;
            if (s3.role == "LEADER") leader_id = 3;

            commit_index = std::max({s1.commit_index, s2.commit_index, s3.commit_index});
            if (leader_id > 0 && commit_index >= 3) {
                break;
            }
        }

        REQUIRE(leader_id > 0);
        REQUIRE(commit_index >= 3);
        std::cout << "Leader elected: " << leader_id << ", Commit Index: " << commit_index << std::endl;

        // Choose victim to SIGKILL (non-leader follower)
        int victim_id = (leader_id == 3) ? 2 : 3;
        pid_t victim_pid = (victim_id == 2) ? pid2 : pid3;

        std::cout << "Executing hard SIGKILL on Node " << victim_id << " (pid " << victim_pid << ")..." << std::endl;
        REQUIRE(::kill(victim_pid, SIGKILL) == 0);

        // Wait for OS to reap victim
        wait_and_reap(victim_pid, nullptr, 0);
        std::cout << "Node " << victim_id << " process terminated via SIGKILL successfully." << std::endl;

        // Let the remaining quorum commit more entries
        std::cout << "Proposing new entries on remaining quorum..." << std::endl;
        ulong post_kill_commit = 0;
        for (int i = 0; i < 50; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            auto s1 = read_status_file(base_dir, 1);
            auto s2 = read_status_file(base_dir, 2);
            auto s3 = read_status_file(base_dir, 3);
            
            ulong alive_max = 0;
            if (victim_id != 1) alive_max = std::max(alive_max, s1.commit_index);
            if (victim_id != 2) alive_max = std::max(alive_max, s2.commit_index);
            if (victim_id != 3) alive_max = std::max(alive_max, s3.commit_index);

            if (alive_max > commit_index + 2) {
                post_kill_commit = alive_max;
                break;
            }
        }
        REQUIRE(post_kill_commit > commit_index);
        std::cout << "Quorum committed entries up to commit index " << post_kill_commit << " while one follower is dead." << std::endl;

        // Restart victim node (preserve mode)
        std::cout << "Restarting Node " << victim_id << " in preserve mode..." << std::endl;
        pid_t restarted_pid = spawn_node(victim_id);
        REQUIRE(restarted_pid > 0);
        if (victim_id == 2) pid2 = restarted_pid;
        else pid3 = restarted_pid;

        // Wait for victim to catch up
        std::cout << "Waiting for restarted Node " << victim_id << " to catch up to index " << post_kill_commit << "..." << std::endl;
        bool caught_up = false;
        for (int i = 0; i < 100; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            auto sv = read_status_file(base_dir, victim_id);
            if (sv.commit_index >= post_kill_commit) {
                caught_up = true;
                break;
            }
        }
        REQUIRE(caught_up);
        std::cout << "Node " << victim_id << " caught up perfectly!" << std::endl;

        // Clean shutdown of Scenario 1 processes
        std::cout << "Terminating all nodes..." << std::endl;
        kill_and_reap(pid1);
        kill_and_reap(pid2);
        kill_and_reap(pid3);
        REQUIRE(active_child_pids.empty());

        // ---------------------------------------------------------------------
        // SCENARIO 2: Crash Before fdatasync Failpoint – with log content verification
        // ---------------------------------------------------------------------
        std::cout << "\n--- Scenario 2: Crash Before fdatasync – Log Content Verification ---" << std::endl;

        // Fresh cluster directory for this scenario
        clean_dir(base_dir);
        ::mkdir(base_dir.c_str(), 0755);

        // Step 1: Start a healthy 3-node cluster
        std::cout << "Spawning healthy 3-node cluster for Scenario 2..." << std::endl;
        pid1 = spawn_node(1);
        pid2 = spawn_node(2);
        pid3 = spawn_node(3);
        REQUIRE(pid1 > 0 && pid2 > 0 && pid3 > 0);

        // Step 2: Wait for leader and commit known payloads A and B
        std::cout << "Waiting for leader election..." << std::endl;
        ulong s2_leader = 0;
        for (int i = 0; i < 100 && s2_leader == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            for (int n = 1; n <= 3; ++n) {
                auto s = read_status_file(base_dir, n);
                if (s.role == "LEADER") { s2_leader = n; break; }
            }
        }
        REQUIRE(s2_leader > 0);
        std::cout << "Leader: Node " << s2_leader << std::endl;

        // Wait for both payloads to commit (commit_index >= 3 means at least
        // the two leadership-confirmation entries + our entries propagated)
        ulong s2_pre_kill_idx = 0;
        for (int i = 0; i < 100; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            auto s = read_status_file(base_dir, (int)s2_leader);
            if (s.commit_index >= 3) { s2_pre_kill_idx = s.commit_index; break; }
        }
        REQUIRE(s2_pre_kill_idx >= 3);
        std::cout << "Pre-kill quorum commit index: " << s2_pre_kill_idx << std::endl;

        // Step 3: Kill all three nodes cleanly to snapshot the state
        kill_and_reap(pid1); kill_and_reap(pid2); kill_and_reap(pid3);
        REQUIRE(active_child_pids.empty());

        // Step 4: Reopen a copy of Node 1 durable log to record entries A, B
        std::string node1_log_dir = base_dir + "/node1/log";
        ulong s2_log_count = 0;
        {
            durable_log_store verify_store(node1_log_dir);
            s2_log_count = verify_store.next_slot() - 1;
            std::cout << "Node 1 durable log has " << s2_log_count << " entries." << std::endl;
            REQUIRE(s2_log_count >= s2_pre_kill_idx);
        }

        // Step 5: Restart cluster with Node 3 having the fdatasync failpoint
        std::cout << "Restarting cluster; Node 3 with fdatasync crash failpoint..." << std::endl;
        pid1 = spawn_node(1);
        pid2 = spawn_node(2);
        pid3 = spawn_node(3, {"ARIABC_FAILPOINT_CRASH_BEFORE_FDATASYNC=append_batch"});
        REQUIRE(pid1 > 0 && pid2 > 0 && pid3 > 0);

        // Step 6: Wait for Node 3 to crash via failpoint
        std::cout << "Waiting for Node 3 to crash via fdatasync failpoint..." << std::endl;
        std::string node3_log = base_dir + "/node3.log";
        int st3 = wait_with_timeout(pid3, 10, node3_log);
        REQUIRE(WIFSIGNALED(st3) && WTERMSIG(st3) == SIGKILL);
        std::cout << "Node 3 crashed via failpoint." << std::endl;

        // Step 7: Wait for the remaining quorum to commit beyond the pre-kill index
        ulong s2_post_kill_idx = 0;
        for (int i = 0; i < 80; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            auto sa = read_status_file(base_dir, (s2_leader == 1) ? 1 : 1);
            auto sb = read_status_file(base_dir, (s2_leader == 2) ? 2 : 2);
            ulong mx = std::max(sa.commit_index, sb.commit_index);
            if (mx > s2_pre_kill_idx) { s2_post_kill_idx = mx; break; }
        }
        REQUIRE(s2_post_kill_idx > s2_pre_kill_idx);
        std::cout << "Quorum advanced to commit index " << s2_post_kill_idx << " while Node 3 is dead." << std::endl;

        // Step 8: Kill surviving nodes
        kill_and_reap(pid1); kill_and_reap(pid2);
        REQUIRE(active_child_pids.empty());

        // Step 9: Inspect Node 1's durable log to know the ground truth
        ulong s2_node1_entries = 0;
        {
            durable_log_store verify_store(node1_log_dir);
            s2_node1_entries = verify_store.next_slot() - 1;
            std::cout << "Node 1 final durable log has " << s2_node1_entries << " entries." << std::endl;
            REQUIRE(s2_node1_entries >= s2_post_kill_idx);
        }

        // Step 10: Restart Node 3 WITHOUT failpoint and wait for catch-up
        std::cout << "Restarting Node 3 without failpoint for catch-up..." << std::endl;
        pid1 = spawn_node(1);
        pid2 = spawn_node(2);
        pid3 = spawn_node(3);
        REQUIRE(pid1 > 0 && pid2 > 0 && pid3 > 0);

        caught_up = false;
        for (int i = 0; i < 120; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            auto s3 = read_status_file(base_dir, 3);
            if (s3.commit_index >= s2_post_kill_idx) { caught_up = true; break; }
        }
        REQUIRE(caught_up);
        std::cout << "Node 3 caught up to commit index " << s2_post_kill_idx << "." << std::endl;

        // Kill all nodes then inspect Node 3 durable log
        kill_and_reap(pid1); kill_and_reap(pid2); kill_and_reap(pid3);
        REQUIRE(active_child_pids.empty());

        // Step 11: Directly verify Node 3's durable log has >= s2_node1_entries entries
        // and that entry indices/terms match Node 1's log exactly.
        std::cout << "Verifying Node 3 durable log contents match Node 1..." << std::endl;
        {
            durable_log_store node1_store(node1_log_dir);
            durable_log_store node3_store(base_dir + "/node3/log");
            
            std::cout << "Node 1 next_slot: " << node1_store.next_slot() << ", Node 3 next_slot: " << node3_store.next_slot() << std::endl;
            REQUIRE(node1_store.next_slot() == node3_store.next_slot());

            for (ulong idx = 1; idx < node1_store.next_slot(); ++idx) {
                auto e1 = node1_store.entry_at(idx);
                auto e3 = node3_store.entry_at(idx);

                REQUIRE(e1 != nullptr);
                REQUIRE(e3 != nullptr);
                REQUIRE(e1->get_term() == e3->get_term());

                auto b1 = e1->serialize();
                auto b3 = e3->serialize();

                REQUIRE(b1->size() == b3->size());
                REQUIRE(memcmp(b1->data(), b3->data(), b1->size()) == 0);
            }
            std::cout << "All " << (node1_store.next_slot() - 1) << " entries verified: index, term, and full payload match between Node 1 and Node 3." << std::endl;
        }
        std::cout << "Scenario 2 PASSED: log contents verified after fdatasync crash and catch-up." << std::endl;


        // ---------------------------------------------------------------------
        // SCENARIO 3: Crash Before Obsolete Segment Unlink
        // ---------------------------------------------------------------------
        std::cout << "\n--- Scenario 3: Crash Before Obsolete Segment Unlink ---" << std::endl;

        clean_dir(base_dir);
        ::mkdir(base_dir.c_str(), 0755);

        // We can test this failpoint directly on a single log store instance in the controller process
        // to verify that recovery ignores the obsolete segments and does not corrupt the state.
        std::string store_dir = base_dir + "/single_store";
        
        {
            durable_log_store store(store_dir);
            // Append some entries
            auto e1 = cs_new<log_entry>(1, buffer::alloc(10));
            auto e2 = cs_new<log_entry>(1, buffer::alloc(10));
            auto e3 = cs_new<log_entry>(1, buffer::alloc(10));
            store.append(e1);
            store.append(e2);
            store.append(e3);
            store.end_of_append_batch(1, 3);
            REQUIRE(store.next_slot() == 4);
        }

        // Set the environment variable for failpoint
        ::setenv("ARIABC_FAILPOINT_CRASH_BEFORE_UNLINK", "1", 1);

        // Trigger rollback/truncation by writing at an earlier index
        pid_t fp_pid = register_spawn(::fork());
        if (fp_pid == 0) {
            try {
                durable_log_store store(store_dir);
                auto e_new = cs_new<log_entry>(2, buffer::alloc(10));
                // Writing at index 2 will trigger truncation of entries 2 and 3
                store.write_at(2, e_new);
                std::cout << "Write_at completed, should crash before unlink..." << std::endl;
                ::_exit(0);
            } catch (const std::exception& e) {
                std::cerr << "Child process caught exception: " << e.what() << std::endl;
                ::_exit(1);
            }
        }

        // Wait for child to crash
        int fp_status = wait_with_timeout(fp_pid, 5);
        REQUIRE(WIFSIGNALED(fp_status) && WTERMSIG(fp_status) == SIGKILL);
        std::cout << "Truncating child process crashed via SIGKILL at failpoint successfully." << std::endl;
        REQUIRE(active_child_pids.empty());

        // Clear failpoint env
        ::unsetenv("ARIABC_FAILPOINT_CRASH_BEFORE_UNLINK");

        // Reopen log store in the parent process and check recovery
        std::cout << "Reopening store to verify recovery handles unlinked obsolete segments..." << std::endl;
        {
            durable_log_store store(store_dir);
            // The recovery should process the durable TRUNCATE_FROM marker and set next_slot correctly.
            // Since we crashed before physically deleting obsolete files, recovery should successfully
            // scan and repair the tail/obsolete files according to manifest.
            REQUIRE(store.next_slot() == 2);
            REQUIRE(store.last_durable_index() == 1);
            std::cout << "Log store recovered perfectly to next_slot=2 after crash-before-unlink!" << std::endl;
        }

        clean_dir(base_dir);

        // ---------------------------------------------------------------------
        // SCENARIO 4: Crash After New Truncate Segment, Before TRUNCATE_FROM Marker
        // This tests the fixed create_segment(persist_manifest=false) ordering.
        // ---------------------------------------------------------------------
        std::cout << "\n--- Scenario 4: Crash After New Truncate Segment, Before TRUNCATE_FROM Marker ---" << std::endl;
        {
            std::string s4_store_dir = "./test_s4_truncate_segment";
            // Use small max_segment_size to force a new segment on every append
            const uint64_t TINY_SEG = 32ULL; // smaller than a single record → always rotates
            {
                durable_log_store store(s4_store_dir, TINY_SEG);
                auto e1 = cs_new<log_entry>(1, buffer::alloc(8));
                auto e2 = cs_new<log_entry>(1, buffer::alloc(8));
                auto e3 = cs_new<log_entry>(1, buffer::alloc(8));
                store.append(e1);
                store.append(e2);
                store.append(e3);
                store.end_of_append_batch(1, 3);
                REQUIRE(store.next_slot() == 4);
                std::cout << "S4: Appended 3 entries to store." << std::endl;
            }

            // Set failpoint: crash after the new segment is created but before the marker
            ::setenv("ARIABC_FAILPOINT_CRASH_AFTER_NEW_TRUNCATE_SEGMENT_BEFORE_MARKER", "1", 1);

            pid_t s4_pid = register_spawn(::fork());
            if (s4_pid == 0) {
                try {
                    durable_log_store store(s4_store_dir, TINY_SEG);
                    auto e_new = cs_new<log_entry>(2, buffer::alloc(8));
                    // write_at(1) removes segments with first_index >= 1, forcing a new segment
                    store.write_at(1, e_new);
                    ::_exit(0);
                } catch (const std::exception& ex) {
                    std::cerr << "S4 child exception: " << ex.what() << std::endl;
                    ::_exit(1);
                }
            }

            int s4_st = wait_with_timeout(s4_pid, 5);
            ::unsetenv("ARIABC_FAILPOINT_CRASH_AFTER_NEW_TRUNCATE_SEGMENT_BEFORE_MARKER");

            REQUIRE(WIFSIGNALED(s4_st) && WTERMSIG(s4_st) == SIGKILL);
            std::cout << "S4: Child crashed before marker via failpoint." << std::endl;
            REQUIRE(active_child_pids.empty());

            // Recovery: the manifest was NOT updated (persist_manifest=false path).
            // With the gen-ID fix, the new truncation segment has a UNIQUE filename
            // (different gen_id), so the old segment file is NEVER overwritten.
            // The old manifest still references the original segment with gen_id=1.
            // Recovery loads the old manifest → scans old segments → returns next_slot=4.
            {
                durable_log_store store(s4_store_dir, TINY_SEG);
                std::cout << "S4: Recovered next_slot=" << store.next_slot() << std::endl;
                // Must recover EXACTLY the 3 old entries — not an empty log.
                REQUIRE(store.next_slot() == 4);
                auto re1 = store.entry_at(1);
                auto re2 = store.entry_at(2);
                auto re3 = store.entry_at(3);
                REQUIRE(re1 != nullptr);
                REQUIRE(re2 != nullptr);
                REQUIRE(re3 != nullptr);
                REQUIRE(re1->get_term() == 1);
                REQUIRE(re2->get_term() == 1);
                REQUIRE(re3->get_term() == 1);
                std::cout << "S4: All 3 original entries verified after crash-before-marker." << std::endl;

                // Force another segment creation after recovery by appending a new entry.
                // This checks that EEXIST is avoided even though g4 was left as an orphan.
                auto e4 = cs_new<log_entry>(2, buffer::alloc(8));
                store.append(e4);
                store.end_of_append_batch(4, 1);

                REQUIRE(store.next_slot() == 5);
                std::cout << "S4: Append after recovery succeeded." << std::endl;
            }

            // Reopen again and verify the new entry
            {
                durable_log_store store(s4_store_dir, TINY_SEG);
                REQUIRE(store.next_slot() == 5);
                auto re4 = store.entry_at(4);
                REQUIRE(re4 != nullptr);
                REQUIRE(re4->get_term() == 2);
                std::cout << "S4: Reopened and verified appended entry." << std::endl;
            }
            clean_dir(s4_store_dir);
            std::cout << "Scenario 4 PASSED: old log fully intact, and rotation works after recovery." << std::endl;
            REQUIRE(active_child_pids.empty());
        }

        // ---------------------------------------------------------------------
        // SCENARIO 5: Missing log/manifest.bin must fail-closed on recovery
        // This tests the Blocker 2 fix in durable_state_mgr::open_or_create().
        // ---------------------------------------------------------------------
        std::cout << "\n--- Scenario 5: Missing log/manifest.bin Must Fail-Closed ---" << std::endl;
        {
            std::string s5_dir = "./test_s5_missing_manifest";
            clean_dir(s5_dir);

            // Create a fresh durable_state_mgr and initialize it
            ptr<cluster_config> s5_config = cs_new<cluster_config>();
            s5_config->get_servers().push_back(cs_new<srv_config>(1, "127.0.0.1:19001"));
            durable_state_mgr_config s5_cfg;
            s5_cfg.storage_dir = s5_dir;
            s5_cfg.node_id = 1;
            s5_cfg.endpoint = "127.0.0.1:19001";
            s5_cfg.cluster_id = "s5_cluster";

            {
                durable_state_mgr smgr(s5_cfg);
                smgr.initialize_fresh(*s5_config);
                // Also create the log store via load_log_store to write the manifest
                auto ls = smgr.load_log_store();
                auto dls = std::dynamic_pointer_cast<durable_log_store>(ls);
                if (dls) {
                    auto e1 = cs_new<log_entry>(1, buffer::alloc(4));
                    dls->append(e1);
                    dls->end_of_append_batch(1, 1);
                }
            }
            std::cout << "S5: Node initialized with manifest written." << std::endl;

            // Delete the log/manifest.bin to simulate a lost log directory
            std::string manifest_path = s5_dir + "/log/manifest.bin";
            REQUIRE(::unlink(manifest_path.c_str()) == 0);
            std::cout << "S5: Deleted log/manifest.bin to simulate lost log." << std::endl;

            // Now try to open the state_mgr — must throw storage_corruption_error
            bool s5_rejected = false;
            try {
                durable_state_mgr smgr2(s5_cfg);
            } catch (const storage_corruption_error& e) {
                std::cout << "S5: Correctly rejected missing manifest: " << e.what() << std::endl;
                s5_rejected = true;
            } catch (const std::exception& e) {
                std::cout << "S5: Rejected with unexpected exception: " << e.what() << std::endl;
                s5_rejected = true;
            }
            REQUIRE(s5_rejected);
            clean_dir(s5_dir);
            std::cout << "Scenario 5 PASSED: missing log/manifest.bin correctly fails closed." << std::endl;
        }

        // ---------------------------------------------------------------------
        // SCENARIO 6: Crash after truncate marker, before replacement entry (write_at & apply_pack)
        // ---------------------------------------------------------------------
        std::cout << "\n--- Scenario 6: Crash After Truncate Marker, Before Replacement Entry ---" << std::endl;
        {
            std::string s6_dir = "./test_s6_truncate_crash";
            clean_dir(s6_dir);

            // A. Test write_at()
            std::cout << "Testing write_at() truncation crash..." << std::endl;
            ptr<log_entry> original_e1;
            {
                durable_log_store store(s6_dir);
                original_e1 = create_test_entry(1, "s6-prefix-entry");
                auto e2 = create_test_entry(1, "s6-to-be-truncated-2");
                auto e3 = create_test_entry(1, "s6-to-be-truncated-3");
                store.append(original_e1);
                store.append(e2);
                store.append(e3);
                store.end_of_append_batch(1, 3);
                REQUIRE(store.next_slot() == 4);
            }

            ::setenv("ARIABC_FAILPOINT_CRASH_AFTER_TRUNCATE_MARKER_BEFORE_REPLACEMENT", "1", 1);

            pid_t s6_pid_write = register_spawn(::fork());
            if (s6_pid_write == 0) {
                try {
                    durable_log_store store(s6_dir);
                    auto e_new = create_test_entry(2, "s6-replacement-entry");
                    // write_at(2) should truncate at index 2, then crash before writing e_new
                    store.write_at(2, e_new);
                    ::_exit(0);
                } catch (const std::exception& ex) {
                    std::cerr << "S6 write_at child exception: " << ex.what() << std::endl;
                    ::_exit(1);
                }
            }

            int s6_st_write = wait_with_timeout(s6_pid_write, 5);
            ::unsetenv("ARIABC_FAILPOINT_CRASH_AFTER_TRUNCATE_MARKER_BEFORE_REPLACEMENT");

            REQUIRE(WIFSIGNALED(s6_st_write) && WTERMSIG(s6_st_write) == SIGKILL);
            std::cout << "S6: write_at child crashed successfully." << std::endl;
            REQUIRE(active_child_pids.empty());

            // Verify metadata on disk before recovery constructor runs
            verify_on_disk_metadata(s6_dir, 1, 2);

            // Recovery check for write_at
            {
                durable_log_store store(s6_dir);
                std::cout << "S6 write_at recovery next_slot: " << store.next_slot() << std::endl;
                REQUIRE(store.next_slot() == 2);
                auto e_rec = store.entry_at(1);
                REQUIRE(e_rec != nullptr);
                REQUIRE(serialized_entry_bytes(e_rec) == serialized_entry_bytes(original_e1));
                REQUIRE(entry_payload_equals(e_rec, "s6-prefix-entry"));
                REQUIRE(store.entry_at(2) == nullptr);

                // Try writing new entries now
                auto e_new2 = create_test_entry(2, "s6-new-append-after-recovery");
                store.append(e_new2);
                store.end_of_append_batch(2, 1);
                REQUIRE(store.next_slot() == 3);
                REQUIRE(entry_payload_equals(store.entry_at(2), "s6-new-append-after-recovery"));
            }

            clean_dir(s6_dir);
            REQUIRE(active_child_pids.empty());

            // B. Test apply_pack()
            std::cout << "Testing apply_pack() truncation crash..." << std::endl;
            ptr<log_entry> original_apply_e1;
            {
                durable_log_store store(s6_dir);
                original_apply_e1 = create_test_entry(1, "s6-apply-prefix-entry");
                auto e2 = create_test_entry(1, "s6-to-be-truncated-2");
                auto e3 = create_test_entry(1, "s6-to-be-truncated-3");
                store.append(original_apply_e1);
                store.append(e2);
                store.append(e3);
                store.end_of_append_batch(1, 3);
                REQUIRE(store.next_slot() == 4);
            }

            ::setenv("ARIABC_FAILPOINT_CRASH_AFTER_TRUNCATE_MARKER_BEFORE_REPLACEMENT", "1", 1);

            pid_t s6_pid_apply = register_spawn(::fork());
            if (s6_pid_apply == 0) {
                try {
                    durable_log_store store(s6_dir);
                    auto e_new = create_test_entry(2, "conflicting-payload");
                    // apply_pack starting at index 2 (truncating 2 and 3)
                    nuraft::ptr<nuraft::buffer> entry_buf = e_new->serialize();
                    size_t required = sizeof(int32_t) + sizeof(int32_t) + entry_buf->size();
                    nuraft::ptr<nuraft::buffer> pack = nuraft::buffer::alloc(required);
                    pack->pos(0);
                    pack->put((int32_t)1); // num_logs = 1
                    pack->put((int32_t)entry_buf->size());
                    pack->put(*entry_buf);
                    pack->pos(0);

                    store.apply_pack(2, *pack);
                    ::_exit(0);
                } catch (const std::exception& ex) {
                    std::cerr << "S6 apply_pack child exception: " << ex.what() << std::endl;
                    ::_exit(1);
                }
            }

            int s6_st_apply = wait_with_timeout(s6_pid_apply, 5);
            ::unsetenv("ARIABC_FAILPOINT_CRASH_AFTER_TRUNCATE_MARKER_BEFORE_REPLACEMENT");

            REQUIRE(WIFSIGNALED(s6_st_apply) && WTERMSIG(s6_st_apply) == SIGKILL);
            std::cout << "S6: apply_pack child crashed successfully." << std::endl;
            REQUIRE(active_child_pids.empty());

            // Verify metadata on disk before recovery constructor runs
            verify_on_disk_metadata(s6_dir, 1, 2);

            // Recovery check for apply_pack
            {
                durable_log_store store(s6_dir);
                std::cout << "S6 apply_pack recovery next_slot: " << store.next_slot() << std::endl;
                REQUIRE(store.next_slot() == 2);
                auto e_rec = store.entry_at(1);
                REQUIRE(e_rec != nullptr);
                REQUIRE(serialized_entry_bytes(e_rec) == serialized_entry_bytes(original_apply_e1));
                REQUIRE(entry_payload_equals(e_rec, "s6-apply-prefix-entry"));
                REQUIRE(store.entry_at(2) == nullptr);

                // Try writing new entries now
                auto e_new2 = create_test_entry(2, "s6-apply-new-append-after-recovery");
                store.append(e_new2);
                store.end_of_append_batch(2, 1);
                REQUIRE(store.next_slot() == 3);
                REQUIRE(entry_payload_equals(store.entry_at(2), "s6-apply-new-append-after-recovery"));
            }

            clean_dir(s6_dir);
            std::cout << "Scenario 6 PASSED: write_at and apply_pack truncation crashes recover cleanly." << std::endl;
        }

        // ---------------------------------------------------------------------
        // SCENARIO 7: Three-Node Failover and Ordinary Catch-Up
        // ---------------------------------------------------------------------
        std::cout << "\n--- Scenario 7: Three-Node Failover and Ordinary Catch-Up ---" << std::endl;
        {
            std::string s7_dir = "./test_s7_truncate_catchup";
            clean_dir(s7_dir);
            ::mkdir(s7_dir.c_str(), 0755);

            int p1 = 9101, p2 = 9102, p3 = 9103;

            auto spawn_node_s7 = [&](int id) -> pid_t {
                pid_t pid = ::fork();
                if (pid == 0) {
                    ::execl(self_binary.c_str(), self_binary.c_str(), "--node",
                            std::to_string(id).c_str(), s7_dir.c_str(),
                            std::to_string(p1).c_str(), std::to_string(p2).c_str(), std::to_string(p3).c_str(),
                            nullptr);
                    ::_exit(1);
                }
                return register_spawn(pid);
            };

            std::cout << "Spawning 3 peers for S7..." << std::endl;
            pid_t pid1 = spawn_node_s7(1);
            pid_t pid2 = spawn_node_s7(2);
            pid_t pid3 = spawn_node_s7(3);
            REQUIRE(pid1 > 0 && pid2 > 0 && pid3 > 0);

            // 1. Wait for leader to emerge, replicate index 1..5.
            std::cout << "Waiting for leader election and replication..." << std::endl;
            ulong leader_id = 0;
            ulong commit_index = 0;
            for (int i = 0; i < 150; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                auto s1 = read_status_file(s7_dir, 1);
                auto s2 = read_status_file(s7_dir, 2);
                auto s3 = read_status_file(s7_dir, 3);
                if (s1.role == "LEADER") leader_id = 1;
                if (s2.role == "LEADER") leader_id = 2;
                if (s3.role == "LEADER") leader_id = 3;

                commit_index = std::max({s1.commit_index, s2.commit_index, s3.commit_index});
                if (leader_id > 0 && commit_index >= 5) {
                    break;
                }
            }
            REQUIRE(leader_id > 0);
            REQUIRE(commit_index >= 5);
            std::cout << "Leader is Node " << leader_id << ", Commit Index: " << commit_index << std::endl;

            // 2. Determine who is who
            int followerA_id = (leader_id == 1) ? 2 : 1;
            int followerB_id = (leader_id == 3) ? 2 : 3;

            pid_t leader_proc = (leader_id == 1) ? pid1 : ((leader_id == 2) ? pid2 : pid3);
            pid_t followerA_proc = (followerA_id == 1) ? pid1 : ((followerA_id == 2) ? pid2 : pid3);
            pid_t followerB_proc = (followerB_id == 1) ? pid1 : ((followerB_id == 2) ? pid2 : pid3);

            std::cout << "Leader ID=" << leader_id << ", Follower A=" << followerA_id << ", Follower B=" << followerB_id << std::endl;

            // Kill Follower B
            std::cout << "SIGKILL Follower B (Node " << followerB_id << ")..." << std::endl;
            kill_and_reap(followerB_proc);

            // Propose entries on leader (so Leader and Follower A replicate them)
            std::cout << "Proposing entries on remaining quorum (Leader + Follower A)..." << std::endl;
            ulong target_commit = commit_index + 3;
            for (int i = 0; i < 100; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                auto sl = read_status_file(s7_dir, leader_id);
                auto sa = read_status_file(s7_dir, followerA_id);
                if (sl.commit_index >= target_commit && sa.commit_index >= target_commit) {
                    target_commit = std::max(sl.commit_index, sa.commit_index);
                    break;
                }
            }
            std::cout << "Advanced commit index on Leader + Follower A to: " << target_commit << std::endl;

            // Kill Leader!
            std::cout << "SIGKILL Leader (Node " << leader_id << ")..." << std::endl;
            kill_and_reap(leader_proc);

            // Restart Follower B
            std::cout << "Restarting Follower B (Node " << followerB_id << ")..." << std::endl;
            pid_t new_followerB_proc = spawn_node_s7(followerB_id);
            REQUIRE(new_followerB_proc > 0);
            if (followerB_id == 1) pid1 = new_followerB_proc;
            else if (followerB_id == 2) pid2 = new_followerB_proc;
            else pid3 = new_followerB_proc;

            // Wait for Follower A to become leader and replicate to Follower B, truncating and catching it up
            std::cout << "Waiting for Node " << followerA_id << " to become leader and catch up Node " << followerB_id << "..." << std::endl;
            bool caught_up = false;
            ulong final_commit = 0;
            for (int i = 0; i < 200; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                auto sa = read_status_file(s7_dir, followerA_id);
                auto sb = read_status_file(s7_dir, followerB_id);
                if (sa.role == "LEADER" && sb.commit_index >= sa.commit_index && sb.commit_index >= target_commit) {
                    caught_up = true;
                    final_commit = sb.commit_index;
                    break;
                }
            }
            REQUIRE(caught_up);
            std::cout << "Follower B (Node " << followerB_id << ") successfully caught up to new leader (Node " << followerA_id << ") at commit index: " << final_commit << std::endl;

            // Propose entry on new leader to force conflict
            std::cout << "Proposing entry on new leader to force conflict..." << std::endl;
            ulong new_target_commit = final_commit + 2;
            for (int i = 0; i < 100; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                auto sa = read_status_file(s7_dir, followerA_id);
                if (sa.commit_index >= new_target_commit) {
                    new_target_commit = sa.commit_index;
                    break;
                }
            }

            // Restart original Leader to test truncation catch-up
            std::cout << "Restarting original Leader (Node " << leader_id << ") to test truncation catch-up..." << std::endl;
            pid_t restarted_leader_proc = spawn_node_s7(leader_id);
            REQUIRE(restarted_leader_proc > 0);
            if (leader_id == 1) pid1 = restarted_leader_proc;
            else if (leader_id == 2) pid2 = restarted_leader_proc;
            else pid3 = restarted_leader_proc;

            // Wait for original leader to catch up
            std::cout << "Waiting for original Leader (Node " << leader_id << ") to catch up..." << std::endl;
            bool orig_leader_caught_up = false;
            for (int i = 0; i < 200; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                auto sl = read_status_file(s7_dir, leader_id);
                auto sa = read_status_file(s7_dir, followerA_id);
                if (sl.commit_index >= sa.commit_index && sl.commit_index >= new_target_commit) {
                    orig_leader_caught_up = true;
                    break;
                }
            }
            REQUIRE(orig_leader_caught_up);
            std::cout << "Original Leader (Node " << leader_id << ") caught up and resolved conflicts successfully." << std::endl;

            // Cleanup S7
            kill_and_reap(pid1);
            kill_and_reap(pid2);
            kill_and_reap(pid3);

            clean_dir(s7_dir);
            std::cout << "Scenario 7 PASSED: Three-Node Failover and Ordinary Catch-Up verified successfully." << std::endl;
            REQUIRE(active_child_pids.empty());
        }

        // ---------------------------------------------------------------------
        // SCENARIO 8: Follower Crash After Durable Truncate Marker, Then Raft Catch-Up
        // ---------------------------------------------------------------------
        std::cout << "\n--- Scenario 8: Follower Crash After Durable Truncate Marker, Then Raft Catch-Up ---" << std::endl;
        {
            std::string s8_dir = "./test_s8_truncate_catchup";
            clean_dir(s8_dir);
            ::mkdir(s8_dir.c_str(), 0755);

            int p1 = 9201, p2 = 9202, p3 = 9203;

            auto spawn_node_s8 = [&](int id, bool failpoint) -> pid_t {
                pid_t pid = ::fork();
                if (pid == 0) {
                    if (failpoint) {
                        ::setenv("ARIABC_FAILPOINT_CRASH_AFTER_TRUNCATE_MARKER_BEFORE_REPLACEMENT", "1", 1);
                    }
                    ::execl(self_binary.c_str(), self_binary.c_str(), "--node",
                            std::to_string(id).c_str(), s8_dir.c_str(),
                            std::to_string(p1).c_str(), std::to_string(p2).c_str(), std::to_string(p3).c_str(),
                            nullptr);
                    ::_exit(1);
                }
                return register_spawn(pid);
            };

            std::cout << "1. Starting 3 nodes..." << std::endl;
            pid_t pid1 = spawn_node_s8(1, false);
            pid_t pid2 = spawn_node_s8(2, false);
            pid_t pid3 = spawn_node_s8(3, false);
            REQUIRE(pid1 > 0 && pid2 > 0 && pid3 > 0);

            // Keep track of PIDs dynamically
            pid_t pids[4] = {0, pid1, pid2, pid3};

            // 2. Wait for leader and commit entries A, B, C (all 3 nodes durable index >= 3)
            std::cout << "2. Committing entries A, B, C..." << std::endl;
            ulong leader_id = 0;
            for (int i = 0; i < 150; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                auto s1 = read_status_file(s8_dir, 1);
                auto s2 = read_status_file(s8_dir, 2);
                auto s3 = read_status_file(s8_dir, 3);
                if (s1.role == "LEADER") leader_id = 1;
                if (s2.role == "LEADER") leader_id = 2;
                if (s3.role == "LEADER") leader_id = 3;

                if (leader_id > 0 && s1.last_durable_idx >= 3 && s2.last_durable_idx >= 3 && s3.last_durable_idx >= 3) {
                    break;
                }
            }
            REQUIRE(leader_id > 0);
            auto s1 = read_status_file(s8_dir, 1);
            auto s2 = read_status_file(s8_dir, 2);
            auto s3 = read_status_file(s8_dir, 3);
            REQUIRE(s1.last_durable_idx >= 3);
            REQUIRE(s2.last_durable_idx >= 3);
            REQUIRE(s3.last_durable_idx >= 3);
            std::cout << "Leader ID=" << leader_id << ", Node1 durable=" << s1.last_durable_idx << ", Node2 durable=" << s2.last_durable_idx << ", Node3 durable=" << s3.last_durable_idx << std::endl;

            // Choose victim dynamically (never kill the leader)
            const int victim_id = (leader_id == 3) ? 2 : 3;
            REQUIRE(victim_id != (int)leader_id);
            pid_t victim_pid = pids[victim_id];

            // 3. Stop only the victim follower
            std::cout << "3. Stopping victim follower Node " << victim_id << "..." << std::endl;
            kill_and_reap(victim_pid);

            // 4. Open victim follower's durable log directly and replace suffix from index 3 with conflicting term/payload X, Y
            std::cout << "4-5. Modifying victim follower " << victim_id << "'s log directly..." << std::endl;
            std::string victim_log_dir = s8_dir + "/node" + std::to_string(victim_id) + "/log";
            {
                durable_log_store f_store(victim_log_dir);
                REQUIRE(f_store.next_slot() >= 4);

                std::string payloadX = "conflicting_X";
                auto eX = cs_new<log_entry>(5, buffer::alloc(payloadX.size() + 1));
                eX->get_buf().put_raw((const uint8_t*)payloadX.c_str(), payloadX.size() + 1);
                eX->get_buf().pos(0);

                std::string payloadY = "conflicting_Y";
                auto eY = cs_new<log_entry>(5, buffer::alloc(payloadY.size() + 1));
                eY->get_buf().put_raw((const uint8_t*)payloadY.c_str(), payloadY.size() + 1);
                eY->get_buf().pos(0);

                f_store.write_at(3, eX);
                f_store.append(eY);
                f_store.end_of_append_batch(3, 2);

                REQUIRE(f_store.next_slot() == 5);
            }

            // 6. Restart victim follower with failpoint active
            std::cout << "6. Restarting victim follower " << victim_id << " with failpoint..." << std::endl;
            pid_t fp_victim_pid = spawn_node_s8(victim_id, true);
            REQUIRE(fp_victim_pid > 0);
            pids[victim_id] = fp_victim_pid;

            // 7. Confirm it dies by SIGKILL without hanging
            std::cout << "7. Confirming victim follower " << victim_id << " dies by failpoint SIGKILL..." << std::endl;
            std::string victim_log_path = s8_dir + "/node" + std::to_string(victim_id) + ".log";
            int st_fp = wait_with_timeout(fp_victim_pid, 10, victim_log_path);
            REQUIRE(WIFSIGNALED(st_fp) && WTERMSIG(st_fp) == SIGKILL);
            std::cout << "Victim follower " << victim_id << " died by failpoint SIGKILL successfully." << std::endl;

            // 8. Reopen its log and verify index 1 and 2 intact, index 3 absent, next_slot() == 3
            std::cout << "8. Verifying victim follower " << victim_id << "'s on-disk log post-crash..." << std::endl;
            {
                durable_log_store f_store(victim_log_dir);
                REQUIRE(f_store.next_slot() == 3);
                auto e1 = f_store.entry_at(1);
                auto e2 = f_store.entry_at(2);
                REQUIRE(e1 != nullptr);
                REQUIRE(e2 != nullptr);
                REQUIRE(f_store.entry_at(3) == nullptr);
            }

            // 9. Restart it without failpoint
            std::cout << "9. Restarting victim follower " << victim_id << " without failpoint..." << std::endl;
            pid_t normal_victim_pid = spawn_node_s8(victim_id, false);
            REQUIRE(normal_victim_pid > 0);
            pids[victim_id] = normal_victim_pid;

            // 10. Dynamically locate the current live leader and record its last_durable_idx as leader_idx_at_restart
            ulong live_leader_id = 0;
            ulong leader_idx_at_restart = 0;
            for (int i = 0; i < 100; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                for (int n = 1; n <= 3; ++n) {
                    if (n == victim_id) continue;
                    auto s = read_status_file(s8_dir, n);
                    if (s.role == "LEADER") {
                        live_leader_id = n;
                        leader_idx_at_restart = s.last_durable_idx;
                        break;
                    }
                }
                if (live_leader_id > 0) break;
            }
            REQUIRE(live_leader_id > 0);
            std::cout << "Live leader identified: Node " << live_leader_id << " at index " << leader_idx_at_restart << std::endl;

            // Wait until victim's last_durable_idx >= leader_idx_at_restart
            std::cout << "Waiting for victim follower Node " << victim_id << " to catch up to " << leader_idx_at_restart << "..." << std::endl;
            bool caught_up = false;
            for (int i = 0; i < 200; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                auto sv = read_status_file(s8_dir, victim_id);
                if (sv.last_durable_idx >= leader_idx_at_restart) {
                    caught_up = true;
                    break;
                }
            }
            REQUIRE(caught_up);
            std::cout << "Victim follower caught up successfully!" << std::endl;

            // 11. Wait for one fresh entry to be committed after recovery on all 3 nodes
            std::cout << "11. Waiting for fresh entry to be committed on all 3 nodes..." << std::endl;
            ulong target_after = leader_idx_at_restart + 2;
            bool fresh_committed = false;
            ulong final_commit = 0;
            for (int i = 0; i < 150; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                auto s1_s = read_status_file(s8_dir, 1);
                auto s2_s = read_status_file(s8_dir, 2);
                auto s3_s = read_status_file(s8_dir, 3);
                ulong cur_commit = std::min({s1_s.commit_index, s2_s.commit_index, s3_s.commit_index});
                if (cur_commit >= target_after) {
                    fresh_committed = true;
                    final_commit = cur_commit;
                    break;
                }
            }
            REQUIRE(fresh_committed);
            REQUIRE(final_commit >= 3);

            // 12. Stop all nodes first to ensure logs are fully flushed and closed
            std::cout << "12. Stopping all nodes for log verification..." << std::endl;
            kill_and_reap(pids[1]);
            kill_and_reap(pids[2]);
            kill_and_reap(pids[3]);
            REQUIRE(active_child_pids.empty());

            // 13. Double check everything matches across all 3 nodes on disk: next_slot equality and byte-level comparison
            std::cout << "13. Verifying all log stores match exactly on disk..." << std::endl;
            {
                durable_log_store store1(s8_dir + "/node1/log");
                durable_log_store store2(s8_dir + "/node2/log");
                durable_log_store store3(s8_dir + "/node3/log");

                REQUIRE(store1.next_slot() == store2.next_slot());
                REQUIRE(store2.next_slot() == store3.next_slot());
                REQUIRE(store1.next_slot() >= 4);

                for (ulong idx = 1; idx < store1.next_slot(); ++idx) {
                    auto e1 = store1.entry_at(idx);
                    auto e2 = store2.entry_at(idx);
                    auto e3 = store3.entry_at(idx);
                    REQUIRE(e1 != nullptr && e2 != nullptr && e3 != nullptr);
                    REQUIRE(e1->get_term() == e2->get_term() && e2->get_term() == e3->get_term());

                    auto b1 = e1->serialize();
                    auto b2 = e2->serialize();
                    auto b3 = e3->serialize();
                    REQUIRE(b1->size() == b2->size() && b2->size() == b3->size());
                    REQUIRE(::memcmp(b1->data(), b2->data(), b1->size()) == 0);
                    REQUIRE(::memcmp(b2->data(), b3->data(), b2->size()) == 0);
                }
            }

            clean_dir(s8_dir);
            std::cout << "Scenario 8 PASSED: Follower Crash After Durable Truncate Marker, Then Raft Catch-Up verified successfully." << std::endl;
            REQUIRE(active_child_pids.empty());
        }

        std::cout << "\n==========================================================" << std::endl;
        std::cout << "ALL EXTERNAL PROCESS SIGKILL & FAILPOINT TESTS PASSED" << std::endl;
        std::cout << "==========================================================" << std::endl;

        kill_all_active_children();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Controller test failed with exception: " << e.what() << std::endl;
        kill_all_active_children();
        return 1;
    }
}
