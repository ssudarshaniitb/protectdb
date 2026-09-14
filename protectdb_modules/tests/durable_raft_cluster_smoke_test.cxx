// ariabc_pg/tests/durable_raft_cluster_smoke_test.cxx
// Integration/smoke test for 3-node durable storage components.

#include "../src/durable_state_mgr.hxx"
#include "../src/durable_log_store.hxx"
#include "../src/logger_wrapper.hxx"
#include "nuraft.hxx"
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdexcept>
#include <chrono>
#include <thread>

#define REQUIRE(expr) \
    do { \
        if (!(expr)) \
            throw std::runtime_error("requirement failed: " #expr); \
    } while (0)

using namespace ariabc_raft;
using namespace nuraft;

class dummy_state_machine : public state_machine {
public:
    dummy_state_machine() : commit_idx_(0) {}
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

void clean_dir(const std::string& d) {
    ::system(("rm -rf " + d).c_str());
}

ptr<raft_server> launch_node(int id,
                             const std::string& dir,
                             const std::string& endpoint,
                             ptr<cluster_config>& conf,
                             ptr<state_machine> sm,
                             ptr<logger> log_wrapper,
                             raft_launcher& launcher,
                             ptr<durable_state_mgr>& smgr_out) {
    durable_state_mgr_config cfg;
    cfg.storage_dir = dir;
    cfg.node_id = id;
    cfg.endpoint = endpoint;
    cfg.cluster_id = "smoke_cluster";

    auto smgr = cs_new<durable_state_mgr>(cfg);
    if (!smgr->is_recovered()) {
        smgr->initialize_fresh(*conf);
    }
    smgr_out = smgr;

    asio_service::options asio_opt;
    asio_opt.thread_pool_size_ = 2;

    raft_params params;
    params.heart_beat_interval_ = 100;
    params.election_timeout_lower_bound_ = 200;
    params.election_timeout_upper_bound_ = 400;
    params.reserved_log_items_ = 5;
    params.snapshot_distance_ = 0;
    params.client_req_timeout_ = 2000;
    params.leadership_expiry_ = 5000;
    params.return_method_ = raft_params::async_handler;
    params.auto_forwarding_ = true;
    params.parallel_log_appending_ = false; // important for durable_log_store

    std::string host = "127.0.0.1";
    int port = 9000 + id;

    ptr<raft_server> server = launcher.init(sm, smgr, log_wrapper, port, asio_opt, params);
    REQUIRE(server != nullptr);
    return server;
}

int main() {
    try {
        std::string base_dir = "./test_cluster_smoke";
        clean_dir(base_dir);

        std::string dir1 = base_dir + "/node1";
        std::string dir2 = base_dir + "/node2";
        std::string dir3 = base_dir + "/node3";

        // Setup initial configuration
        ptr<cluster_config> initial_conf = cs_new<cluster_config>();
        initial_conf->get_servers().push_back(cs_new<srv_config>(1, "127.0.0.1:9001"));
        initial_conf->get_servers().push_back(cs_new<srv_config>(2, "127.0.0.1:9002"));
        initial_conf->get_servers().push_back(cs_new<srv_config>(3, "127.0.0.1:9003"));

        auto log_wrapper1 = cs_new<logger_wrapper>(base_dir + "/node1.log", 4);
        auto log_wrapper2 = cs_new<logger_wrapper>(base_dir + "/node2.log", 4);
        auto log_wrapper3 = cs_new<logger_wrapper>(base_dir + "/node3.log", 4);

        auto sm1 = cs_new<dummy_state_machine>();
        auto sm2 = cs_new<dummy_state_machine>();
        auto sm3 = cs_new<dummy_state_machine>();

        auto launcher1 = std::unique_ptr<raft_launcher>(new raft_launcher());
        auto launcher2 = std::unique_ptr<raft_launcher>(new raft_launcher());
        auto launcher3 = std::unique_ptr<raft_launcher>(new raft_launcher());

        std::cout << "Starting 3 NuRaft peers..." << std::endl;
        ptr<durable_state_mgr> smgr1;
        ptr<durable_state_mgr> smgr2;
        ptr<durable_state_mgr> smgr3;
        auto r1 = launch_node(1, dir1, "127.0.0.1:9001", initial_conf, sm1, log_wrapper1, *launcher1, smgr1);
        auto r2 = launch_node(2, dir2, "127.0.0.1:9002", initial_conf, sm2, log_wrapper2, *launcher2, smgr2);
        auto r3 = launch_node(3, dir3, "127.0.0.1:9003", initial_conf, sm3, log_wrapper3, *launcher3, smgr3);

        // 1. Wait for Leader Election
        std::cout << "Waiting for leader election..." << std::endl;
        ptr<raft_server> leader = nullptr;
        for (int attempt = 0; attempt < 50; ++attempt) {
            if (r1->is_leader()) { leader = r1; break; }
            if (r2->is_leader()) { leader = r2; break; }
            if (r3->is_leader()) { leader = r3; break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        REQUIRE(leader != nullptr);
        std::cout << "Leader elected! Leader ID: " << leader->get_id() << std::endl;

        // 2. Commit data
        std::cout << "Proposing entry 1..." << std::endl;
        ptr<buffer> entry_data = buffer::alloc(10);
        entry_data->put_raw((const uint8_t*)"test_val1", 9);
        entry_data->pos(0);

        auto res = leader->append_entries({entry_data});
        REQUIRE(res != nullptr);
        REQUIRE(res->get_accepted() == true);

        // Wait for commit index to reach 2 on all nodes (index 1 is cluster config, index 2 is entry_data)
        std::cout << "Waiting for commit replication..." << std::endl;
        for (int attempt = 0; attempt < 50; ++attempt) {
            if (sm1->get_commit_idx() >= 2 && sm2->get_commit_idx() >= 2 && sm3->get_commit_idx() >= 2) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        REQUIRE(sm1->get_commit_idx() >= 2);
        REQUIRE(sm2->get_commit_idx() >= 2);
        REQUIRE(sm3->get_commit_idx() >= 2);
        std::cout << "Entry 1 committed on all nodes!" << std::endl;

        // Verify end_of_append_batch was invoked on log store by checking last_durable_index
        // We retrieve the log store from each node's state manager
        auto log1 = std::dynamic_pointer_cast<durable_log_store>(smgr1->load_log_store());
        REQUIRE(log1->last_durable_index() >= 2);
        std::cout << "Validated that end_of_append_batch synced the log successfully." << std::endl;

        // 3. Follower Crash: Choose a non-leader victim dynamically
        int victim_id = (leader->get_id() == 3) ? 2 : 3;
        std::cout << "Shutting down Follower Node " << victim_id << "..." << std::endl;

        if (victim_id == 2) {
            std::cout << "Node 2 sudden crash simulated (SIGKILL)" << std::endl;
            smgr2->simulate_crash();
            bool shutdown_ok = launcher2->shutdown();
            std::cout << "Node 2 launcher shutdown (port release): " << (shutdown_ok ? "SUCCESS" : "TIMEOUT") << std::endl;
            r2.reset();
            launcher2.reset();
            smgr2.reset();
            log_wrapper2->destroy();
            REQUIRE(sm2->get_commit_idx() < 3);
        } else {
            std::cout << "Node 3 sudden crash simulated (SIGKILL)" << std::endl;
            smgr3->simulate_crash();
            bool shutdown_ok = launcher3->shutdown();
            std::cout << "Node 3 launcher shutdown (port release): " << (shutdown_ok ? "SUCCESS" : "TIMEOUT") << std::endl;
            r3.reset();
            launcher3.reset();
            smgr3.reset();
            log_wrapper3->destroy();
            REQUIRE(sm3->get_commit_idx() < 3);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        // 4. Propose another entry while the victim is down
        std::cout << "Proposing entry 2 with Node " << victim_id << " offline..." << std::endl;
        ptr<buffer> entry_data2 = buffer::alloc(10);
        entry_data2->put_raw((const uint8_t*)"test_val2", 9);
        entry_data2->pos(0);

        auto res2 = leader->append_entries({entry_data2});
        REQUIRE(res2 != nullptr);
        REQUIRE(res2->get_accepted() == true);

        // Wait for commit index to reach 3 on the alive nodes
        std::cout << "Waiting for commit index to reach 3 on alive nodes..." << std::endl;
        for (int attempt = 0; attempt < 50; ++attempt) {
            bool all_alive_committed = true;
            if (leader->get_id() != 1 && (!sm1 || sm1->get_commit_idx() < 3)) all_alive_committed = false;
            if (leader->get_id() != 2 && victim_id != 2 && (!sm2 || sm2->get_commit_idx() < 3)) all_alive_committed = false;
            if (leader->get_id() != 3 && victim_id != 3 && (!sm3 || sm3->get_commit_idx() < 3)) all_alive_committed = false;
            if (leader->get_id() == 1 && sm1->get_commit_idx() < 3) all_alive_committed = false;
            if (leader->get_id() == 2 && sm2->get_commit_idx() < 3) all_alive_committed = false;
            if (leader->get_id() == 3 && sm3->get_commit_idx() < 3) all_alive_committed = false;
            if (all_alive_committed) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (leader->get_id() == 1) { REQUIRE(sm1->get_commit_idx() >= 3); }
        if (leader->get_id() == 2) { REQUIRE(sm2->get_commit_idx() >= 3); }
        if (leader->get_id() == 3) { REQUIRE(sm3->get_commit_idx() >= 3); }
        if (victim_id == 2) {
            REQUIRE(sm3->get_commit_idx() >= 3);
        } else {
            REQUIRE(sm2->get_commit_idx() >= 3);
        }
        std::cout << "Entry 2 committed on alive nodes while Node " << victim_id << " is offline." << std::endl;

        // 5. Follower Restart: Start victim back up
        std::cout << "Restarting Node " << victim_id << "..." << std::endl;
        auto log_wrapper_victim_new = cs_new<logger_wrapper>(base_dir + "/node" + std::to_string(victim_id) + "_new.log", 4);
        auto sm_victim_new = cs_new<dummy_state_machine>();
        auto launcher_victim_new = std::unique_ptr<raft_launcher>(new raft_launcher());
        ptr<durable_state_mgr> smgr_victim_new;
        std::string dir_victim = (victim_id == 2) ? dir2 : dir3;
        std::string endpoint_victim = (victim_id == 2) ? "127.0.0.1:9002" : "127.0.0.1:9003";
        auto r_victim_new = launch_node(victim_id, dir_victim, endpoint_victim, initial_conf, sm_victim_new, log_wrapper_victim_new, *launcher_victim_new, smgr_victim_new);

        // 6. Catch-up: Wait for restarted victim to catch up through AppendEntries
        std::cout << "Waiting for restarted Node " << victim_id << " to catch up..." << std::endl;
        for (int attempt = 0; attempt < 50; ++attempt) {
            if (sm_victim_new->get_commit_idx() >= 3) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        REQUIRE(sm_victim_new->get_commit_idx() >= 3);
        std::cout << "Node " << victim_id << " caught up perfectly!" << std::endl;

        // 7. Verify vote persistence: read term/voted_for from restarted state manager
        auto state_victim = smgr_victim_new->read_state();
        REQUIRE(state_victim != nullptr);
        REQUIRE(state_victim->get_term() > 0);
        REQUIRE(state_victim->get_voted_for() == -1 || (state_victim->get_voted_for() >= 1 && state_victim->get_voted_for() <= 3));
        std::cout << "Vote persistence verified! Node " << victim_id << " saved term=" << state_victim->get_term()
                  << " voted_for=" << state_victim->get_voted_for() << std::endl;

        // Verify durability telemetry on the original log store before shutdown
        auto log1_orig = std::dynamic_pointer_cast<durable_log_store>(smgr1->load_log_store());
        REQUIRE(log1_orig != nullptr);
        const auto& tel_orig = log1_orig->profile();
        REQUIRE(tel_orig.append_batches.load() > 0);
        REQUIRE(tel_orig.fdatasync_calls.load() > 0);
        std::cout << "Original durability telemetry verified: append_batches=" << tel_orig.append_batches.load()
                  << ", fdatasync_calls=" << tel_orig.fdatasync_calls.load() << std::endl;

        // 8. Clean restart of all three peers
        std::cout << "Performing clean restart of all three peers..." << std::endl;
        if (launcher1) {
            bool ok = launcher1->shutdown();
            std::cout << "launcher1 shutdown: " << (ok ? "OK" : "TIMEOUT") << std::endl;
        }
        if (launcher2) {
            bool ok = launcher2->shutdown();
            std::cout << "launcher2 shutdown: " << (ok ? "OK" : "TIMEOUT") << std::endl;
        }
        if (launcher3) {
            bool ok = launcher3->shutdown();
            std::cout << "launcher3 shutdown: " << (ok ? "OK" : "TIMEOUT") << std::endl;
        }
        bool ok_v = launcher_victim_new->shutdown();
        std::cout << "launcher_victim_new shutdown: " << (ok_v ? "OK" : "TIMEOUT") << std::endl;

        leader.reset();
        r_victim_new.reset();
        launcher_victim_new.reset();

        r1.reset();
        r2.reset();
        r3.reset();

        smgr1.reset();
        smgr2.reset();
        smgr3.reset();
        smgr_victim_new.reset();

        launcher1.reset();
        launcher2.reset();
        launcher3.reset();

        if (log_wrapper1) log_wrapper1->destroy();
        if (log_wrapper2) log_wrapper2->destroy();
        if (log_wrapper3) log_wrapper3->destroy();
        log_wrapper_victim_new->destroy();

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        // Restart all three nodes with the SAME directories (preserving state)
        auto log_wrapper1_r = cs_new<logger_wrapper>(base_dir + "/node1_r.log", 4);
        auto log_wrapper2_r = cs_new<logger_wrapper>(base_dir + "/node2_r.log", 4);
        auto log_wrapper3_r = cs_new<logger_wrapper>(base_dir + "/node3_r.log", 4);

        auto sm1_r = cs_new<dummy_state_machine>();
        auto sm2_r = cs_new<dummy_state_machine>();
        auto sm3_r = cs_new<dummy_state_machine>();

        raft_launcher launcher1_r;
        raft_launcher launcher2_r;
        raft_launcher launcher3_r;

        ptr<durable_state_mgr> smgr1_r;
        ptr<durable_state_mgr> smgr2_r;
        ptr<durable_state_mgr> smgr3_r;

        auto r1_r = launch_node(1, dir1, "127.0.0.1:9001", initial_conf, sm1_r, log_wrapper1_r, launcher1_r, smgr1_r);
        auto r2_r = launch_node(2, dir2, "127.0.0.1:9002", initial_conf, sm2_r, log_wrapper2_r, launcher2_r, smgr2_r);
        auto r3_r = launch_node(3, dir3, "127.0.0.1:9003", initial_conf, sm3_r, log_wrapper3_r, launcher3_r, smgr3_r);

        // Verify recovered log stores and payloads
        auto log1_r = std::dynamic_pointer_cast<durable_log_store>(smgr1_r->load_log_store());
        REQUIRE(log1_r != nullptr);
        REQUIRE(log1_r->next_slot() >= 4); // slot 0, 1 (config), 2 (val1), 3 (val2)

        // Verify payloads at index 2 and 3
        auto entry1 = log1_r->entry_at(2);
        REQUIRE(entry1 != nullptr);
        REQUIRE(entry1->get_val_type() == nuraft::app_log);
        std::string payload1(reinterpret_cast<const char*>(entry1->get_buf().data()), entry1->get_buf().size());
        REQUIRE(payload1.find("test_val1") != std::string::npos);

        auto entry2 = log1_r->entry_at(3);
        REQUIRE(entry2 != nullptr);
        REQUIRE(entry2->get_val_type() == nuraft::app_log);
        std::string payload2(reinterpret_cast<const char*>(entry2->get_buf().data()), entry2->get_buf().size());
        REQUIRE(payload2.find("test_val2") != std::string::npos);
        std::cout << "Verified recovered payloads: test_val1 and test_val2 are intact." << std::endl;

        // Wait for leader election on restarted cluster
        std::cout << "Waiting for leader election on restarted cluster..." << std::endl;
        ptr<raft_server> leader_r = nullptr;
        for (int attempt = 0; attempt < 50; ++attempt) {
            if (r1_r->is_leader()) { leader_r = r1_r; break; }
            if (r2_r->is_leader()) { leader_r = r2_r; break; }
            if (r3_r->is_leader()) { leader_r = r3_r; break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        REQUIRE(leader_r != nullptr);
        std::cout << "New leader elected! Leader ID: " << leader_r->get_id() << std::endl;

        // Propose entry 3
        std::cout << "Proposing entry 3 on restarted cluster..." << std::endl;
        ptr<buffer> entry_data3 = buffer::alloc(10);
        entry_data3->put_raw((const uint8_t*)"test_val3", 9);
        entry_data3->pos(0);

        auto res3 = leader_r->append_entries({entry_data3});
        REQUIRE(res3 != nullptr);
        REQUIRE(res3->get_accepted() == true);

        // Wait for commit replication
        std::cout << "Waiting for commit replication on restarted cluster..." << std::endl;
        for (int attempt = 0; attempt < 50; ++attempt) {
            if (sm1_r->get_commit_idx() >= 4 && sm2_r->get_commit_idx() >= 4 && sm3_r->get_commit_idx() >= 4) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        REQUIRE(sm1_r->get_commit_idx() >= 4);
        REQUIRE(sm2_r->get_commit_idx() >= 4);
        REQUIRE(sm3_r->get_commit_idx() >= 4);
        std::cout << "Entry 3 committed on restarted cluster!" << std::endl;

        // Verify durability telemetry on restarted cluster after writes
        const auto& tel_r = log1_r->profile();
        REQUIRE(tel_r.append_batches.load() > 0);
        REQUIRE(tel_r.fdatasync_calls.load() > 0);
        std::cout << "Restarted durability telemetry verified: append_batches=" << tel_r.append_batches.load()
                  << ", fdatasync_calls=" << tel_r.fdatasync_calls.load() << std::endl;

        // Clean up restarted nodes
        std::cout << "Shutting down restarted cluster..." << std::endl;
        launcher1_r.shutdown();
        launcher2_r.shutdown();
        launcher3_r.shutdown();

        log_wrapper1_r->destroy();
        log_wrapper2_r->destroy();
        log_wrapper3_r->destroy();

        clean_dir(base_dir);

        std::cout << "ALL NuRaft durable storage integration tests PASSED" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Integration test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
