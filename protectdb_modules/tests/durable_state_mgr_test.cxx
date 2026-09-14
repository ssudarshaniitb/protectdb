// ariabc_pg/tests/durable_state_mgr_test.cxx
// Unit tests for durable_state_mgr.

#include "../src/durable_state_mgr.hxx"
#include "nuraft.hxx"
#include <iostream>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include <stdexcept>

#define REQUIRE(expr) \
    do { \
        if (!(expr)) \
            throw std::runtime_error("requirement failed: " #expr); \
    } while (0)

using namespace ariabc_raft;

void test_basic_save_load() {
    std::string test_dir = "./test_state_mgr_dir";
    // Clean up previous run
    ::system(("rm -rf " + test_dir).c_str());

    durable_state_mgr_config cfg;
    cfg.storage_dir = test_dir;
    cfg.node_id = 1;
    cfg.endpoint = "127.0.0.1:9000";
    cfg.cluster_id = "test_cluster";

    {
        durable_state_mgr mgr(cfg);
        REQUIRE(!mgr.is_recovered());

        // Save state
        nuraft::srv_state state;
        state.set_term(5);
        state.set_voted_for(2);
        mgr.save_state(state);

        // Save config via initialize_fresh
        nuraft::ptr<nuraft::srv_config> srv = nuraft::cs_new<nuraft::srv_config>(1, "127.0.0.1:9000");
        nuraft::cluster_config config;
        config.get_servers().push_back(srv);
        mgr.initialize_fresh(config);
    }

    // Reopen and verify
    {
        durable_state_mgr mgr(cfg);
        REQUIRE(mgr.is_recovered());

        auto state = mgr.read_state();
        REQUIRE(state != nullptr);
        REQUIRE(state->get_term() == 5);
        REQUIRE(state->get_voted_for() == 2);

        auto config = mgr.load_config();
        REQUIRE(config != nullptr);
        REQUIRE(config->get_servers().size() == 1);
        REQUIRE(config->get_servers().front()->get_id() == 1);
        REQUIRE(config->get_servers().front()->get_endpoint() == "127.0.0.1:9000");
    }

    std::cout << "test_basic_save_load passed" << std::endl;
}

void test_identity_mismatches() {
    std::string test_dir = "./test_state_mgr_dir";
    ::system(("rm -rf " + test_dir).c_str());

    durable_state_mgr_config cfg;
    cfg.storage_dir = test_dir;
    cfg.node_id = 1;
    cfg.endpoint = "127.0.0.1:9000";
    cfg.cluster_id = "test_cluster";

    {
        durable_state_mgr mgr(cfg);
        nuraft::ptr<nuraft::srv_config> srv = nuraft::cs_new<nuraft::srv_config>(1, "127.0.0.1:9000");
        nuraft::cluster_config config;
        config.get_servers().push_back(srv);
        mgr.initialize_fresh(config);
    }

    // 1. Node ID mismatch
    {
        durable_state_mgr_config bad_cfg = cfg;
        bad_cfg.node_id = 2;
        try {
            durable_state_mgr mgr(bad_cfg);
            REQUIRE(false);
        } catch (const std::exception& e) {
            std::cout << "Node ID mismatch exception: " << e.what() << std::endl;
            REQUIRE(std::string(e.what()).find("IDENTITY_MISMATCH") != std::string::npos);
        }
    }

    // 2. Endpoint mismatch
    {
        durable_state_mgr_config bad_cfg = cfg;
        bad_cfg.endpoint = "127.0.0.1:9001";
        try {
            durable_state_mgr mgr(bad_cfg);
            REQUIRE(false);
        } catch (const std::exception& e) {
            std::cout << "Endpoint mismatch exception: " << e.what() << std::endl;
            REQUIRE(std::string(e.what()).find("IDENTITY_MISMATCH") != std::string::npos);
        }
    }

    // 3. Cluster ID mismatch
    {
        durable_state_mgr_config bad_cfg = cfg;
        bad_cfg.cluster_id = "different_cluster";
        try {
            durable_state_mgr mgr(bad_cfg);
            REQUIRE(false);
        } catch (const std::exception& e) {
            std::cout << "Cluster ID mismatch exception: " << e.what() << std::endl;
            REQUIRE(std::string(e.what()).find("IDENTITY_MISMATCH") != std::string::npos);
        }
    }

    std::cout << "test_identity_mismatches passed" << std::endl;
}

void test_locking() {
    std::string test_dir = "./test_state_mgr_dir";
    ::system(("rm -rf " + test_dir).c_str());

    durable_state_mgr_config cfg;
    cfg.storage_dir = test_dir;
    cfg.node_id = 1;
    cfg.endpoint = "127.0.0.1:9000";
    cfg.cluster_id = "test_cluster";

    durable_state_mgr mgr1(cfg);
    try {
        durable_state_mgr mgr2(cfg);
        REQUIRE(false);
    } catch (const storage_lock_error& e) {
        // Success
    }

    std::cout << "test_locking passed" << std::endl;
}

void test_corrupt_files() {
    std::string test_dir = "./test_state_mgr_dir";
    ::system(("rm -rf " + test_dir).c_str());

    durable_state_mgr_config cfg;
    cfg.storage_dir = test_dir;
    cfg.node_id = 1;
    cfg.endpoint = "127.0.0.1:9000";
    cfg.cluster_id = "test_cluster";

    {
        durable_state_mgr mgr(cfg);
        nuraft::ptr<nuraft::srv_config> srv = nuraft::cs_new<nuraft::srv_config>(1, "127.0.0.1:9000");
        nuraft::cluster_config config;
        config.get_servers().push_back(srv);
        mgr.initialize_fresh(config);
    }

    // Damage identity.bin
    std::ofstream f(test_dir + "/identity.bin", std::ios::binary | std::ios::trunc);
    f << "corrupted data";
    f.close();

    try {
        durable_state_mgr mgr(cfg);
        REQUIRE(false);
    } catch (const storage_corruption_error& e) {
        // Success
    }

    std::cout << "test_corrupt_files passed" << std::endl;
}

void test_state_mgr_corruption_edge_cases() {
    std::string test_dir = "./test_state_mgr_dir";
    ::system(("rm -rf " + test_dir).c_str());

    durable_state_mgr_config cfg;
    cfg.storage_dir = test_dir;
    cfg.node_id = 1;
    cfg.endpoint = "127.0.0.1:9000";
    cfg.cluster_id = "test_cluster";

    // 1. Initial creation
    {
        durable_state_mgr mgr(cfg);
        nuraft::srv_state state;
        state.set_term(3);
        state.set_voted_for(1);
        mgr.save_state(state);

        nuraft::ptr<nuraft::srv_config> srv = nuraft::cs_new<nuraft::srv_config>(1, "127.0.0.1:9000");
        nuraft::cluster_config config;
        config.get_servers().push_back(srv);
        mgr.initialize_fresh(config);
    }

    // 2. Corrupt srv_state.bin Magic
    {
        ::system(("cp " + test_dir + "/srv_state.bin " + test_dir + "/srv_state.bin.bak").c_str());
        ::system(("cp " + test_dir + "/cluster_config.bin " + test_dir + "/cluster_config.bin.bak").c_str());

        std::fstream fs(test_dir + "/srv_state.bin", std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(fs.is_open());
        uint32_t bad_magic = 0x99999999;
        fs.write(reinterpret_cast<char*>(&bad_magic), 4);
        fs.close();

        try {
            durable_state_mgr mgr(cfg);
            REQUIRE(false);
        } catch (const storage_corruption_error& e) {
            std::cout << "Successfully caught bad magic: " << e.what() << std::endl;
        }

        ::system(("cp " + test_dir + "/srv_state.bin.bak " + test_dir + "/srv_state.bin").c_str());
    }

    // 3. Corrupt srv_state.bin CRC
    {
        std::fstream fs(test_dir + "/srv_state.bin", std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(fs.is_open());
        fs.seekp(-4, std::ios::end);
        uint32_t bad_crc = 0x12345678;
        fs.write(reinterpret_cast<char*>(&bad_crc), 4);
        fs.close();

        try {
            durable_state_mgr mgr(cfg);
            REQUIRE(false);
        } catch (const storage_corruption_error& e) {
            std::cout << "Successfully caught bad CRC: " << e.what() << std::endl;
        }

        ::system(("cp " + test_dir + "/srv_state.bin.bak " + test_dir + "/srv_state.bin").c_str());
    }

    // 4. Missing cluster_config.bin on recovery
    {
        ::unlink((test_dir + "/cluster_config.bin").c_str());
        try {
            durable_state_mgr mgr(cfg);
            REQUIRE(false);
        } catch (const storage_corruption_error& e) {
            std::cout << "Successfully caught missing cluster config: " << e.what() << std::endl;
        }
        ::system(("cp " + test_dir + "/cluster_config.bin.bak " + test_dir + "/cluster_config.bin").c_str());
    }

    // 5. Missing srv_state.bin on recovery
    {
        ::unlink((test_dir + "/srv_state.bin").c_str());
        try {
            durable_state_mgr mgr(cfg);
            REQUIRE(false);
        } catch (const storage_corruption_error& e) {
            std::cout << "Successfully caught missing srv state: " << e.what() << std::endl;
        }
        ::system(("cp " + test_dir + "/srv_state.bin.bak " + test_dir + "/srv_state.bin").c_str());
    }

    // 6. Trailing bytes after envelope
    {
        std::ofstream fs(test_dir + "/srv_state.bin", std::ios::app | std::ios::binary);
        REQUIRE(fs.is_open());
        fs << "garbage extra bytes";
        fs.close();

        try {
            durable_state_mgr mgr(cfg);
            REQUIRE(false);
        } catch (const storage_corruption_error& e) {
            std::cout << "Successfully caught trailing garbage: " << e.what() << std::endl;
        }
        ::system(("cp " + test_dir + "/srv_state.bin.bak " + test_dir + "/srv_state.bin").c_str());
    }

    // 7. Corrupt cluster_config.bin Magic
    {
        std::fstream fs(test_dir + "/cluster_config.bin", std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(fs.is_open());
        uint32_t bad_magic = 0x99999999;
        fs.write(reinterpret_cast<char*>(&bad_magic), 4);
        fs.close();

        try {
            durable_state_mgr mgr(cfg);
            REQUIRE(false);
        } catch (const storage_corruption_error& e) {
            std::cout << "Successfully caught bad magic in config: " << e.what() << std::endl;
        }

        ::system(("cp " + test_dir + "/cluster_config.bin.bak " + test_dir + "/cluster_config.bin").c_str());
    }

    // 8. Corrupt cluster_config.bin CRC
    {
        std::fstream fs(test_dir + "/cluster_config.bin", std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(fs.is_open());
        fs.seekp(-4, std::ios::end);
        uint32_t bad_crc = 0x12345678;
        fs.write(reinterpret_cast<char*>(&bad_crc), 4);
        fs.close();

        try {
            durable_state_mgr mgr(cfg);
            REQUIRE(false);
        } catch (const storage_corruption_error& e) {
            std::cout << "Successfully caught bad CRC in config: " << e.what() << std::endl;
        }

        ::system(("cp " + test_dir + "/cluster_config.bin.bak " + test_dir + "/cluster_config.bin").c_str());
    }

    // 9. Valid envelope but invalid/malformed payload
    {
        auto put_le32_helper = [](std::vector<uint8_t>& buf, uint32_t val) {
            buf.push_back((uint8_t)(val & 0xFF));
            buf.push_back((uint8_t)((val >> 8) & 0xFF));
            buf.push_back((uint8_t)((val >> 16) & 0xFF));
            buf.push_back((uint8_t)((val >> 24) & 0xFF));
        };

        std::vector<uint8_t> bad_payload(10, 0xAA);
        std::vector<uint8_t> env;
        put_le32_helper(env, 0xAB1CDEFE); // MAGIC_SRV_STATE
        put_le32_helper(env, 1);
        put_le32_helper(env, (uint32_t)bad_payload.size());
        env.insert(env.end(), bad_payload.begin(), bad_payload.end());
        uint32_t crc = ariabc_raft::crc32_bytes(env.data(), env.size());
        put_le32_helper(env, crc);

        std::ofstream fs(test_dir + "/srv_state.bin", std::ios::binary | std::ios::trunc);
        REQUIRE(fs.is_open());
        fs.write(reinterpret_cast<char*>(env.data()), env.size());
        fs.close();

        try {
            durable_state_mgr mgr(cfg);
            REQUIRE(false);
        } catch (const storage_corruption_error& e) {
            std::cout << "Successfully caught malformed srv_state deserialization: " << e.what() << std::endl;
        }

        ::system(("cp " + test_dir + "/srv_state.bin.bak " + test_dir + "/srv_state.bin").c_str());
    }

    std::cout << "test_state_mgr_corruption_edge_cases passed" << std::endl;
}

void test_incomplete_initialization_fails() {
    std::string test_dir = "./test_state_mgr_dir";
    ::system(("rm -rf " + test_dir).c_str());

    durable_state_mgr_config cfg;
    cfg.storage_dir = test_dir;
    cfg.node_id = 1;
    cfg.endpoint = "127.0.0.1:9000";
    cfg.cluster_id = "test_cluster";

    {
        durable_state_mgr mgr(cfg);
        // Interrupted before calling initialize_fresh.
        // identity.bin and srv_state.bin exist, but storage_ready.bin is missing.
    }

    try {
        durable_state_mgr mgr(cfg);
        REQUIRE(false);
    } catch (const storage_corruption_error& e) {
        std::cout << "Successfully caught incomplete initialization: " << e.what() << std::endl;
        REQUIRE(std::string(e.what()).find("incomplete/interrupted initialization") != std::string::npos);
    }
    std::cout << "test_incomplete_initialization_fails passed" << std::endl;
}

int main() {
    try {
        test_basic_save_load();
        test_identity_mismatches();
        test_locking();
        test_corrupt_files();
        test_state_mgr_corruption_edge_cases();
        test_incomplete_initialization_fails();
        std::cout << "ALL durable_state_mgr tests PASSED" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
