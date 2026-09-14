// ariabc_pg/tests/b4_codec_sig_test.cxx
// Unit tests for B4 payload codec and signature verification.

#include <iostream>
#include <vector>
#include <string>
#include <stdexcept>
#include <cassert>
#include <cstring>
#include <cstdint>

#define REQUIRE(expr) \
    do { \
        if (!(expr)) { \
            std::cerr << "Requirement failed at line " << __LINE__ << ": " << #expr << std::endl; \
            throw std::runtime_error("requirement failed: " #expr); \
        } \
    } while (0)

namespace ariabc_pg {

struct kafka_reply_record {
    uint64_t req_num = 0;
    uint64_t raft_log_idx = 0;
    std::string req_id;
    int node_id = -1;
    int leader_node_id = -1;
    std::string result_hash;
    std::string hash_algo;
    std::string server_sig;
    uint64_t timestamp_ms = 0;
    std::string full_result;
    bool has_full_result = false;

    // B4 / Epoch fields
    uint32_t raft_item_ordinal = 0;
    std::string terminal_state;
    int format_version = 0;
    std::string epoch_hex;
    int wire_version = 3;
};

// Functions to test
extern std::string test_build_bin_batch_payload_v2(const std::vector<std::string>& req_ids,
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
                                                   bool safe_ledger_mode);

extern bool parse_kafka_payload_records(const std::string& payload,
                                        std::vector<kafka_reply_record>& out);

extern bool verify_result_signature(const kafka_reply_record& rec, const std::string& sig_key);

} // namespace ariabc_pg

using namespace ariabc_pg;

void test_b4_codec_success() {
    std::cout << "Running test_b4_codec_success..." << std::endl;

    std::vector<std::string> req_ids = {"req-1", "req-2"};
    std::vector<std::string> results = {"res1", "res2"};
    std::vector<uint64_t> raft_log_idxs = {101, 102};
    std::vector<int> leader_node_hints = {1, 2};
    std::vector<std::string> terminal_digests = {
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    };
    std::vector<uint32_t> raft_item_ordinals = {0, 1};
    std::vector<std::string> terminal_states = {"OK", "ERROR"};
    std::vector<int> format_versions = {1, 1};
    std::string epoch_hex = "1111111111111111111111111111111111111111111111111111111111111111";
    std::string sig_key = "my_secret_key";

    std::string payload = test_build_bin_batch_payload_v2(
        req_ids, results, raft_log_idxs, leader_node_hints,
        terminal_digests, raft_item_ordinals, terminal_states,
        format_versions, 1, sig_key, epoch_hex, true
    );

    REQUIRE(!payload.empty());
    REQUIRE(payload.size() > 8);
    REQUIRE(payload[0] == 'B');
    REQUIRE(payload[1] == '4');

    std::vector<kafka_reply_record> parsed;
    bool success = parse_kafka_payload_records(payload, parsed);
    REQUIRE(success);
    REQUIRE(parsed.size() == 2);

    REQUIRE(parsed[0].req_id == "req-1");
    REQUIRE(parsed[0].raft_log_idx == 101);
    REQUIRE(parsed[0].raft_item_ordinal == 0);
    REQUIRE(parsed[0].node_id == 1);
    REQUIRE(parsed[0].leader_node_id == 1);
    REQUIRE(parsed[0].epoch_hex == epoch_hex);
    REQUIRE(parsed[0].result_hash == terminal_digests[0]);
    REQUIRE(parsed[0].terminal_state == "OK");
    REQUIRE(parsed[0].format_version == 1);

    REQUIRE(parsed[1].req_id == "req-2");
    REQUIRE(parsed[1].raft_log_idx == 102);
    REQUIRE(parsed[1].raft_item_ordinal == 1);
    REQUIRE(parsed[1].node_id == 1);
    REQUIRE(parsed[1].leader_node_id == 2);
    REQUIRE(parsed[1].epoch_hex == epoch_hex);
    REQUIRE(parsed[1].result_hash == terminal_digests[1]);
    REQUIRE(parsed[1].terminal_state == "ERROR");
    REQUIRE(parsed[1].format_version == 1);

    // Verify signatures
    REQUIRE(verify_result_signature(parsed[0], sig_key));
    REQUIRE(verify_result_signature(parsed[1], sig_key));

    // Verify signature verification fails with incorrect key
    REQUIRE(!verify_result_signature(parsed[0], "wrong_key"));

    // Verify signature verification fails with modified field
    parsed[0].terminal_state = "ERROR";
    REQUIRE(!verify_result_signature(parsed[0], sig_key));

    std::cout << "test_b4_codec_success passed." << std::endl;
}

void test_b4_codec_truncation() {
    std::cout << "Running test_b4_codec_truncation..." << std::endl;

    std::vector<std::string> req_ids = {"req-1"};
    std::vector<std::string> results = {"res1"};
    std::vector<uint64_t> raft_log_idxs = {101};
    std::vector<int> leader_node_hints = {1};
    std::vector<std::string> terminal_digests = {
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    };
    std::vector<uint32_t> raft_item_ordinals = {0};
    std::vector<std::string> terminal_states = {"OK"};
    std::vector<int> format_versions = {1};
    std::string epoch_hex = "1111111111111111111111111111111111111111111111111111111111111111";
    std::string sig_key = "my_secret_key";

    std::string payload = test_build_bin_batch_payload_v2(
        req_ids, results, raft_log_idxs, leader_node_hints,
        terminal_digests, raft_item_ordinals, terminal_states,
        format_versions, 1, sig_key, epoch_hex, true
    );

    // Truncate payload byte by byte and make sure parse fails
    for (size_t len = 0; len < payload.size(); ++len) {
        std::string truncated = payload.substr(0, len);
        std::vector<kafka_reply_record> parsed;
        bool success = parse_kafka_payload_records(truncated, parsed);
        REQUIRE(!success);
    }

    std::cout << "test_b4_codec_truncation passed." << std::endl;
}

void test_b4_codec_invalid_inputs() {
    std::cout << "Running test_b4_codec_invalid_inputs..." << std::endl;

    std::vector<std::string> req_ids = {"req-1"};
    std::vector<std::string> results = {"res1"};
    std::vector<uint64_t> raft_log_idxs = {101};
    std::vector<int> leader_node_hints = {1};
    std::vector<std::string> terminal_digests = {"invalid_digest"};
    std::vector<uint32_t> raft_item_ordinals = {0};
    std::vector<std::string> terminal_states = {"OK"};
    std::vector<int> format_versions = {1};
    std::string epoch_hex = "1111111111111111111111111111111111111111111111111111111111111111";
    std::string sig_key = "my_secret_key";

    // test with safe_ledger_mode = false.
    // With safe_ledger_mode = false, it should fallback to canonical_result_hash.
    std::string payload = test_build_bin_batch_payload_v2(
        req_ids, results, raft_log_idxs, leader_node_hints,
        terminal_digests, raft_item_ordinals, terminal_states,
        format_versions, 1, sig_key, epoch_hex, false
    );
    REQUIRE(!payload.empty());
    std::vector<kafka_reply_record> parsed;
    REQUIRE(parse_kafka_payload_records(payload, parsed));
    REQUIRE(parsed.size() == 1);
    // Result hash should be canonical result hash of "res1"
    REQUIRE(parsed[0].result_hash.size() == 64);

    std::cout << "test_b4_codec_invalid_inputs passed." << std::endl;
}

namespace ariabc_pg {
    bool test_vote_store_b4_logic();
}

int main() {
    try {
        test_b4_codec_success();
        test_b4_codec_truncation();
        test_b4_codec_invalid_inputs();
        if (!ariabc_pg::test_vote_store_b4_logic()) {
            std::cerr << "test_vote_store_b4_logic failed" << std::endl;
            return 1;
        }
        std::cout << "test_vote_store_b4_logic passed." << std::endl;
        std::cout << "ALL B4 codec & signature unit tests PASSED!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
