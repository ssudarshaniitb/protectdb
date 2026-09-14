#include "../src/pg_state_machine.hxx"
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <cstring>
#include <thread>
#include <vector>
#include <string>
#include <openssl/sha.h>
#include <libpq-fe.h>

#define REQUIRE(expr) \
    do { \
        if (!(expr)) { \
            std::cerr << "CRITICAL FAILURE at " << __FILE__ << ":" << __LINE__ << " - requirement failed: " #expr << std::endl; \
            std::abort(); \
        } \
    } while (0)

using namespace ariabc_pg;

// ============================================================================
// Libpq mocks to test safe_sync_startup without a database
// ============================================================================
struct pg_conn {
    // dummy struct
};

struct pg_result {
    ExecStatusType status;
    std::vector<std::vector<std::string>> rows;
};

static int g_mock_claimed_rows_count = 0;
enum class terminal_case {
    valid,
    null_digest,
    digest_31_bytes,
    digest_33_bytes,
    bad_format_version,
    digest_payload_mismatch
};
static terminal_case g_mock_terminal_case = terminal_case::valid;

static uint32_t to_be32(uint32_t val) {
    uint32_t ret = 0;
    ret |= ((val >> 24) & 0xFF);
    ret |= ((val >> 16) & 0xFF) << 8;
    ret |= ((val >> 8) & 0xFF) << 16;
    ret |= (val & 0xFF) << 24;
    return ret;
}

extern "C" {

PGconn* PQconnectdb(const char* conninfo) {
    (void)conninfo;
    return (PGconn*)new pg_conn();
}

ConnStatusType PQstatus(const PGconn* conn) {
    (void)conn;
    return CONNECTION_OK;
}

void PQfinish(PGconn* conn) {
    if (conn) {
        delete (pg_conn*)conn;
    }
}

void PQclear(PGresult* res) {
    if (res) {
        delete (pg_result*)res;
    }
}

ExecStatusType PQresultStatus(const PGresult* res) {
    if (!res) return PGRES_FATAL_ERROR;
    return ((pg_result*)res)->status;
}

int PQntuples(const PGresult* res) {
    if (!res) return 0;
    return ((pg_result*)res)->rows.size();
}

int PQnfields(const PGresult* res) {
    if (!res || ((pg_result*)res)->rows.empty()) return 0;
    return ((pg_result*)res)->rows[0].size();
}

char* PQgetvalue(const PGresult* res, int tup_num, int field_num) {
    if (!res) return nullptr;
    auto* r = (pg_result*)res;
    if (tup_num < 0 || tup_num >= (int)r->rows.size()) return nullptr;
    if (field_num < 0 || field_num >= (int)r->rows[tup_num].size()) return nullptr;
    return const_cast<char*>(r->rows[tup_num][field_num].data());
}

int PQgetisnull(const PGresult* res, int tup_num, int field_num) {
    if (!res) return 1;
    auto* r = (pg_result*)res;
    if (tup_num < 0 || tup_num >= (int)r->rows.size()) return 1;
    if (field_num < 0 || field_num >= (int)r->rows[tup_num].size()) return 1;
    if (r->rows[tup_num][field_num] == "__NULL__") return 1;
    return 0;
}

int PQgetlength(const PGresult* res, int tup_num, int field_num) {
    if (!res) return 0;
    auto* r = (pg_result*)res;
    if (tup_num < 0 || tup_num >= (int)r->rows.size()) return 0;
    if (field_num < 0 || field_num >= (int)r->rows[tup_num].size()) return 0;
    if (r->rows[tup_num][field_num] == "__NULL__") return 0;
    return r->rows[tup_num][field_num].size();
}

static std::string compute_test_terminal_digest(bool is_error, int fmtver, const std::string& sqlstate, const std::string& payload) {
    unsigned char computed_hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX ctx;
    SHA256_Init(&ctx);

    const char* prefix = is_error ? "ariabc-terminal-error-v1" : "ariabc-terminal-ok-v1";
    SHA256_Update(&ctx, prefix, strlen(prefix));

    uint32_t fmtver_be = to_be32(static_cast<uint32_t>(fmtver));
    SHA256_Update(&ctx, &fmtver_be, sizeof(fmtver_be));

    if (is_error) {
        uint32_t sqlstate_len = sqlstate.size();
        uint32_t sqlstate_len_be = to_be32(sqlstate_len);
        SHA256_Update(&ctx, &sqlstate_len_be, sizeof(sqlstate_len_be));
        SHA256_Update(&ctx, sqlstate.data(), sqlstate.size());
    }

    uint32_t payload_len = payload.size();
    uint32_t payload_len_be = to_be32(payload_len);
    SHA256_Update(&ctx, &payload_len_be, sizeof(payload_len_be));
    if (!payload.empty()) {
        SHA256_Update(&ctx, payload.data(), payload.size());
    }

    SHA256_Final(computed_hash, &ctx);
    return std::string((char*)computed_hash, SHA256_DIGEST_LENGTH);
}

PGresult* PQexec(PGconn* conn, const char* query) {
    (void)conn;
    std::string q(query);
    pg_result* res = new pg_result();
    res->status = PGRES_TUPLES_OK;

    if (q.find("raft_apply_schema_meta") != std::string::npos) {
        res->rows.push_back({"1", "2", "2"}); // count = 1, min = 2, max = 2
    } else if (q.find("raft_apply_epoch") != std::string::npos) {
        res->rows.push_back({"1"}); // protocol_version = 1
    } else if (q.find("raft_apply_item") != std::string::npos && q.find("state = 1") != std::string::npos) {
        res->rows.push_back({std::to_string(g_mock_claimed_rows_count)});
    } else if (q.find("state IN (2, 3)") != std::string::npos) {
        if (g_mock_terminal_case == terminal_case::null_digest) {
            res->rows.push_back({
                "2",
                "0",
                "2",
                "1",
                "payload_2",
                "__NULL__",
                "__NULL__",
                "__NULL__",
                "__NULL__" // Null digest
            });
        } else if (g_mock_terminal_case == terminal_case::digest_31_bytes) {
            res->rows.push_back({
                "2",
                "0",
                "2",
                "1",
                "payload_2",
                "__NULL__",
                "__NULL__",
                "__NULL__",
                std::string(31, 'x')
            });
        } else if (g_mock_terminal_case == terminal_case::digest_33_bytes) {
            res->rows.push_back({
                "2",
                "0",
                "2",
                "1",
                "payload_2",
                "__NULL__",
                "__NULL__",
                "__NULL__",
                std::string(33, 'x')
            });
        } else if (g_mock_terminal_case == terminal_case::bad_format_version) {
            res->rows.push_back({
                "2",
                "0",
                "2",
                "99", // Unsupported format version
                "payload_2",
                "__NULL__",
                "__NULL__",
                "__NULL__",
                compute_test_terminal_digest(false, 99, "", "payload_2")
            });
        } else if (g_mock_terminal_case == terminal_case::digest_payload_mismatch) {
            res->rows.push_back({
                "2",
                "0",
                "2",
                "1",
                "payload_2",
                "__NULL__",
                "__NULL__",
                "__NULL__",
                compute_test_terminal_digest(false, 1, "", "different_payload")
            });
        } else {
            // Logs 2 and 5 terminal, log 4 is missing/incomplete
            res->rows.push_back({
                "2",
                "0",
                "2",
                "1",
                "payload_2",
                "__NULL__",
                "__NULL__",
                "__NULL__",
                compute_test_terminal_digest(false, 1, "", "payload_2")
            });
            res->rows.push_back({
                "5",
                "0",
                "3",
                "__NULL__",
                "__NULL__",
                "1",
                "42000",
                "error_payload_5",
                compute_test_terminal_digest(true, 1, "42000", "error_payload_5")
            });
        }
    }
    return (PGresult*)res;
}

} // extern "C"

static std::vector<client_api_request_item>
make_request_items(uint64_t token, int count) {
    std::vector<client_api_request_item> items;
    items.reserve(static_cast<size_t>(count));
    for (int i = 0; i < count; ++i) {
        client_api_request_item item;
        item.req_id = "req_" + std::to_string(token) + "_" + std::to_string(i);
        item.sql = "SELECT " + std::to_string(i);
        items.push_back(item);
    }
    return items;
}

static void
test_raft_payload_assigned_det_seq_roundtrip() {
    {
        client_api_request req;
        req.req_id = "req-assigned-zero";
        req.sql = "s 00000000 SELECT 1";
        req.has_assigned_det_seq = true;
        req.assigned_det_seq = 0;

        std::string err;
        nuraft::ptr<nuraft::buffer> log = build_raft_request_log(req, 7, err);
        REQUIRE(log != nullptr);
        REQUIRE(err.empty());

        raft_request_batch batch;
        REQUIRE(parse_raft_request_log(*log, batch, err));
        REQUIRE(batch.leader_node_hint == 7);
        REQUIRE(batch.items.size() == 1);
        REQUIRE(batch.items[0].req_id == req.req_id);
        REQUIRE(batch.items[0].sql == req.sql);
        REQUIRE(batch.items[0].has_assigned_det_seq);
        REQUIRE(batch.items[0].assigned_det_seq == 0);
    }

    {
        client_api_request req;
        client_api_request_item item0;
        item0.req_id = "req-assigned-seven";
        item0.sql = "s 00000007 SELECT 7";
        item0.has_assigned_det_seq = true;
        item0.assigned_det_seq = 7;
        client_api_request_item item1;
        item1.req_id = "req-unassigned";
        item1.sql = "s 00000008 SELECT 8";
        req.batch_items.push_back(item0);
        req.batch_items.push_back(item1);

        std::string err;
        nuraft::ptr<nuraft::buffer> log = build_raft_request_log(req, 8, err);
        REQUIRE(log != nullptr);
        REQUIRE(err.empty());

        raft_request_batch batch;
        REQUIRE(parse_raft_request_log(*log, batch, err));
        REQUIRE(batch.leader_node_hint == 8);
        REQUIRE(batch.items.size() == 2);
        REQUIRE(batch.items[0].has_assigned_det_seq);
        REQUIRE(batch.items[0].assigned_det_seq == 7);
        REQUIRE(!batch.items[1].has_assigned_det_seq);
        REQUIRE(batch.items[1].assigned_det_seq == 0);
    }

    std::cout << "Raft payload assigned DET sequence round-trip tests passed." << std::endl;
}

int main() {
    std::cout << "Running pg_state_machine contract tests..." << std::endl;

    test_raft_payload_assigned_det_seq_roundtrip();

    db_options db_opt;
    db_opt.dbname = "dummy_test";
    db_opt.port = "5432";
    db_opt.raft_epoch_hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
    kafka_options k_opt;
    pg_state_machine sm(1, db_opt, k_opt);

    // 1. Test zero-byte empty commit (Raft no-op entry, data.size() == 0)
    {
        sm.seed_durable_prefix(99);
        nuraft::ptr<nuraft::buffer> empty_buf = nuraft::buffer::alloc(0);
        nuraft::ptr<nuraft::buffer> ack = sm.commit(100, *empty_buf);
        REQUIRE(ack != nullptr);
        REQUIRE(ack->size() == 0);
        REQUIRE(sm.last_commit_index() >= 100);
        std::cout << "Zero-byte no-op commit test passed." << std::endl;
    }

    // 1b. Result-token cleanup: one request succeeds, waits once, all maps empty.
    {
        const uint64_t token = 1000;
        std::vector<client_api_request_item> items = make_request_items(token, 1);
        sm.test_register_result_batch(token, items);
        sm.test_note_result_applied(token);
        std::string failure_reason = "stale";
        REQUIRE(sm.wait_for_result_id(items[0].req_id, 100, &failure_reason));
        REQUIRE(failure_reason.empty());
        auto counts = sm.test_result_tracker_counts();
        REQUIRE(counts.pending_result_counts == 0);
        REQUIRE(counts.result_token_by_req_id == 0);
        REQUIRE(counts.completed_result_tokens == 0);
        REQUIRE(counts.failed_result_tokens == 0);
        REQUIRE(counts.outstanding_waiters == 0);
        REQUIRE(counts.completed_at_ns == 0);
        std::cout << "Result-token single-wait cleanup test passed." << std::endl;
    }

    // 1c. Two request IDs share one token; first wait keeps token, second removes it.
    {
        const uint64_t token = 1001;
        std::vector<client_api_request_item> items = make_request_items(token, 2);
        sm.test_register_result_batch(token, items);
        sm.test_note_result_item_applied(token, 0);
        sm.test_note_result_item_applied(token, 1);
        std::string failure_reason;
        REQUIRE(sm.wait_for_result_id(items[0].req_id, 100, &failure_reason));
        auto counts = sm.test_result_tracker_counts();
        REQUIRE(counts.pending_result_counts == 0);
        REQUIRE(counts.result_token_by_req_id == 1);
        REQUIRE(counts.completed_result_tokens == 1);
        REQUIRE(counts.outstanding_waiters == 1);
        REQUIRE(counts.completed_at_ns == 1);
        REQUIRE(sm.wait_for_result_id(items[1].req_id, 100, &failure_reason));
        counts = sm.test_result_tracker_counts();
        REQUIRE(counts.pending_result_counts == 0);
        REQUIRE(counts.result_token_by_req_id == 0);
        REQUIRE(counts.completed_result_tokens == 0);
        REQUIRE(counts.failed_result_tokens == 0);
        REQUIRE(counts.outstanding_waiters == 0);
        REQUIRE(counts.completed_at_ns == 0);
        std::cout << "Result-token shared-wait cleanup test passed." << std::endl;
    }

    // 1d. Timeout must not consume the request ID or pending token.
    {
        const uint64_t token = 1002;
        std::vector<client_api_request_item> items = make_request_items(token, 1);
        sm.test_register_result_batch(token, items);
        std::string failure_reason;
        REQUIRE(!sm.wait_for_result_id(items[0].req_id, 1, &failure_reason));
        auto counts = sm.test_result_tracker_counts();
        REQUIRE(counts.pending_result_counts == 1);
        REQUIRE(counts.result_token_by_req_id == 1);
        REQUIRE(counts.completed_result_tokens == 0);
        REQUIRE(counts.failed_result_tokens == 0);
        REQUIRE(counts.outstanding_waiters == 1);
        REQUIRE(counts.completed_at_ns == 0);
        sm.test_note_result_applied(token);
        REQUIRE(sm.wait_for_result_id(items[0].req_id, 100, &failure_reason));
        std::cout << "Result-token timeout preservation test passed." << std::endl;
    }

    // 1e. Terminal failure remains visible until every mapped request consumes it.
    {
        const uint64_t token = 1003;
        std::vector<client_api_request_item> items = make_request_items(token, 2);
        sm.test_register_result_batch(token, items);
        sm.test_note_result_failed(token, "boom");
        std::string failure_reason;
        REQUIRE(!sm.wait_for_result_id(items[0].req_id, 100, &failure_reason));
        REQUIRE(failure_reason == "boom");
        auto counts = sm.test_result_tracker_counts();
        REQUIRE(counts.result_token_by_req_id == 1);
        REQUIRE(counts.failed_result_tokens == 1);
        REQUIRE(counts.outstanding_waiters == 1);
        REQUIRE(counts.completed_at_ns == 1);
        failure_reason.clear();
        REQUIRE(!sm.wait_for_result_id(items[1].req_id, 100, &failure_reason));
        REQUIRE(failure_reason == "boom");
        counts = sm.test_result_tracker_counts();
        REQUIRE(counts.pending_result_counts == 0);
        REQUIRE(counts.result_token_by_req_id == 0);
        REQUIRE(counts.completed_result_tokens == 0);
        REQUIRE(counts.failed_result_tokens == 0);
        REQUIRE(counts.outstanding_waiters == 0);
        REQUIRE(counts.completed_at_ns == 0);
        std::cout << "Result-token failure cleanup test passed." << std::endl;
    }

    // 1f. Per-item completion wakes only that request ID while the peer remains pending.
    {
        const uint64_t token = 1004;
        std::vector<client_api_request_item> items = make_request_items(token, 2);
        sm.test_register_result_batch(token, items);
        sm.test_note_result_item_applied(token, 0);
        std::string failure_reason = "stale";
        REQUIRE(sm.wait_for_result_id(items[0].req_id, 100, &failure_reason));
        REQUIRE(failure_reason.empty());
        failure_reason.clear();
        REQUIRE(!sm.wait_for_result_id(items[1].req_id, 1, &failure_reason));
        auto counts = sm.test_result_tracker_counts();
        REQUIRE(counts.pending_result_counts == 1);
        REQUIRE(counts.result_token_by_req_id == 1);
        REQUIRE(counts.completed_result_tokens == 0);
        REQUIRE(counts.failed_result_tokens == 0);
        REQUIRE(counts.outstanding_waiters == 1);
        sm.test_note_result_item_applied(token, 1);
        REQUIRE(sm.wait_for_result_id(items[1].req_id, 100, &failure_reason));
        REQUIRE(failure_reason.empty());
        counts = sm.test_result_tracker_counts();
        REQUIRE(counts.pending_result_counts == 0);
        REQUIRE(counts.result_token_by_req_id == 0);
        REQUIRE(counts.completed_result_tokens == 0);
        REQUIRE(counts.failed_result_tokens == 0);
        REQUIRE(counts.outstanding_waiters == 0);
        std::cout << "Per-item completion isolation test passed." << std::endl;
    }

    // 1g. Item failure fails that item and the legacy token, without poisoning peer item success.
    {
        const uint64_t token = 1005;
        std::vector<client_api_request_item> items = make_request_items(token, 2);
        sm.test_register_result_batch(token, items);
        sm.test_note_result_item_failed(token, 0, "boom");
        std::string failure_reason;
        REQUIRE(!sm.wait_for_result(token, 100, &failure_reason));
        REQUIRE(failure_reason == "boom");
        failure_reason.clear();
        REQUIRE(!sm.wait_for_result_id(items[0].req_id, 100, &failure_reason));
        REQUIRE(failure_reason == "boom");
        failure_reason.clear();
        sm.test_note_result_item_applied(token, 1);
        REQUIRE(sm.wait_for_result_id(items[1].req_id, 100, &failure_reason));
        REQUIRE(failure_reason.empty());
        auto counts = sm.test_result_tracker_counts();
        REQUIRE(counts.pending_result_counts == 0);
        REQUIRE(counts.result_token_by_req_id == 0);
        REQUIRE(counts.completed_result_tokens == 0);
        REQUIRE(counts.failed_result_tokens == 0);
        REQUIRE(counts.outstanding_waiters == 0);
        std::cout << "Per-item failure plus legacy-token failure test passed." << std::endl;
    }

    // 1h. Abandoned terminal waits are TTL-cleaned, keeping long runs bounded.
    {
        setenv("ARIABC_RESULT_TOKEN_TTL_MS", "1", 1);
        for (uint64_t i = 0; i < 100000; ++i) {
            const uint64_t token = 200000 + i;
            std::vector<client_api_request_item> items = make_request_items(token, 1);
            sm.test_register_result_batch(token, items);
            sm.test_note_result_applied(token);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        sm.test_force_result_cleanup();
        unsetenv("ARIABC_RESULT_TOKEN_TTL_MS");
        auto counts = sm.test_result_tracker_counts();
        REQUIRE(counts.pending_result_counts == 0);
        REQUIRE(counts.result_token_by_req_id == 0);
        REQUIRE(counts.completed_result_tokens == 0);
        REQUIRE(counts.failed_result_tokens == 0);
        REQUIRE(counts.outstanding_waiters == 0);
        REQUIRE(counts.completed_at_ns == 0);
        std::cout << "Result-token TTL cleanup boundedness test passed." << std::endl;
    }

    // 2. Test commit_config (configuration change entries advance prefix)
    {
        nuraft::ptr<nuraft::cluster_config> dummy_conf;
        sm.commit_config(101, dummy_conf);
        REQUIRE(sm.last_commit_index() >= 101);
        std::cout << "Commit config test passed." << std::endl;
    }

    // 3. Test non-empty buffer that parses as zero-item batch (invalid payload in non-safe mode)
    {
        nuraft::ptr<nuraft::buffer> junk_buf = nuraft::buffer::alloc(4);
        nuraft::buffer_serializer bs(*junk_buf);
        bs.put_u32(0xDEADBEEF);
        nuraft::ptr<nuraft::buffer> ack = sm.commit(102, *junk_buf);
        REQUIRE(ack != nullptr);
        std::cout << "Invalid-payload non-safe commit test passed." << std::endl;
    }

    // 4. Test safe_sync_startup with logs 2 and 5, missing 4
    {
        db_options db_opt_safe;
        db_opt_safe.dbname = "dummy_test";
        db_opt_safe.port = "5432";
        db_opt_safe.raft_epoch_hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
        db_opt_safe.raft_apply_ledger_mode = "safe";
        kafka_options k_opt_safe;
        pg_state_machine sm_safe(1, db_opt_safe, k_opt_safe);

        g_mock_claimed_rows_count = 0;
        g_mock_terminal_case = terminal_case::valid;

        uint64_t prefix = sm_safe.safe_sync_startup(0);
        REQUIRE(prefix == 0);
        prefix = sm_safe.safe_sync_startup(1);
        REQUIRE(prefix == 0);
        bool compacted_threw = false;
        try {
            sm_safe.safe_sync_startup(2);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: safe v1 requires retained Raft logs") != std::string::npos);
            compacted_threw = true;
        }
        REQUIRE(compacted_threw);
        std::cout << "safe_sync_startup gap test passed: returned 0 for logs 2 and 5, missing 4." << std::endl;
    }

    // 5. Test safe_sync_startup throwing on persistent CLAIMED rows
    {
        db_options db_opt_safe;
        db_opt_safe.dbname = "dummy_test";
        db_opt_safe.port = "5432";
        db_opt_safe.raft_epoch_hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
        db_opt_safe.raft_apply_ledger_mode = "safe";
        kafka_options k_opt_safe;
        pg_state_machine sm_safe(1, db_opt_safe, k_opt_safe);

        g_mock_claimed_rows_count = 3; // 3 claimed rows
        bool threw = false;
        try {
            sm_safe.safe_sync_startup(0);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: persistent_claimed_row") != std::string::npos);
            threw = true;
        }
        REQUIRE(threw);
        std::cout << "safe_sync_startup persistent CLAIMED row check passed." << std::endl;
    }

    // 6. Test safe_sync_startup throwing on malformed terminal digest
    {
        db_options db_opt_safe;
        db_opt_safe.dbname = "dummy_test";
        db_opt_safe.port = "5432";
        db_opt_safe.raft_epoch_hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
        db_opt_safe.raft_apply_ledger_mode = "safe";
        kafka_options k_opt_safe;
        pg_state_machine sm_safe(1, db_opt_safe, k_opt_safe);

        g_mock_claimed_rows_count = 0;
        g_mock_terminal_case = terminal_case::null_digest;
        bool threw = false;
        try {
            sm_safe.safe_sync_startup(0);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: terminal digest malformed") != std::string::npos);
            threw = true;
        }
        REQUIRE(threw);
        g_mock_terminal_case = terminal_case::digest_31_bytes;
        threw = false;
        try {
            sm_safe.safe_sync_startup(0);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: terminal digest malformed") != std::string::npos);
            threw = true;
        }
        REQUIRE(threw);
        g_mock_terminal_case = terminal_case::digest_33_bytes;
        threw = false;
        try {
            sm_safe.safe_sync_startup(0);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: terminal digest malformed") != std::string::npos);
            threw = true;
        }
        REQUIRE(threw);
        std::cout << "safe_sync_startup malformed terminal digest checks passed." << std::endl;
    }

    // 7. Test safe_sync_startup throwing on bad terminal metadata
    {
        db_options db_opt_safe;
        db_opt_safe.dbname = "dummy_test";
        db_opt_safe.port = "5432";
        db_opt_safe.raft_epoch_hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
        db_opt_safe.raft_apply_ledger_mode = "safe";
        kafka_options k_opt_safe;
        pg_state_machine sm_safe(1, db_opt_safe, k_opt_safe);

        g_mock_claimed_rows_count = 0;
        g_mock_terminal_case = terminal_case::bad_format_version;
        bool threw = false;
        try {
            sm_safe.safe_sync_startup(0);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: terminal row metadata bad") != std::string::npos);
            threw = true;
        }
        REQUIRE(threw);
        g_mock_terminal_case = terminal_case::digest_payload_mismatch;
        threw = false;
        try {
            sm_safe.safe_sync_startup(0);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: terminal row metadata bad") != std::string::npos);
            threw = true;
        }
        REQUIRE(threw);
        std::cout << "safe_sync_startup bad terminal metadata checks passed." << std::endl;
    }

    std::cout << "ALL pg_state_machine contract tests PASSED!" << std::endl;
    return 0;
}
