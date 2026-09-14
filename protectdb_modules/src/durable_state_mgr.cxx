// ariabc_pg/src/durable_state_mgr.cxx
// Durable NuRaft state manager implementation.
// Persists srv_state, cluster_config, and identity using atomic writes.

#include "durable_state_mgr.hxx"
#include "durable_log_store.hxx"

#include "nuraft.hxx"

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cstring>

// POSIX
#include <unistd.h>
#include <sys/stat.h>

namespace ariabc_raft {

static constexpr uint32_t MAGIC_SRV_STATE      = 0xAB1CDEFE;
static constexpr uint32_t MAGIC_CLUSTER_CONFIG = 0xAB1CC0FF;

static std::vector<uint8_t> wrap_envelope(uint32_t magic, const std::vector<uint8_t>& payload) {
    std::vector<uint8_t> env;
    env.reserve(16 + payload.size());
    put_le32(env, magic);
    put_le32(env, 1); // format version
    put_le32(env, (uint32_t)payload.size());
    env.insert(env.end(), payload.begin(), payload.end());
    uint32_t crc = crc32_bytes(env.data(), env.size());
    put_le32(env, crc);
    return env;
}

static std::vector<uint8_t> unwrap_envelope(uint32_t expected_magic, const std::vector<uint8_t>& env, const std::string& context) {
    if (env.size() < 16) {
        throw storage_corruption_error(context + " too small to contain envelope");
    }
    uint32_t magic = get_le32(env.data());
    if (magic != expected_magic) {
        throw storage_corruption_error(context + " magic mismatch");
    }
    uint32_t version = get_le32(env.data() + 4);
    if (version != 1) {
        throw storage_corruption_error(context + " version mismatch");
    }
    uint32_t payload_len = get_le32(env.data() + 8);
    if (env.size() != 16 + payload_len) {
        throw storage_corruption_error(context + " size mismatch: expected " + std::to_string(16 + payload_len) + ", got " + std::to_string(env.size()));
    }
    uint32_t expected_crc = get_le32(env.data() + 12 + payload_len);
    uint32_t actual_crc = crc32_bytes(env.data(), 12 + payload_len);
    if (actual_crc != expected_crc) {
        throw storage_corruption_error(context + " CRC mismatch");
    }
    return std::vector<uint8_t>(env.begin() + 12, env.begin() + 12 + payload_len);
}

// -------------------------------------------------------------------------
// Format version for identity.bin
// -------------------------------------------------------------------------
static constexpr uint32_t IDENTITY_FORMAT_VER = 1;

// -------------------------------------------------------------------------
// Helpers: serialize/deserialize a length-prefixed string
// -------------------------------------------------------------------------
static void put_string(std::vector<uint8_t>& buf, const std::string& s) {
    const uint32_t len = static_cast<uint32_t>(s.size());
    put_le32(buf, len);
    buf.insert(buf.end(), s.begin(), s.end());
}

static std::string get_string(const uint8_t*& p, const uint8_t* end) {
    if (p + 4 > end) throw storage_corruption_error("identity.bin truncated (string length)");
    const uint32_t len = get_le32(p);
    p += 4;
    if (p + len > end) throw storage_corruption_error("identity.bin truncated (string data)");
    std::string s(reinterpret_cast<const char*>(p), len);
    p += len;
    return s;
}

// -------------------------------------------------------------------------
// Path helpers
// -------------------------------------------------------------------------
std::string durable_state_mgr::identity_path()       const {
    return cfg_.storage_dir + "/identity.bin";
}
std::string durable_state_mgr::srv_state_path()      const {
    return cfg_.storage_dir + "/srv_state.bin";
}
std::string durable_state_mgr::cluster_config_path() const {
    return cfg_.storage_dir + "/cluster_config.bin";
}
std::string durable_state_mgr::ready_path() const {
    return cfg_.storage_dir + "/storage_ready.bin";
}

// -------------------------------------------------------------------------
// write_identity
// -------------------------------------------------------------------------
void durable_state_mgr::write_identity() {
    std::vector<uint8_t> buf;
    if (!cfg_.raft_epoch_hex.empty()) {
        put_le32(buf, 2); // IDENTITY_FORMAT_VER = 2 when epoch is present
    } else {
        put_le32(buf, 1); // Legacy format version 1
    }
    put_le32(buf, static_cast<uint32_t>(cfg_.node_id));
    put_string(buf, cfg_.endpoint);
    put_string(buf, cfg_.cluster_id);
    if (!cfg_.raft_epoch_hex.empty()) {
        put_string(buf, cfg_.raft_epoch_hex);
    }
    // CRC of the body.
    const uint32_t crc = crc32_bytes(buf.data(), buf.size());
    put_le32(buf, crc);
    atomic_write_file(identity_path(), buf);
}

// -------------------------------------------------------------------------
// verify_identity
// -------------------------------------------------------------------------
void durable_state_mgr::verify_identity() {
    const std::vector<uint8_t> raw = read_entire_file(identity_path());
    if (raw.size() < 12) {
        throw storage_corruption_error("identity.bin too small");
    }
    const uint8_t* p   = raw.data();
    const uint8_t* end = raw.data() + raw.size();

    // CRC check (last 4 bytes are CRC of everything before).
    if (raw.size() < 5) throw storage_corruption_error("identity.bin too small for CRC");
    const uint32_t stored_crc = get_le32(end - 4);
    const uint32_t actual_crc = crc32_bytes(raw.data(), raw.size() - 4);
    if (stored_crc != actual_crc) {
        throw storage_corruption_error(
            "identity.bin CRC mismatch: stored=" + std::to_string(stored_crc) +
            " actual=" + std::to_string(actual_crc));
    }
    end -= 4; // exclude CRC field from parsing.

    const uint32_t ver = get_le32(p); p += 4;
    if (ver != 1 && ver != 2) {
        throw storage_corruption_error(
            "identity.bin unknown format version: " + std::to_string(ver));
    }

    const uint32_t stored_node_id = get_le32(p); p += 4;
    const std::string stored_endpoint   = get_string(p, end);
    const std::string stored_cluster_id = get_string(p, end);

    std::string stored_epoch_hex;
    if (ver == 2) {
        stored_epoch_hex = get_string(p, end);
        recovered_epoch_hex_ = stored_epoch_hex;
    }

    if (static_cast<int>(stored_node_id) != cfg_.node_id) {
        throw std::runtime_error(
            "RAFT_STORAGE_IDENTITY_MISMATCH: stored node_id=" +
            std::to_string(stored_node_id) + " command-line node_id=" +
            std::to_string(cfg_.node_id) + " [" + cfg_.storage_dir + "]");
    }
    if (stored_endpoint != cfg_.endpoint) {
        throw std::runtime_error(
            "RAFT_STORAGE_IDENTITY_MISMATCH: stored endpoint=" + stored_endpoint +
            " command-line endpoint=" + cfg_.endpoint + " [" + cfg_.storage_dir + "]");
    }
    if (!cfg_.cluster_id.empty() && stored_cluster_id != cfg_.cluster_id) {
        throw std::runtime_error(
            "RAFT_STORAGE_IDENTITY_MISMATCH: stored cluster_id=" + stored_cluster_id +
            " command-line cluster_id=" + cfg_.cluster_id + " [" + cfg_.storage_dir + "]");
    }

    // CLI epoch ID = persisted Raft epoch ID verification
    if (!cfg_.raft_epoch_hex.empty()) {
        if (ver < 2) {
            throw std::runtime_error(
                "RAFT_STORAGE_EPOCH_MISMATCH: recovered storage does not support epoch, but safe mode is enabled.");
        }
        if (stored_epoch_hex != cfg_.raft_epoch_hex) {
            throw std::runtime_error(
                "RAFT_STORAGE_EPOCH_MISMATCH: stored epoch_hex=" + stored_epoch_hex +
                " command-line epoch_hex=" + cfg_.raft_epoch_hex + " [" + cfg_.storage_dir + "]");
        }
    }
}

// -------------------------------------------------------------------------
// open_or_create
// -------------------------------------------------------------------------
void durable_state_mgr::open_or_create() {
    // Create storage_dir and log_dir if needed.
    ensure_directory(cfg_.storage_dir);
    ensure_directory(log_dir_);

    // Acquire exclusive lock.
    lock_fd_ = acquire_exclusive_lock(cfg_.storage_dir);

    // Check if identity.bin exists → this is a recovery vs fresh start.
    try {
        struct stat st;
        const bool has_identity = (::stat(identity_path().c_str(), &st) == 0);

        if (has_identity) {
            const bool has_ready = (::stat(ready_path().c_str(), &st) == 0);
            if (!has_ready) {
                throw storage_corruption_error("Raft storage directory contains identity.bin but storage_ready.bin is missing (incomplete/interrupted initialization). Please clean the directory and retry.");
            }
            // Recovering: verify identity, then reload state.
            recovered_ = true;
            verify_identity();

            // Load saved srv_state. Must exist on recovery!
            struct stat ss;
            if (::stat(srv_state_path().c_str(), &ss) != 0) {
                throw storage_corruption_error("Missing srv_state.bin in recovered directory");
            }
            const std::vector<uint8_t> raw_state_env = read_entire_file(srv_state_path());
            const std::vector<uint8_t> raw_state = unwrap_envelope(MAGIC_SRV_STATE, raw_state_env, "srv_state.bin");
            if (raw_state.empty()) {
                throw storage_corruption_error("srv_state.bin payload is empty");
            }
            nuraft::ptr<nuraft::buffer> buf_state = nuraft::buffer::alloc(raw_state.size());
            buf_state->pos(0);
            ::memcpy(buf_state->data(), raw_state.data(), raw_state.size());
            buf_state->pos(0);
            try {
                saved_state_ = nuraft::srv_state::deserialize(*buf_state);
                if (!saved_state_) {
                    throw storage_corruption_error("Failed to deserialize srv_state.bin");
                }
            } catch (const std::exception& e) {
                throw storage_corruption_error(std::string("Failed to deserialize srv_state.bin: ") + e.what());
            }

            // Load saved cluster_config. Must exist on recovery!
            struct stat cs;
            if (::stat(cluster_config_path().c_str(), &cs) != 0) {
                throw storage_corruption_error("Missing cluster_config.bin in recovered directory");
            }
            const std::vector<uint8_t> raw_config_env = read_entire_file(cluster_config_path());
            const std::vector<uint8_t> raw_config = unwrap_envelope(MAGIC_CLUSTER_CONFIG, raw_config_env, "cluster_config.bin");
            if (raw_config.empty()) {
                throw storage_corruption_error("cluster_config.bin payload is empty");
            }
            nuraft::ptr<nuraft::buffer> buf_config = nuraft::buffer::alloc(raw_config.size());
            buf_config->pos(0);
            ::memcpy(buf_config->data(), raw_config.data(), raw_config.size());
            buf_config->pos(0);
            try {
                saved_config_ = nuraft::cluster_config::deserialize(*buf_config);
                if (!saved_config_) {
                    throw storage_corruption_error("Failed to deserialize cluster_config.bin");
                }
            } catch (const std::exception& e) {
                throw storage_corruption_error(std::string("Failed to deserialize cluster_config.bin: ") + e.what());
            }
        } else {
            // Fresh start: write identity.
            recovered_ = false;
            write_identity();

            // Persist an initial Raft state immediately: term = 0, voted_for = -1
            nuraft::srv_state init_state;
            init_state.set_term(0);
            init_state.set_voted_for(-1);
            save_state(init_state);
        }

        // Create the log store only if recovered.
        if (recovered_) {
            // SAFETY: on recovery, the log manifest MUST exist. If it is
            // absent, the log directory was lost or never initialized. Starting
            // with an empty Raft log in this state would silently discard all
            // committed entries and break linearizability. Fail closed instead.
            const std::string manifest_check_path = log_dir_ + "/manifest.bin";
            struct stat mst;
            if (::stat(manifest_check_path.c_str(), &mst) != 0) {
                throw storage_corruption_error(
                    "Recovered storage directory is missing log/manifest.bin. "
                    "The Raft log directory may have been deleted or was never properly "
                    "initialized. Cannot safely start with an empty log. "
                    "Please investigate or clean the directory before retrying.");
            }
            log_store_ = nuraft::cs_new<durable_log_store>(log_dir_);
        }
    } catch (...) {
        if (lock_fd_ >= 0) {
            ::close(lock_fd_);
            lock_fd_ = -1;
        }
        throw;
    }
}

// -------------------------------------------------------------------------
// Constructor
// -------------------------------------------------------------------------
durable_state_mgr::durable_state_mgr(const durable_state_mgr_config& cfg)
    : cfg_(cfg)
    , log_dir_(cfg.storage_dir + "/log")
    , lock_fd_(-1)
    , recovered_(false)
{
    open_or_create();
}

// -------------------------------------------------------------------------
// Destructor
// -------------------------------------------------------------------------
durable_state_mgr::~durable_state_mgr() {
    log_store_.reset();   // flush and close first
    if (lock_fd_ >= 0) {
        // Releasing flock on close.
        ::close(lock_fd_);
        lock_fd_ = -1;
    }
}

// -------------------------------------------------------------------------
// load_config
// -------------------------------------------------------------------------
nuraft::ptr<nuraft::cluster_config> durable_state_mgr::load_config() {
    std::lock_guard<std::recursive_mutex> l(mu_);
    return saved_config_;
}

// -------------------------------------------------------------------------
// save_config
// -------------------------------------------------------------------------
void durable_state_mgr::save_config(const nuraft::cluster_config& config) {
    std::lock_guard<std::recursive_mutex> l(mu_);
    nuraft::ptr<nuraft::buffer> buf = config.serialize();
    std::vector<uint8_t> raw(buf->size());
    buf->pos(0);
    ::memcpy(raw.data(), buf->data(), buf->size());

    std::vector<uint8_t> env = wrap_envelope(MAGIC_CLUSTER_CONFIG, raw);
    atomic_write_file(cluster_config_path(), env);

    // Refresh in-memory copy.
    buf->pos(0);
    saved_config_ = nuraft::cluster_config::deserialize(*buf);
}

// -------------------------------------------------------------------------
// save_state
// -------------------------------------------------------------------------
void durable_state_mgr::save_state(const nuraft::srv_state& state) {
    std::lock_guard<std::recursive_mutex> l(mu_);
    nuraft::ptr<nuraft::buffer> buf = state.serialize();
    std::vector<uint8_t> raw(buf->size());
    buf->pos(0);
    ::memcpy(raw.data(), buf->data(), buf->size());

    std::vector<uint8_t> env = wrap_envelope(MAGIC_SRV_STATE, raw);
    atomic_write_file(srv_state_path(), env);

    // Refresh in-memory copy.
    buf->pos(0);
    saved_state_ = nuraft::srv_state::deserialize(*buf);
}

// -------------------------------------------------------------------------
// read_state
// -------------------------------------------------------------------------
nuraft::ptr<nuraft::srv_state> durable_state_mgr::read_state() {
    std::lock_guard<std::recursive_mutex> l(mu_);
    return saved_state_;
}

// -------------------------------------------------------------------------
// load_log_store
// -------------------------------------------------------------------------
nuraft::ptr<nuraft::log_store> durable_state_mgr::load_log_store() {
    std::lock_guard<std::recursive_mutex> l(mu_);
    if (!log_store_) {
        throw std::runtime_error("load_log_store called but state manager is not initialized (call initialize_fresh first)");
    }
    return log_store_;
}

void durable_state_mgr::initialize_fresh(const nuraft::cluster_config& config) {
    std::lock_guard<std::recursive_mutex> l(mu_);
    if (recovered_) {
        throw std::runtime_error("initialize_fresh called on an already recovered/initialized state manager");
    }
    save_config(config);
    log_store_ = nuraft::cs_new<durable_log_store>(log_dir_);

    // Write storage_ready.bin
    std::vector<uint8_t> ready_data = {0xAB, 0x1C, 0x1E, 0xAD, 0x01, 0x00, 0x00, 0x00}; // magic + version
    atomic_write_file(ready_path(), ready_data);
    recovered_ = true;
}

void durable_state_mgr::simulate_crash() {
    std::lock_guard<std::recursive_mutex> l(mu_);
    if (log_store_) {
        auto d_store = std::dynamic_pointer_cast<durable_log_store>(log_store_);
        if (d_store) {
            d_store->simulate_crash_close();
        }
    }
    if (lock_fd_ >= 0) {
        ::close(lock_fd_);
        lock_fd_ = -1;
    }
    log_store_.reset();
}

// -------------------------------------------------------------------------
// server_id
// -------------------------------------------------------------------------
int32_t durable_state_mgr::server_id() {
    return cfg_.node_id;
}

// -------------------------------------------------------------------------
// system_exit
// -------------------------------------------------------------------------
void durable_state_mgr::system_exit(const int /*exit_code*/) {
    // Nothing special — process will terminate.
}

} // namespace ariabc_raft
