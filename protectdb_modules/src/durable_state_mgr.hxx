// ariabc_pg/src/durable_state_mgr.hxx
// Durable NuRaft state manager: persists srv_state, cluster_config, and
// node identity to local disk using atomic file replacement.
//
// Storage layout under <dir>:
//   LOCK               – exclusive flock (one process per directory)
//   identity.bin       – node_id, endpoint, cluster_id (written once)
//   srv_state.bin      – serialized NuRaft srv_state (term/vote)
//   cluster_config.bin – serialized NuRaft cluster_config
//   log/               – Raft log (managed by durable_log_store)
//
// Use save_state()/save_config() for durable writes.
// Use read_state()/load_config() to recover after restart.
#pragma once

#include "raft_storage_common.hxx"

#include "nuraft.hxx"

#include <string>
#include <memory>
#include <mutex>

namespace ariabc_raft {

struct durable_state_mgr_config {
    std::string storage_dir;   // e.g. /home/neel/ariabc_raft_data/node1
    int         node_id   = 0;
    std::string endpoint;      // host:port used by NuRaft transport
    std::string cluster_id;    // cluster identity string (mismatch → abort)
    std::string raft_epoch_hex; // Commit B2: cluster epoch identifier
};

class durable_state_mgr : public nuraft::state_mgr {
public:
    /// Create or reopen durable state.
    /// Throws on identity mismatch, lock contention, or storage errors.
    explicit durable_state_mgr(const durable_state_mgr_config& cfg);

    ~durable_state_mgr() override;

    // NuRaft state_mgr interface ----------------------------------------

    nuraft::ptr<nuraft::cluster_config> load_config() override;

    void save_config(const nuraft::cluster_config& config) override;

    void save_state(const nuraft::srv_state& state) override;

    nuraft::ptr<nuraft::srv_state> read_state() override;

    nuraft::ptr<nuraft::log_store> load_log_store() override;

    int32_t server_id() override;

    void system_exit(const int exit_code) override;

    // Extra introspection -----------------------------------------------

    bool is_recovered() const {
        std::lock_guard<std::recursive_mutex> l(mu_);
        return recovered_;
    }
    const std::string& recovered_epoch_hex() const {
        std::lock_guard<std::recursive_mutex> l(mu_);
        return recovered_epoch_hex_;
    }
    void initialize_fresh(const nuraft::cluster_config& config);
    void simulate_crash();
    const std::string& storage_dir() const { return cfg_.storage_dir; }
    const std::string& log_dir() const { return log_dir_; }

private:
    void open_or_create();
    void write_identity();
    void verify_identity();

    std::string identity_path()     const;
    std::string srv_state_path()    const;
    std::string cluster_config_path() const;
    std::string ready_path() const;

    durable_state_mgr_config cfg_;
    std::string log_dir_;

    int lock_fd_ = -1;
    bool recovered_ = false;   // true if directory pre-existed
    std::string recovered_epoch_hex_;

    nuraft::ptr<nuraft::cluster_config>  saved_config_;
    nuraft::ptr<nuraft::srv_state>       saved_state_;

    // Log store instance (created once).
    nuraft::ptr<nuraft::log_store> log_store_;
    mutable std::recursive_mutex mu_;
};

} // namespace ariabc_raft
