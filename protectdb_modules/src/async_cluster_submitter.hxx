#pragma once

#include "ariabc_pg_util.hxx"
#include "wire_protocol.hxx"
#include "wire_protocol_async.hxx"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace ariabc_pg {

struct async_submitter_stats {
    uint64_t attempts = 0;
    uint64_t connect_calls = 0;
    uint64_t connect_ns = 0;
    uint64_t write_calls = 0;
    uint64_t write_ns = 0;
    uint64_t read_calls = 0;
    uint64_t read_ns = 0;
    uint64_t not_accepted = 0;
};

// Shared submit reactor for the gateway:
// - owns a small number of persistent nonblocking TCP connections
// - multiplexes request/response frames via poll()
// - supports pipelining multiple in-flight requests per connection
// - optional conn_fanout: open N parallel sockets per logical node, so
//   gateway-side write paths and server-side per-connection handler threads
//   scale beyond one per node. Round-robins per-node when dispatching.
class async_cluster_submitter {
public:
    struct submit_ctx {
        // Physical connection index (logical_node_idx * conn_fanout + lane).
        // The caller passes a logical node_idx into submit_async_to_node and
        // submit_to_node; this field stores the chosen physical conn for the
        // io_loop's dispatch.
        size_t node_idx = 0;
        client_api_request req;
        client_api_response resp;
        std::string err;
        bool done = false;
        bool io_ok = false;
        std::mutex mu;
        std::condition_variable cv;
    };

    async_cluster_submitter();
    ~async_cluster_submitter();

    // conn_fanout >= 1. If conn_fanout > 1, opens N sockets per logical node.
    bool start(const std::vector<host_port>& nodes, std::string& err);
    bool start(const std::vector<host_port>& nodes, size_t conn_fanout, std::string& err);
    void stop();

    // Submit exactly one request to the specified node index and wait for its response.
    // Returns false on I/O failure. On success returns true and fills out_resp.
    bool submit_to_node(size_t node_idx,
                        const client_api_request& req,
                        client_api_response& out_resp,
                        std::string& err);

    bool submit_async_to_node(size_t node_idx,
                              const client_api_request& req,
                              std::shared_ptr<submit_ctx>& out_ctx,
                              std::string& err);

    bool submit_async_to_node_lane(size_t node_idx,
                                   size_t lane,
                                   const client_api_request& req,
                                   std::shared_ptr<submit_ctx>& out_ctx,
                                   std::string& err);

    bool wait_submit(const std::shared_ptr<submit_ctx>& ctx,
                     client_api_response& out_resp,
                     std::string& err);

    bool try_collect_submit(const std::shared_ptr<submit_ctx>& ctx,
                            client_api_response& out_resp,
                            std::string& err,
                            bool& done);

    async_submitter_stats stats() const;

private:
    struct node_conn {
        host_port hp;
        int fd = -1;
        io_buffer in;
        std::string out;
        size_t out_off = 0;
        std::deque<std::shared_ptr<submit_ctx>> inflight;
    };

    void io_loop();
    bool ensure_connected(node_conn& nc, std::string& err);
    void close_conn(node_conn& nc);
    void fail_inflight(node_conn& nc, const std::string& err);

    std::atomic<bool> stop_{false};
    std::thread io_thread_;

    // Wakeup pipe (self-pipe trick).
    int wake_rfd_ = -1;
    int wake_wfd_ = -1;

    // Pending submissions from callers.
    mutable std::mutex mu_;
    std::deque<std::shared_ptr<submit_ctx>> pending_;

    std::vector<node_conn> conns_;

    // Mapping/configuration for multi-conn-per-node fanout.
    size_t num_logical_nodes_ = 0;
    size_t conn_fanout_ = 1;
    // Per-logical-node round-robin counter, used by submit_async_to_node to
    // pick which physical lane to dispatch the next request to.
    std::vector<std::atomic<size_t>> rr_per_node_;

    // Stats updated by the I/O thread.
    std::atomic<uint64_t> st_attempts_{0};
    std::atomic<uint64_t> st_connect_calls_{0};
    std::atomic<uint64_t> st_connect_ns_{0};
    std::atomic<uint64_t> st_write_calls_{0};
    std::atomic<uint64_t> st_write_ns_{0};
    std::atomic<uint64_t> st_read_calls_{0};
    std::atomic<uint64_t> st_read_ns_{0};
    std::atomic<uint64_t> st_not_accepted_{0};
};

} // namespace ariabc_pg
