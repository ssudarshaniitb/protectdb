#include "async_cluster_submitter.hxx"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <iostream>

namespace ariabc_pg {
namespace {

int connect_tcp_blocking(const std::string& host, int port, std::string& err) {
    struct addrinfo hints;
    ::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* res = nullptr;
    const std::string port_s = std::to_string(port);
    const int rc = ::getaddrinfo(host.c_str(), port_s.c_str(), &hints, &res);
    if (rc != 0) {
        err = std::string("getaddrinfo failed: ") + gai_strerror(rc);
        return -1;
    }

    int fd = -1;
    for (struct addrinfo* p = res; p; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;
        if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
            int one = 1;
            (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            ::freeaddrinfo(res);
            return fd;
        }
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(res);
    err = std::string("connect failed: ") + ::strerror(errno);
    return -1;
}

void set_nonblocking(int fd) {
    if (fd < 0) return;
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        (void)::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }
    const int fdflags = ::fcntl(fd, F_GETFD, 0);
    if (fdflags >= 0) {
        (void)::fcntl(fd, F_SETFD, fdflags | FD_CLOEXEC);
    }
}

uint64_t now_ns() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

} // namespace

async_cluster_submitter::async_cluster_submitter() {}

async_cluster_submitter::~async_cluster_submitter() {
    stop();
}

bool async_cluster_submitter::start(const std::vector<host_port>& nodes, std::string& err) {
    return start(nodes, /*conn_fanout=*/1, err);
}

bool async_cluster_submitter::start(const std::vector<host_port>& nodes,
                                    size_t conn_fanout,
                                    std::string& err) {
    err.clear();
    stop_.store(false);

    if (nodes.empty()) {
        err = "no nodes";
        return false;
    }
    if (conn_fanout == 0) conn_fanout = 1;

    int pfd[2];
    if (::pipe(pfd) != 0) {
        err = std::string("pipe failed: ") + ::strerror(errno);
        return false;
    }
    wake_rfd_ = pfd[0];
    wake_wfd_ = pfd[1];
    set_nonblocking(wake_rfd_);
    set_nonblocking(wake_wfd_);

    num_logical_nodes_ = nodes.size();
    conn_fanout_ = conn_fanout;

    // One node_conn per (logical_node, lane). conns_[i].hp = nodes[i / fanout].
    conns_.clear();
    conns_.reserve(nodes.size() * conn_fanout_);
    for (size_t i = 0; i < nodes.size() * conn_fanout_; ++i) {
        node_conn nc;
        nc.hp = nodes[i / conn_fanout_];
        conns_.push_back(std::move(nc));
    }

    // Per-node round-robin counters. std::atomic is non-copyable/movable, so
    // we have to construct in place rather than resize().
    std::vector<std::atomic<size_t>> rr(num_logical_nodes_);
    rr_per_node_.swap(rr);

    io_thread_ = std::thread([this] { io_loop(); });
    return true;
}

void async_cluster_submitter::stop() {
    bool expected = false;
    if (!stop_.compare_exchange_strong(expected, true)) {
        // already stopped
    }
    if (wake_wfd_ >= 0) {
        const uint8_t b = 1;
        (void)::write(wake_wfd_, &b, 1);
    }
    if (io_thread_.joinable()) io_thread_.join();

    for (auto& nc : conns_) {
        close_conn(nc);
    }
    conns_.clear();

    if (wake_rfd_ >= 0) {
        ::close(wake_rfd_);
        wake_rfd_ = -1;
    }
    if (wake_wfd_ >= 0) {
        ::close(wake_wfd_);
        wake_wfd_ = -1;
    }
}

async_submitter_stats async_cluster_submitter::stats() const {
    async_submitter_stats s;
    s.attempts = st_attempts_.load(std::memory_order_relaxed);
    s.connect_calls = st_connect_calls_.load(std::memory_order_relaxed);
    s.connect_ns = st_connect_ns_.load(std::memory_order_relaxed);
    s.write_calls = st_write_calls_.load(std::memory_order_relaxed);
    s.write_ns = st_write_ns_.load(std::memory_order_relaxed);
    s.read_calls = st_read_calls_.load(std::memory_order_relaxed);
    s.read_ns = st_read_ns_.load(std::memory_order_relaxed);
    s.not_accepted = st_not_accepted_.load(std::memory_order_relaxed);
    return s;
}

bool async_cluster_submitter::submit_async_to_node(size_t logical_node_idx,
                                                   const client_api_request& req,
                                                   std::shared_ptr<submit_ctx>& out_ctx,
                                                   std::string& err) {
    if (logical_node_idx >= num_logical_nodes_) {
        err = "invalid node_idx";
        out_ctx.reset();
        return false;
    }

    // Round-robin among the fanout lanes for this logical node, so a server
    // sees multiple concurrent connections from this gateway and per-connection
    // handler threads on the server can parallelize through the BCDB pool.
    const size_t lane = (conn_fanout_ > 1)
        ? (rr_per_node_[logical_node_idx].fetch_add(1, std::memory_order_relaxed) % conn_fanout_)
        : 0;
    return submit_async_to_node_lane(logical_node_idx, lane, req, out_ctx, err);
}

bool async_cluster_submitter::submit_async_to_node_lane(size_t logical_node_idx,
                                                        size_t lane,
                                                        const client_api_request& req,
                                                        std::shared_ptr<submit_ctx>& out_ctx,
                                                        std::string& err) {
    err.clear();
    out_ctx.reset();
    if (stop_.load()) {
        err = "stopped";
        return false;
    }
    if (logical_node_idx >= num_logical_nodes_) {
        err = "invalid node_idx";
        return false;
    }
    if (conn_fanout_ == 0) {
        err = "invalid conn_fanout";
        return false;
    }
    if (lane >= conn_fanout_) {
        lane = conn_fanout_ - 1;
    }

    const size_t physical_idx = logical_node_idx * conn_fanout_ + lane;

    std::shared_ptr<submit_ctx> ctx(new submit_ctx());
    ctx->node_idx = physical_idx;
    ctx->req = req;

    {
        std::lock_guard<std::mutex> lk(mu_);
        pending_.push_back(ctx);
    }
    if (wake_wfd_ >= 0) {
        const uint8_t b = 1;
        (void)::write(wake_wfd_, &b, 1);
    }

    out_ctx = ctx;
    return true;
}

bool async_cluster_submitter::wait_submit(const std::shared_ptr<submit_ctx>& ctx,
                                          client_api_response& out_resp,
                                          std::string& err) {
    err.clear();
    if (!ctx) {
        err = "invalid submit ctx";
        return false;
    }

    std::unique_lock<std::mutex> lk(ctx->mu);
    ctx->cv.wait(lk, [&] { return ctx->done; });
    if (!ctx->io_ok) {
        err = ctx->err.empty() ? std::string("io_failed") : ctx->err;
        return false;
    }
    out_resp = ctx->resp;
    return true;
}

bool async_cluster_submitter::try_collect_submit(const std::shared_ptr<submit_ctx>& ctx,
                                                 client_api_response& out_resp,
                                                 std::string& err,
                                                 bool& done) {
    err.clear();
    done = false;
    if (!ctx) {
        done = true;
        err = "invalid submit ctx";
        return false;
    }

    std::lock_guard<std::mutex> lk(ctx->mu);
    if (!ctx->done) {
        return true;
    }
    done = true;
    if (!ctx->io_ok) {
        err = ctx->err.empty() ? std::string("io_failed") : ctx->err;
        return false;
    }
    out_resp = ctx->resp;
    return true;
}

bool async_cluster_submitter::submit_to_node(size_t node_idx,
                                             const client_api_request& req,
                                             client_api_response& out_resp,
                                             std::string& err) {
    std::shared_ptr<submit_ctx> ctx;
    if (!submit_async_to_node(node_idx, req, ctx, err)) {
        return false;
    }
    return wait_submit(ctx, out_resp, err);
}

bool async_cluster_submitter::ensure_connected(node_conn& nc, std::string& err) {
    err.clear();
    if (nc.fd >= 0) return true;

    st_connect_calls_.fetch_add(1, std::memory_order_relaxed);
    const uint64_t t0 = now_ns();
    const int fd = connect_tcp_blocking(nc.hp.host, nc.hp.port, err);
    const uint64_t t1 = now_ns();
    if (t1 >= t0) {
        st_connect_ns_.fetch_add(t1 - t0, std::memory_order_relaxed);
    }
    if (fd < 0) return false;

    set_nonblocking(fd);
    nc.fd = fd;
    nc.in.data.clear();
    nc.in.off = 0;
    nc.out.clear();
    nc.out_off = 0;
    return true;
}

void async_cluster_submitter::close_conn(node_conn& nc) {
    if (nc.fd >= 0) {
        ::close(nc.fd);
        nc.fd = -1;
    }
    nc.in.data.clear();
    nc.in.off = 0;
    nc.out.clear();
    nc.out_off = 0;
    nc.inflight.clear();
}

void async_cluster_submitter::fail_inflight(node_conn& nc, const std::string& err) {
    while (!nc.inflight.empty()) {
        std::shared_ptr<submit_ctx> ctx = nc.inflight.front();
        nc.inflight.pop_front();
        {
            std::lock_guard<std::mutex> lk(ctx->mu);
            ctx->done = true;
            ctx->io_ok = false;
            ctx->err = err;
        }
        ctx->cv.notify_one();
    }
}

void async_cluster_submitter::io_loop() {
    while (!stop_.load()) {
        // Drain wakeups.
        if (wake_rfd_ >= 0) {
            uint8_t buf[64];
            while (::read(wake_rfd_, buf, sizeof(buf)) > 0) {
            }
        }

        // Move pending submissions into per-node out buffers.
        std::deque<std::shared_ptr<submit_ctx>> work;
        {
            std::lock_guard<std::mutex> lk(mu_);
            work.swap(pending_);
        }
        while (!work.empty()) {
            std::shared_ptr<submit_ctx> ctx = work.front();
            work.pop_front();
            if (!ctx) continue;
            if (ctx->node_idx >= conns_.size()) {
                std::lock_guard<std::mutex> lk(ctx->mu);
                ctx->done = true;
                ctx->io_ok = false;
                ctx->err = "invalid node_idx";
                ctx->cv.notify_one();
                continue;
            }

            node_conn& nc = conns_[ctx->node_idx];
            std::string cerr;
            if (!ensure_connected(nc, cerr)) {
                std::lock_guard<std::mutex> lk(ctx->mu);
                ctx->done = true;
                ctx->io_ok = false;
                ctx->err = cerr.empty() ? std::string("connect_failed") : cerr;
                ctx->cv.notify_one();
                continue;
            }

            std::string frame;
            std::string ferr;
            if (!encode_request_frame(ctx->req, frame, ferr)) {
                std::lock_guard<std::mutex> lk(ctx->mu);
                ctx->done = true;
                ctx->io_ok = false;
                ctx->err = ferr.empty() ? std::string("encode_failed") : ferr;
                ctx->cv.notify_one();
                continue;
            }

            st_attempts_.fetch_add(1, std::memory_order_relaxed);
            nc.out.append(frame);
            nc.inflight.push_back(ctx);
        }

        // Build poll set.
        std::vector<pollfd> pfds;
        pfds.reserve(1 + conns_.size());
        if (wake_rfd_ >= 0) {
            pollfd p;
            p.fd = wake_rfd_;
            p.events = POLLIN;
            p.revents = 0;
            pfds.push_back(p);
        }
        for (const auto& nc : conns_) {
            if (nc.fd < 0) continue;
            short ev = 0;
            if (nc.out_off < nc.out.size()) ev |= POLLOUT;
            if (!nc.inflight.empty()) ev |= POLLIN;
            if (!ev) continue;
            pollfd p;
            p.fd = nc.fd;
            p.events = ev;
            p.revents = 0;
            pfds.push_back(p);
        }

        const int timeout_ms = pfds.empty() ? 50 : 50;
        int prc = 0;
        if (!pfds.empty()) {
            prc = ::poll(pfds.data(), pfds.size(), timeout_ms);
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
            prc = 0;
        }
        if (prc < 0 && errno != EINTR) {
            continue;
        }

        // Drive I/O.
        for (auto& nc : conns_) {
            if (nc.fd < 0) continue;
            short re = 0;
            for (const auto& p : pfds) {
                if (p.fd == nc.fd) {
                    re = p.revents;
                    break;
                }
            }
            if (!re) continue;

            if ((re & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                fail_inflight(nc, "socket_error");
                close_conn(nc);
                continue;
            }

            if ((re & POLLOUT) && nc.out_off < nc.out.size()) {
                st_write_calls_.fetch_add(1, std::memory_order_relaxed);
                const uint64_t t0 = now_ns();
                const ssize_t w = ::write(nc.fd, nc.out.data() + nc.out_off, nc.out.size() - nc.out_off);
                const uint64_t t1 = now_ns();
                if (t1 >= t0) st_write_ns_.fetch_add(t1 - t0, std::memory_order_relaxed);
                if (w > 0) {
                    nc.out_off += static_cast<size_t>(w);
                    if (nc.out_off >= nc.out.size()) {
                        nc.out.clear();
                        nc.out_off = 0;
                    }
                } else if (w < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                    fail_inflight(nc, std::string("write_failed: ") + ::strerror(errno));
                    close_conn(nc);
                    continue;
                }
            }

            if ((re & POLLIN) && !nc.inflight.empty()) {
                st_read_calls_.fetch_add(1, std::memory_order_relaxed);
                const uint64_t t0 = now_ns();
                char buf[64 * 1024];
                const ssize_t r = ::read(nc.fd, buf, sizeof(buf));
                const uint64_t t1 = now_ns();
                if (t1 >= t0) st_read_ns_.fetch_add(t1 - t0, std::memory_order_relaxed);
                if (r == 0) {
                    fail_inflight(nc, "EOF");
                    close_conn(nc);
                    continue;
                }
                if (r < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                        continue;
                    }
                    fail_inflight(nc, std::string("read_failed: ") + ::strerror(errno));
                    close_conn(nc);
                    continue;
                }
                nc.in.append(buf, static_cast<size_t>(r));

                while (!nc.inflight.empty()) {
                    client_api_response resp;
                    std::string derr;
                    const decode_status st = try_decode_response_frame(nc.in, resp, derr);
                    if (st == decode_status::NEED_MORE) break;
                    if (st == decode_status::ERROR) {
                        fail_inflight(nc, derr.empty() ? std::string("decode_failed") : derr);
                        close_conn(nc);
                        break;
                    }

                    std::shared_ptr<submit_ctx> ctx = nc.inflight.front();
                    nc.inflight.pop_front();
                    if (resp.status == 1) {
                        st_not_accepted_.fetch_add(1, std::memory_order_relaxed);
                    }
                    {
                        std::lock_guard<std::mutex> lk(ctx->mu);
                        ctx->resp = resp;
                        ctx->done = true;
                        ctx->io_ok = true;
                    }
                    ctx->cv.notify_one();
                }
            }
        }
    }

    // Fail any remaining inflight operations on shutdown.
    for (auto& nc : conns_) {
        fail_inflight(nc, "stopped");
    }
}

} // namespace ariabc_pg
