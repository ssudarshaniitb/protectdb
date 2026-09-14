#include "kafka_console.hxx"

#ifdef ARIABC_HAVE_RDKAFKA
#include <librdkafka/rdkafka.h>

#include <chrono>
#include <cstdlib>
#include <string.h>
#include <thread>
#include <vector>

namespace ariabc_pg {
namespace {

uint64_t steady_now_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

void update_max_u64(uint64_t& target, uint64_t value) {
    if (value > target) target = value;
}

struct delivery_opaque {
    kafka_console_producer* producer;
    uint64_t enqueue_ns;
};

void producer_delivery_cb(rd_kafka_t*, const rd_kafka_message_t* msg, void*) {
    delivery_opaque* d = msg ? static_cast<delivery_opaque*>(msg->_private) : nullptr;
    if (!d) return;
    const uint64_t now_ns = steady_now_ns();
    const uint64_t delta_ns = (now_ns >= d->enqueue_ns) ? (now_ns - d->enqueue_ns) : 0;
    d->producer->note_delivery(delta_ns, msg && msg->err);
    delete d;
}

std::string rd_errstr(rd_kafka_resp_err_t e) {
    const char* s = rd_kafka_err2str(e);
    return s ? std::string(s) : std::string("rd_kafka error");
}

bool conf_set(rd_kafka_conf_t* conf,
              const char* name,
              const std::string& val,
              std::string& err)
{
    char errstr[512];
    if (rd_kafka_conf_set(conf, name, val.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        err = std::string("rd_kafka_conf_set(") + name + ") failed: " + errstr;
        return false;
    }
    return true;
}

bool conf_set(rd_kafka_conf_t* conf,
              const char* name,
              const char* val,
              std::string& err)
{
    return conf_set(conf, name, std::string(val), err);
}

bool assign_topics_from_beginning(rd_kafka_t* rk,
                                  const std::vector<std::string>& topics,
                                  std::string& err)
{
    if (!rk) {
        err = "consumer handle is null";
        return false;
    }

    for (int attempt = 0; attempt < 20; ++attempt) {
        const rd_kafka_metadata_t* md = nullptr;
        const rd_kafka_resp_err_t md_err =
            rd_kafka_metadata(rk, 1 /* all topics */, nullptr, &md, 2000);
        if (md_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            if (md) rd_kafka_metadata_destroy(md);
            if (attempt + 1 < 20) {
                std::this_thread::sleep_for(std::chrono::milliseconds(250));
                continue;
            }
            err = std::string("rd_kafka_metadata failed: ") + rd_errstr(md_err);
            return false;
        }

        rd_kafka_topic_partition_list_t* plist =
            rd_kafka_topic_partition_list_new(static_cast<int>(topics.size() * 12));
        bool missing_topic = false;
        std::string missing_reason;

        for (size_t i = 0; i < topics.size(); ++i) {
            const std::string& topic = topics[i];
            const rd_kafka_metadata_topic_t* found = nullptr;
            for (int ti = 0; ti < md->topic_cnt; ++ti) {
                const rd_kafka_metadata_topic_t& mt = md->topics[ti];
                if (topic == mt.topic) {
                    found = &mt;
                    break;
                }
            }

            if (!found) {
                missing_topic = true;
                missing_reason = "topic metadata missing: " + topic;
                break;
            }
            if (found->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                missing_topic = true;
                missing_reason =
                    std::string("topic metadata error for ") + topic + ": " + rd_errstr(found->err);
                break;
            }
            if (found->partition_cnt <= 0) {
                missing_topic = true;
                missing_reason = "topic has no partitions: " + topic;
                break;
            }

            for (int pi = 0; pi < found->partition_cnt; ++pi) {
                const int32_t pid = found->partitions[pi].id;
                rd_kafka_topic_partition_t* tp =
                    rd_kafka_topic_partition_list_add(plist, topic.c_str(), pid);
                // start_latest_multi must actually start at the END of the topic.
                // Reading from BEGINNING re-consumes hundreds of thousands of stale
                // result records from prior runs before reaching the current run's
                // votes, which causes the gateway to time out waiting for majority
                // even though the live cluster is publishing correctly.
                //
                // RD_KAFKA_OFFSET_END as the assign offset proved unreliable in
                // practice (librdkafka returned no messages even after fresh
                // publishes appeared in the topic). Query the high-water mark
                // explicitly and assign at that exact offset so the consumer
                // is guaranteed to start above the historical backlog and pick
                // up everything published from this point forward.
                int64_t lo = 0, hi = 0;
                const rd_kafka_resp_err_t wm_err =
                    rd_kafka_query_watermark_offsets(rk, topic.c_str(), pid,
                                                     &lo, &hi, 5000);
                if (wm_err == RD_KAFKA_RESP_ERR_NO_ERROR && hi >= 0) {
                    tp->offset = hi;
                } else {
                    tp->offset = RD_KAFKA_OFFSET_END;
                }
            }
        }

        rd_kafka_metadata_destroy(md);

        if (missing_topic) {
            rd_kafka_topic_partition_list_destroy(plist);
            if (attempt + 1 < 20) {
                std::this_thread::sleep_for(std::chrono::milliseconds(250));
                continue;
            }
            err = missing_reason;
            return false;
        }

        const rd_kafka_resp_err_t assign_err = rd_kafka_assign(rk, plist);
        rd_kafka_topic_partition_list_destroy(plist);
        if (assign_err == RD_KAFKA_RESP_ERR_NO_ERROR) {
            return true;
        }
        if (attempt + 1 < 20) {
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
            continue;
        }
        err = std::string("rd_kafka_assign failed: ") + rd_errstr(assign_err);
        return false;
    }

    err = "unable to assign Kafka partitions";
    return false;
}

} // namespace

kafka_console_producer::kafka_console_producer()
    : rk_(nullptr)
    , topic_()
    , poll_thread_stop_(false)
    , poll_thread_()
{}

kafka_console_producer::~kafka_console_producer() {
    stop();
}

bool kafka_console_producer::start(const std::string& bootstrap,
                                   const std::string& topic,
                                   kafka_producer_profile profile,
                                   std::string& err)
{
    stop();
    if (bootstrap.empty()) {
        err = "kafka bootstrap is empty";
        return false;
    }
    if (topic.empty()) {
        err = "kafka topic is empty";
        return false;
    }

    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set_dr_msg_cb(conf, producer_delivery_cb);
    if (!conf_set(conf, "bootstrap.servers", bootstrap, err)) {
        rd_kafka_conf_destroy(conf);
        return false;
    }

    const char* acks = (profile == kafka_producer_profile::control_durable) ? "all" : "1";
    const char* linger_ms = (profile == kafka_producer_profile::result_fast) ? "0" : "5";
    const char* default_comp = (profile == kafka_producer_profile::result_fast) ? "lz4" : "snappy";
    const char* env_comp = ::getenv("ARIABC_KAFKA_COMPRESSION_TYPE");
    const char* compression = (env_comp && *env_comp) ? env_comp : default_comp;
    if (!conf_set(conf, "acks", acks, err) ||
        !conf_set(conf, "linger.ms", linger_ms, err) ||
        !conf_set(conf, "batch.num.messages", "10000", err) ||
        !conf_set(conf, "batch.size", "1048576", err) ||
        !conf_set(conf, "compression.type", compression, err) ||
        !conf_set(conf, "queue.buffering.max.messages", "1000000", err) ||
        !conf_set(conf, "queue.buffering.max.kbytes", "1048576", err) ||
        !conf_set(conf, "socket.send.buffer.bytes", "4194304", err) ||
        !conf_set(conf, "socket.receive.buffer.bytes", "4194304", err) ||
        !conf_set(conf, "socket.nagle.disable", "true", err)) {
        rd_kafka_conf_destroy(conf);
        return false;
    }

    char errstr[512];
    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        err = std::string("rd_kafka_new(PRODUCER) failed: ") + errstr;
        rd_kafka_conf_destroy(conf);
        return false;
    }

    rk_ = reinterpret_cast<rd_kafka_s*>(rk);
    topic_ = topic;
    poll_thread_stop_.store(false, std::memory_order_relaxed);
    poll_thread_ = std::thread([this]() {
        rd_kafka_t* rk_local = reinterpret_cast<rd_kafka_t*>(rk_);
        uint64_t local_calls = 0;
        uint64_t local_poll_ns = 0;
        while (!poll_thread_stop_.load(std::memory_order_relaxed)) {
            const auto p0 = std::chrono::steady_clock::now();
            rd_kafka_poll(rk_local, 1);
            const auto p1 = std::chrono::steady_clock::now();
            local_poll_ns += static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(p1 - p0).count());
            ++local_calls;
            if (local_calls >= 128) {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                stats_.producer_callback_poll_calls += local_calls;
                stats_.producer_callback_poll_ns += local_poll_ns;
                local_calls = 0;
                local_poll_ns = 0;
            }
        }
        if (local_calls > 0) {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.producer_callback_poll_calls += local_calls;
            stats_.producer_callback_poll_ns += local_poll_ns;
        }
        rd_kafka_poll(rk_local, 0);
    });
    return true;
}

bool kafka_console_producer::send_line(const std::string& line, std::string& err) {
    return send_payload(line + "\n", std::string(), err);
}

bool kafka_console_producer::send_payload(const std::string& payload,
                                          const std::string& key,
                                          std::string& err) {
    if (!rk_) {
        err = "producer not started";
        return false;
    }
    rd_kafka_t* rk = reinterpret_cast<rd_kafka_t*>(rk_);
    for (int attempt = 0; attempt < 6; ++attempt) {
        delivery_opaque* opaque = new delivery_opaque;
        opaque->producer = this;
        opaque->enqueue_ns = steady_now_ns();
        const auto p0 = std::chrono::steady_clock::now();
        const rd_kafka_resp_err_t e = rd_kafka_producev(
            rk,
            RD_KAFKA_V_TOPIC(topic_.c_str()),
            RD_KAFKA_V_PARTITION(RD_KAFKA_PARTITION_UA),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_KEY(key.empty() ? nullptr : key.data(), key.size()),
            RD_KAFKA_V_VALUE(const_cast<char*>(payload.data()), payload.size()),
            RD_KAFKA_V_OPAQUE(opaque),
            RD_KAFKA_V_END);
        const auto p1 = std::chrono::steady_clock::now();

        if (e == RD_KAFKA_RESP_ERR_NO_ERROR) {
            const uint64_t prod_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(p1 - p0).count());
            uint64_t before = delivery_pending_.fetch_add(1, std::memory_order_relaxed);
            uint64_t dp = before + 1;
            {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                ++stats_.send_calls;
                stats_.payload_bytes += static_cast<uint64_t>(payload.size());
                stats_.producev_ns += prod_ns;
                ++stats_.send_ok;
                update_max_u64(stats_.delivery_pending_max, dp);
            }
            if (before == 8) {
                pending_crossed_above_8_.fetch_add(1, std::memory_order_relaxed);
            }
            if (before == 32) {
                pending_crossed_above_32_.fetch_add(1, std::memory_order_relaxed);
            }
            return true;
        }

        delete opaque;
        if (e == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                ++stats_.queue_full_retries;
            }
            const auto q0 = std::chrono::steady_clock::now();
            rd_kafka_poll(rk, 1);
            const auto q1 = std::chrono::steady_clock::now();
            {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                stats_.poll_ns += static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(q1 - q0).count());
            }
            continue;
        }

        err = std::string("rd_kafka_producev failed: ") + rd_errstr(e);
        return false;
    }

    err = "rd_kafka_producev failed: queue full";
    return false;
}

void kafka_console_producer::note_delivery(uint64_t delivery_ns, bool error) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    ++stats_.delivery_callback_count;
    ++stats_.delivery_calls;
    if (error) ++stats_.delivery_errors;
    stats_.delivery_ns += delivery_ns;
    update_max_u64(stats_.delivery_max_ns, delivery_ns);
    if (delivery_pending_.load(std::memory_order_relaxed) > 0) {
        delivery_pending_.fetch_sub(1, std::memory_order_relaxed);
    }
}

kafka_producer_stats kafka_console_producer::stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    kafka_producer_stats s = stats_;
    s.delivery_pending_current = delivery_pending_.load(std::memory_order_relaxed);
    s.pending_crossed_above_8 = pending_crossed_above_8_.load(std::memory_order_relaxed);
    s.pending_crossed_above_32 = pending_crossed_above_32_.load(std::memory_order_relaxed);
    return s;
}

uint64_t kafka_console_producer::delivery_pending() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return delivery_pending_.load(std::memory_order_relaxed);
}

uint64_t kafka_console_producer::delivery_pending_relaxed() const {
    return delivery_pending_.load(std::memory_order_relaxed);
}

bool kafka_console_producer::wait_for_delivery(int timeout_ms, std::string& err) {
    if (!rk_) {
        err = "producer not started";
        return false;
    }

    rd_kafka_t* rk = reinterpret_cast<rd_kafka_t*>(rk_);
    const auto f0 = std::chrono::steady_clock::now();
    const rd_kafka_resp_err_t rc = rd_kafka_flush(rk, timeout_ms);
    const auto f1 = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.flush_ns += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(f1 - f0).count());
    }
    if (rc != RD_KAFKA_RESP_ERR_NO_ERROR) {
        err = std::string("rd_kafka_flush failed: ") + rd_kafka_err2str(rc);
        return false;
    }
    if (delivery_pending_.load(std::memory_order_relaxed) != 0) {
        err = "rd_kafka_flush returned with pending deliveries";
        return false;
    }
    return true;
}

void kafka_console_producer::stop() {
    if (poll_thread_.joinable()) {
        poll_thread_stop_.store(true, std::memory_order_relaxed);
        poll_thread_.join();
    }
    if (rk_) {
        rd_kafka_t* rk = reinterpret_cast<rd_kafka_t*>(rk_);
        const auto f0 = std::chrono::steady_clock::now();
        rd_kafka_flush(rk, 2000);
        const auto f1 = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.flush_ns += static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(f1 - f0).count());
        }
        rd_kafka_destroy(rk);
        rk_ = nullptr;
    }
    topic_.clear();
}

kafka_console_consumer::kafka_console_consumer()
    : rk_(nullptr)
    , busy_hint_(false)
{}

kafka_console_consumer::~kafka_console_consumer() {
    stop();
}

bool kafka_console_consumer::start_latest(const std::string& bootstrap,
                                          const std::string& topic,
                                          const std::string& group_id,
                                          std::string& err)
{
    std::vector<std::string> topics;
    topics.push_back(topic);
    return start_latest_multi(bootstrap, topics, group_id, err);
}

bool kafka_console_consumer::start_latest_multi(const std::string& bootstrap,
                                                const std::vector<std::string>& topics,
                                                const std::string& group_id,
                                                std::string& err)
{
    stop();
    if (bootstrap.empty()) {
        err = "kafka bootstrap is empty";
        return false;
    }
    if (topics.empty()) {
        err = "kafka topics are empty";
        return false;
    }

    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    if (!conf_set(conf, "bootstrap.servers", bootstrap, err)) {
        rd_kafka_conf_destroy(conf);
        return false;
    }

    std::string gid = group_id;
    if (gid.empty()) {
        gid = "ariabc_pg_gateway";
    }

    if (!conf_set(conf, "group.id", gid, err) ||
        !conf_set(conf, "enable.auto.commit", "false", err) ||
        !conf_set(conf, "auto.offset.reset", "latest", err) ||
        // Majority-completion waits on these replies synchronously. Favor
        // low-latency fetch return over large broker-side batching.
        !conf_set(conf, "fetch.min.bytes", "1", err) ||
        !conf_set(conf, "fetch.wait.max.ms", "100", err) ||
        !conf_set(conf, "max.partition.fetch.bytes", "8388608", err) ||
        !conf_set(conf, "socket.receive.buffer.bytes", "4194304", err) ||
        !conf_set(conf, "socket.nagle.disable", "true", err)) {
        rd_kafka_conf_destroy(conf);
        return false;
    }

    char errstr[512];
    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        err = std::string("rd_kafka_new(CONSUMER) failed: ") + errstr;
        rd_kafka_conf_destroy(conf);
        return false;
    }

    rd_kafka_poll_set_consumer(rk);
    if (!assign_topics_from_beginning(rk, topics, err)) {
        rd_kafka_destroy(rk);
        return false;
    }

    rk_ = reinterpret_cast<rd_kafka_s*>(rk);
    busy_hint_.store(false, std::memory_order_relaxed);
    return true;
}

bool kafka_console_consumer::poll_batch_messages(std::vector<kafka_consumed_message>& out,
                                                 size_t max_batch,
                                                 int timeout_ms,
                                                 std::string& err)
{
    out.clear();
    if (!rk_) {
        err = "consumer not started";
        return false;
    }
    if (max_batch == 0) {
        err = "max_batch must be > 0";
        return false;
    }

    rd_kafka_t* rk = reinterpret_cast<rd_kafka_t*>(rk_);
    ++stats_.poll_calls;

    const auto p0 = std::chrono::steady_clock::now();
    rd_kafka_message_t* first = rd_kafka_consumer_poll(rk, timeout_ms);
    const auto p1 = std::chrono::steady_clock::now();
    stats_.poll_ns += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(p1 - p0).count());

    if (!first) {
        ++stats_.poll_timeouts;
        err = "timeout";
        return false;
    }

    auto push_msg = [&](rd_kafka_message_t* msg) {
        kafka_consumed_message m;
        if (msg->rkt) {
            const char* tn = rd_kafka_topic_name(msg->rkt);
            if (tn) m.topic = tn;
        }
        if (msg->payload && msg->len > 0) {
            m.payload.assign(static_cast<const char*>(msg->payload), msg->len);
            stats_.payload_bytes += static_cast<uint64_t>(msg->len);
        }
        out.push_back(std::move(m));
        ++stats_.message_count;
        rd_kafka_message_destroy(msg);
    };

    if (first->err) {
        ++stats_.poll_errors;
        const rd_kafka_resp_err_t me = first->err;
        rd_kafka_message_destroy(first);
        if (me == RD_KAFKA_RESP_ERR__PARTITION_EOF ||
            me == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART ||
            me == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC) {
            ++stats_.poll_timeouts;
            err = "timeout";
            return false;
        }
        err = std::string("consumer poll error: ") + rd_errstr(me);
        return false;
    }
    push_msg(first);

    while (out.size() < max_batch) {
        rd_kafka_message_t* next = rd_kafka_consumer_poll(rk, 0);
        if (!next) break;
        if (next->err) {
            const rd_kafka_resp_err_t me = next->err;
            rd_kafka_message_destroy(next);
            if (me == RD_KAFKA_RESP_ERR__PARTITION_EOF ||
                me == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART ||
                me == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC) {
                break;
            }
            ++stats_.poll_errors;
            err = std::string("consumer poll error: ") + rd_errstr(me);
            return false;
        }
        push_msg(next);
    }

    return true;
}

bool kafka_console_consumer::poll_message(std::string& payload_out,
                                          std::string& topic_out,
                                          std::string& err)
{
    payload_out.clear();
    topic_out.clear();
    std::vector<kafka_consumed_message> out;
    const int timeout = busy_hint_.load(std::memory_order_relaxed) ? 5 : 50;
    if (!poll_batch_messages(out, 1, timeout, err)) {
        return false;
    }
    if (out.empty()) {
        err = "timeout";
        return false;
    }
    payload_out = std::move(out[0].payload);
    topic_out = std::move(out[0].topic);
    return true;
}

void kafka_console_consumer::set_busy_hint(bool busy) {
    busy_hint_.store(busy, std::memory_order_relaxed);
}

kafka_consumer_stats kafka_console_consumer::stats() const {
    return stats_;
}

void kafka_console_consumer::stop() {
    if (rk_) {
        rd_kafka_t* rk = reinterpret_cast<rd_kafka_t*>(rk_);
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
        rk_ = nullptr;
    }
    busy_hint_.store(false, std::memory_order_relaxed);
}

} // namespace ariabc_pg

#else // !ARIABC_HAVE_RDKAFKA — no-op stubs for builds without librdkafka

namespace ariabc_pg {

kafka_console_producer::kafka_console_producer() : rk_(nullptr), poll_thread_stop_(false) {}
kafka_console_producer::~kafka_console_producer() {}

bool kafka_console_producer::start(const std::string&, const std::string&,
                                   kafka_producer_profile, std::string& err) {
    err = "Kafka disabled at build time (KAFKA_OPTIONAL=ON)";
    return false;
}
bool kafka_console_producer::send_line(const std::string&, std::string& err) {
    err = "Kafka disabled";
    return false;
}
bool kafka_console_producer::send_payload(const std::string&, const std::string&,
                                          std::string& err) {
    err = "Kafka disabled";
    return false;
}
bool kafka_console_producer::wait_for_delivery(int, std::string& err) {
    err = "Kafka disabled";
    return false;
}
void kafka_console_producer::note_delivery(uint64_t, bool) {}
kafka_producer_stats kafka_console_producer::stats() const { return stats_; }
uint64_t kafka_console_producer::delivery_pending() const { return 0; }
uint64_t kafka_console_producer::delivery_pending_relaxed() const { return 0; }
void kafka_console_producer::stop() {}

kafka_console_consumer::kafka_console_consumer() : rk_(nullptr), busy_hint_(false) {}
kafka_console_consumer::~kafka_console_consumer() {}

bool kafka_console_consumer::start_latest(const std::string&, const std::string&,
                                           const std::string&, std::string& err) {
    err = "Kafka disabled at build time (KAFKA_OPTIONAL=ON)";
    return false;
}
bool kafka_console_consumer::start_latest_multi(const std::string&,
                                                  const std::vector<std::string>&,
                                                  const std::string&, std::string& err) {
    err = "Kafka disabled";
    return false;
}
bool kafka_console_consumer::poll_batch_messages(std::vector<kafka_consumed_message>&,
                                                   size_t, int, std::string& err) {
    err = "timeout";
    return false;
}
bool kafka_console_consumer::poll_message(std::string&, std::string&,
                                           std::string& err) {
    err = "timeout";
    return false;
}
void kafka_console_consumer::set_busy_hint(bool busy) { busy_hint_.store(busy, std::memory_order_relaxed); }
kafka_consumer_stats kafka_console_consumer::stats() const { return stats_; }
void kafka_console_consumer::stop() {}

} // namespace ariabc_pg

#endif // ARIABC_HAVE_RDKAFKA
