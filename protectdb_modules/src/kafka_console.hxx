#pragma once

#include <cstdint>
#include <string>
#include <thread>
#include <atomic>
#include <vector>
#include <mutex>

struct rd_kafka_s;

namespace ariabc_pg {

struct kafka_producer_stats {
    uint64_t send_calls = 0;
    uint64_t send_ok = 0;
    uint64_t queue_full_retries = 0;
    uint64_t payload_bytes = 0;
    uint64_t producev_ns = 0;
    uint64_t delivery_calls = 0;
    uint64_t delivery_errors = 0;
    uint64_t delivery_ns = 0;
    uint64_t delivery_max_ns = 0;
    uint64_t poll_ns = 0;
    uint64_t backoff_sleep_ns = 0;
    uint64_t flush_ns = 0;
    uint64_t producer_callback_poll_calls = 0;
    uint64_t producer_callback_poll_ns = 0;
    uint64_t delivery_pending_max = 0;
    uint64_t delivery_callback_count = 0;
    uint64_t delivery_pending_current = 0;
    uint64_t pending_crossed_above_8 = 0;
    uint64_t pending_crossed_above_32 = 0;
};

struct kafka_consumer_stats {
    uint64_t poll_calls = 0;
    uint64_t poll_timeouts = 0;
    uint64_t poll_errors = 0;
    uint64_t message_count = 0;
    uint64_t payload_bytes = 0;
    uint64_t poll_ns = 0;
};

enum class kafka_producer_profile : uint8_t {
    result_fast = 0,
    control_durable = 1,
};

struct kafka_consumed_message {
    std::string payload;
    std::string topic;
};

class kafka_console_producer {
public:
    kafka_console_producer();
    ~kafka_console_producer();

    bool start(const std::string& bootstrap,
               const std::string& topic,
               kafka_producer_profile profile,
               std::string& err);

    bool send_line(const std::string& line, std::string& err);
    bool send_payload(const std::string& payload,
                      const std::string& key,
                      std::string& err);
    bool wait_for_delivery(int timeout_ms, std::string& err);

    kafka_producer_stats stats() const;
    uint64_t delivery_pending() const;
    uint64_t delivery_pending_relaxed() const;

    void stop();

    void note_delivery(uint64_t delivery_ns, bool error);

private:
    rd_kafka_s* rk_;
    std::string topic_;
    std::atomic<bool> poll_thread_stop_;
    std::thread poll_thread_;

    mutable std::mutex stats_mutex_;
    kafka_producer_stats stats_;
    std::atomic<uint64_t> delivery_pending_{0};
    std::atomic<uint64_t> pending_crossed_above_8_{0};
    std::atomic<uint64_t> pending_crossed_above_32_{0};
};

class kafka_console_consumer {
public:
    kafka_console_consumer();
    ~kafka_console_consumer();

    bool start_latest(const std::string& bootstrap,
                      const std::string& topic,
                      const std::string& group_id,
                      std::string& err);

    bool start_latest_multi(const std::string& bootstrap,
                            const std::vector<std::string>& topics,
                            const std::string& group_id,
                            std::string& err);

    bool poll_batch_messages(std::vector<kafka_consumed_message>& out,
                             size_t max_batch,
                             int timeout_ms,
                             std::string& err);

    // Poll one Kafka message payload. topic_out is set when available.
    // Returns false with err="timeout" on poll timeout.
    bool poll_message(std::string& payload_out,
                      std::string& topic_out,
                      std::string& err);

    void set_busy_hint(bool busy);

    kafka_consumer_stats stats() const;

    void stop();

private:
    rd_kafka_s* rk_;
    std::atomic<bool> busy_hint_;

    kafka_consumer_stats stats_;
};

} // namespace ariabc_pg
