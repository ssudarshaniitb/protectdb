// ariabc_pg/src/durable_log_store.hxx
// Durable append-only segmented Raft log store.
//
// File format (binary, little-endian):
//   Segment header: 16 bytes
//     magic      : 8 bytes  ('RAFT_SEG')
//     version    : uint32
//     reserved   : uint32
//
//   Each record:  [header(44 bytes)][payload bytes]
//     magic        : uint32  (0xAB1CFACE)
//     format_ver   : uint32
//     record_type  : uint32  (ENTRY=1, TRUNCATE_FROM=2)
//     record_length: uint32  (total = header + payload)
//     raft_log_idx : uint64
//     raft_term    : uint64
//     payload_len  : uint32
//     payload_crc32: uint32
//     header_crc32 : uint32  (CRC of first 40 bytes of header)
//     payload bytes: [payload_len]
//
// Segment files: segment_<first_index_20digits>_g<gen_id_20digits>.log
// Manifest: log/manifest.bin  – stores active segment list (first-index, gen_id)
//
// Durability model:
//   - append() writes to segment file; does NOT fdatasync.
//   - end_of_append_batch() syncs ALL dirty segments (group commit fence).
//   - flush(), write_at(), apply_pack(), close() also sync all dirty segments.
//   - compact() is disabled (throws).
#pragma once

#include "raft_storage_common.hxx"

#include "log_store.hxx"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace nuraft {
class raft_server;
}

namespace ariabc_raft {

// Per-record location in a segment file.
struct log_location {
    uint32_t segment_seq = 0;   // segment sequence number (= first index of segment)
    uint64_t file_offset = 0;   // byte offset of record start in segment
    uint32_t record_size = 0;   // total record bytes (header + payload)
};

// Fsync profiling counters (exported for startup log).
struct log_store_profile {
    std::atomic<uint64_t> append_calls{0};
    std::atomic<uint64_t> append_batches{0};
    std::atomic<uint64_t> bytes_appended{0};
    std::atomic<uint64_t> append_write_total_ns{0};
    std::atomic<uint64_t> append_write_max_ns{0};
    std::atomic<uint64_t> fdatasync_calls{0};
    std::atomic<uint64_t> fdatasync_total_ns{0};
    std::atomic<uint64_t> fdatasync_max_ns{0};
    std::atomic<uint64_t> directory_fsync_total_ns{0};
    std::atomic<uint64_t> directory_fsync_max_ns{0};
    std::atomic<uint64_t> append_batch_entries_max{0};
    std::atomic<uint64_t> append_batch_entries_total{0};

    std::atomic<uint64_t> segment_fdatasync_calls{0};
    std::atomic<uint64_t> directory_fsync_calls{0};
    std::atomic<uint64_t> segment_rollovers{0};
    std::atomic<uint64_t> truncate_records_written{0};
    std::atomic<uint64_t> tail_repairs{0};
    std::atomic<uint64_t> recovery_entries_loaded{0};
    std::atomic<uint64_t> async_flush_jobs{0};
    std::atomic<uint64_t> async_flush_coalesced_jobs{0};
    std::atomic<uint64_t> async_flush_notifications{0};
    std::atomic<uint64_t> async_flush_queue_max{0};
    std::atomic<uint64_t> async_flush_waits{0};

    // Non-atomic, only read after shutdown.
    uint64_t last_durable_index = 0;
};

struct log_store_latency_profile {
    uint64_t append_write_p50_ns = 0;
    uint64_t append_write_p95_ns = 0;
    uint64_t append_write_p99_ns = 0;
    uint64_t fdatasync_p50_ns = 0;
    uint64_t fdatasync_p95_ns = 0;
    uint64_t fdatasync_p99_ns = 0;
    uint64_t directory_fsync_p50_ns = 0;
    uint64_t directory_fsync_p95_ns = 0;
    uint64_t directory_fsync_p99_ns = 0;
};

class durable_log_store : public nuraft::log_store {
public:
    static constexpr uint64_t SEGMENT_MAX_BYTES = 64ULL * 1024ULL * 1024ULL; // 64 MiB

    /// Open or create a durable log store under log_dir.
    explicit durable_log_store(const std::string& log_dir, uint64_t max_segment_size = 64ULL * 1024ULL * 1024ULL);

    ~durable_log_store() override;

    // NuRaft log_store interface -----------------------------------------

    nuraft::ulong next_slot() const override;
    nuraft::ulong start_index() const override;
    nuraft::ptr<nuraft::log_entry> last_entry() const override;
    nuraft::ulong append(nuraft::ptr<nuraft::log_entry>& entry) override;
    void write_at(nuraft::ulong index, nuraft::ptr<nuraft::log_entry>& entry) override;

    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
        log_entries(nuraft::ulong start, nuraft::ulong end) override;

    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
        log_entries_ext(nuraft::ulong start,
                        nuraft::ulong end,
                        nuraft::int64 batch_size_hint_in_bytes = 0) override;

    nuraft::ptr<nuraft::log_entry> entry_at(nuraft::ulong index) override;
    nuraft::ulong term_at(nuraft::ulong index) override;

    nuraft::ptr<nuraft::buffer> pack(nuraft::ulong index, nuraft::int32 cnt) override;
    void apply_pack(nuraft::ulong index, nuraft::buffer& pack) override;

    bool compact(nuraft::ulong last_log_index) override;
    bool flush() override;
    void close();
    void simulate_crash_close();
    nuraft::ulong last_durable_index() override;

    void end_of_append_batch(nuraft::ulong start, nuraft::ulong cnt) override;

    // Enable NuRaft parallel log appending support. Normal append batches
    // are fdatasync'ed by a background worker, and last_durable_index()
    // advances only after the worker finishes.
    void enable_async_flush(nuraft::raft_server* raft);
    void set_raft_server(nuraft::raft_server* raft);
    bool async_flush_enabled() const;

    // Profiling access ---------------------------------------------------
    const log_store_profile& profile() const { return profile_; }
    log_store_latency_profile latency_profile() const;

private:
    // ---- Record types --------------------------------------------------
    static constexpr uint32_t MAGIC         = 0xAB1CFACE;
    // Manifest v1: 16-byte header + num_segs*(8 first_index) + 4 CRC
    // Manifest v2: 16-byte header + 8 next_gen_id + num_segs*(8 first_index + 8 gen_id)
    //              + 4 CRC
    // FORMAT_VER is incremented to 2 to distinguish the two.
    static constexpr uint32_t FORMAT_VER    = 2;
    static constexpr uint32_t FORMAT_VER_V1 = 1;   // legacy, read-only
    static constexpr uint32_t RT_ENTRY      = 1;
    static constexpr uint32_t RT_TRUNCATE   = 2;

    static constexpr size_t OFF_MAGIC       = 0;
    static constexpr size_t OFF_VERSION     = 4;
    static constexpr size_t OFF_TYPE        = 8;
    static constexpr size_t OFF_RECORD_LEN  = 12;
    static constexpr size_t OFF_INDEX       = 16;
    static constexpr size_t OFF_TERM        = 24;
    static constexpr size_t OFF_PAYLOAD_LEN = 32;
    static constexpr size_t OFF_PAYLOAD_CRC = 36;
    static constexpr size_t OFF_HEADER_CRC  = 40;

    static constexpr size_t HEADER_SIZE = 44;
    static constexpr size_t HEADER_CRC_INPUT_SIZE = 40;

    // ---- Internal segment representation --------------------------------
    struct Segment {
        uint64_t    first_index = 0;   // first Raft log index in this segment
        uint64_t    gen_id      = 0;   // unique generation counter (never reused)
        std::string path;
        int         fd   = -1;
        uint64_t    size = 0;          // bytes written
        bool        is_active = false;
    };

    struct AsyncFlushSegment {
        int fd = -1;
        std::string path;
    };

    struct AsyncFlushJob {
        uint64_t target_index = 0;
        std::vector<AsyncFlushSegment> segments;
        std::string context;
    };

    // ---- Initialization -------------------------------------------------
    void open_or_create();
    void load_manifest();
    void save_manifest();
    void scan_and_recover();
    void process_record_on_recovery(const uint8_t* header,
                                    const std::vector<uint8_t>& payload,
                                    uint64_t file_offset,
                                    size_t seg_idx);

    // ---- Segment management --------------------------------------------
    // When persist_manifest=false, the segment header is written and synced
    // but the manifest is NOT saved to disk. The caller is responsible for
    // saving the manifest later, after any durable marker records are synced.
    // This ordering is required by the truncate/rollback flow:
    //   1. create_segment(persist_manifest=false)
    //   2. write TRUNCATE_FROM marker
    //   3. fdatasync marker segment
    //   4. save_manifest()
    //   5. fsync directory
    //   6. unlink obsolete files
    //   7. fsync directory again
    void create_segment(uint64_t first_index, bool persist_manifest = true);
    // Rotate to a new segment if the current one is full, using
    // first_record_index as the first_index of the new segment.
    // This must be the actual Raft index of the record about to be written,
    // so that TRUNCATE_FROM records (which may have an earlier index than
    // next_slot_unlocked()) are stored correctly.
    void rotate_segment_if_needed(uint64_t first_record_index);

    // ---- Record I/O ----------------------------------------------------
    void write_entry_record(uint64_t raft_idx,
                            uint64_t raft_term,
                            const std::vector<uint8_t>& payload);
    void write_truncate_record(uint64_t from_index);

    std::vector<uint8_t> build_header(uint32_t rtype,
                                      uint64_t raft_idx,
                                      uint64_t raft_term,
                                      uint32_t payload_len,
                                      uint32_t payload_crc) const;

    // segment_path: legacy (first_index only) kept for manifest v1 backward compat.
    std::string segment_path(uint64_t first_index, uint64_t gen_id) const;
    std::string segment_path_legacy(uint64_t first_index) const;
    int segment_fd(size_t seg_idx);

    // ---- Helpers -------------------------------------------------------
    static nuraft::ptr<nuraft::log_entry> make_clone(const nuraft::ptr<nuraft::log_entry>& e);
    nuraft::ptr<nuraft::log_entry> sentinel_entry() const;
    uint64_t next_slot_unlocked() const;

    // ---- State ---------------------------------------------------------
    mutable std::mutex mu_;
    std::string log_dir_;

    // Segments in ascending first_index order.
    std::vector<Segment> segments_;

    // In-memory log cache (index → entry).
    std::map<uint64_t, nuraft::ptr<nuraft::log_entry>> logs_;

    // Index → location on disk.
    std::map<uint64_t, log_location> index_locations_;

    uint64_t start_index_       = 1;
    uint64_t last_durable_idx_  = 0;
    bool     dirty_             = false;
    std::set<size_t> dirty_segments_;

    // Monotonically increasing counter; each new segment gets the current value
    // and the counter is incremented. Stored in manifest so recovery always
    // issues higher IDs than any previously created file.
    uint64_t next_gen_id_ = 1;

    void sync_dirty_segments_unlocked(const std::string& context);
    void fdatasync_and_profile(int fd, const std::string& context);
    void fsync_directory_and_profile(const std::string& path);
    void save_durable_watermark_unlocked();
    void load_durable_watermark_unlocked();
    void enqueue_async_flush_locked(std::unique_lock<std::mutex>& lock,
                                    uint64_t target_index,
                                    const std::string& context);
    void wait_for_async_flush_idle_locked(std::unique_lock<std::mutex>& lock);
    void close_async_flush_segment_fds(std::vector<AsyncFlushSegment>& segments);
    void async_flush_loop();
    void throw_if_async_flush_failed_unlocked() const;

    // Manifest: path to manifest file.
    std::string manifest_path_;

    // Durability profile.
    log_store_profile profile_;
    mutable std::mutex latency_samples_mu_;
    std::vector<uint64_t> append_write_samples_ns_;
    std::vector<uint64_t> fdatasync_samples_ns_;
    std::vector<uint64_t> directory_fsync_samples_ns_;

    // Track appended range for end_of_append_batch validation.
    uint64_t batch_first_  = 0;
    uint64_t batch_last_   = 0;
    uint64_t max_segment_size_;

    bool async_flush_enabled_ = false;
    bool async_flush_stop_ = false;
    bool async_flush_active_ = false;
    bool async_flush_failed_ = false;
    std::string async_flush_error_;
    size_t async_flush_max_jobs_ = 128;
    std::deque<AsyncFlushJob> async_flush_jobs_;
    std::condition_variable async_flush_cv_;
    std::condition_variable async_flush_idle_cv_;
    std::thread async_flush_thread_;
    nuraft::raft_server* raft_server_bwd_pointer_ = nullptr;
};

} // namespace ariabc_raft
