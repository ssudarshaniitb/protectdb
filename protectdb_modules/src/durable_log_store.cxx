// ariabc_pg/src/durable_log_store.cxx
// Durable append-only segmented Raft log store implementation.

#include "durable_log_store.hxx"
#include "nuraft.hxx"
#include <csignal>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

namespace ariabc_raft {

namespace {

void profile_atomic_max(std::atomic<uint64_t>& target, uint64_t value) {
    uint64_t cur = target.load(std::memory_order_relaxed);
    while (value > cur &&
           !target.compare_exchange_weak(cur,
                                         value,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
    }
}

uint64_t percentile_ns(std::vector<uint64_t> samples, double pct) {
    if (samples.empty()) return 0;
    std::sort(samples.begin(), samples.end());
    const double raw_idx = (pct / 100.0) * static_cast<double>(samples.size() - 1);
    const size_t idx = static_cast<size_t>(raw_idx + 0.5);
    return samples[idx >= samples.size() ? samples.size() - 1 : idx];
}

bool env_flag_enabled(const char* name, bool default_value) {
    const char* env = ::getenv(name);
    if (!env || !*env) return default_value;
    const std::string s(env);
    return !(s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
}

uint64_t env_u64(const char* name, uint64_t default_value) {
    const char* env = ::getenv(name);
    if (!env || !*env) return default_value;
    try {
        return static_cast<uint64_t>(std::stoull(env));
    } catch (...) {
        return default_value;
    }
}

bool durable_trace_enabled() {
    static const bool enabled = env_flag_enabled("ARIABC_RAFT_DURABLE_TRACE", false);
    return enabled;
}

} // namespace

static bool parse_segment_filename(const std::string& name, uint64_t& fidx, uint64_t& gen) {
    if (name.find("segment_") != 0) return false;
    if (name.length() < 4 || name.compare(name.length() - 4, 4, ".log") != 0) return false;
    unsigned long long parsed_fidx = 0;
    unsigned long long parsed_gen = 0;
    int parsed = ::sscanf(name.c_str(), "segment_%llu_g%llu.log", &parsed_fidx, &parsed_gen);
    if (parsed != 2) return false;
    char expected_name[128];
    snprintf(expected_name, sizeof(expected_name), "segment_%020llu_g%020llu.log", parsed_fidx, parsed_gen);
    if (name != expected_name) return false;
    fidx = parsed_fidx;
    gen = parsed_gen;
    return true;
}

enum class FilenameType {
    VALID_V2_SEGMENT,
    VALID_V1_SEGMENT,
    MALFORMED_SEGMENT,
    OTHER
};

static FilenameType classify_filename(const std::string& name, uint64_t& fidx, uint64_t& gen) {
    if (name.find("segment_") != 0) {
        return FilenameType::OTHER;
    }
    if (name.length() < 4 || name.compare(name.length() - 4, 4, ".log") != 0) {
        return FilenameType::OTHER;
    }
    // Attempt V2 parse
    if (parse_segment_filename(name, fidx, gen)) {
        return FilenameType::VALID_V2_SEGMENT;
    }
    // Attempt V1 parse: segment_%020llu.log
    unsigned long long parsed_fidx = 0;
    int parsed = ::sscanf(name.c_str(), "segment_%llu.log", &parsed_fidx);
    if (parsed == 1) {
        char expected_name[128];
        snprintf(expected_name, sizeof(expected_name), "segment_%020llu.log", parsed_fidx);
        if (name == expected_name) {
            fidx = parsed_fidx;
            gen = 0;
            return FilenameType::VALID_V1_SEGMENT;
        }
    }
    return FilenameType::MALFORMED_SEGMENT;
}

durable_log_store::durable_log_store(const std::string& log_dir, uint64_t max_segment_size)
    : log_dir_(log_dir)
    , start_index_(1)
    , last_durable_idx_(0)
    , dirty_(false)
    , max_segment_size_(max_segment_size)
{
    manifest_path_ = log_dir_ + "/manifest.bin";
    async_flush_max_jobs_ =
        static_cast<size_t>(env_u64("ARIABC_RAFT_ASYNC_FLUSH_MAX_JOBS", 128));
    if (async_flush_max_jobs_ == 0) {
        async_flush_max_jobs_ = 1;
    }
    open_or_create();
}

durable_log_store::~durable_log_store() {
    close();
}

nuraft::ptr<nuraft::log_entry> durable_log_store::make_clone(const nuraft::ptr<nuraft::log_entry>& e) {
    if (!e) return nullptr;
    return nuraft::cs_new<nuraft::log_entry>(
        e->get_term(),
        nuraft::buffer::clone(e->get_buf()),
        e->get_val_type(),
        e->get_timestamp(),
        e->has_crc32(),
        e->get_crc32(),
        false
    );
}

nuraft::ptr<nuraft::log_entry> durable_log_store::sentinel_entry() const {
    // Dummy entry for index 0.
    nuraft::ptr<nuraft::buffer> buf = nuraft::buffer::alloc(sizeof(uint64_t));
    buf->pos(0);
    buf->put((uint64_t)0);
    return nuraft::cs_new<nuraft::log_entry>(0, buf);
}

uint64_t durable_log_store::next_slot_unlocked() const {
    if (logs_.empty()) {
        return start_index_;
    }
    return logs_.rbegin()->first + 1;
}

nuraft::ulong durable_log_store::next_slot() const {
    std::lock_guard<std::mutex> l(mu_);
    return next_slot_unlocked();
}

nuraft::ulong durable_log_store::start_index() const {
    std::lock_guard<std::mutex> l(mu_);
    return start_index_;
}

nuraft::ptr<nuraft::log_entry> durable_log_store::last_entry() const {
    std::lock_guard<std::mutex> l(mu_);
    if (logs_.empty()) {
        return sentinel_entry();
    }
    return make_clone(logs_.rbegin()->second);
}

// New format: segment_<first_index>_g<gen_id>.log
std::string durable_log_store::segment_path(uint64_t first_index, uint64_t gen_id) const {
    char buf[128];
    snprintf(buf, sizeof(buf), "/segment_%020llu_g%020llu.log",
             (unsigned long long)first_index, (unsigned long long)gen_id);
    return log_dir_ + buf;
}

// Legacy: segment_<first_index>.log (manifest v1 backward compat, read-only)
std::string durable_log_store::segment_path_legacy(uint64_t first_index) const {
    char buf[64];
    snprintf(buf, sizeof(buf), "/segment_%020llu.log", (unsigned long long)first_index);
    return log_dir_ + buf;
}

int durable_log_store::segment_fd(size_t seg_idx) {
    if (segments_[seg_idx].fd < 0) {
        // Open existing segment file for append; do NOT use O_CREAT|O_TRUNC.
        int fd = ::open(segments_[seg_idx].path.c_str(), O_RDWR, 0600);
        if (fd < 0) {
            throw storage_io_error("Failed to open segment file: " + segments_[seg_idx].path + " error: " + strerror(errno));
        }
        // Seek to end
        if (::lseek(fd, 0, SEEK_END) < 0) {
            ::close(fd);
            throw storage_io_error("Failed to seek end of segment: " + segments_[seg_idx].path);
        }
        segments_[seg_idx].fd = fd;
    }
    return segments_[seg_idx].fd;
}

void durable_log_store::create_segment(uint64_t first_index, bool persist_manifest) {
    // Allocate a unique generation ID and build the new segment filename.
    // Using O_CREAT|O_EXCL means we NEVER silently overwrite an existing file.
    // If two segments ever produced the same name (which cannot happen given the
    // monotonically increasing gen_id) the open would fail with EEXIST rather
    // than truncating durable history.
    const uint64_t gen = next_gen_id_++;
    Segment seg;
    seg.first_index = first_index;
    seg.gen_id      = gen;
    seg.path = segment_path(first_index, gen);
    seg.fd = ::open(seg.path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    if (seg.fd < 0) {
        throw storage_io_error("Failed to create segment file: " + seg.path + " error: " + strerror(errno));
    }
    seg.size = 0;
    seg.is_active = true;

    // Write header
    uint8_t seg_hdr[16];
    memset(seg_hdr, 0, sizeof(seg_hdr));
    ::memcpy(seg_hdr, "RAFT_SEG", 8);
    seg_hdr[8] = (uint8_t)(FORMAT_VER & 0xFF);
    seg_hdr[9] = (uint8_t)((FORMAT_VER >> 8) & 0xFF);
    seg_hdr[10] = (uint8_t)((FORMAT_VER >> 16) & 0xFF);
    seg_hdr[11] = (uint8_t)((FORMAT_VER >> 24) & 0xFF);
    write_full(seg.fd, seg_hdr, sizeof(seg_hdr), seg.path);
    seg.size += sizeof(seg_hdr);

    fdatasync_and_profile(seg.fd, "create_segment fdatasync");
    
    if (!segments_.empty()) {
        segments_.back().is_active = false;
        size_t old_active_idx = segments_.size() - 1;
        if (dirty_segments_.count(old_active_idx)) {
            if (segments_.back().fd >= 0) {
                fdatasync_and_profile(segments_.back().fd, "closing dirty segment sync");
            }
            dirty_segments_.erase(old_active_idx);
        }
        if (segments_.back().fd >= 0) {
            ::close(segments_.back().fd);
            segments_.back().fd = -1;
        }
    }
    segments_.push_back(seg);

    // Only persist the manifest immediately when requested.
    // For the truncate/rollback path, the caller writes the TRUNCATE_FROM marker
    // first, syncs it, and then calls save_manifest() to preserve the ordering:
    //   segment file durable → marker durable → manifest durable → unlinks.
    if (persist_manifest) {
        save_manifest();
        fsync_directory_and_profile(log_dir_);
    }
}


void durable_log_store::rotate_segment_if_needed(uint64_t first_record_index) {
    if (segments_.empty()) {
        create_segment(first_record_index);
        return;
    }
    if (segments_.back().size >= max_segment_size_) {
        profile_.segment_rollovers.fetch_add(1, std::memory_order_relaxed);
        create_segment(first_record_index);
    }
}

std::vector<uint8_t> durable_log_store::build_header(
    uint32_t rtype, uint64_t raft_idx, uint64_t raft_term,
    uint32_t payload_len, uint32_t payload_crc) const
{
    std::vector<uint8_t> hdr;
    hdr.reserve(HEADER_SIZE);
    put_le32(hdr, MAGIC);
    put_le32(hdr, FORMAT_VER);
    put_le32(hdr, rtype);
    put_le32(hdr, (uint32_t)(HEADER_SIZE + payload_len));
    put_le64(hdr, raft_idx);
    put_le64(hdr, raft_term);
    put_le32(hdr, payload_len);
    put_le32(hdr, payload_crc);
    
    // Header CRC is CRC of the first 40 bytes (HEADER_CRC_INPUT_SIZE)
    uint32_t hcrc = crc32_bytes(hdr.data(), HEADER_CRC_INPUT_SIZE);
    put_le32(hdr, hcrc);
    return hdr;
}

void durable_log_store::write_entry_record(
    uint64_t raft_idx, uint64_t raft_term, const std::vector<uint8_t>& payload)
{
    rotate_segment_if_needed(raft_idx);
    size_t active_idx = segments_.size() - 1;
    int fd = segment_fd(active_idx);
    uint64_t offset = segments_[active_idx].size;

    uint32_t pcrc = crc32_bytes(payload.data(), payload.size());
    std::vector<uint8_t> hdr = build_header(RT_ENTRY, raft_idx, raft_term, payload.size(), pcrc);

    if (payload.empty()) {
        write_full(fd, hdr.data(), hdr.size(), segments_[active_idx].path);
    } else {
        struct iovec iov[2];
        iov[0].iov_base = hdr.data();
        iov[0].iov_len = hdr.size();
        iov[1].iov_base = const_cast<uint8_t*>(payload.data());
        iov[1].iov_len = payload.size();
        size_t remaining = hdr.size() + payload.size();
        int iovcnt = 2;
        struct iovec* cur_iov = iov;
        while (remaining > 0) {
            ssize_t written = ::writev(fd, cur_iov, iovcnt);
            if (written < 0) {
                if (errno == EINTR) continue;
                throw storage_io_error("writev failed [" + segments_[active_idx].path +
                                       "]: " + strerror(errno));
            }
            if (written == 0) {
                throw storage_io_error("writev: unexpected zero-length write [" +
                                       segments_[active_idx].path + "]");
            }
            remaining -= static_cast<size_t>(written);
            while (written > 0 && iovcnt > 0) {
                if (static_cast<size_t>(written) >= cur_iov[0].iov_len) {
                    written -= static_cast<ssize_t>(cur_iov[0].iov_len);
                    ++cur_iov;
                    --iovcnt;
                } else {
                    cur_iov[0].iov_base =
                        static_cast<uint8_t*>(cur_iov[0].iov_base) + written;
                    cur_iov[0].iov_len -= static_cast<size_t>(written);
                    written = 0;
                }
            }
        }
    }

    segments_[active_idx].size += HEADER_SIZE + payload.size();
    
    log_location loc;
    loc.segment_seq = segments_[active_idx].first_index;
    loc.file_offset = offset;
    loc.record_size = HEADER_SIZE + payload.size();
    index_locations_[raft_idx] = loc;

    profile_.bytes_appended += loc.record_size;
    dirty_segments_.insert(active_idx);
    if (durable_trace_enabled()) {
        std::cerr << "RAFT_DURABLE_LOG_WRITE"
                  << " idx=" << raft_idx
                  << " term=" << raft_term
                  << " payload=" << payload.size()
                  << " path=" << segments_[active_idx].path
                  << " offset=" << offset
                  << " segment_size=" << segments_[active_idx].size
                  << std::endl;
    }
}

void durable_log_store::write_truncate_record(uint64_t from_index) {
    // 1. Identify obsolete segments (first_index >= from_index)
    std::vector<Segment> obsolete_segments;
    while (!segments_.empty() && segments_.back().first_index >= from_index) {
        obsolete_segments.push_back(segments_.back());
        size_t idx = segments_.size() - 1;
        dirty_segments_.erase(idx);
        segments_.pop_back();
    }

    // 2. Ensure we have an active segment for the marker.
    //    CRITICAL: do NOT persist the manifest yet. The new segment must be
    //    durable on disk before the manifest is updated, and the TRUNCATE_FROM
    //    marker must be durable before the manifest excludes old segments.
    //    If we saved the manifest here, a crash before the marker is written
    //    would leave the recovered log empty/incomplete.
    if (segments_.empty() || segments_.back().size >= max_segment_size_) {
        // Need a new segment. Create it WITHOUT persisting the manifest.
        create_segment(from_index, /*persist_manifest=*/false);
    }
    size_t active_idx = segments_.size() - 1;
    int fd = segment_fd(active_idx);

    // FAILPOINT: crash after segment is created but before the marker is written.
    // This validates that recovery handles an orphaned new segment header.
    if (const char* fp = std::getenv("ARIABC_FAILPOINT_CRASH_AFTER_NEW_TRUNCATE_SEGMENT_BEFORE_MARKER"); fp && fp[0] != '\0') {
        if (!obsolete_segments.empty()) {
            std::cerr << "FAILPOINT: crashing after new truncate segment created, before TRUNCATE_FROM marker" << std::endl;
            ::kill(::getpid(), SIGKILL);
            ::_exit(124);
        }
    }

    // 3. Write TRUNCATE_FROM marker record
    std::vector<uint8_t> hdr = build_header(RT_TRUNCATE, from_index, 0, 0, 0);
    write_full(fd, hdr.data(), hdr.size(), segments_[active_idx].path);
    segments_[active_idx].size += HEADER_SIZE;
    dirty_segments_.insert(active_idx);
    profile_.truncate_records_written.fetch_add(1, std::memory_order_relaxed);

    // 4. fdatasync the marker's segment
    //    The marker is now durable: it is safe to persist the manifest.
    fdatasync_and_profile(fd, "truncate marker sync");

    // 5. Update the manifest file on disk (now safe: marker is durable)
    save_manifest();

    // 6. fsync log directory to commit the manifest update
    fsync_directory_and_profile(log_dir_);

    // 7. Physically unlink the obsolete segments
    if (const char* fp = std::getenv("ARIABC_FAILPOINT_CRASH_BEFORE_UNLINK"); fp && fp[0] != '\0') {
        std::cerr << "FAILPOINT: crashing before obsolete segment unlink" << std::endl;
        ::kill(::getpid(), SIGKILL);
        ::_exit(124);
    }
    for (auto& o_seg : obsolete_segments) {
        if (o_seg.fd >= 0) {
            ::close(o_seg.fd);
            o_seg.fd = -1;
        }
        bool keep = false;
        for (auto& seg : segments_) {
            if (seg.first_index == o_seg.first_index && seg.gen_id == o_seg.gen_id) {
                keep = true;
                break;
            }
        }
        if (!keep) {
            if (::unlink(o_seg.path.c_str()) != 0 && errno != ENOENT) {
                throw storage_io_error("Failed to unlink obsolete segment file: " + o_seg.path + " error: " + strerror(errno));
            }
        }
    }

    // 8. fsync log directory again to commit the unlinks
    fsync_directory_and_profile(log_dir_);
}


nuraft::ulong durable_log_store::append(nuraft::ptr<nuraft::log_entry>& entry) {
    std::lock_guard<std::mutex> l(mu_);
    throw_if_async_flush_failed_unlocked();
    uint64_t idx = next_slot_unlocked();
    
    nuraft::ptr<nuraft::buffer> buf = entry->serialize();
    std::vector<uint8_t> payload(buf->size());
    buf->pos(0);
    ::memcpy(payload.data(), buf->data(), buf->size());

    const auto w0 = std::chrono::steady_clock::now();
    write_entry_record(idx, entry->get_term(), payload);
    const auto w1 = std::chrono::steady_clock::now();
    const uint64_t write_ns =
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count());
    profile_.append_write_total_ns.fetch_add(write_ns, std::memory_order_relaxed);
    profile_atomic_max(profile_.append_write_max_ns, write_ns);
    {
        std::lock_guard<std::mutex> sample_lk(latency_samples_mu_);
        append_write_samples_ns_.push_back(write_ns);
    }

    logs_[idx] = make_clone(entry);
    dirty_ = true;

    if (batch_first_ == 0) {
        batch_first_ = idx;
    }
    batch_last_ = idx;

    profile_.append_calls.fetch_add(1, std::memory_order_relaxed);
    if (durable_trace_enabled()) {
        std::cerr << "RAFT_DURABLE_APPEND"
                  << " idx=" << idx
                  << " term=" << entry->get_term()
                  << " next_slot=" << next_slot_unlocked()
                  << " dirty_segments=" << dirty_segments_.size()
                  << std::endl;
    }
    return idx;
}

void durable_log_store::write_at(nuraft::ulong index, nuraft::ptr<nuraft::log_entry>& entry) {
    std::unique_lock<std::mutex> l(mu_);
    wait_for_async_flush_idle_locked(l);
    throw_if_async_flush_failed_unlocked();
    write_truncate_record(index);

    // Truncate in memory cache
    auto it = logs_.lower_bound(index);
    while (it != logs_.end()) {
        index_locations_.erase(it->first);
        it = logs_.erase(it);
    }

    if (const char* fp = std::getenv("ARIABC_FAILPOINT_CRASH_AFTER_TRUNCATE_MARKER_BEFORE_REPLACEMENT"); fp && fp[0] != '\0') {
        std::cerr << "FAILPOINT: crashing after truncate marker, before replacement in write_at" << std::endl;
        ::kill(::getpid(), SIGKILL);
        ::_exit(124);
    }

    nuraft::ptr<nuraft::buffer> buf = entry->serialize();
    std::vector<uint8_t> payload(buf->size());
    buf->pos(0);
    ::memcpy(payload.data(), buf->data(), buf->size());

    write_entry_record(index, entry->get_term(), payload);
    logs_[index] = make_clone(entry);
    dirty_ = true;

    sync_dirty_segments_unlocked("write_at sync");
    if (durable_trace_enabled()) {
        std::cerr << "RAFT_DURABLE_WRITE_AT"
                  << " idx=" << index
                  << " term=" << entry->get_term()
                  << " next_slot=" << next_slot_unlocked()
                  << " last_durable=" << last_durable_idx_
                  << std::endl;
    }
}

nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
durable_log_store::log_entries(nuraft::ulong start, nuraft::ulong end) {
    std::lock_guard<std::mutex> l(mu_);
    auto ret = nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();
    ret->resize(end - start);
    size_t cc = 0;
    for (uint64_t idx = start; idx < end; ++idx) {
        auto it = logs_.find(idx);
        if (it != logs_.end()) {
            (*ret)[cc++] = make_clone(it->second);
        } else {
            (*ret)[cc++] = nullptr;
        }
    }
    return ret;
}

nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
durable_log_store::log_entries_ext(nuraft::ulong start, nuraft::ulong end, nuraft::int64 batch_size_hint_in_bytes) {
    std::lock_guard<std::mutex> l(mu_);
    auto ret = nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();
    size_t accum_size = 0;
    for (uint64_t idx = start; idx < end; ++idx) {
        auto it = logs_.find(idx);
        if (it == logs_.end()) break;
        ret->push_back(make_clone(it->second));
        accum_size += it->second->get_buf().size();
        if (batch_size_hint_in_bytes && accum_size >= (size_t)batch_size_hint_in_bytes) {
            break;
        }
    }
    return ret;
}

nuraft::ptr<nuraft::log_entry> durable_log_store::entry_at(nuraft::ulong index) {
    std::lock_guard<std::mutex> l(mu_);
    auto it = logs_.find(index);
    if (it == logs_.end()) return nullptr;
    return make_clone(it->second);
}

nuraft::ulong durable_log_store::term_at(nuraft::ulong index) {
    std::lock_guard<std::mutex> l(mu_);
    auto it = logs_.find(index);
    if (it == logs_.end()) return 0;
    return it->second->get_term();
}

nuraft::ptr<nuraft::buffer> durable_log_store::pack(nuraft::ulong index, nuraft::int32 cnt) {
    std::lock_guard<std::mutex> l(mu_);
    if (cnt < 0) {
        throw std::runtime_error("pack: cnt must be non-negative: " + std::to_string(cnt));
    }
    if (index + cnt < index) {
        throw std::runtime_error("pack: index + cnt overflow");
    }
    std::vector<nuraft::ptr<nuraft::buffer>> serialized_logs;
    size_t size_total = 0;
    for (uint64_t ii = index; ii < index + cnt; ++ii) {
        auto it = logs_.find(ii);
        if (it == logs_.end()) {
            throw std::runtime_error("pack: index not found: " + std::to_string(ii));
        }
        nuraft::ptr<nuraft::buffer> buf = it->second->serialize();
        size_total += buf->size();
        if (size_total < buf->size()) {
            throw std::runtime_error("pack: size_total overflow");
        }
        serialized_logs.push_back(buf);
    }
    size_t required_alloc = sizeof(int32_t) + (size_t)cnt * sizeof(int32_t) + size_total;
    if (required_alloc < size_total) {
        throw std::runtime_error("pack: allocation size overflow");
    }
    nuraft::ptr<nuraft::buffer> buf_out = nuraft::buffer::alloc(required_alloc);
    buf_out->pos(0);
    buf_out->put((int32_t)cnt);
    for (auto& entry : serialized_logs) {
        buf_out->put((int32_t)entry->size());
        buf_out->put(*entry);
    }
    return buf_out;
}

void durable_log_store::apply_pack(nuraft::ulong index, nuraft::buffer& pack) {
    std::unique_lock<std::mutex> l(mu_);
    wait_for_async_flush_idle_locked(l);
    throw_if_async_flush_failed_unlocked();
    if (pack.size() < sizeof(int32_t)) {
        throw std::runtime_error("apply_pack: missing entry count");
    }
    pack.pos(0);
    int32_t num_logs = pack.get_int();

    // Input bounds validation (Item 8 from plan_left.md)
    if (num_logs < 0 || (size_t)num_logs > pack.size()) {
        throw std::runtime_error("apply_pack: invalid/malformed num_logs: " + std::to_string(num_logs));
    }
    if (index + num_logs < index) {
        throw std::runtime_error("apply_pack: index + num_logs overflow");
    }

    // Verify/decode all first
    std::vector<nuraft::ptr<nuraft::log_entry>> decoded_entries;
    decoded_entries.reserve(num_logs);
    for (int32_t ii = 0; ii < num_logs; ++ii) {
        if (pack.pos() + sizeof(int32_t) > pack.size()) {
            throw std::runtime_error("apply_pack: buffer underflow reading entry size");
        }
        int32_t buf_size = pack.get_int();
        if (buf_size < 0 || (size_t)buf_size > (pack.size() - pack.pos())) {
            throw std::runtime_error("apply_pack: entry size out of bounds or negative: " + std::to_string(buf_size));
        }
        nuraft::ptr<nuraft::buffer> buf_local = nuraft::buffer::alloc(buf_size);
        pack.get(buf_local);
        nuraft::ptr<nuraft::log_entry> le = nuraft::log_entry::deserialize(*buf_local);
        if (!le) {
            throw std::runtime_error("apply_pack: failed to deserialize entry");
        }
        decoded_entries.push_back(le);
    }
    if (pack.pos() != pack.size()) {
        throw std::runtime_error("apply_pack: trailing/malformed bytes in pack: pos=" + std::to_string(pack.pos()) + ", size=" + std::to_string(pack.size()));
    }

    // Write truncate record
    write_truncate_record(index);

    // Erase memory cache from index
    auto it = logs_.lower_bound(index);
    while (it != logs_.end()) {
        index_locations_.erase(it->first);
        it = logs_.erase(it);
    }

    if (const char* fp = std::getenv("ARIABC_FAILPOINT_CRASH_AFTER_TRUNCATE_MARKER_BEFORE_REPLACEMENT"); fp && fp[0] != '\0') {
        std::cerr << "FAILPOINT: crashing after truncate marker, before replacement in apply_pack" << std::endl;
        ::kill(::getpid(), SIGKILL);
        ::_exit(124);
    }

    // Write entries to segment
    for (int32_t ii = 0; ii < num_logs; ++ii) {
        uint64_t cur_idx = index + ii;
        nuraft::ptr<nuraft::buffer> buf = decoded_entries[ii]->serialize();
        std::vector<uint8_t> payload(buf->size());
        buf->pos(0);
        ::memcpy(payload.data(), buf->data(), buf->size());

        write_entry_record(cur_idx, decoded_entries[ii]->get_term(), payload);
        logs_[cur_idx] = decoded_entries[ii];
    }
    dirty_ = true;

    // Flush to disk
    sync_dirty_segments_unlocked("apply_pack sync");

    // Adjust start index
    auto start_it = logs_.upper_bound(0);
    if (start_it != logs_.end()) {
        start_index_ = start_it->first;
    } else {
        start_index_ = 1;
    }
}

bool durable_log_store::compact(nuraft::ulong last_log_index) {
    // Compaction is explicitly disabled in this phase.
    throw std::runtime_error("Compaction is disabled in this version of durable_log_store.");
}

bool durable_log_store::flush() {
    std::unique_lock<std::mutex> l(mu_);
    wait_for_async_flush_idle_locked(l);
    throw_if_async_flush_failed_unlocked();
    if (!dirty_) return true;
    sync_dirty_segments_unlocked("flush sync");
    return true;
}

void durable_log_store::close() {
    {
        std::unique_lock<std::mutex> l(mu_);
        try {
            wait_for_async_flush_idle_locked(l);
            throw_if_async_flush_failed_unlocked();
            if (dirty_) {
                sync_dirty_segments_unlocked("close sync");
            }
        } catch (const std::exception& e) {
            std::cerr << "RAFT_STORAGE_FATAL: failed to flush log store on close: " << e.what() << std::endl;
            std::terminate();
        }
        async_flush_stop_ = true;
        async_flush_cv_.notify_all();
        for (auto& seg : segments_) {
            if (seg.fd >= 0) {
                ::close(seg.fd);
                seg.fd = -1;
            }
        }
    }
    if (async_flush_thread_.joinable()) {
        async_flush_thread_.join();
    }
}

nuraft::ulong durable_log_store::last_durable_index() {
    std::lock_guard<std::mutex> l(mu_);
    return last_durable_idx_;
}

log_store_latency_profile durable_log_store::latency_profile() const {
    std::vector<uint64_t> append_write_samples;
    std::vector<uint64_t> fdatasync_samples;
    std::vector<uint64_t> directory_fsync_samples;
    {
        std::lock_guard<std::mutex> l(latency_samples_mu_);
        append_write_samples = append_write_samples_ns_;
        fdatasync_samples = fdatasync_samples_ns_;
        directory_fsync_samples = directory_fsync_samples_ns_;
    }

    log_store_latency_profile out;
    out.append_write_p50_ns = percentile_ns(append_write_samples, 50.0);
    out.append_write_p95_ns = percentile_ns(append_write_samples, 95.0);
    out.append_write_p99_ns = percentile_ns(append_write_samples, 99.0);
    out.fdatasync_p50_ns = percentile_ns(fdatasync_samples, 50.0);
    out.fdatasync_p95_ns = percentile_ns(fdatasync_samples, 95.0);
    out.fdatasync_p99_ns = percentile_ns(fdatasync_samples, 99.0);
    out.directory_fsync_p50_ns = percentile_ns(directory_fsync_samples, 50.0);
    out.directory_fsync_p95_ns = percentile_ns(directory_fsync_samples, 95.0);
    out.directory_fsync_p99_ns = percentile_ns(directory_fsync_samples, 99.0);
    return out;
}

void durable_log_store::end_of_append_batch(nuraft::ulong start, nuraft::ulong cnt) {
    std::unique_lock<std::mutex> l(mu_);
    throw_if_async_flush_failed_unlocked();
    if (cnt == 0) return;

    if (batch_first_ != start || batch_last_ != start + cnt - 1) {
        // Tolerantly handle range mismatch or out-of-order batches.
        // We sync whatever is dirty to maintain durability of all written records.
        if (async_flush_enabled_) {
            enqueue_async_flush_locked(l,
                                       next_slot_unlocked() - 1,
                                       "end_of_append_batch async (range mismatch fallback)");
        } else {
            sync_dirty_segments_unlocked("end_of_append_batch sync (range mismatch fallback)");
        }
    } else {
        if (async_flush_enabled_) {
            enqueue_async_flush_locked(l, start + cnt - 1, "end_of_append_batch async");
        } else {
            sync_dirty_segments_unlocked("end_of_append_batch sync");
        }
    }

    profile_.append_batches.fetch_add(1, std::memory_order_relaxed);
    profile_.append_batch_entries_total.fetch_add(cnt, std::memory_order_relaxed);

    uint64_t cur_max_entries = profile_.append_batch_entries_max.load(std::memory_order_relaxed);
    while (cnt > cur_max_entries && !profile_.append_batch_entries_max.compare_exchange_weak(cur_max_entries, cnt)) {}

    batch_first_ = 0;
    batch_last_ = 0;
    if (durable_trace_enabled()) {
        std::cerr << "RAFT_DURABLE_APPEND_BATCH"
                  << " start=" << start
                  << " cnt=" << cnt
                  << " next_slot=" << next_slot_unlocked()
                  << " last_durable=" << last_durable_idx_
                  << " async=" << (async_flush_enabled_ ? 1 : 0)
                  << std::endl;
    }
}

void durable_log_store::open_or_create() {
    ensure_directory(log_dir_);
    load_manifest();
    scan_and_recover();
}

void durable_log_store::load_manifest() {
    struct stat st;
    if (::stat(manifest_path_.c_str(), &st) != 0) {
        // Manifest doesn't exist, check if there are segments on disk (lost manifest case)
        DIR* dir = ::opendir(log_dir_.c_str());
        if (dir) {
            struct dirent* entry;
            bool has_segments = false;
            while ((entry = ::readdir(dir)) != nullptr) {
                std::string name = entry->d_name;
                uint64_t dummy_fidx = 0;
                uint64_t dummy_gen = 0;
                FilenameType type = classify_filename(name, dummy_fidx, dummy_gen);
                if (type == FilenameType::VALID_V2_SEGMENT || type == FilenameType::VALID_V1_SEGMENT) {
                    has_segments = true;
                    break;
                } else if (type == FilenameType::MALFORMED_SEGMENT) {
                    ::closedir(dir);
                    throw storage_corruption_error("Malformed segment filename in log directory: " + name);
                }
            }
            ::closedir(dir);
            if (has_segments) {
                throw storage_corruption_error("Missing manifest file in non-empty log directory");
            }
        }
        // Manifest doesn't exist and log directory is clean/empty, create it empty
        save_manifest();
        return;
    }

    std::vector<uint8_t> raw = read_entire_file(manifest_path_);
    if (raw.size() < 16) {
        throw storage_corruption_error("Manifest file too short or corrupt");
    }

    // Check manifest CRC (last 4 bytes are CRC of everything before)
    uint32_t expected_mcrc = get_le32(raw.data() + raw.size() - 4);
    uint32_t actual_mcrc = crc32_bytes(raw.data(), raw.size() - 4);
    if (actual_mcrc != expected_mcrc) {
        throw storage_corruption_error("Manifest file CRC mismatch");
    }

    uint32_t magic = get_le32(raw.data());
    uint32_t ver = get_le32(raw.data() + 4);
    uint32_t num_segs = get_le32(raw.data() + 8);
    if (magic != MAGIC) {
        throw storage_corruption_error("Manifest file corrupt (bad magic)");
    }

    if (ver == FORMAT_VER_V1) {
        throw storage_corruption_error("legacy durable Raft storage format is unsupported; start with fresh storage.");
    } else if (ver == FORMAT_VER) {
        // v2 format: 16-byte header + 8 next_gen_id + num_segs*(8 first_index + 8 gen_id) + 4 CRC
        const size_t expected_sz = 16 + 8 + (size_t)num_segs * 16 + 4;
        if (raw.size() != expected_sz) {
            throw storage_corruption_error("Manifest v2 size does not match segment count");
        }
        const uint8_t* p = raw.data() + 16;   // skip magic(4)+ver(4)+num_segs(4)+reserved(4)
        next_gen_id_ = get_le64(p); p += 8;
        uint64_t prev_first_idx = 0;
        for (uint32_t i = 0; i < num_segs; ++i) {
            uint64_t fidx = get_le64(p); p += 8;
            uint64_t gen  = get_le64(p); p += 8;
            if (fidx == 0) throw storage_corruption_error("Manifest v2: invalid segment first_index=0");
            if (i > 0 && fidx <= prev_first_idx) throw storage_corruption_error("Manifest v2: segments out of order");
            prev_first_idx = fidx;
            Segment seg;
            seg.first_index = fidx;
            seg.gen_id      = gen;
            seg.path = segment_path(fidx, gen);
            seg.fd = -1; seg.size = 0; seg.is_active = false;
            segments_.push_back(seg);
        }
        if (!segments_.empty()) segments_.back().is_active = true;
    } else {
        throw storage_corruption_error("Manifest file: unknown version " + std::to_string(ver));
    }

    // Scan for orphan segment files and adjust next_gen_id_ to avoid EEXIST collisions
    uint64_t max_gen = 0;
    std::set<std::string> active_paths;
    for (const auto& seg : segments_) {
        active_paths.insert(seg.path);
    }

    DIR* d = ::opendir(log_dir_.c_str());
    if (d) {
        struct dirent* entry;
        std::vector<std::string> orphans_to_delete;
        while ((entry = ::readdir(d)) != nullptr) {
            std::string name = entry->d_name;
            uint64_t fidx = 0;
            uint64_t gen = 0;
            FilenameType type = classify_filename(name, fidx, gen);
            if (type == FilenameType::VALID_V2_SEGMENT) {
                if (gen > max_gen) {
                    max_gen = gen;
                }
                std::string full_path = log_dir_ + "/" + name;
                if (!log_dir_.empty() && log_dir_.back() == '/') {
                    full_path = log_dir_ + name;
                }
                if (active_paths.count(full_path) == 0) {
                    orphans_to_delete.push_back(full_path);
                }
            } else if (type == FilenameType::VALID_V1_SEGMENT) {
                ::closedir(d);
                throw storage_corruption_error("legacy durable Raft storage format is unsupported; start with fresh storage.");
            } else if (type == FilenameType::MALFORMED_SEGMENT) {
                ::closedir(d);
                throw storage_corruption_error("Malformed segment filename in log directory: " + name);
            }
        }
        ::closedir(d);

        // Delete orphan segment files only after validating manifest is loaded
        for (const auto& path : orphans_to_delete) {
            if (::unlink(path.c_str()) != 0 && errno != ENOENT) {
                throw storage_io_error("Failed to unlink orphan segment file: " + path + " error: " + strerror(errno));
            }
        }
        if (!orphans_to_delete.empty()) {
            fsync_directory_and_profile(log_dir_);
        }
    }
    if (max_gen >= next_gen_id_) {
        next_gen_id_ = max_gen + 1;
    }
}

void durable_log_store::save_manifest() {
    // v2 format: magic(4) + ver(4) + num_segs(4) + reserved(4) +
    //            next_gen_id(8) + [first_index(8) + gen_id(8)] * num_segs + CRC(4)
    std::vector<uint8_t> raw;
    put_le32(raw, MAGIC);
    put_le32(raw, FORMAT_VER);          // v2
    put_le32(raw, (uint32_t)segments_.size());
    put_le32(raw, 0);                   // reserved (was the 4th word of the 16-byte header)
    put_le64(raw, next_gen_id_);
    for (auto& seg : segments_) {
        put_le64(raw, seg.first_index);
        put_le64(raw, seg.gen_id);
    }
    uint32_t mcrc = crc32_bytes(raw.data(), raw.size());
    put_le32(raw, mcrc);
    atomic_write_file(manifest_path_, raw);
}


void durable_log_store::process_record_on_recovery(
    const uint8_t* header, const std::vector<uint8_t>& payload, uint64_t file_offset, size_t seg_idx)
{
    uint32_t rtype = get_le32(header + 8);
    uint64_t raft_idx = get_le64(header + 16);
    uint64_t raft_term = get_le64(header + 24);

    if (rtype == RT_ENTRY) {
        nuraft::ptr<nuraft::buffer> buf = nuraft::buffer::alloc(payload.size());
        buf->pos(0);
        ::memcpy(buf->data(), payload.data(), payload.size());
        buf->pos(0);
        nuraft::ptr<nuraft::log_entry> le = nuraft::log_entry::deserialize(*buf);
        if (!le) {
            throw storage_corruption_error("Failed to deserialize log entry on recovery");
        }
        if (le->get_term() != raft_term) {
            throw storage_corruption_error("Raft term mismatch between header (" + std::to_string(raft_term) + ") and payload (" + std::to_string(le->get_term()) + ") at index " + std::to_string(raft_idx));
        }
        logs_[raft_idx] = le;
        profile_.recovery_entries_loaded.fetch_add(1, std::memory_order_relaxed);

        log_location loc;
        loc.segment_seq = segments_[seg_idx].first_index;
        loc.file_offset = file_offset;
        loc.record_size = HEADER_SIZE + payload.size();
        index_locations_[raft_idx] = loc;
    } else if (rtype == RT_TRUNCATE) {
        uint32_t payload_len = get_le32(header + OFF_PAYLOAD_LEN);
        if (payload_len != 0) {
            throw storage_corruption_error("RT_TRUNCATE record payload_len must be 0, got " + std::to_string(payload_len));
        }
        if (raft_term != 0) {
            throw storage_corruption_error("RT_TRUNCATE record term must be 0, got " + std::to_string(raft_term));
        }

        auto it = logs_.lower_bound(raft_idx);
        while (it != logs_.end()) {
            index_locations_.erase(it->first);
            it = logs_.erase(it);
        }
    }
}

void durable_log_store::scan_and_recover() {
    if (segments_.empty()) {
        create_segment(1);
        return;
    }

    logs_.clear();
    index_locations_.clear();
    uint64_t expected_next_idx = 0;

    for (size_t i = 0; i < segments_.size(); ++i) {
        std::string path = segments_[i].path;
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw storage_io_error("Failed to open segment file for recovery scan: " + path);
        }

        struct stat st;
        if (::fstat(fd, &st) != 0) {
            ::close(fd);
            throw storage_io_error("fstat failed on segment: " + path);
        }
        uint64_t file_size = st.st_size;
        segments_[i].size = file_size;

        if (file_size < 16) {
            // Unusable segment header, delete it if it is the final segment
            if (i == segments_.size() - 1) {
                ::close(fd);
                if (::unlink(path.c_str()) != 0 && errno != ENOENT) {
                    throw storage_io_error("Failed to unlink unusable tail segment file: " + path + " error: " + strerror(errno));
                }
                segments_.pop_back();
                save_manifest();
                fsync_directory_and_profile(log_dir_);
                uint64_t next_idx = (expected_next_idx == 0) ? 1 : expected_next_idx;
                create_segment(next_idx);
                break;
            } else {
                ::close(fd);
                throw storage_corruption_error("Invalid/truncated segment header in non-tail segment: " + path);
            }
        }

        uint8_t seg_hdr[16];
        if (!read_full(fd, seg_hdr, 16, path)) {
            ::close(fd);
            throw storage_corruption_error("Failed to read segment header: " + path);
        }
        if (::memcmp(seg_hdr, "RAFT_SEG", 8) != 0) {
            ::close(fd);
            throw storage_corruption_error("Invalid segment magic: " + path);
        }
        uint32_t seg_ver = get_le32(seg_hdr + 8);
        if (seg_ver != FORMAT_VER) {
            ::close(fd);
            throw storage_corruption_error("Invalid segment version: " + path);
        }

        bool is_first_record_in_segment = true;
        uint64_t offset = 16;
        while (offset < file_size) {
            if (offset + HEADER_SIZE > file_size) {
                // Partial final header at EOF
                if (i == segments_.size() - 1) {
                    ::close(fd);
                    fd = -1;
                    int write_fd = ::open(path.c_str(), O_RDWR);
                    if (write_fd < 0) {
                        throw storage_io_error("Cannot open segment for tail repair: " + path);
                    }
                    if (::ftruncate(write_fd, offset) != 0) {
                        ::close(write_fd);
                        throw storage_io_error("ftruncate failed during tail header repair: " + path);
                    }
                    try {
                        fdatasync_and_profile(write_fd, "tail header repair: " + path);
                        profile_.tail_repairs.fetch_add(1, std::memory_order_relaxed);
                    } catch (...) {
                        ::close(write_fd);
                        throw;
                    }
                    ::close(write_fd);
                    segments_[i].size = offset;
                    break;
                } else {
                    ::close(fd);
                    throw storage_corruption_error("Partial non-tail header in segment: " + path + " at offset " + std::to_string(offset));
                }
            }

            uint8_t header[HEADER_SIZE];
            if (::lseek(fd, offset, SEEK_SET) < 0 || !read_full(fd, header, HEADER_SIZE, path)) {
                ::close(fd);
                throw storage_io_error("Failed to read record header at offset " + std::to_string(offset));
            }

            uint32_t magic = get_le32(header + OFF_MAGIC);
            uint32_t ver = get_le32(header + OFF_VERSION);
            uint32_t rtype = get_le32(header + OFF_TYPE);
            uint32_t rec_len = get_le32(header + OFF_RECORD_LEN);
            uint64_t raft_idx = get_le64(header + OFF_INDEX);
            uint64_t raft_term = get_le64(header + OFF_TERM);
            uint32_t payload_len = get_le32(header + OFF_PAYLOAD_LEN);
            uint32_t payload_crc = get_le32(header + OFF_PAYLOAD_CRC);
            uint32_t expected_hcrc = get_le32(header + OFF_HEADER_CRC);
            uint32_t actual_hcrc = crc32_bytes(header, HEADER_CRC_INPUT_SIZE);

            if (magic != MAGIC) {
                ::close(fd);
                throw storage_corruption_error("Invalid record magic in segment: " + path + " at offset " + std::to_string(offset));
            }
            if (ver != FORMAT_VER) {
                ::close(fd);
                throw storage_corruption_error("Invalid record format version in segment: " + path + " at offset " + std::to_string(offset));
            }
            if (rtype != RT_ENTRY && rtype != RT_TRUNCATE) {
                ::close(fd);
                throw storage_corruption_error("Unknown record type in segment: " + path + " at offset " + std::to_string(offset));
            }
            if (actual_hcrc != expected_hcrc) {
                ::close(fd);
                throw storage_corruption_error("Record header CRC mismatch in segment: " + path + " at offset " + std::to_string(offset));
            }
            if (rec_len != HEADER_SIZE + payload_len) {
                ::close(fd);
                throw storage_corruption_error("Record length mismatch in segment: " + path + " at offset " + std::to_string(offset));
            }

            // Record index continuity validation
            if (is_first_record_in_segment) {
                if (raft_idx != segments_[i].first_index) {
                    ::close(fd);
                    throw storage_corruption_error("First record index " + std::to_string(raft_idx) +
                                                   " in segment " + path + " does not match segment first_index " +
                                                   std::to_string(segments_[i].first_index));
                }
                if (expected_next_idx != 0) {
                    if (rtype == RT_ENTRY) {
                        if (raft_idx != expected_next_idx) {
                            ::close(fd);
                            throw storage_corruption_error("Segment boundary index gap detected in " + path +
                                                           ": expected " + std::to_string(expected_next_idx) +
                                                           " but got " + std::to_string(raft_idx));
                        }
                    } else if (rtype == RT_TRUNCATE) {
                        if (raft_idx > expected_next_idx) {
                            ::close(fd);
                            throw storage_corruption_error("Segment boundary truncate index jump forward detected in " + path +
                                                           ": expected <= " + std::to_string(expected_next_idx) +
                                                           " but got " + std::to_string(raft_idx));
                        }
                    }
                }
                is_first_record_in_segment = false;
            } else {
                if (rtype == RT_ENTRY) {
                    if (expected_next_idx != 0 && raft_idx != expected_next_idx) {
                        ::close(fd);
                        throw storage_corruption_error("Index gap/jump detected in " + path + " at offset " + std::to_string(offset) +
                                                       ": expected " + std::to_string(expected_next_idx) +
                                                       " but got " + std::to_string(raft_idx));
                    }
                } else if (rtype == RT_TRUNCATE) {
                    if (expected_next_idx != 0 && raft_idx > expected_next_idx) {
                        ::close(fd);
                        throw storage_corruption_error("Truncate index jump forward detected in " + path + " at offset " + std::to_string(offset) +
                                                       ": expected <= " + std::to_string(expected_next_idx) +
                                                       " but got " + std::to_string(raft_idx));
                    }
                }
            }

            if (rtype == RT_ENTRY) {
                expected_next_idx = raft_idx + 1;
            } else if (rtype == RT_TRUNCATE) {
                expected_next_idx = raft_idx;
            }

            if (offset + HEADER_SIZE + payload_len > file_size) {
                // Partial final payload at EOF
                if (i == segments_.size() - 1) {
                    ::close(fd);
                    fd = -1;
                    int write_fd = ::open(path.c_str(), O_RDWR);
                    if (write_fd < 0) {
                        throw storage_io_error("Cannot open segment for tail payload repair: " + path);
                    }
                    if (::ftruncate(write_fd, offset) != 0) {
                        ::close(write_fd);
                        throw storage_io_error("ftruncate failed during tail payload repair: " + path);
                    }
                    try {
                        fdatasync_and_profile(write_fd, "tail payload repair: " + path);
                        profile_.tail_repairs.fetch_add(1, std::memory_order_relaxed);
                    } catch (...) {
                        ::close(write_fd);
                        throw;
                    }
                    ::close(write_fd);
                    segments_[i].size = offset;
                    break;
                } else {
                    ::close(fd);
                    throw storage_corruption_error("Partial non-tail payload in segment: " + path + " at offset " + std::to_string(offset));
                }
            }

            std::vector<uint8_t> payload(payload_len);
            if (payload_len > 0) {
                if (!read_full(fd, payload.data(), payload_len, path)) {
                    ::close(fd);
                    throw storage_io_error("Failed to read record payload at offset " + std::to_string(offset + HEADER_SIZE));
                }
                uint32_t actual_pcrc = crc32_bytes(payload.data(), payload_len);
                if (actual_pcrc != payload_crc) {
                    ::close(fd);
                    throw storage_corruption_error("CRC mismatch in record payload in segment: " + path + " at offset " + std::to_string(offset));
                }
            }

            process_record_on_recovery(header, payload, offset, i);
            offset += HEADER_SIZE + payload_len;
        }

        if (fd >= 0) {
            ::close(fd);
        }
    }

    // Set start index
    auto start_it = logs_.upper_bound(0);
    if (start_it != logs_.end()) {
        start_index_ = start_it->first;
    } else {
        start_index_ = 1;
    }

    load_durable_watermark_unlocked();
}

void durable_log_store::enable_async_flush(nuraft::raft_server* raft) {
    std::lock_guard<std::mutex> l(mu_);
    raft_server_bwd_pointer_ = raft;
    if (async_flush_enabled_) {
        return;
    }
    async_flush_enabled_ = true;
    async_flush_stop_ = false;
    async_flush_thread_ = std::thread(&durable_log_store::async_flush_loop, this);
    std::cerr << "RAFT_DURABLE_ASYNC_FLUSH enabled"
              << " max_jobs=" << async_flush_max_jobs_
              << std::endl;
}

void durable_log_store::set_raft_server(nuraft::raft_server* raft) {
    std::lock_guard<std::mutex> l(mu_);
    raft_server_bwd_pointer_ = raft;
}

bool durable_log_store::async_flush_enabled() const {
    std::lock_guard<std::mutex> l(mu_);
    return async_flush_enabled_;
}

void durable_log_store::throw_if_async_flush_failed_unlocked() const {
    if (!async_flush_failed_) {
        return;
    }
    throw storage_io_error("asynchronous durable log flush failed: " + async_flush_error_);
}

void durable_log_store::close_async_flush_segment_fds(
    std::vector<AsyncFlushSegment>& segments)
{
    for (auto& seg : segments) {
        if (seg.fd >= 0) {
            ::close(seg.fd);
            seg.fd = -1;
        }
    }
}

void durable_log_store::wait_for_async_flush_idle_locked(
    std::unique_lock<std::mutex>& lock)
{
    if (!async_flush_enabled_) {
        return;
    }
    while (async_flush_active_ || !async_flush_jobs_.empty()) {
        profile_.async_flush_waits.fetch_add(1, std::memory_order_relaxed);
        async_flush_idle_cv_.wait(lock);
    }
}

void durable_log_store::enqueue_async_flush_locked(std::unique_lock<std::mutex>& lock,
                                                   uint64_t target_index,
                                                   const std::string& context)
{
    if (dirty_segments_.empty()) {
        if (target_index > last_durable_idx_) {
            last_durable_idx_ = target_index;
        }
        dirty_ = false;
        return;
    }

    while (async_flush_jobs_.size() >= async_flush_max_jobs_ && !async_flush_failed_) {
        profile_.async_flush_waits.fetch_add(1, std::memory_order_relaxed);
        async_flush_idle_cv_.wait(lock);
    }
    throw_if_async_flush_failed_unlocked();

    AsyncFlushJob job;
    job.target_index = target_index;
    job.context = context;
    job.segments.reserve(dirty_segments_.size());
    for (size_t seg_idx : dirty_segments_) {
        if (seg_idx >= segments_.size()) {
            continue;
        }
        int fd = segment_fd(seg_idx);
        int dup_fd = ::dup(fd);
        if (dup_fd < 0) {
            throw storage_io_error("dup failed for async fdatasync [" +
                                   segments_[seg_idx].path + "]: " + strerror(errno));
        }
        AsyncFlushSegment seg;
        seg.fd = dup_fd;
        seg.path = segments_[seg_idx].path;
        job.segments.push_back(seg);
    }

    dirty_segments_.clear();
    dirty_ = false;
    async_flush_jobs_.push_back(std::move(job));
    profile_.async_flush_jobs.fetch_add(1, std::memory_order_relaxed);
    profile_atomic_max(profile_.async_flush_queue_max,
                       static_cast<uint64_t>(async_flush_jobs_.size()));
    async_flush_cv_.notify_one();
}

void durable_log_store::async_flush_loop() {
    for (;;) {
        AsyncFlushJob job;
        size_t coalesced = 0;
        {
            std::unique_lock<std::mutex> l(mu_);
            async_flush_cv_.wait(l, [this] {
                return async_flush_stop_ || !async_flush_jobs_.empty();
            });
            if (async_flush_stop_ && async_flush_jobs_.empty()) {
                return;
            }
            async_flush_active_ = true;
            job = std::move(async_flush_jobs_.front());
            async_flush_jobs_.pop_front();

            std::map<std::string, int> merged_fds;
            for (auto& seg : job.segments) {
                merged_fds[seg.path] = seg.fd;
                seg.fd = -1;
            }
            while (!async_flush_jobs_.empty()) {
                AsyncFlushJob extra = std::move(async_flush_jobs_.front());
                async_flush_jobs_.pop_front();
                job.target_index = std::max(job.target_index, extra.target_index);
                for (auto& seg : extra.segments) {
                    auto existing = merged_fds.find(seg.path);
                    if (existing != merged_fds.end()) {
                        ::close(existing->second);
                    }
                    merged_fds[seg.path] = seg.fd;
                    seg.fd = -1;
                }
                ++coalesced;
            }
            job.segments.clear();
            job.segments.reserve(merged_fds.size());
            for (auto& entry : merged_fds) {
                AsyncFlushSegment seg;
                seg.path = entry.first;
                seg.fd = entry.second;
                job.segments.push_back(seg);
            }
        }

        bool ok = true;
        std::string error;
        try {
            for (const auto& seg : job.segments) {
                fdatasync_and_profile(seg.fd, job.context + " (" + seg.path + ")");
            }
        } catch (const std::exception& e) {
            ok = false;
            error = e.what();
        }

        close_async_flush_segment_fds(job.segments);

        nuraft::raft_server* raft = nullptr;
        {
            std::lock_guard<std::mutex> l(mu_);
            if (ok) {
                if (job.target_index > last_durable_idx_) {
                    last_durable_idx_ = job.target_index;
                }
                profile_.async_flush_coalesced_jobs.fetch_add(
                    static_cast<uint64_t>(coalesced), std::memory_order_relaxed);
            } else {
                async_flush_failed_ = true;
                async_flush_error_ = error;
            }
            async_flush_active_ = false;
            raft = raft_server_bwd_pointer_;
            async_flush_idle_cv_.notify_all();
        }

        if (raft) {
            raft->notify_log_append_completion(ok);
            profile_.async_flush_notifications.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

void durable_log_store::sync_dirty_segments_unlocked(const std::string& context) {
    if (dirty_segments_.empty()) {
        last_durable_idx_ = next_slot_unlocked() - 1;
        dirty_ = false;
        return;
    }

    for (size_t seg_idx : dirty_segments_) {
        if (seg_idx < segments_.size()) {
            int fd = segment_fd(seg_idx);
            fdatasync_and_profile(fd, context + " (segment " + std::to_string(seg_idx) + ")");
        }
    }

    dirty_segments_.clear();
    last_durable_idx_ = next_slot_unlocked() - 1;
    dirty_ = false;

    save_durable_watermark_unlocked();
}

void durable_log_store::fdatasync_and_profile(int fd, const std::string& context) {
    if (const char* fp = std::getenv("ARIABC_FAILPOINT_CRASH_BEFORE_FDATASYNC"); fp && fp[0] != '\0') {
        std::string fp_val(fp);
        if (fp_val == "1" || context.find(fp_val) != std::string::npos) {
            std::cerr << "FAILPOINT: crashing before fdatasync in context '" << context << "'" << std::endl;
            ::kill(::getpid(), SIGKILL);
            ::_exit(123);
        }
    }

    auto s0 = std::chrono::steady_clock::now();
    if (::fdatasync(fd) != 0) {
        throw storage_io_error("fdatasync failed in " + context + " error: " + strerror(errno));
    }
    auto s1 = std::chrono::steady_clock::now();
    uint64_t elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count();
    profile_.fdatasync_total_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
    profile_.fdatasync_calls.fetch_add(1, std::memory_order_relaxed);
    profile_.segment_fdatasync_calls.fetch_add(1, std::memory_order_relaxed);
    profile_atomic_max(profile_.fdatasync_max_ns, elapsed_ns);
    {
        std::lock_guard<std::mutex> sample_lk(latency_samples_mu_);
        fdatasync_samples_ns_.push_back(elapsed_ns);
    }
}

void durable_log_store::fsync_directory_and_profile(const std::string& path) {
    const auto d0 = std::chrono::steady_clock::now();
    fsync_directory_or_throw(path);
    const auto d1 = std::chrono::steady_clock::now();
    const uint64_t elapsed_ns =
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(d1 - d0).count());
    profile_.directory_fsync_calls.fetch_add(1, std::memory_order_relaxed);
    profile_.directory_fsync_total_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
    profile_atomic_max(profile_.directory_fsync_max_ns, elapsed_ns);
    {
        std::lock_guard<std::mutex> sample_lk(latency_samples_mu_);
        directory_fsync_samples_ns_.push_back(elapsed_ns);
    }
}

void durable_log_store::save_durable_watermark_unlocked() {
    // No-op in synchronous mode (parallel_log_appending_ = false).
    // Durability watermark is not needed as all written records are in-order.
}

void durable_log_store::load_durable_watermark_unlocked() {
    // In synchronous mode, the last durable index is the index of the last valid record.
    last_durable_idx_ = next_slot_unlocked() - 1;
}

void durable_log_store::simulate_crash_close() {
    std::lock_guard<std::mutex> l(mu_);
    dirty_ = false;
    dirty_segments_.clear();
    for (auto& seg : segments_) {
        if (seg.fd >= 0) {
            ::close(seg.fd);
            seg.fd = -1;
        }
    }
}

} // namespace ariabc_raft
