#include "wire_protocol.hxx"

#include <arpa/inet.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

namespace ariabc_pg {
namespace {

const uint32_t kMaxReqIdBytes = 16u * 1024u * 1024u;
const uint32_t kMaxSqlBytes = 64u * 1024u * 1024u;
const uint32_t kBatchFrameTag = 0xFFFFFFFFu;
const uint32_t kSingleRequestFrameV2Tag = 0xFFFFFFFEu;
const uint32_t kMaxBatchItems = 4096u;
const char kRaftBatchMarker[] = "__ARIABC_RAFT_BATCH_V1__";
const char kRaftBatchMarkerV2[] = "__ARIABC_RAFT_BATCH_V2__";
const uint32_t kRequestFlagWaitForTerminal = 0x1u;

bool read_exact(int fd, void* buf, size_t n, std::string& err) {
    uint8_t* p = reinterpret_cast<uint8_t*>(buf);
    size_t off = 0;
    while (off < n) {
        const ssize_t r = ::read(fd, p + off, n - off);
        if (r == 0) {
            err = "EOF";
            return false;
        }
        if (r < 0) {
            if (errno == EINTR) continue;
            err = std::string("read failed: ") + ::strerror(errno);
            return false;
        }
        off += static_cast<size_t>(r);
    }
    return true;
}

bool write_exact(int fd, const void* buf, size_t n, std::string& err) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(buf);
    size_t off = 0;
    while (off < n) {
        const ssize_t w = ::write(fd, p + off, n - off);
        if (w < 0) {
            if (errno == EINTR) continue;
            err = std::string("write failed: ") + ::strerror(errno);
            return false;
        }
        off += static_cast<size_t>(w);
    }
    return true;
}

bool read_u32(int fd, uint32_t& out, std::string& err) {
    uint32_t tmp = 0;
    if (!read_exact(fd, &tmp, sizeof(tmp), err)) return false;
    out = ntohl(tmp);
    return true;
}

bool write_u32(int fd, uint32_t v, std::string& err) {
    const uint32_t tmp = htonl(v);
    return write_exact(fd, &tmp, sizeof(tmp), err);
}

bool read_bytes(int fd, uint32_t n, std::string& out, std::string& err) {
    out.assign(n, '\0');
    if (n == 0) return true;
    return read_exact(fd, &out[0], n, err);
}

bool validate_request_item(const client_api_request_item& item, std::string& err) {
    if (item.req_id.size() > kMaxReqIdBytes || item.sql.size() > kMaxSqlBytes) {
        err = "request too large";
        return false;
    }
    return true;
}

size_t raft_item_bytes(const client_api_request_item& item) {
    return sizeof(uint32_t) + item.req_id.size() +
           sizeof(uint32_t) + item.sql.size();
}

bool request_has_assigned_det_seq(const client_api_request& req) {
    if (!req.is_batch()) return req.has_assigned_det_seq;
    for (const client_api_request_item& item : req.batch_items) {
        if (item.has_assigned_det_seq) return true;
    }
    return false;
}

} // namespace

bool read_request_frame(int fd, client_api_request& out_req, std::string& err) {
    out_req.req_id.clear();
    out_req.sql.clear();
    out_req.has_assigned_det_seq = false;
    out_req.assigned_det_seq = 0;
    out_req.batch_items.clear();
    out_req.wait_for_terminal = false;
    out_req.terminal_timeout_ms = 30000;

    uint32_t header = 0;
    if (!read_u32(fd, header, err)) return false;
    if (header == kSingleRequestFrameV2Tag) {
        uint32_t flags = 0;
        uint32_t timeout_ms = 0;
        uint32_t req_len = 0;
        uint32_t sql_len = 0;
        if (!read_u32(fd, flags, err)) return false;
        if (!read_u32(fd, timeout_ms, err)) return false;
        if (!read_u32(fd, req_len, err)) return false;
        if (!read_u32(fd, sql_len, err)) return false;
        if (req_len > kMaxReqIdBytes || sql_len > kMaxSqlBytes) {
            err = "frame too large";
            return false;
        }
        if (!read_bytes(fd, req_len, out_req.req_id, err)) return false;
        if (!read_bytes(fd, sql_len, out_req.sql, err)) return false;
        out_req.wait_for_terminal = (flags & kRequestFlagWaitForTerminal) != 0;
        out_req.terminal_timeout_ms = timeout_ms;
        return true;
    }
    if (header != kBatchFrameTag) {
        uint32_t sql_len = 0;
        const uint32_t req_len = header;
        if (!read_u32(fd, sql_len, err)) return false;
        if (req_len > kMaxReqIdBytes || sql_len > kMaxSqlBytes) {
            err = "frame too large";
            return false;
        }
        if (!read_bytes(fd, req_len, out_req.req_id, err)) return false;
        if (!read_bytes(fd, sql_len, out_req.sql, err)) return false;
        return true;
    }

    uint32_t item_count = 0;
    if (!read_u32(fd, item_count, err)) return false;
    if (item_count == 0 || item_count > kMaxBatchItems) {
        err = "invalid batch item count";
        return false;
    }

    out_req.batch_items.reserve(item_count);
    for (uint32_t i = 0; i < item_count; ++i) {
        uint32_t req_len = 0;
        uint32_t sql_len = 0;
        if (!read_u32(fd, req_len, err)) return false;
        if (!read_u32(fd, sql_len, err)) return false;
        if (req_len > kMaxReqIdBytes || sql_len > kMaxSqlBytes) {
            err = "frame too large";
            return false;
        }
        client_api_request_item item;
        if (!read_bytes(fd, req_len, item.req_id, err)) return false;
        if (!read_bytes(fd, sql_len, item.sql, err)) return false;
        out_req.batch_items.push_back(std::move(item));
    }
    return true;
}

bool write_response_frame(int fd, const client_api_response& resp, std::string& err) {
    const uint8_t st = resp.status;
    if (!write_exact(fd, &st, sizeof(st), err)) return false;
    if (resp.msg.size() > (16u * 1024u * 1024u)) {
        err = "response too large";
        return false;
    }
    if (!write_u32(fd, static_cast<uint32_t>(resp.msg.size()), err)) return false;
    if (!resp.msg.empty() && !write_exact(fd, resp.msg.data(), resp.msg.size(), err)) return false;
    return true;
}

bool write_request_frame(int fd, const client_api_request& req, std::string& err) {
    if (!req.is_batch()) {
        if (req.req_id.size() > kMaxReqIdBytes || req.sql.size() > kMaxSqlBytes) {
            err = "request too large";
            return false;
        }
        if (req.wait_for_terminal) {
            const uint32_t flags = kRequestFlagWaitForTerminal;
            if (!write_u32(fd, kSingleRequestFrameV2Tag, err)) return false;
            if (!write_u32(fd, flags, err)) return false;
            if (!write_u32(fd, req.terminal_timeout_ms, err)) return false;
            if (!write_u32(fd, static_cast<uint32_t>(req.req_id.size()), err)) return false;
            if (!write_u32(fd, static_cast<uint32_t>(req.sql.size()), err)) return false;
            if (!req.req_id.empty() && !write_exact(fd, req.req_id.data(), req.req_id.size(), err)) return false;
            if (!req.sql.empty() && !write_exact(fd, req.sql.data(), req.sql.size(), err)) return false;
            return true;
        }
        if (!write_u32(fd, static_cast<uint32_t>(req.req_id.size()), err)) return false;
        if (!write_u32(fd, static_cast<uint32_t>(req.sql.size()), err)) return false;
        if (!req.req_id.empty() && !write_exact(fd, req.req_id.data(), req.req_id.size(), err)) return false;
        if (!req.sql.empty() && !write_exact(fd, req.sql.data(), req.sql.size(), err)) return false;
        return true;
    }

    if (req.wait_for_terminal) {
        err = "terminal wait flag is only supported for single-request frames";
        return false;
    }
    if (req.batch_items.empty() || req.batch_items.size() > kMaxBatchItems) {
        err = "invalid batch item count";
        return false;
    }
    if (!write_u32(fd, kBatchFrameTag, err)) return false;
    if (!write_u32(fd, static_cast<uint32_t>(req.batch_items.size()), err)) return false;
    for (size_t i = 0; i < req.batch_items.size(); ++i) {
        const client_api_request_item& item = req.batch_items[i];
        if (!validate_request_item(item, err)) return false;
        if (!write_u32(fd, static_cast<uint32_t>(item.req_id.size()), err)) return false;
        if (!write_u32(fd, static_cast<uint32_t>(item.sql.size()), err)) return false;
        if (!item.req_id.empty() &&
            !write_exact(fd, item.req_id.data(), item.req_id.size(), err)) {
            return false;
        }
        if (!item.sql.empty() &&
            !write_exact(fd, item.sql.data(), item.sql.size(), err)) {
            return false;
        }
    }
    return true;
}

bool read_response_frame(int fd, client_api_response& out_resp, std::string& err) {
    uint8_t st = 2;
    if (!read_exact(fd, &st, sizeof(st), err)) return false;
    out_resp.status = st;
    uint32_t msg_len = 0;
    if (!read_u32(fd, msg_len, err)) return false;
    if (msg_len > (16u * 1024u * 1024u)) {
        err = "response too large";
        return false;
    }
    if (!read_bytes(fd, msg_len, out_resp.msg, err)) return false;
    return true;
}

nuraft::ptr<nuraft::buffer> build_raft_request_log(const client_api_request& req,
                                                   int leader_node_hint,
                                                   std::string& err) {
    err.clear();

    const bool force_v2 = request_has_assigned_det_seq(req);
    if ((!req.is_batch() || req.batch_items.size() == 1) && !force_v2) {
        client_api_request_item item;
        if (req.is_batch()) {
            item = req.batch_items.front();
        } else {
            item.req_id = req.req_id;
            item.sql = req.sql;
        }
        if (!validate_request_item(item, err)) {
            return nullptr;
        }
        const size_t sz = raft_item_bytes(item) + sizeof(int32_t);
        nuraft::ptr<nuraft::buffer> log = nuraft::buffer::alloc(sz);
        nuraft::buffer_serializer bs(log);
        bs.put_str(item.req_id);
        bs.put_str(item.sql);
        bs.put_i32(leader_node_hint);
        return log;
    }

    std::vector<client_api_request_item> items;
    if (req.is_batch()) {
        items = req.batch_items;
    } else {
        client_api_request_item item;
        item.req_id = req.req_id;
        item.sql = req.sql;
        item.has_assigned_det_seq = req.has_assigned_det_seq;
        item.assigned_det_seq = req.assigned_det_seq;
        items.push_back(std::move(item));
    }

    if (items.empty() || items.size() > kMaxBatchItems) {
        err = "invalid batch item count";
        return nullptr;
    }

    const bool use_v2 = request_has_assigned_det_seq(req);
    const char* marker = use_v2 ? kRaftBatchMarkerV2 : kRaftBatchMarker;
    size_t sz = sizeof(uint32_t) + ::strlen(marker) +
                sizeof(int32_t) + sizeof(uint32_t);
    for (size_t i = 0; i < items.size(); ++i) {
        if (!validate_request_item(items[i], err)) {
            return nullptr;
        }
        sz += raft_item_bytes(items[i]);
        if (use_v2) {
            sz += sizeof(uint8_t);
            sz += sizeof(uint64_t);
        }
    }

    nuraft::ptr<nuraft::buffer> log = nuraft::buffer::alloc(sz);
    nuraft::buffer_serializer bs(log);
    bs.put_str(marker);
    bs.put_i32(leader_node_hint);
    bs.put_u32(static_cast<uint32_t>(items.size()));
    for (size_t i = 0; i < items.size(); ++i) {
        bs.put_str(items[i].req_id);
        bs.put_str(items[i].sql);
        if (use_v2) {
            bs.put_u8(items[i].has_assigned_det_seq ? 1 : 0);
            bs.put_u64(items[i].assigned_det_seq);
        }
    }
    return log;
}

bool parse_raft_request_log(nuraft::buffer& data,
                            raft_request_batch& out,
                            std::string& err) {
    err.clear();
    out.leader_node_hint = -1;
    out.items.clear();

    try {
        nuraft::buffer_serializer bs(data);
        const std::string first = bs.get_str();
        if (first == kRaftBatchMarker || first == kRaftBatchMarkerV2) {
            const bool is_v2 = (first == kRaftBatchMarkerV2);
            out.leader_node_hint = bs.get_i32();
            const uint32_t item_count = bs.get_u32();
            if (item_count == 0 || item_count > kMaxBatchItems) {
                err = "invalid raft batch item count";
                return false;
            }
            out.items.reserve(item_count);
            for (uint32_t i = 0; i < item_count; ++i) {
                client_api_request_item item;
                item.req_id = bs.get_str();
                item.sql = bs.get_str();
                if (is_v2) {
                    item.has_assigned_det_seq = (bs.get_u8() != 0);
                    item.assigned_det_seq = bs.get_u64();
                }
                out.items.push_back(std::move(item));
            }
            return true;
        }

        client_api_request_item item;
        item.req_id = first;
        item.sql = bs.get_str();
        if (bs.pos() + sizeof(int32_t) <= bs.size()) {
            out.leader_node_hint = bs.get_i32();
        }
        out.items.push_back(std::move(item));
        return true;
    } catch (const std::exception& e) {
        err = e.what();
        out.items.clear();
        return false;
    }
}

} // namespace ariabc_pg
