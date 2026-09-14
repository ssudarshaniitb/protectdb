#include "wire_protocol_async.hxx"

#include <arpa/inet.h>

#include <cstring>

namespace ariabc_pg {
namespace {

const uint32_t kMaxReqIdBytes = 16u * 1024u * 1024u;
const uint32_t kMaxSqlBytes = 64u * 1024u * 1024u;
const uint32_t kMaxMsgBytes = 16u * 1024u * 1024u;
const uint32_t kBatchFrameTag = 0xFFFFFFFFu;
const uint32_t kSingleRequestFrameV2Tag = 0xFFFFFFFEu;
const uint32_t kMaxBatchItems = 4096u;
const uint32_t kRequestFlagWaitForTerminal = 0x1u;

bool read_u32_be(const char* p, uint32_t& out) {
    if (!p) return false;
    uint32_t tmp = 0;
    std::memcpy(&tmp, p, sizeof(tmp));
    out = ntohl(tmp);
    return true;
}

void append_u32_be(std::string& out, uint32_t v) {
    const uint32_t tmp = htonl(v);
    out.append(reinterpret_cast<const char*>(&tmp), sizeof(tmp));
}

} // namespace

bool encode_request_frame(const client_api_request& req, std::string& out, std::string& err) {
    err.clear();
    out.clear();

    if (!req.is_batch()) {
        if (req.req_id.size() > kMaxReqIdBytes || req.sql.size() > kMaxSqlBytes) {
            err = "request too large";
            return false;
        }
        if (req.wait_for_terminal) {
            out.reserve(20 + req.req_id.size() + req.sql.size());
            append_u32_be(out, kSingleRequestFrameV2Tag);
            append_u32_be(out, kRequestFlagWaitForTerminal);
            append_u32_be(out, req.terminal_timeout_ms);
            append_u32_be(out, static_cast<uint32_t>(req.req_id.size()));
            append_u32_be(out, static_cast<uint32_t>(req.sql.size()));
            if (!req.req_id.empty()) out.append(req.req_id);
            if (!req.sql.empty()) out.append(req.sql);
            return true;
        }
        out.reserve(8 + req.req_id.size() + req.sql.size());
        append_u32_be(out, static_cast<uint32_t>(req.req_id.size()));
        append_u32_be(out, static_cast<uint32_t>(req.sql.size()));
        if (!req.req_id.empty()) out.append(req.req_id);
        if (!req.sql.empty()) out.append(req.sql);
        return true;
    }

    if (req.batch_items.empty() || req.batch_items.size() > kMaxBatchItems) {
        err = "invalid batch item count";
        return false;
    }
    if (req.wait_for_terminal) {
        err = "terminal wait flag is only supported for single-request frames";
        return false;
    }

    size_t total = 8;
    for (size_t i = 0; i < req.batch_items.size(); ++i) {
        if (req.batch_items[i].req_id.size() > kMaxReqIdBytes ||
            req.batch_items[i].sql.size() > kMaxSqlBytes) {
            err = "request too large";
            return false;
        }
        total += 8 + req.batch_items[i].req_id.size() + req.batch_items[i].sql.size();
    }
    out.reserve(total);
    append_u32_be(out, kBatchFrameTag);
    append_u32_be(out, static_cast<uint32_t>(req.batch_items.size()));
    for (size_t i = 0; i < req.batch_items.size(); ++i) {
        append_u32_be(out, static_cast<uint32_t>(req.batch_items[i].req_id.size()));
        append_u32_be(out, static_cast<uint32_t>(req.batch_items[i].sql.size()));
        if (!req.batch_items[i].req_id.empty()) out.append(req.batch_items[i].req_id);
        if (!req.batch_items[i].sql.empty()) out.append(req.batch_items[i].sql);
    }
    return true;
}

bool encode_response_frame(const client_api_response& resp, std::string& out, std::string& err) {
    err.clear();
    if (resp.msg.size() > kMaxMsgBytes) {
        err = "response too large";
        return false;
    }
    out.clear();
    out.reserve(1 + 4 + resp.msg.size());
    const uint8_t st = resp.status;
    out.push_back(static_cast<char>(st));
    append_u32_be(out, static_cast<uint32_t>(resp.msg.size()));
    if (!resp.msg.empty()) out.append(resp.msg);
    return true;
}

decode_status try_decode_request_frame(io_buffer& buf, client_api_request& out_req, std::string& err) {
    err.clear();
    const size_t avail = buf.size();
    if (avail < 8) return decode_status::NEED_MORE;
    const char* p = buf.ptr();
    uint32_t header = 0;
    if (!read_u32_be(p, header)) {
        err = "bad header";
        return decode_status::ERROR;
    }
    out_req.req_id.clear();
    out_req.sql.clear();
    out_req.has_assigned_det_seq = false;
    out_req.assigned_det_seq = 0;
    out_req.batch_items.clear();
    out_req.wait_for_terminal = false;
    out_req.terminal_timeout_ms = 30000;

    if (header == kSingleRequestFrameV2Tag) {
        if (avail < 20) return decode_status::NEED_MORE;
        uint32_t flags = 0;
        uint32_t timeout_ms = 0;
        uint32_t req_len = 0;
        uint32_t sql_len = 0;
        if (!read_u32_be(p + 4, flags) ||
            !read_u32_be(p + 8, timeout_ms) ||
            !read_u32_be(p + 12, req_len) ||
            !read_u32_be(p + 16, sql_len)) {
            err = "bad header";
            return decode_status::ERROR;
        }
        if (req_len > kMaxReqIdBytes || sql_len > kMaxSqlBytes) {
            err = "frame too large";
            return decode_status::ERROR;
        }
        const size_t total = 20ull + static_cast<size_t>(req_len) +
                             static_cast<size_t>(sql_len);
        if (avail < total) return decode_status::NEED_MORE;
        out_req.req_id.assign(p + 20, p + 20 + req_len);
        out_req.sql.assign(p + 20 + req_len, p + total);
        out_req.wait_for_terminal = (flags & kRequestFlagWaitForTerminal) != 0;
        out_req.terminal_timeout_ms = timeout_ms;
        buf.consume(total);
        return decode_status::OK;
    }

    if (header != kBatchFrameTag) {
        uint32_t sql_len = 0;
        const uint32_t req_len = header;
        if (!read_u32_be(p + 4, sql_len)) {
            err = "bad header";
            return decode_status::ERROR;
        }
        if (req_len > kMaxReqIdBytes || sql_len > kMaxSqlBytes) {
            err = "frame too large";
            return decode_status::ERROR;
        }
        const size_t total = 8ull + static_cast<size_t>(req_len) + static_cast<size_t>(sql_len);
        if (avail < total) return decode_status::NEED_MORE;
        out_req.req_id.assign(p + 8, p + 8 + req_len);
        out_req.sql.assign(p + 8 + req_len, p + total);
        buf.consume(total);
        return decode_status::OK;
    }

    uint32_t item_count = 0;
    if (!read_u32_be(p + 4, item_count)) {
        err = "bad header";
        return decode_status::ERROR;
    }
    if (item_count == 0 || item_count > kMaxBatchItems) {
        err = "invalid batch item count";
        return decode_status::ERROR;
    }

    size_t need = 8;
    size_t off = 8;
    std::vector<client_api_request_item> items;
    items.reserve(item_count);
    for (uint32_t i = 0; i < item_count; ++i) {
        if (avail < off + 8) return decode_status::NEED_MORE;
        uint32_t req_len = 0;
        uint32_t sql_len = 0;
        if (!read_u32_be(p + off, req_len) || !read_u32_be(p + off + 4, sql_len)) {
            err = "bad header";
            return decode_status::ERROR;
        }
        if (req_len > kMaxReqIdBytes || sql_len > kMaxSqlBytes) {
            err = "frame too large";
            return decode_status::ERROR;
        }
        const size_t item_total = 8ull + static_cast<size_t>(req_len) + static_cast<size_t>(sql_len);
        if (avail < off + item_total) return decode_status::NEED_MORE;
        client_api_request_item item;
        item.req_id.assign(p + off + 8, p + off + 8 + req_len);
        item.sql.assign(p + off + 8 + req_len, p + off + item_total);
        items.push_back(std::move(item));
        off += item_total;
        need += item_total;
    }

    out_req.batch_items.swap(items);
    buf.consume(need);
    return decode_status::OK;
}

decode_status try_decode_response_frame(io_buffer& buf, client_api_response& out_resp, std::string& err) {
    err.clear();
    const size_t avail = buf.size();
    if (avail < 5) return decode_status::NEED_MORE;
    const char* p = buf.ptr();
    const uint8_t st = static_cast<uint8_t>(p[0]);
    uint32_t msg_len = 0;
    if (!read_u32_be(p + 1, msg_len)) {
        err = "bad header";
        return decode_status::ERROR;
    }
    if (msg_len > kMaxMsgBytes) {
        err = "response too large";
        return decode_status::ERROR;
    }
    const size_t total = 5ull + static_cast<size_t>(msg_len);
    if (avail < total) return decode_status::NEED_MORE;
    out_resp.status = st;
    out_resp.msg.assign(p + 5, p + total);
    buf.consume(total);
    return decode_status::OK;
}

} // namespace ariabc_pg
