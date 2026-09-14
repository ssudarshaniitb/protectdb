#pragma once

#include "wire_protocol.hxx"

#include <cstddef>
#include <cstdint>
#include <string>

namespace ariabc_pg {

struct io_buffer {
    std::string data;
    size_t off = 0;

    size_t size() const { return (data.size() >= off) ? (data.size() - off) : 0; }
    const char* ptr() const { return data.data() + off; }

    void append(const void* p, size_t n) {
        if (!p || n == 0) return;
        data.append(reinterpret_cast<const char*>(p), n);
    }

    void consume(size_t n) {
        off += n;
        if (off == 0) return;
        if (off >= data.size()) {
            data.clear();
            off = 0;
            return;
        }
        // Avoid unbounded growth: compact occasionally.
        if (off * 2 >= data.size()) {
            data.erase(0, off);
            off = 0;
        }
    }
};

enum class decode_status : uint8_t {
    NEED_MORE = 0,
    OK = 1,
    ERROR = 2,
};

// Encode into an in-memory buffer (for nonblocking sockets / pipelining).
bool encode_request_frame(const client_api_request& req, std::string& out, std::string& err);
bool encode_response_frame(const client_api_response& resp, std::string& out, std::string& err);

// Try to decode one full frame from `buf`. On OK, consumes bytes and sets `out_*`.
// On NEED_MORE, leaves buf unchanged and err empty.
// On ERROR, leaves buf unchanged and sets err.
decode_status try_decode_request_frame(io_buffer& buf, client_api_request& out_req, std::string& err);
decode_status try_decode_response_frame(io_buffer& buf, client_api_response& out_resp, std::string& err);

} // namespace ariabc_pg

