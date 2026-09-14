#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace ariabc_pg {

inline std::string trim_copy(const std::string& s) {
    size_t b = 0;
    while (b < s.size() && std::isspace(static_cast<unsigned char>(s[b]))) b++;
    size_t e = s.size();
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) e--;
    return s.substr(b, e - b);
}

inline std::string to_hex_string(const unsigned char* hash, size_t len) {
    char buf[256];
    for (size_t i = 0; i < len; ++i) {
        sprintf(buf + 2 * i, "%02x", hash[i]);
    }
    return std::string(buf, 2 * len);
}

inline std::vector<uint8_t> decode_hex_string(const std::string& hex) {
    if (hex.size() % 2 != 0) return {};
    std::vector<uint8_t> bytes;
    bytes.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2) {
        char c1 = hex[i];
        char c2 = hex[i + 1];
        if (!std::isxdigit(static_cast<unsigned char>(c1)) ||
            !std::isxdigit(static_cast<unsigned char>(c2))) {
            return {};
        }
        std::string byteString = hex.substr(i, 2);
        uint8_t byte = static_cast<uint8_t>(strtol(byteString.c_str(), nullptr, 16));
        bytes.push_back(byte);
    }
    return bytes;
}

inline bool is_number(const std::string& s) {
    if (s.empty()) return false;
    for (char c : s) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    }
    return true;
}

inline std::vector<std::string> split_char(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::string cur;
    std::istringstream iss(s);
    while (std::getline(iss, cur, delim)) {
        out.push_back(cur);
    }
    return out;
}

inline std::vector<std::string> split_csv_trim(const std::string& s) {
    std::vector<std::string> parts = split_char(s, ',');
    std::vector<std::string> out;
    out.reserve(parts.size());
    for (auto& p : parts) {
        std::string t = trim_copy(p);
        if (!t.empty()) out.push_back(t);
    }
    return out;
}

struct host_port {
    std::string host;
    int port = -1;
};

inline host_port parse_host_port(const std::string& s) {
    const std::string t = trim_copy(s);
    const size_t pos = t.rfind(':');
    if (pos == std::string::npos) {
        throw std::runtime_error("expected host:port, got: " + t);
    }
    host_port hp;
    hp.host = t.substr(0, pos);
    hp.port = std::stoi(t.substr(pos + 1));
    if (hp.port <= 0 || hp.port > 65535) {
        throw std::runtime_error("invalid port in: " + t);
    }
    if (hp.host.empty()) hp.host = "127.0.0.1";
    return hp;
}

inline std::string read_file_all(const std::string& path) {
    std::ifstream ifs(path.c_str(), std::ios::in | std::ios::binary);
    if (!ifs) throw std::runtime_error("failed to open file: " + path);
    std::ostringstream oss;
    oss << ifs.rdbuf();
    return oss.str();
}

inline std::string read_file_trimmed(const std::string& path) {
    return trim_copy(read_file_all(path));
}

// Minimal base64 (RFC 4648) encode/decode.
// - Ignores whitespace in decode input.
// - Throws on invalid characters.
inline std::string base64_encode(const std::string& bin) {
    static const char* kTable =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((bin.size() + 2) / 3) * 4);
    size_t i = 0;
    while (i < bin.size()) {
        const size_t remaining = bin.size() - i;
        const uint32_t a = static_cast<unsigned char>(bin[i++]);
        const uint32_t b = (remaining > 1) ? static_cast<unsigned char>(bin[i++]) : 0;
        const uint32_t c = (remaining > 2) ? static_cast<unsigned char>(bin[i++]) : 0;
        const uint32_t triple = (a << 16) | (b << 8) | c;

        out.push_back(kTable[(triple >> 18) & 0x3F]);
        out.push_back(kTable[(triple >> 12) & 0x3F]);
        out.push_back((remaining > 1) ? kTable[(triple >> 6) & 0x3F] : '=');
        out.push_back((remaining > 2) ? kTable[triple & 0x3F] : '=');
    }
    return out;
}

inline int base64_val(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    if (c == '=') return -2; // padding
    return -1;
}

inline std::string base64_decode(const std::string& b64) {
    std::string in;
    in.reserve(b64.size());
    for (char c : b64) {
        if (std::isspace(static_cast<unsigned char>(c))) continue;
        in.push_back(c);
    }
    if (in.size() % 4 != 0) {
        throw std::runtime_error("invalid base64 length");
    }

    std::string out;
    out.reserve((in.size() / 4) * 3);
    for (size_t i = 0; i < in.size(); i += 4) {
        int v0 = base64_val(in[i]);
        int v1 = base64_val(in[i + 1]);
        int v2 = base64_val(in[i + 2]);
        int v3 = base64_val(in[i + 3]);
        if (v0 < 0 || v1 < 0 || v2 == -1 || v3 == -1) {
            throw std::runtime_error("invalid base64 character");
        }

        const uint32_t triple =
            (static_cast<uint32_t>(v0) << 18) |
            (static_cast<uint32_t>(v1) << 12) |
            ((v2 > -1 ? static_cast<uint32_t>(v2) : 0) << 6) |
            (v3 > -1 ? static_cast<uint32_t>(v3) : 0);

        out.push_back(static_cast<char>((triple >> 16) & 0xFF));
        if (v2 != -2) out.push_back(static_cast<char>((triple >> 8) & 0xFF));
        if (v3 != -2) out.push_back(static_cast<char>(triple & 0xFF));
    }
    return out;
}

inline bool is_skippable_sql_line(const std::string& line) {
    const std::string s = trim_copy(line);
    if (s.empty()) return true;
    if (s.rfind("--", 0) == 0) return true;
    if (s.rfind("/*", 0) == 0) return true;
    if (s.rfind("\\", 0) == 0) return true;
    return false;
}

} // namespace ariabc_pg
