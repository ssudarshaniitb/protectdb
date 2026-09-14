// ariabc_pg/src/raft_storage_common.cxx
// POSIX I/O helpers for durable Raft storage. C++11 compatible.

#include "raft_storage_common.hxx"

#include <cerrno>
#include <cstring>
#include <sstream>

// POSIX headers
#include <dirent.h>
#include <fcntl.h>
#include <sys/file.h>   // flock
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// zlib for CRC-32
#include <zlib.h>

namespace ariabc_raft {

// -------------------------------------------------------------------------
// Helper: build error message with path and errno
// -------------------------------------------------------------------------
static std::string io_err(const std::string& context,
                          const std::string& path,
                          int saved_errno) {
    std::ostringstream os;
    os << context;
    if (!path.empty()) os << " [" << path << "]";
    os << ": " << ::strerror(saved_errno);
    return os.str();
}

// -------------------------------------------------------------------------
// write_full
// -------------------------------------------------------------------------
void write_full(int fd, const void* buf, size_t len, const std::string& path) {
    const uint8_t* p = static_cast<const uint8_t*>(buf);
    size_t remaining = len;
    while (remaining > 0) {
        const ssize_t n = ::write(fd, p, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw storage_io_error(io_err("write_full failed", path, errno));
        }
        if (n == 0) {
            throw storage_io_error("write_full: unexpected zero-length write [" + path + "]");
        }
        p         += static_cast<size_t>(n);
        remaining -= static_cast<size_t>(n);
    }
}

// -------------------------------------------------------------------------
// read_full
// -------------------------------------------------------------------------
bool read_full(int fd, void* buf, size_t len, const std::string& path) {
    uint8_t* p = static_cast<uint8_t*>(buf);
    size_t remaining = len;
    bool got_any = false;
    while (remaining > 0) {
        const ssize_t n = ::read(fd, p, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw storage_io_error(io_err("read_full failed", path, errno));
        }
        if (n == 0) {
            // EOF
            if (!got_any) return false;   // clean EOF before first byte → caller decides
            // Partial read: that is recoverable only if the caller handles it as a torn write.
            // We expose this as a truncation so the caller can detect incomplete trailing records.
            throw storage_io_error(
                "read_full: unexpected EOF mid-record (partial read " +
                std::to_string(len - remaining) + "/" + std::to_string(len) +
                " bytes) [" + path + "]");
        }
        got_any   = true;
        p         += static_cast<size_t>(n);
        remaining -= static_cast<size_t>(n);
    }
    return true;
}

// -------------------------------------------------------------------------
// fdatasync_or_throw
// -------------------------------------------------------------------------
void fdatasync_or_throw(int fd, const std::string& context) {
    if (::fdatasync(fd) != 0) {
        throw storage_io_error(io_err("fdatasync failed in " + context, "", errno));
    }
}

// -------------------------------------------------------------------------
// fsync_directory_or_throw
// -------------------------------------------------------------------------
void fsync_directory_or_throw(const std::string& dir_path) {
    const int fd = ::open(dir_path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw storage_io_error(io_err("fsync_directory open failed", dir_path, errno));
    }
    if (::fsync(fd) != 0) {
        const int saved = errno;
        ::close(fd);
        throw storage_io_error(io_err("fsync_directory fsync failed", dir_path, saved));
    }
    ::close(fd);
}

// -------------------------------------------------------------------------
// atomic_write_file  — write → fdatasync(tmp) → rename → fsync(dir)
// -------------------------------------------------------------------------
void atomic_write_file(const std::string& path, const std::vector<uint8_t>& bytes) {
    const std::string tmp = path + ".tmp";

    // Determine parent directory.
    std::string dir = ".";
    const size_t slash = path.rfind('/');
    if (slash != std::string::npos && slash > 0) {
        dir = path.substr(0, slash);
    }

    const int fd = ::open(tmp.c_str(),
                          O_WRONLY | O_CREAT | O_TRUNC,
                          static_cast<mode_t>(0600));
    if (fd < 0) {
        throw storage_io_error(io_err("atomic_write_file open tmp failed", tmp, errno));
    }

    try {
        if (!bytes.empty()) {
            write_full(fd, bytes.data(), bytes.size(), tmp);
        }
        fdatasync_or_throw(fd, "atomic_write_file tmp sync");
    } catch (...) {
        ::close(fd);
        ::unlink(tmp.c_str());
        throw;
    }
    ::close(fd);

    if (::rename(tmp.c_str(), path.c_str()) != 0) {
        const int saved = errno;
        ::unlink(tmp.c_str());
        throw storage_io_error(io_err("atomic_write_file rename failed", path, saved));
    }

    // Flush directory so rename is visible after crash.
    fsync_directory_or_throw(dir);
}

// -------------------------------------------------------------------------
// read_entire_file
// -------------------------------------------------------------------------
std::vector<uint8_t> read_entire_file(const std::string& path) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw storage_io_error(io_err("read_entire_file open failed", path, errno));
    }

    // Stat to know size.
    struct stat st;
    if (::fstat(fd, &st) != 0) {
        const int saved = errno;
        ::close(fd);
        throw storage_io_error(io_err("read_entire_file fstat failed", path, saved));
    }

    std::vector<uint8_t> out(static_cast<size_t>(st.st_size));
    if (st.st_size > 0) {
        try {
            read_full(fd, out.data(), out.size(), path);
        } catch (...) {
            ::close(fd);
            throw;
        }
    }
    ::close(fd);
    return out;
}

// -------------------------------------------------------------------------
// crc32_bytes
// -------------------------------------------------------------------------
uint32_t crc32_bytes(const uint8_t* data, size_t len) {
    uLong crc = ::crc32(0L, Z_NULL, 0);
    if (len > 0) {
        crc = ::crc32(crc, static_cast<const Bytef*>(data), static_cast<uInt>(len));
    }
    return static_cast<uint32_t>(crc);
}

// -------------------------------------------------------------------------
// ensure_directory  — mkdir -p equivalent using POSIX mkdir
// -------------------------------------------------------------------------
void ensure_directory(const std::string& path) {
    // Walk the path, creating each component.
    std::string cur;
    cur.reserve(path.size());
    for (size_t i = 0; i <= path.size(); ++i) {
        if (i == path.size() || path[i] == '/') {
            if (!cur.empty()) {
                if (::mkdir(cur.c_str(), static_cast<mode_t>(0755)) != 0) {
                    if (errno != EEXIST) {
                        throw storage_io_error(
                            io_err("ensure_directory mkdir failed", cur, errno));
                    }
                }
            }
        }
        if (i < path.size()) {
            cur += path[i];
        }
    }
}

// -------------------------------------------------------------------------
// acquire_exclusive_lock
// -------------------------------------------------------------------------
int acquire_exclusive_lock(const std::string& dir_path) {
    const std::string lock_path = dir_path + "/LOCK";

    const int fd = ::open(lock_path.c_str(),
                          O_WRONLY | O_CREAT,
                          static_cast<mode_t>(0600));
    if (fd < 0) {
        throw storage_io_error(io_err("acquire_exclusive_lock open failed", lock_path, errno));
    }

    // LOCK_EX | LOCK_NB: non-blocking exclusive lock.
    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        const int saved = errno;
        ::close(fd);
        if (saved == EWOULDBLOCK || saved == EAGAIN) {
            throw storage_lock_error(
                "Raft storage directory is already locked by another process: " + dir_path +
                " (LOCK file: " + lock_path + ")");
        }
        throw storage_io_error(io_err("acquire_exclusive_lock flock failed", lock_path, saved));
    }

    // Write our PID for diagnostic purposes.
    const std::string pid_str = std::to_string(static_cast<long>(::getpid())) + "\n";
    // Truncate first (ignore errors — the lock is what matters).
    int tr_res = ::ftruncate(fd, 0);
    (void)tr_res;
    ssize_t wr_res = ::write(fd, pid_str.c_str(), pid_str.size());
    (void)wr_res;

    return fd;
}

// -------------------------------------------------------------------------
// Integer serialization: little-endian fixed-width
// -------------------------------------------------------------------------
void put_le32(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >>  8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
}

void put_le64(std::vector<uint8_t>& buf, uint64_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >>  8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 32) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 40) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 48) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 56) & 0xFF));
}

uint32_t get_le32(const uint8_t* p) {
    return ( static_cast<uint32_t>(p[0])
           | static_cast<uint32_t>(p[1]) <<  8
           | static_cast<uint32_t>(p[2]) << 16
           | static_cast<uint32_t>(p[3]) << 24 );
}

uint64_t get_le64(const uint8_t* p) {
    return ( static_cast<uint64_t>(p[0])
           | static_cast<uint64_t>(p[1]) <<  8
           | static_cast<uint64_t>(p[2]) << 16
           | static_cast<uint64_t>(p[3]) << 24
           | static_cast<uint64_t>(p[4]) << 32
           | static_cast<uint64_t>(p[5]) << 40
           | static_cast<uint64_t>(p[6]) << 48
           | static_cast<uint64_t>(p[7]) << 56 );
}

} // namespace ariabc_raft
