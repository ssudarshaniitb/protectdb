// ariabc_pg/src/raft_storage_common.hxx
// Common POSIX helpers for durable Raft storage.
// Uses only POSIX APIs compatible with C++11 (no C++17 filesystem).
#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace ariabc_raft {

// -------------------------------------------------------------------------
// Exceptions
// -------------------------------------------------------------------------

/// Thrown when a storage I/O error occurs (includes path + errno).
class storage_io_error : public std::runtime_error {
public:
    explicit storage_io_error(const std::string& msg) : std::runtime_error(msg) {}
};

/// Thrown when the storage directory lock is held by another process.
class storage_lock_error : public std::runtime_error {
public:
    explicit storage_lock_error(const std::string& msg) : std::runtime_error(msg) {}
};

/// Thrown when a storage record's CRC does not match (corruption).
class storage_corruption_error : public std::runtime_error {
public:
    explicit storage_corruption_error(const std::string& msg) : std::runtime_error(msg) {}
};

// -------------------------------------------------------------------------
// Low-level I/O helpers
// -------------------------------------------------------------------------

/// Write exactly len bytes from buf to fd; retries on EINTR; throws on short write.
void write_full(int fd, const void* buf, size_t len, const std::string& path);

/// Read exactly len bytes from fd into buf; retries on EINTR; throws on short read.
/// Returns false if EOF is reached before any byte is read (clean EOF).
/// Throws if partial read is encountered.
bool read_full(int fd, void* buf, size_t len, const std::string& path);

/// fdatasync fd; throws storage_io_error on failure.
void fdatasync_or_throw(int fd, const std::string& context);

/// fsync the directory at path (so that renames become visible).
void fsync_directory_or_throw(const std::string& dir_path);

/// Write bytes atomically to path using tmp→rename pattern with fdatasync.
void atomic_write_file(const std::string& path, const std::vector<uint8_t>& bytes);

/// Read the entire content of a file into a vector.
std::vector<uint8_t> read_entire_file(const std::string& path);

/// Compute CRC-32 of a byte buffer using zlib crc32.
uint32_t crc32_bytes(const uint8_t* data, size_t len);

/// Create a directory (and parents) if it does not exist.
void ensure_directory(const std::string& path);

/// Acquire an exclusive POSIX flock on path/LOCK; returns the open fd.
/// Throws storage_lock_error if already locked by another process.
int acquire_exclusive_lock(const std::string& dir_path);

// -------------------------------------------------------------------------
// Little-endian integer serialization (fixed width, explicit byte order)
// -------------------------------------------------------------------------

void put_le32(std::vector<uint8_t>& buf, uint32_t v);
void put_le64(std::vector<uint8_t>& buf, uint64_t v);
uint32_t get_le32(const uint8_t* p);
uint64_t get_le64(const uint8_t* p);

} // namespace ariabc_raft
