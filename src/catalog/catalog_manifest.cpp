/**
 * @file catalog_manifest.cpp
 * @brief Catalog manifest serialization and durable I/O
 */

#include "catalog/catalog_manifest.hpp"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

#include "catalog/column.hpp"

namespace entropy {
namespace {

// On-disk format identifier and version. Bump the version if the layout below
// changes in an incompatible way.
constexpr char kMagic[4] = {'E', 'C', 'A', 'T'};
constexpr uint32_t kVersion = 1;

// ─────────────────────────────────────────────────────────────────────────────
// Little-ish serialization helpers (native byte order; single-machine format)
// ─────────────────────────────────────────────────────────────────────────────

void put_bytes(std::vector<char> &b, const void *src, size_t n) {
  const char *p = static_cast<const char *>(src);
  b.insert(b.end(), p, p + n);
}

void put_u8(std::vector<char> &b, uint8_t v) { b.push_back(static_cast<char>(v)); }
void put_u16(std::vector<char> &b, uint16_t v) { put_bytes(b, &v, sizeof(v)); }
void put_u32(std::vector<char> &b, uint32_t v) { put_bytes(b, &v, sizeof(v)); }
void put_i32(std::vector<char> &b, int32_t v) { put_bytes(b, &v, sizeof(v)); }
void put_u64(std::vector<char> &b, uint64_t v) { put_bytes(b, &v, sizeof(v)); }

void put_str(std::vector<char> &b, const std::string &s) {
  put_u32(b, static_cast<uint32_t>(s.size()));
  put_bytes(b, s.data(), s.size());
}

// Bounds-checked cursor over a byte buffer. Every getter returns false on
// underflow so deserialize() can report corruption instead of reading OOB.
class Reader {
public:
  explicit Reader(const std::vector<char> &bytes) : data_(bytes) {}

  [[nodiscard]] bool get_bytes(void *dst, size_t n) {
    if (pos_ + n > data_.size()) {
      return false;
    }
    std::memcpy(dst, data_.data() + pos_, n);
    pos_ += n;
    return true;
  }

  [[nodiscard]] bool get_u8(uint8_t *v) { return get_bytes(v, sizeof(*v)); }
  [[nodiscard]] bool get_u16(uint16_t *v) { return get_bytes(v, sizeof(*v)); }
  [[nodiscard]] bool get_u32(uint32_t *v) { return get_bytes(v, sizeof(*v)); }
  [[nodiscard]] bool get_i32(int32_t *v) { return get_bytes(v, sizeof(*v)); }
  [[nodiscard]] bool get_u64(uint64_t *v) { return get_bytes(v, sizeof(*v)); }

  [[nodiscard]] bool get_str(std::string *s) {
    uint32_t len = 0;
    if (!get_u32(&len)) {
      return false;
    }
    if (pos_ + len > data_.size()) {
      return false;
    }
    s->assign(data_.data() + pos_, len);
    pos_ += len;
    return true;
  }

private:
  const std::vector<char> &data_;
  size_t pos_ = 0;
};

} // namespace

std::vector<char> CatalogManifest::serialize() const {
  std::vector<char> b;

  put_bytes(b, kMagic, sizeof(kMagic));
  put_u32(b, kVersion);
  put_u32(b, next_oid);

  put_u32(b, static_cast<uint32_t>(tables.size()));
  for (const auto &t : tables) {
    put_u32(b, t.oid);
    put_i32(b, t.first_page_id);
    put_str(b, t.name);
    put_u32(b, static_cast<uint32_t>(t.schema.column_count()));
    for (const auto &col : t.schema.columns()) {
      put_str(b, col.name());
      put_u8(b, static_cast<uint8_t>(col.type()));
      put_u64(b, col.length());
      put_u8(b, col.is_nullable() ? 1 : 0);
    }
  }

  put_u32(b, static_cast<uint32_t>(indexes.size()));
  for (const auto &idx : indexes) {
    put_u32(b, idx.oid);
    put_u32(b, idx.table_oid);
    put_u16(b, idx.key_column);
    put_i32(b, idx.root_page_id);
    put_str(b, idx.name);
  }

  return b;
}

Status CatalogManifest::deserialize(const std::vector<char> &bytes,
                                    CatalogManifest *out) {
  if (out == nullptr) {
    return Status::InvalidArgument("Manifest output pointer is null");
  }

  Reader r(bytes);

  char magic[4];
  if (!r.get_bytes(magic, sizeof(magic)) ||
      std::memcmp(magic, kMagic, sizeof(kMagic)) != 0) {
    return Status::Corruption("Bad catalog manifest magic");
  }

  uint32_t version = 0;
  if (!r.get_u32(&version)) {
    return Status::Corruption("Truncated catalog manifest header");
  }
  if (version != kVersion) {
    return Status::Corruption("Unsupported catalog manifest version");
  }

  CatalogManifest m;
  if (!r.get_u32(&m.next_oid)) {
    return Status::Corruption("Truncated catalog manifest next_oid");
  }

  uint32_t table_count = 0;
  if (!r.get_u32(&table_count)) {
    return Status::Corruption("Truncated catalog manifest table count");
  }
  m.tables.reserve(table_count);
  for (uint32_t i = 0; i < table_count; ++i) {
    ManifestTable t;
    uint32_t col_count = 0;
    if (!r.get_u32(&t.oid) || !r.get_i32(&t.first_page_id) ||
        !r.get_str(&t.name) || !r.get_u32(&col_count)) {
      return Status::Corruption("Truncated catalog manifest table entry");
    }
    std::vector<Column> columns;
    columns.reserve(col_count);
    for (uint32_t c = 0; c < col_count; ++c) {
      std::string col_name;
      uint8_t type = 0;
      uint64_t length = 0;
      uint8_t nullable = 0;
      if (!r.get_str(&col_name) || !r.get_u8(&type) || !r.get_u64(&length) ||
          !r.get_u8(&nullable)) {
        return Status::Corruption("Truncated catalog manifest column entry");
      }
      Column col(std::move(col_name), static_cast<TypeId>(type),
                 static_cast<size_t>(length));
      col.set_nullable(nullable != 0);
      columns.push_back(std::move(col));
    }
    t.schema = Schema(std::move(columns));
    m.tables.push_back(std::move(t));
  }

  uint32_t index_count = 0;
  if (!r.get_u32(&index_count)) {
    return Status::Corruption("Truncated catalog manifest index count");
  }
  m.indexes.reserve(index_count);
  for (uint32_t i = 0; i < index_count; ++i) {
    ManifestIndex idx;
    if (!r.get_u32(&idx.oid) || !r.get_u32(&idx.table_oid) ||
        !r.get_u16(&idx.key_column) || !r.get_i32(&idx.root_page_id) ||
        !r.get_str(&idx.name)) {
      return Status::Corruption("Truncated catalog manifest index entry");
    }
    m.indexes.push_back(std::move(idx));
  }

  *out = std::move(m);
  return Status::Ok();
}

Status CatalogManifest::save(const std::string &path) const {
  const std::vector<char> bytes = serialize();
  const std::string tmp = path + ".tmp";

#ifdef _WIN32
  const HANDLE handle =
      ::CreateFileA(tmp.c_str(), GENERIC_WRITE, 0, nullptr, CREATE_ALWAYS,
                    FILE_ATTRIBUTE_NORMAL, nullptr);
  if (handle == INVALID_HANDLE_VALUE) {
    return Status::IOError("Failed to open temp manifest: " + tmp);
  }

  size_t written = 0;
  while (written < bytes.size()) {
    DWORD chunk = 0;
    if (!::WriteFile(handle, bytes.data() + written,
                     static_cast<DWORD>(bytes.size() - written), &chunk,
                     nullptr)) {
      ::CloseHandle(handle);
      return Status::IOError("Failed to write manifest");
    }
    written += chunk;
  }

  if (!::FlushFileBuffers(handle)) {
    ::CloseHandle(handle);
    return Status::IOError("Failed to flush manifest to disk");
  }
  ::CloseHandle(handle);

  // rename() fails on Windows when the destination exists; MoveFileEx with
  // MOVEFILE_REPLACE_EXISTING is the atomic-replace primitive there.
  if (!::MoveFileExA(tmp.c_str(), path.c_str(),
                     MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
    return Status::IOError("Failed to move manifest into place");
  }
#else
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    return Status::IOError("Failed to open temp manifest: " + tmp);
  }

  size_t written = 0;
  while (written < bytes.size()) {
    ssize_t n = ::write(fd, bytes.data() + written, bytes.size() - written);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      ::close(fd);
      return Status::IOError("Failed to write manifest");
    }
    written += static_cast<size_t>(n);
  }

  if (::fsync(fd) != 0) {
    ::close(fd);
    return Status::IOError("Failed to fsync manifest");
  }
  if (::close(fd) != 0) {
    return Status::IOError("Failed to close manifest");
  }

  if (::rename(tmp.c_str(), path.c_str()) != 0) {
    return Status::IOError("Failed to rename manifest into place");
  }

  // fsync the directory so the rename survives a crash (POSIX-only concept).
  std::string dir = std::filesystem::path(path).parent_path().string();
  if (dir.empty()) {
    dir = ".";
  }
  int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (dfd >= 0) {
    (void)::fsync(dfd);
    ::close(dfd);
  }
#endif

  return Status::Ok();
}

Status CatalogManifest::load(const std::string &path, CatalogManifest *out) {
  std::error_code ec;
  if (!std::filesystem::exists(path, ec)) {
    return Status::NotFound("Catalog manifest not found: " + path);
  }

  std::ifstream in(path, std::ios::binary);
  if (!in) {
    return Status::IOError("Failed to open manifest for read: " + path);
  }

  std::vector<char> bytes((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());
  return deserialize(bytes, out);
}

} // namespace entropy
