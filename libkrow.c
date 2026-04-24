/**
 * libkrow.c - Persistent mmap key-value store implementation
 * Summary: Core implementation of the krow storage engine.
 *
 * Author:  KaisarCode
 * Website: https://kaisarcode.com
 * License: https://www.gnu.org/licenses/gpl-3.0.html
 */

#ifndef _WIN32
#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#else
#include <windows.h>
#endif

#include "krow.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define KC_KROW_FIBO 11400714819323198485ULL
#define KC_KROW_INITIAL_HEAP (1024ULL * 1024ULL)
#define KC_KROW_VERSION 1U
#define KC_KROW_COMMIT 0x6b726f77636d7441ULL
#define KC_KROW_TOMBSTONE 0x6b726f7764656c41ULL
#define KC_KROW_TMP_SUFFIX ".tmp"

typedef struct {
    uint64_t key;
    uint64_t offset;
    uint64_t length;
    uint64_t checksum;
    uint64_t commit;
} kc_krow_node_t;

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t header_size;
    uint64_t node_size;
    uint64_t capacity;
    uint64_t count;
    uint64_t deleted;
    uint64_t data_tail;
    uint64_t checksum;
} kc_krow_header_t;

typedef struct {
#ifndef _WIN32
    int fd;
#else
    HANDLE file;
    HANDLE map;
#endif
    void *ptr;
    size_t size;
} kc_map_t;

typedef struct {
#ifndef _WIN32
    int fd;
#else
    HANDLE file;
#endif
} kc_lock_t;

struct kc_krow {
    kc_lock_t lock;
    kc_map_t map;
#ifndef _WIN32
    pthread_mutex_t mutex;
#else
    CRITICAL_SECTION mutex;
#endif
    kc_krow_header_t *header;
    kc_krow_node_t *index;
    uint8_t *heap;
    char *path;
};

/**
 * Check if two size_t values would overflow on addition.
 * @param a First operand.
 * @param b Second operand.
 * @return 1 on overflow, 0 otherwise.
 */
static int kc_size_add_overflows(size_t a, size_t b) {
    return b > SIZE_MAX - a ? 1 : 0;
}

/**
 * Check if two uint64_t values would overflow on addition.
 * @param a First operand.
 * @param b Second operand.
 * @return 1 on overflow, 0 otherwise.
 */
static int kc_u64_add_overflows(uint64_t a, uint64_t b) {
    return b > UINT64_MAX - a ? 1 : 0;
}

/**
 * Check if a multiplication of size_t values would overflow.
 * @param a First operand.
 * @param b Second operand.
 * @return 1 on overflow, 0 otherwise.
 */
static int kc_size_mul_overflows(size_t a, size_t b) {
    if (a == 0 || b == 0) {
        return 0;
    }
    return a > SIZE_MAX / b ? 1 : 0;
}

/**
 * Compute a stable FNV-1a checksum.
 * @param data Input buffer.
 * @param size Input size.
 * @param seed Initial checksum seed.
 * @return Checksum value.
 */
static uint64_t kc_checksum_bytes(const void *data, size_t size,
uint64_t seed) {
    const uint8_t *p;
    size_t i;

    p = data;
    for (i = 0; i < size; i++) {
        seed ^= p[i];
        seed *= 1099511628211ULL;
    }
    return seed;
}

/**
 * Compute checksum for a header.
 * @param h Header pointer.
 * @return Header checksum.
 */
static uint64_t kc_header_checksum(const kc_krow_header_t *h) {
    kc_krow_header_t copy;

    copy = *h;
    copy.checksum = 0;
    return kc_checksum_bytes(&copy, sizeof(copy), 1469598103934665603ULL);
}

/**
 * Compute checksum for one live entry.
 * @param key Entry key.
 * @param offset Entry absolute value offset.
 * @param length Entry value length.
 * @param value Entry value bytes.
 * @return Entry checksum.
 */
static uint64_t kc_entry_checksum(uint64_t key, uint64_t offset,
uint64_t length, const void *value) {
    uint64_t hash;

    hash = 1469598103934665603ULL;
    hash = kc_checksum_bytes(&key, sizeof(key), hash);
    hash = kc_checksum_bytes(&offset, sizeof(offset), hash);
    hash = kc_checksum_bytes(&length, sizeof(length), hash);
    return kc_checksum_bytes(value, (size_t)length, hash);
}

/**
 * Compute hash slot for a key within a capacity.
 * @param key Record key.
 * @param capacity Index capacity.
 * @return Slot index.
 */
static uint64_t kc_krow_hash_slot(uint64_t key, uint64_t capacity) {
    return (key * KC_KROW_FIBO) % capacity;
}

/**
 * Query the size of an existing file.
 * @param path File path.
 * @param size_out Output pointer for file size.
 * @return 0 if file exists and size is readable, -1 otherwise.
 */
static int kc_stat_size(const char *path, uint64_t *size_out) {
#ifndef _WIN32
    struct stat st;

    if (!path || !size_out || stat(path, &st) == -1) {
        return -1;
    }
    if (st.st_size < 0) {
        return -1;
    }
    *size_out = (uint64_t)st.st_size;
    return 0;
#else
    WIN32_FILE_ATTRIBUTE_DATA st;

    if (!path || !size_out ||
        !GetFileAttributesExA(path, GetFileExInfoStandard, &st)) {
        return -1;
    }
    *size_out = ((uint64_t)st.nFileSizeHigh << 32) | st.nFileSizeLow;
    return 0;
#endif
}

/**
 * Allocate a path with an appended suffix.
 * @param path Base path.
 * @param suffix Suffix text.
 * @return Allocated path or NULL on failure.
 */
static char *kc_path_with_suffix(const char *path, const char *suffix) {
    char *out;
    size_t path_len;
    size_t suffix_len;

    if (!path || !suffix) {
        return NULL;
    }
    path_len = strlen(path);
    suffix_len = strlen(suffix);
    if (kc_size_add_overflows(path_len, suffix_len + 1)) {
        return NULL;
    }
    out = malloc(path_len + suffix_len + 1);
    if (!out) {
        return NULL;
    }
    memcpy(out, path, path_len);
    memcpy(out + path_len, suffix, suffix_len + 1);
    return out;
}

/**
 * Acquire the stable path lock used across atomic replacement.
 * @param lock Lock context.
 * @param path Store path.
 * @return 0 on success, -1 on failure.
 */
static int kc_lock_open(kc_lock_t *lock, const char *path) {
    char *lock_path;
#ifndef _WIN32
    struct flock lk;
#else
    OVERLAPPED ov;
#endif

    if (!lock || !path) {
        return -1;
    }
    lock_path = kc_path_with_suffix(path, ".lock");
    if (!lock_path) {
        return -1;
    }
#ifndef _WIN32
    lock->fd = open(lock_path, O_RDWR | O_CREAT, 0644);
    free(lock_path);
    if (lock->fd == -1) {
        return -1;
    }
    memset(&lk, 0, sizeof(lk));
    lk.l_type = F_WRLCK;
    lk.l_whence = SEEK_SET;
    lk.l_start = 0;
    lk.l_len = 0;
    if (fcntl(lock->fd, F_SETLK, &lk) == -1) {
        close(lock->fd);
        lock->fd = -1;
        return -1;
    }
    return 0;
#else
    lock->file = CreateFileA(lock_path, GENERIC_READ | GENERIC_WRITE,
        0, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    free(lock_path);
    if (lock->file == INVALID_HANDLE_VALUE) {
        return -1;
    }
    memset(&ov, 0, sizeof(ov));
    if (!LockFileEx(lock->file,
        LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
        0, MAXDWORD, MAXDWORD, &ov)) {
        CloseHandle(lock->file);
        lock->file = INVALID_HANDLE_VALUE;
        return -1;
    }
    return 0;
#endif
}

/**
 * Release a stable path lock.
 * @param lock Lock context.
 * @return None.
 */
static void kc_lock_close(kc_lock_t *lock) {
    if (!lock) {
        return;
    }
#ifndef _WIN32
    if (lock->fd != -1) {
        close(lock->fd);
    }
    lock->fd = -1;
#else
    if (lock->file && lock->file != INVALID_HANDLE_VALUE) {
        CloseHandle(lock->file);
    }
    lock->file = INVALID_HANDLE_VALUE;
#endif
}

/**
 * Initialize in-process synchronization primitives.
 * @param ctx Context pointer.
 * @return Status code.
 */
static int kc_krow_thread_init(kc_krow_t *ctx) {
#ifndef _WIN32
    if (pthread_mutex_init(&ctx->mutex, NULL) != 0) {
        return KC_KROW_ERROR;
    }
#else
    InitializeCriticalSection(&ctx->mutex);
#endif
    return KC_KROW_OK;
}

/**
 * Destroy in-process synchronization primitives.
 * @param ctx Context pointer.
 * @return None.
 */
static void kc_krow_thread_destroy(kc_krow_t *ctx) {
#ifndef _WIN32
    pthread_mutex_destroy(&ctx->mutex);
#else
    DeleteCriticalSection(&ctx->mutex);
#endif
}

/**
 * Enter one serialized public operation.
 * @param ctx Context pointer.
 * @return Status code.
 */
static int kc_krow_enter(kc_krow_t *ctx) {
    if (!ctx) {
        return KC_KROW_ERROR;
    }
#ifndef _WIN32
    if (pthread_mutex_lock(&ctx->mutex) != 0) {
        return KC_KROW_ERROR;
    }
#else
    EnterCriticalSection(&ctx->mutex);
#endif
    return KC_KROW_OK;
}

/**
 * Leave one serialized public operation.
 * @param ctx Context pointer.
 * @return None.
 */
static void kc_krow_leave(kc_krow_t *ctx) {
#ifndef _WIN32
    pthread_mutex_unlock(&ctx->mutex);
#else
    LeaveCriticalSection(&ctx->mutex);
#endif
}

/**
 * Acquire an exclusive process lock on the backing file.
 * @param m Map context.
 * @return 0 on success, -1 on failure.
 */
static int kc_map_lock(kc_map_t *m) {
#ifndef _WIN32
    struct flock lk;

    memset(&lk, 0, sizeof(lk));
    lk.l_type = F_WRLCK;
    lk.l_whence = SEEK_SET;
    lk.l_start = 0;
    lk.l_len = 0;
    return fcntl(m->fd, F_SETLK, &lk) == -1 ? -1 : 0;
#else
    OVERLAPPED ov;

    memset(&ov, 0, sizeof(ov));
    if (!LockFileEx(m->file,
        LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
        0, MAXDWORD, MAXDWORD, &ov)) {
        return -1;
    }
    return 0;
#endif
}

/**
 * Open memory map abstraction.
 * @param m Map context.
 * @param path File path.
 * @param size Requested size.
 * @param is_new Flag if new file.
 * @return 0 on success, -1 on failure.
 */
static int kc_map_open(kc_map_t *m, const char *path, size_t size,
int is_new) {
#ifndef _WIN32
    int flags;

    memset(m, 0, sizeof(*m));
    m->fd = -1;
    flags = O_RDWR;
    if (is_new) {
        flags |= O_CREAT | O_EXCL;
    }
    m->fd = open(path, flags, 0644);
    if (m->fd == -1) {
        return -1;
    }
    if (kc_map_lock(m) != 0) {
        close(m->fd);
        m->fd = -1;
        return -1;
    }
    if (is_new && ftruncate(m->fd, (off_t)size) == -1) {
        close(m->fd);
        m->fd = -1;
        return -1;
    }
    m->ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, m->fd, 0);
    if (m->ptr == MAP_FAILED) {
        m->ptr = NULL;
        close(m->fd);
        m->fd = -1;
        return -1;
    }
    m->size = size;
    return 0;
#else
    DWORD size_high;
    DWORD size_low;
    DWORD creation;

    memset(m, 0, sizeof(*m));
    m->file = INVALID_HANDLE_VALUE;
    creation = is_new ? CREATE_NEW : OPEN_EXISTING;
    m->file = CreateFileA(path, GENERIC_READ | GENERIC_WRITE,
        0, NULL, creation, FILE_ATTRIBUTE_NORMAL, NULL);
    if (m->file == INVALID_HANDLE_VALUE) {
        return -1;
    }
    if (kc_map_lock(m) != 0) {
        CloseHandle(m->file);
        m->file = INVALID_HANDLE_VALUE;
        return -1;
    }
    size_high = (DWORD)(size >> 32);
    size_low = (DWORD)(size & 0xFFFFFFFF);
    m->map = CreateFileMappingA(m->file, NULL, PAGE_READWRITE,
        size_high, size_low, NULL);
    if (!m->map) {
        CloseHandle(m->file);
        m->file = INVALID_HANDLE_VALUE;
        return -1;
    }
    m->ptr = MapViewOfFile(m->map, FILE_MAP_ALL_ACCESS, 0, 0, size);
    if (!m->ptr) {
        CloseHandle(m->map);
        CloseHandle(m->file);
        m->map = NULL;
        m->file = INVALID_HANDLE_VALUE;
        return -1;
    }
    m->size = size;
    return 0;
#endif
}

/**
 * Synchronize a byte range and backing file.
 * @param m Map context.
 * @param offset Range offset.
 * @param length Range length.
 * @return 0 on success, -1 on failure.
 */
static int kc_map_sync_range(kc_map_t *m, uint64_t offset, uint64_t length) {
#ifndef _WIN32
    long page;
    uintptr_t base;
    uintptr_t start;
    uintptr_t end;
    size_t sync_len;

    if (!m || !m->ptr || length == 0 ||
        offset > (uint64_t)m->size ||
        length > (uint64_t)m->size - offset) {
        return -1;
    }
    page = sysconf(_SC_PAGESIZE);
    if (page <= 0) {
        return -1;
    }
    base = (uintptr_t)m->ptr;
    start = (base + (uintptr_t)offset) & ~((uintptr_t)page - 1U);
    end = base + (uintptr_t)offset + (uintptr_t)length;
    if (end < base + (uintptr_t)offset) {
        return -1;
    }
    sync_len = (size_t)(end - start);
    if (msync((void *)start, sync_len, MS_SYNC) == -1) {
        return -1;
    }
    return fsync(m->fd) == -1 ? -1 : 0;
#else
    (void)offset;
    (void)length;
    if (!m || !m->ptr || !FlushViewOfFile(m->ptr, m->size)) {
        return -1;
    }
    return FlushFileBuffers(m->file) ? 0 : -1;
#endif
}

/**
 * Sync the entire memory map and backing file.
 * @param m Map context.
 * @return 0 on success, -1 on failure.
 */
static int kc_map_sync(kc_map_t *m) {
    if (!m || !m->ptr || m->size == 0) {
        return -1;
    }
    return kc_map_sync_range(m, 0, (uint64_t)m->size);
}

/**
 * Release resources held by the map context.
 * @param m Map context.
 * @return None.
 */
static void kc_map_close(kc_map_t *m) {
#ifndef _WIN32
    if (m->ptr) {
        munmap(m->ptr, m->size);
    }
    if (m->fd != -1) {
        close(m->fd);
    }
    m->fd = -1;
#else
    if (m->ptr) {
        UnmapViewOfFile(m->ptr);
    }
    if (m->map) {
        CloseHandle(m->map);
    }
    if (m->file && m->file != INVALID_HANDLE_VALUE) {
        CloseHandle(m->file);
    }
    m->file = INVALID_HANDLE_VALUE;
    m->map = NULL;
#endif
    m->ptr = NULL;
    m->size = 0;
}

/**
 * Resize memory map to a new size.
 * @param m Map context.
 * @param new_size New size in bytes.
 * @return 0 on success, -1 on failure.
 */
static int kc_map_resize(kc_map_t *m, size_t new_size) {
#ifndef _WIN32
    void *new_ptr;

    if (!m || new_size == 0 || ftruncate(m->fd, (off_t)new_size) == -1) {
        return -1;
    }
    new_ptr = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED,
        m->fd, 0);
    if (new_ptr == MAP_FAILED) {
        return -1;
    }
    if (munmap(m->ptr, m->size) == -1) {
        munmap(new_ptr, new_size);
        return -1;
    }
    m->ptr = new_ptr;
    m->size = new_size;
    return 0;
#else
    DWORD size_high;
    DWORD size_low;
    HANDLE new_map;
    void *new_ptr;

    if (!m || new_size == 0) {
        return -1;
    }
    size_high = (DWORD)(new_size >> 32);
    size_low = (DWORD)(new_size & 0xFFFFFFFF);
    new_map = CreateFileMappingA(m->file, NULL, PAGE_READWRITE,
        size_high, size_low, NULL);
    if (!new_map) {
        return -1;
    }
    new_ptr = MapViewOfFile(new_map, FILE_MAP_ALL_ACCESS, 0, 0, new_size);
    if (!new_ptr) {
        CloseHandle(new_map);
        return -1;
    }
    UnmapViewOfFile(m->ptr);
    CloseHandle(m->map);
    m->map = new_map;
    m->ptr = new_ptr;
    m->size = new_size;
    return 0;
#endif
}

/**
 * Fsync the directory containing a path so a rename is persisted.
 * @param path File path whose containing directory must be synced.
 * @return 0 on success, -1 on failure.
 */
static int kc_dir_fsync(const char *path) {
#ifndef _WIN32
    const char *slash;
    char *dir_buf;
    size_t len;
    int dir_fd;
    int rc;

    slash = strrchr(path, '/');
    if (!slash) {
        dir_fd = open(".", O_RDONLY);
    } else if (slash == path) {
        dir_fd = open("/", O_RDONLY);
    } else {
        len = (size_t)(slash - path);
        dir_buf = malloc(len + 1);
        if (!dir_buf) {
            return -1;
        }
        memcpy(dir_buf, path, len);
        dir_buf[len] = '\0';
        dir_fd = open(dir_buf, O_RDONLY);
        free(dir_buf);
    }
    if (dir_fd == -1) {
        return -1;
    }
    rc = fsync(dir_fd);
    close(dir_fd);
    return rc == -1 ? -1 : 0;
#else
    (void)path;
    return 0;
#endif
}

/**
 * Compute byte offsets for index and heap regions.
 * @param capacity Store capacity.
 * @param index_size Output pointer for index size.
 * @param base_heap Output pointer for heap start.
 * @return Status code.
 */
static int kc_krow_layout(uint64_t capacity, size_t *index_size,
uint64_t *base_heap) {
    size_t idx;

    if (capacity == 0 || capacity > (uint64_t)(SIZE_MAX /
        sizeof(kc_krow_node_t))) {
        return KC_KROW_ERROR;
    }
    if (kc_size_mul_overflows((size_t)capacity, sizeof(kc_krow_node_t))) {
        return KC_KROW_ERROR;
    }
    idx = (size_t)capacity * sizeof(kc_krow_node_t);
    if (kc_size_add_overflows(sizeof(kc_krow_header_t), idx)) {
        return KC_KROW_ERROR;
    }
    *index_size = idx;
    *base_heap = (uint64_t)(sizeof(kc_krow_header_t) + idx);
    return KC_KROW_OK;
}

/**
 * Refresh derived pointers from the base map.
 * @param ctx Context pointer.
 * @return Status code.
 */
static int kc_krow_refresh_pointers(kc_krow_t *ctx) {
    size_t index_size;
    uint64_t base_heap;

    if (!ctx || !ctx->map.ptr) {
        return KC_KROW_ERROR;
    }
    ctx->header = (kc_krow_header_t *)ctx->map.ptr;
    if (kc_krow_layout(ctx->header->capacity, &index_size, &base_heap) !=
        KC_KROW_OK) {
        return KC_KROW_ERROR;
    }
    if (base_heap > (uint64_t)ctx->map.size) {
        return KC_KROW_ERROR;
    }
    ctx->index = (kc_krow_node_t *)((uint8_t *)ctx->map.ptr +
        sizeof(kc_krow_header_t));
    ctx->heap = (uint8_t *)ctx->map.ptr + base_heap;
    return KC_KROW_OK;
}

/**
 * Store a fresh header checksum.
 * @param ctx Context pointer.
 * @return None.
 */
static void kc_krow_update_header_checksum(kc_krow_t *ctx) {
    ctx->header->checksum = kc_header_checksum(ctx->header);
}

/**
 * Validate a mapped file header.
 * @param ctx Context pointer.
 * @return Status code.
 */
static int kc_krow_validate_header(kc_krow_t *ctx) {
    size_t index_size;
    uint64_t base_heap;

    if (!ctx || ctx->map.size < sizeof(kc_krow_header_t)) {
        return KC_KROW_ERROR;
    }
    if (ctx->header->magic != KC_KROW_MAGIC ||
        ctx->header->version != KC_KROW_VERSION ||
        ctx->header->header_size != sizeof(kc_krow_header_t) ||
        ctx->header->node_size != sizeof(kc_krow_node_t) ||
        ctx->header->capacity == 0 ||
        ctx->header->checksum != kc_header_checksum(ctx->header)) {
        return KC_KROW_ERROR;
    }
    if (kc_krow_layout(ctx->header->capacity, &index_size, &base_heap) !=
        KC_KROW_OK) {
        return KC_KROW_ERROR;
    }
    if (base_heap > (uint64_t)ctx->map.size ||
        ctx->header->data_tail < base_heap ||
        ctx->header->data_tail > (uint64_t)ctx->map.size ||
        ctx->header->count > ctx->header->capacity ||
        ctx->header->deleted > ctx->header->capacity ||
        ctx->header->count + ctx->header->deleted > ctx->header->capacity) {
        return KC_KROW_ERROR;
    }
    (void)index_size;
    return KC_KROW_OK;
}

/**
 * Insert a verified live node into a clean index.
 * @param ctx Context pointer.
 * @param node Node to insert.
 * @return Status code.
 */
static int kc_krow_reindex_node(kc_krow_t *ctx, const kc_krow_node_t *node) {
    uint64_t slot;
    uint64_t i;

    slot = kc_krow_hash_slot(node->key, ctx->header->capacity);
    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t idx = (slot + i) % ctx->header->capacity;

        if (ctx->index[idx].commit == 0) {
            ctx->index[idx] = *node;
            return KC_KROW_OK;
        }
    }
    return KC_KROW_ERROR;
}

/**
 * Rebuild the entire index from committed and checksummed entries.
 * @param ctx Context pointer.
 * @return Status code.
 */
static int kc_krow_recover(kc_krow_t *ctx) {
    kc_krow_node_t *old_index;
    size_t index_size;
    uint64_t base_heap;
    uint64_t live_count;
    uint64_t max_tail;
    uint64_t i;

    if (kc_krow_layout(ctx->header->capacity, &index_size, &base_heap) !=
        KC_KROW_OK) {
        return KC_KROW_ERROR;
    }
    old_index = malloc(index_size);
    if (!old_index) {
        return KC_KROW_ERROR;
    }
    memcpy(old_index, ctx->index, index_size);
    memset(ctx->index, 0, index_size);
    live_count = 0;
    max_tail = base_heap;

    for (i = 0; i < ctx->header->capacity; i++) {
        kc_krow_node_t node;
        uint64_t end;
        uint64_t rel;
        uint64_t sum;

        node = old_index[i];
        if (node.commit != KC_KROW_COMMIT || node.length == 0) {
            continue;
        }
        if (node.offset < base_heap ||
            kc_u64_add_overflows(node.offset, node.length)) {
            continue;
        }
        end = node.offset + node.length;
        if (end > (uint64_t)ctx->map.size) {
            continue;
        }
        rel = node.offset - base_heap;
        sum = kc_entry_checksum(node.key, node.offset, node.length,
            ctx->heap + rel);
        if (sum != node.checksum) {
            continue;
        }
        if (kc_krow_reindex_node(ctx, &node) != KC_KROW_OK) {
            free(old_index);
            return KC_KROW_ERROR;
        }
        if (end > max_tail) {
            max_tail = end;
        }
        live_count++;
    }
    free(old_index);
    ctx->header->count = live_count;
    ctx->header->deleted = 0;
    ctx->header->data_tail = max_tail;
    kc_krow_update_header_checksum(ctx);
    return kc_map_sync(&ctx->map) == 0 ? KC_KROW_OK : KC_KROW_ERROR;
}

/**
 * Grow the backing map to accommodate an incoming write.
 * @param ctx Context pointer.
 * @param needed Required new data_tail.
 * @return Status code.
 */
static int kc_krow_grow_for(kc_krow_t *ctx, uint64_t needed) {
    size_t new_size;

    if (needed <= (uint64_t)ctx->map.size) {
        return KC_KROW_OK;
    }
    new_size = ctx->map.size;
    while ((uint64_t)new_size < needed) {
        if (kc_size_mul_overflows(new_size, 2)) {
            return KC_KROW_ERROR;
        }
        new_size *= 2;
    }
    if (kc_map_resize(&ctx->map, new_size) != 0 ||
        kc_krow_refresh_pointers(ctx) != KC_KROW_OK) {
        return KC_KROW_ERROR;
    }
    return KC_KROW_OK;
}

/**
 * Initialize a newly created store map.
 * @param ctx Context pointer.
 * @param capacity Index capacity.
 * @return Status code.
 */
static int kc_krow_init_new(kc_krow_t *ctx, uint64_t capacity) {
    size_t index_size;
    uint64_t base_heap;

    if (kc_krow_layout(capacity, &index_size, &base_heap) != KC_KROW_OK) {
        return KC_KROW_ERROR;
    }
    memset(ctx->map.ptr, 0, ctx->map.size);
    ctx->header = (kc_krow_header_t *)ctx->map.ptr;
    ctx->header->magic = KC_KROW_MAGIC;
    ctx->header->version = KC_KROW_VERSION;
    ctx->header->header_size = sizeof(kc_krow_header_t);
    ctx->header->node_size = sizeof(kc_krow_node_t);
    ctx->header->capacity = capacity;
    ctx->header->count = 0;
    ctx->header->deleted = 0;
    ctx->header->data_tail = base_heap;
    kc_krow_update_header_checksum(ctx);
    if (kc_krow_refresh_pointers(ctx) != KC_KROW_OK) {
        return KC_KROW_ERROR;
    }
    return kc_map_sync(&ctx->map) == 0 ? KC_KROW_OK : KC_KROW_ERROR;
}

/**
 * Open or create a krow store.
 * @param path File path.
 * @param capacity Maximum number of index slots for new stores.
 * @return Context pointer or NULL on failure.
 */
kc_krow_t *kc_krow_open(const char *path, uint64_t capacity) {
    kc_krow_t *ctx;
    uint64_t existing_size;
    size_t index_size;
    uint64_t base_heap;
    size_t map_size;
    size_t path_len;
    int exists;
    int is_new;

    if (!path || path[0] == '\0') {
        return NULL;
    }
    exists = kc_stat_size(path, &existing_size) == 0 ? 1 : 0;
    if (!exists && capacity == 0) {
        return NULL;
    }
    if (exists && capacity > 0) {
        return NULL;
    }
    is_new = !exists;
    if (exists && existing_size == 0) {
        return NULL;
    }
    if (is_new && kc_krow_layout(capacity, &index_size, &base_heap) !=
        KC_KROW_OK) {
        return NULL;
    }
    if (is_new && kc_size_add_overflows((size_t)base_heap,
        (size_t)KC_KROW_INITIAL_HEAP)) {
        return NULL;
    }
    map_size = is_new ? (size_t)base_heap + (size_t)KC_KROW_INITIAL_HEAP :
        (size_t)existing_size;
    if (map_size == 0 || (uint64_t)map_size !=
        (is_new ? (uint64_t)map_size : existing_size)) {
        return NULL;
    }
    ctx = calloc(1, sizeof(kc_krow_t));
    if (!ctx) {
        return NULL;
    }
#ifndef _WIN32
    ctx->lock.fd = -1;
#else
    ctx->lock.file = INVALID_HANDLE_VALUE;
#endif
    if (kc_krow_thread_init(ctx) != KC_KROW_OK) {
        free(ctx);
        return NULL;
    }
    path_len = strlen(path);
    ctx->path = malloc(path_len + 1);
    if (!ctx->path) {
        kc_krow_thread_destroy(ctx);
        free(ctx);
        return NULL;
    }
    memcpy(ctx->path, path, path_len + 1);
    if (kc_lock_open(&ctx->lock, path) != 0) {
        kc_krow_thread_destroy(ctx);
        free(ctx->path);
        free(ctx);
        return NULL;
    }
    if (kc_map_open(&ctx->map, path, map_size, is_new) != 0) {
        kc_lock_close(&ctx->lock);
        kc_krow_thread_destroy(ctx);
        free(ctx->path);
        free(ctx);
        return NULL;
    }
    if (is_new) {
        if (kc_krow_init_new(ctx, capacity) != KC_KROW_OK) {
            kc_map_close(&ctx->map);
            kc_lock_close(&ctx->lock);
            kc_krow_thread_destroy(ctx);
            remove(path);
            free(ctx->path);
            free(ctx);
            return NULL;
        }
        return ctx;
    }
    ctx->header = (kc_krow_header_t *)ctx->map.ptr;
    if (kc_krow_validate_header(ctx) != KC_KROW_OK ||
        kc_krow_refresh_pointers(ctx) != KC_KROW_OK ||
        kc_krow_recover(ctx) != KC_KROW_OK) {
        kc_map_close(&ctx->map);
        kc_lock_close(&ctx->lock);
        kc_krow_thread_destroy(ctx);
        free(ctx->path);
        free(ctx);
        return NULL;
    }
    return ctx;
}

/**
 * Close the krow store.
 * @param ctx Context pointer.
 * @return None.
 */
void kc_krow_close(kc_krow_t *ctx) {
    if (!ctx) {
        return;
    }
    if (kc_krow_enter(ctx) != KC_KROW_OK) {
        return;
    }
    kc_map_close(&ctx->map);
    kc_lock_close(&ctx->lock);
    kc_krow_leave(ctx);
    kc_krow_thread_destroy(ctx);
    free(ctx->path);
    free(ctx);
}

/**
 * Add a record to the store.
 * @param ctx Context pointer.
 * @param key Record key.
 * @param value Value data.
 * @param size Value size in bytes.
 * @return Status code.
 */
int kc_krow_set(kc_krow_t *ctx, uint64_t key, const void *value,
size_t size) {
    kc_krow_node_t node;
    uint64_t slot;
    uint64_t i;
    uint64_t target;
    uint64_t base_heap;
    uint64_t relative_tail;
    uint64_t new_tail;
    size_t index_size;
    int reused_deleted;
    int rc;

    if (kc_krow_enter(ctx) != KC_KROW_OK) {
        return KC_KROW_ERROR;
    }
    rc = KC_KROW_ERROR;
    if (!ctx || !ctx->header || !value || size == 0 ||
        (uint64_t)size != (size_t)size ||
        ctx->header->capacity == 0 ||
        ctx->header->count >= ctx->header->capacity) {
        goto out;
    }
    if (kc_krow_layout(ctx->header->capacity, &index_size, &base_heap) !=
        KC_KROW_OK ||
        kc_u64_add_overflows(ctx->header->data_tail, (uint64_t)size)) {
        goto out;
    }
    new_tail = ctx->header->data_tail + (uint64_t)size;
    if (kc_krow_grow_for(ctx, new_tail) != KC_KROW_OK) {
        goto out;
    }
    slot = kc_krow_hash_slot(key, ctx->header->capacity);
    target = ctx->header->capacity;
    reused_deleted = 0;
    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t idx = (slot + i) % ctx->header->capacity;

        if (ctx->index[idx].commit == 0 ||
            ctx->index[idx].commit == KC_KROW_TOMBSTONE) {
            target = idx;
            if (ctx->index[idx].commit == KC_KROW_TOMBSTONE) {
                reused_deleted = 1;
            }
            break;
        }
    }
    if (target == ctx->header->capacity) {
        goto out;
    }
    relative_tail = ctx->header->data_tail - base_heap;
    memcpy(ctx->heap + relative_tail, value, size);
    if (kc_map_sync_range(&ctx->map, ctx->header->data_tail,
        (uint64_t)size) != 0) {
        goto out;
    }
    node.key = key;
    node.offset = ctx->header->data_tail;
    node.length = (uint64_t)size;
    node.checksum = kc_entry_checksum(key, node.offset, node.length, value);
    node.commit = KC_KROW_COMMIT;
    ctx->index[target] = node;
    if (kc_map_sync_range(&ctx->map,
        sizeof(kc_krow_header_t) + target * sizeof(kc_krow_node_t),
        sizeof(kc_krow_node_t)) != 0) {
        goto out;
    }
    ctx->header->count++;
    if (reused_deleted && ctx->header->deleted > 0) {
        ctx->header->deleted--;
    }
    ctx->header->data_tail = new_tail;
    kc_krow_update_header_checksum(ctx);
    if (kc_map_sync_range(&ctx->map, 0, sizeof(kc_krow_header_t)) != 0) {
        goto out;
    }
    rc = KC_KROW_OK;
out:
    kc_krow_leave(ctx);
    return rc;
}

/**
 * Retrieve records by key.
 * @param ctx Context pointer.
 * @param key Record key.
 * @param cb Result callback.
 * @param arg User argument.
 * @return Status code.
 */
int kc_krow_get(kc_krow_t *ctx, uint64_t key, kc_krow_cb cb, void *arg) {
    uint64_t slot;
    uint64_t i;
    size_t index_size;
    uint64_t base_heap;
    int rc;

    if (kc_krow_enter(ctx) != KC_KROW_OK) {
        return KC_KROW_ERROR;
    }
    rc = KC_KROW_ERROR;
    if (!ctx || !ctx->header || !cb ||
        ctx->header->capacity == 0 ||
        kc_krow_layout(ctx->header->capacity, &index_size, &base_heap) !=
        KC_KROW_OK) {
        goto out;
    }
    slot = kc_krow_hash_slot(key, ctx->header->capacity);
    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t idx = (slot + i) % ctx->header->capacity;
        uint64_t rel;

        if (ctx->index[idx].commit == 0) {
            break;
        }
        if (ctx->index[idx].commit != KC_KROW_COMMIT ||
            ctx->index[idx].key != key) {
            continue;
        }
        rel = ctx->index[idx].offset - base_heap;
        if (cb(key, ctx->heap + rel, (size_t)ctx->index[idx].length,
            arg) != 0) {
            break;
        }
    }
    (void)index_size;
    rc = KC_KROW_OK;
out:
    kc_krow_leave(ctx);
    return rc;
}

/**
 * Delete records by key.
 * @param ctx Context pointer.
 * @param key Record key.
 * @return Status code.
 */
int kc_krow_del(kc_krow_t *ctx, uint64_t key) {
    uint64_t slot;
    uint64_t i;
    uint64_t removed;
    int rc;

    if (kc_krow_enter(ctx) != KC_KROW_OK) {
        return KC_KROW_ERROR;
    }
    rc = KC_KROW_ERROR;
    if (!ctx || !ctx->header || ctx->header->capacity == 0) {
        goto out;
    }
    slot = kc_krow_hash_slot(key, ctx->header->capacity);
    removed = 0;
    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t idx = (slot + i) % ctx->header->capacity;

        if (ctx->index[idx].commit == 0) {
            break;
        }
        if (ctx->index[idx].commit != KC_KROW_COMMIT ||
            ctx->index[idx].key != key) {
            continue;
        }
        ctx->index[idx].commit = KC_KROW_TOMBSTONE;
        ctx->index[idx].key = 0;
        if (kc_map_sync_range(&ctx->map,
            sizeof(kc_krow_header_t) + idx * sizeof(kc_krow_node_t),
            sizeof(kc_krow_node_t)) != 0) {
            goto out;
        }
        removed++;
    }
    if (removed > 0) {
        ctx->header->count -= removed;
        ctx->header->deleted += removed;
        kc_krow_update_header_checksum(ctx);
        if (kc_map_sync_range(&ctx->map, 0, sizeof(kc_krow_header_t)) != 0) {
            goto out;
        }
    }
    rc = KC_KROW_OK;
out:
    kc_krow_leave(ctx);
    return rc;
}

/**
 * Build a fresh store file containing only the live entries of ctx.
 * @param ctx Source context.
 * @param tmp_path Destination path for the new file.
 * @return Status code.
 */
static int kc_krow_build_snapshot(kc_krow_t *ctx, const char *tmp_path) {
    kc_krow_t *tmp;
    size_t index_size;
    uint64_t base_heap;
    uint64_t i;

    tmp = kc_krow_open(tmp_path, ctx->header->capacity);
    if (!tmp) {
        return KC_KROW_ERROR;
    }
    if (kc_krow_layout(ctx->header->capacity, &index_size, &base_heap) !=
        KC_KROW_OK) {
        kc_krow_close(tmp);
        return KC_KROW_ERROR;
    }
    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t rel;

        if (ctx->index[i].commit != KC_KROW_COMMIT) {
            continue;
        }
        rel = ctx->index[i].offset - base_heap;
        if (kc_krow_set(tmp, ctx->index[i].key, ctx->heap + rel,
            (size_t)ctx->index[i].length) != KC_KROW_OK) {
            kc_krow_close(tmp);
            return KC_KROW_ERROR;
        }
    }
    if (kc_krow_sync(tmp) != KC_KROW_OK) {
        kc_krow_close(tmp);
        return KC_KROW_ERROR;
    }
    kc_krow_close(tmp);
    (void)index_size;
    return KC_KROW_OK;
}

/**
 * Replace the current backing file with a freshly built snapshot.
 * @param ctx Context pointer.
 * @param tmp_path Path of the new snapshot file.
 * @return Status code.
 */
static int kc_krow_swap_backing(kc_krow_t *ctx, const char *tmp_path) {
    uint64_t new_size;

#ifndef _WIN32
    if (rename(tmp_path, ctx->path) != 0 || kc_dir_fsync(ctx->path) != 0) {
        return KC_KROW_ERROR;
    }
#else
    if (!MoveFileExA(tmp_path, ctx->path,
        MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        return KC_KROW_ERROR;
    }
#endif
    kc_map_close(&ctx->map);
    if (kc_stat_size(ctx->path, &new_size) == -1 ||
        new_size == 0 ||
        (size_t)new_size != new_size ||
        kc_map_open(&ctx->map, ctx->path, (size_t)new_size, 0) != 0) {
        return KC_KROW_ERROR;
    }
    ctx->header = (kc_krow_header_t *)ctx->map.ptr;
    if (kc_krow_validate_header(ctx) != KC_KROW_OK ||
        kc_krow_refresh_pointers(ctx) != KC_KROW_OK) {
        return KC_KROW_ERROR;
    }
    return KC_KROW_OK;
}

/**
 * Prune tombstones and defragment the heap atomically.
 * @param ctx Context pointer.
 * @return Status code.
 */
int kc_krow_prune(kc_krow_t *ctx) {
    char *tmp_path;
    size_t path_len;
    size_t suffix_len;
    int rc;

    if (kc_krow_enter(ctx) != KC_KROW_OK) {
        return KC_KROW_ERROR;
    }
    rc = KC_KROW_ERROR;
    if (!ctx || !ctx->path || !ctx->header ||
        ctx->header->capacity == 0) {
        goto out;
    }
    path_len = strlen(ctx->path);
    suffix_len = strlen(KC_KROW_TMP_SUFFIX);
    if (kc_size_add_overflows(path_len, suffix_len + 1)) {
        goto out;
    }
    tmp_path = malloc(path_len + suffix_len + 1);
    if (!tmp_path) {
        goto out;
    }
    memcpy(tmp_path, ctx->path, path_len);
    memcpy(tmp_path + path_len, KC_KROW_TMP_SUFFIX, suffix_len + 1);
#ifndef _WIN32
    unlink(tmp_path);
#else
    DeleteFileA(tmp_path);
#endif
    rc = kc_krow_build_snapshot(ctx, tmp_path);
    if (rc != KC_KROW_OK) {
#ifndef _WIN32
        unlink(tmp_path);
#else
        DeleteFileA(tmp_path);
#endif
        free(tmp_path);
        rc = KC_KROW_ERROR;
        goto out;
    }
    rc = kc_krow_swap_backing(ctx, tmp_path);
    free(tmp_path);
out:
    kc_krow_leave(ctx);
    return rc;
}

/**
 * Synchronize memory map to disk.
 * @param ctx Context pointer.
 * @return Status code.
 */
int kc_krow_sync(kc_krow_t *ctx) {
    int rc;

    if (kc_krow_enter(ctx) != KC_KROW_OK) {
        return KC_KROW_ERROR;
    }
    rc = kc_map_sync(&ctx->map) == 0 ? KC_KROW_OK : KC_KROW_ERROR;
    kc_krow_leave(ctx);
    return rc;
}
