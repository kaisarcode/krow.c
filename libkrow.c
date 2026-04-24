/**
 * libkrow.c - Summary of the functionality
 * Summary: Core implementation of the krow storage engine.
 *
 * Author:  KaisarCode
 * Website: https://kaisarcode.com
 * License: https://www.gnu.org/licenses/gpl-3.0.html
 */

#ifndef _WIN32
#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#else
#include <windows.h>
#endif

#include "krow.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#define KC_KROW_FIBO 11400714819323198485ULL
#define KC_KROW_INITIAL_HEAP (1024ULL * 1024ULL * 64ULL)
#define KC_KROW_TOMBSTONE ((uint64_t)-1)
#define KC_KROW_TMP_SUFFIX ".tmp"

#if defined(__GNUC__) || defined(__clang__)
#define KC_KROW_STORE_RELEASE(ptr, val) \
    __atomic_store_n((ptr), (val), __ATOMIC_RELEASE)
#elif defined(_MSC_VER)
#include <intrin.h>
#define KC_KROW_STORE_RELEASE(ptr, val) \
    _InterlockedExchange64((volatile __int64 *)(ptr), (__int64)(val))
#else
#define KC_KROW_STORE_RELEASE(ptr, val) (*(ptr) = (val))
#endif

typedef struct {
    uint64_t key;
    uint64_t offset;
    uint64_t length;
} kc_krow_node_t;

typedef struct {
    uint32_t magic;
    uint32_t padding;
    uint64_t capacity;
    uint64_t count;
    uint64_t data_tail;
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

struct kc_krow {
    kc_map_t map;
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
    if (b > SIZE_MAX - a) {
        return 1;
    }
    return 0;
}

/**
 * Check if two uint64_t values would overflow on addition.
 * @param a First operand.
 * @param b Second operand.
 * @return 1 on overflow, 0 otherwise.
 */
static int kc_u64_add_overflows(uint64_t a, uint64_t b) {
    if (b > UINT64_MAX - a) {
        return 1;
    }
    return 0;
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
    if (a > SIZE_MAX / b) {
        return 1;
    }
    return 0;
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
 * Acquire an exclusive advisory lock on the backing file.
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

    if (fcntl(m->fd, F_SETLK, &lk) == -1) {
        return -1;
    }
    return 0;
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
static int kc_map_open(kc_map_t *m, const char *path, size_t size, int is_new) {
#ifndef _WIN32
    m->ptr = NULL;
    m->size = 0;
    m->fd = open(path, O_RDWR | (is_new ? O_CREAT : 0), 0644);
    if (m->fd == -1) {
        return -1;
    }

    if (kc_map_lock(m) != 0) {
        close(m->fd);
        m->fd = -1;
        return -1;
    }

    if (is_new && size > 0) {
        if (ftruncate(m->fd, (off_t)size) == -1) {
            close(m->fd);
            m->fd = -1;
            return -1;
        }
    }

    m->size = size;
    m->ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, m->fd, 0);
    if (m->ptr == MAP_FAILED) {
        m->ptr = NULL;
        close(m->fd);
        m->fd = -1;
        return -1;
    }

    return 0;
#else
    DWORD size_high;
    DWORD size_low;

    m->ptr = NULL;
    m->map = NULL;
    m->file = INVALID_HANDLE_VALUE;
    m->size = 0;

    m->file = CreateFileA(path, GENERIC_READ | GENERIC_WRITE,
        FILE_SHARE_READ, NULL,
        is_new ? OPEN_ALWAYS : OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
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
 * Sync memory map to disk.
 * @param m Map context.
 * @return 0 on success, -1 on failure.
 */
static int kc_map_sync(kc_map_t *m) {
#ifndef _WIN32
    if (msync(m->ptr, m->size, MS_SYNC) == -1) {
        return -1;
    }
    return 0;
#else
    if (!FlushViewOfFile(m->ptr, m->size)) {
        return -1;
    }
    return 0;
#endif
}

/**
 * Release resources held by the map context.
 * @param m Map context.
 * @return None.
 */
static void kc_map_close(kc_map_t *m) {
#ifndef _WIN32
    if (m->ptr && m->ptr != MAP_FAILED) {
        munmap(m->ptr, m->size);
    }
    if (m->fd != -1) {
        close(m->fd);
    }
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
#endif
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

    if (ftruncate(m->fd, (off_t)new_size) == -1) {
        return -1;
    }

    new_ptr = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED,
        m->fd, 0);
    if (new_ptr == MAP_FAILED) {
        return -1;
    }

    munmap(m->ptr, m->size);
    m->ptr = new_ptr;
    m->size = new_size;

    return 0;
#else
    DWORD size_high;
    DWORD size_low;
    HANDLE new_map;
    void *new_ptr;

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
 * Query the size of an existing file.
 * @param path File path.
 * @param size_out Output pointer for file size.
 * @return 0 if file exists and size is readable, -1 otherwise.
 */
static int kc_stat_size(const char *path, uint64_t *size_out) {
#ifndef _WIN32
    struct stat st;

    if (stat(path, &st) == -1) {
        return -1;
    }
    *size_out = (uint64_t)st.st_size;
    return 0;
#else
    WIN32_FILE_ATTRIBUTE_DATA st;

    if (!GetFileAttributesExA(path, GetFileExInfoStandard, &st)) {
        return -1;
    }
    *size_out = ((uint64_t)st.nFileSizeHigh << 32) | st.nFileSizeLow;
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
    if (fsync(dir_fd) == -1) {
        close(dir_fd);
        return -1;
    }
    close(dir_fd);
    return 0;
#else
    (void)path;
    return 0;
#endif
}

/**
 * Refresh derived pointers from the base map.
 * @param ctx Context pointer.
 * @return None.
 */
static void kc_krow_refresh_pointers(kc_krow_t *ctx) {
    size_t header_size;
    size_t index_size;

    ctx->header = (kc_krow_header_t *)ctx->map.ptr;
    header_size = sizeof(kc_krow_header_t);
    index_size = (size_t)ctx->header->capacity * sizeof(kc_krow_node_t);

    ctx->index = (kc_krow_node_t *)((uint8_t *)ctx->header + header_size);
    ctx->heap = (uint8_t *)ctx->map.ptr + header_size + index_size;
}

/**
 * Repair an index after an unclean shutdown.
 * @param ctx Context pointer.
 * @return None.
 */
static void kc_krow_recover(kc_krow_t *ctx) {
    size_t header_size;
    size_t index_size;
    uint64_t base_heap_offset;
    uint64_t live_count;
    uint64_t i;

    header_size = sizeof(kc_krow_header_t);
    index_size = (size_t)ctx->header->capacity * sizeof(kc_krow_node_t);
    base_heap_offset = header_size + index_size;

    if (ctx->header->data_tail < base_heap_offset ||
        ctx->header->data_tail > ctx->map.size) {
        ctx->header->data_tail = base_heap_offset;
    }

    live_count = 0;
    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t len = ctx->index[i].length;
        uint64_t off = ctx->index[i].offset;

        if (len == 0 || len == KC_KROW_TOMBSTONE) {
            continue;
        }

        if (off < base_heap_offset ||
            kc_u64_add_overflows(off, len) ||
            off + len > ctx->header->data_tail) {
            ctx->index[i].key = 0;
            ctx->index[i].offset = 0;
            ctx->index[i].length = 0;
            continue;
        }

        live_count++;
    }

    if (ctx->header->count != live_count) {
        ctx->header->count = live_count;
    }
}

/**
 * Grow the backing map to accommodate an incoming write.
 * @param ctx Context pointer.
 * @param needed Required new data_tail.
 * @return Status code.
 */
static int kc_krow_grow_for(kc_krow_t *ctx, uint64_t needed) {
    size_t new_size;

    if (needed <= ctx->map.size) {
        return KC_KROW_OK;
    }

    new_size = ctx->map.size;
    if (new_size == 0) {
        return KC_KROW_ERROR;
    }

    while ((uint64_t)new_size < needed) {
        if (kc_size_mul_overflows(new_size, 2)) {
            return KC_KROW_ERROR;
        }
        new_size *= 2;
    }

    if (kc_map_resize(&ctx->map, new_size) != 0) {
        return KC_KROW_ERROR;
    }

    kc_krow_refresh_pointers(ctx);
    return KC_KROW_OK;
}

/**
 * Open or create a krow store.
 * @param path File path.
 * @param capacity Maximum number of index slots for new stores.
 * @return Context pointer or NULL on failure.
 */
kc_krow_t *kc_krow_open(const char *path, uint64_t capacity) {
    kc_krow_t *ctx;
    size_t header_size;
    size_t index_size;
    int is_new;
    size_t map_size;
    uint64_t existing_size;
    size_t path_len;

    ctx = calloc(1, sizeof(kc_krow_t));
    if (!ctx) {
        return NULL;
    }

    path_len = strlen(path);
    ctx->path = malloc(path_len + 1);
    if (!ctx->path) {
        free(ctx);
        return NULL;
    }
    memcpy(ctx->path, path, path_len + 1);

    if (kc_stat_size(path, &existing_size) == -1) {
        is_new = 1;
        map_size = 0;
    } else {
        is_new = (existing_size == 0);
        map_size = (size_t)existing_size;
    }

    header_size = sizeof(kc_krow_header_t);
    index_size = 0;

    if (is_new) {
        if (capacity == 0) {
            free(ctx->path);
            free(ctx);
            return NULL;
        }
        if (kc_size_mul_overflows((size_t)capacity,
            sizeof(kc_krow_node_t))) {
            free(ctx->path);
            free(ctx);
            return NULL;
        }
        index_size = (size_t)capacity * sizeof(kc_krow_node_t);
        if (kc_size_add_overflows(header_size, index_size)) {
            free(ctx->path);
            free(ctx);
            return NULL;
        }
        if (kc_size_add_overflows(header_size + index_size,
            (size_t)KC_KROW_INITIAL_HEAP)) {
            free(ctx->path);
            free(ctx);
            return NULL;
        }
        map_size = header_size + index_size + (size_t)KC_KROW_INITIAL_HEAP;
    }

    if (kc_map_open(&ctx->map, path, map_size, is_new) != 0) {
        free(ctx->path);
        free(ctx);
        return NULL;
    }

    ctx->header = (kc_krow_header_t *)ctx->map.ptr;

    if (is_new) {
        ctx->header->magic = KC_KROW_MAGIC;
        ctx->header->padding = 0;
        ctx->header->capacity = capacity;
        ctx->header->count = 0;
        ctx->header->data_tail = header_size + index_size;
    } else if (ctx->header->magic != KC_KROW_MAGIC) {
        kc_map_close(&ctx->map);
        free(ctx->path);
        free(ctx);
        return NULL;
    }

    kc_krow_refresh_pointers(ctx);

    if (!is_new) {
        kc_krow_recover(ctx);
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

    kc_map_close(&ctx->map);
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
int kc_krow_set(kc_krow_t *ctx, uint64_t key, const void *value, size_t size) {
    uint64_t slot;
    uint64_t i;
    int64_t first_empty_slot;
    int64_t first_tombstone_slot;
    int64_t target_idx;
    size_t header_size;
    size_t index_size;
    uint64_t absolute_offset;
    uint64_t relative_tail;
    uint64_t new_tail;

    if (!ctx || !value || size == 0) {
        return KC_KROW_ERROR;
    }

    if (ctx->header->capacity == 0) {
        return KC_KROW_ERROR;
    }

    if (ctx->header->count >= ctx->header->capacity) {
        return KC_KROW_ERROR;
    }

    if (kc_u64_add_overflows(ctx->header->data_tail, (uint64_t)size)) {
        return KC_KROW_ERROR;
    }
    new_tail = ctx->header->data_tail + (uint64_t)size;

    if (kc_krow_grow_for(ctx, new_tail) != KC_KROW_OK) {
        return KC_KROW_ERROR;
    }

    slot = kc_krow_hash_slot(key, ctx->header->capacity);
    first_empty_slot = -1;
    first_tombstone_slot = -1;

    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t idx = (slot + i) % ctx->header->capacity;

        if (ctx->index[idx].length == 0) {
            if (first_empty_slot == -1) {
                first_empty_slot = (int64_t)idx;
            }
            break;
        }

        if (ctx->index[idx].length == KC_KROW_TOMBSTONE) {
            if (first_tombstone_slot == -1) {
                first_tombstone_slot = (int64_t)idx;
            }
        }
    }

    if (first_tombstone_slot != -1) {
        target_idx = first_tombstone_slot;
    } else if (first_empty_slot != -1) {
        target_idx = first_empty_slot;
    } else {
        return KC_KROW_ERROR;
    }

    header_size = sizeof(kc_krow_header_t);
    index_size = (size_t)ctx->header->capacity * sizeof(kc_krow_node_t);
    absolute_offset = ctx->header->data_tail;
    relative_tail = absolute_offset - (header_size + index_size);

    memcpy(ctx->heap + relative_tail, value, size);
    KC_KROW_STORE_RELEASE(&ctx->header->data_tail, new_tail);

    ctx->index[target_idx].key = key;
    ctx->index[target_idx].offset = absolute_offset;
    KC_KROW_STORE_RELEASE(&ctx->index[target_idx].length, (uint64_t)size);

    ctx->header->count++;

    return KC_KROW_OK;
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
    size_t header_size;
    size_t index_size;
    uint64_t base_heap_offset;
    uint64_t relative_offset;

    if (!ctx || !cb) {
        return KC_KROW_ERROR;
    }

    if (ctx->header->capacity == 0) {
        return KC_KROW_ERROR;
    }

    slot = kc_krow_hash_slot(key, ctx->header->capacity);
    header_size = sizeof(kc_krow_header_t);
    index_size = (size_t)ctx->header->capacity * sizeof(kc_krow_node_t);
    base_heap_offset = header_size + index_size;

    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t idx = (slot + i) % ctx->header->capacity;

        if (ctx->index[idx].length == 0) {
            break;
        }

        if (ctx->index[idx].length == KC_KROW_TOMBSTONE) {
            continue;
        }

        if (ctx->index[idx].key != key) {
            continue;
        }

        if (ctx->index[idx].offset < base_heap_offset) {
            continue;
        }

        if (kc_u64_add_overflows(ctx->index[idx].offset,
            ctx->index[idx].length)) {
            continue;
        }

        if (ctx->index[idx].offset + ctx->index[idx].length >
            ctx->header->data_tail) {
            continue;
        }

        relative_offset = ctx->index[idx].offset - base_heap_offset;
        if (cb(key, ctx->heap + relative_offset,
            ctx->index[idx].length, arg) != 0) {
            break;
        }
    }

    return KC_KROW_OK;
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

    if (!ctx) {
        return KC_KROW_ERROR;
    }

    if (ctx->header->capacity == 0) {
        return KC_KROW_ERROR;
    }

    slot = kc_krow_hash_slot(key, ctx->header->capacity);
    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t idx = (slot + i) % ctx->header->capacity;

        if (ctx->index[idx].length == 0) {
            break;
        }

        if (ctx->index[idx].length == KC_KROW_TOMBSTONE) {
            continue;
        }

        if (ctx->index[idx].key != key) {
            continue;
        }

        KC_KROW_STORE_RELEASE(&ctx->index[idx].length, KC_KROW_TOMBSTONE);
        ctx->index[idx].key = 0;
        if (ctx->header->count > 0) {
            ctx->header->count--;
        }
    }

    return KC_KROW_OK;
}

/**
 * Build a fresh store file containing only the live entries of ctx.
 * @param ctx Source context.
 * @param tmp_path Destination path for the new file.
 * @return Status code.
 */
static int kc_krow_build_snapshot(kc_krow_t *ctx, const char *tmp_path) {
    kc_krow_t *tmp;
    size_t header_size;
    size_t index_size;
    uint64_t base_heap_offset;
    uint64_t i;

    tmp = kc_krow_open(tmp_path, ctx->header->capacity);
    if (!tmp) {
        return KC_KROW_ERROR;
    }

    header_size = sizeof(kc_krow_header_t);
    index_size = (size_t)ctx->header->capacity * sizeof(kc_krow_node_t);
    base_heap_offset = header_size + index_size;

    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t len = ctx->index[i].length;
        uint64_t off = ctx->index[i].offset;
        uint64_t rel;

        if (len == 0 || len == KC_KROW_TOMBSTONE) {
            continue;
        }
        if (off < base_heap_offset) {
            continue;
        }
        rel = off - base_heap_offset;
        if (kc_krow_set(tmp, ctx->index[i].key, ctx->heap + rel, (size_t)len)
            != KC_KROW_OK) {
            kc_krow_close(tmp);
            return KC_KROW_ERROR;
        }
    }

    if (kc_krow_sync(tmp) != KC_KROW_OK) {
        kc_krow_close(tmp);
        return KC_KROW_ERROR;
    }

    kc_krow_close(tmp);
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
    if (rename(tmp_path, ctx->path) != 0) {
        return KC_KROW_ERROR;
    }
    kc_dir_fsync(ctx->path);
#else
    if (!MoveFileExA(tmp_path, ctx->path,
        MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        return KC_KROW_ERROR;
    }
#endif

    kc_map_close(&ctx->map);
    memset(&ctx->map, 0, sizeof(ctx->map));
#ifndef _WIN32
    ctx->map.fd = -1;
#else
    ctx->map.file = INVALID_HANDLE_VALUE;
#endif

    if (kc_stat_size(ctx->path, &new_size) == -1) {
        return KC_KROW_ERROR;
    }

    if (kc_map_open(&ctx->map, ctx->path, (size_t)new_size, 0) != 0) {
        return KC_KROW_ERROR;
    }

    ctx->header = (kc_krow_header_t *)ctx->map.ptr;
    if (ctx->header->magic != KC_KROW_MAGIC) {
        return KC_KROW_ERROR;
    }

    kc_krow_refresh_pointers(ctx);
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

    if (!ctx || ctx->header->capacity == 0) {
        return KC_KROW_ERROR;
    }

    path_len = strlen(ctx->path);
    suffix_len = strlen(KC_KROW_TMP_SUFFIX);
    if (kc_size_add_overflows(path_len, suffix_len + 1)) {
        return KC_KROW_ERROR;
    }

    tmp_path = malloc(path_len + suffix_len + 1);
    if (!tmp_path) {
        return KC_KROW_ERROR;
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
        return KC_KROW_ERROR;
    }

    rc = kc_krow_swap_backing(ctx, tmp_path);
    free(tmp_path);
    return rc;
}

/**
 * Synchronize memory map to disk.
 * @param ctx Context pointer.
 * @return Status code.
 */
int kc_krow_sync(kc_krow_t *ctx) {
    if (!ctx) {
        return KC_KROW_ERROR;
    }
    return kc_map_sync(&ctx->map);
}
