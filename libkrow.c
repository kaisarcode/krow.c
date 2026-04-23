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
};

/**
 * Open memory map abstraction.
 * @param m Map context.
 * @param path File path.
 * @param size Requested size.
 * @param is_new Flag if new file.
 * @return Status code.
 */
static int kc_map_open(kc_map_t *m, const char *path, size_t size, int is_new) {
#ifndef _WIN32
    m->fd = open(path, O_RDWR | (is_new ? O_CREAT : 0), 0644);
    if (m->fd == -1) {
        return -1;
    }

    if (is_new && size > 0) {
        if (ftruncate(m->fd, size) == -1) {
            return -1;
        }
    }

    m->size = size;
    m->ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, m->fd, 0);
    if (m->ptr == MAP_FAILED) {
        return -1;
    }

    return 0;
#else
    DWORD size_high;
    DWORD size_low;

    m->file = CreateFileA(path, GENERIC_READ | GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
        is_new ? OPEN_ALWAYS : OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (m->file == INVALID_HANDLE_VALUE) {
        return -1;
    }

    size_high = (DWORD)(size >> 32);
    size_low = (DWORD)(size & 0xFFFFFFFF);

    m->map = CreateFileMappingA(m->file, NULL, PAGE_READWRITE,
        size_high, size_low, NULL);
    if (!m->map) {
        CloseHandle(m->file);
        return -1;
    }

    m->ptr = MapViewOfFile(m->map, FILE_MAP_ALL_ACCESS, 0, 0, size);
    if (!m->ptr) {
        CloseHandle(m->map);
        CloseHandle(m->file);
        return -1;
    }

    m->size = size;
    return 0;
#endif
}

/**
 * Sync memory map.
 * @param m Map context.
 * @return Status code.
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
 * Close memory map.
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
 * Resize memory map.
 * @param m Map context.
 * @param new_size New size.
 * @return Status code.
 */
static int kc_map_resize(kc_map_t *m, size_t new_size) {
#ifndef _WIN32
    void *new_ptr;

    if (ftruncate(m->fd, new_size) == -1) {
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
 * Refresh derived pointers from the base map.
 * @param ctx Context pointer.
 * @return None.
 */
static void kc_krow_refresh_pointers(kc_krow_t *ctx) {
    size_t header_size;
    size_t index_size;

    ctx->header = (kc_krow_header_t *)ctx->map.ptr;
    header_size = sizeof(kc_krow_header_t);
    index_size = ctx->header->capacity * sizeof(kc_krow_node_t);

    ctx->index = (kc_krow_node_t *)((uint8_t *)ctx->header + header_size);
    ctx->heap = (uint8_t *)ctx->map.ptr + header_size + index_size;
}

/**
 * Initialize a new krow store.
 * @param path File path.
 * @param capacity Maximum number of index slots.
 * @return Context pointer.
 */
kc_krow_t *kc_krow_open(const char *path, uint64_t capacity) {
    kc_krow_t *ctx;
    size_t header_size;
    size_t index_size;
    size_t initial_heap;
    int is_new;
    size_t map_size;

    ctx = calloc(1, sizeof(kc_krow_t));
    if (!ctx) {
        return NULL;
    }

#ifndef _WIN32
    struct stat st;
    if (stat(path, &st) == -1) {
        is_new = 1;
        map_size = 0;
    } else {
        is_new = (st.st_size == 0);
        map_size = st.st_size;
    }
#else
    WIN32_FILE_ATTRIBUTE_DATA st;
    if (!GetFileAttributesExA(path, GetFileExInfoStandard, &st)) {
        is_new = 1;
        map_size = 0;
    } else {
        uint64_t sz = ((uint64_t)st.nFileSizeHigh << 32) | st.nFileSizeLow;
        is_new = (sz == 0);
        map_size = sz;
    }
#endif

    header_size = sizeof(kc_krow_header_t);
    initial_heap = 1024 * 1024 * 64;

    if (is_new) {
        index_size = capacity * sizeof(kc_krow_node_t);
        map_size = header_size + index_size + initial_heap;
    }

    if (kc_map_open(&ctx->map, path, map_size, is_new) != 0) {
        free(ctx);
        return NULL;
    }

    ctx->header = (kc_krow_header_t *)ctx->map.ptr;

    if (is_new) {
        ctx->header->magic = KC_KROW_MAGIC;
        ctx->header->capacity = capacity;
        ctx->header->count = 0;
        ctx->header->data_tail = header_size + index_size;
    } else if (ctx->header->magic != KC_KROW_MAGIC) {
        kc_map_close(&ctx->map);
        free(ctx);
        return NULL;
    }

    kc_krow_refresh_pointers(ctx);

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
    free(ctx);
}

/**
 * Add a record to the store.
 * @param ctx Context pointer.
 * @param key Record key.
 * @param value Value data.
 * @param size Value size.
 * @return Status code.
 */
int kc_krow_set(kc_krow_t *ctx, uint64_t key, const void *value, size_t size) {
    uint64_t slot;
    uint64_t i;
    int64_t first_empty_slot;
    int64_t first_tombstone_slot;
    int64_t target_idx;
    int is_empty_insert;
    size_t header_size;
    size_t index_size;
    uint64_t relative_tail;

    if (ctx->header->count >= ctx->header->capacity) {
        return KC_KROW_ERROR;
    }

    if (size > ctx->map.size) {
        return KC_KROW_ERROR;
    }

    if (ctx->header->data_tail + size > ctx->map.size) {
        size_t new_size = ctx->map.size * 2;
        while (ctx->header->data_tail + size > new_size) {
            new_size *= 2;
        }

        if (kc_map_resize(&ctx->map, new_size) != 0) {
            return KC_KROW_ERROR;
        }

        kc_krow_refresh_pointers(ctx);
    }

    slot = (key * 11400714819323198485ULL) % ctx->header->capacity;
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

        if (ctx->index[idx].length == (uint64_t)-1) {
            if (first_tombstone_slot == -1) {
                first_tombstone_slot = (int64_t)idx;
            }
        }
    }

    if (first_tombstone_slot != -1) {
        target_idx = first_tombstone_slot;
        is_empty_insert = 0;
    } else if (first_empty_slot != -1) {
        target_idx = first_empty_slot;
        is_empty_insert = 1;
    } else {
        return KC_KROW_ERROR;
    }

    header_size = sizeof(kc_krow_header_t);
    index_size = ctx->header->capacity * sizeof(kc_krow_node_t);
    relative_tail = ctx->header->data_tail - (header_size + index_size);

    memcpy(ctx->heap + relative_tail, value, size);

    ctx->index[target_idx].key = key;
    ctx->index[target_idx].offset = ctx->header->data_tail;
    ctx->index[target_idx].length = size;
    ctx->header->data_tail += size;

    if (is_empty_insert) {
        ctx->header->count++;
    }

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
    uint64_t relative_offset;

    slot = (key * 11400714819323198485ULL) % ctx->header->capacity;
    header_size = sizeof(kc_krow_header_t);
    index_size = ctx->header->capacity * sizeof(kc_krow_node_t);

    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t idx = (slot + i) % ctx->header->capacity;

        if (ctx->index[idx].length == 0) {
            break;
        }

        if (ctx->index[idx].length == (uint64_t)-1) {
            continue;
        }

        if (ctx->index[idx].key == key) {
            if (ctx->index[idx].offset + ctx->index[idx].length > ctx->map.size) {
                continue;
            }

            relative_offset = ctx->index[idx].offset -
                (header_size + index_size);
            if (cb(key, ctx->heap + relative_offset,
                ctx->index[idx].length, arg) != 0) {
                break;
            }
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

    slot = (key * 11400714819323198485ULL) % ctx->header->capacity;
    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t idx = (slot + i) % ctx->header->capacity;

        if (ctx->index[idx].length == 0) {
            break;
        }

        if (ctx->index[idx].length != (uint64_t)-1 &&
            ctx->index[idx].key == key) {
            ctx->index[idx].length = (uint64_t)-1;
            if (ctx->header->count > 0) {
                ctx->header->count--;
            }
        }
    }

    return KC_KROW_OK;
}

/**
 * Synchronize memory map to disk.
 * @param ctx Context pointer.
 * @return Status code.
 */
int kc_krow_sync(kc_krow_t *ctx) {
    return kc_map_sync(&ctx->map);
}
