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
#endif

#include "krow.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

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

struct kc_krow {
    int fd;
    size_t map_size;
    kc_krow_header_t *header;
    kc_krow_node_t *index;
    uint8_t *heap;
};

/**
 * Handle critical errors by terminating the process.
 * @param msg Error message.
 * @return None.
 */
static void kc_krow_die(const char *msg) {
    perror(msg);
    exit(1);
}

/**
 * Initialize a new krow store.
 * @param path File path.
 * @param capacity Maximum number of index slots.
 * @return Context pointer.
 */
kc_krow_t *kc_krow_open(const char *path, uint64_t capacity) {
    kc_krow_t *ctx;
    struct stat st;
    size_t header_size;
    size_t index_size;
    size_t initial_heap;
    int is_new;

    ctx = calloc(1, sizeof(kc_krow_t));
    if (!ctx) {
        return NULL;
    }

    ctx->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (ctx->fd == -1) {
        kc_krow_die("krow: open");
    }

    if (fstat(ctx->fd, &st) == -1) {
        kc_krow_die("krow: fstat");
    }

    is_new = (st.st_size == 0);
    header_size = sizeof(kc_krow_header_t);
    index_size = capacity * sizeof(kc_krow_node_t);
    initial_heap = 1024 * 1024 * 64;

    if (is_new) {
        ctx->map_size = header_size + index_size + initial_heap;
        if (ftruncate(ctx->fd, ctx->map_size) == -1) {
            kc_krow_die("krow: ftruncate");
        }
    } else {
        ctx->map_size = st.st_size;
    }

    ctx->header = mmap(NULL, ctx->map_size, PROT_READ | PROT_WRITE,
        MAP_SHARED, ctx->fd, 0);
    if (ctx->header == MAP_FAILED) {
        kc_krow_die("krow: mmap");
    }

    if (is_new) {
        ctx->header->magic = KC_KROW_MAGIC;
        ctx->header->capacity = capacity;
        ctx->header->count = 0;
        ctx->header->data_tail = header_size + index_size;
    } else if (ctx->header->magic != KC_KROW_MAGIC) {
        fprintf(stderr, "krow: invalid magic\n");
        exit(1);
    }

    ctx->index = (kc_krow_node_t *)((uint8_t *)ctx->header + header_size);
    ctx->heap = (uint8_t *)ctx->header;

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

    munmap(ctx->header, ctx->map_size);
    close(ctx->fd);
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
int kc_krow_put(kc_krow_t *ctx, uint64_t key, const void *value, size_t size) {
    uint64_t slot;
    uint64_t i;

    if (ctx->header->count >= ctx->header->capacity) {
        kc_krow_die("krow: capacity reached");
    }

    if (ctx->header->data_tail + size > ctx->map_size) {
        /* Simple expansion strategy for minimalist design */
        size_t new_size = ctx->map_size * 2;
        while (ctx->header->data_tail + size > new_size) {
            new_size *= 2;
        }

        munmap(ctx->header, ctx->map_size);
        if (ftruncate(ctx->fd, new_size) == -1) {
            kc_krow_die("krow: ftruncate expansion");
        }

        ctx->header = mmap(NULL, new_size, PROT_READ | PROT_WRITE,
            MAP_SHARED, ctx->fd, 0);
        if (ctx->header == MAP_FAILED) {
            kc_krow_die("krow: mmap expansion");
        }

        ctx->map_size = new_size;
        ctx->index = (kc_krow_node_t *)((uint8_t *)ctx->header +
            sizeof(kc_krow_header_t));
        ctx->heap = (uint8_t *)ctx->header;
    }

    slot = key % ctx->header->capacity;
    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t idx = (slot + i) % ctx->header->capacity;
        if (ctx->index[idx].length == 0) {
            memcpy(ctx->heap + ctx->header->data_tail, value, size);
            ctx->index[idx].key = key;
            ctx->index[idx].offset = ctx->header->data_tail;
            ctx->index[idx].length = size;
            ctx->header->data_tail += size;
            ctx->header->count++;
            return KC_KROW_OK;
        }
    }

    return KC_KROW_ERROR;
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

    slot = key % ctx->header->capacity;
    for (i = 0; i < ctx->header->capacity; i++) {
        uint64_t idx = (slot + i) % ctx->header->capacity;
        if (ctx->index[idx].length == 0) {
            break;
        }

        if (ctx->index[idx].key == key) {
            if (cb(key, ctx->heap + ctx->index[idx].offset,
                ctx->index[idx].length, arg) != 0) {
                break;
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
    if (msync(ctx->header, ctx->map_size, MS_SYNC) == -1) {
        return KC_KROW_ERROR;
    }

    return KC_KROW_OK;
}
