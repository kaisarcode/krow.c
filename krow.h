/**
 * krow.h - Summary of the functionality
 * Summary: Public API for the krow persistent key-value store.
 *
 * Author:  KaisarCode
 * Website: https://kaisarcode.com
 * License: https://www.gnu.org/licenses/gpl-3.0.html
 */

#ifndef KC_KROW_H
#define KC_KROW_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kc_krow kc_krow_t;

#define KC_KROW_MAGIC 0x4b524f57
#define KC_KROW_OK 0
#define KC_KROW_ERROR -1

/**
 * Callback for key matches.
 * @param key Record key.
 * @param value Pointer to value data.
 * @param size Value size in bytes.
 * @param arg User argument.
 * @return 0 to continue, non-zero to stop.
 */
typedef int (*kc_krow_cb)(uint64_t key, const void *value, size_t size,
    void *arg);

/**
 * Open or create a krow store.
 * @param path File path.
 * @param capacity Maximum number of index slots.
 * @return Context pointer.
 */
kc_krow_t *kc_krow_open(const char *path, uint64_t capacity);

/**
 * Close the krow store.
 * @param ctx Context pointer.
 * @return None.
 */
void kc_krow_close(kc_krow_t *ctx);

/**
 * Add a record to the store.
 * @param ctx Context pointer.
 * @param key Record key.
 * @param value Value data.
 * @param size Value size.
 * @return Status code.
 */
int kc_krow_set(kc_krow_t *ctx, uint64_t key, const void *value, size_t size);

/**
 * Retrieve records by key.
 * @param ctx Context pointer.
 * @param key Record key.
 * @param cb Result callback.
 * @param arg User argument.
 * @return Status code.
 */
int kc_krow_get(kc_krow_t *ctx, uint64_t key, kc_krow_cb cb, void *arg);

/**
 * Delete records by key.
 * @param ctx Context pointer.
 * @param key Record key.
 * @return Status code.
 */
int kc_krow_del(kc_krow_t *ctx, uint64_t key);

/**
 * Prune tombstones and defragment heap.
 * @param ctx Context pointer.
 * @return Status code.
 */
int kc_krow_prune(kc_krow_t *ctx);

/**
 * Synchronize memory map to disk.
 * @param ctx Context pointer.
 * @return Status code.
 */
int kc_krow_sync(kc_krow_t *ctx);

#ifdef __cplusplus
}
#endif

#endif
