/**
 * krow.c - Summary of the functionality
 * Summary: Command line interface for the krow storage engine.
 *
 * Author:  KaisarCode
 * Website: https://kaisarcode.com
 * License: https://www.gnu.org/licenses/gpl-3.0.html
 */

#define _POSIX_C_SOURCE 200809L

#include "krow.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * Print command usage information.
 * @param name Program executable name.
 * @return None.
 */
static void kc_print_help(const char *name) {
    printf("Usage: %s <command> <file> [args]\n", name);
    printf("\n");
    printf("Commands:\n");
    printf("    ini <capacity>      Initialize a new store\n");
    printf("    set <key> <value>   Add a record\n");
    printf("    get <key>           Retrieve records\n");
    printf("    del <key>           Delete records\n");
    printf("    prune               Defragment heap and rehash index\n");
    printf("\n");
    printf("Options:\n");
    printf("    -h, --help          Show this help message\n");
}

/**
 * Parse a decimal unsigned 64-bit integer from a string.
 * @param s Input string.
 * @param out Output pointer for the parsed value.
 * @return 0 on success, -1 on failure.
 */
static int kc_parse_u64(const char *s, uint64_t *out) {
    char *end;
    unsigned long long v;

    if (!s || *s == '\0') {
        return -1;
    }

    errno = 0;
    v = strtoull(s, &end, 10);
    if (errno != 0) {
        return -1;
    }
    if (end == s || *end != '\0') {
        return -1;
    }

    *out = (uint64_t)v;
    return 0;
}

/**
 * Check whether a file already exists with non-zero size.
 * @param path File path.
 * @return 1 if present and non-empty, 0 otherwise.
 */
static int kc_file_has_content(const char *path) {
    FILE *f;
    long size;

    f = fopen(path, "rb");
    if (!f) {
        return 0;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return 0;
    }
    size = ftell(f);
    fclose(f);

    return size > 0 ? 1 : 0;
}

/**
 * Callback for printing matches.
 * @param key Record key.
 * @param value Value data.
 * @param size Value size in bytes.
 * @param arg Unused.
 * @return 0 to continue iteration.
 */
static int kc_on_match(uint64_t key, const void *value, size_t size,
    void *arg) {
    (void)key;
    (void)arg;
    fwrite(value, 1, size, stdout);
    printf("\n");
    return 0;
}

/**
 * Execute the ini command.
 * @param path Store path.
 * @param cap_arg Capacity argument string.
 * @return Process exit code.
 */
static int kc_cmd_ini(const char *path, const char *cap_arg) {
    kc_krow_t *ctx;
    uint64_t cap;

    if (kc_file_has_content(path)) {
        fprintf(stderr, "krow: %s already exists\n", path);
        return 1;
    }

    if (kc_parse_u64(cap_arg, &cap) != 0 || cap == 0) {
        fprintf(stderr, "krow: invalid capacity\n");
        return 1;
    }

    ctx = kc_krow_open(path, cap);
    if (!ctx) {
        fprintf(stderr, "krow: failed to initialize %s\n", path);
        return 1;
    }

    kc_krow_close(ctx);
    return 0;
}

/**
 * Execute the set command.
 * @param ctx Open store context.
 * @param key_arg Key argument string.
 * @param value Value string.
 * @return Process exit code.
 */
static int kc_cmd_set(kc_krow_t *ctx, const char *key_arg, const char *value) {
    uint64_t key;

    if (kc_parse_u64(key_arg, &key) != 0) {
        fprintf(stderr, "krow: invalid key\n");
        return 1;
    }

    if (kc_krow_set(ctx, key, value, strlen(value)) != KC_KROW_OK) {
        fprintf(stderr, "krow: set failed\n");
        return 1;
    }

    if (kc_krow_sync(ctx) != KC_KROW_OK) {
        fprintf(stderr, "krow: sync failed\n");
        return 1;
    }

    return 0;
}

/**
 * Execute the get command.
 * @param ctx Open store context.
 * @param key_arg Key argument string.
 * @return Process exit code.
 */
static int kc_cmd_get(kc_krow_t *ctx, const char *key_arg) {
    uint64_t key;

    if (kc_parse_u64(key_arg, &key) != 0) {
        fprintf(stderr, "krow: invalid key\n");
        return 1;
    }

    if (kc_krow_get(ctx, key, kc_on_match, NULL) != KC_KROW_OK) {
        fprintf(stderr, "krow: get failed\n");
        return 1;
    }

    return 0;
}

/**
 * Execute the del command.
 * @param ctx Open store context.
 * @param key_arg Key argument string.
 * @return Process exit code.
 */
static int kc_cmd_del(kc_krow_t *ctx, const char *key_arg) {
    uint64_t key;

    if (kc_parse_u64(key_arg, &key) != 0) {
        fprintf(stderr, "krow: invalid key\n");
        return 1;
    }

    if (kc_krow_del(ctx, key) != KC_KROW_OK) {
        fprintf(stderr, "krow: del failed\n");
        return 1;
    }

    if (kc_krow_sync(ctx) != KC_KROW_OK) {
        fprintf(stderr, "krow: sync failed\n");
        return 1;
    }

    return 0;
}

/**
 * Execute the prune command.
 * @param ctx Open store context.
 * @return Process exit code.
 */
static int kc_cmd_prune(kc_krow_t *ctx) {
    if (kc_krow_prune(ctx) != KC_KROW_OK) {
        fprintf(stderr, "krow: failed to prune store\n");
        return 1;
    }

    if (kc_krow_sync(ctx) != KC_KROW_OK) {
        fprintf(stderr, "krow: sync failed\n");
        return 1;
    }

    return 0;
}

/**
 * Dispatch a CLI command other than ini.
 * @param cmd Command name.
 * @param path Store path.
 * @param argc Remaining argument count from the call site.
 * @param argv Remaining argument vector from the call site.
 * @return Process exit code.
 */
static int kc_dispatch(const char *cmd, const char *path, int argc,
    char **argv) {
    kc_krow_t *ctx;
    int rc;

    ctx = kc_krow_open(path, 0);
    if (!ctx) {
        fprintf(stderr, "krow: failed to open %s\n", path);
        return 1;
    }

    rc = 1;
    if (strcmp(cmd, "set") == 0) {
        if (argc < 5) {
            fprintf(stderr, "krow: missing value\n");
        } else {
            rc = kc_cmd_set(ctx, argv[3], argv[4]);
        }
    } else if (strcmp(cmd, "get") == 0) {
        rc = kc_cmd_get(ctx, argv[3]);
    } else if (strcmp(cmd, "del") == 0) {
        rc = kc_cmd_del(ctx, argv[3]);
    } else if (strcmp(cmd, "prune") == 0) {
        rc = kc_cmd_prune(ctx);
    } else {
        fprintf(stderr, "krow: unknown command %s\n", cmd);
    }

    kc_krow_close(ctx);
    return rc;
}

/**
 * Main entry point.
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return Process exit code.
 */
int main(int argc, char **argv) {
    const char *cmd;
    const char *path;

    if (argc < 2) {
        kc_print_help(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
        kc_print_help(argv[0]);
        return 0;
    }

    if (argc < 4) {
        kc_print_help(argv[0]);
        return 1;
    }

    cmd = argv[1];
    path = argv[2];

    if (strcmp(cmd, "ini") == 0) {
        return kc_cmd_ini(path, argv[3]);
    }

    return kc_dispatch(cmd, path, argc, argv);
}
