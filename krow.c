/**
 * krow.c - Summary of the functionality
 * Summary: Command line interface for the krow storage engine.
 *
 * Author:  KaisarCode
 * Website: https://kaisarcode.com
 * License: https://www.gnu.org/licenses/gpl-3.0.html
 */

#include "krow.h"

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
    printf("    init <capacity>     Initialize a new store\n");
    printf("    put <key> <value>   Add a record\n");
    printf("    get <key>           Retrieve records\n");
    printf("\n");
    printf("Options:\n");
    printf("    -h, --help          Show this help message\n");
}

/**
 * Callback for printing matches.
 * @param key Record key.
 * @param value Value data.
 * @param size Value size.
 * @param arg Unused.
 * @return 0 to continue.
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
 * Main entry point.
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return Status code.
 */
int main(int argc, char **argv) {
    kc_krow_t *ctx;
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

    if (strcmp(cmd, "init") == 0) {
        uint64_t cap = strtoull(argv[3], NULL, 10);
        ctx = kc_krow_open(path, cap);
        kc_krow_close(ctx);
        return 0;
    }

    /* For other commands, we open with 0 capacity (load existing) */
    ctx = kc_krow_open(path, 0);
    if (!ctx) {
        fprintf(stderr, "krow: failed to open %s\n", path);
        return 1;
    }

    if (strcmp(cmd, "put") == 0) {
        if (argc < 5) {
            fprintf(stderr, "krow: missing value\n");
            kc_krow_close(ctx);
            return 1;
        }
        uint64_t key = strtoull(argv[3], NULL, 10);
        kc_krow_put(ctx, key, argv[4], strlen(argv[4]));
        kc_krow_sync(ctx);
    } else if (strcmp(cmd, "get") == 0) {
        uint64_t key = strtoull(argv[3], NULL, 10);
        kc_krow_get(ctx, key, kc_on_match, NULL);
    } else {
        fprintf(stderr, "krow: unknown command %s\n", cmd);
        kc_krow_close(ctx);
        return 1;
    }

    kc_krow_close(ctx);
    return 0;
}
