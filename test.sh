#!/bin/bash
# test.sh - Validation suite for krow functionality
# Summary: Exercises the CLI across core success and failure cases.
# Author:  KaisarCode
# Website: https://kaisarcode.com
# License: https://www.gnu.org/licenses/gpl-3.0.html

KC_TEST_DIR=""
KC_TEST_ROOT=""

# Remove temporary test files.
# @return Always 0.
kc_test_cleanup() {
    if [ -n "$KC_TEST_DIR" ]; then
        rm -rf "$KC_TEST_DIR"
    fi
}

# Print one failure line.
# @param $1 Failure message.
# @return Always 1.
kc_test_fail() {
    printf '\033[31m[FAIL]\033[0m %s\n' "$1"
    return 1
}

# Print one success line.
# @param $1 Success message.
# @return Always 0.
kc_test_pass() {
    printf '\033[32m[PASS]\033[0m %s\n' "$1"
}

# Verify the binary exists.
# @return 0 on success, 1 on failure.
kc_test_check_binary() {
    if [ ! -x "./krow" ]; then
        return 1
    fi
    return 0
}

# Exercise store initialization.
# @param $1 Store path.
# @return 0 on pass, 1 on fail.
kc_test_init() {
    local db="$1"
    if ./krow ini "$db" 1000 >/dev/null 2>&1; then
        kc_test_pass "store initialization"
        return 0
    fi
    kc_test_fail "store initialization"
}

# Exercise basic set/get round trip.
# @param $1 Store path.
# @return 0 on pass, 1 on fail.
kc_test_basic_roundtrip() {
    local db="$1"
    local res
    ./krow set "$db" 123 "hello world" >/dev/null 2>&1
    res=$(./krow get "$db" 123)
    if [ "$res" = "hello world" ]; then
        kc_test_pass "basic set/get"
        return 0
    fi
    kc_test_fail "basic set/get"
}

# Exercise multi-value support.
# @param $1 Store path.
# @return 0 on pass, 1 on fail.
kc_test_multi_value() {
    local db="$1"
    local count
    ./krow set "$db" 456 "val1" >/dev/null 2>&1
    ./krow set "$db" 456 "val2" >/dev/null 2>&1
    count=$(./krow get "$db" 456 | wc -l)
    if [ "$count" -eq 2 ]; then
        kc_test_pass "multi-value support"
        return 0
    fi
    kc_test_fail "multi-value support (expected 2, got $count)"
}

# Exercise deletion across duplicates.
# @param $1 Store path.
# @return 0 on pass, 1 on fail.
kc_test_delete() {
    local db="$1"
    local count
    ./krow del "$db" 456 >/dev/null 2>&1
    count=$(./krow get "$db" 456 | wc -l)
    if [ "$count" -eq 0 ]; then
        kc_test_pass "delete support"
        return 0
    fi
    kc_test_fail "delete support (expected 0, got $count)"
}

# Exercise invalid key input rejection.
# @param $1 Store path.
# @return 0 on pass, 1 on fail.
kc_test_invalid_key() {
    local db="$1"
    if ./krow set "$db" "notanumber" "val" >/dev/null 2>&1; then
        kc_test_fail "invalid key should be rejected"
        return 1
    fi
    if ./krow set "$db" "1x" "val" >/dev/null 2>&1; then
        kc_test_fail "partial key should be rejected"
        return 1
    fi
    kc_test_pass "invalid key rejection"
}

# Exercise get on a missing key.
# @return 0 on pass, 1 on fail.
kc_test_get_missing_key() {
    local db
    local res
    db="$KC_TEST_DIR/test-misskey.krow"
    rm -f "$db" "$db.lock"
    ./krow ini "$db" 10 >/dev/null 2>&1
    if ! res=$(./krow get "$db" 999 2>/dev/null); then
        rm -f "$db" "$db.lock"
        kc_test_fail "get on missing key should succeed (return 0)"
        return 1
    fi
    if [ -n "$res" ]; then
        rm -f "$db" "$db.lock"
        kc_test_fail "get on missing key should print nothing to stdout"
        return 1
    fi
    rm -f "$db" "$db.lock"
    kc_test_pass "get on missing key"
}

# Exercise missing file open rejection.
# @return 0 on pass, 1 on fail.
kc_test_missing_open() {
    local db
    local out
    db="$KC_TEST_DIR/test-missing.krow"
    rm -f "$db" "$db.lock"
    if out=$(./krow get "$db" 1 2>/dev/null); then
        rm -f "$db" "$db.lock"
        kc_test_fail "missing store should not open"
        return 1
    fi
    if [ -n "$out" ]; then
        rm -f "$db" "$db.lock"
        kc_test_fail "failed get should not print stdout"
        return 1
    fi
    if [ -e "$db" ]; then
        rm -f "$db" "$db.lock"
        kc_test_fail "missing store open should not create file"
        return 1
    fi
    rm -f "$db" "$db.lock"
    kc_test_pass "missing store open rejection"
}

# Exercise prune correctness under hash collisions.
# @return 0 on pass, 1 on fail.
kc_test_prune_collision() {
    local db
    local res
    db="$KC_TEST_DIR/test-prune.krow"
    rm -f "$db" "$db.lock" "$db.tmp.lock"
    ./krow ini "$db" 4 >/dev/null 2>&1
    ./krow set "$db" 1 "one" >/dev/null 2>&1
    ./krow set "$db" 5 "five" >/dev/null 2>&1
    ./krow del "$db" 1 >/dev/null 2>&1
    ./krow prune "$db" >/dev/null 2>&1
    res=$(./krow get "$db" 5)
    rm -f "$db" "$db.lock" "$db.tmp.lock"
    if [ "$res" = "five" ]; then
        kc_test_pass "prune preserves displaced keys"
        return 0
    fi
    kc_test_fail "prune preserves displaced keys (got '$res')"
}

# Exercise prune overwriting stale tmp files safely.
# @return 0 on pass, 1 on fail.
kc_test_prune_stale_tmp() {
    local db
    local res
    db="$KC_TEST_DIR/test-stale.krow"
    rm -f "$db" "$db.lock" "$db.tmp" "$db.tmp.lock"
    ./krow ini "$db" 10 >/dev/null 2>&1
    ./krow set "$db" 1 "one" >/dev/null 2>&1
    echo "stale data" > "$db.tmp"
    ./krow prune "$db" >/dev/null 2>&1
    res=$(./krow get "$db" 1)
    if [ "$res" != "one" ]; then
        rm -f "$db" "$db.lock" "$db.tmp" "$db.tmp.lock"
        kc_test_fail "prune failed to preserve data with stale tmp"
        return 1
    fi
    if [ -e "$db.tmp" ]; then
        rm -f "$db" "$db.lock" "$db.tmp" "$db.tmp.lock"
        kc_test_fail "prune left behind a tmp file"
        return 1
    fi
    rm -f "$db" "$db.lock" "$db.tmp" "$db.tmp.lock"
    kc_test_pass "prune stale tmp overwrite"
}

# Exercise capacity saturation rejection.
# @return 0 on pass, 1 on fail.
kc_test_capacity_full() {
    local db
    db="$KC_TEST_DIR/test-cap.krow"
    rm -f "$db" "$db.lock"
    ./krow ini "$db" 2 >/dev/null 2>&1
    ./krow set "$db" 1 "a" >/dev/null 2>&1
    ./krow set "$db" 2 "b" >/dev/null 2>&1
    if ./krow set "$db" 3 "c" >/dev/null 2>&1; then
        rm -f "$db" "$db.lock" "$db.tmp.lock"
        kc_test_fail "capacity full should reject inserts"
        return 1
    fi
    rm -f "$db" "$db.lock" "$db.tmp.lock"
    kc_test_pass "capacity full rejection"
}

# Exercise ini refusal on an existing store.
# @return 0 on pass, 1 on fail.
kc_test_ini_exists() {
    local db
    db="$KC_TEST_DIR/test-exists.krow"
    rm -f "$db" "$db.lock"
    ./krow ini "$db" 10 >/dev/null 2>&1
    if ./krow ini "$db" 10 >/dev/null 2>&1; then
        rm -f "$db" "$db.lock"
        kc_test_fail "ini should refuse existing store"
        return 1
    fi
    rm -f "$db" "$db.lock"
    kc_test_pass "ini refuses existing store"
}

# Exercise data persistence across reopen.
# @return 0 on pass, 1 on fail.
kc_test_reopen() {
    local db
    local res
    db="$KC_TEST_DIR/test-reopen.krow"
    rm -f "$db" "$db.lock"
    ./krow ini "$db" 10 >/dev/null 2>&1
    ./krow set "$db" 42 "persisted" >/dev/null 2>&1
    res=$(./krow get "$db" 42)
    rm -f "$db" "$db.lock"
    if [ "$res" = "persisted" ]; then
        kc_test_pass "reopen preserves data"
        return 0
    fi
    kc_test_fail "reopen preserves data (got '$res')"
}

# Exercise corrupted header rejection.
# @return 0 on pass, 1 on fail.
kc_test_corrupted_header() {
    local db
    db="$KC_TEST_DIR/test-recover.krow"
    rm -f "$db" "$db.lock"
    ./krow ini "$db" 10 >/dev/null 2>&1
    ./krow set "$db" 7 "seven" >/dev/null 2>&1
    printf '\xff\xff\xff\xff\xff\xff\xff\xff' \
        | dd of="$db" bs=1 seek=16 count=8 conv=notrunc status=none
    if ./krow get "$db" 7 >/dev/null 2>&1; then
        rm -f "$db" "$db.lock"
        kc_test_fail "corrupted header should be rejected"
        return 1
    fi
    rm -f "$db" "$db.lock"
    kc_test_pass "corrupted header rejection"
}

# Exercise invalid capacity rejection.
# @return 0 on pass, 1 on fail.
kc_test_invalid_capacity() {
    local db
    db="$KC_TEST_DIR/test-badcap.krow"
    rm -f "$db" "$db.lock"
    if ./krow ini "$db" 0 >/dev/null 2>&1; then
        rm -f "$db" "$db.lock"
        kc_test_fail "zero capacity should be rejected"
        return 1
    fi
    if ./krow ini "$db" abc >/dev/null 2>&1; then
        rm -f "$db" "$db.lock"
        kc_test_fail "non-numeric capacity should be rejected"
        return 1
    fi
    rm -f "$db" "$db.lock"
    kc_test_pass "invalid capacity rejection"
}

# Exercise shared-context multithread access.
# @return 0 on pass, 1 on fail.
kc_test_multithread() {
    local tmp
    tmp=$(mktemp -d)
    printf '%s\n' \
        '#define _POSIX_C_SOURCE 200809L' \
        '#include "krow.h"' \
        '#include <pthread.h>' \
        '#include <stdint.h>' \
        '#include <stdio.h>' \
        '#include <stdlib.h>' \
        '#include <string.h>' \
        '#include <unistd.h>' \
        '#define KC_THREADS 6' \
        '#define KC_OPS 500' \
        'typedef struct { kc_krow_t *ctx; uint64_t seed; int failed; } worker_t;' \
        'static uint64_t rnd(uint64_t *s) { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; return *s; }' \
        'static int on_get(uint64_t key, const void *value, size_t size, void *arg) { (void)key; (void)value; (void)size; (void)arg; return 0; }' \
        'static void *run(void *arg) {' \
        '    worker_t *w = arg;' \
        '    unsigned int i;' \
        '    for (i = 0; i < KC_OPS; i++) {' \
        '        uint64_t r = rnd(&w->seed);' \
        '        uint64_t key = r % 128;' \
        '        char value[64];' \
        '        if ((r % 7) == 0) {' \
        '            if (kc_krow_del(w->ctx, key) != KC_KROW_OK) {' \
        '                w->failed = 1;' \
        '            }' \
        '        } else if ((r % 5) == 0) {' \
        '            if (kc_krow_get(w->ctx, key, on_get, NULL) != KC_KROW_OK) {' \
        '                w->failed = 1;' \
        '            }' \
        '        } else {' \
        '            snprintf(value, sizeof(value), "v-%llu", (unsigned long long)r);' \
        '            if (kc_krow_set(w->ctx, key, value, strlen(value)) != KC_KROW_OK) {' \
        '                w->failed = 1;' \
        '            }' \
        '        }' \
        '    }' \
        '    return NULL;' \
        '}' \
        'int main(int argc, char **argv) {' \
        '    const char *path;' \
        '    char *lock_path;' \
        '    pthread_t thread[KC_THREADS];' \
        '    worker_t worker[KC_THREADS];' \
        '    kc_krow_t *ctx;' \
        '    unsigned int i;' \
        '    if (argc != 2) {' \
        '        return 4;' \
        '    }' \
        '    path = argv[1];' \
        '    lock_path = malloc(strlen(path) + 6);' \
        '    if (!lock_path) {' \
        '        return 5;' \
        '    }' \
        '    snprintf(lock_path, strlen(path) + 6, "%s.lock", path);' \
        '    unlink(path);' \
        '    unlink(lock_path);' \
        '    ctx = kc_krow_open(path, 4096);' \
        '    if (!ctx) {' \
        '        free(lock_path);' \
        '        return 1;' \
        '    }' \
        '    for (i = 0; i < KC_THREADS; i++) {' \
        '        worker[i].ctx = ctx;' \
        '        worker[i].seed = 0x12345678ULL + i;' \
        '        worker[i].failed = 0;' \
        '        if (pthread_create(&thread[i], NULL, run, &worker[i]) != 0) {' \
        '            return 2;' \
        '        }' \
        '    }' \
        '    for (i = 0; i < KC_THREADS; i++) {' \
        '        if (pthread_join(thread[i], NULL) != 0 || worker[i].failed) {' \
        '            return 3;' \
        '        }' \
        '    }' \
        '    kc_krow_close(ctx);' \
        '    unlink(path);' \
        '    unlink(lock_path);' \
        '    free(lock_path);' \
        '    return 0;' \
        '}' > "$tmp/thread.c"
    if cc -std=c11 -Wall -Wextra -Werror -pthread -I"$KC_TEST_ROOT" \
        "$tmp/thread.c" "$KC_TEST_ROOT/libkrow.c" -o "$tmp/thread" &&
        "$tmp/thread" "$KC_TEST_DIR/test-thread.krow" >/dev/null 2>&1; then
        rm -rf "$tmp"
        kc_test_pass "multithread shared context"
        return 0
    fi
    rm -rf "$tmp"
    kc_test_fail "multithread shared context"
}

# Run the full validation suite.
# @return 0 on success, 1 on failure.
kc_test_main() {
    local failed
    local db
    failed=0
    KC_TEST_ROOT=$(pwd)
    KC_TEST_DIR=$(mktemp -d)
    trap kc_test_cleanup EXIT INT HUP TERM
    db="$KC_TEST_DIR/test.krow"

    kc_test_check_binary || exit 1

    rm -f "$db" "$db.lock"

    kc_test_init "$db" || failed=$((failed + 1))
    kc_test_basic_roundtrip "$db" || failed=$((failed + 1))
    kc_test_multi_value "$db" || failed=$((failed + 1))
    kc_test_delete "$db" || failed=$((failed + 1))
    kc_test_invalid_key "$db" || failed=$((failed + 1))

    rm -f "$db" "$db.lock"

    kc_test_missing_open || failed=$((failed + 1))
    kc_test_get_missing_key || failed=$((failed + 1))
    kc_test_prune_collision || failed=$((failed + 1))
    kc_test_prune_stale_tmp || failed=$((failed + 1))
    kc_test_capacity_full || failed=$((failed + 1))
    kc_test_ini_exists || failed=$((failed + 1))
    kc_test_reopen || failed=$((failed + 1))
    kc_test_corrupted_header || failed=$((failed + 1))
    kc_test_invalid_capacity || failed=$((failed + 1))
    kc_test_multithread || failed=$((failed + 1))

    if [ "$failed" -eq 0 ]; then
        return 0
    fi

    return 1
}

kc_test_main
