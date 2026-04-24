#!/bin/bash
# test.sh - Validation suite for krow functionality
# Summary: Exercises the CLI across happy-path and failure cases.
# Author:  KaisarCode
# Website: https://kaisarcode.com
# License: https://www.gnu.org/licenses/gpl-3.0.html

# Print one failure line.
# @param $1 Failure message.
# @return Always 1.
kc_test_fail() {
    printf '[FAIL] %s\n' "$1"
    return 1
}

# Print one success line.
# @param $1 Success message.
# @return Always 0.
kc_test_pass() {
    printf '[PASS] %s\n' "$1"
}

# Verify the binary exists.
# @return 0 on success, 1 on failure.
kc_test_check_binary() {
    if [ ! -x "./krow" ]; then
        printf '%s\n' '[ERROR] krow binary not found. Please compile first.'
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

# Exercise prune correctness under hash collisions.
# @return 0 on pass, 1 on fail.
kc_test_prune_collision() {
    local db
    local res
    db="test-prune.krow"
    rm -f "$db"
    ./krow ini "$db" 4 >/dev/null 2>&1
    ./krow set "$db" 1 "one" >/dev/null 2>&1
    ./krow set "$db" 5 "five" >/dev/null 2>&1
    ./krow del "$db" 1 >/dev/null 2>&1
    ./krow prune "$db" >/dev/null 2>&1
    res=$(./krow get "$db" 5)
    rm -f "$db"
    if [ "$res" = "five" ]; then
        kc_test_pass "prune preserves displaced keys"
        return 0
    fi
    kc_test_fail "prune preserves displaced keys (got '$res')"
}

# Exercise capacity saturation rejection.
# @return 0 on pass, 1 on fail.
kc_test_capacity_full() {
    local db
    db="test-cap.krow"
    rm -f "$db"
    ./krow ini "$db" 2 >/dev/null 2>&1
    ./krow set "$db" 1 "a" >/dev/null 2>&1
    ./krow set "$db" 2 "b" >/dev/null 2>&1
    if ./krow set "$db" 3 "c" >/dev/null 2>&1; then
        rm -f "$db"
        kc_test_fail "capacity full should reject inserts"
        return 1
    fi
    rm -f "$db"
    kc_test_pass "capacity full rejection"
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
    kc_test_pass "invalid key rejection"
}

# Exercise ini refusal on an existing store.
# @return 0 on pass, 1 on fail.
kc_test_ini_exists() {
    local db
    db="test-exists.krow"
    rm -f "$db"
    ./krow ini "$db" 10 >/dev/null 2>&1
    if ./krow ini "$db" 10 >/dev/null 2>&1; then
        rm -f "$db"
        kc_test_fail "ini should refuse existing store"
        return 1
    fi
    rm -f "$db"
    kc_test_pass "ini refuses existing store"
}

# Exercise data persistence across reopen.
# @return 0 on pass, 1 on fail.
kc_test_reopen() {
    local db
    local res
    db="test-reopen.krow"
    rm -f "$db"
    ./krow ini "$db" 10 >/dev/null 2>&1
    ./krow set "$db" 42 "persisted" >/dev/null 2>&1
    res=$(./krow get "$db" 42)
    rm -f "$db"
    if [ "$res" = "persisted" ]; then
        kc_test_pass "reopen preserves data"
        return 0
    fi
    kc_test_fail "reopen preserves data (got '$res')"
}

# Exercise open-time recovery of a corrupted count field.
# @return 0 on pass, 1 on fail.
kc_test_recovery_count() {
    local db
    local res
    db="test-recover.krow"
    rm -f "$db"
    ./krow ini "$db" 10 >/dev/null 2>&1
    ./krow set "$db" 7 "seven" >/dev/null 2>&1
    ./krow set "$db" 11 "eleven" >/dev/null 2>&1
    printf '\xff\xff\xff\xff\xff\xff\xff\xff' \
        | dd of="$db" bs=1 seek=16 count=8 conv=notrunc status=none
    res=$(./krow get "$db" 7)
    rm -f "$db"
    if [ "$res" = "seven" ]; then
        kc_test_pass "recovery repairs corrupted count"
        return 0
    fi
    kc_test_fail "recovery repairs corrupted count (got '$res')"
}

# Exercise prune survival of a stale tmp file.
# @return 0 on pass, 1 on fail.
kc_test_prune_stale_tmp() {
    local db
    local res
    db="test-staletmp.krow"
    rm -f "$db" "$db.tmp"
    ./krow ini "$db" 10 >/dev/null 2>&1
    ./krow set "$db" 42 "answer" >/dev/null 2>&1
    printf 'garbage' > "$db.tmp"
    ./krow prune "$db" >/dev/null 2>&1
    res=$(./krow get "$db" 42)
    rm -f "$db" "$db.tmp"
    if [ "$res" = "answer" ]; then
        kc_test_pass "prune overrides stale tmp file"
        return 0
    fi
    kc_test_fail "prune overrides stale tmp file (got '$res')"
}

# Exercise invalid capacity rejection.
# @return 0 on pass, 1 on fail.
kc_test_invalid_capacity() {
    local db
    db="test-badcap.krow"
    rm -f "$db"
    if ./krow ini "$db" 0 >/dev/null 2>&1; then
        rm -f "$db"
        kc_test_fail "zero capacity should be rejected"
        return 1
    fi
    if ./krow ini "$db" abc >/dev/null 2>&1; then
        rm -f "$db"
        kc_test_fail "non-numeric capacity should be rejected"
        return 1
    fi
    rm -f "$db"
    kc_test_pass "invalid capacity rejection"
}

# Run the full validation suite.
# @return 0 on success, 1 on failure.
kc_test_main() {
    local failed
    local db
    failed=0
    db="test.krow"

    kc_test_check_binary || exit 1

    rm -f "$db"

    kc_test_init "$db" || failed=$((failed + 1))
    kc_test_basic_roundtrip "$db" || failed=$((failed + 1))
    kc_test_multi_value "$db" || failed=$((failed + 1))
    kc_test_delete "$db" || failed=$((failed + 1))
    kc_test_invalid_key "$db" || failed=$((failed + 1))

    rm -f "$db"

    kc_test_prune_collision || failed=$((failed + 1))
    kc_test_capacity_full || failed=$((failed + 1))
    kc_test_ini_exists || failed=$((failed + 1))
    kc_test_reopen || failed=$((failed + 1))
    kc_test_recovery_count || failed=$((failed + 1))
    kc_test_prune_stale_tmp || failed=$((failed + 1))
    kc_test_invalid_capacity || failed=$((failed + 1))

    if [ "$failed" -eq 0 ]; then
        printf '%s\n' '[SUCCESS] All tests passed!'
        return 0
    fi

    printf '[FAILURE] %s tests failed.\n' "$failed"
    return 1
}

kc_test_main
