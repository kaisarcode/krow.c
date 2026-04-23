#!/bin/sh
# test.sh
# Summary: Validation suite for krow functionality.
# Author:  KaisarCode
# Website: https://kaisarcode.com
# License: https://www.gnu.org/licenses/gpl-3.0.html

# Prints one failure line.
# @param 1 Failure message.
# @return 1 on failure.
kc_test_fail() {
    printf '[FAIL] %s\n' "$1"
    return 1
}

# Prints one success line.
# @param 1 Success message.
# @return 0 on success.
kc_test_pass() {
    printf '[PASS] %s\n' "$1"
}

# Verifies the binary exists.
# @return 0 on success, 1 on failure.
kc_test_check_binary() {
    if [ ! -x "./krow" ]; then
        printf '%s\n' '[ERROR] krow binary not found. Please compile first.'
        return 1
    fi

    return 0
}

# Runs the full validation suite.
# @return 0 on success, 1 on failure.
kc_test_main() {
    failed=0
    test_db="test.krow"

    kc_test_check_binary || exit 1

    rm -f "$test_db"

    if ! ./krow ini "$test_db" 1000; then
        kc_test_fail "store initialization"
        failed=$((failed + 1))
    else
        kc_test_pass "store initialization"
    fi

    ./krow set "$test_db" 123 "hello world"
    res=$(./krow get "$test_db" 123)
    if [ "$res" != "hello world" ]; then
        kc_test_fail "basic set/get"
        failed=$((failed + 1))
    else
        kc_test_pass "basic set/get"
    fi

    ./krow set "$test_db" 456 "val1"
    ./krow set "$test_db" 456 "val2"
    res_count=$(./krow get "$test_db" 456 | wc -l)
    if [ "$res_count" -ne 2 ]; then
        kc_test_fail "multi-value support (expected 2, got $res_count)"
        failed=$((failed + 1))
    else
        kc_test_pass "multi-value support"
    fi

    ./krow del "$test_db" 456
    res_count_after=$(./krow get "$test_db" 456 | wc -l)
    if [ "$res_count_after" -ne 0 ]; then
        kc_test_fail "delete support (expected 0, got $res_count_after)"
        failed=$((failed + 1))
    else
        kc_test_pass "delete support"
    fi

    rm -f "$test_db"

    if [ "$failed" -eq 0 ]; then
        printf '%s\n' '[SUCCESS] All tests passed!'
        return 0
    fi

    printf '[FAILURE] %s tests failed.\n' "$failed"
    return 1
}

kc_test_main
