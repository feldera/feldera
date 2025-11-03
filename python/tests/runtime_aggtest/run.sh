#!/usr/bin/env bash
set -euo pipefail

# List of all test modules to run
readonly TESTS=(
    "aggregate_tests/main.py"
    "aggregate_tests2/main.py"
    "aggregate_tests3/main.py"
    "aggregate_tests4/main.py"
    "aggregate_tests5/main.py"
    "aggregate_tests6/main.py"
    "arithmetic_tests/main.py"
    "asof_tests/main.py"
    "complex_type_tests/main.py"
    "orderby_tests/main.py"
    "variant_tests/main.py"
    "illarg_tests/main.py"
    "negative_tests/main.py"
)

readonly MAX_JOBS="${RUNTIME_AGGTEST_JOBS:-1}"
readonly VERBOSE="${RUNTIME_AGGTEST_VERBOSE:-false}"
readonly KEEP_LOGS="${RUNTIME_AGGTEST_KEEP_LOGS:-false}"

LOG_DIR="$(mktemp -d)"
OUTPUT_PREFIX="$LOG_DIR/test_output"


# Arrays for tracking jobs
pids=()
test_names=()
log_files=()
failed_tests=()
failed_exit_codes=()

cleanup() {
    echo "Cleaning up..."
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait
    if [[ "$KEEP_LOGS" == "false" && ${#failed_tests[@]} -eq 0 ]]; then
        rm -rf "$LOG_DIR"
    fi
}
trap cleanup EXIT INT TERM

echo "Running ${#TESTS[@]} runtime_aggtest modules with max $MAX_JOBS parallel jobs"

run_test() {
    local test_name="$1"
    local log_file="$2"
    local test_path="$(cd "$(dirname "$0")" && pwd)/$test_name"

    echo "=== Test: $test_name ===" > "$log_file"
    echo "Started at: $(date)" >> "$log_file"
    echo "" >> "$log_file"

    if uv run --locked "$test_path" >> "$log_file" 2>&1; then
        echo "✅ PASSED: $test_name"
        echo "Completed at: $(date)" >> "$log_file"
        echo "Status: PASSED" >> "$log_file"
        [[ "$VERBOSE" == "true" ]] && cat "$log_file"
    else
        local exit_code=$?
        echo "❌ FAILED: $test_name" >&2
        echo "Completed at: $(date)" >> "$log_file"
        echo "Status: FAILED (exit code: $exit_code)" >> "$log_file"
        cat "$log_file" >&2
        return $exit_code
    fi
}

# Launch jobs with controlled parallelism
for test in "${TESTS[@]}"; do
    # Wait for a slot if at max jobs
    while (( ${#pids[@]} >= MAX_JOBS )); do
        for i in "${!pids[@]}"; do
            if ! kill -0 "${pids[$i]}" 2>/dev/null; then
                if wait "${pids[$i]}"; then
                    exit_code=0
                else
                    exit_code=$?
                fi
                if [[ $exit_code -ne 0 ]]; then
                    failed_tests+=("${test_names[$i]}")
                    failed_exit_codes+=("$exit_code")
                fi
                unset pids[$i] test_names[$i] log_files[$i]
                # Repack arrays
                pids=("${pids[@]}")
                test_names=("${test_names[@]}")
                log_files=("${log_files[@]}")
                break
            fi
        done
        sleep 0.2
    done

    test_safe_name="${test//\//_}"
    log_file="${OUTPUT_PREFIX}_${test_safe_name}.log"
    run_test "$test" "$log_file" &
    pids+=($!)
    test_names+=("$test")
    log_files+=("$log_file")
done

# Wait for all remaining jobs
for i in "${!pids[@]}"; do
    if wait "${pids[$i]}"; then
        exit_code=0
    else
        exit_code=$?
    fi
    if [[ $exit_code -ne 0 ]]; then
        failed_tests+=("${test_names[$i]}")
        failed_exit_codes+=("$exit_code")
    fi
done

if [[ ${#failed_tests[@]} -eq 0 ]]; then
    echo "✅ All tests passed"
    echo "📋 Test logs: $LOG_DIR"
else
    echo "❌ ${#failed_tests[@]} test(s) failed:" >&2
    for i in "${!failed_tests[@]}"; do
        printf '  %s (exit code: %s)\n' "${failed_tests[$i]}" "${failed_exit_codes[$i]}" >&2
    done
    echo "📋 Logs: $LOG_DIR" >&2
    exit 1
fi

