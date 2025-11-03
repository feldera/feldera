set -e

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

# Maximum number of parallel jobs
readonly MAX_JOBS=${RUNTIME_AGGTEST_JOBS:-1}

# Verbosity control
readonly VERBOSE=${RUNTIME_AGGTEST_VERBOSE:-false}

# Create temporary directory for test outputs
readonly LOG_DIR=${(mktemp -d)}
readonly OUTPUT_PREFIX="$LOG_DIR/test_output"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Arrays to track running jobs
declare -a job_pids=()
declare -a job_names=()
declare -a job_log_files=()
declare -a failed_tests=()

# Function to run a single test
runtest() {
    local test_name="$1"
    local log_file="$2"

    echo "Starting test: $test_name"
    echo "=== Test: $test_name ===" > "$log_file"
    echo "Started at: $(date)" >> "$log_file"
    echo "" >> "$log_file"

    if uv run --locked "$PYTHONPATH/tests/runtime_aggtest/$test_name" >> "$log_file" 2>&1; then
        echo "✅ PASSED: $test_name"
        echo "" >> "$log_file"
        echo "Completed at: $(date)" >> "$log_file"
        echo "Status: PASSED" >> "$log_file"

        # Show output for successful tests if requested
        if [[ "$VERBOSE" == "true" ]]; then
            echo "--- Output from successful test: $test_name ---"
            cat "$log_file"
            echo "--- End of output for: $test_name ---"
            echo ""
        fi
    else
        local exit_code=$?
        echo "❌ FAILED: $test_name" >&2
        echo "" >> "$log_file"
        echo "Completed at: $(date)" >> "$log_file"
        echo "Status: FAILED (exit code: $exit_code)" >> "$log_file"
        return $exit_code
    fi
}

# Function to wait for any job to complete and clean up
wait_for_job() {
    if [[ ${#job_pids[@]} -eq 0 ]]; then
        return 0
    fi

    # Wait for any background job to complete
    local completed_pid
    completed_pid=$(wait -n "${job_pids[@]}" 2>/dev/null; echo $?)
    local exit_code=$?

    # Find which job completed by checking process status
    for i in "${!job_pids[@]}"; do
        if ! kill -0 "${job_pids[$i]}" 2>/dev/null; then
            local completed_test="${job_names[$i]}"
            local completed_log="${job_log_files[$i]}"

            if [[ $exit_code -ne 0 ]]; then
                failed_tests+=("$completed_test")
                echo "❌ Test failed: $completed_test" >&2
                echo "--- Output from failed test: $completed_test ---" >&2
                cat "$completed_log" >&2
                echo "--- End of output for: $completed_test ---" >&2
                echo "" >&2
            fi

            # Remove completed job from tracking arrays
            unset job_pids["$i"]
            unset job_names["$i"]
            unset job_log_files["$i"]

            # Repack arrays to remove gaps
            job_pids=("${job_pids[@]}")
            job_names=("${job_names[@]}")
            job_log_files=("${job_log_files[@]}")
            break
        fi
    done
}

# Function to wait for all remaining jobs
wait_all_jobs() {
    while [[ ${#job_pids[@]} -gt 0 ]]; do
        wait_for_job
    done
}

# Cleanup function for proper signal handling
cleanup() {
    echo "Cleaning up background processes..."
    for pid in "${job_pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    wait_all_jobs

    # Clean up temporary files
    if [[ -d "$LOG_DIR" ]]; then
        rm -rf "$LOG_DIR"
    fi
}

# Set up signal handlers for clean shutdown
trap cleanup EXIT INT TERM

echo "Running ${#TESTS[@]} runtime_aggtest modules with max $MAX_JOBS parallel jobs"

# Run all tests with controlled parallelism
for test in "${TESTS[@]}"; do
    # Wait if we've reached the maximum number of parallel jobs
    while [[ ${#job_pids[@]} -ge $MAX_JOBS ]]; do
        wait_for_job
    done

    # Create unique log file for this test
    local test_safe_name="${test//\//_}"  # Replace slashes with underscores
    local log_file="${OUTPUT_PREFIX}_${test_safe_name}.log"

    # Start the test in background
    runtest "$test" "$log_file" &
    local pid=$!
    job_pids+=("$pid")
    job_names+=("$test")
    job_log_files+=("$log_file")
done

# Wait for all remaining jobs to complete
wait_all_jobs

# Report results
if [[ ${#failed_tests[@]} -eq 0 ]]; then
    echo "✅ All ${#TESTS[@]} runtime_aggtest modules passed"
    echo "📋 Test logs are available in: $LOG_DIR"
    exit 0
else
    echo "❌ ${#failed_tests[@]} test(s) failed:" >&2
    printf '  %s\n' "${failed_tests[@]}" >&2
    echo "" >&2
    echo "📋 Full test logs are available in: $LOG_DIR" >&2
    echo "💡 To view a specific test log: cat $LOG_DIR/test_output_<test_name>.log" >&2
    exit 1
fi
