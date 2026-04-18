#!/bin/bash
#
# pyfeldera build & test script.
#
# Usage: .scripts/run.sh <target>
#
# Targets:
#   extract       — Pull binary + JAR from the official Docker image
#   collect       — Copy Rust crate sources from the repo
#   build-wheel   — Build the pyfeldera wheel (runs extract + collect first)
#   build-image   — Build the customer Docker image
#   test          — Run basic pyfeldera Docker tests
#   dbt-test      — Run dbt-feldera integration tests with the customer image
#   all           — Run all targets in sequence
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_DIR}/../../.." && pwd)"
DATA_DIR="${PROJECT_DIR}/pyfeldera/data"
DIST_DIR="${PROJECT_DIR}/dist"
DOCKER_DIR="${PROJECT_DIR}/tests/docker"
CUSTOMER_IMAGE="pyfeldera-customer:latest"

FELDERA_IMAGE="${FELDERA_IMAGE:-images.feldera.com/feldera/pipeline-manager:latest}"

# ── Helpers ──────────────────────────────────────────────────────────

ensure_tools() {
    for tool in docker python3 pip; do
        if ! command -v "$tool" &>/dev/null; then
            echo "ERROR: $tool not found"; exit 1
        fi
    done
    pip install --quiet hatchling 2>/dev/null || true
}

print_usage() {
    echo
    printf "  %-15s %s\n" "TARGET" "DESCRIPTION"
    printf "  %-15s %s\n" "extract"     "Pull binary + JAR from Docker image"
    printf "  %-15s %s\n" "collect"     "Copy Rust crate sources from repo"
    printf "  %-15s %s\n" "build-wheel" "Build the pyfeldera wheel"
    printf "  %-15s %s\n" "build-image" "Build the customer Docker image"
    printf "  %-15s %s\n" "test"        "Run basic Docker tests"
    printf "  %-15s %s\n" "dbt-test"    "Run dbt integration tests with customer image"
    printf "  %-15s %s\n" "all"         "Run all targets"
    echo
}

# ── Targets ──────────────────────────────────────────────────────────

run_extract() {
    echo "=== Extracting binaries from Docker image ==="
    mkdir -p "${DATA_DIR}/bin" "${DATA_DIR}/build"

    # Create a temporary container (don't start it)
    local cid
    cid=$(docker create --entrypoint=bash "${FELDERA_IMAGE}" -c "true")

    echo "  Extracting pipeline-manager..."
    docker cp "${cid}:/home/ubuntu/feldera/build/pipeline-manager" \
        "${DATA_DIR}/bin/pipeline-manager"
    chmod +x "${DATA_DIR}/bin/pipeline-manager"

    echo "  Extracting sql2dbsp-jar-with-dependencies.jar..."
    docker cp "${cid}:/home/ubuntu/feldera/build/sql2dbsp-jar-with-dependencies.jar" \
        "${DATA_DIR}/build/sql2dbsp-jar-with-dependencies.jar"

    docker rm "${cid}" >/dev/null
    echo "  Done. Binary=$(du -h "${DATA_DIR}/bin/pipeline-manager" | cut -f1), JAR=$(du -h "${DATA_DIR}/build/sql2dbsp-jar-with-dependencies.jar" | cut -f1)"
}

run_collect() {
    echo "=== Collecting Rust crate sources from repo ==="

    # Cargo workspace files
    cp "${REPO_ROOT}/Cargo.toml" "${DATA_DIR}/Cargo.toml"
    cp "${REPO_ROOT}/Cargo.lock" "${DATA_DIR}/Cargo.lock"
    cp "${REPO_ROOT}/README.md"  "${DATA_DIR}/README.md"

    # All workspace member crates (excluding build artifacts)
    echo "  Copying crates/..."
    rm -rf "${DATA_DIR}/crates"
    mkdir -p "${DATA_DIR}/crates"
    cd "${REPO_ROOT}/crates"
    # Copy each crate dir, resolving symlinks so the data is self-contained
    for d in */; do
        if [ -d "$d" ]; then
            cp -aL "$d" "${DATA_DIR}/crates/" 2>/dev/null || \
            cp -a "$d" "${DATA_DIR}/crates/"
        fi
    done
    # Remove any target directories that got copied
    find "${DATA_DIR}/crates" -type d -name "target" -exec rm -rf {} + 2>/dev/null || true

    # SQL compiler runtime libraries
    echo "  Copying sql-to-dbsp-compiler/lib/..."
    rm -rf "${DATA_DIR}/sql-to-dbsp-compiler"
    mkdir -p "${DATA_DIR}/sql-to-dbsp-compiler"
    cp -a "${REPO_ROOT}/sql-to-dbsp-compiler/lib" "${DATA_DIR}/sql-to-dbsp-compiler/lib"

    # SQL compiler temp/stubs (needed for compilation workspace)
    echo "  Copying sql-to-dbsp-compiler/temp/..."
    cp -a "${REPO_ROOT}/sql-to-dbsp-compiler/temp" "${DATA_DIR}/sql-to-dbsp-compiler/temp"

    echo "  Done. Total data size: $(du -sh "${DATA_DIR}" | cut -f1)"
}

run_build_wheel() {
    echo "=== Building pyfeldera wheel ==="

    # Ensure data is populated
    if [ ! -f "${DATA_DIR}/bin/pipeline-manager" ]; then
        run_extract
    fi
    if [ ! -f "${DATA_DIR}/Cargo.toml" ]; then
        run_collect
    fi

    cd "${PROJECT_DIR}"
    rm -rf "${DIST_DIR}"

    pip wheel . --no-deps --wheel-dir="${DIST_DIR}" 2>&1 | tail -10

    local whl
    whl=$(ls "${DIST_DIR}"/*.whl 2>/dev/null | head -1)
    if [ -z "$whl" ]; then
        echo "ERROR: No wheel produced"
        exit 1
    fi

    echo "  Built: ${whl} ($(du -h "$whl" | cut -f1))"
}

run_build_image() {
    echo "=== Building customer Docker image ==="

    if ! ls "${DIST_DIR}"/*.whl &>/dev/null; then
        run_build_wheel
    fi

    # Copy wheel to Docker build context
    mkdir -p "${DOCKER_DIR}/dist"
    cp "${DIST_DIR}"/*.whl "${DOCKER_DIR}/dist/"

    docker build \
        -t "${CUSTOMER_IMAGE}" \
        -f "${DOCKER_DIR}/Dockerfile.customer" \
        "${DOCKER_DIR}"

    echo "  Built image: ${CUSTOMER_IMAGE}"
}

run_test() {
    echo "=== Running basic pyfeldera tests ==="

    if ! docker image inspect "${CUSTOMER_IMAGE}" &>/dev/null; then
        run_build_image
    fi

    cd "${DOCKER_DIR}"
    docker compose -p pyfeldera-test down -v --remove-orphans 2>/dev/null || true
    docker compose -p pyfeldera-test up -d --wait --wait-timeout 300

    local rc=0
    python3 -m pytest test_pyfeldera.py -vv --timeout=120 || rc=$?

    if [ $rc -ne 0 ]; then
        echo ""
        echo "── Docker logs ──"
        docker compose -p pyfeldera-test logs --tail=100 || true
    fi

    docker compose -p pyfeldera-test down -v --remove-orphans
    return $rc
}

run_dbt_test() {
    echo "=== Running dbt integration tests with customer image ==="

    if ! docker image inspect "${CUSTOMER_IMAGE}" &>/dev/null; then
        run_build_image
    fi

    local dbt_dir="${REPO_ROOT}/python/dbt-feldera"
    export FELDERA_IMAGE="${CUSTOMER_IMAGE}"
    cd "${dbt_dir}"
    .scripts/run.sh integration-test
}

run_all() {
    run_extract
    run_collect
    run_build_wheel
    run_build_image
    run_test
    run_dbt_test
}

# ── Main ─────────────────────────────────────────────────────────────

TARGET="${1:-}"
if [ -z "$TARGET" ]; then
    echo "ERROR: No target provided."
    print_usage
    exit 1
fi

ensure_tools

echo "=== pyfeldera: ${TARGET} ==="

case "$TARGET" in
    extract)     run_extract ;;
    collect)     run_collect ;;
    build-wheel) run_build_wheel ;;
    build-image) run_build_image ;;
    test)        run_test ;;
    dbt-test)    run_dbt_test ;;
    all)         run_all ;;
    *)
        echo "ERROR: Unknown target '${TARGET}'"
        print_usage
        exit 1
        ;;
esac
