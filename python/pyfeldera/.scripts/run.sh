#!/bin/bash
#
# pyfeldera build & test script.
#
# Usage: .scripts/run.sh <target>
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_DIR}/../.." && pwd)"
DATA_DIR="${PROJECT_DIR}/pyfeldera/data"
DIST_DIR="${PROJECT_DIR}/dist"
DOCKER_DIR="${PROJECT_DIR}/tests/docker"
CUSTOMER_IMAGE="pyfeldera-customer:latest"

FELDERA_IMAGE="${FELDERA_IMAGE:-images.feldera.com/feldera/pipeline-manager:latest}"

ensure_tools() {
    for tool in docker python3 pip; do
        command -v "$tool" &>/dev/null || { echo "ERROR: $tool not found"; exit 1; }
    done
    pip install --quiet hatchling 2>/dev/null || true
}

print_usage() {
    echo "Targets: extract | collect | build-wheel | build-image | test | dbt-test | all"
}

run_extract() {
    echo "=== Extracting everything from Docker image ==="
    mkdir -p "${DATA_DIR}/bin" "${DATA_DIR}/build" "${DATA_DIR}/toolchain"

    local cid
    cid=$(docker run -d --entrypoint bash "${FELDERA_IMAGE}" -c "sleep 300")

    echo "  [1/7] pipeline-manager binary..."
    docker cp "${cid}:/home/ubuntu/feldera/build/pipeline-manager" "${DATA_DIR}/bin/pipeline-manager"
    chmod +x "${DATA_DIR}/bin/pipeline-manager"

    echo "  [2/7] SQL compiler JAR..."
    docker cp "${cid}:/home/ubuntu/feldera/build/sql2dbsp-jar-with-dependencies.jar" \
        "${DATA_DIR}/build/sql2dbsp-jar-with-dependencies.jar"

    echo "  [3/7] Java 21 JRE (tar to resolve symlinks)..."
    rm -rf "${DATA_DIR}/toolchain/java"
    mkdir -p "${DATA_DIR}/toolchain"
    docker exec "${cid}" tar -chf /tmp/java21.tar -C /usr/lib/jvm/java-21-openjdk-amd64 .
    docker cp "${cid}:/tmp/java21.tar" /tmp/java21.tar
    mkdir -p "${DATA_DIR}/toolchain/java"
    tar -xf /tmp/java21.tar -C "${DATA_DIR}/toolchain/java/"
    rm /tmp/java21.tar

    echo "  [4/7] Rust toolchain..."
    rm -rf "${DATA_DIR}/toolchain/rustup" "${DATA_DIR}/toolchain/cargo-bin"
    docker cp "${cid}:/home/ubuntu/.rustup/toolchains/1.93.1-x86_64-unknown-linux-gnu" \
        "${DATA_DIR}/toolchain/rustup"
    mkdir -p "${DATA_DIR}/toolchain/cargo-bin"
    docker cp "${cid}:/home/ubuntu/.cargo/bin/rustup" "${DATA_DIR}/toolchain/cargo-bin/rustup"
    chmod +x "${DATA_DIR}/toolchain/cargo-bin/rustup"
    cd "${DATA_DIR}/toolchain/cargo-bin"
    for tool in cargo rustc rustdoc cargo-clippy cargo-fmt clippy-driver rustfmt; do
        ln -sf rustup "$tool"
    done
    cd "${PROJECT_DIR}"

    echo "  [5/7] mold linker..."
    rm -rf "${DATA_DIR}/toolchain/mold"
    docker cp "${cid}:/home/ubuntu/mold" "${DATA_DIR}/toolchain/mold"

    echo "  [6/7] Precompile cache (~4 GB)..."
    rm -rf "${DATA_DIR}/cache"
    mkdir -p "${DATA_DIR}/cache"
    docker exec "${cid}" tar -cf /tmp/feldera-cache.tar -C /home/ubuntu .feldera/
    docker cp "${cid}:/tmp/feldera-cache.tar" /tmp/feldera-cache.tar
    tar -xf /tmp/feldera-cache.tar -C "${DATA_DIR}/cache/"
    rm /tmp/feldera-cache.tar

    echo "  [7/7] Cargo registry (~726 MB)..."
    docker exec "${cid}" tar -czf /tmp/cargo-reg.tar.gz -C /home/ubuntu .cargo/registry/
    docker cp "${cid}:/tmp/cargo-reg.tar.gz" /tmp/cargo-reg.tar.gz
    tar -xzf /tmp/cargo-reg.tar.gz -C "${DATA_DIR}/cache/"
    rm /tmp/cargo-reg.tar.gz

    docker stop "${cid}" >/dev/null && docker rm "${cid}" >/dev/null
    echo "  Sizes:"
    for d in "${DATA_DIR}/bin" "${DATA_DIR}/build" \
             "${DATA_DIR}/toolchain/java" "${DATA_DIR}/toolchain/rustup" \
             "${DATA_DIR}/toolchain/cargo-bin" "${DATA_DIR}/toolchain/mold" \
             "${DATA_DIR}/cache"; do
        du -sh "$d" | sed 's/^/    /'
    done
}

run_collect() {
    echo "=== Collecting Rust crate sources from repo ==="

    cp "${REPO_ROOT}/Cargo.toml" "${DATA_DIR}/Cargo.toml"
    cp "${REPO_ROOT}/Cargo.lock" "${DATA_DIR}/Cargo.lock"
    cp "${REPO_ROOT}/README.md"  "${DATA_DIR}/README.md"

    echo "  Copying crates/..."
    rm -rf "${DATA_DIR}/crates"
    mkdir -p "${DATA_DIR}/crates"
    cd "${REPO_ROOT}/crates"
    for d in */; do
        [ -d "$d" ] && { cp -aL "$d" "${DATA_DIR}/crates/" 2>/dev/null || cp -a "$d" "${DATA_DIR}/crates/"; }
    done
    find "${DATA_DIR}/crates" -type d -name "target" -exec rm -rf {} + 2>/dev/null || true

    echo "  Copying sql-to-dbsp-compiler/{lib,temp}/..."
    rm -rf "${DATA_DIR}/sql-to-dbsp-compiler"
    mkdir -p "${DATA_DIR}/sql-to-dbsp-compiler"
    cp -a "${REPO_ROOT}/sql-to-dbsp-compiler/lib" "${DATA_DIR}/sql-to-dbsp-compiler/lib"
    cp -a "${REPO_ROOT}/sql-to-dbsp-compiler/temp" "${DATA_DIR}/sql-to-dbsp-compiler/temp"

    echo "  Done. Total data size: $(du -sh "${DATA_DIR}" | cut -f1)"
}

run_build_wheel() {
    echo "=== Building pyfeldera wheel ==="
    [ -f "${DATA_DIR}/bin/pipeline-manager" ] || run_extract
    [ -f "${DATA_DIR}/Cargo.toml" ] || run_collect

    cd "${PROJECT_DIR}"
    rm -rf "${DIST_DIR}"
    pip wheel . --no-deps --wheel-dir="${DIST_DIR}" 2>&1 | tail -10

    local whl
    whl=$(ls "${DIST_DIR}"/*.whl 2>/dev/null | head -1)
    [ -z "$whl" ] && { echo "ERROR: No wheel produced"; exit 1; }
    echo "  Built: ${whl} ($(du -h "$whl" | cut -f1))"
}

run_build_image() {
    echo "=== Building customer Docker image (Fabric simulation) ==="
    ls "${DIST_DIR}"/*.whl &>/dev/null || run_build_wheel

    mkdir -p "${DOCKER_DIR}/dist"
    cp "${DIST_DIR}"/*.whl "${DOCKER_DIR}/dist/"

    docker build -t "${CUSTOMER_IMAGE}" -f "${DOCKER_DIR}/Dockerfile.customer" "${DOCKER_DIR}"
    echo "  Built image: ${CUSTOMER_IMAGE}"
}

run_test() {
    echo "=== Running basic pyfeldera tests ==="
    docker image inspect "${CUSTOMER_IMAGE}" &>/dev/null || run_build_image

    cd "${DOCKER_DIR}"
    docker compose -p pyfeldera-test down -v --remove-orphans 2>/dev/null || true
    docker compose -p pyfeldera-test up -d --wait --wait-timeout 300

    local rc=0
    python3 -m pytest test_pyfeldera.py -vv --timeout=120 || rc=$?

    [ $rc -ne 0 ] && { echo "── Docker logs ──"; docker compose -p pyfeldera-test logs --tail=100 || true; }
    docker compose -p pyfeldera-test down -v --remove-orphans
    return $rc
}

run_dbt_test() {
    echo "=== Running dbt integration tests with customer image ==="
    docker image inspect "${CUSTOMER_IMAGE}" &>/dev/null || run_build_image

    export FELDERA_IMAGE="${CUSTOMER_IMAGE}"
    cd "${REPO_ROOT}/python/dbt-feldera"
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

TARGET="${1:-}"
[ -z "$TARGET" ] && { echo "ERROR: No target provided."; print_usage; exit 1; }
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
    *)           echo "ERROR: Unknown target '${TARGET}'"; print_usage; exit 1 ;;
esac
