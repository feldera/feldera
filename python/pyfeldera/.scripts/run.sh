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
    echo "Targets: extract | collect | build-wheel | build-image | test | dbt-test | upload | fabric-test | all"
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
    # Re-precompile with --dbsp-override-path=/tmp/feldera so that the
    # cache fingerprints use a universally writable path.  /tmp is always
    # writable, unlike /home/ubuntu which requires root on Fabric VMs.
    # We use a REAL copy (not symlink) because Cargo resolves symlinks
    # and would fingerprint the target path instead of /tmp/feldera.
    echo "    Rebasing precompile cache to /tmp/feldera ..."
    docker exec "${cid}" bash -c "
        cp -a /home/ubuntu/feldera /tmp/feldera
        /tmp/feldera/build/pipeline-manager \
            --precompile \
            --dbsp-override-path=/tmp/feldera \
            --compilation-cargo-lock-path=/tmp/feldera/Cargo.lock \
            --sql-compiler-path=/tmp/feldera/build/sql2dbsp-jar-with-dependencies.jar \
            --compiler-working-directory=/home/ubuntu/.feldera/compiler \
            --runner-working-directory=/home/ubuntu/.feldera/local-runner
    "
    docker exec "${cid}" bash -c "rm -rf /home/ubuntu/.feldera/compiler/rust-compilation/target/debug/incremental"
    docker exec "${cid}" tar -cf /tmp/feldera-cache.tar -C /home/ubuntu .feldera/
    docker cp "${cid}:/tmp/feldera-cache.tar" /tmp/feldera-cache.tar
    tar -xf /tmp/feldera-cache.tar -C "${DATA_DIR}/cache/"
    rm /tmp/feldera-cache.tar

    echo "  [7/8] Cargo registry (~726 MB)..."
    docker exec "${cid}" tar -czf /tmp/cargo-reg.tar.gz -C /home/ubuntu .cargo/registry/
    docker cp "${cid}:/tmp/cargo-reg.tar.gz" /tmp/cargo-reg.tar.gz
    tar -xzf /tmp/cargo-reg.tar.gz -C "${DATA_DIR}/cache/"
    rm /tmp/cargo-reg.tar.gz

    echo "  [8/9] glibc 2.39 compat libs..."
    rm -rf "${DATA_DIR}/toolchain/glibc"
    mkdir -p "${DATA_DIR}/toolchain/glibc"
    docker exec "${cid}" tar -chf /tmp/glibc.tar \
        /lib/x86_64-linux-gnu/libc.so.6 \
        /lib/x86_64-linux-gnu/libm.so.6 \
        /lib/x86_64-linux-gnu/libgcc_s.so.1 \
        /lib/x86_64-linux-gnu/libssl.so.3 \
        /lib/x86_64-linux-gnu/libcrypto.so.3 \
        /lib64/ld-linux-x86-64.so.2
    docker cp "${cid}:/tmp/glibc.tar" /tmp/glibc.tar
    tar -xf /tmp/glibc.tar -C "${DATA_DIR}/toolchain/glibc/"
    rm /tmp/glibc.tar

    echo "  [9/10] libclang + LLVM libs (for bindgen during pipeline compilation)..."
    rm -rf "${DATA_DIR}/toolchain/libclang"
    mkdir -p "${DATA_DIR}/toolchain/libclang"
    docker exec "${cid}" tar -chf /tmp/libclang.tar \
        /usr/lib/x86_64-linux-gnu/libclang-18.so.18 \
        /usr/lib/llvm-18/lib/libLLVM.so.1 \
        /lib/x86_64-linux-gnu/libffi.so.8 \
        /lib/x86_64-linux-gnu/libedit.so.2 \
        /lib/x86_64-linux-gnu/libzstd.so.1 \
        /lib/x86_64-linux-gnu/libtinfo.so.6 \
        /lib/x86_64-linux-gnu/libxml2.so.2 \
        /lib/x86_64-linux-gnu/libbsd.so.0 \
        /lib/x86_64-linux-gnu/libicuuc.so.74 \
        /lib/x86_64-linux-gnu/libicudata.so.74 \
        /lib/x86_64-linux-gnu/liblzma.so.5 \
        /lib/x86_64-linux-gnu/libmd.so.0 \
        /usr/lib/llvm-18/lib/clang/18/include
    docker cp "${cid}:/tmp/libclang.tar" /tmp/libclang.tar
    # Flatten .so files into libclang dir, copy include dir separately
    local tmpex="/tmp/libclang-extract"
    rm -rf "${tmpex}"
    mkdir -p "${tmpex}"
    tar -xf /tmp/libclang.tar -C "${tmpex}"
    find "${tmpex}" -name '*.so*' -type f -exec cp {} "${DATA_DIR}/toolchain/libclang/" \;
    # Copy clang resource include directory
    mkdir -p "${DATA_DIR}/toolchain/libclang/clang/18"
    cp -a "${tmpex}/usr/lib/llvm-18/lib/clang/18/include" "${DATA_DIR}/toolchain/libclang/clang/18/"
    # Create symlinks that bindgen looks for
    cd "${DATA_DIR}/toolchain/libclang"
    ln -sf libclang-18.so.18 libclang.so
    ln -sf libLLVM.so.1 libLLVM.so.18.1
    cd "${PROJECT_DIR}"
    rm -rf "${tmpex}" /tmp/libclang.tar

    echo "  [10/10] Native dev headers + pkg-config (sasl2, ssl, etc.)..."
    rm -rf "${DATA_DIR}/toolchain/sysroot"
    mkdir -p "${DATA_DIR}/toolchain/sysroot/include" \
             "${DATA_DIR}/toolchain/sysroot/lib" \
             "${DATA_DIR}/toolchain/sysroot/lib/pkgconfig"
    # sasl2 headers + pkg-config + linker stub
    docker exec "${cid}" tar -chf /tmp/sysroot.tar \
        /usr/include/sasl \
        /usr/lib/x86_64-linux-gnu/pkgconfig/libsasl2.pc \
        /usr/lib/x86_64-linux-gnu/libsasl2.so \
        /usr/lib/x86_64-linux-gnu/libsasl2.a \
        /usr/include/openssl \
        /usr/include/x86_64-linux-gnu/openssl \
        /usr/lib/x86_64-linux-gnu/pkgconfig/libssl.pc \
        /usr/lib/x86_64-linux-gnu/pkgconfig/libcrypto.pc \
        /usr/lib/x86_64-linux-gnu/pkgconfig/openssl.pc
    docker cp "${cid}:/tmp/sysroot.tar" /tmp/sysroot.tar
    local sysex="/tmp/sysroot-extract"
    rm -rf "${sysex}"
    mkdir -p "${sysex}"
    tar -xf /tmp/sysroot.tar -C "${sysex}"
    cp -a "${sysex}/usr/include/"* "${DATA_DIR}/toolchain/sysroot/include/"
    # Arch-specific headers (opensslconf.h, configuration.h live here on Debian)
    if [ -d "${sysex}/usr/include/x86_64-linux-gnu/openssl" ]; then
        cp -a "${sysex}/usr/include/x86_64-linux-gnu/openssl/"* \
            "${DATA_DIR}/toolchain/sysroot/include/openssl/"
    fi
    find "${sysex}" -name '*.pc' -exec cp {} "${DATA_DIR}/toolchain/sysroot/lib/pkgconfig/" \;
    find "${sysex}" -name '*.so' -o -name '*.a' | while read f; do
        cp -a "$f" "${DATA_DIR}/toolchain/sysroot/lib/"
    done
    # Create versioned copies matching SONAME (tar -h loses the symlink
    # chain, and wheel packaging does not preserve symlinks).  The
    # dynamic linker looks up the SONAME, not the unversioned name.
    for sofile in "${DATA_DIR}/toolchain/sysroot/lib/"lib*.so; do
        [ -f "$sofile" ] || continue
        soname=$(readelf -d "$sofile" 2>/dev/null \
                 | grep SONAME | sed 's/.*\[\(.*\)\]/\1/')
        if [ -n "$soname" ] && [ "$soname" != "$(basename "$sofile")" ]; then
            cp "$sofile" "${DATA_DIR}/toolchain/sysroot/lib/${soname}"
            echo "    Created versioned copy: ${soname}"
        fi
    done
    # Fix paths in .pc files — use simple placeholders replaced at deploy time
    for pc_file in "${DATA_DIR}/toolchain/sysroot/lib/pkgconfig/"*.pc; do
        sed -i \
            -e "s|prefix=.*|prefix=SYSROOT_PREFIX|" \
            -e "s|exec_prefix=.*|exec_prefix=SYSROOT_PREFIX|" \
            -e "s|libdir=.*|libdir=SYSROOT_LIB|" \
            -e "s|includedir=.*|includedir=SYSROOT_INCLUDE|" \
            "$pc_file"
    done
    rm -rf "${sysex}" /tmp/sysroot.tar

    docker stop "${cid}" >/dev/null && docker rm "${cid}" >/dev/null
    echo "  Sizes:"
    for d in "${DATA_DIR}/bin" "${DATA_DIR}/build" \
             "${DATA_DIR}/toolchain/java" "${DATA_DIR}/toolchain/rustup" \
             "${DATA_DIR}/toolchain/cargo-bin" "${DATA_DIR}/toolchain/mold" \
             "${DATA_DIR}/toolchain/glibc" "${DATA_DIR}/toolchain/libclang" \
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

    # Stamp a unique build ID so the server module can detect stale deploys
    # after ``pip install --force-reinstall``.
    date -u +"%Y%m%dT%H%M%SZ-$(head -c8 /dev/urandom | od -An -tx1 | tr -d ' \n')" \
        > "${DATA_DIR}/.build_id"

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

run_upload() {
    echo "=== Uploading pyfeldera wheel to Azure blob ==="
    ls "${DIST_DIR}"/*.whl &>/dev/null || run_build_wheel

    local whl
    whl=$(ls "${DIST_DIR}"/*.whl 2>/dev/null | head -1)
    [ -z "$whl" ] && { echo "ERROR: No wheel found"; exit 1; }

    local STORAGE_ACCOUNT="${STORAGE_ACCOUNT:-rakirahman}"
    local STORAGE_CONTAINER="${STORAGE_CONTAINER:-public}"
    local STORAGE_KEY="${STORAGE_KEY:-}"

    # Try .env file for storage key
    if [ -z "$STORAGE_KEY" ] && [ -f "${REPO_ROOT}/.vite/privy/.env" ]; then
        STORAGE_KEY=$(grep '^STORAGE_KEY=' "${REPO_ROOT}/.vite/privy/.env" | cut -d= -f2)
    fi
    [ -z "$STORAGE_KEY" ] && { echo "ERROR: STORAGE_KEY not set"; exit 1; }

    echo "  Uploading $(basename "$whl") ($(du -h "$whl" | cut -f1))..."
    az storage blob upload \
        --account-name "$STORAGE_ACCOUNT" \
        --account-key "$STORAGE_KEY" \
        --container-name "$STORAGE_CONTAINER" \
        --name "whls/$(basename "$whl")" \
        --file "$whl" \
        --overwrite \
        --no-progress

    echo "  URL: https://${STORAGE_ACCOUNT}.blob.core.windows.net/${STORAGE_CONTAINER}/whls/$(basename "$whl")"
}

run_fabric_test() {
    echo "=== Running dbt Fabric tests via Privy proxy ==="
    cd "${REPO_ROOT}/python/dbt-feldera"
    .scripts/run.sh fabric-test
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
    upload)      run_upload ;;
    fabric-test) run_fabric_test ;;
    all)         run_all ;;
    *)           echo "ERROR: Unknown target '${TARGET}'"; print_usage; exit 1 ;;
esac
