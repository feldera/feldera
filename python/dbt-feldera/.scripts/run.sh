#!/bin/bash
#
# dbt-feldera dev/test script. Requires Python 3.10+, uv, Docker (for integration/e2e).
#
# Usage: .scripts/run.sh <target>
#
# Targets: venv | build | lint | unit-test | integration-test | e2e | all
#
# Each target is idempotent — auto-creates the venv if missing.
# Set FELDERA_SKIP_DOCKER=1 / FELDERA_URL to control Docker behavior.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${PROJECT_DIR}/.venv"
DOCKER_COMPOSE_FILE="${PROJECT_DIR}/integration_tests/docker-compose.yml"
DOCKER_PROJECT="feldera-dbt-test"

declare -A TARGETS=(
    ["venv"]="Fresh venv + install deps"
    ["build"]="Build wheel to dist/"
    ["fix"]="ruff auto-fix + format"
    ["lint"]="ruff check + format"
    ["unit-test"]="pytest unit tests"
    ["seed-ci"]="Download seed data from GitHub Gist"
    ["integration-test"]="pytest integration with Feldera in Docker"
    ["e2e"]="dbt CLI end-to-end with Feldera in Docker"
    ["all"]="Run all targets in sequence"
)
TARGET_ORDER=("venv" "build" "fix" "lint" "unit-test" "seed-ci" "integration-test" "e2e")

print_usage() {
    echo
    printf "  %-20s %s\n" "TARGET" "DESCRIPTION"
    printf "  %-20s %s\n" "all" "${TARGETS[all]}"
    for t in "${TARGET_ORDER[@]}"; do printf "  %-20s %s\n" "$t" "${TARGETS[$t]}"; done
    echo
    echo "Usage: .scripts/run.sh <target>"
    echo
}

TARGET="${1:-}"
if [[ -z "$TARGET" ]]; then echo "ERROR: No target provided."; print_usage; exit 1; fi
if [[ "$TARGET" != "all" && -z "${TARGETS[$TARGET]:-}" ]]; then echo "ERROR: Unknown target '$TARGET'"; print_usage; exit 1; fi

activate_venv() {
    # shellcheck disable=SC1091
    source "${VENV_DIR}/bin/activate"
}

create_venv() {
    rm -rf "${VENV_DIR}"
    cd "${PROJECT_DIR}"
    uv venv "${VENV_DIR}"
    activate_venv
    uv sync --all-extras
}

ensure_venv() {
    if [[ ! -d "${VENV_DIR}" ]]; then create_venv; else activate_venv; fi
}

run_venv()             { create_venv; }
run_build()            { ensure_venv; cd "${PROJECT_DIR}"; rm -rf dist/; uv build --wheel 2>&1 | tail -5; echo "  Built: $(ls dist/*.whl)"; }
run_fix()              { ensure_venv; cd "${PROJECT_DIR}"; uv run ruff check --fix dbt/ tests/; uv run ruff format dbt/ tests/; }
run_lint()             { ensure_venv; cd "${PROJECT_DIR}"; uv run ruff check dbt/ tests/; uv run ruff format --check dbt/ tests/; }
run_unit_test()        { ensure_venv; cd "${PROJECT_DIR}"; uv run pytest tests/ -vv; }

run_seed_ci() {
    ensure_venv
    cd "${PROJECT_DIR}"
    echo "Downloading seed data from GitHub Gist..."
    uv run python integration_tests/scripts/download_seeds.py
}

start_feldera() {
    docker compose -f "${DOCKER_COMPOSE_FILE}" \
        -p "${DOCKER_PROJECT}" up -d --wait --wait-timeout 300
}

resolve_feldera_url() {
    local base="${FELDERA_URL:-http://localhost:8080}"
    if curl -sf --connect-timeout 3 "${base}/healthz" >/dev/null 2>&1; then
        echo "$base"
        return
    fi
    if [[ "$base" == *localhost* || "$base" == *127.0.0.1* ]]; then
        local alt="${base//localhost/host.docker.internal}"
        alt="${alt//127.0.0.1/host.docker.internal}"
        if curl -sf --connect-timeout 3 "${alt}/healthz" >/dev/null 2>&1; then
            echo "$alt"
            return
        fi
    fi
    echo "$base"
}

wait_for_feldera() {
    local base="${FELDERA_URL:-http://localhost:8080}"
    local alt=""
    if [[ "$base" == *localhost* || "$base" == *127.0.0.1* ]]; then
        alt="${base//localhost/host.docker.internal}"
        alt="${alt//127.0.0.1/host.docker.internal}"
    fi

    for i in $(seq 1 60); do
        if curl -sf --connect-timeout 3 "${base}/healthz" >/dev/null 2>&1; then
            export FELDERA_URL="$base"
            echo "Feldera is healthy at ${base}"
            return 0
        fi
        if [[ -n "$alt" ]] && curl -sf --connect-timeout 3 "${alt}/healthz" >/dev/null 2>&1; then
            export FELDERA_URL="$alt"
            echo "Feldera is healthy at ${alt} (Docker-in-Docker fallback)"
            return 0
        fi
        echo "Waiting for Feldera... ($i/60)"
        sleep 5
    done
    echo "ERROR: Feldera did not become healthy in time"
    return 1
}

feldera_logs() {
    docker compose -f "${DOCKER_COMPOSE_FILE}" \
        -p "${DOCKER_PROJECT}" logs --tail=200
}

stop_feldera() {
    docker compose -f "${DOCKER_COMPOSE_FILE}" \
        -p "${DOCKER_PROJECT}" down -v --remove-orphans
}

run_integration_test() {
    ensure_venv
    cd "${PROJECT_DIR}"

    run_seed_ci

    local skip_docker="${FELDERA_SKIP_DOCKER:-}"

    if [[ -z "$skip_docker" ]]; then
        echo "Starting Feldera via Docker..."
        start_feldera
        wait_for_feldera
    else
        export FELDERA_URL="$(resolve_feldera_url)"
    fi

    echo "Using FELDERA_URL=${FELDERA_URL}"

    local rc=0
    uv run pytest integration_tests/test_dbt_feldera.py -vv --timeout=600 -m integration || rc=$?

    if [[ -z "$skip_docker" ]]; then
        if [[ $rc -ne 0 ]]; then
            echo ""; echo "── Feldera logs (last 200 lines) ──"
            feldera_logs || true
        fi
        echo "Stopping Feldera..."
        stop_feldera || true
    fi

    return $rc
}

run_e2e() {
    ensure_venv
    cd "${PROJECT_DIR}"
    run_seed_ci
    bash integration_tests/scripts/run-dbt-local.sh
}

echo "=== dbt-feldera: ${TARGET} ==="

case "$TARGET" in
    "all")
        for t in "${TARGET_ORDER[@]}"; do
            echo ""; echo "── ${t} ──"
            "run_${t//-/_}"
        done
        echo ""; echo "=== All targets completed. ==="
        ;;
    *) "run_${TARGET//-/_}" ;;
esac
