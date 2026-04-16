#!/usr/bin/env bash
#
# End-to-end test: build the dbt-feldera wheel and run a full dbt lifecycle
# against the adventureworks project on a Feldera Docker instance.
##
# Usage:
#   ./run-dbt-local.sh [target]
#
# Arguments:
#   target  - dbt target to use (default: local)
#
set -euo pipefail

TARGET="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/../dbt-adventureworks"
ADAPTER_DIR="${SCRIPT_DIR}/../.."
COMPOSE_FILE="${SCRIPT_DIR}/../docker-compose.yml"
PROJECT_NAME="feldera-dbt-test"
VENV_DIR="${SCRIPT_DIR}/.venv-dbt"
DBT_THREADS="${DBT_THREADS:-4}"
export DBT_THREADS

_compose() {
    docker compose -f "${COMPOSE_FILE}" -p "${PROJECT_NAME}" "$@"
}

_teardown() {
    echo ""
    echo "[teardown] Stopping Feldera..."
    _compose down -v --remove-orphans 2>/dev/null || true
}

if [[ "${SKIP_TEARDOWN:-}" != "1" ]]; then
    trap _teardown EXIT
fi

echo "============================================"
echo " dbt-feldera end-to-end test"
echo " Target: ${TARGET}"
echo "============================================"

echo ""
echo "[0/6] Downloading seed data from GitHub Gist..."
python3 "${SCRIPT_DIR}/download_seeds.py" "${PROJECT_DIR}"

echo ""
echo "[1/6] Starting Feldera via Docker Compose..."
_teardown
_compose up -d --wait --wait-timeout 300
echo "  Feldera is ready."

FELDERA_URL="http://localhost:8080"
if ! curl -sf --connect-timeout 5 "${FELDERA_URL}/v0/config" >/dev/null 2>&1; then
    FELDERA_URL="http://host.docker.internal:8080"
fi
export FELDERA_URL
echo "  Using FELDERA_URL=${FELDERA_URL}"
echo "  Using DBT_THREADS=${DBT_THREADS}"

echo ""
echo "[2/6] Building dbt-feldera wheel..."
cd "${ADAPTER_DIR}"
rm -rf dist/
uv build --wheel 2>&1 | tail -3
WHEEL=$(ls dist/*.whl | head -1)
echo "  Built: ${WHEEL}"

echo ""
echo "[3/6] Setting up virtual environment..."
rm -rf "${VENV_DIR}"
uv venv "${VENV_DIR}"

source "${VENV_DIR}/bin/activate"
uv pip install "${WHEEL}" dbt-core
echo "  Installed dbt-feldera"

cd "${PROJECT_DIR}"
export DBT_PROFILES_DIR="${PROJECT_DIR}"

echo ""
echo "[4/6] Running dbt debug to confirm connectivity..."
dbt debug --target "${TARGET}"

echo ""
echo "[5/6] Running dbt seed + build..."
dbt seed --target "${TARGET}" --full-refresh
dbt build --exclude resource_type:seed --target "${TARGET}"

echo ""
echo "[6/6] Generating docs..."
dbt docs generate --target "${TARGET}"

DBT_DOCS_PORT="${DBT_DOCS_PORT:-18081}"
FELDERA_HOST_PORT="${FELDERA_PORT:-8080}"

echo ""
echo "============================================"
echo " All dbt commands completed successfully."
echo "============================================"
echo ""
echo "  To browse the dbt documentation:"
echo ""
echo "    # 1. Create & activate a virtual environment"
echo "    uv venv /tmp/dbt-docs-venv"
echo "    source /tmp/dbt-docs-venv/bin/activate"
echo ""
echo "    # 2. Install dbt-feldera"
echo "    uv pip install dbt-core $(ls "${ADAPTER_DIR}"/dist/*.whl | head -1)"
echo ""
echo "    # 3. Serve the docs"
echo "    cd ${PROJECT_DIR}"
echo "    export DBT_PROFILES_DIR=${PROJECT_DIR}"
echo "    dbt docs serve --port ${DBT_DOCS_PORT}"
echo ""
echo "    # 4. Open in your browser"
echo "    #    http://localhost:${DBT_DOCS_PORT}"
echo ""

if [[ "${SKIP_TEARDOWN:-}" == "1" ]]; then
    echo "  ┌──────────────────────────────────────────┐"
    echo "  │  SKIP_TEARDOWN=1 — Feldera is still up   │"
    echo "  └──────────────────────────────────────────┘"
    echo ""
    echo "  ┌──────────┬──────────────────────────────────┐"
    echo "  │ UI       │ URL                              │"
    echo "  ├──────────┼──────────────────────────────────┤"
    printf "  │ Feldera  │ %-32s │\n" "http://localhost:${FELDERA_HOST_PORT}"
    printf "  │ dbt docs │ %-32s │\n" "http://localhost:${DBT_DOCS_PORT}"
    echo "  └──────────┴──────────────────────────────────┘"
    echo ""
    echo "  NOTE: If you are running inside WSL, make sure the ports"
    echo "  (${FELDERA_HOST_PORT}, ${DBT_DOCS_PORT}) are forwarded to your"
    echo "  Windows host. In VS Code this happens automatically via the"
    echo "  Ports tab."
    echo ""
    echo "  When done, tear down Feldera manually:"
    echo "    docker compose -f ${COMPOSE_FILE} -p ${PROJECT_NAME} down -v --remove-orphans"
    echo ""
fi

deactivate
