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

_compose() {
    docker compose -f "${COMPOSE_FILE}" -p "${PROJECT_NAME}" "$@"
}

_teardown() {
    echo ""
    echo "[teardown] Stopping Feldera..."
    _compose down -v --remove-orphans 2>/dev/null || true
}

trap _teardown EXIT

echo "============================================"
echo " dbt-feldera end-to-end test"
echo " Target: ${TARGET}"
echo "============================================"

echo ""
echo "[0/5] Starting Feldera via Docker Compose..."
_teardown
_compose up -d --wait --wait-timeout 300
echo "  Feldera is ready."

FELDERA_URL="http://localhost:8080"
if ! curl -sf --connect-timeout 5 "${FELDERA_URL}/v0/config" >/dev/null 2>&1; then
    FELDERA_URL="http://host.docker.internal:8080"
fi
export FELDERA_URL
echo "  Using FELDERA_URL=${FELDERA_URL}"

echo ""
echo "[1/5] Building dbt-feldera wheel..."
cd "${ADAPTER_DIR}"
rm -rf dist/
uv build --wheel 2>&1 | tail -3
WHEEL=$(ls dist/*.whl | head -1)
echo "  Built: ${WHEEL}"

echo ""
echo "[2/5] Setting up virtual environment..."
rm -rf "${VENV_DIR}"
uv venv "${VENV_DIR}"

source "${VENV_DIR}/bin/activate"
uv pip install "${WHEEL}" dbt-core
echo "  Installed dbt-feldera"

cd "${PROJECT_DIR}"
export DBT_PROFILES_DIR="${PROJECT_DIR}"

echo ""
echo "[3/5] Running dbt debug..."
dbt debug --target "${TARGET}"

echo ""
echo "[4/5] Running dbt seed + build..."
dbt seed --target "${TARGET}" --full-refresh
dbt build --exclude resource_type:seed --target "${TARGET}"

echo ""
echo "[5/5] Generating docs..."
dbt docs generate --target "${TARGET}"

echo ""
echo "============================================"
echo " All dbt commands completed successfully."
echo "============================================"

deactivate
