#!/usr/bin/env bash

set -e

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT_DIR="${THIS_DIR}/.."
SQL_COMPILER_DIR="${ROOT_DIR}/sql-to-dbsp-compiler"
MANAGER_DIR="${ROOT_DIR}/crates/pipeline-manager"
if [[ -z "${RUST_BUILD_PROFILE+set}" ]]; then
    RUST_BUILD_PROFILE='--release'
fi

# if [ "$#" -lt 1 ]; then
#    echo "Usage '$0 <working_directory_path> <bind address (optional)>'"
#    exit 1
# fi

cd "${SQL_COMPILER_DIR}" && ./build.sh

DEFAULT_BIND_ADDRESS="127.0.0.1"
BIND_ADDRESS="${2:-$DEFAULT_BIND_ADDRESS}"

set -ex

# If $WITH_POSTGRES is defined, manager should use a real Postgres server
# instead of embedded postgresql.
if [ -z "$WITH_POSTGRES" ]; then
    DB_CONNECTION_STRING="--db-connection-string=postgres-embed"
else
    DB_CONNECTION_STRING="--db-connection-string=postgresql://${PGUSER}@localhost"
fi

manager_pid=$(pgrep "pipeline-mana" || echo "")

if [ -n "$manager_pid" ]; then
    echo "Previous manager instance is running"
    exit 1
fi

cd "${MANAGER_DIR}" && ~/.cargo/bin/cargo build $RUST_BUILD_PROFILE
cd "${ROOT_DIR}" && ~/.cargo/bin/cargo run --bin pipeline-manager $RUST_BUILD_PROFILE $PG_EMBED -- \
    --bind-address="${BIND_ADDRESS}" \
    ${DB_CONNECTION_STRING}
