#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT_DIR="${THIS_DIR}/.."
SQL_COMPILER_DIR="${ROOT_DIR}/sql-to-dbsp-compiler"
MANAGER_DIR="${ROOT_DIR}/crates/pipeline_manager"
if [[ -z "${RUST_BUILD_PROFILE+set}" ]]; then
    RUST_BUILD_PROFILE='--release'
fi

# if [ "$#" -lt 1 ]; then
#    echo "Usage '$0 <working_directory_path> <bind address (optional)>'"
#    exit 1
# fi

cd "${SQL_COMPILER_DIR}/SQL-compiler" && mvn -DskipTests package

WORKING_DIR="${1:-${HOME}/.dbsp}"

# This is the most portable way to get an absolute path since
# 'realpath' is not available on MacOS by default.
WORKING_DIR_ABS=$(cd "$(dirname "${WORKING_DIR}")" && pwd -P)/$(basename "${WORKING_DIR}")
DEFAULT_BIND_ADDRESS="127.0.0.1"
BIND_ADDRESS="${2:-$DEFAULT_BIND_ADDRESS}"

set -ex

# If $WITH_POSTGRES is defined, manager should use a real Postgres server
# instead of pg-embed.
if [ -z "$WITH_POSTGRES" ]; then
    PG_EMBED="--features pg-embed"
    DB_CONNECTION_STRING="--db-connection-string=postgres-embed"
else
    DB_CONNECTION_STRING="--db-connection-string=postgresql://${PGUSER}@localhost"
fi

manager_pid=$(pgrep "pipeline-mana" || echo "")

if [ -n "$manager_pid" ]; then
    echo "Previous manager instance is running"
    exit 1
fi

cd "${MANAGER_DIR}" && ~/.cargo/bin/cargo build $RUST_BUILD_PROFILE $PG_EMBED
cd "${MANAGER_DIR}" && ~/.cargo/bin/cargo run --bin pipeline-manager $RUST_BUILD_PROFILE $PG_EMBED -- \
    --bind-address="${BIND_ADDRESS}" \
    --api-server-working-directory="${WORKING_DIR_ABS}" \
    --compiler-working-directory="${WORKING_DIR_ABS}" \
    --runner-working-directory="${WORKING_DIR_ABS}" \
    --sql-compiler-home="${SQL_COMPILER_DIR}" \
    --dbsp-override-path="${ROOT_DIR}" \
    ${DB_CONNECTION_STRING}
