#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT_DIR="${THIS_DIR}/.."
SQL_COMPILER_DIR="${ROOT_DIR}/sql-to-dbsp-compiler"
MANAGER_DIR="${ROOT_DIR}/pipeline_manager"

if [ "$#" -lt 1 ]; then
    echo "Usage '$0 <working_directory_path> <bind address (optional)>'"
    exit 1
fi

# This is the most portable way to get an absolute path since
# 'realpath' is not available on MacOS by default.
WORKING_DIR=$(cd "$(dirname "${1}")" && pwd -P)/$(basename "${1}")
DEFAULT_BIND_ADDRESS="127.0.0.1"
BIND_ADDRESS="${2:-$DEFAULT_BIND_ADDRESS}"

mkdir -p "${WORKING_DIR}"

# Kill manager. pkill doesn't handle process names >15 characters.
pkill -9 dbsp_pipeline_

set -e

# Start pipeline manager.

manager_config="
    port: 8080
    bind_address:  \"${BIND_ADDRESS}\"
    logfile: \"${WORKING_DIR}/manager.log\"
    working_directory: \"${WORKING_DIR}\"
    sql_compiler_home: \"${SQL_COMPILER_DIR}\"
    dbsp_override_path: \"${ROOT_DIR}\"
    with_prometheus: false
    debug: false"

printf "$manager_config" > "${WORKING_DIR}/manager.yaml"

cd "${MANAGER_DIR}" && ~/.cargo/bin/cargo build --release
cd "${MANAGER_DIR}" && ~/.cargo/bin/cargo run --release -- --static-html=static --config-file="${WORKING_DIR}/manager.yaml"
