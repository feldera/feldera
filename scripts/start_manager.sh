#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT_DIR="${THIS_DIR}/.."
SQL_COMPILER_DIR="${ROOT_DIR}/sql-to-dbsp-compiler"
MANAGER_DIR="${ROOT_DIR}/crates/pipeline_manager"

# if [ "$#" -lt 1 ]; then
#    echo "Usage '$0 <working_directory_path> <bind address (optional)>'"
#    exit 1
# fi

if [[ ! -e ${SQL_COMPILER_DIR}/.git || -z $(cd ${SQL_COMPILER_DIR} && git branch --show-current) ]]
then
    git submodule update --init
fi
cd "${SQL_COMPILER_DIR}/SQL-compiler" && mvn -DskipTests package


WORKING_DIR="${1:-${HOME}/.dbsp}"

# This is the most portable way to get an absolute path since
# 'realpath' is not available on MacOS by default.
WORKING_DIR_ABS=$(cd "$(dirname "${WORKING_DIR}")" && pwd -P)/$(basename "${WORKING_DIR}")
DEFAULT_BIND_ADDRESS="127.0.0.1"
BIND_ADDRESS="${2:-$DEFAULT_BIND_ADDRESS}"

# Kill manager. pkill doesn't handle process names >15 characters.
pkill -9 dbsp_pipeline_

set -e

cd "${MANAGER_DIR}" && ~/.cargo/bin/cargo build --release
cd "${MANAGER_DIR}" && ~/.cargo/bin/cargo run --release -- \
    --bind-address="${BIND_ADDRESS}" \
    --working-directory="${WORKING_DIR_ABS}" \
    --sql-compiler-home="${SQL_COMPILER_DIR}" \
    --dbsp-override-path="${ROOT_DIR}" \
    --static-html=static \
    --unix-daemon
