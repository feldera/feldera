#!/bin/bash
set -e

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DEFAULT_SQL_COMPILER_PATH="${THIS_DIR}/../sql-to-dbsp-compiler"
SQL_COMPILER="${1:-$DEFAULT_SQL_COMPILER_PATH}"

if [ ! -d "$SQL_COMPILER" ]; then
    echo "Could not find SQL compiler source (https://github.com/vmware/sql-to-dbsp-compiler) at path $SQL_COMPILER."
    exit 1
fi

docker build -f "${THIS_DIR}/Dockerfile" -t dbspmanager ${THIS_DIR}/..
