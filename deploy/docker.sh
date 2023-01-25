#!/bin/bash
set -e

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DEFAULT_SQL_COMPILER_PATH="${THIS_DIR}/../../sql-to-dbsp-compiler"
SQL_COMPILER="${1:-$DEFAULT_SQL_COMPILER_PATH}"
DBSP_PATH="${THIS_DIR}/../"

(cd ${DBSP_PATH} && (git ls-files | tar -cvf ${THIS_DIR}/dbsp_files.tar -T -))
(cd ${SQL_COMPILER} && (git ls-files | tar -cvf ${THIS_DIR}/sql_compiler_files.tar -T -))

docker build -f "${THIS_DIR}/Dockerfile" -t dbspmanager ${THIS_DIR}

rm ${THIS_DIR}/dbsp_files.tar
rm ${THIS_DIR}/sql_compiler_files.tar
