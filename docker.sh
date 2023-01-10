#!/bin/bash
set -e

CURRENT_PATH=`pwd`
DEFAULT_SQL_COMPILER_PATH="${CURRENT_PATH}/../sql-to-dbsp-compiler"
SQL_COMPILER="${1:-$DEFAULT_SQL_COMPILER_PATH}"

(git ls-files | tar -cvf $CURRENT_PATH/dbsp_files.tar -T -)
(cd $SQL_COMPILER && (git ls-files | tar -cvf $CURRENT_PATH/sql_compiler_files.tar -T -))

docker build -f deploy/Dockerfile -t dbspmanager .

rm $CURRENT_PATH/dbsp_files.tar
rm $CURRENT_PATH/sql_compiler_files.tar
