#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    echo "This script builds the sql-to-dbsp compiler"
    echo "known options: -n -c"
    echo "-c use the current released version of Calcite"
    echo "-n use the next (unreleased) version of Calcite"
    exit 1
}

# Parse command line arguments to pass to build_calcite.sh
CALCITE_ARGS=""
while getopts "nch" opt; do
    case $opt in
        n)
            CALCITE_ARGS="-n"
            ;;
        c)
            CALCITE_ARGS="-c"
            ;;
        h)
            usage
            ;;
        *)
            usage
            ;;
    esac
done

# Shift past the options we processed
shift $((OPTIND-1))

# Build Calcite first
"${SCRIPT_DIR}/build_calcite.sh" ${CALCITE_ARGS}

# Build the SQL compiler with any remaining arguments
mvn package -DskipTests --no-transfer-progress -DargLine="-ea" -q -B "$@"
