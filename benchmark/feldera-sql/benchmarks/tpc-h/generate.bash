#!/bin/sh -e

echo "running tpc-h/generate.bash"
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${THIS_DIR}"

if test ! -d "data-large"; then
    git clone --quiet https://github.com/dbtoaster/dbtoaster-experiments-data.git
    mv dbtoaster-experiments-data/tpch/big data-large
    mv dbtoaster-experiments-data/tpch/standard data-medium
    rm -rf dbtoaster-experiments-data
fi
