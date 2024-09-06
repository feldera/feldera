#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
pushd ${THIS_DIR}

if [ ! -d "data-large" ]; then
        git clone --quiet https://github.com/dbtoaster/dbtoaster-experiments-data.git
        mv dbtoaster-experiments-data/tpch/big data-large
        mv dbtoaster-experiments-data/tpch/standard data-medium
        rm -rf dbtoaster-experiments-data
fi

python3 ${THIS_DIR}/generate.py
popd
