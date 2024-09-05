#!/bin/bash

set -e

echo "running tpc-h/generate.bash"
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
pushd ${THIS_DIR}

rewrite_csv() {
    for i in `ls *.csv`; do
        csvformat -d"|" $i >x && mv x ${i}
    done
}

if [[ ! -d "data-large" ]]; then
    git clone --quiet https://github.com/dbtoaster/dbtoaster-experiments-data.git
    mv dbtoaster-experiments-data/tpch/big data-large
    cd data-large
    rewrite_csv
    cd ..
    mv dbtoaster-experiments-data/tpch/standard data-medium
    cd data-medium
    rewrite_csv
    cd ..
    rm -rf dbtoaster-experiments-data
fi

popd
