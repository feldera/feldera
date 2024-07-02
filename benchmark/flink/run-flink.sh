#!/bin/sh -x
cd "$(dirname "$0")"
rm -rf nexmark nexmark-flink
git clone git@github.com:blp/nexmark.git
(cd nexmark/nexmark-flink && ./build.sh)
tar xzf nexmark/nexmark-flink/nexmark-flink.tgz
cd ..
./run-nexmark.sh --events 100000000 -r flink -o flink-results.csv
