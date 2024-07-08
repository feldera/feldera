#!/bin/sh -x
cd "$(dirname "$0")"
rm -rf nexmark nexmark-flink
git clone https://github.com/blp/nexmark.git
(cd nexmark/nexmark-flink && ./build.sh)
tar xzf nexmark/nexmark-flink/nexmark-flink.tgz