#!/bin/sh -x
cd "$(dirname "$0")"
rm -rf nexmark nexmark-flink
git clone https://github.com/blp/nexmark.git
(cd nexmark/nexmark-flink && MVN='mvn --no-transfer-progress' ./build.sh)
tar xzf nexmark/nexmark-flink/nexmark-flink.tgz
