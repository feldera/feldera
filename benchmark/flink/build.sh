#! /bin/sh
set -e
test -d nexmark || git clone git@github.com:blp/nexmark.git
(cd nexmark/nexmark-flink && ./build.sh)
tar xzf nexmark/nexmark-flink/nexmark-flink.tgz
docker build -t nexmark .
