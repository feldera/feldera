#!/bin/sh -x

cd "$(dirname "$0")"

# Run Flink benchmark
flink_dir=../../gh-pages/flink/
mkdir $flink_dir
cd ../..	
earthly --verbose -P +flink-benchmark
cd benchmark/flink
mv ../../flink_results.csv $flink_dir