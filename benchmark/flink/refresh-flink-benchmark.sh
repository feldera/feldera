#!/bin/sh -x

# How old should Flink results be before they are re-run, in days
expiry_days=30

cd "$(dirname "$0")"

last_time=0
flink_dir=../../gh-pages/flink/
flink_last_run_file="${flink_dir}last_flink_run"

if [ -f $flink_last_run_file ]; then
	last_time=$(cat $flink_last_run_file) 
else
	echo "Could not find previous Flink results. They will be recomputed."
fi

time=$(date +%s)
diff=$((time - last_time))
diff_days=$((diff / (60 * 60 * 24)))

echo "Flink benchmark results were last computed $diff_days days ago. Results expire after $expiry_days days."
if [ $diff_days -gt $expiry_days ]; then
	echo "Flink results have expired, re-running Flink benchmarks!"
	mkdir $flink_dir
	cd ../..	
	earthly --verbose -P +flink-benchmark
	cd benchmark/flink
	mv ../../flink_results.csv $flink_dir
	echo $time > $flink_last_run_file
else
	echo "Flink results were recently computed, skipping Flink benchmarks."
fi