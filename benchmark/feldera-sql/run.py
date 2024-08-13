#! /usr/bin/python3

import csv
import json
import os
import sys
import time
import requests
import argparse

# File locations
FILE_DIR = os.path.join(os.path.dirname(__file__))

def load_queries(folder):
    queries = {}
    p = os.path.join(FILE_DIR, folder)
    for f in os.listdir(p):
        if f.endswith('.sql'):
            file = open(p + f, 'r')
            queries[f.split('.')[0]] = file.read()
    return queries

def load_table(folder, with_lateness, suffix, events, cores):
    p = os.path.join(FILE_DIR, folder + '/table.sql')
    file = open(p, 'r')
    text = file.read()
    table_start_string = 'create table '
    inputs = []
    for line in text.lower().split('\n'):
        i = line.find(table_start_string)
        if i >= 0:
            inputs += [line[i + len(table_start_string):].split(' ')[0]]
    subst = {input: make_connector(input, suffix) for input in inputs}
    subst["lateness"] = "LATENESS INTERVAL 4 SECONDS"
    subst["events"] = events
    subst["cores"] = cores
    return text.format(**subst)

def sort_queries(queries):
    return sorted(queries, key=lambda q: int(q[1:]))
    
def parse_queries(all_queries, arg):
    if arg is not None:
        queries = set()
        for s in arg:
            for q in s.split(','):
                q = q.lower()
                if q == 'all':
                    queries = set(all_queries.keys())
                elif q in all_queries:
                    queries.add(q)
                elif q != '':
                    sys.stderr.write(f'unknown query {q}\n')
                    sys.exit(1)
    else:
        queries = set(all_queries.keys())

    if len(queries) == 0:
        sys.stderr.write('no queries specified\n')
        sys.exit(1)

    return queries

def make_connector(topic, suffix):
    name = "nexmark"
    config = {
        "topics": [topic + suffix],
        "enable.partition.eof": "true",
        "auto.offset.reset": "earliest"
    }

    return json.dumps([{
        "format": {
            "name": "csv",
            "config": {},
        },
        "transport": {
            "name": name,
            "config": config | kafka_options
        }
    }], indent=4)

def get_full_name(folder, name):
    return folder.split('/')[-1] + '-' + name 
    
def stop_pipeline(pipeline_name, wait):
    r = requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/shutdown", headers=headers)
    if r.status_code == 404:
        return
    r.raise_for_status()
    if wait:
        return wait_for_status(pipeline_name, "Shutdown")

def start_pipeline(pipeline_name, wait):
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/start", headers=headers).raise_for_status()
    if wait:
        return wait_for_status(pipeline_name, "Running")

def wait_for_status(pipeline_name, status):
    start = time.time()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}", headers=headers).json()["deployment_status"] != status:
        time.sleep(.1)
    return time.time() - start

def write_results(results, outfile):
    writer = csv.writer(outfile)
    writer.writerow(['when', 'runner', 'mode', 'language', 'name', 'num_cores', 'num_events', 'elapsed', 'peak_memory_bytes', 'cpu_msecs'])
    writer.writerows(results)
    
def write_metrics(keys, results, outfile):
    writer = csv.writer(outfile)
    sorted_keys = sorted(keys)
    writer.writerow(sorted_keys)
    for row_dict in results:
        row_list = []
        for key in sorted_keys:
            if key in row_dict:
                row_list += [row_dict[key]]
            else:
                row_list += [""]
        writer.writerow(row_list)

def main():
    # Command-line arguments
    parser = argparse.ArgumentParser(
        description='Nexmark benchmark demo'
    )

    group = parser.add_argument_group("Options for all benchmarks")
    group.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    group.add_argument("--api-key", required=False, help="Feldera API key (e.g., \"apikey:0123456789ABCDEF\")")
    group.add_argument("--cores", type=int, help="Number of cores to use for workers (default: 16)")
    group.add_argument('--storage', action=argparse.BooleanOptionalAction, help='whether to enable storage (default: --no-storage)')
    group.add_argument('--min-storage-bytes', type=int, help='If storage is enabled, the minimum number of bytes to write a batch to storage.')
    group.add_argument('--folder', help='Folder with table and queries, organized as folder/table.sql, folder/queries/qN.sql for numbers N (default: benchmarks/nexmark)')
    group.add_argument('--query', action='append', help='queries to run (by default, all queries), specify one or more')
    group.add_argument('--csv', help='File to write results in .csv format')
    group.add_argument('--csv-metrics', help='File to write pipeline metrics (memory, disk) in .csv format')
    group.add_argument('--metrics-interval', help='How often metrics should be sampled, in seconds (default: 1)')
    group.add_argument('--include-disabled', action=argparse.BooleanOptionalAction, help='Include queries from the disabled-queries/ directory.')

    group = parser.add_argument_group("Options for Nexmark benchmark only")
    group.add_argument('--lateness', action=argparse.BooleanOptionalAction, help='whether to use lateness for GC to save memory (default: --lateness)')
    group.add_argument('--events', help='How many events to simulate (default: 100000)')

    group = parser.add_argument_group("Options only for benchmarks other than Nexmark")
    group.add_argument("-O", "--option", action='append', required=False,
                        help="Kafka options passed as -O option=value, e.g., -O bootstrap.servers=localhost:9092; ignored for Nexmark, required for other benchmarks")
    group.add_argument("--poller-threads", required=False, type=int, help="Override number of poller threads to use")
    group.add_argument('--input-topic-suffix', help='suffix to apply to input topic names (by default, "")')
    parser.set_defaults(lateness=True, storage=False, cores=16, metrics_interval=1, folder='benchmarks/nexmark', events=100000)
    
    global api_url, kafka_options, headers
    api_url = parser.parse_args().api_url
    api_key = parser.parse_args().api_key
    headers = {} if api_key is None else {"authorization": f"Bearer {api_key}"}
    kafka_options = {}
    for option_value in parser.parse_args().option or ():
        option, value = option_value.split("=")
        kafka_options[option] = value
    suffix = parser.parse_args().input_topic_suffix or ''
    events = parser.parse_args().events
    cores = int(parser.parse_args().cores)

    folder = parser.parse_args().folder
    table = load_table(folder, parser.parse_args().lateness, suffix, events, cores)
    all_queries = load_queries(folder + '/queries/')
    include_disabled = parser.parse_args().include_disabled or False
    disabled_folder = folder + '/disabled-queries/'
    if include_disabled and os.path.exists(disabled_folder):
        all_queries |= load_queries(disabled_folder)

    queries = sort_queries(parse_queries(all_queries, parser.parse_args().query))
    storage = parser.parse_args().storage
    poller_threads = parser.parse_args().poller_threads
    if poller_threads is not None:
        kafka_options["poller_threads"] = poller_threads
    min_storage_bytes = parser.parse_args().min_storage_bytes
    if min_storage_bytes is not None:
        min_storage_bytes = int(min_storage_bytes)
    csvfile = parser.parse_args().csv
    csvmetricsfile = parser.parse_args().csv_metrics
    metricsinterval = float(parser.parse_args().metrics_interval)

    when = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time()))

    # Stop pipelines
    print("Stopping pipeline(s)...")
    for pipeline_name in queries:
        stop_pipeline(get_full_name(folder, pipeline_name), False)
    for pipeline_name in queries:
        stop_pipeline(get_full_name(folder, pipeline_name), True)

    print("Creating programs...")
    for program_name in queries:
        # Create program
        full_name = get_full_name(folder, program_name)
        program_sql = table + all_queries[program_name]
        requests.put(f"{api_url}/v0/pipelines/{full_name}", headers=headers, json={
            "name": full_name,
            "description": f"Benchmark: {full_name}",
            "runtime_config": {
                "workers": cores,
                "storage": storage,
                "min_storage_bytes": min_storage_bytes,
                "cpu_profiler": True,
                "resources": {
                    # "cpu_cores_min": 0,
                    # "cpu_cores_max": 16,
                    # "memory_mb_min": 100,
                    # "memory_mb_max": 32000,
                    # "storage_mb_max": 128000,
                    # "storage_class": "..."
                }
            },
            "program_config": {},
            "program_code": program_sql,
        }).raise_for_status()

    print("Compiling program(s)...")
    for program_name in queries:
        full_name = get_full_name(folder, program_name)
        while True:
            status = requests.get(f"{api_url}/v0/pipelines/{full_name}", headers=headers).json()["program_status"]
            print(f"Program {full_name} status: {status}")
            if status == "Success":
                break
            elif status != "Pending" and status != "CompilingRust" and status != "CompilingSql":
                raise RuntimeError(f"Failed program compilation with status {status}")
            time.sleep(5)

    # Run the pipelines
    results = []
    histogram_values = ["count", "sample", "minimum", "maximum", "mean"]
    pipeline_metrics = []
    metrics_seen = {"name", "elapsed_seconds"}
    for pipeline_name in queries:
        start = time.time()
        full_name = get_full_name(folder, pipeline_name)

        # Start pipeline
        elapsed = start_pipeline(full_name, True)
        print(f"Started pipeline {full_name} in {elapsed:.1f} s")

        # Wait till the pipeline is completed
        start = time.time()
        last_processed = 0
        last_metrics = 0
        peak_memory = 0
        while True:
            stats = requests.get(f"{api_url}/v0/pipelines/{full_name}/stats", headers=headers).json()
            elapsed = time.time() - start
            if "global_metrics" in stats:
                global_metrics = stats["global_metrics"]
                processed = global_metrics["total_processed_records"]
                peak_memory = max(peak_memory, global_metrics["rss_bytes"])
                cpu_msecs = global_metrics.get("cpu_msecs", 0)
                if processed > last_processed:
                    before, after = ('\r', '') if os.isatty(1) else ('', '\n')
                    peak_gib = peak_memory / 1024 / 1024 / 1024
                    cpu_secs = cpu_msecs / 1000
                    sys.stdout.write(f"{before}Pipeline {full_name} processed {processed} records in {elapsed:.1f} seconds ({peak_gib:.1f} GiB peak memory, {cpu_secs:.1f} s CPU time){after}")
                last_metrics = elapsed
                metrics_dict = {"name":pipeline_name, "elapsed_seconds":elapsed}
                for key, value in global_metrics.items():
                    metrics_seen.add(key)
                    metrics_dict[key] = value
                for s in stats["metrics"]:
                    key = s["key"].replace(".", "_")
                    value = s["value"]
                    if "Counter" in value and value["Counter"] is not None:
                        metrics_seen.add(key)
                        metrics_dict[key] = value["Counter"]
                    elif "Gauge" in value and value["Gauge"] is not None:
                        metrics_seen.add(key)
                        metrics_dict[key] = value["Gauge"]
                        
                    if "Histogram" in value and value["Histogram"] is not None:
                        for v in histogram_values:
                            k = key + "_histogram_" + v
                            metrics_seen.add(k)
                            metrics_dict[k] = value["Histogram"][v]
                pipeline_metrics += [metrics_dict]
                last_processed = processed
                if stats["global_metrics"]["pipeline_complete"]:
                    break
            time.sleep(metricsinterval)
        if os.isatty(1):
            print()
        elapsed = "{:.1f}".format(time.time() - start)
        print(f"Pipeline {full_name} completed in {elapsed} s")

        results += [[when, "feldera", "stream", "sql", pipeline_name, cores, last_processed, elapsed, peak_memory, cpu_msecs]]

        # Start pipeline
        elapsed = stop_pipeline(full_name, True)
        print(f"Stopped pipeline {full_name} in {elapsed:.1f} s")
    
    write_results(results, sys.stdout)
    if csvfile is not None:
        write_results(results, open(csvfile, 'w', newline=''))
    if csvmetricsfile is not None:
        write_metrics(metrics_seen, pipeline_metrics, open(csvmetricsfile, 'w', newline=''))

if __name__ == "__main__":
    main()
