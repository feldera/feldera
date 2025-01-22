#! /usr/bin/python3

# This script does a bunch of stuff
#
# - it rewrites table connector declarations,
#   - replacing {table} with a kafka connector for the same topic
#   - it substitutes {lateness} with the lateness parameter if asked to
#   - it substitutes {events} with the number of events to generate
#   - it substitutes {cores} with the number of cores to use for the generator
#   - it substitutes {folder} with the path where the table declarations are
# - it creates a pipeline to compile all queries in the directory folder/queries
# - it then runs the queries using the feldera pipeline manager and records the performance

import csv
import json
import os
import re
import sys
import time
import requests
import argparse

# File locations
FILE_DIR = os.path.join(os.path.dirname(__file__))


def load_queries(dir, materialize):
    queries = {}
    for f in os.listdir(dir):
        if f.endswith(".sql"):
            file = open(dir + f, "r")
            query = f.split(".")[0]

            try:
                with open(dir + "/" + query + ".rs", "r") as rs_file:
                    udf_rust = rs_file.read()
            except FileNotFoundError:
                udf_rust = ""

            try:
                with open(dir + "/" + query + ".toml", "r") as toml_file:
                    udf_toml = toml_file.read()
            except FileNotFoundError:
                udf_toml = ""

            sql = file.read()
            views = re.findall("CREATE VIEW ([a-zA-Z0-9_]*)", sql)
            if materialize:
                sql = re.sub(
                    "CREATE VIEW", "CREATE MATERIALIZED VIEW", sql, flags=re.IGNORECASE
                )
            queries[query] = (sql, udf_rust, udf_toml, views)
    return queries


def load_table(folder: str, with_lateness: bool, suffix, events, cores, batchsize):
    p = os.path.join(FILE_DIR, folder, "table.sql")
    file = open(p, "r")
    text = file.read()
    table_start_string = "create table "
    inputs = []
    for line in text.lower().split("\n"):
        i = line.find(table_start_string)
        if i >= 0:
            inputs += [line[i + len(table_start_string) :].split(" ")[0]]
    subst = {input: make_connector(input, suffix) for input in inputs}
    if with_lateness:
        subst["lateness"] = "LATENESS INTERVAL 4 SECONDS"
    else:
        subst["lateness"] = ""
    subst["events"] = events
    subst["cores"] = cores
    subst["folder"] = os.path.join(FILE_DIR, folder)
    subst["batchsize"] = batchsize
    return text.format(**subst)


def sort_queries(queries):
    return sorted(queries, key=lambda q: int(q[1:]))


def parse_queries(all_queries, arg):
    if arg is not None:
        queries = set()
        for s in arg:
            for q in s.split(","):
                q = q.lower()
                if q == "all":
                    queries = set(all_queries.keys())
                elif q in all_queries:
                    queries.add(q)
                elif q != "":
                    sys.stderr.write(f"unknown query {q}\n")
                    sys.exit(1)
    else:
        queries = set(all_queries.keys())

    if len(queries) == 0:
        sys.stderr.write("no queries specified\n")
        sys.exit(1)

    return queries


def make_connector(topic, suffix):
    name = "kafka_input"
    config = {
        "topics": [topic + suffix],
        "enable.partition.eof": "true",
        "auto.offset.reset": "earliest",
    }

    return json.dumps(
        [
            {
                "format": {
                    "name": "csv",
                    "config": {},
                },
                "transport": {"name": name, "config": config | kafka_options},
            }
        ],
        indent=4,
    )


def get_full_name(folder, name):
    return folder.split("/")[-1] + "-" + name


def configure_program(pipeline_name, program, ft):
    program_sql = table + program[0]
    udf_rust = program[1]
    udf_toml = program[2]
    requests.put(
        f"{api_url}/v0/pipelines/{pipeline_name}",
        headers=headers,
        json={
            "name": pipeline_name,
            "description": f"Benchmark: {pipeline_name}",
            "runtime_config": {
                "workers": cores,
                "storage": storage,
                "fault_tolerance": ft,
                "cpu_profiler": True,
                "resources": {
                    # "cpu_cores_min": 0,
                    # "cpu_cores_max": 16,
                    # "memory_mb_min": 100,
                    # "memory_mb_max": 32000,
                    # "storage_mb_max": 128000,
                    # "storage_class": "..."
                },
            },
            "program_config": {},
            "program_code": program_sql,
            "udf_rust": udf_rust,
            "udf_toml": udf_toml,
        },
    ).raise_for_status()


def stop_pipeline(pipeline_name, wait):
    r = requests.post(
        f"{api_url}/v0/pipelines/{pipeline_name}/shutdown", headers=headers
    )
    if r.status_code == 404:
        return
    r.raise_for_status()
    if wait:
        return wait_for_status(pipeline_name, "Shutdown")


def start_pipeline(pipeline_name, wait):
    requests.post(
        f"{api_url}/v0/pipelines/{pipeline_name}/start", headers=headers
    ).raise_for_status()
    if wait:
        return wait_for_status(pipeline_name, "Running")


def checkpoint_pipeline(pipeline_name):
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/checkpoint", headers=headers)


def wait_for_status(pipeline_name, status):
    start = time.time()
    while (
        requests.get(f"{api_url}/v0/pipelines/{pipeline_name}", headers=headers).json()[
            "deployment_status"
        ]
        != status
    ):
        time.sleep(0.1)
    return time.time() - start


def write_results(results, outfile):
    writer = csv.writer(outfile)
    writer.writerow(
        [
            "when",
            "runner",
            "mode",
            "language",
            "name",
            "num_cores",
            "num_events",
            "elapsed",
            "peak_memory_bytes",
            "cpu_msecs",
            "late_drops",
        ]
    )
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
    parser = argparse.ArgumentParser(description="Nexmark benchmark demo")

    group = parser.add_argument_group("Options for all benchmarks")
    group.add_argument(
        "--api-url",
        required=True,
        help="Feldera API URL (e.g., http://localhost:8080 )",
    )
    group.add_argument(
        "--api-key",
        required=False,
        help='Feldera API key (e.g., "apikey:0123456789ABCDEF")',
    )
    group.add_argument(
        "--cores", type=int, help="Number of cores to use for workers (default: 16)"
    )
    group.add_argument(
        "--batchsize", type=int, help="Batch size to use for input (default: 10000)"
    )
    group.add_argument(
        "--storage",
        action=argparse.BooleanOptionalAction,
        help="whether to enable storage (default: --no-storage)",
    )
    group.add_argument(
        "--min-storage-bytes",
        type=int,
        help="If storage is enabled, the minimum number of bytes to write a batch to storage.",
    )
    group.add_argument(
        "--folder",
        help="Folder with table and queries, organized as folder/table.sql, folder/queries/qN.sql for numbers N (default: benchmarks/nexmark)",
    )
    group.add_argument(
        "--query",
        action="append",
        help="queries to run (by default, all queries), specify one or more",
    )
    group.add_argument("--csv", help="File to write results in .csv format")
    group.add_argument(
        "--csv-metrics",
        help="File to write pipeline metrics (memory, disk) in .csv format",
    )
    group.add_argument(
        "--metrics-interval",
        help="How often metrics should be sampled, in seconds (default: 1)",
    )
    group.add_argument(
        "--include-disabled",
        action=argparse.BooleanOptionalAction,
        help="Include queries from the disabled-queries/ directory.",
    )
    group.add_argument(
        "--circuit-profile",
        action=argparse.BooleanOptionalAction,
        help="If set to true, will save a circuit profile (default: --no-circuit-profile)",
    )
    group.add_argument(
        "--ft",
        action=argparse.BooleanOptionalAction,
        help="If set to true, enable fault tolerance (also enables --storage and --save-results)",
    )
    group.add_argument(
        "--save-results",
        action=argparse.BooleanOptionalAction,
        help="If set to true, save views' final content",
    )

    group = parser.add_argument_group("Options for Nexmark benchmark only")
    group.add_argument(
        "--lateness",
        action=argparse.BooleanOptionalAction,
        help="whether to use lateness for GC to save memory (default: --lateness)",
    )
    group.add_argument("--events", help="How many events to simulate (default: 100000)")

    group = parser.add_argument_group("Options only for benchmarks other than Nexmark")
    group.add_argument(
        "-O",
        "--option",
        action="append",
        required=False,
        help="Kafka options passed as -O option=value, e.g., -O bootstrap.servers=localhost:9092; ignored for Nexmark, required for other benchmarks",
    )
    group.add_argument(
        "--poller-threads",
        required=False,
        type=int,
        help="Override number of poller threads to use",
    )
    group.add_argument(
        "--input-topic-suffix",
        help='suffix to apply to input topic names (by default, "")',
    )
    parser.set_defaults(
        lateness=True,
        storage=False,
        batchsize=10000,
        cores=16,
        metrics_interval=1,
        folder="benchmarks/nexmark",
        events=100000,
    )

    global api_url, kafka_options, headers, cores
    api_url = parser.parse_args().api_url
    api_key = parser.parse_args().api_key
    headers = {} if api_key is None else {"authorization": f"Bearer {api_key}"}
    kafka_options = {}
    for option_value in parser.parse_args().option or ():
        option, value = option_value.split("=")
        kafka_options[option] = value
    suffix = parser.parse_args().input_topic_suffix or ""
    events = int(parser.parse_args().events)
    cores = int(parser.parse_args().cores)
    batchsize = int(parser.parse_args().batchsize)
    ft = parser.parse_args().ft or False
    save_results = parser.parse_args().save_results or ft

    global table
    folder = parser.parse_args().folder
    table = load_table(
        folder, parser.parse_args().lateness, suffix, events, cores, batchsize
    )
    all_queries = load_queries(
        os.path.join(FILE_DIR, folder + "/queries/"), save_results
    )
    include_disabled = parser.parse_args().include_disabled or False
    disabled_folder = os.path.join(FILE_DIR, folder + "/disabled-queries/")
    if include_disabled and os.path.exists(disabled_folder):
        all_queries |= load_queries(disabled_folder, save_results)
    profile = parser.parse_args().circuit_profile

    global storage
    queries = sort_queries(parse_queries(all_queries, parser.parse_args().query))
    if parser.parse_args().storage or ft:
        storage = {}
        min_storage_bytes = parser.parse_args().min_storage_bytes
        if min_storage_bytes is not None:
            storage["min_storage_bytes"] = int(min_storage_bytes)
    else:
        storage = None
    poller_threads = parser.parse_args().poller_threads
    if poller_threads is not None:
        kafka_options["poller_threads"] = poller_threads
    csvfile = parser.parse_args().csv
    csvmetricsfile = parser.parse_args().csv_metrics
    metricsinterval = float(parser.parse_args().metrics_interval)

    when = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))

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

        configure_program(
            full_name, all_queries[program_name], "initial_state" if ft else None
        )

    print("Compiling program(s)...")
    for program_name in queries:
        full_name = get_full_name(folder, program_name)
        while True:
            status = requests.get(
                f"{api_url}/v0/pipelines/{full_name}", headers=headers
            ).json()["program_status"]
            print(f"Program {full_name} compilation status: {status}")
            if status == "Success":
                break
            elif (
                status != "Pending"
                and status != "CompilingRust"
                and status != "SqlCompiled"
                and status != "CompilingSql"
            ):
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
        peak_memory = 0
        error = False
        while True:
            req = requests.get(
                f"{api_url}/v0/pipelines/{full_name}/stats", headers=headers
            )
            if req.status_code != 200:
                print("Failed to get stats: ", req)
            if req.status_code == 400:
                break

            stats = req.json()
            # for input in stats["inputs"]:
            #    print(input["endpoint_name"], input["metrics"]["end_of_input"])
            elapsed = time.time() - start
            if "global_metrics" in stats:
                global_metrics = stats["global_metrics"]
                processed = global_metrics["total_processed_records"]
                peak_memory = max(peak_memory, global_metrics["rss_bytes"])
                cpu_msecs = global_metrics.get("cpu_msecs", 0)
                last_metrics = elapsed
                metrics_dict = {"name": pipeline_name, "elapsed_seconds": elapsed}
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
                late_drops = metrics_dict.get("records_late", 0)
                steps = metrics_dict.get("feldera_dbsp_step", 0)
                if processed > last_processed:
                    before, after = ("\r", "") if os.isatty(1) else ("", "\n")
                    peak_gib = peak_memory / 1024 / 1024 / 1024
                    cpu_secs = cpu_msecs / 1000
                    sys.stdout.write(
                        f"{before}Pipeline {full_name} processed {processed} records in {elapsed:.1f} seconds ({peak_gib:.1f} GiB peak memory, {cpu_secs:.1f} s CPU time, {late_drops} late drops){after}"
                    )
                    if ft and processed >= events / 5:
                        if os.isatty(1):
                            print()
                        print("checkpointing...")
                        checkpoint_pipeline(full_name)
                        print("stopping...")
                        stop_pipeline(full_name, True)
                        configure_program(
                            full_name, all_queries[pipeline_name], "latest_checkpoint"
                        )
                        print("restarting...")
                        start_pipeline(full_name, True)
                last_processed = processed
                if stats["global_metrics"]["pipeline_complete"]:
                    break
            time.sleep(metricsinterval)
        if os.isatty(1):
            print()
        elapsed = "{:.1f}".format(time.time() - start)
        if error:
            print(f"*** Pipeline {pipeline_name} terminated with error")
        else:
            print(f"Pipeline {full_name} completed in {elapsed} s")

            results += [
                [
                    when,
                    "feldera",
                    "stream",
                    "sql",
                    pipeline_name,
                    cores,
                    last_processed,
                    elapsed,
                    peak_memory,
                    cpu_msecs,
                    late_drops,
                ]
            ]

            if profile:
                response = requests.get(
                    f"{api_url}/v0/pipelines/{full_name}/circuit_profile",
                    headers=headers,
                )
                if response.status_code == 200:
                    profile_file_name = full_name + ".zip"
                    print(
                        "\nWriting circuit profile stats to "
                        + profile_file_name
                        + "..."
                    )
                    with open(profile_file_name, "wb") as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
                else:
                    print("Failed to get stats")

        if save_results:
            view = all_queries[pipeline_name][3][0]
            suffix = "ft" if ft else "noft"
            filename = f"{pipeline_name}-{suffix}.txt"
            print(f"saving materialized view {view} to {filename}")
            r = requests.get(
                f"{api_url}/v0/pipelines/{full_name}/query",
                params={"sql": f"SELECT * FROM {view};"},
                headers=headers,
            )
            open(filename, "w").write(r.text)

        # Stop pipeline
        elapsed = stop_pipeline(full_name, True)
        print(f"Stopped pipeline {full_name} in {elapsed:.1f} s")
        if error:
            exit(1)

    write_results(results, sys.stdout)
    if csvfile is not None:
        write_results(results, open(csvfile, "w", newline=""))
    if csvmetricsfile is not None:
        write_metrics(
            metrics_seen, pipeline_metrics, open(csvmetricsfile, "w", newline="")
        )


if __name__ == "__main__":
    main()
