#!/usr/bin/env python3
"""Measure the matched classic/native Unicode graphs in pinned Linux processes."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import time


def run(command, log, *, cpu, env, cwd=None):
    with log.open("w") as stream:
        start = time.perf_counter_ns()
        process = subprocess.Popen(
            ["taskset", "-c", str(cpu), *command],
            env=env, cwd=cwd, stdout=stream, stderr=subprocess.STDOUT,
        )
        _, status, usage = os.wait4(process.pid, 0)
        elapsed = time.perf_counter_ns() - start
        process.returncode = os.waitstatus_to_exitcode(status)
    if process.returncode:
        raise RuntimeError(f"{command[0]} failed; see {log}")
    return elapsed, usage.ru_maxrss


def summary(values):
    sorted_values = sorted(values)
    return {
        "p50": statistics.median(sorted_values),
        "p95": sorted_values[(95 * len(sorted_values) + 99) // 100 - 1],
        "min": sorted_values[0],
        "max": sorted_values[-1],
    }


def runtime_samples(binary, output, cpu):
    measurements = []
    for rows, steps in [(64, 200), (4096, 50)]:
        for pair in range(1, 32):
            order = ["classic", "native"] if pair % 2 else ["native", "classic"]
            for engine in order:
                log = output / f"runtime-{rows}-{pair}-{engine}.log"
                env = os.environ | {
                    "P0_NATIVE_BENCH_ENGINE": engine,
                    "P0_NATIVE_BENCH_ROWS": str(rows),
                    "P0_NATIVE_BENCH_STEPS": str(steps),
                }
                wall_ns, max_rss = run([
                    binary, "--exact", "paired_projection::paired_unicode_runtime_sample",
                    "--ignored", "--nocapture", "--test-threads=1",
                ], log, cpu=cpu, env=env)
                counts = re.search(r"init_ns=(\d+) elapsed_ns=(\d+)", log.read_text())
                init_ns, elapsed_ns = map(int, counts.groups())
                measurements.append({
                    "rows_per_step": rows,
                    "steps": steps,
                    "pair": pair,
                    "engine": engine,
                    "init_ns": init_ns,
                    "elapsed_ns": elapsed_ns,
                    "step_latency_ns": elapsed_ns / steps,
                    "rows_per_second": rows * steps * 1_000_000_000 / elapsed_ns,
                    "wall_ns": wall_ns,
                    "peak_rss_kib": max_rss,
                })
        print(f"Completed {rows}-row paired runtime samples", flush=True)
    return measurements


def classic_build_samples(classic, output, cpu):
    measurements = []
    env = os.environ | {"CARGO_PROFILE_DEV_DEBUG": "0", "CARGO_INCREMENTAL": "0"}
    command = ["cargo", "rustc", "--locked", "--offline", "--jobs", "1",
               "--features", "catalog,adapterlib", "--lib", "--",
               "-C", "link-arg=-Wl,--strip-debug"]
    for trial in range(1, 16):
        # Leave the source bytes and dependency cache unchanged. Rebuild the
        # generated package and relink its shared object for each observation.
        os.utime(classic / "src/lib.rs", None)
        log = output / f"classic-build-{trial}.log"
        elapsed, rss = run(command, log, cpu=cpu, cwd=classic, env=env)
        if "Compiling p0-native-classic-build " not in log.read_text():
            raise RuntimeError(f"missing generated-package compilation in {log}")
        measurements.append({"trial": trial, "wall_ns": elapsed, "peak_rss_kib": rss})
    return measurements


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("classic", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir()
    cpu = min(os.sched_getaffinity(0))
    binary = args.binary.resolve()
    samples = runtime_samples(binary, args.output, cpu)
    builds = classic_build_samples(args.classic.resolve(), args.output, cpu)
    runtime = {}
    for rows in (64, 4096):
        runtime[str(rows)] = {}
        for engine in ("classic", "native"):
            selected = [record for record in samples
                        if record["rows_per_step"] == rows and record["engine"] == engine]
            runtime[str(rows)][engine] = {
                metric: summary([record[metric] for record in selected])
                for metric in ["init_ns", "step_latency_ns", "rows_per_second", "peak_rss_kib"]
            }
    report = {
        "schema_version": 1,
        "cpu": cpu,
        "runtime_binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "runtime_summary": runtime,
        "classic_build_summary": {
            metric: summary([sample[metric] for sample in builds])
            for metric in ["wall_ns", "peak_rss_kib"]
        },
        "runtime_samples": samples,
        "classic_build_samples": builds,
    }
    (args.output / "measurements.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
