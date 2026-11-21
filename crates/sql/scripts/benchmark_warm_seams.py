#!/usr/bin/env python3
"""Measure direct compilation and S2 file transport in one warmed Java process.

Requires a prepared benchmark archive. This isolates process reuse; it does not
model a Rust mid-end, IPC, concurrent workers, or production tail latency.
"""

import argparse
import json
import math
import os
import platform
import statistics
import subprocess
from pathlib import Path

from benchmark_seams import artifact_hashes, bootstrap_ratio, measure
from compare_parsers import digest
from run_determinism import classpath_digest


def summarize(schedule, observations):
    if len(schedule) != len(observations):
        raise ValueError("incomplete warm-process schedule")
    pairs = {}
    for index, (command, observation) in enumerate(zip(schedule, observations)):
        if (type(observation["index"]) is not int or observation["index"] != index
                or observation["exit_code"] != 0 or type(observation["elapsed_ns"]) is not int
                or observation["elapsed_ns"] <= 0):
            raise ValueError("invalid or failed warm-process observation")
        key = (command["case"], command["iteration"])
        pair = pairs.setdefault(key, {"warmup": command["warmup"]})
        if command["part"] in pair or pair["warmup"] != command["warmup"]:
            raise ValueError("duplicate part or inconsistent warmup status")
        pair[command["part"]] = observation["elapsed_ns"] / 1e9
    result = {}
    for case in sorted({case for case, _ in pairs}):
        selected = [pair for (name, _), pair in pairs.items() if name == case and not pair["warmup"]]
        if not selected or any(set(pair) != {"warmup", "direct", "head", "tail"} for pair in pairs.values()):
            raise ValueError("incomplete direct/head/tail pair")
        direct = [row["direct"] for row in selected]
        split = [row["head"] + row["tail"] for row in selected]
        costs = {}
        for name, values in [("direct", direct), ("split", split)]:
            costs[name] = dict(mean_seconds=statistics.mean(values), median_seconds=statistics.median(values),
                               empirical_p95_seconds=sorted(values)[math.ceil(len(values) * .95) - 1],
                               min_seconds=min(values), max_seconds=max(values))
        result[case] = dict(samples=len(selected), **costs, split_over_direct=sum(split) / sum(direct),
                            paired_bootstrap_95_percent_interval=bootstrap_ratio(direct, split))
    return result


def prepare(inputs, output, runtime, samples, warmups):
    common = ["--crates", "bench", "--runtime", str(runtime.resolve()), "--je",
              "--dump-producer-commit", "p0-warm-seam-benchmark", "--dump-runtime-abi", "working-tree"]
    schedule = []
    trials = []
    for iteration in range(-warmups, samples):
        for index, sql in enumerate(inputs):
            trial = output / f"{sql.stem}-{iteration + warmups:03}"
            trial.mkdir()
            for part in ["direct", "head", "tail"]:
                (trial / part).mkdir()
            for part in ["direct", "tail"]:
                (trial / part / "artifacts").mkdir()
            seam = trial / "seam.json"
            commands = {}
            for part in ["direct", "tail"]:
                folder = trial / part
                commands[part] = [*common, "--errors", str(folder / "errors.json"), "--js", str(folder / "schema.json"),
                                  "-o", str(folder / "artifacts"), *(["--read-s2", str(seam)] if part == "tail" else [str(sql.resolve())])]
            commands["head"] = [*common, "--input_circuit", "--noRust", "--write-s2", str(seam),
                                "--errors", str(trial / "head/errors.json"), str(sql.resolve())]
            order = ["direct", "head", "tail"] if (iteration + index) % 2 == 0 else ["head", "tail", "direct"]
            for part in order:
                schedule.append(dict(case=sql.name, iteration=iteration, warmup=iteration < 0,
                                     part=part, arguments=commands[part]))
            trials.append(trial)
    return schedule, trials


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--runtime", required=True, type=Path)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--samples", type=int, default=31)
    parser.add_argument("--warmups", type=int, default=5)
    args = parser.parse_args()
    for name in ["commit.txt", "changes.patch", "description.md", "env.txt"]:
        if not (args.archive / name).is_file():
            parser.error(f"prepare benchmark archive first: missing {name}")
    inputs = sorted(args.inputs.glob("*.sql"))
    if not inputs or not 5 <= args.samples <= 100 or not 1 <= args.warmups <= 30:
        parser.error("need SQL inputs, 5..100 samples, and 1..30 warmup pairs")
    output = args.output.resolve()
    output.mkdir()
    schedule, trials = prepare(inputs, output, args.runtime, args.samples, args.warmups)
    (output / "schedule.json").write_text(json.dumps(schedule, indent=2) + "\n")
    (output / "arguments.json").write_text(json.dumps([row["arguments"] for row in schedule]) + "\n")
    cpus = sorted(os.sched_getaffinity(0))[:4]
    environment = dict(platform=platform.platform(), host=platform.node(), affinity=cpus,
                       java=subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT, text=True),
                       cpuinfo=Path("/proc/cpuinfo").read_text(), meminfo=Path("/proc/meminfo").read_text(),
                       loadavg_before=Path("/proc/loadavg").read_text(), samples=args.samples, warmups=args.warmups,
                       inputs_sha256={str(path): digest(path) for path in inputs},
                       classpath_sha256=classpath_digest(args.classpath), runner_sha256=digest(Path(__file__)),
                       archive=str(args.archive),
                       limitations=["VM host load and CPU frequency are uncontrolled.",
                                    "Selected SQL workloads use CLI defaults, not the JUnit option matrix.",
                                    "One JVM executes both paths; no Rust mid-end or IPC is measured.",
                                    "Per-command latency excludes JVM startup and schedule parsing; it includes output and S2 file I/O.",
                                    "Warmup count is fixed; JIT/GC can still evolve during measured samples.",
                                    "RSS is the high-water mark of the entire shared JVM, not an attributable per-path measurement.",
                                    "Empirical p95 and paired bootstrap intervals do not certify production tail latency."])
    (output / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    command = ["taskset", "-c", ",".join(map(str, cpus)), "java", "-Xms128m", "-Xmx1g", "-cp", args.classpath,
               "org.dbsp.sqlCompiler.compiler.sql.tools.WarmCompilerBenchmark", str(output / "arguments.json"), str(output / "measurements.jsonl")]
    process = measure(command, output)
    observations = [json.loads(line) for line in (output / "measurements.jsonl").read_text().splitlines()]
    workloads = summarize(schedule, observations)
    comparisons = []
    for trial in trials:
        for name in ["schema.json", "errors.json"]:
            if (trial / "direct" / name).read_bytes() != (trial / "tail" / name).read_bytes():
                raise ValueError(f"split {name} differs: {trial}")
        artifacts = artifact_hashes(trial / "direct/artifacts")
        if not artifacts or artifacts != artifact_hashes(trial / "tail/artifacts"):
            raise ValueError(f"split output artifacts differ: {trial}")
        comparisons.append(dict(trial=trial.name, artifacts=artifacts, seam_bytes=(trial / "seam.json").stat().st_size))
    if environment["inputs_sha256"] != {str(path): digest(path) for path in inputs} or environment["classpath_sha256"] != classpath_digest(args.classpath):
        raise ValueError("benchmark inputs or classpath changed")
    result = dict(schema_version=1, complete=True, workloads=workloads, comparisons=comparisons,
                  shared_process=process, production_budget_approved=False, runtime_executed=False)
    (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(dict(workloads=workloads, shared_process=process), indent=2))


if __name__ == "__main__":
    main()
