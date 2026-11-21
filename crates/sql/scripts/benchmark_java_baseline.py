#!/usr/bin/env python3
"""Measure cold and warm Java compiler latency in classic and Gen-2 modes."""

import argparse
import json
import math
import os
import platform
import statistics
import subprocess
from pathlib import Path

from benchmark_seams import artifact_hashes, measure
from run_determinism import classpath_digest, file_digest

COMPILER = "org.dbsp.sqlCompiler.CompilerMain"
WARM_RUNNER = "org.dbsp.sqlCompiler.compiler.sql.tools.WarmCompilerBenchmark"


def latency(seconds):
    return {
        "samples": len(seconds),
        "p50_seconds": statistics.median(seconds),
        "empirical_p95_seconds": sorted(seconds)[math.ceil(len(seconds) * 0.95) - 1],
    }


def output_hashes(trial, pipeline):
    artifact = trial / "program.out"
    artifact_files = artifact_hashes(artifact) if artifact.is_dir() else {artifact.name: file_digest(artifact)}
    if not artifact_files:
        raise ValueError(f"No compiler artifact in {trial}")
    metadata = ["schema.json", "diagnostics.json"]
    if pipeline:
        metadata.append("dataflow.json")
    return {
        "metadata": {name: file_digest(trial / name) for name in metadata},
        "artifact": artifact_files,
    }


def schedule(inputs, output, mode, samples, warmups, pipeline_runtime):
    for iteration in range(-warmups, samples):
        # Rotate order to distribute first-after-start and adjacent work.
        offset = (iteration + warmups) % len(inputs)
        for sql in inputs[offset:] + inputs[:offset]:
            directory = output / f"{iteration + warmups:03}-{sql.stem}"
            directory.mkdir()
            arguments = [
                "--je", "--errors", str(directory / "diagnostics.json"),
                "--js", str(directory / "schema.json"), "-o", str(directory / "program.out"),
            ]
            if mode == "gen2":
                arguments.append("--gen2")
            if pipeline_runtime is not None:
                arguments += ["-i", "--alltables", "--ignoreOrder", "--dataflow", str(directory / "dataflow.json")]
                if mode == "classic":
                    (directory / "program.out").mkdir()
                    arguments += ["--runtime", str(pipeline_runtime), "--crates", "baseline"]
            yield directory, sql, iteration, arguments + [str(sql)]


def validate_outputs(reference, trial, hashes, mode, comparator):
    reference_path, reference_hashes = reference
    if hashes["metadata"] != reference_hashes["metadata"]:
        differing = [name for name, sha in hashes["metadata"].items()
                     if sha != reference_hashes["metadata"][name]]
        raise ValueError(f"{differing} differ from the cold reference in {trial}")
    if mode == "classic":
        if hashes["artifact"] != reference_hashes["artifact"]:
            raise ValueError(f"Generated Rust differs from the cold reference in {trial}")
    else:
        subprocess.run([comparator, reference_path / "program.out", trial / "program.out"],
                       check=True, capture_output=True, timeout=30)


def run_mode(args, mode, inputs, java):
    root = args.output / mode
    root.mkdir()
    results = {}
    references = {}
    for lifetime in ("cold", "warm"):
        directory = root / lifetime
        directory.mkdir()
        trials = list(schedule(inputs, directory, mode, args.samples, args.warmups, args.pipeline_runtime))
        commands = [trial[3] for trial in trials]
        (directory / "arguments.json").write_text(json.dumps(commands, indent=2) + "\n")
        warm_process = None
        if lifetime == "warm":
            raw = directory / "measurements.jsonl"
            warm_process = measure([*java, WARM_RUNNER, str(directory / "arguments.json"), str(raw)], directory)
            observations = [json.loads(line) for line in raw.read_text().splitlines()]
            if len(observations) != len(trials):
                raise ValueError("Warm compiler omitted schedule entries")
        records = []
        for index, (trial, sql, iteration, command) in enumerate(trials):
            if lifetime == "cold":
                timing = measure([*java, COMPILER, *command], trial)
            else:
                row = observations[index]
                if row["index"] != index or row["exit_code"] != 0 or row["elapsed_ns"] <= 0:
                    raise ValueError("Invalid warm compiler observation")
                timing = {"seconds": row["elapsed_ns"] / 1e9}
            hashes = output_hashes(trial, pipeline=args.pipeline_runtime is not None)
            reference = references.setdefault(sql.name, (trial, hashes))
            validate_outputs(reference, trial, hashes, mode, args.comparator)
            records.append(dict(case=sql.name, iteration=iteration, artifact_hashes=hashes, **timing))
        (directory / "records.json").write_text(json.dumps(records, indent=2) + "\n")
        workloads = {}
        for sql in inputs:
            measured = [r for r in records if r["case"] == sql.name and r["iteration"] >= 0]
            entry = latency([r["seconds"] for r in measured])
            if lifetime == "cold":
                entry["peak_child_rss_kib"] = max(r["max_rss_kib"] for r in measured)
            workloads[sql.name] = entry
        results[lifetime] = dict(workloads=workloads, shared_process=warm_process)
        print(f"{mode} {lifetime}: {len(records)} compilations verified", flush=True)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--comparator", type=Path, required=True)
    parser.add_argument("--pipeline-runtime", type=Path,
                        help="Emit incremental pipeline-manager artifacts using this runtime checkout")
    parser.add_argument("--samples", type=int, default=31)
    parser.add_argument("--warmups", type=int, default=5)
    args = parser.parse_args()
    args.output = args.output.resolve()
    inputs = sorted(args.inputs.resolve().glob("*.sql"))
    if not inputs or not 5 <= args.samples <= 100 or not 1 <= args.warmups <= 30:
        parser.error("need SQL inputs, 5..100 samples and 1..30 warmups")
    if args.pipeline_runtime is not None:
        args.pipeline_runtime = args.pipeline_runtime.resolve(strict=True)
        if not args.pipeline_runtime.is_dir():
            parser.error("--pipeline-runtime must be a directory")
    args.output.mkdir()
    cpus = sorted(os.sched_getaffinity(0))[:4]
    java = ["taskset", "-c", ",".join(map(str, cpus)), "java", "-Xms128m", "-Xmx1g",
            "-XX:ActiveProcessorCount=4", "-cp", args.classpath]
    inputs_before = {str(path): file_digest(path) for path in inputs}
    classpath_before = classpath_digest(args.classpath)
    comparator_before = file_digest(args.comparator)
    environment = {
        "platform": platform.platform(),
        "affinity": cpus,
        "java": subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT, text=True),
        "cpuinfo": Path("/proc/cpuinfo").read_text(),
        "loadavg_before": Path("/proc/loadavg").read_text(),
        "inputs": inputs_before,
        "classpath": classpath_before,
        "comparator_sha256": comparator_before,
        "runner_sha256": file_digest(Path(__file__)),
        "samples": args.samples,
        "warmups": args.warmups,
        "pipeline_runtime": None if args.pipeline_runtime is None else str(args.pipeline_runtime),
    }
    (args.output / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    results = {mode: run_mode(args, mode, inputs, java) for mode in ("classic", "gen2")}
    if ({str(path): file_digest(path) for path in inputs} != inputs_before
            or classpath_digest(args.classpath) != classpath_before
            or file_digest(args.comparator) != comparator_before):
        raise ValueError("Benchmark input or executable changed during the run")
    result = {
        "schema_version": 1, "complete": True,
        "compiler_baseline": results,
        "loadavg_after": Path("/proc/loadavg").read_text(),
        "runtime_executed": False,
        "production_budget_approved": False,
    }
    (args.output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
