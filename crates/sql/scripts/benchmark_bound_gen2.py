#!/usr/bin/env python3
"""Time fresh processes from SQL source to Gen-2 circuit, schema and dataflow files."""

import argparse
import json
from pathlib import Path
import platform
import subprocess
import time

from benchmark_java_baseline import latency
from compile_bound_gen2 import compile_program, optimizer_library, stages
from run_determinism import classpath_digest, file_digest


def direct(source, output, classpath):
    output.mkdir()
    start = time.perf_counter_ns()
    subprocess.run([
        "java", "-ea", "-cp", classpath, "org.dbsp.sqlCompiler.CompilerMain", "--gen2",
        "--je", "--errors", output / "diagnostics.json",
        "--js", output / "schema.json", "--dataflow", output / "dataflow.json",
        "-o", output / "circuit.json", source,
    ], check=True)
    return time.perf_counter_ns() - start


def compare(reference, candidate, comparator):
    subprocess.run([comparator, reference / "circuit.json", candidate / "circuit.json"], check=True)
    for name in ("schema.json", "dataflow.json"):
        expected = json.loads((reference / name).read_bytes())
        actual = json.loads((candidate / "lowered" / name).read_bytes())
        if expected != actual:
            raise ValueError(f"{candidate}: {name} differs from direct Java compilation")
    messages = (reference / "diagnostics.json").read_bytes().strip()
    expected = json.loads(messages) if messages else []
    if expected != json.loads((candidate / "lowered" / "diagnostics.json").read_bytes()):
        raise ValueError(f"{candidate}: diagnostics differ from direct Java compilation")


def benchmark(args):
    if args.samples < 1 or args.warmups < 0:
        raise ValueError("Expected at least one sample and a nonnegative warmup count")
    args.output.mkdir()
    source = args.output / "input.sql"
    source.write_bytes(args.source.read_bytes())
    profile = {
        "source_sha256": file_digest(source), "driver_sha256": file_digest(args.driver),
        "comparator_sha256": file_digest(args.comparator), "classpath": classpath_digest(args.classpath),
        "revision": args.revision, "architecture": platform.machine(), "platform": platform.platform(),
        "java": subprocess.run(["java", "-version"], capture_output=True, check=True, text=True).stderr,
        "samples": args.samples, "warmups": args.warmups,
        "lowering": "Rust" if args.rust_lowering else "Java",
        "tail": "sql2dbsp --s3-worker",
        "scope": "Fresh process per stage, warmed filesystem, Gen-2 default nonincremental options",
    }
    profile["stages"] = stages(args.rust_lowering, args.rust_optimizer_passes)
    if args.rust_optimizer_passes:
        library = optimizer_library(args.driver)
        profile["optimizer_library"] = {"path": str(library), "sha256": file_digest(library)}
    (args.output / "profile.json").write_text(json.dumps(profile, indent=2) + "\n")
    observations = {"direct_java": [], "mixed": []}
    with (args.output / "samples.jsonl").open("x") as records:
        for index in range(args.warmups + args.samples):
            trial = args.output / f"pair-{index:03}"
            trial.mkdir()
            reference = trial / "direct"
            candidate = trial / "mixed"
            order = list(observations)
            if index % 2:
                order.reverse()
            elapsed = {}
            for mode in order:
                if mode == "direct_java":
                    elapsed[mode] = direct(source, reference, args.classpath)
                else:
                    candidate_times = compile_program(
                        source, candidate, args.driver, args.classpath, args.revision,
                        args.rust_lowering, args.rust_optimizer_passes
                    )
                    elapsed[mode] = candidate_times["end_to_end"]
            compare(reference, candidate, args.comparator)
            warmup = index < args.warmups
            record = {"pair": index, "warmup": warmup, "order": order, "elapsed_ns": elapsed}
            record["mixed_stage_ns"] = {k: v for k, v in candidate_times.items() if k != "end_to_end"}
            print(json.dumps(record), file=records, flush=True)
            if not warmup:
                for mode, duration in elapsed.items():
                    observations[mode].append(duration / 1e9)
    summary = {mode: latency(values) for mode, values in observations.items()}
    (args.output / "complete.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path, help="new directory for raw samples and artifacts")
    parser.add_argument("--driver", required=True, type=Path)
    parser.add_argument("--comparator", required=True, type=Path)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--rust-lowering", action="store_true", help="time Rust lowering through S2")
    parser.add_argument("--rust-optimizer-passes", action="store_true",
                        help="benchmark the Java optimizer with the Rust JNI passes enabled")
    parser.add_argument("--samples", type=int, default=20)
    benchmark(parser.parse_args())
