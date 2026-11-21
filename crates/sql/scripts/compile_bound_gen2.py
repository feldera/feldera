#!/usr/bin/env python3
"""Compile raw SQL with Rust binding and Gen-2 emission and a Java optimizer."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import time


def compile_program(source, output, driver, classpath, revision, rust_lowering=False):
    output.mkdir()
    timings = {}

    def run(label, command, artifact=None):
        start = time.perf_counter_ns()
        if artifact is None:
            subprocess.run(command, check=True)
        else:
            with artifact.open("xb") as stream:
                subprocess.run(command, stdout=stream, check=True)
        timings[label] = time.perf_counter_ns() - start

    start = time.perf_counter_ns()
    bound = output / "bound.json"
    run("rust_parse_and_bind", [driver, "--emit-s1", source], bound)
    middle = output / "lowered"
    if rust_lowering:
        middle.mkdir()
        lowered = middle / "s2.json"
        run("rust_lower", [driver, "--lower-s1", revision, revision, bound], lowered)
        run("java_optimize", ["java", "-ea", "-cp", classpath, "org.dbsp.sqlCompiler.CompilerMain",
            "--gen2", "--noRust", "--read-s2", lowered, "--write-s3", middle / "s3.json",
            "--dump-producer-commit", revision, "--dump-runtime-abi", revision])
    else:
        run("java_lower_and_optimize", ["java", "-ea", "-cp", classpath,
            "org.dbsp.sqlCompiler.compiler.sql.tools.BoundProgramReplay", bound, middle, revision, "S3"])
    emit_start = time.perf_counter_ns()
    attempt = output / "tail"
    completed = subprocess.run([driver, "--s3-worker", revision, revision, middle / "s3.json", attempt])
    ready = attempt / "result"
    status_file = ready / "result.json"
    if not status_file.is_file():
        raise RuntimeError(f"Gen-2 worker exited with status {completed.returncode} before publishing a result")
    result = json.loads(status_file.read_bytes())
    if (result["status"] != "ok" or completed.returncode != 0
            or result["producer_commit"] != revision or result["runtime_abi"] != revision):
        raise RuntimeError(f"Gen-2 worker failed: {result}")
    # Preserve the public layout while keeping the published attempt intact.
    for name, target in (("circuit", output), ("dataflow", middle), ("schema", middle), ("diagnostics", middle)):
        filename = name + ".json"
        os.link(ready / filename, target / filename)
    timings["rust_gen2_emission"] = time.perf_counter_ns() - emit_start
    timings["end_to_end"] = time.perf_counter_ns() - start
    (output / "complete.json").write_text(json.dumps({
        "compiler": "Rust binder and Gen-2 writer, Java optimizer",
        "lowering": "Rust" if rust_lowering else "Java",
        "source": str(source), "revision": revision, "elapsed_ns": timings,
    }, indent=2) + "\n")
    return timings


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path, help="new directory for this compilation")
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--classpath", required=True, help="Java compiler jar and test classes")
    parser.add_argument("--revision", required=True)
    parser.add_argument("--rust-lowering", action="store_true", help="run the Rust S1-to-S2 lowerer")
    args = parser.parse_args()
    compile_program(args.source, args.output, args.driver, args.classpath, args.revision, args.rust_lowering)
