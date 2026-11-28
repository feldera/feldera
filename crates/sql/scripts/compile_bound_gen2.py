#!/usr/bin/env python3
"""Compile raw SQL with Rust binding and Gen-2 emission and the selected optimizer passes."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def optimizer_library(driver):
    suffix = ".dylib" if sys.platform == "darwin" else ".so"
    return driver.resolve().with_name("libfeldera_sql_opt_jni" + suffix).resolve(strict=True)


def stages(rust_lowering, rust_optimizer_passes):
    return {
        "parse_and_bind": "Rust",
        "lower": "Rust" if rust_lowering else "Java",
        "optimize": "Java with Rust passes via JNI" if rust_optimizer_passes else "Java",
        "gen2_emission": "Rust",
    }


def compile_program(source, output, driver, classpath, revision, rust_lowering=False,
                    rust_optimizer_passes=False):
    library = optimizer_library(driver) if rust_optimizer_passes else None
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
    lowered = middle / "s2.json"
    java = ["java", "-ea", "-cp", classpath]
    replay = [*java, "org.dbsp.sqlCompiler.compiler.sql.tools.BoundProgramReplay", bound, middle, revision]
    if rust_lowering:
        middle.mkdir()
        run("rust_lower", [driver, "--lower-s1", revision, revision, bound], lowered)
    elif rust_optimizer_passes:
        run("java_lower", [*replay, "S2"])
    else:
        run("java_lower_and_optimize", [*replay, "S3"])

    if rust_lowering or rust_optimizer_passes:
        jvm_options, pass_options = [], []
        if library is not None:
            jvm_options = ["--enable-native-access=ALL-UNNAMED", f"-Djava.library.path={library.parent}"]
            pass_options = ["--rust-optimizer-passes"]
        run("java_rust_optimize" if rust_optimizer_passes else "java_optimize",
            [*java, *jvm_options, "org.dbsp.sqlCompiler.CompilerMain", *pass_options,
             "--gen2", "--noRust", "--read-s2", lowered, "--write-s3", middle / "s3.json",
             "--dump-producer-commit", revision, "--dump-runtime-abi", revision])
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
    pipeline = stages(rust_lowering, rust_optimizer_passes)
    (output / "complete.json").write_text(json.dumps({
        "compiler": f"Rust binder and Gen-2 writer, optimizer: {pipeline['optimize']}" if rust_optimizer_passes
                    else "Rust binder and Gen-2 writer, Java optimizer",
        "stages": pipeline,
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
    parser.add_argument("--rust-optimizer-passes", action="store_true",
                        help="load the sql-opt-jni library beside the driver for circuit optimization")
    args = parser.parse_args()
    compile_program(args.source, args.output, args.driver, args.classpath, args.revision,
                    args.rust_lowering, args.rust_optimizer_passes)
