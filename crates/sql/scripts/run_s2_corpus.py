#!/usr/bin/env python3
"""Compare direct compilation and S2/S3 replay for a prepared corpus population."""

import argparse
import json
import os
import sys
import threading
from pathlib import Path

from determinism import compare
from run_determinism import OutputBudget, classpath_digest, file_digest, run_process
from validate_live_optimizer import compare as compare_live_optimizer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="JSONL prepared by determinism.py")
    parser.add_argument("output", type=Path)
    parser.add_argument("--stage", choices=("S2", "S3"), default="S2")
    parser.add_argument("--full-optimizer-trace", action="store_true",
                        help="Compare the ordered S2, S4 and S3 graphs through the mixed optimizer")
    parser.add_argument("--rust-optimizer", action="store_true",
                        help="Replay S2 through the mixed optimizer using the JNI library next to the comparator")
    parser.add_argument(
        "--replay-first",
        action="store_true",
        help="Expose decoder failures before spending time on the direct capture",
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="Retain failed cases and continue the audit; any failure prevents a pass",
    )
    parser.add_argument(
        "--source-class", help="Exact Java test class; retains all its cases"
    )
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--comparator", required=True, type=Path)
    parser.add_argument("--producer", required=True)
    parser.add_argument("--runtime", required=True)
    parser.add_argument("--timeout", type=int, default=1200)
    parser.add_argument("--free-space-path", action="append", type=Path, default=[])
    args = parser.parse_args()
    if args.timeout <= 0 or not args.producer.strip() or not args.runtime.strip():
        parser.error("Timeout and build identities must be positive and nonempty")
    args.output = args.output.resolve()
    args.comparator = args.comparator.resolve(strict=True)
    if args.full_optimizer_trace and not args.rust_optimizer:
        parser.error("--full-optimizer-trace requires --rust-optimizer")
    library = None
    if args.rust_optimizer:
        if args.stage != "S2":
            parser.error("The Rust optimizer starts from S2")
        suffix = ".dylib" if sys.platform == "darwin" else ".so"
        library = args.comparator.with_name("libfeldera_sql_opt_jni" + suffix).resolve(strict=True)
    args.classpath = os.pathsep.join(
        str(Path(entry).resolve(strict=True))
        for entry in args.classpath.split(os.pathsep)
    )
    args.output.mkdir()
    budget = OutputBudget(
        args.output,
        512 * 1024 * 1024,
        [args.output, *args.free_space_path],
        512 * 1024 * 1024,
    )
    budget.check(force=True)
    selected = args.output / "input.jsonl"
    identities = set()
    with args.input.open() as source, selected.open("x") as target:
        for line in source:
            case = json.loads(line)
            identity = case["id"]
            if args.source_class and identity.split("#", 1)[0] != args.source_class:
                continue
            if identity in identities:
                raise ValueError(f"Duplicate case identity: {identity}")
            identities.add(identity)
            target.write(json.dumps(case, separators=(",", ":")) + "\n")
            budget.check()
    budget.check(force=True)
    if not identities:
        raise ValueError("Empty corpus selection")
    capture_mode = "capture-s3" if args.stage == "S3" else "capture"
    replay_mode = f"replay-{args.stage.lower()}"
    if library:
        capture_mode, replay_mode = "live-s3", "live-s2"
    modes = (
        [replay_mode, capture_mode]
        if args.replay_first
        else [capture_mode, replay_mode]
    )
    provenance = {
        "schema_version": 1,
        "producer": args.producer,
        "runtime": args.runtime,
        "classpath": classpath_digest(args.classpath),
        "comparator_sha256": file_digest(args.comparator),
        "input_sha256": file_digest(selected),
        "source_class": args.source_class,
        "cases": len(identities),
        "stage": args.stage,
        "execution_order": modes,
        "keep_going": args.keep_going,
        "artifact_contract": "seam-v1",
        "runtime_executed": False,
        "limits": {
            "heap_mib": 1024,
            "output_mib": 512,
            "min_free_mib": 512,
            "timeout_seconds_per_jvm": args.timeout,
        },
    }
    if library:
        provenance["full_optimizer_trace"] = args.full_optimizer_trace
        provenance["optimizer_library_sha256"] = file_digest(library)
    (args.output / "provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )
    for mode in modes:
        optimizer_options = []
        if library:
            optimizer_options = [
                "-Xss8m", "-XX:ActiveProcessorCount=1",
                f"-Dfeldera.determinism.fullOptimizerTrace={str(args.full_optimizer_trace).lower()}",
                f"-Djava.library.path={library.parent}",
                f"-Dfeldera.optimizer.rustPasses={str(mode == replay_mode).lower()}",
            ]
        command = [
            "java",
            "-ea",
            "-Xmx1g",
            *optimizer_options,
            f"-Dfeldera.determinism.producer={args.producer}",
            f"-Dfeldera.determinism.runtime={args.runtime}",
            "-Dfeldera.determinism.compress=true",
            "-Dfeldera.determinism.seamArtifacts=true",
            f"-Dfeldera.determinism.keepGoing={str(args.keep_going).lower()}",
            "-cp",
            args.classpath,
            "org.dbsp.sqlCompiler.compiler.sql.tools.CorpusDeterminism",
            str(selected),
            str(args.output / mode),
            str(args.comparator),
            str(args.output / f"{mode}-generated"),
            mode,
        ]
        print(f"Starting {mode}: {len(identities)} cases", flush=True)
        with (args.output / f"{mode}.log").open("w") as log:
            run_process(
                command, args.output, log, args.timeout, threading.Event(), budget
            )
    if (
        classpath_digest(args.classpath) != provenance["classpath"]
        or file_digest(args.comparator) != provenance["comparator_sha256"]
    ):
        raise ValueError("Compiler or comparator changed during replay")
    direct, replay = args.output / capture_mode, args.output / replay_mode
    if library:
        if file_digest(library) != provenance["optimizer_library_sha256"]:
            raise ValueError("Rust optimizer library changed during replay")
        result = compare_live_optimizer(selected, [direct], [replay], full_trace=args.full_optimizer_trace,
                                        require_native_limiters=True)
        result["equal"] = result["mismatches"] == 0
    else:
        result = compare(direct, replay)
        result["equal"] &= (
            not result["exported_schema_differences"]
            and not result["exported_diagnostic_differences"]
        )
    report = {**provenance, **result, "jvm_runs": 2}
    (args.output / "complete.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"Compared {result['cases']} cases: equal={result['equal']}", flush=True)
    return 0 if result["equal"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
