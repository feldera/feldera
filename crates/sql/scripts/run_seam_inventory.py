#!/usr/bin/env python3
"""Inventory both circuit boundaries and preselect cases covering observed forms."""

import argparse
import json
import os
import threading
from pathlib import Path

from run_determinism import OutputBudget, classpath_digest, file_digest, run_process
from seam_inventory import summarize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--producer", required=True)
    parser.add_argument("--runtime", required=True)
    parser.add_argument("--timeout", type=int, default=3600)
    args = parser.parse_args()
    if args.timeout <= 0 or not args.producer.strip() or not args.runtime.strip():
        parser.error("Timeout and build identities must be positive and nonempty")
    args.input = args.input.resolve(strict=True)
    args.output = args.output.resolve()
    args.classpath = os.pathsep.join(
        str(Path(entry).resolve(strict=True))
        for entry in args.classpath.split(os.pathsep)
    )
    args.output.mkdir()
    budget = OutputBudget(args.output, 512 * 1024**2, [args.output], 512 * 1024**2)
    budget.check(force=True)
    provenance = {
        "schema_version": 1,
        "producer": args.producer,
        "runtime": args.runtime,
        "classpath": classpath_digest(args.classpath),
        "input_sha256": file_digest(args.input),
        "limits": {
            "heap_mib": 1024,
            "output_mib": 512,
            "min_free_mib": 512,
            "timeout_seconds_per_jvm": args.timeout,
        },
    }
    (args.output / "provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )
    requests = [json.loads(line) for line in args.input.read_text().splitlines()]
    results = {}
    for stage in ("S2", "S3"):
        observations = args.output / f"{stage}.jsonl"
        command = [
            "java",
            "-ea",
            "-Xmx1g",
            "-cp",
            args.classpath,
            "org.dbsp.sqlCompiler.compiler.sql.tools.SeamInventory",
            str(args.input),
            str(observations),
            stage,
        ]
        print(f"Inventory {stage}: {len(requests)} cases", flush=True)
        with (args.output / f"{stage}.log").open("w") as log:
            run_process(
                command, args.output, log, args.timeout, threading.Event(), budget
            )
        records = [json.loads(line) for line in observations.read_text().splitlines()]
        result = summarize(requests, records, stage)
        result["observations_sha256"] = file_digest(observations)
        selected = set(result["cover_case_ids"])
        with (args.output / f"{stage}-selected.jsonl").open("x") as output:
            for request in requests:
                if request["id"] in selected:
                    output.write(json.dumps(request) + "\n")
        results[stage] = result
    if (
        classpath_digest(args.classpath) != provenance["classpath"]
        or file_digest(args.input) != provenance["input_sha256"]
    ):
        raise ValueError("Compiler or input changed during inventory")
    report = {**provenance, "stages": results}
    report["complete"] = not any(result["failures"] for result in results.values())
    (args.output / "complete.json").write_text(json.dumps(report, indent=2) + "\n")
    budget.check(force=True)
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
