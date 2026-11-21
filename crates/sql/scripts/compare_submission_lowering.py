#!/usr/bin/env python3
"""Compare every available Rust S2 against Java lowering of the same bound program."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from validate_table_lowering import compare_checkpoints


def compare(audit, output, compiler, comparator, classpath):
    output.mkdir()
    revision = "submission-audit"
    with (audit / "cases.jsonl").open() as source:
        requested = {case["id"] for line in source if (case := json.loads(line)).get("rust_s2")}
    checked = []
    with (audit / "bound.jsonl").open() as source:
        for line in source:
            case_id = json.loads(line)["id"]
            if case_id not in requested:
                continue
            requested.remove(case_id)
            directory = output / hashlib.sha256(case_id.encode()).hexdigest()
            directory.mkdir()
            bound = directory / "bound.json"
            bound.write_text(line)
            golden = directory / "java"
            subprocess.run(["java", "-ea", "-cp", classpath,
                            "org.dbsp.sqlCompiler.compiler.sql.tools.BoundProgramReplay",
                            bound, golden, revision, "S2"], check=True)
            actual = directory / "rust.s2.json"
            with actual.open("xb") as artifact:
                subprocess.run([compiler, "--lower-s1", revision, revision, bound], stdout=artifact, check=True)
            compare_checkpoints(directory, golden / "s2.json", actual, comparator)
            # The structural oracle follows graph edges irrespective of definition order.
            # The runtime decoder must also resolve references while reading the checkpoint.
            subprocess.run(["java", "-ea", "-cp", classpath, "org.dbsp.sqlCompiler.CompilerMain",
                            "--gen2", "--noRust", "--dump-producer-commit", revision,
                            "--dump-runtime-abi", revision, "--read-s2", actual,
                            "--write-s3", directory / "resumed.s3.json"], check=True)
            checked.append(case_id)
    if requested or not checked or len(checked) != json.loads((audit / "complete.json").read_bytes())["lowered"]:
        raise ValueError("Incomplete S2 comparison")
    report = {"scope": "Detached lowering of all available cases in the default Gen-2 submission audit",
              "equal": True, "cases": checked}
    (output / "complete.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("audit", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compiler", required=True, type=Path)
    parser.add_argument("--comparator", required=True, type=Path)
    parser.add_argument("--classpath", required=True)
    args = parser.parse_args()
    compare(args.audit, args.output, args.compiler, args.comparator, args.classpath)
