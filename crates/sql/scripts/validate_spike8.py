#!/usr/bin/env python3
"""Replay the directed Spike 8 SQL programs at every complete Java pass placement."""

import argparse
import json
import subprocess
import tempfile
from pathlib import Path

from validate_spike8_pass import compare, write_limiter_inputs


def run(args):
    fixtures = Path(__file__).resolve().parents[1] / "sql-opt/tests/fixtures"
    programs = json.loads((fixtures / "array_agg.json").read_bytes())
    programs["duplicate_branches"] = (fixtures / "duplicate_branches.sql").read_text()
    limiters = json.loads((fixtures / "limiters.json").read_bytes())
    args.output.mkdir()
    results = []
    for engine in ("classic", "gen2"):
        for name, sql in (programs | {n: config["sql"] for n, config in limiters.items()}).items():
            dest = args.output / engine / name
            dest.mkdir(parents=True)
            source = dest / "input.sql"
            source.write_text(sql)
            with tempfile.TemporaryDirectory(dir=dest) as scratch:
                trace = Path(scratch) / "trace"
                setting = "limiterState" if name in limiters else "passState"
                java = subprocess.run([
                    "java", "-ea", f"-Dfeldera.dump.{setting}=true", "-cp", args.classpath,
                    "org.dbsp.sqlCompiler.CompilerMain", *(["--gen2"] if engine == "gen2" else []),
                    *(["-i"] if limiters.get(name, {}).get("incremental", True) else []),
                    "--dump-circuits", trace, "--dump-producer-commit", args.producer,
                    "--dump-runtime-abi", args.runtime, "-o", Path(scratch) / "product", source,
                ], capture_output=True, text=True)
                refusal = limiters.get(name, {}).get("gen2_error") if engine == "gen2" else None
                if refusal:
                    if java.returncode == 0 or refusal not in java.stderr:
                        raise ValueError(f"Expected Java to reject {engine}/{name}: {java.stderr}")
                    directory = trace / "compilation-0000"
                    checkpoint, = directory.glob("*-limiters-input.json")
                    state, = directory.glob("*-limiters-entry.json")
                    sidecar = dest / "limiter-inputs.json"
                    write_limiter_inputs(checkpoint, json.loads(state.read_bytes()), sidecar)
                    rust = subprocess.run([args.compiler, "--insert-limiters", sidecar, checkpoint],
                                          capture_output=True, text=True)
                    if rust.returncode == 0 or refusal not in rust.stderr:
                        raise ValueError(f"Expected Rust to reject {engine}/{name}: {rust.stderr}")
                    results.append({"engine": engine, "program": name, "pass": "InsertLimiters",
                                    "refusal": refusal})
                    continue
                if java.returncode:
                    raise ValueError(f"Java failed for {engine}/{name}: {java.stderr}")
                passes = ("InsertLimiters",) if name in limiters else ("CSE", "FuseIntoArrayAgg")
                for name_of_pass in passes:
                    stage = dest / name_of_pass
                    stage.mkdir()
                    comparisons = compare(trace / "compilation-0000", stage, args.compiler,
                                          args.comparator, name_of_pass)
                    if name == "duplicate_branches" and not any(c["wire_changed"] for c in comparisons):
                        raise ValueError(f"{name} did not activate {name_of_pass} on {engine}")
                    results.append({"engine": engine, "program": name, "pass": name_of_pass,
                                    "comparisons": comparisons})
    (args.output / "complete.json").write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    for option in ("compiler", "comparator", "classpath", "producer", "runtime"):
        parser.add_argument(f"--{option}", required=True)
    run(parser.parse_args())
