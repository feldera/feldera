#!/usr/bin/env python3
"""Compare the Rust tail with the Java S4 graphs on either side of the hash pass."""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile


def run(compiler, mode, graph):
    return subprocess.check_output([compiler, mode, "-"], input=graph)


def check(invocation, compiler, comparator, temp, java_classpath=None):
    records = sorted(invocation.glob("*-S4.json"))
    expected = temp / "java.json"
    actual = temp / "rust.json"
    previous = None
    result = None
    checks = []
    for path in records:
        data = json.loads(path.read_bytes())
        graph = json.dumps(data["circuit"]).encode()
        steps = data["observation"]["path"]
        if len(steps) == 1:
            name = steps[0]["pass"]
            if name == "CheckHints":
                if json.loads(run(compiler, "--check-hints", previous)):
                    raise ValueError(f"Unexpected diagnostic at {path}")
                result = previous
            elif name == "MerkleHashes":
                # The Java tail owns compatible hashes. Its tagged graph is the next Rust input.
                result = graph
            elif result is not None and name in ("ComparatorDeclarations", "CompactNames", "TagRegions", "Gen2Passes"):
                flag = {"ComparatorDeclarations": "--declare-comparators", "CompactNames": "--compact-names",
                        "TagRegions": "--tag-regions", "Gen2Passes": "--gen2-passes"}[name]
                if name != "Gen2Passes" or data["options"]["ioOptions"]["gen2"]:
                    result = run(compiler, flag, result)
            elif name == "CircuitStatistics" and java_classpath:
                counters = json.loads(run(compiler, "--statistics", result))
                baseline = json.loads(subprocess.check_output(["java", "-ea", "-cp", java_classpath,
                    "org.dbsp.sqlCompiler.compiler.visitors.outer.TailStatisticsOracle", path]))
                if counters != baseline:
                    raise ValueError(f"Statistics differ at {path}: Rust {counters}, Java {baseline}")
            if result is not None:
                expected.write_bytes(graph)
                actual.write_bytes(result)
                proc = subprocess.run([comparator, expected, actual], capture_output=True)
                if proc.returncode:
                    raise ValueError(f"{path} {name}: {proc.stderr.decode()} {proc.stdout.decode()}")
                checks.append(name)
        previous = graph
    if checks[:2] != ["CheckHints", "ComparatorDeclarations"] or checks[-1:] != ["CircuitStatistics"]:
        raise ValueError(f"Incomplete tail in {invocation}: {checks}")
    return checks


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("traces", type=Path)
    parser.add_argument("--compiler", required=True)
    parser.add_argument("--comparator", required=True)
    parser.add_argument("--java-classpath", help="include the compiled TailStatisticsOracle to compare counters")
    args = parser.parse_args()
    summary = {}
    with tempfile.TemporaryDirectory() as directory:
        for path in sorted(args.traces.glob("*/*/direct/compilation-*")):
            summary[str(path.relative_to(args.traces))] = check(
                path, args.compiler, args.comparator, Path(directory), args.java_classpath)
    if not summary:
        raise SystemExit("No S4 traces found")
    print(json.dumps(summary, indent=2))
