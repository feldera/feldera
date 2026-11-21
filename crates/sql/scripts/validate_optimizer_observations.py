#!/usr/bin/env python3
"""Round-trip each observed graph and compare the Rust naming pass with its Java output."""

import argparse
import json
from pathlib import Path
import subprocess


def validate(traces, compiler, comparator):
    manifest = json.loads((traces / "complete.json").read_bytes())
    results = []
    for case in manifest["cases"]:
        if not case["equal"]:
            raise ValueError(f"Replay failed for {case['name']}")
        capture = traces / case["mode"] / case["name"] / "direct"
        observed = named = regions = 0
        for invocation in sorted(capture.glob("compilation-*")):
            previous = None
            for path in sorted(invocation.glob("*-S[234].json")):
                record = json.loads(path.read_bytes())
                graph = record["circuit"]
                checked = subprocess.check_output([comparator, "--roundtrip-circuit", path])
                if json.loads(checked) != record:
                    raise ValueError(f"Round-trip changed {path}")
                observed += 1
                step = record["observation"].get("path", [])
                if step and step[-1]["pass"] == "CompactNames":
                    if previous is None:
                        raise ValueError(f"Missing input graph for {path}")
                    actual = subprocess.check_output([compiler, "--compact-names", "-"], input=json.dumps(previous).encode())
                    if json.loads(actual) != graph:
                        raise ValueError(f"CompactNames differs at {path}")
                    named += 1
                # DetectConflicts runs immediately before RemoveRegions and leaves
                # the circuit unchanged. Use it as the input to the combined pass.
                if step and step[-1]["container"] == "TagRegions" and step[-1]["pass"] == "RemoveRegions":
                    actual = subprocess.check_output([compiler, "--tag-regions", "-"], input=json.dumps(previous).encode())
                    if json.loads(actual) != graph:
                        raise ValueError(f"TagRegions differs at {path}")
                    regions += 1
                previous = graph
        if observed != case["observations"] or not named or regions != named:
            raise ValueError(f"Incomplete S4 comparison for {case['name']}: {observed} graphs, {named} naming passes")
        results.append({"mode": case["mode"], "name": case["name"], "graphs": observed, "compact_names": named, "tag_regions": regions})
    if not results:
        raise ValueError("No replay cases found")
    return {"equal": True, "graphs": sum(c["graphs"] for c in results), "cases": results}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("traces", type=Path)
    parser.add_argument("--compiler", type=Path, required=True)
    parser.add_argument("--comparator", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(validate(args.traces, args.compiler, args.comparator), indent=2))
