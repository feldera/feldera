#!/usr/bin/env python3
"""Compare complete optimizer pass invocations in Java S4 traces to the Rust pass."""

import argparse
import json
import subprocess
from pathlib import Path


def singleton_ids(value):
    if isinstance(value, dict):
        if value.get("singleton") is True:
            yield value["id"]
        for item in value.values():
            yield from singleton_ids(item)
    elif isinstance(value, list):
        for item in value:
            yield from singleton_ids(item)


def write_limiter_inputs(checkpoint, state, sidecar):
    inputs = {key: state[key] for key in ("operators", "next_generated_name", "gen2")}
    for key in ("transfer_roots", "transfer_functions"):
        if key in state:
            inputs[key] = state[key]
    inputs["singletons"] = [*singleton_ids(json.loads(checkpoint.read_bytes())), state["weight_type"]]
    sidecar.write_text(json.dumps(inputs))


def compare(trace, output, compiler, comparator, pass_name="CSE"):
    states = {}
    if pass_name in ("FuseIntoArrayAgg", "InsertLimiters"):
        for line in (trace / "pass-state.jsonl").read_bytes().splitlines():
            state = json.loads(line)
            states[json.dumps(state["path"], sort_keys=True)] = state
    original = {}
    previous = None
    start = None
    cases = []
    for path in sorted(trace.glob("*-S[234].json")):
        record = json.loads(path.read_bytes())
        steps = record["observation"].get("path", [])
        graph = record["circuit"]
        if steps and steps[-1]["pass"] == "OrderErrorConsumers":
            original[json.dumps(steps[:-1], sort_keys=True)] = graph
        for i, step in enumerate(steps):
            if step["pass"] != pass_name:
                continue
            if start is None:
                if previous is None:
                    raise ValueError(f"Missing input to {path}")
                if pass_name == "InsertLimiters":
                    start = original[json.dumps(steps[:i], sort_keys=True)]
                else:
                    start = previous
                entry_sequence = record["observation"]["sequence"]
            if i + 1 == len(steps):
                prefix = output / path.stem
                before = prefix.with_suffix(".input")
                expected = prefix.with_suffix(".java")
                actual = prefix.with_suffix(".rust")
                before.write_text(json.dumps(start))
                expected.write_text(json.dumps(graph))
                if pass_name in ("FuseIntoArrayAgg", "InsertLimiters"):
                    state = states[json.dumps(steps, sort_keys=True)]
                    if (state["entry_sequence"] != entry_sequence
                            or state["exit_sequence"] != record["observation"]["sequence"]):
                        raise ValueError(f"Pass state does not match {path}")
                    if pass_name == "InsertLimiters":
                        checkpoint = trace / f'{state["entry_sequence"]:06}-limiters-input.json'
                        checkpoint_result = trace / f'{state["exit_sequence"]:06}-limiters-result.json'
                        sidecar = prefix.with_suffix(".state")
                        write_limiter_inputs(checkpoint, state, sidecar)
                        command = [compiler, "--insert-limiters", sidecar, before]
                        resumed = json.loads(subprocess.check_output([*command[:-1], checkpoint]))
                        if "warnings_after" in state:
                            # Java deduplicates warnings that the compiler already reported.
                            warnings = state["warnings_before"].copy()
                            for warning in resumed.get("warnings", []):
                                if warning not in warnings:
                                    warnings.append(warning)
                            if warnings != state["warnings_after"]:
                                raise ValueError(f"Checkpoint warnings differ at {path}: {warnings}")
                        if resumed["next_generated_name"] != state["next_generated_name_after"]:
                            raise ValueError(f"Checkpoint name state differs at {path}")
                        checkpoint_actual = prefix.with_suffix(".rust-checkpoint")
                        checkpoint_actual.write_text(json.dumps(resumed["circuit"]))
                        subprocess.run([comparator, checkpoint_result, checkpoint_actual], check=True)
                    else:
                        command = [compiler, "--fuse-array-agg", str(state["next_generated_name"]), before]
                    result = json.loads(subprocess.check_output(command))
                    if result["next_generated_name"] != state["next_generated_name_after"]:
                        raise ValueError(f"Generated name after {path} differs: "
                                         f'Rust {result["next_generated_name"]}, '
                                         f'Java {state["next_generated_name_after"]}')
                    actual.write_text(json.dumps(result["circuit"]))
                else:
                    with actual.open("wb") as out:
                        subprocess.run([compiler, "--cse", before], stdout=out, check=True)
                subprocess.run([comparator, expected, actual], check=True)
                cases.append({"sequence": record["observation"]["sequence"],
                              "before": len(start["allOperators"]), "after": len(graph["allOperators"]),
                              "wire_changed": start != graph})
                start = None
            break
        previous = graph
    if start is not None or not cases:
        raise ValueError(f"Incomplete {pass_name} trace in {trace}")
    return cases


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--pass-name", choices=["CSE", "FuseIntoArrayAgg", "InsertLimiters"], default="CSE")
    parser.add_argument("--compiler", required=True)
    parser.add_argument("--comparator", required=True)
    args = parser.parse_args()
    args.output.mkdir()
    results = compare(args.trace, args.output, args.compiler, args.comparator, args.pass_name)
    (args.output / "complete.json").write_text(json.dumps(results, indent=2) + "\n")
