#!/usr/bin/env python3
"""Recheck seam fingerprints and prove disjoint, complete prepared-input coverage."""

import argparse
import json
from pathlib import Path

from determinism import compare, open_cases
from run_determinism import file_digest


def read_input(path):
    cases = {}
    with path.open() as stream:
        for line in stream:
            case = json.loads(line)
            identity = case["id"]
            if not isinstance(identity, str) or not identity or identity in cases:
                raise ValueError(f"Missing or duplicate input identity in {path}")
            if not case.get("input_sha256"):
                raise ValueError(f"Missing original input digest: {identity}")
            cases[identity] = case
    if not cases:
        raise ValueError(f"Empty input: {path}")
    return cases


def summarize(inputs, shards):
    expected = {}
    for path in inputs:
        cases = read_input(path)
        if expected.keys() & cases.keys():
            raise ValueError("Prepared input populations overlap")
        expected.update(cases)
    if not expected or not shards:
        raise ValueError("Input population and replay shards must be nonempty")
    seen = set()
    build = None
    reports = []
    total_observations = 0
    identity_fields = ("producer", "runtime", "classpath", "comparator_sha256", "stage")
    for shard in shards:
        completion = shard / "complete.json"
        report = json.loads(completion.read_text())
        identity = {key: report[key] for key in identity_fields}
        if not all(identity.values()) or identity["stage"] not in ("S2", "S3"):
            raise ValueError(f"Missing build or stage identity: {shard}")
        if build is None:
            build = identity
        elif identity != build:
            raise ValueError("Replay shards use different builds or stages")
        if report["equal"] is not True or report["runtime_executed"] is not False:
            raise ValueError(f"Shard is not a passing compile/replay audit: {shard}")
        if report["dataflow_contract"] != "all-engines-v1":
            raise ValueError("Every engine must compare dataflow fingerprints")
        if report["artifact_contract"] != "seam-v1" or report["jvm_runs"] != 2:
            raise ValueError(f"Unexpected seam contract or JVM count: {shard}")
        observation_contract = (
            "full-prefix-v1" if identity["stage"] == "S2" else "final-s3-v1"
        )
        if report["observation_contract"] != observation_contract:
            raise ValueError(f"Incomplete observation contract for {identity['stage']}")
        selected_path = shard / "input.jsonl"
        selected = read_input(selected_path)
        if file_digest(selected_path) != report["input_sha256"]:
            raise ValueError(f"Shard input digest changed: {shard}")
        if selected.keys() & seen:
            raise ValueError("Replay shard populations overlap")
        for key, case in selected.items():
            if expected.get(key) != case:
                raise ValueError(f"Replay input differs from prepared input: {key}")
        seen.update(selected)
        capture = "capture" if build["stage"] == "S2" else "capture-s3"
        replay = "replay-" + build["stage"].lower()
        artifacts = {}
        for mode in (capture, replay):
            directory = shard / mode
            manifest_path = directory / "complete.json"
            manifest = json.loads(manifest_path.read_text())
            for key in ("producer", "runtime", "input_sha256", "cases"):
                if manifest[key] != report[key]:
                    raise ValueError(f"Shard completion disagrees with {mode}: {key}")
            with open_cases(directory, manifest) as stream:
                actual = [
                    (case["id"], case["input_sha256"])
                    for line in stream
                    if (case := json.loads(line))
                ]
            wanted = [(case["id"], case["input_sha256"]) for case in selected.values()]
            if actual != wanted:
                raise ValueError(
                    f"Replay population/order differs from input: {directory}"
                )
            records = directory / manifest.get("records_file", "cases.jsonl")
            artifacts[str(manifest_path)] = file_digest(manifest_path)
            artifacts[str(records)] = file_digest(records)
        checked = compare(shard / capture, shard / replay)
        checked["equal"] &= (
            not checked["exported_schema_differences"]
            and not checked["exported_diagnostic_differences"]
        )
        if any(report.get(key) != value for key, value in checked.items()):
            raise ValueError(f"Fresh comparison differs from shard summary: {shard}")
        total_observations += checked["observations"]
        reports.append(
            {
                "directory": str(shard),
                "completion_sha256": file_digest(completion),
                "input_sha256": file_digest(selected_path),
                "cases": len(selected),
                "observations": checked["observations"],
                "artifacts": artifacts,
            }
        )
    if seen != expected.keys():
        raise ValueError(
            f"Replay population is incomplete: {len(expected.keys() - seen)} missing cases"
        )
    return {
        "schema_version": 1,
        **build,
        "cases": len(seen),
        "observations_per_side": total_observations,
        "jvm_runs": 2 * len(shards),
        "equal": True,
        "runtime_executed": False,
        "dataflow_contract": "all-engines-v1",
        "artifact_contract": "seam-v1",
        "complete_prepared_population": True,
        "inputs": {str(path): file_digest(path) for path in inputs},
        "shards": reports,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--input", type=Path, action="append", required=True)
    parser.add_argument("--shard", type=Path, action="append", required=True)
    args = parser.parse_args()
    result = summarize(args.input, args.shard)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(
        json.dumps(
            {key: result[key] for key in ("cases", "observations_per_side", "equal")}
        )
    )


if __name__ == "__main__":
    main()
