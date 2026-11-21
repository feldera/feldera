#!/usr/bin/env python3
"""Prepare a validated corpus for JVM replay and compare completed fingerprints."""

import argparse
import gzip
import hashlib
import itertools
import json
import subprocess
from pathlib import Path


def open_cases(directory, manifest):
    name = manifest.get("records_file", "cases.jsonl")
    if name == "cases.jsonl.gz":
        return gzip.open(directory / name, "rt", encoding="utf-8")
    if name == "cases.jsonl":
        return (directory / name).open(encoding="utf-8")
    raise ValueError("Unsupported case-record file")


def prepare(corpus: Path, output: Path, comparator: str, record_skipped=None) -> dict:
    subprocess.run([comparator, "--validate-corpus", str(corpus)], check=True)
    count = 0
    identities = set()
    skipped = []
    with output.open("x") as stream:
        for path in sorted(corpus.glob("*/case-*.json")):
            raw = path.read_bytes()
            case = json.loads(raw)
            identity = f"{case['source_test']}/{case['case_index']}"
            if identity in identities:
                raise ValueError(f"Duplicate case {identity}")
            identities.add(identity)
            if case["status"] != "exported":
                if record_skipped is None:
                    raise ValueError(f"Cannot replay skipped case {path}")
                skipped.append(
                    {
                        "id": identity,
                        "reason": case["reason"],
                        "input_sha256": hashlib.sha256(raw).hexdigest(),
                    }
                )
                continue
            record = {
                key: case[key]
                for key in (
                    "program",
                    "submissions",
                    "options",
                    "schema",
                    "diagnostics",
                )
            }
            if "compilation_options" in case:
                record["options"] = case["compilation_options"]
            elif case["engine_mode"] == "gen2":
                raise ValueError(
                    f"Gen-2 replay requires effective compilation options: {path}"
                )
            record.update(id=identity, input_sha256=hashlib.sha256(raw).hexdigest())
            stream.write(json.dumps(record, separators=(",", ":")) + "\n")
            count += 1
    if not count:
        raise ValueError("Empty corpus")
    if record_skipped is not None:
        with record_skipped.open("x") as exclusions:
            json.dump(
                {
                    "schema_version": 1,
                    "registered_cases": len(identities),
                    "replayed_cases": count,
                    "skipped": skipped,
                    "approved_dispositions": False,
                },
                exclusions,
                indent=2,
            )
            exclusions.write("\n")
    return {
        "cases": count,
        "skipped_registered_cases": len(skipped),
        "input_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
    }


def compare(expected: Path, actual: Path) -> dict:
    left_manifest = json.loads((expected / "complete.json").read_text())
    right_manifest = json.loads((actual / "complete.json").read_text())
    for manifest in (left_manifest, right_manifest):
        if manifest.get("schema_version") != 1 or manifest.get("cases", 0) <= 0:
            raise ValueError("Missing or invalid completion record")
        if not all(
            manifest.get(key) for key in ("producer", "runtime", "input_sha256")
        ):
            raise ValueError("Missing build or input identity")
        if type(manifest.get("capture")) is not bool:
            raise ValueError("Missing capture mode")
    for field in ("schema_version", "cases", "input_sha256", "producer", "runtime"):
        if left_manifest[field] != right_manifest[field]:
            raise ValueError(f"Incompatible completion records: {field}")
    contract = left_manifest.get("artifact_contract", "exact-v1")
    if contract not in ("exact-v1", "seam-v1") or contract != right_manifest.get(
        "artifact_contract", "exact-v1"
    ):
        raise ValueError("Incompatible artifact comparison contracts")
    if contract == "seam-v1" and not (
        left_manifest["capture"] and right_manifest["capture"]
    ):
        raise ValueError("Seam artifact comparison requires captured observations")
    dataflow_contract = left_manifest.get("dataflow_contract", "gen2-only-v1")
    if dataflow_contract not in (
        "gen2-only-v1",
        "all-engines-v1",
    ) or dataflow_contract != right_manifest.get("dataflow_contract", "gen2-only-v1"):
        raise ValueError("Incompatible dataflow comparison contracts")
    transparency = left_manifest["capture"] != right_manifest["capture"]
    observation_contract = left_manifest.get("observation_contract", "full-prefix-v1")
    if observation_contract not in (
        "full-prefix-v1",
        "final-s3-v1",
    ) or observation_contract != right_manifest.get(
        "observation_contract", "full-prefix-v1"
    ):
        raise ValueError("Incompatible observation comparison contracts")
    if observation_contract == "final-s3-v1" and (
        transparency or not left_manifest["capture"]
    ):
        raise ValueError("Final S3 comparison requires captured observations")
    artifact_byte_differences = []
    differences = []
    original_schema_differences = []
    original_diagnostic_differences = []
    failures = {"expected": [], "actual": []}
    count = 0
    observations = 0
    identities = set()
    with (
        open_cases(expected, left_manifest) as left,
        open_cases(actual, right_manifest) as right,
    ):
        for a, b in itertools.zip_longest(left, right):
            if a is None or b is None:
                raise ValueError("Truncated replay results")
            a, b = json.loads(a), json.loads(b)
            if a["id"] != b["id"] or a["input_sha256"] != b["input_sha256"]:
                raise ValueError("Different input cases or ordering")
            if a["id"] in identities:
                raise ValueError("Duplicate case identity")
            identities.add(a["id"])
            count += 1
            for record, manifest in ((a, left_manifest), (b, right_manifest)):
                if "failure" in record:
                    if (
                        set(record) != {"id", "input_sha256", "failure"}
                        or not isinstance(record["failure"], dict)
                        or set(record["failure"]) != {"class", "message"}
                        or not all(
                            isinstance(value, str) and value
                            for value in record["failure"].values()
                        )
                    ):
                        raise ValueError("Invalid failed-case record")
                    continue
                artifacts = "gen2" if "gen2" in record else "rust"
                if dataflow_contract == "all-engines-v1" and not record[artifacts].get(
                    "dataflow.json"
                ):
                    raise ValueError("Missing dataflow fingerprint")
                required = {
                    "id",
                    "input_sha256",
                    "schema",
                    "diagnostics",
                    "exported_schema_equal",
                    "exported_diagnostics_equal",
                    artifacts,
                    "hash_inputs",
                }
                if artifacts == "gen2" and contract == "seam-v1":
                    required.add("gen2_bytes")
                    if not record.get("gen2_bytes"):
                        raise ValueError("Missing raw artifact fingerprints")
                if manifest["capture"]:
                    required.add("observations")
                    stages = record.get("observations", {})
                    expected_stages = (
                        {"S3"}
                        if observation_contract == "final-s3-v1"
                        else {"S2", "S3", "S4"}
                    )
                    if set(stages) != expected_stages:
                        raise ValueError("Missing observation stage")
                    if stages["S3"]["count"] != 1 or (
                        observation_contract == "full-prefix-v1"
                        and (stages["S2"]["count"] != 1 or stages["S4"]["count"] <= 0)
                    ):
                        raise ValueError("Invalid observation counts")
                if (
                    set(record) != required
                    or not record[artifacts]
                    or not record["hash_inputs"]
                ):
                    raise ValueError("Incomplete case result")
            if "failure" in a or "failure" in b:
                for side, record in (("expected", a), ("actual", b)):
                    if "failure" in record:
                        failures[side].append(record["id"])
                differences.append({"id": a["id"], "fields": ["failure"]})
                continue
            observations += sum(
                stage["count"] for stage in a.get("observations", {}).values()
            )
            if not a["exported_schema_equal"] or not b["exported_schema_equal"]:
                original_schema_differences.append(a["id"])
            if (
                not a["exported_diagnostics_equal"]
                or not b["exported_diagnostics_equal"]
            ):
                original_diagnostic_differences.append(a["id"])
            if transparency:
                a.pop("observations", None)
                b.pop("observations", None)
            if contract == "seam-v1":
                raw_a = a.pop("gen2_bytes", None)
                raw_b = b.pop("gen2_bytes", None)
                if raw_a != raw_b:
                    artifact_byte_differences.append(a["id"])
            changed = [
                key for key in sorted(a.keys() | b.keys()) if a.get(key) != b.get(key)
            ]
            if changed:
                differences.append({"id": a["id"], "fields": changed})
    if count != left_manifest["cases"]:
        raise ValueError("Case count does not match the completion record")
    for side, manifest in (("expected", left_manifest), ("actual", right_manifest)):
        declared = manifest.get("failed_cases", 0)
        if type(declared) is not int or declared != len(failures[side]):
            raise ValueError("Failed-case count does not match the completion record")
    return {
        "equal": not differences,
        "cases": count,
        "observations": observations,
        "capture_transparency": transparency,
        "artifact_contract": contract,
        "dataflow_contract": dataflow_contract,
        "observation_contract": observation_contract,
        "artifact_byte_differences": artifact_byte_differences,
        "differences": differences,
        "failed_cases": failures,
        "exported_schema_differences": original_schema_differences,
        "exported_diagnostic_differences": original_diagnostic_differences,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preparation = commands.add_parser("prepare")
    preparation.add_argument("corpus", type=Path)
    preparation.add_argument("output", type=Path)
    preparation.add_argument("--comparator", required=True)
    preparation.add_argument(
        "--record-skipped",
        type=Path,
        help="Measure only exported cases and retain every registered skip in this file; does not approve exclusions",
    )
    comparison = commands.add_parser("compare")
    comparison.add_argument("expected", type=Path)
    comparison.add_argument("actual", type=Path)
    args = parser.parse_args()
    if args.command == "prepare":
        result = prepare(args.corpus, args.output, args.comparator, args.record_skipped)
    else:
        result = compare(args.expected, args.actual)
    print(json.dumps(result, indent=2))
    return 0 if result.get("equal", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
