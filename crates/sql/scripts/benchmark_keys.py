#!/usr/bin/env python3
"""Run the fixed or mixed nullable key sort/merge benchmark on one Linux CPU.

Prepare a gitignored .bench-results archive before running. Copy the complete
output into it afterward. Run without competing compiler/build jobs.
"""

import argparse
import itertools
import json
import os
import platform
import statistics
import subprocess
from pathlib import Path

from benchmark_seams import bootstrap_ratio
from compare_parsers import digest

CORPUS_KEYS = {
    "unit": (0, 0),
    "integer": (1, 9),
    "integer_pair": (2, 18),
    "nullable_decimal": (1, 17),
    "unsigned_time": (1, 17),
}


def summarize(rows, samples, mixed=False, corpus=False):
    groups = {}
    distributions = ("Uniform", "CommonPrefix", "Duplicates")
    default_kind = "mixed" if mixed else "bigint"
    if corpus:
        expected = {
            (size, columns, distribution, key_type)
            for size in (4096, 65536)
            for key_type, (columns, _) in CORPUS_KEYS.items()
            for distribution in (("Uniform",) if key_type == "unit" else distributions)
        }
    else:
        expected = set(itertools.product(
            (4096, 65536), (3,) if mixed else (1, 4, 12), distributions, (default_kind,)
        ))
    timings = (
        "direct_sort_ns",
        "direct_merge_ns",
        "encode_ns",
        "normalized_sort_ns",
        "normalized_merge_ns",
    )
    for row in rows:
        key_type = row.get("key_type", default_kind)
        key = (row["rows"], row["columns"], row["distribution"], key_type)
        if key not in expected:
            raise ValueError(f"Unexpected workload: {key}")
        iteration = row["iteration"]
        group = groups.setdefault(key, {})
        if type(iteration) is not int or iteration in group:
            raise ValueError("Missing or repeated iteration")
        byte_count = row["key_bytes"]
        if type(byte_count) is not int:
            raise ValueError("Key byte count must be an integer")
        slots = row["rows"] * row["columns"]
        if mixed:
            valid_size = slots <= byte_count <= 16 * 1024 * 1024
        elif corpus:
            valid_size = byte_count == row["rows"] * CORPUS_KEYS[key_type][1]
        else:
            valid_size = byte_count == slots * 9
        # An empty tuple writes zero bytes. Its encode can complete within
        # a single clock tick.
        valid_timing = all(
            type(row[name]) is int and row[name] >= (
                0 if corpus and key_type == "unit" and name == "encode_ns" else 1
            )
            for name in timings
        )
        if (
            row["warmup"] is not (iteration < 2)
            or row["direct_first"] is not (iteration % 2 == 0)
            or row["seed"] != 0x9E3779B97F4A7C15 + iteration
            or not valid_size
            or not valid_timing
        ):
            raise ValueError("Invalid timing or input identity")
        group[iteration] = row
    if groups.keys() != expected:
        raise ValueError("Missing workloads")
    result = []
    for (size, columns, distribution, key_type), group in sorted(groups.items()):
        if group.keys() != set(range(samples + 2)):
            raise ValueError("Incomplete sample population")
        measured = [group[i] for i in range(2, samples + 2)]
        comparisons = {}
        for name, direct_fields, normalized_fields in (
            ("sort_reused_keys", ("direct_sort_ns",), ("normalized_sort_ns",)),
            (
                "sort_including_encoding",
                ("direct_sort_ns",),
                ("encode_ns", "normalized_sort_ns"),
            ),
            ("merge_reused_keys", ("direct_merge_ns",), ("normalized_merge_ns",)),
            (
                "encode_sort_merge",
                ("direct_sort_ns", "direct_merge_ns"),
                ("encode_ns", "normalized_sort_ns", "normalized_merge_ns"),
            ),
        ):
            direct = [sum(row[f] for f in direct_fields) for row in measured]
            normalized = [sum(row[f] for f in normalized_fields) for row in measured]
            comparisons[name] = {
                "direct_mean_ns": statistics.mean(direct),
                "normalized_mean_ns": statistics.mean(normalized),
                "normalized_over_direct": sum(normalized) / sum(direct),
                "paired_bootstrap_95_percent_interval": bootstrap_ratio(
                    direct, normalized
                ),
            }
        sizes = [row["key_bytes"] for row in measured]
        if mixed:
            storage = {"key_bytes_min": min(sizes), "key_bytes_max": max(sizes)}
        else:
            storage = {"key_bytes": sizes[0]}
            if not corpus:
                storage["batch_payload_and_null_bytes"] = size * (8 * columns + 8)
        result.append(
            {
                "rows": size,
                "columns": columns,
                "distribution": distribution,
                "key_type": key_type,
                "samples": samples,
                **storage,
                "phase_mean_ns": {
                    name: statistics.mean(row[name] for row in measured)
                    for name in timings
                },
                "comparisons": comparisons,
            }
        )
    return result


def source_hashes():
    root = Path(__file__).resolve().parents[1]
    files = [Path(__file__), root / "scripts/benchmark_seams.py"]
    files.extend((root / "native-probe/src").glob("*.rs"))
    files.extend(
        root / "native-probe" / name
        for name in ("Cargo.toml", "Cargo.lock", "examples/key_benchmark.rs")
    )
    return {str(p): digest(p) for p in sorted(files)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=31)
    profile = parser.add_mutually_exclusive_group()
    profile.add_argument("--mixed", action="store_true")
    profile.add_argument("--corpus", action="store_true")
    args = parser.parse_args()
    if not 5 <= args.samples <= 1000:
        parser.error("Expected 5..=1000 samples")
    args.binary = args.binary.resolve(strict=True)
    args.output.mkdir()
    cpu = min(os.sched_getaffinity(0))
    command = ["taskset", "-c", str(cpu), str(args.binary), str(args.samples)]
    if args.mixed:
        command.append("--mixed")
    if args.corpus:
        command.append("--corpus")
    provenance = {
        "schema_version": 1,
        "command": command,
        "platform": platform.platform(),
        "cpuinfo": Path("/proc/cpuinfo").read_text(),
        "meminfo": Path("/proc/meminfo").read_text(),
        "loadavg_before": Path("/proc/loadavg").read_text(),
        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True),
        "binary_sha256": digest(args.binary),
        "sources": source_hashes(),
        "samples": args.samples,
        "warmups": 2,
        "mixed": args.mixed,
        "corpus": args.corpus,
        "limitations": [
            ("Synthetic values for all five index-key shapes in the frozen ten programs."
             if args.corpus else "Synthetic nullable text, DECIMAL mantissa, and BIGINT keys."
             if args.mixed else "Synthetic nullable-BIGINT keys."),
            "Baseline is a dynamic field comparator, not generated typed Rust tuples.",
            "Warm input caches; VM host load/frequency are uncontrolled.",
            "Sorting permutes row indices; it does not move payloads.",
            "Merge allocation is timed; input/index preparation and validation are not.",
            "Phase sums exclude key destruction and input preparation.",
            "No archive, checkpoint, DBSP execution, throughput or total RSS measurement.",
        ],
    }
    (args.output / "provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )
    raw = args.output / "samples.jsonl"
    with raw.open("x") as stdout, (args.output / "stderr.log").open("x") as stderr:
        process = subprocess.run(
            command, stdout=stdout, stderr=stderr, timeout=600, check=False
        )
    (args.output / "process-result.json").write_text(
        json.dumps({"returncode": process.returncode}) + "\n"
    )
    if process.returncode:
        raise ValueError("Key benchmark failed; retain the partial output")
    if (
        source_hashes() != provenance["sources"]
        or digest(args.binary) != provenance["binary_sha256"]
    ):
        raise ValueError("Sources or binary changed during measurement")
    result = {
        "schema_version": 1,
        "complete": True,
        "workloads": summarize(
            [json.loads(line) for line in raw.read_text().splitlines()],
            args.samples, args.mixed, args.corpus,
        ),
        "samples_sha256": digest(raw),
        "production_budget_approved": False,
    }
    (args.output / "complete.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
