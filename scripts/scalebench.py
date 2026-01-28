#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "feldera",
# ]
# ///
"""Benchmark Feldera pipeline performance across workers and payload sizes."""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Tuple

from feldera import FelderaClient, PipelineBuilder
from feldera.runtime_config import RuntimeConfig

PROGRAMS = ["u64", "u64_binary", "u64_binary_primary_key"]
WORKER_COUNTS = [1, 2, 4, 8, 12, 16, 20]
DEFAULT_PAYLOAD_BYTES = [8, 128, 512, 4096, 32768]
BENCH_DURATION_S = 120
CSV_PATH = "bench_results.csv"


def default_payload_bytes(program: str) -> list[int]:
    if program == "u64":
        return [8]
    return DEFAULT_PAYLOAD_BYTES


def parse_csv_ints(value: str, label: str) -> list[int]:
    items = [item.strip() for item in value.split(",") if item.strip()]
    if not items:
        raise argparse.ArgumentTypeError(f"{label} list cannot be empty")
    values = []
    for item in items:
        try:
            parsed = int(item)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"{label} values must be integers: {item}"
            ) from exc
        if parsed <= 0:
            raise argparse.ArgumentTypeError(f"{label} values must be positive: {item}")
        values.append(parsed)
    return values


def normalize_payload_bytes(
    program: str, payload_bytes: list[int], payload_arg_provided: bool
) -> list[int]:
    if program == "u64":
        if payload_arg_provided and any(size != 8 for size in payload_bytes):
            raise SystemExit(
                "Program u64 does not support payload sizes other than 8 bytes."
            )
        return [8]
    return payload_bytes


def make_sql(program: str, datagen_workers: int, payload_bytes: int) -> str:
    if program == "u64":
        table_fields = "    payload BIGINT NOT NULL\n"
        fields = None
    elif program == "u64_binary":
        table_fields = "    payload BINARY NOT NULL\n"
        fields = {
            "payload": {
                "range": [payload_bytes, payload_bytes + 1],
                "value": {"strategy": "uniform"},
            }
        }
    elif program == "u64_binary_primary_key":
        table_fields = (
            "    id BIGINT NOT NULL PRIMARY KEY,\n    payload BINARY NOT NULL\n"
        )
        fields = {
            "payload": {
                "range": [payload_bytes, payload_bytes + 1],
                "value": {"strategy": "uniform"},
            }
        }
    else:
        raise ValueError(f"Unknown program: {program}")

    plan_entry = {"fields": fields} if fields else {}
    connectors = json.dumps(
        [
            {
                "name": "data",
                "transport": {
                    "name": "datagen",
                    "config": {
                        "workers": datagen_workers,
                        "plan": [plan_entry],
                    },
                },
            }
        ],
        separators=(",", ":"),
    )
    return (
        "CREATE TABLE simple (\n"
        f"{table_fields}"
        ") WITH (\n"
        "  'materialized' = 'true',\n"
        f"  'connectors' = '{connectors}'\n"
        ");"
    )


def parse_bench_output(output: str) -> Dict[str, Any]:
    lines = [line for line in output.splitlines() if line.strip()]
    for idx, line in enumerate(lines):
        if line.lstrip().startswith("{"):
            json_text = "\n".join(lines[idx:])
            decoder = json.JSONDecoder()
            obj, _ = decoder.raw_decode(json_text)
            return obj
    raise ValueError("No JSON payload found in fda bench output")


def run_fda_bench(pipeline_name: str, duration_s: int) -> Dict[str, Any]:
    cmd = [
        "fda",
        "bench",
        pipeline_name,
        "--format",
        "json",
        "--duration",
        str(duration_s),
    ]
    result = subprocess.run(cmd, text=True, capture_output=True)
    if result.stderr:
        print(result.stderr.strip(), file=sys.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"fda bench failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return parse_bench_output(result.stdout)


def extract_metrics(payload: Dict[str, Any], pipeline_name: str) -> Dict[str, Any]:
    if pipeline_name in payload:
        metrics = payload[pipeline_name]
    elif len(payload) == 1:
        metrics = next(iter(payload.values()))
    else:
        raise KeyError(f"Benchmark output missing metrics for {pipeline_name}")

    throughput = metrics.get("throughput") or {}
    memory = metrics.get("memory") or {}
    storage = metrics.get("storage") or {}
    uptime = metrics.get("uptime") or {}
    state_amp = metrics.get("state-amplification") or {}
    buffered = (
        metrics.get("buffered_input_records")
        or metrics.get("buffered-input-records")
        or {}
    )

    return {
        "throughput_value": throughput.get("value"),
        "memory_value": memory.get("value"),
        "memory_lower_value": memory.get("lower_value"),
        "storage_value": storage.get("value"),
        "storage_lower_value": storage.get("lower_value"),
        "buffered_input_records_value": buffered.get("value"),
        "buffered_input_records_lower_value": buffered.get("lower_value"),
        "buffered_input_records_upper_value": buffered.get("upper_value"),
        "uptime_value": uptime.get("value"),
        "state_amplification_value": state_amp.get("value"),
    }


def open_csv(path: str, fieldnames: Iterable[str]) -> Tuple[csv.DictWriter, Any]:
    is_new = not os.path.exists(path)
    file_obj = open(path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
    if is_new:
        writer.writeheader()
        file_obj.flush()
    return writer, file_obj


def resolve_platform_version(pipeline: Any) -> str:
    descriptor = getattr(pipeline, "_inner", None)
    if descriptor is not None:
        version = getattr(descriptor, "platform_version", None)
        if version:
            return str(version)
    try:
        version = pipeline.platform_version()
        if version:
            return str(version)
    except Exception as exc:  # noqa: BLE001
        print(f"Warning: failed to read platform version: {exc}", file=sys.stderr)
    return "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark Feldera pipeline performance across workers/payload bytes."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned steps without running pipeline or benchmarks.",
    )
    parser.add_argument(
        "--workers",
        type=str,
        help="Comma-separated runtime worker counts, e.g. 1,2,4.",
    )
    parser.add_argument(
        "--payload-bytes",
        dest="payload_bytes",
        type=str,
        help="Comma-separated payload sizes in bytes, e.g. 8,8192.",
    )
    parser.add_argument(
        "--program",
        choices=PROGRAMS,
        default="u64_binary",
        help="Program to benchmark.",
    )
    parser.add_argument(
        "--repetition",
        type=int,
        default=3,
        help="Repeat each benchmark configuration N times.",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=BENCH_DURATION_S,
        help="Benchmark duration in seconds.",
    )
    args = parser.parse_args()
    dry_run = args.dry_run
    program = args.program
    pipeline_name = program
    repetitions = args.repetition
    duration_s = args.duration
    if repetitions <= 0:
        raise SystemExit("--repetition must be a positive integer")
    if duration_s <= 0:
        raise SystemExit("--duration must be a positive integer")

    client = None if dry_run else FelderaClient()
    worker_counts = (
        parse_csv_ints(args.workers, "workers") if args.workers else WORKER_COUNTS
    )
    payload_bytes_list = (
        parse_csv_ints(args.payload_bytes, "payload-bytes")
        if args.payload_bytes
        else default_payload_bytes(program)
    )
    payload_bytes_list = normalize_payload_bytes(
        program,
        payload_bytes_list,
        payload_arg_provided=args.payload_bytes is not None,
    )
    total_runs = len(worker_counts) * len(payload_bytes_list) * repetitions

    if dry_run:
        print(f"dry-run: program={program}")

    fieldnames = [
        "timestamp",
        "pipeline_name",
        "platform_version",
        "program_sql",
        "run_id",
        "pipeline_workers",
        "datagen_workers",
        "payload_bytes",
        "duration_s",
        "throughput_value",
        "memory_value",
        "memory_lower_value",
        "storage_value",
        "storage_lower_value",
        "buffered_input_records_value",
        "buffered_input_records_lower_value",
        "buffered_input_records_upper_value",
        "uptime_value",
        "state_amplification_value",
    ]
    writer = None
    file_obj = None
    if not dry_run:
        writer, file_obj = open_csv(CSV_PATH, fieldnames)

    run_idx = 0
    try:
        for workers in worker_counts:
            for payload_bytes in payload_bytes_list:
                for run_id in range(1, repetitions + 1):
                    run_idx += 1
                    payload_kib = payload_bytes / 1024.0
                    print(
                        f"[{run_idx}/{total_runs}] run_id={run_id} "
                        f"workers={workers} payload_kib={payload_kib:.3f}"
                    )

                    datagen_workers = max(8, workers + 8)
                    sql = make_sql(
                        program=program,
                        datagen_workers=datagen_workers,
                        payload_bytes=payload_bytes,
                    )
                    runtime_config = RuntimeConfig(workers=workers, storage=True)

                    program_sql = sql.replace("\n", "\\n")

                    if dry_run:
                        print(
                            f"dry-run: create/replace pipeline {pipeline_name} "
                            f"with workers={workers}, datagen_workers={datagen_workers}, "
                            f"payload_kib={payload_kib:.3f}, run_id={run_id}"
                        )
                        print("dry-run: sql:")
                        print(sql)
                        print("dry-run: clear storage")
                        print(
                            "dry-run: run "
                            f"fda bench {pipeline_name} --format json --duration {duration_s}"
                        )
                        print("dry-run: stop pipeline (force)")
                        print("dry-run: clear storage")
                        print(f"dry-run: append results to {CSV_PATH}")
                        continue

                    pipeline = PipelineBuilder(
                        client,
                        name=pipeline_name,
                        sql=sql,
                        runtime_config=runtime_config,
                    ).create_or_replace()
                    platform_version = resolve_platform_version(pipeline)

                    pipeline.clear_storage()

                    try:
                        bench_payload = run_fda_bench(pipeline_name, duration_s)
                    finally:
                        try:
                            pipeline.stop(force=True)
                        except Exception as exc:  # noqa: BLE001
                            print(
                                f"Warning: failed to stop pipeline: {exc}",
                                file=sys.stderr,
                            )
                        try:
                            pipeline.clear_storage()
                        except Exception as exc:  # noqa: BLE001
                            print(
                                f"Warning: failed to clear storage: {exc}",
                                file=sys.stderr,
                            )

                    metrics = extract_metrics(bench_payload, pipeline_name)
                    row = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "pipeline_name": pipeline_name,
                        "platform_version": platform_version,
                        "program_sql": program_sql,
                        "run_id": run_id,
                        "pipeline_workers": workers,
                        "datagen_workers": datagen_workers,
                        "payload_bytes": payload_bytes,
                        "duration_s": duration_s,
                        **metrics,
                    }
                    writer.writerow(row)
                    file_obj.flush()

                    print(
                        "Result "
                        f"run_id={run_id} "
                        f"throughput={metrics['throughput_value']} "
                        f"memory={metrics['memory_value']} "
                        f"storage={metrics['storage_value']} "
                        f"buffered_input_records={metrics['buffered_input_records_value']} "
                        f"uptime_ms={metrics['uptime_value']} "
                        f"state_amp={metrics['state_amplification_value']}"
                    )
    finally:
        if file_obj is not None:
            file_obj.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
