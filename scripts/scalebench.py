#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "feldera",
# ]
# ///
"""
Opinionated script to benchmark "micro-pipelines" with different configurations.
The script relies on `fda bench` so it needs fda installed.

```
$ ./scripts/scalebench.py --help
```

Example usage:

```
$ ./scripts/scalebench.py --mem-bw --program u64 --workers 1,4,8,16,20 \
  --payload 8 --duration 120 --repetition 1 \
  --version-suffix dbsp-sorting
```
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Tuple

from feldera import FelderaClient, PipelineBuilder
from feldera.runtime_config import RuntimeConfig

PROGRAMS = ["u64", "binary", "string", "binary_primary_key"]
WORKER_COUNTS = [1, 2, 4, 8, 12, 16, 20]
DEFAULT_PAYLOAD_BYTES = [8, 128, 512, 4096, 32768]
BENCH_DURATION_S = 120
CSV_PATH = "bench_results.csv"
MEM_BW_TOOL = "AMDuProfPcm"
MEM_BW_COOLDOWN_S = 5


def default_payload_bytes(program: str) -> list[int]:
    if program == "u64":
        return [8]
    return DEFAULT_PAYLOAD_BYTES


def parse_positive_csv_ints(value: str, label: str) -> list[int]:
    """
    Parses a list of positive integers provided as a string
    e.g., to turn CLI arguments like --threads 1,2,3 into [1,2,3]
    """
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
    """
    Validate payload bytes for all programs and return it as a list.
    The u64 program can not have payloads bigger than 8 bytes.
    """
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
    elif program == "binary":
        table_fields = "    payload BINARY NOT NULL\n"
        fields = {
            "payload": {
                "range": [payload_bytes, payload_bytes + 1],
                "value": {"strategy": "uniform"},
            }
        }
    elif program == "string":
        table_fields = "    payload string NOT NULL\n"
        fields = {
            "payload": {
                "range": [payload_bytes, payload_bytes + 1],
                "strategy": "words",
            }
        }
    elif program == "binary_primary_key":
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
    "Find json payload in fda bench output and parse it."
    lines = [line for line in output.splitlines() if line.strip()]
    for idx, line in enumerate(lines):
        if line.lstrip().startswith("{"):
            json_text = "\n".join(lines[idx:])
            decoder = json.JSONDecoder()
            obj, _ = decoder.raw_decode(json_text)
            return obj
    raise ValueError("No JSON payload found in fda bench output")


def check_mem_bw_requirements() -> None:
    if not is_amd_cpu():
        raise SystemExit(
            "Memory bandwidth collection requires an AMD CPU. "
            "Detected a non-AMD processor."
        )
    if shutil.which(MEM_BW_TOOL) is None:
        raise SystemExit(
            f"{MEM_BW_TOOL} not found in PATH. Install AMDuProfPcm and add it to PATH."
        )
    if not is_kernel_module_loaded("amd_uncore"):
        raise SystemExit(
            "Kernel module amd_uncore is not loaded. Run: sudo modprobe amd_uncore"
        )
    perf_val = read_file_to_string("/proc/sys/kernel/perf_event_paranoid")
    if perf_val != "0":
        raise SystemExit(
            "kernel.perf_event_paranoid must be 0 for AMDuProfPcm. "
            "Run: sudo sysctl -w kernel.perf_event_paranoid=0"
        )
    nmi_watchdog = read_file_to_string("/proc/sys/kernel/nmi_watchdog")
    if nmi_watchdog != "0":
        raise SystemExit(
            "kernel.nmi_watchdog must be 0 for AMDuProfPcm. "
            "Run: sudo sysctl -w kernel.nmi_watchdog=0"
        )


def is_amd_cpu() -> bool:
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as file_obj:
            for line in file_obj:
                if line.startswith("vendor_id"):
                    _, value = line.split(":", 1)
                    return value.strip() == "AuthenticAMD"
    except OSError:
        return False
    return False


def is_kernel_module_loaded(name: str) -> bool:
    try:
        with open("/proc/modules", encoding="utf-8") as file_obj:
            for line in file_obj:
                if line.split(" ", 1)[0] == name:
                    return True
    except OSError:
        return False
    return False


def read_file_to_string(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as file_obj:
            return file_obj.read().strip()
    except OSError:
        return ""


def start_mem_bw_monitor(output_path: str) -> subprocess.Popen[str]:
    cmd = [MEM_BW_TOOL, "-m", "memory", "-a", "-o", output_path]
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def stop_mem_bw_monitor(proc: subprocess.Popen[str]) -> tuple[str, int | None]:
    if proc.poll() is None:
        proc.send_signal(signal.SIGINT)
    try:
        _stdout, stderr = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.terminate()
        _stdout, stderr = proc.communicate(timeout=5)
    return stderr.strip(), proc.returncode


def parse_mem_bw_csv(path: str) -> tuple[Dict[str, Any], str]:
    "Parse the numbers returned by AmdUProf, see `start_mem_bw_monitor`"
    try:
        raw_text = open(path, encoding="utf-8", errors="replace").read()
    except OSError as exc:
        raise RuntimeError(f"Failed to read memory bandwidth CSV: {exc}") from exc

    lines = raw_text.splitlines()
    header_idx = None
    for idx, line in enumerate(lines):
        if (
            "Total Mem Bw" in line
            and "Total Mem RdBw" in line
            and "Total Mem WrBw" in line
        ):
            header_idx = idx
            break
    if header_idx is None:
        raise RuntimeError("Memory bandwidth CSV missing headers.")

    header_row = next(csv.reader([lines[header_idx]]))
    total_idx = None
    read_idx = None
    write_idx = None
    for i, header in enumerate(header_row):
        lower = header.lower()
        if "total mem bw" in lower:
            total_idx = i
        elif "mem rdbw" in lower or ("read" in lower and "bw" in lower):
            read_idx = i
        elif "mem wrbw" in lower or ("write" in lower and "bw" in lower):
            write_idx = i

    if total_idx is None or read_idx is None or write_idx is None:
        raise RuntimeError(
            "Failed to locate read/write/total bandwidth columns in "
            f"{path}. Columns: {header_row}"
        )

    read_vals: list[float] = []
    write_vals: list[float] = []
    total_vals: list[float] = []
    for line in lines[header_idx + 1 :]:
        if not line.strip():
            if read_vals:
                break
            continue
        row = next(csv.reader([line]))
        total_val = parse_float(row[total_idx]) if total_idx < len(row) else None
        read_val = parse_float(row[read_idx]) if read_idx < len(row) else None
        write_val = parse_float(row[write_idx]) if write_idx < len(row) else None
        if total_val is None or read_val is None or write_val is None:
            if read_vals:
                break
            continue
        total_vals.append(total_val)
        read_vals.append(read_val)
        write_vals.append(write_val)

    if not read_vals:
        raise RuntimeError("No memory bandwidth samples parsed from CSV.")

    read_stats = compute_stats(read_vals)
    write_stats = compute_stats(write_vals)
    total_stats = compute_stats(total_vals)

    raw_clean = raw_text.replace("\r\n", "\n").replace("\n", "\\n")

    metrics = {
        "mem_bw_read_min": read_stats["min"],
        "mem_bw_read_max": read_stats["max"],
        "mem_bw_read_mean": read_stats["mean"],
        "mem_bw_read_stdev": read_stats["stdev"],
        "mem_bw_write_min": write_stats["min"],
        "mem_bw_write_max": write_stats["max"],
        "mem_bw_write_mean": write_stats["mean"],
        "mem_bw_write_stdev": write_stats["stdev"],
        "mem_bw_total_min": total_stats["min"],
        "mem_bw_total_max": total_stats["max"],
        "mem_bw_total_mean": total_stats["mean"],
        "mem_bw_total_stdev": total_stats["stdev"],
        "mem_bw_samples": len(read_vals),
        "mem_bw_csv": raw_clean,
    }
    return metrics, raw_text


def parse_float(value: Any) -> float | None:
    "Parse numbers and return as a float, return None in case it's not a number"
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def compute_stats(values: list[float]) -> Dict[str, float]:
    if not values:
        return {"min": 0.0, "max": 0.0, "mean": 0.0, "stdev": 0.0}
    mean_val = statistics.mean(values)
    stdev_val = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "min": min(values),
        "max": max(values),
        "mean": mean_val,
        "stdev": stdev_val,
    }


def read_text_file(path: str) -> str:
    try:
        return open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        return ""


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
    "Open CSV file in append mode"
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


def parse_semver_build_suffix(value: str) -> str:
    "Parse --version-suffix CLI argument"
    raw = value.strip()
    if not raw:
        raise argparse.ArgumentTypeError("--version-suffix cannot be empty")
    cleaned = re.sub(r"[^0-9A-Za-z.-]+", "-", raw).strip(".-")
    if not cleaned:
        raise argparse.ArgumentTypeError(
            "--version-suffix must contain at least one alphanumeric character"
        )
    identifiers = [part.strip("-") for part in cleaned.split(".")]
    identifiers = [part for part in identifiers if part]
    if not identifiers:
        raise argparse.ArgumentTypeError(
            "--version-suffix must contain at least one non-separator identifier"
        )
    return ".".join(identifiers)


def apply_semver_build_suffix(version: str, suffix: str | None) -> str:
    "Adds a `suffix` to version: `v2.3.1` -> `v2.3.1+suffix`"
    if not suffix:
        return version
    if "+" in version:
        prefix, build_metadata = version.split("+", 1)
        if build_metadata:
            return f"{prefix}+{build_metadata}.{suffix}"
        return f"{prefix}+{suffix}"
    return f"{version}+{suffix}"


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
        default="u64",
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
    parser.add_argument(
        "--mem-bw",
        action="store_true",
        help="Collect memory bandwidth metrics using AMDuProfPcm.",
    )
    parser.add_argument(
        "--version-suffix",
        type=parse_semver_build_suffix,
        help=(
            "Append semver build metadata to the platform version in CSV output, "
            "e.g. sort-change -> 0.23.0+sort-change."
        ),
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
    if args.mem_bw and not dry_run:
        check_mem_bw_requirements()

    client = None if dry_run else FelderaClient()
    worker_counts = (
        parse_positive_csv_ints(args.workers, "workers")
        if args.workers
        else WORKER_COUNTS
    )
    payload_bytes_list = (
        parse_positive_csv_ints(args.payload_bytes, "payload-bytes")
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
        if args.version_suffix:
            print(f"dry-run: version suffix={args.version_suffix}")
        if args.mem_bw:
            print("dry-run: memory bandwidth collection enabled")

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
        "mem_bw_read_min",
        "mem_bw_read_max",
        "mem_bw_read_mean",
        "mem_bw_read_stdev",
        "mem_bw_write_min",
        "mem_bw_write_max",
        "mem_bw_write_mean",
        "mem_bw_write_stdev",
        "mem_bw_total_min",
        "mem_bw_total_max",
        "mem_bw_total_mean",
        "mem_bw_total_stdev",
        "mem_bw_samples",
        "mem_bw_csv",
    ]
    writer = None
    file_obj = None
    if not dry_run:
        writer, file_obj = open_csv(CSV_PATH, fieldnames)

    run_idx = 0
    try:
        for payload_bytes in payload_bytes_list:
            for run_id in range(1, repetitions + 1):
                for workers in worker_counts:
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
                        if args.mem_bw:
                            print("dry-run: run AMDuProfPcm during benchmark")
                        print(f"dry-run: append results to {CSV_PATH}")
                        if args.mem_bw and run_idx < total_runs:
                            print(f"dry-run: cooldown {MEM_BW_COOLDOWN_S}s")
                        continue

                    pipeline = PipelineBuilder(
                        client,
                        name=pipeline_name,
                        sql=sql,
                        runtime_config=runtime_config,
                    ).create_or_replace()
                    platform_version = apply_semver_build_suffix(
                        resolve_platform_version(pipeline), args.version_suffix
                    )

                    pipeline.clear_storage()

                    mem_bw_metrics: Dict[str, Any] = {}
                    mem_bw_proc = None
                    mem_bw_path = None

                    if args.mem_bw:
                        results_dir = os.path.dirname(CSV_PATH) or "."
                        mem_bw_path = os.path.join(
                            results_dir,
                            f"mem_bw_{pipeline_name}_{workers}_{payload_bytes}_{run_id}.csv",
                        )
                        mem_bw_proc = start_mem_bw_monitor(mem_bw_path)

                    bench_payload = None
                    try:
                        bench_payload = run_fda_bench(pipeline_name, duration_s)
                    finally:
                        if (
                            args.mem_bw
                            and mem_bw_proc is not None
                            and mem_bw_path is not None
                        ):
                            stderr, returncode = stop_mem_bw_monitor(mem_bw_proc)
                            if stderr:
                                print(
                                    f"AMDuProfPcm stderr: {stderr}",
                                    file=sys.stderr,
                                )
                            if returncode not in (None, 0):
                                print(
                                    f"Warning: AMDuProfPcm exited with code {returncode}.",
                                    file=sys.stderr,
                                )
                            try:
                                mem_bw_metrics, _raw = parse_mem_bw_csv(mem_bw_path)
                            except Exception as exc:  # noqa: BLE001
                                print(
                                    f"Warning: failed to parse memory bandwidth CSV: {exc}",
                                    file=sys.stderr,
                                )
                                raw_text = read_text_file(mem_bw_path)
                                if raw_text:
                                    mem_bw_metrics["mem_bw_csv"] = raw_text.replace(
                                        "\r\n", "\n"
                                    ).replace("\n", "\\n")
                            finally:
                                try:
                                    os.remove(mem_bw_path)
                                except OSError:
                                    pass
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
                        "mem_bw_read_min": mem_bw_metrics.get("mem_bw_read_min", ""),
                        "mem_bw_read_max": mem_bw_metrics.get("mem_bw_read_max", ""),
                        "mem_bw_read_mean": mem_bw_metrics.get("mem_bw_read_mean", ""),
                        "mem_bw_read_stdev": mem_bw_metrics.get(
                            "mem_bw_read_stdev", ""
                        ),
                        "mem_bw_write_min": mem_bw_metrics.get("mem_bw_write_min", ""),
                        "mem_bw_write_max": mem_bw_metrics.get("mem_bw_write_max", ""),
                        "mem_bw_write_mean": mem_bw_metrics.get(
                            "mem_bw_write_mean", ""
                        ),
                        "mem_bw_write_stdev": mem_bw_metrics.get(
                            "mem_bw_write_stdev", ""
                        ),
                        "mem_bw_total_min": mem_bw_metrics.get("mem_bw_total_min", ""),
                        "mem_bw_total_max": mem_bw_metrics.get("mem_bw_total_max", ""),
                        "mem_bw_total_mean": mem_bw_metrics.get(
                            "mem_bw_total_mean", ""
                        ),
                        "mem_bw_total_stdev": mem_bw_metrics.get(
                            "mem_bw_total_stdev", ""
                        ),
                        "mem_bw_samples": mem_bw_metrics.get("mem_bw_samples", ""),
                        "mem_bw_csv": mem_bw_metrics.get("mem_bw_csv", ""),
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
                    if args.mem_bw and run_idx < total_runs:
                        time.sleep(MEM_BW_COOLDOWN_S)
    finally:
        if file_obj is not None:
            file_obj.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
