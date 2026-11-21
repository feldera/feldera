#!/usr/bin/env python3
"""Measure the paired incremental COUNT DISTINCT circuits on one Linux CPU."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile

from paired_projection import run, summary


def sample(binary, output, cpu, rows, steps, pair, engine):
    log = output / f"count-{rows}-{pair}-{engine}.log"
    with tempfile.TemporaryDirectory(prefix="state-", dir=output) as storage:
        env = os.environ | {
            "P0_NATIVE_BENCH_ENGINE": engine,
            "P0_NATIVE_BENCH_ROWS": str(rows),
            "P0_NATIVE_BENCH_STEPS": str(steps),
            "P0_NATIVE_BENCH_STATE": storage,
        }
        command = [
            str(binary), "--exact", "paired_count::paired_count_runtime_sample",
            "--ignored", "--nocapture", "--test-threads=1",
        ]
        wall_ns, rss = run(command, log, cpu=cpu, env=env)
        timings = re.search(
            r"init_ns=(\d+) elapsed_ns=(\d+) checkpoint_ns=(\d+)", log.read_text()
        )
        if timings is None:
            raise ValueError(f"Missing runtime measurement in {log}")
        init_ns, elapsed_ns, checkpoint_ns = map(int, timings.groups())
        batches = sorted(path.stat().st_size for path in Path(storage).rglob("*.feldera"))
        if not batches or elapsed_ns <= 0 or checkpoint_ns <= 0:
            raise ValueError(f"Missing spill files or timings in {log}")
        return {
            "rows_per_step": rows,
            "steps": steps,
            "pair": pair,
            "engine": engine,
            "init_ns": init_ns,
            "step_latency_ns": elapsed_ns / steps,
            "rows_per_second": rows * steps * 1_000_000_000 / elapsed_ns,
            "checkpoint_ns": checkpoint_ns,
            "batch_file_sizes": batches,
            "wall_ns": wall_ns,
            "peak_rss_kib": rss,
            "spilled_batch_bytes": sum(batches),
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--pairs", type=int, default=31)
    args = parser.parse_args()
    if args.pairs < 1:
        parser.error("--pairs must be positive")
    args.output.mkdir()
    binary = args.binary.resolve(strict=True)
    cpu = min(os.sched_getaffinity(0))
    results = {}
    with (args.output / "samples.jsonl").open("x") as raw:
        for rows, steps in [(256, 40), (2048, 20)]:
            paired = []
            for pair in range(1, args.pairs + 1):
                order = ("classic", "native") if pair % 2 else ("native", "classic")
                for engine in order:
                    row = sample(binary, args.output, cpu, rows, steps, pair, engine)
                    paired.append(row)
                    print(json.dumps(row), file=raw, flush=True)
            metrics = (
                "init_ns", "step_latency_ns", "rows_per_second", "checkpoint_ns",
                "peak_rss_kib", "spilled_batch_bytes",
            )
            results[rows] = {
                engine: {
                    key: summary([row[key] for row in paired if row["engine"] == engine])
                    for key in metrics
                }
                for engine in ("classic", "native")
            }
    with binary.open("rb") as compiled:
        binary_sha256 = hashlib.file_digest(compiled, "sha256").hexdigest()
    result = {
        "schema_version": 1,
        "cpu": cpu,
        "runtime_binary_sha256": binary_sha256,
        "pairs_per_batch_size": args.pairs,
        "runtime_summary": results,
    }
    (args.output / "measurements.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
