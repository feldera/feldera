#!/usr/bin/env python3
"""Measure cold-process Java compilation versus the S2 file boundary on Linux.

Prepare a gitignored .bench-results archive before running; retain this script,
the input SQL and the resulting directory in that archive. This is not a runtime
throughput benchmark or a production G7/G10 budget approval.
"""

import argparse
import json
import math
import os
import platform
import random
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path

from compare_parsers import digest
from run_determinism import classpath_digest


def artifact_hashes(directory):
    return {
        str(path.relative_to(directory)): digest(path)
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def measure(command, directory):
    timing = directory / "resource.json"
    wrapped = [sys.executable, __file__, "--measure", str(timing), *command]
    (directory / "command.json").write_text(json.dumps(wrapped) + "\n")
    with (
        (directory / "stdout.txt").open("x") as stdout,
        (directory / "stderr.txt").open("x") as stderr,
    ):
        result = subprocess.run(
            wrapped, stdout=stdout, stderr=stderr, timeout=190, check=False
        )
    if result.returncode:
        raise ValueError(f"Compilation failed ({result.returncode}); see {directory}")
    return json.loads(timing.read_text())


def measure_child(timing, command):
    # A fresh helper has no previous children, so ru_maxrss belongs to this
    # compilation. Linux reports KiB. Exclude the helper's own startup time.
    start = time.perf_counter()
    result = subprocess.run(command, capture_output=True, timeout=180, check=False)
    elapsed = time.perf_counter() - start
    sys.stdout.buffer.write(result.stdout)
    sys.stderr.buffer.write(result.stderr)
    record = {
        "seconds": elapsed,
        "max_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
        "returncode": result.returncode,
    }
    Path(timing).write_text(json.dumps(record) + "\n")
    return result.returncode


def bootstrap_ratio(direct, hybrid):
    """Paired bootstrap of summed costs, never an average of per-case ratios."""
    randomizer = random.Random(0)
    ratios = []
    for _ in range(10000):
        indices = randomizer.choices(range(len(direct)), k=len(direct))
        ratios.append(sum(hybrid[i] for i in indices) / sum(direct[i] for i in indices))
    ratios.sort()
    return [ratios[249], ratios[9749]]


def summarize(rows):
    result = {}
    for case in sorted({row["case"] for row in rows}):
        selected = [row for row in rows if row["case"] == case and not row["warmup"]]
        direct = [row["direct"]["seconds"] for row in selected]
        hybrid = [row["head"]["seconds"] + row["tail"]["seconds"] for row in selected]
        costs = {}
        for name, values in [("direct", direct), ("hybrid", hybrid)]:
            costs[name] = {
                "mean_seconds": statistics.mean(values),
                "median_seconds": statistics.median(values),
                "empirical_p95_seconds": sorted(values)[
                    math.ceil(0.95 * len(values)) - 1
                ],
                "min_seconds": min(values),
                "max_seconds": max(values),
            }
        result[case] = {
            "samples": len(selected),
            **costs,
            "ratio_of_mean_times": sum(hybrid) / sum(direct),
            "paired_bootstrap_95_percent_interval": bootstrap_ratio(direct, hybrid),
            "direct_max_rss_kib": max(row["direct"]["max_rss_kib"] for row in selected),
            "hybrid_max_child_rss_kib": max(
                max(row["head"]["max_rss_kib"], row["tail"]["max_rss_kib"])
                for row in selected
            ),
            "seam_bytes": sorted({row["seam_bytes"] for row in selected}),
        }
    return result


def run(args):
    args.output.mkdir()
    inputs = sorted(args.inputs.glob("*.sql"))
    if not inputs or args.samples < 5 or args.warmups < 1:
        raise ValueError("Need SQL inputs, at least five samples, and a warmup")
    cpus = sorted(os.sched_getaffinity(0))[:4]
    java = [
        "taskset",
        "-c",
        ",".join(map(str, cpus)),
        "java",
        "-Xms128m",
        "-Xmx1g",
        "-cp",
        args.classpath,
        "org.dbsp.sqlCompiler.CompilerMain",
    ]
    common = ["--crates", "bench", "--runtime", str(args.runtime.resolve()), "--je"]
    identity = [
        "--dump-producer-commit",
        "p0-seam-benchmark",
        "--dump-runtime-abi",
        "working-tree",
    ]
    environment = {
        "platform": platform.platform(),
        "host": platform.node(),
        "affinity": cpus,
        "java": subprocess.check_output(
            ["java", "-version"], stderr=subprocess.STDOUT, text=True
        ),
        "cpuinfo": Path("/proc/cpuinfo").read_text(),
        "meminfo": Path("/proc/meminfo").read_text(),
        "loadavg_before": Path("/proc/loadavg").read_text(),
        "inputs_sha256": {str(p): digest(p) for p in inputs},
        "runner_sha256": digest(Path(__file__)),
        "classpath_sha256": classpath_digest(args.classpath),
        "samples": args.samples,
        "warmups": args.warmups,
        "limitations": [
            "VM host load and CPU frequency are not controlled.",
            "Three selected SQL workloads do not represent the full corpus.",
            "Every compilation uses a fresh JVM with warm filesystem caches.",
            "Hybrid RSS is the maximum of sequential children, not simultaneous service RSS.",
            "The CLI uses default language options, not each source JUnit case's option matrix.",
        ],
    }
    (args.output / "environment.json").write_text(
        json.dumps(environment, indent=2) + "\n"
    )
    rows = []
    with (args.output / "results.jsonl").open("x") as journal:
        for iteration in range(-args.warmups, args.samples):
            for index, sql in enumerate(inputs):
                trial = args.output / f"{sql.stem}-{iteration + args.warmups:03}"
                trial.mkdir()
                for part in ("direct", "head", "tail"):
                    (trial / part).mkdir()
                for part in ("direct", "tail"):
                    (trial / part / "artifacts").mkdir()
                seam = trial / "seam.json"
                commands = {}
                for part in ("direct", "tail"):
                    folder = trial / part
                    commands[part] = [
                        *java,
                        *common,
                        *identity,
                        "--errors",
                        str(folder / "errors.json"),
                        "--js",
                        str(folder / "schema.json"),
                        "-o",
                        str(folder / "artifacts"),
                        *(
                            ["--read-s2", str(seam)]
                            if part == "tail"
                            else [str(sql.resolve())]
                        ),
                    ]
                commands["head"] = [
                    *java,
                    *common,
                    *identity,
                    "--input_circuit",
                    "--noRust",
                    "--write-s2",
                    str(seam),
                    "--errors",
                    str(trial / "head/errors.json"),
                    str(sql.resolve()),
                ]
                order = (
                    ["direct", "head", "tail"]
                    if (iteration + index) % 2 == 0
                    else ["head", "tail", "direct"]
                )
                row = {
                    "case": sql.name,
                    "iteration": iteration,
                    "warmup": iteration < 0,
                    "order": order,
                }
                for part in order:
                    row[part] = measure(commands[part], trial / part)
                for name in ("schema.json", "errors.json"):
                    if (trial / "direct" / name).read_bytes() != (
                        trial / "tail" / name
                    ).read_bytes():
                        raise ValueError(f"Hybrid {name} differs: {trial}")
                direct_artifacts = artifact_hashes(trial / "direct/artifacts")
                if not direct_artifacts or direct_artifacts != artifact_hashes(
                    trial / "tail/artifacts"
                ):
                    raise ValueError(f"Hybrid generated artifacts differ: {trial}")
                row["artifact_hashes"] = direct_artifacts
                row["seam_bytes"] = seam.stat().st_size
                rows.append(row)
                journal.write(json.dumps(row) + "\n")
                journal.flush()
    if environment["inputs_sha256"] != {str(p): digest(p) for p in inputs}:
        raise ValueError("Inputs changed during measurement")
    if environment["classpath_sha256"] != classpath_digest(args.classpath):
        raise ValueError("Java classpath changed during measurement")
    result = {
        "schema_version": 1,
        "workloads": summarize(rows),
        "complete": True,
        "production_budget_approved": False,
        "runtime_executed": False,
    }
    (args.output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--runtime", required=True, type=Path)
    parser.add_argument("--samples", type=int, default=11)
    parser.add_argument("--warmups", type=int, default=2)
    args = parser.parse_args()
    args.output = args.output.resolve()
    run(args)


if __name__ == "__main__":
    if len(sys.argv) > 3 and sys.argv[1] == "--measure":
        sys.exit(measure_child(sys.argv[2], sys.argv[3:]))
    main()
