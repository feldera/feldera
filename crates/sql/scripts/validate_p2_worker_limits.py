#!/usr/bin/env python3
"""Measure cancellation of four occupied hybrid worker slots on Linux."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import threading
import time


def interrupted_workers(compiler, classpath, directory, jobs=4, rust_optimizer_passes=False):
    def cleanup(proc):
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()

    def rss(pid):
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
        raise ValueError(f"Missing RSS for worker process {pid}")

    def cancel(proc):
        barrier.wait()
        started = time.monotonic_ns()
        proc.send_signal(signal.SIGTERM)
        proc.wait()
        return (time.monotonic_ns() - started) / 1_000_000

    with ExitStack() as stack:
        workers = []
        for slot in range(jobs):
            base = directory / str(slot)
            base.mkdir()
            pipe = base / "input.sql"
            os.mkfifo(pipe)
            request = {"java_classpath": classpath, "engine": "classic", "sql": str(pipe),
                       "producer_commit": "limits", "runtime_abi": "limits",
                       "slots": str(directory / "slots"), "deadline_ms": 60000}
            if rust_optimizer_passes:
                request["rust_optimizer_passes"] = True
            manifest = base / "request.json"
            manifest.write_text(json.dumps(request))
            attempt = base / "attempt"
            child = subprocess.Popen([compiler, "--hybrid", manifest, attempt], start_new_session=True)
            stack.callback(cleanup, child)
            workers.append((pipe, attempt, child))

        rss_bytes = []
        for pipe, _, proc in workers:
            # A nonblocking writer can open only after the JVM begins its input read.
            # Leave the writer open without sending SQL until all four slots are occupied.
            deadline = time.monotonic() + 30
            while True:
                if proc.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("Hybrid compiler did not reach its SQL input")
                try:
                    fd = os.open(pipe, os.O_NONBLOCK | os.O_WRONLY)
                    stack.callback(os.close, fd)
                    break
                except OSError as error:
                    if error.errno != errno.ENXIO:
                        raise
                    time.sleep(.01)
            children = Path(f"/proc/{proc.pid}/task/{proc.pid}/children").read_text().split()
            if len(children) != 1:
                raise RuntimeError(f"Expected one front-end child of {proc.pid}")
            rss_bytes.append(rss(proc.pid) + rss(int(children[0])))

        barrier = threading.Barrier(jobs)
        with ThreadPoolExecutor(max_workers=jobs) as pool:
            times = list(pool.map(cancel, [p for _, _, p in workers]))

        for _, attempt, proc in workers:
            result = json.loads((attempt / "result/result.json").read_bytes())
            if proc.returncode == 0 or result["status"] != "cancelled" or result["artifacts"]:
                raise RuntimeError(f"Incomplete cancellation at {attempt}")
    return times, rss_bytes


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compiler", type=Path, required=True)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--rust-optimizer-passes", action="store_true")
    args = parser.parse_args()
    compiler = args.compiler.resolve(strict=True)
    timings, rss_samples = [], []
    for _ in range(5):
        with tempfile.TemporaryDirectory() as directory:
            samples, rss_values = interrupted_workers(compiler, args.classpath, Path(directory),
                                                    rust_optimizer_passes=args.rust_optimizer_passes)
            timings.extend(samples)
            rss_samples.extend(rss_values)
    p95 = sorted(timings)[math.ceil(len(timings) * .95) - 1]
    print(json.dumps({"profile": "Linux devcontainer, four concurrent Java front ends waiting on SQL input",
                      "cpu_affinity": sorted(os.sched_getaffinity(0)),
                      "rust_optimizer_passes": args.rust_optimizer_passes, "rounds": 5, "slots_per_round": 4, "signal_to_reap_ms": timings,
                      "p95_ms": p95, "largest_driver_plus_front_end_rss_bytes": max(rss_samples),
                      "compiler_sha256": hashlib.sha256(compiler.read_bytes()).hexdigest()}, indent=2))
    if p95 > 25 or max(rss_samples) > 1024 ** 3:
        raise SystemExit("Measured hybrid worker resource limit exceeded")
