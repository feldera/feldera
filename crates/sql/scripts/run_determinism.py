#!/usr/bin/env python3
"""Run three independent JVM replays and a baseline for every corpus case."""

import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from determinism import compare


def file_digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def classpath_digest(classpath):
    """Record the exact classes, resources, and dependency JAR used by each JVM."""
    result = {}
    for entry in classpath.split(os.pathsep):
        path = Path(entry)
        if path.is_file():
            result[str(path)] = file_digest(path)
        elif path.is_dir():
            digest = hashlib.sha256()
            for file in sorted(path.rglob("*")):
                if file.is_file():
                    digest.update(str(file.relative_to(path)).encode() + b"\0")
                    digest.update(bytes.fromhex(file_digest(file)))
            result[str(path)] = digest.hexdigest()
        else:
            raise ValueError(f"Classpath entry does not exist: {path}")
    return result


class OutputBudget:
    """Cancel writers when their shared artifact tree exceeds a byte budget.

    This is a sampled limit, not a filesystem quota. Leave headroom for writes
    between checks and for files outside this run's directory.
    """

    def __init__(self, directory, limit, space_paths=(), min_free=0):
        self.directory = directory
        self.limit = limit
        self.space_paths = space_paths
        self.min_free = min_free
        self.lock = threading.Lock()
        self.next_check = 0.0

    def check(self, force=False):
        with self.lock:
            now = time.monotonic()
            if not force and now < self.next_check:
                return
            for path in self.space_paths:
                if shutil.disk_usage(path).free < self.min_free:
                    raise RuntimeError(
                        f"Free space at {path} is below {self.min_free} bytes; "
                        "partial evidence is retained"
                    )
            total = 0
            for directory, _, files in os.walk(self.directory):
                for name in files:
                    try:
                        total += (Path(directory) / name).lstat().st_size
                    except FileNotFoundError:
                        # Each case removes its temporary generated files.
                        continue
                    if total > self.limit:
                        raise RuntimeError(
                            f"Replay output exceeds {self.limit} bytes; "
                            "partial evidence is retained"
                        )
            self.next_check = now + 0.5


def wait_for_descendants(group, timeout=2):
    """Wait for killed descendants to release their files before reusing a shard."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            os.killpg(group, 0)
        except ProcessLookupError:
            return
        # The container's init can leave zombies in the group after they have
        # released their descriptors. Only a live member can still hold a lock.
        processes = subprocess.check_output(["ps", "-A", "-o", "pgid=,stat="], text=True)
        if not any(pgid == str(group) and not state.startswith("Z")
                   for pgid, state in (row.split() for row in processes.splitlines() if row.strip())):
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Processes in determinism group {group} survived cancellation")
        time.sleep(0.01)


def run_process(
    command,
    directory,
    log,
    timeout,
    cancelled,
    budget=None,
    *,
    stderr=subprocess.STDOUT,
):
    """Bound a JVM and its canonicalizer together, including on sibling failure."""
    if cancelled.is_set():
        raise RuntimeError("Determinism run cancelled")
    if budget is not None:
        budget.check(force=True)
    with subprocess.Popen(
        command,
        cwd=directory,
        stdout=log,
        stderr=stderr,
        start_new_session=True,
    ) as process:
        deadline = time.monotonic() + timeout
        try:
            while process.poll() is None:
                if cancelled.wait(0.1):
                    raise RuntimeError("Determinism run cancelled")
                if time.monotonic() >= deadline:
                    raise subprocess.TimeoutExpired(command, timeout)
                if budget is not None:
                    budget.check()
            if process.returncode:
                raise subprocess.CalledProcessError(process.returncode, command)
            if budget is not None:
                budget.check(force=True)
        except BaseException:
            # The JVM may have exited while its Rust child is still alive. Signal
            # the process group even if poll() has already returned an exit code.
            for sig in (signal.SIGTERM, signal.SIGKILL):
                try:
                    os.killpg(process.pid, sig)
                except ProcessLookupError:
                    break
                if sig == signal.SIGTERM:
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        pass
            process.wait()
            wait_for_descendants(process.pid)
            raise


def run_shard(args, shard, cancelled, budget):
    directory = args.output / f"shard-{shard:03}"
    directory.mkdir()
    input_path = args.output / f"input-{shard:03}.jsonl"
    start = time.monotonic()
    for mode in ("baseline", "a", "b", "c"):
        command = [
            "java",
            "-ea",
            f"-Xmx{args.heap}",
            f"-Dfeldera.determinism.verifyIr={str(args.verify_ir).lower()}",
            f"-Dfeldera.determinism.producer={args.producer}",
            f"-Dfeldera.determinism.runtime={args.runtime}",
            f"-Dfeldera.determinism.compress={str(args.compress).lower()}",
            "-cp",
            args.classpath,
            "org.dbsp.sqlCompiler.compiler.sql.tools.CorpusDeterminism",
            str(input_path),
            str(directory / mode),
            str(args.comparator),
            "generated",
            "baseline" if mode == "baseline" else "capture",
        ]
        print(f"shard {shard}: starting {mode}", flush=True)
        with (directory / f"{mode}.log").open("w") as log:
            run_process(command, directory, log, args.timeout, cancelled, budget)
        manifest = json.loads((directory / mode / "complete.json").read_bytes())
        if args.verify_ir and manifest.get("verify_ir") is not True:
            raise ValueError(f"shard {shard}: {mode} compiler did not enable Rust IR verification")
        print(f"shard {shard}: completed {mode}", flush=True)
    comparisons = {
        name: compare(directory / "a", directory / name)
        for name in ("b", "c", "baseline")
    }
    for name, result in comparisons.items():
        (directory / f"compare-{name}.json").write_text(json.dumps(result, indent=2))
    return {
        "shard": shard,
        "seconds": time.monotonic() - start,
        "comparisons": comparisons,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--comparator", type=Path, required=True)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--producer", required=True)
    parser.add_argument("--runtime", required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--verify-ir", action="store_true", help="pass each captured graph through the production Rust arena and independent comparator")
    parser.add_argument("--heap", default="2g")
    parser.add_argument(
        "--max-output-mib",
        type=int,
        default=512,
        help="Sampled shared artifact budget; retain headroom beyond this limit",
    )
    parser.add_argument("--min-free-mib", type=int, default=512)
    parser.add_argument(
        "--free-space-path",
        type=Path,
        action="append",
        default=[],
        help="Also monitor free space here, e.g. a host mount backing a VM disk",
    )
    parser.add_argument(
        "--compress",
        action="store_true",
        help="Write gzip case records directly to limit disk use",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=7200,
        help="Maximum seconds per JVM; a timeout leaves no completion certificate",
    )
    args = parser.parse_args()
    if (
        args.workers < 1
        or args.timeout < 1
        or args.max_output_mib < 1
        or args.min_free_mib < 1
        or not args.producer.strip()
        or not args.runtime.strip()
    ):
        parser.error(
            "Workers, timeout, disk budgets, and build identities must be nonempty and positive"
        )
    args.input = args.input.resolve(strict=True)
    args.output = args.output.resolve()
    args.comparator = args.comparator.resolve(strict=True)
    args.classpath = os.pathsep.join(
        str(Path(p).resolve(strict=True)) for p in args.classpath.split(os.pathsep)
    )
    args.output.mkdir()
    space_paths = [args.output] + [
        path.resolve(strict=True) for path in args.free_space_path
    ]
    budget = OutputBudget(
        args.output,
        args.max_output_mib * 1024 * 1024,
        space_paths,
        args.min_free_mib * 1024 * 1024,
    )
    budget.check(force=True)
    provenance = {
        "input_sha256": file_digest(args.input),
        "comparator_sha256": file_digest(args.comparator),
        "classpath": classpath_digest(args.classpath),
        "workers": args.workers,
        "heap": args.heap,
        "compressed_records": args.compress,
        "verify_ir": args.verify_ir,
        "max_output_mib": args.max_output_mib,
        "min_free_mib": args.min_free_mib,
        "free_space_paths": [str(path) for path in space_paths],
        "producer": args.producer,
        "runtime": args.runtime,
        "java": subprocess.run(
            ["java", "-version"], capture_output=True, text=True, check=True
        ).stderr,
    }
    (args.output / "provenance.json").write_text(json.dumps(provenance, indent=2))
    # No deduplication: every registered case occurs once in each of the four runs.
    inputs = [[] for _ in range(args.workers)]
    with args.input.open() as stream:
        for index, line in enumerate(stream):
            inputs[index % args.workers].append(line)
    if any(not shard for shard in inputs):
        raise ValueError("Each worker needs at least one case")
    for index, lines in enumerate(inputs):
        (args.output / f"input-{index:03}.jsonl").write_text("".join(lines))
    cancelled = threading.Event()
    budget.check(force=True)
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(run_shard, args, shard, cancelled, budget)
            for shard in range(args.workers)
        ]
        try:
            results = [future.result() for future in as_completed(futures)]
        except BaseException:
            cancelled.set()
            raise
    results.sort(key=lambda result: result["shard"])
    if (
        classpath_digest(args.classpath) != provenance["classpath"]
        or file_digest(args.comparator) != provenance["comparator_sha256"]
    ):
        raise ValueError("Compiler or comparator changed during the run")
    cases = sum(result["comparisons"]["b"]["cases"] for result in results)
    if cases != sum(map(len, inputs)):
        raise ValueError("Replay count differs from the prepared corpus")
    equal = all(
        comparison["equal"]
        and not comparison["exported_schema_differences"]
        and not comparison["exported_diagnostic_differences"]
        for result in results
        for comparison in result["comparisons"].values()
    )
    complete = {
        "schema_version": 1,
        "equal": equal,
        "cases": cases,
        "jvm_runs": 4 * args.workers,
        "observations": sum(
            result["comparisons"]["b"]["observations"] for result in results
        ),
        "isolation": "three independent JVMs per ordered shard plus one capture-disabled baseline",
        "shards": results,
        "provenance": provenance,
    }
    budget.check(force=True)
    (args.output / "complete.json").write_text(json.dumps(complete, indent=2))
    print(
        json.dumps(
            {
                key: value
                for key, value in complete.items()
                if key not in ("shards", "provenance")
            },
            indent=2,
        )
    )
    return 0 if equal else 1


if __name__ == "__main__":
    raise SystemExit(main())
