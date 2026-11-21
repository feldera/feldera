#!/usr/bin/env python3
"""Compare standalone tail-worker results to the four verified Gen-2 artifacts."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


def read(path):
    return json.loads(path.read_bytes())


def validate(runs, output, compiler, comparator):
    output.mkdir()
    cases = []
    for case in read(runs / "complete.json")["cases"]:
        name = case["case"]
        expected = runs / name / "rust-lowered"
        checkpoint = expected / "lowered/s3.json"
        identity = read(checkpoint)
        command = [compiler, "--s3-worker", identity["producer_commit"], identity["runtime_abi"]]
        attempt = output / name
        subprocess.run([*command, checkpoint, attempt], check=True)
        ready = attempt / "result"
        result = read(ready / "result.json")
        if (result["status"] != "ok" or result["producer_commit"] != identity["producer_commit"]
                or result["runtime_abi"] != identity["runtime_abi"]):
            raise ValueError(f"{name}: wrong compiler result")
        subprocess.run([comparator, expected / "circuit.json", ready / "circuit.json"], check=True)
        for artifact in ("schema.json", "dataflow.json", "diagnostics.json"):
            if read(ready / artifact) != read(expected / "lowered" / artifact):
                raise ValueError(f"{name}: worker changed {artifact}")
        if result["messages"] != read(ready / "diagnostics.json"):
            raise ValueError(f"{name}: worker messages differ")
        cases.append(name)
    if not cases:
        raise ValueError("Empty lowering corpus")

    invalid = output / "wrong-identity"
    if subprocess.run([compiler, "--s3-worker", "wrong-producer", identity["runtime_abi"],
                       checkpoint, invalid]).returncode != 1:
        raise ValueError("Expected compiler identity rejection")
    ready = invalid / "result"
    if ([f.name for f in ready.iterdir()] != ["result.json"]
            or read(ready / "result.json")["status"] != "internal_error"):
        raise ValueError("Rejected input published artifacts")

    # Exercise a SQL-error status independently of compiler syntax coverage.
    injected = output / "reported-error.json"
    message = {"warning": False, "error_type": "Test error", "message": "worker status test",
               "start_line_number": 0, "start_column": 0, "end_line_number": 0, "end_column": 0}
    identity["diagnostics"]["messages"] = [{
        "type": message["error_type"], "message": message["message"], "warning": False,
        "range": {name: message[name] for name in ("start_line_number", "start_column", "end_line_number", "end_column")},
    }]
    injected.write_text(json.dumps(identity))
    rejected = output / "sql-error"
    if subprocess.run([*command, injected, rejected]).returncode != 1:
        raise ValueError("Expected a SQL-error result")
    ready = rejected / "result"
    result = read(ready / "result.json")
    if ([f.name for f in ready.iterdir()] != ["result.json"] or result["status"] != "sql_error"
            or result["messages"] != [dict(message, snippet="")]):
        raise ValueError("Worker lost SQL diagnostics or published failed artifacts")

    # A pipe keeps the worker in the read stage while a second attempt races it.
    with tempfile.TemporaryDirectory() as local:
        fifo = Path(local) / "blocked-input"
        os.mkfifo(fifo)
        attempt = output / "interrupted"
        with subprocess.Popen([*command, fifo, attempt]) as process:
            try:
                deadline = time.monotonic() + 5
                while not attempt.exists():
                    if process.poll() is not None or time.monotonic() >= deadline:
                        raise ValueError("Worker did not acquire the attempt")
                    time.sleep(0.01)
                if subprocess.run([*command, checkpoint, attempt], capture_output=True).returncode != 2:
                    raise ValueError("Concurrent worker replaced an active attempt")
            finally:
                process.terminate()
            process.wait(timeout=5)
        if (attempt / "result").exists():
            raise ValueError("Interrupted attempt published a result")
    return {"equal": True, "cases": cases, "wrong_identity": True,
            "sql_error_preserved": True, "concurrent_attempt_excluded": True, "interrupted_attempt_unpublished": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compiler", type=Path, required=True)
    parser.add_argument("--comparator", type=Path, required=True)
    args = parser.parse_args()
    report = validate(args.runs, args.output, args.compiler, args.comparator)
    (args.output / "complete.json").write_text(json.dumps(report, indent=2) + "\n")
