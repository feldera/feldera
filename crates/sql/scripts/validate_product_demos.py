#!/usr/bin/env python3
"""Compare the ordinary Java and hybrid compiler entry points on the shipped SQL demos."""
import argparse
import json
from pathlib import Path
import tempfile

from benchmark_live_optimizer import JsonNumber, artifact_files, artifact_json, trial
from run_determinism import classpath_digest, file_digest


def expected_rejection(engine, name, scratch, failures):
    if (engine, name) != ("gen2", "01-medallion-architecture.sql"):
        return False
    if set(failures) != {"direct", "hybrid"} or failures["direct"].get("exit_code") != 1:
        return False
    direct = scratch / "direct/diagnostics.json"
    published = scratch / "hybrid/result"
    if not direct.is_file() or artifact_files(published) != {"result.json"}:
        return False
    result = artifact_json((published / "result.json").read_bytes())
    messages = artifact_json(direct.read_bytes() or b"[]")
    errors = [m for m in messages if m["warning"] is False]
    return (result["status"] == "sql_error" and result["compiler"] == "hybrid"
            and result["attempts"] == JsonNumber("1") and result["rust_optimizer_passes"] is True
            and result["engine"] == engine and result["artifacts"] == {}
            and result["messages"] == messages and len(errors) == 1
            and errors[0]["error_type"] == "Not yet implemented"
            and "LATENESS on the primary-key table 'bronze_clickstream_events'" in errors[0]["message"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compiler", required=True, type=Path)
    parser.add_argument("--comparator", required=True, type=Path)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--producer", required=True)
    parser.add_argument("--engine", choices=["classic", "gen2"], required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[3]
    args.workspace_runtime = repo if args.engine == "classic" else None
    inputs = sorted((repo / "crates/pipeline-manager/demos/sql").glob("*.sql"))
    if not inputs:
        parser.error("no shipped SQL demos found")
    cases = {}
    report = dict(engine=args.engine, producer=args.producer, compiler=file_digest(args.compiler),
                  optimizer=file_digest(args.compiler.with_name("libfeldera_sql_opt_jni.so")),
                  comparator=file_digest(args.comparator), classpath=classpath_digest(args.classpath),
                  cases=cases)
    for sql in inputs:
        case = cases[sql.name] = {"sql_sha256": file_digest(sql)}
        with tempfile.TemporaryDirectory() as directory:
            scratch = Path(directory)
            result = trial(args, sql, ["direct", "hybrid"], scratch)
            if "artifacts" in result:
                case["artifacts"] = result["artifacts"]
            elif expected_rejection(args.engine, sql.name, scratch, result["failures"]):
                case["diagnostics"] = result["failures"]["hybrid"]["messages"]
            else:
                case["failures"] = result["failures"]
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    failures = [name for name, case in cases.items() if "failures" in case]
    if failures:
        raise SystemExit(f"{args.engine} compiler differences in {', '.join(failures)}. See {args.output}")
    print(f"{len(cases)} {args.engine} demo comparisons passed")


if __name__ == "__main__":
    main()
