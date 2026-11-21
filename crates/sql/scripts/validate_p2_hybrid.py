#!/usr/bin/env python3
"""Exercise the supervised P2 compilation protocol with a real Java compiler."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time


def check_sql_error(compiler, classpath, directory, name, program, expected):
    sql = directory / f"{name}-error.sql"
    sql.write_text(program)
    reference = directory / f"{name}-java.json"
    command = ["java", "-cp", classpath, "org.dbsp.sqlCompiler.CompilerMain", "-i", "-je",
               "--alltables", "--ignoreOrder", "--errors", reference, "-o", directory / "unused.rs", sql]
    direct = subprocess.run(command, timeout=60)
    messages = json.loads(reference.read_bytes())
    if direct.returncode == 0 or not any(not m["warning"] and expected in m["message"] for m in messages):
        raise ValueError(f"Expected the Java {name} diagnostic: {messages}")
    request = directory / f"{name}-request.json"
    request.write_text(json.dumps(dict(java_classpath=classpath, engine="classic", sql=str(sql),
        producer_commit="p3-check", runtime_abi="p3-check", slots=str(directory / "slots"),
        deadline_ms=60000, rust_optimizer_passes=True, allow_stateless_java_fallback=True)))
    attempt = directory / f"{name}-attempt"
    probe = subprocess.run([compiler, "--hybrid", request, attempt], timeout=65)
    result = json.loads((attempt / "result/result.json").read_bytes())
    if (probe.returncode == 0 or result["status"] != "sql_error" or result["messages"] != messages
            or result["attempts"] != 1 or result["compiler"] != "hybrid" or result["artifacts"]):
        raise ValueError(f"Hybrid {name} diagnostic or terminal outcome differs from direct Java")
    return messages


def validate(compiler, comparator, classpath, directory, lateness=False, rust_optimizer_passes=False):
    directory.mkdir()
    slots = directory / "slots"
    source = directory / "program.sql"
    column = "x INT NOT NULL LATENESS 2" if lateness else "x INT"
    source.write_text(f"CREATE TABLE t ({column}); CREATE VIEW v AS SELECT x, COUNT(*) AS c FROM t GROUP BY x;")
    base = dict(java_classpath=classpath, engine="classic", producer_commit="p2-check",
                runtime_abi="p2-check", rust_optimizer_passes=rust_optimizer_passes, sql=str(source), slots=str(slots), deadline_ms=60000)

    def start(name, env=None, **changes):
        request = directory / f"{name}.json"
        request.write_text(json.dumps(dict(base, **changes)))
        return subprocess.Popen([compiler, "--hybrid", request, directory / name], env=env)

    def finish(process, name, status):
        code = process.wait(timeout=90)
        result = directory / name / "result"
        record = json.loads((result / "result.json").read_bytes())
        assert (code == 0) == (status == "ok") and record["status"] == status, record
        assert record["rust_optimizer_passes"] == (rust_optimizer_passes and record["compiler"] == "hybrid"), record
        actual = {str(f.relative_to(result)): hashlib.sha256(f.read_bytes()).hexdigest()
                  for f in result.rglob("*") if f.is_file() and f.name != "result.json"}
        assert actual == record["artifacts"], record
        if status != "ok":
            assert not actual, record
        return record

    def compare_files(name, record, direct, expected_files):
        assert set(record["artifacts"]) == expected_files, name
        for filename in sorted(expected_files):
            expected = direct / filename
            actual = directory / name / "result" / filename
            if filename == "circuit.json":
                subprocess.run([comparator, expected, actual], check=True, timeout=30)
            elif filename in {"schema.json", "dataflow.json"}:
                assert json.loads(actual.read_bytes()) == json.loads(expected.read_bytes()), filename
            else:
                assert actual.read_bytes() == expected.read_bytes(), filename

    for engine in ["classic", "gen2"]:
        proc = start(engine, engine=engine)
        record = finish(proc, engine, "ok")
        assert record["compiler"] == "hybrid" and record["attempts"] == 1
        direct = directory / f"{engine}-java"
        direct.mkdir()
        artifact = "circuit.json" if engine == "gen2" else "circuit.rs"
        args = ["java", "-cp", classpath, "org.dbsp.sqlCompiler.CompilerMain", "-i", "-je",
                "--alltables", "--ignoreOrder", "-js", direct / "schema.json", "--dataflow",
                direct / "dataflow.json", "-o", direct / artifact]
        if engine == "gen2":
            args.append("--gen2")
        subprocess.run([*args, source], check=True, timeout=60)
        compare_files(engine, record, direct, {file.name for file in direct.iterdir() if file.is_file()})

    runtime_sources = Path(__file__).resolve().parents[2]
    workspace = {"crate_name": "p2_example", "runtime_sources": str(runtime_sources)}
    direct_workspace = directory / "workspace-java"
    (direct_workspace / "rust" / "crates").mkdir(parents=True)
    subprocess.run(["java", "-cp", classpath, "org.dbsp.sqlCompiler.CompilerMain", "-i", "-je",
                    "--alltables", "--ignoreOrder", "--crates", workspace["crate_name"], "--runtime",
                    runtime_sources, "-js", direct_workspace / "schema.json", "--dataflow",
                    direct_workspace / "dataflow.json", "-o", direct_workspace / "rust", source],
                   check=True, timeout=60)

    workspace_files = {str(f.relative_to(direct_workspace)) for f in direct_workspace.rglob("*") if f.is_file()}
    assert "rust/Cargo.toml" in workspace_files
    normal_workspace = finish(start("workspace", workspace=workspace), "workspace", "ok")
    assert normal_workspace["compiler"] == "hybrid" and normal_workspace["attempts"] == 1
    compare_files("workspace", normal_workspace, direct_workspace, workspace_files)

    bad = directory / "invalid.sql"
    bad.write_text("SELECT FROM;")
    sql = finish(start("invalid", sql=str(bad), allow_stateless_java_fallback=True), "invalid", "sql_error")
    assert sql["attempts"] == 1 and any(m["warning"] is False for m in sql["messages"])

    if rust_optimizer_passes:
        check_sql_error(compiler, classpath, directory, "recursive",
                        "CREATE TABLE E(x INT, y INT);\n"
                        "DECLARE RECURSIVE VIEW CLOSURE(x INT, y INT);\n"
                        "CREATE MATERIALIZED VIEW CLOSURE AS (SELECT * FROM E) UNION\n"
                        "(SELECT LAG(x) OVER (PARTITION BY x ORDER BY y), x FROM CLOSURE);\n",
                        "Unsupported operation 'LAG' in recursive code")
        for name, query in [
            ("clock-map", "SELECT TRANSFORM(x, e -> e + EXTRACT(EPOCH FROM NOW())) FROM t"),
            ("clock-filter", "SELECT * FROM t WHERE ARRAY_EXISTS(x, e -> e > EXTRACT(EPOCH FROM NOW()))"),
        ]:
            check_sql_error(compiler, classpath, directory, name,
                            f"CREATE TABLE t(x BIGINT ARRAY);\nCREATE VIEW v AS {query};",
                            "NOW() is not supported inside a lambda expression")

    # Hold Java at an ordinary input read so the process cannot finish before cancellation.
    with tempfile.TemporaryDirectory() as local:
        pipe = Path(local) / "pending.sql"
        os.mkfifo(pipe)
        limited = finish(start("deadline", sql=str(pipe), deadline_ms=250), "deadline", "resource_limit")
        assert limited["attempts"] == 1
        child = start("cancelled", sql=str(pipe), allow_stateless_java_fallback=True)
        end = time.monotonic() + 10
        while not list((directory / "cancelled").glob("*/front.log")):
            if child.poll() is not None or time.monotonic() >= end:
                raise RuntimeError("Compiler failed to start its Java stage")
            time.sleep(.01)
        child.send_signal(signal.SIGTERM)
        cancelled = finish(child, "cancelled", "cancelled")
        assert cancelled["attempts"] == 1
    missing = str(directory / "absent-compiler.jar")
    primary = finish(start("stateful", java_classpath=missing), "stateful", "internal_error")
    assert primary["compiler"] == "hybrid" and primary["attempts"] == 1
    retried = finish(start("retry", java_classpath=missing, allow_stateless_java_fallback=True),
                     "retry", "internal_error")
    assert retried["compiler"] == "java" and retried["attempts"] == 2

    # Fail the checkpoint export alone. The fallback must publish a full direct-Java result.
    with tempfile.TemporaryDirectory() as local:
        real_java = shutil.which("java")
        if real_java is None:
            raise RuntimeError("Java is required for the stateless fallback test")
        launcher = Path(local) / "java"
        launcher.write_text(
            '#!/bin/sh\nfor arg do\n  if [ "$arg" = "--write-s3" ]; then exit 1; fi\ndone\n'
            'exec "$P2_TEST_JAVA" "$@"\n'
        )
        launcher.chmod(0o755)
        env = dict(os.environ, PATH=local + os.pathsep + os.environ["PATH"], P2_TEST_JAVA=real_java)
        ordinary_launcher = launcher.read_text()
        bug = json.dumps([{"warning": False, "error_type": "Compiler error", "message": "invalid circuit"}])
        launcher.write_text(ordinary_launcher.replace("then exit 1", f"then echo '{bug}' >&2; exit 1"))
        internal = finish(start("compiler-bug", env=env), "compiler-bug", "internal_error")
        assert internal["attempts"] == 1 and "invalid circuit" in internal["reason"]
        launcher.write_text(ordinary_launcher)
        for name, engine, ir_only, engine_files in [
            ("recovered", "classic", False, ["circuit.rs", "stubs.rs"]),
            ("recovered-ir", "classic", True, []),
            ("recovered-gen2", "gen2", True, ["circuit.json"]),
        ]:
            recovered = finish(start(name, env=env, engine=engine, ir_only=ir_only,
                                     allow_stateless_java_fallback=True), name, "ok")
            assert recovered["compiler"] == "java" and recovered["attempts"] == 2
            compare_files(name, recovered, directory / f"{engine}-java",
                          {"schema.json", "dataflow.json", *engine_files})
        retried_workspace = finish(start("recovered-workspace", env=env, workspace=workspace,
                                         allow_stateless_java_fallback=True), "recovered-workspace", "ok")
        assert retried_workspace["compiler"] == "java" and retried_workspace["attempts"] == 2
        compare_files("recovered-workspace", retried_workspace, direct_workspace, workspace_files)
    return {"classic_workspace_bytes": True, "gen2_canonical_ir": True, "schema_and_dataflow": True,
            "sql_error_terminal": True, "deadline_terminal": True, "cancellation_terminal": True,
            "validated_artifact_digests": True, "stateful_fallback_refused": True,
            "stateless_retry_bounded": True, "stateless_retry_matches_direct_java": True,
            "stateless_retry_ir_only_engines": ["classic", "gen2"],
            "classic_multi_crate_workspace": True, "stateless_retry_multi_crate_workspace": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compiler", required=True, type=Path)
    parser.add_argument("--comparator", required=True, type=Path)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--rust-optimizer-passes", action="store_true")
    parser.add_argument("--lateness", action="store_true", help="retain a watermark for the checkpoint test")
    args = parser.parse_args()
    checks = validate(args.compiler.resolve(), args.comparator.resolve(), args.classpath, args.output.resolve(), args.lateness, args.rust_optimizer_passes)
    (args.output / "complete.json").write_text(json.dumps(checks, indent=2) + "\n")
