#!/usr/bin/env python3
"""Exercise the supervised P2 compilation protocol with a real Java compiler."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time
import tomllib


def check_sql_error(compiler, classpath, directory, name, program, expected, *, engine="classic"):
    sql = directory / f"{name}-error.sql"
    sql.write_text(program)
    reference = directory / f"{name}-java.json"
    command = ["java", "-cp", classpath, "org.dbsp.sqlCompiler.CompilerMain", "-i", "-je",
               "--alltables", "--ignoreOrder", "--errors", reference, "-o", directory / "unused.rs"]
    if engine == "gen2":
        command.append("--gen2")
    command.append(sql)
    direct = subprocess.run(command, timeout=60)
    messages = json.loads(reference.read_bytes())
    if direct.returncode == 0 or not any(not m["warning"] and expected in m["message"] for m in messages):
        raise ValueError(f"Expected the Java {name} diagnostic: {messages}")
    request = directory / f"{name}-request.json"
    request.write_text(json.dumps(dict(java_classpath=classpath, engine=engine, sql=str(sql),
        producer_commit="p3-check", runtime_abi="p3-check", slots=str(directory / "slots"),
        deadline_ms=60000, rust_optimizer_passes=True, allow_stateless_java_fallback=True)))
    attempt = directory / f"{name}-attempt"
    probe = subprocess.run([compiler, "--hybrid", request, attempt], timeout=65)
    result = json.loads((attempt / "result/result.json").read_bytes())
    if (probe.returncode == 0 or result["status"] != "sql_error" or result["messages"] != messages
            or result["attempts"] != 1 or result["compiler"] != "hybrid" or result["artifacts"]):
        raise ValueError(f"Hybrid {name} diagnostic or terminal outcome differs from direct Java")
    return messages


def check_replay_limits(classpath, directory, source):
    diagnostics = directory / "checkpoint-errors.json"
    java = ["java", "-cp", classpath, "org.dbsp.sqlCompiler.CompilerMain", "-je", "--noRust",
            "--dump-producer-commit", "test", "--dump-runtime-abi", "test", "--errors", diagnostics]
    for stage in ("s2", "s3"):
        checkpoint = directory / f"{stage}.json"
        write = [f"--write-{stage}", checkpoint, source]
        if stage == "s2":
            write.insert(0, "--input_circuit")
        subprocess.run([*java, *write], check=True, timeout=60)
        size = checkpoint.stat().st_size
        for operation, args in [("replay", [f"--read-{stage}", checkpoint]), ("export", write)]:
            if operation == "export":
                checkpoint.unlink()
            limited = subprocess.run([java[0], "-Dfeldera.test.checkpointMaxBytes=16", *java[1:], *args],
                                     timeout=60)
            errors = [m for m in json.loads(diagnostics.read_bytes() or b"[]") if not m["warning"]]
            assert limited.returncode and len(errors) == 1, (stage, operation, errors)
            assert errors[0]["error_type"] == "Optimizer resource limit", errors
            assert f"{stage.upper()} {operation} is {size} bytes" in errors[0]["message"], errors
            assert "limit is 16 bytes" in errors[0]["message"], errors
            assert "without --rust-optimizer-passes" in errors[0]["message"], errors


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

    def compare_compilation(name, engine, sql):
        record = finish(start(name, engine=engine, sql=str(sql)), name, "ok")
        assert record["compiler"] == "hybrid" and record["attempts"] == 1
        direct = directory / f"{name}-java"
        direct.mkdir()
        artifact = direct / ("circuit.json" if engine == "gen2" else "circuit.rs")
        diagnostics = directory / f"{name}-messages.json"
        args = ["java", "-cp", classpath, "org.dbsp.sqlCompiler.CompilerMain", "-i", "-je",
                "--alltables", "--ignoreOrder", "--errors", diagnostics, "-js", direct / "schema.json",
                "--dataflow", direct / "dataflow.json", "-o", artifact]
        if engine == "gen2":
            args.append("--gen2")
        subprocess.run([*args, sql], check=True, timeout=60)
        warnings = json.loads(diagnostics.read_bytes() or b"[]")
        assert record["messages"] == warnings, name
        compare_files(name, record, direct, {file.name for file in direct.iterdir() if file.is_file()})
        return warnings, artifact

    for engine in ("classic", "gen2"):
        compare_compilation(engine, engine, source)

    runtime_sources = Path(__file__).resolve().parents[3]
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
    manifest = directory / "workspace/result/rust/Cargo.toml"
    with manifest.open("rb") as stream:
        dependencies = tomllib.load(stream)["workspace"]["dependencies"]
    assert "path" in dependencies["dbsp"]
    for name, dependency in dependencies.items():
        if isinstance(dependency, dict) and "path" in dependency:
            crate = manifest.parent / dependency["path"] / "Cargo.toml"
            if not crate.is_file():
                raise ValueError(f"Generated workspace dependency {name} points to missing {crate}")

    bad = directory / "invalid.sql"
    bad.write_text("SELECT FROM;")
    sql = finish(start("invalid", sql=str(bad), allow_stateless_java_fallback=True), "invalid", "sql_error")
    assert sql["attempts"] == 1 and any(m["warning"] is False for m in sql["messages"])

    if rust_optimizer_passes:
        defaults = directory / "temporal-defaults.sql"
        defaults.write_text("""CREATE TABLE t (
            ts TIMESTAMP DEFAULT CAST('2020-02-30 00:00:00' AS TIMESTAMP),
            d DATE DEFAULT CAST('-001-01-01' AS DATE),
            zero_year DATE DEFAULT CAST('0000-01-01' AS DATE),
            negative_year DATE DEFAULT CAST('-0001-01-01' AS DATE),
            expanded_year DATE DEFAULT CAST('+10000-01-01' AS DATE),
            chosen TIMESTAMP DEFAULT CASE WHEN 1 = 1
                THEN CAST('2021-04-31 00:00:00' AS TIMESTAMP)
                ELSE TIMESTAMP '1999-01-01 00:00:00' END
        );
        CREATE VIEW v AS SELECT * FROM t;
        """)
        for engine in ("classic", "gen2"):
            warnings, artifact = compare_compilation(f"temporal-defaults-{engine}", engine, defaults)
            assert [(m["warning"], m["error_type"], m["message"]) for m in warnings] == [
                (True, "Suspicious argument", f"String '{value}' cannot be interpreted as a DATE")
                for value in ("-001-01-01", "0000-01-01", "-0001-01-01", "+10000-01-01")]
            if engine == "classic":
                assert "Timestamp::from_microseconds(1583020800000000" in artifact.read_text()
        check_sql_error(compiler, classpath, directory, "emit-final",
                        "CREATE TABLE t(ts BIGINT LATENESS 1, other BIGINT);\n"
                        "CREATE VIEW v WITH ('emit_final' = 'other') AS SELECT * FROM t;",
                        "Could not infer a waterline for column 'v'.'other'")
        check_sql_error(compiler, classpath, directory, "key-lateness",
                        "CREATE TABLE t(ts BIGINT NOT NULL PRIMARY KEY LATENESS 2, x INT);\n"
                        "CREATE VIEW v AS SELECT * FROM t;",
                        "LATENESS on the primary-key table 't'", engine="gen2")
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

    check_replay_limits(classpath, directory, source)
    options = os.environ.get("JAVA_TOOL_OPTIONS", "")
    env = dict(os.environ, JAVA_TOOL_OPTIONS=(options + " -Dfeldera.test.checkpointMaxBytes=16").strip())

    def check_recovery(name, env, direct, files, stage, **changes):
        recovered = finish(start(name, env=env, allow_stateless_java_fallback=True, **changes), name, "ok")
        assert recovered["compiler"] == "java" and recovered["attempts"] == 2, recovered
        reason = recovered["fallback_reason"]
        assert reason["status"] == "resource_limit", reason
        assert stage in reason["reason"] and "limit is 16 bytes" in reason["reason"], reason
        assert "without --rust-optimizer-passes" in reason["reason"], reason
        assert set(recovered["artifacts"]) == files, recovered
        for filename in files:
            actual = directory / name / "result" / filename
            assert actual.read_bytes() == (direct / filename).read_bytes(), (name, filename)

    for name, engine, ir_only, engine_files in [
        ("recovered", "classic", False, ["circuit.rs", "stubs.rs"]),
        ("recovered-ir", "classic", True, []),
        ("recovered-gen2", "gen2", True, ["circuit.json"]),
    ]:
        check_recovery(name, env, directory / f"{engine}-java", {"schema.json", "dataflow.json", *engine_files},
                       "S3 export", engine=engine, ir_only=ir_only)
    check_recovery("recovered-workspace", env, direct_workspace, workspace_files, "S3 export", workspace=workspace)
    if rust_optimizer_passes:
        for payload, limit in [("input", 16), ("output", 64)]:
            name = f"native-{payload}-limit"
            env = dict(os.environ, FELDERA_TEST_OPTIMIZER_MAX_BYTES=str(limit))
            native = finish(start(name, env=env), name, "resource_limit")
            assert native["compiler"] == "hybrid" and native["attempts"] == 1, native
            size = re.search(rf"OptimizerOrder {payload} is (\d+) bytes", native["reason"])
            assert size and int(size.group(1)) > limit and f"limit is {limit} bytes" in native["reason"], native
        env = dict(os.environ, FELDERA_TEST_OPTIMIZER_MAX_BYTES="16")
        check_recovery("recovered-native", env, directory / "classic-java",
                       {"schema.json", "dataflow.json", "circuit.rs", "stubs.rs"}, "OptimizerOrder input")
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
