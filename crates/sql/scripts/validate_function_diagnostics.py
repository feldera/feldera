#!/usr/bin/env python3
"""Compare SQL function diagnostics in direct Java and supervised mixed compilation."""
import argparse
import json
from pathlib import Path
import subprocess

from run_determinism import classpath_digest, file_digest


CASES = [
    ("connector_in_view", "CREATE VIEW v AS SELECT CONNECTOR_METADATA()['a'];",
     ["only allowed in DEFAULT column initializers"]),
    ("two_connector_calls", "CREATE VIEW v AS SELECT CAST(CONNECTOR_METADATA()['a'] AS INT)"
                            " + CAST(CONNECTOR_METADATA()['b'] AS INT);",
     ["only allowed in DEFAULT column initializers"]),
    ("transitive_udf", "CREATE FUNCTION f(x INT) RETURNS BIGINT AS YEAR(NOW()) + x;\n"
                       "CREATE FUNCTION g(x INT) RETURNS BIGINT AS f(x);",
     ["Non-deterministic UDF"]),
    ("nondeterministic_udf", "CREATE FUNCTION f(x INT) RETURNS BIGINT AS YEAR(NOW()) + x;",
     ["Non-deterministic UDF"]),
    ("nondeterministic_default", "CREATE TABLE t (x TIMESTAMP DEFAULT NOW());",
     ["Non-deterministic initializer"]),
]


def compare(compiler, classpath, directory, engine, sql, expected):
    directory.mkdir()
    source = directory / "program.sql"
    source.write_text(sql + "\n")
    reference = directory / "direct.json"
    flags = ["--gen2"] if engine == "gen2" else []
    direct = subprocess.run(["java", "-cp", classpath, "org.dbsp.sqlCompiler.CompilerMain",
                             "-i", "-je", "--alltables", "--ignoreOrder", *flags,
                             "--errors", reference, "-o", directory / "unused", source], timeout=60)
    messages = json.loads(reference.read_bytes())
    errors = [m for m in messages if not m["warning"]]
    if not direct.returncode or not all(any(token in (m["error_type"] + " " + m["message"])
                                           for m in errors) for token in expected):
        raise ValueError(f"Unexpected Java diagnostics for {source}: {messages}")
    request = directory / "request.json"
    request.write_text(json.dumps(dict(java_classpath=classpath, engine=engine,
        producer_commit="p3-check", runtime_abi="p3-check", sql=str(source),
        slots=str(directory / "slots"), deadline_ms=60000, rust_optimizer_passes=True,
        allow_stateless_java_fallback=True)))
    attempt = directory / "attempt"
    hybrid = subprocess.run([compiler, "--hybrid", request, attempt], timeout=65)
    result = json.loads((attempt / "result/result.json").read_bytes())
    if (not hybrid.returncode or result["status"] != "sql_error" or result["messages"] != messages
            or result["attempts"] != 1 or result["compiler"] != "hybrid" or result["artifacts"]
            or result["rust_optimizer_passes"] is not True):
        raise ValueError(f"Mixed diagnostics for {source} differ from {messages}: {result}")
    return messages


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compiler", type=Path, required=True)
    parser.add_argument("--classpath", required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir()
    compiler = args.compiler.resolve(strict=True)
    checks = {engine: {name: compare(compiler, args.classpath, output / f"{engine}-{name}",
                                     engine, sql, expected) for name, sql, expected in CASES}
              for engine in ["classic", "gen2"]}
    receipt = dict(diagnostics=checks, compiler_sha256=file_digest(compiler),
                   optimizer_library_sha256=file_digest(compiler.with_name("libfeldera_sql_opt_jni.so")),
                   classpath_sha256=classpath_digest(args.classpath))
    (output / "complete.json").write_text(json.dumps(receipt, indent=2) + "\n")
