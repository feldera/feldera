#!/usr/bin/env python3
"""Measure paired cold SQL compilation through classic or Gen-2 artifact emission."""
import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time

from benchmark_java_baseline import latency
from benchmark_seams import bootstrap_ratio
from run_determinism import classpath_digest, file_digest
from type_binding_compare import no_duplicates


@dataclass(frozen=True)
class JsonNumber:
    """Keep the compiler's number token distinct from booleans and strings."""
    token: str


def artifact_json(data):
    def invalid_constant(token):
        raise ValueError(f"Invalid JSON constant {token}")

    return json.loads(data, parse_int=JsonNumber, parse_float=JsonNumber,
                      parse_constant=invalid_constant, object_pairs_hook=no_duplicates)


def artifact_files(root):
    return {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}


def compare(direct, hybrid, artifact="circuit.rs", comparator=None):
    result = artifact_json((hybrid / "result.json").read_bytes())
    if ((result["compiler"], result["status"], result["attempts"]) != ("hybrid", "ok", JsonNumber("1"))
            or result.get("rust_optimizer_passes") is not True):
        raise ValueError(f"Unexpected hybrid result {result}")
    diagnostics = direct / "diagnostics.json"
    messages = artifact_json(diagnostics.read_bytes() or b"[]") if diagnostics.exists() else []
    if messages != result["messages"]:
        raise ValueError("Compiler diagnostics differ")
    artifacts = artifact_files(direct) - {"diagnostics.json"}
    published = artifact_files(hybrid) - {"result.json"}
    required = {artifact, "schema.json", "dataflow.json"}
    if artifacts != set(result["artifacts"]) or artifacts != published or not required <= artifacts:
        raise ValueError("Compiler output file sets differ")
    if artifact == "rust/Cargo.toml" and not all(
            any(path.startswith("rust/") and path.endswith(suffix) for path in artifacts)
            for suffix in ("/src/lib.rs", "/src/stubs.rs")):
        raise ValueError("Compiler workspace is missing generated Rust sources")
    differences = []
    for name in sorted(artifacts):
        if file_digest(hybrid / name) != result["artifacts"][name]:
            raise ValueError(f"Incorrect output digest for {name}")
        if name in {"dataflow.json", "schema.json"}:
            equal = artifact_json((direct / name).read_bytes()) == artifact_json((hybrid / name).read_bytes())
        elif name == "circuit.json" and artifact == name:
            comparison = subprocess.run([comparator, direct / name, hybrid / name],
                                        capture_output=True, text=True, timeout=30)
            if comparison.returncode:
                differences.append(f"Gen-2 circuit comparison failed: {comparison.stderr[-4096:].strip()}")
            continue
        else:
            equal = file_digest(direct / name) == result["artifacts"][name]
        if not equal:
            differences.append(f"{name} differs")
    if differences:
        raise ValueError("\n".join(differences))
    return result["artifacts"]


def jvm_options(compiler):
    return ["--enable-native-access=ALL-UNNAMED", f"-Djava.library.path={compiler.parent}",
            "-Xss8m", "-XX:TieredStopAtLevel=1", "-Xmx512m", "-XX:MaxMetaspaceSize=128m",
            "-XX:MaxDirectMemorySize=64m", "-XX:ActiveProcessorCount=1",
            "-XX:-HeapDumpOnOutOfMemoryError", "-XX:ErrorFile=/dev/null"]


def direct_java(profile, compiler, classpath):
    if profile == "production":
        return ["java", "-jar", classpath]
    return ["java", *jvm_options(compiler), "-cp", classpath, "org.dbsp.sqlCompiler.CompilerMain"]


def trial(args, sql, order, scratch, *, profile="matched"):
    direct, hybrid = scratch / "direct", scratch / "hybrid"
    direct.mkdir()
    manifest = scratch / "request.json"
    request = dict(java_classpath=args.classpath, engine=args.engine,
        producer_commit=args.producer, runtime_abi="live-benchmark", rust_optimizer_passes=True,
        sql=str(sql), slots=str(scratch / "slots"), deadline_ms=120000)
    artifact = "circuit.json" if args.engine == "gen2" else "circuit.rs"
    flags = ["--gen2"] if args.engine == "gen2" else []
    output = direct / artifact
    if args.workspace_runtime:
        output = direct / "rust"
        (output / "crates").mkdir(parents=True)
        artifact = "rust/Cargo.toml"
        request["workspace"] = {"crate_name": "baseline", "runtime_sources": str(args.workspace_runtime)}
        flags = ["--crates", "baseline", "--runtime", args.workspace_runtime]
    manifest.write_text(json.dumps(request))
    commands = {
        "hybrid": [args.compiler, "--hybrid", manifest, hybrid],
        "direct": [*direct_java(profile, args.compiler, args.classpath), "-i", "-je", "--alltables", "--ignoreOrder",
                   "--dump-producer-commit", args.producer, "--dump-runtime-abi", "live-benchmark",
                   "--errors", direct / "diagnostics.json", "-js", direct / "schema.json",
                   "--dataflow", direct / "dataflow.json", *flags, "-o", output, sql],
    }
    times = {}
    failures = {}
    for mode in order:
        start = time.monotonic_ns()
        try:
            result = subprocess.run(commands[mode], capture_output=True, timeout=125)
            if result.returncode:
                receipt = hybrid / "result/result.json"
                failures[mode] = (json.loads(receipt.read_bytes()) if mode == "hybrid" and receipt.exists()
                                  else {"exit_code": result.returncode, "stderr": result.stderr[-4096:].decode(errors="replace")})
        except subprocess.TimeoutExpired:
            failures[mode] = {"timeout_seconds": 125}
        times[mode] = (time.monotonic_ns() - start) / 1e9
    if not failures:
        try:
            return dict(seconds=times, artifacts=compare(direct, hybrid / "result", artifact, args.comparator))
        except (OSError, KeyError, ValueError, subprocess.TimeoutExpired) as error:
            failures["comparison"] = str(error)
    return dict(seconds=times, failures=failures)


def summarize(profiles):
    results = {}
    for profile, times in profiles.items():
        if "failures" in times:
            results[profile] = {"failures": times["failures"], "completed_pairs": len(times["direct"])}
            continue
        result = {mode: latency(values) for mode, values in times.items()}
        result["p95_ratio"] = (result["hybrid"]["empirical_p95_seconds"]
                               / result["direct"]["empirical_p95_seconds"])
        result["mean_ratio_paired_95pct_interval"] = bootstrap_ratio(times["direct"], times["hybrid"])
        results[profile] = result
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--compiler", type=Path, required=True)
    parser.add_argument("--producer", required=True)
    parser.add_argument("--engine", choices=("classic", "gen2"), default="classic")
    parser.add_argument("--workspace-runtime", type=Path,
                        help="emit a multi-crate classic workspace using this Feldera checkout")
    parser.add_argument("--comparator", type=Path, help="Gen-2 circuit comparator (default: sql-difftest beside the worker)")
    parser.add_argument("--direct-profile", action="append", choices=("matched", "production"),
                        help="direct JVM profile (default: matched). Repeat to measure both.")
    parser.add_argument("--samples", type=int, default=21)
    parser.add_argument("--warmups", type=int, default=2)
    args = parser.parse_args()
    if args.samples < 20 or args.warmups < 1:
        parser.error("Specify at least 20 measured pairs and one warmup for p95.")
    profiles = list(dict.fromkeys(args.direct_profile or ["matched"]))
    if "production" in profiles and not Path(args.classpath).is_file():
        parser.error("The production JVM uses java -jar. Supply the packaged compiler jar with --classpath.")
    args.compiler = args.compiler.resolve(strict=True)
    if args.workspace_runtime:
        if args.engine != "classic" or not (args.workspace_runtime / "crates/dbsp/Cargo.toml").is_file():
            parser.error("--workspace-runtime requires classic output and a checkout containing crates/dbsp/Cargo.toml")
        args.workspace_runtime = args.workspace_runtime.resolve()
    if args.engine == "gen2":
        args.comparator = (args.comparator or args.compiler.with_name("sql-difftest")).resolve(strict=True)
    elif args.comparator:
        parser.error("--comparator applies only to Gen-2")
    inputs = [p.resolve(strict=True) for p in args.inputs]
    if len({p.name for p in inputs}) != len(inputs):
        parser.error("workload names must be unique")
    suffix = "dylib" if platform.system() == "Darwin" else "so"
    library = args.compiler.with_name(f"libfeldera_sql_opt_jni.{suffix}")
    def build_inputs():
        return dict(inputs={p.name: file_digest(p) for p in inputs}, compiler=file_digest(args.compiler),
                    library=file_digest(library), classpath=classpath_digest(args.classpath),
                    comparator=file_digest(args.comparator) if args.comparator else None)

    original = build_inputs()
    identity = dict(original, engine=args.engine, workspace_runtime=str(args.workspace_runtime) if args.workspace_runtime else None,
                    affinity=sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
                    platform=platform.platform(), hybrid_jvm=["java", *jvm_options(args.compiler), "-cp", args.classpath],
                    direct_jvms={p: direct_java(p, args.compiler, args.classpath) for p in profiles},
                    java_environment={key: os.environ.get(key) for key in
                                      ("JDK_JAVA_OPTIONS", "JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS")},
                    java=subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT, text=True))
    args.output.mkdir()
    (args.output / "environment.json").write_text(json.dumps(identity, indent=2) + "\n")
    measured = {p.name: {profile: {"hybrid": [], "direct": []} for profile in profiles} for p in inputs}
    failed = False
    with (args.output / "pairs.jsonl").open("x") as raw:
        for iteration in range(-args.warmups, args.samples):
            offset = iteration % len(inputs)
            for sql in inputs[offset:] + inputs[:offset]:
                parity = (iteration + inputs.index(sql)) % 2
                order = ["direct", "hybrid"] if parity else ["hybrid", "direct"]
                for profile in (profiles if parity else reversed(profiles)):
                    times = measured[sql.name][profile]
                    if "failures" in times:
                        continue
                    with tempfile.TemporaryDirectory() as scratch:
                        record = trial(args, sql, order, Path(scratch), profile=profile)
                    print(json.dumps(dict(workload=sql.name, direct_profile=profile, iteration=iteration,
                                          order=order, **record)), file=raw, flush=True)
                    if "failures" in record:
                        times["failures"] = record["failures"]
                        failed = True
                    elif iteration >= 0:
                        for mode, value in record["seconds"].items():
                            times[mode].append(value)
    if build_inputs() != original:
        raise ValueError("Compiler, comparator, classpath, or input changed. Repeat the benchmark.")
    summary = {name: summarize(profiles) for name, profiles in measured.items()}
    (args.output / "results.json").write_text(json.dumps(summary, indent=2) + "\n")
    if failed:
        raise SystemExit("One or more compiler comparisons failed. See results.json and pairs.jsonl.")


if __name__ == "__main__":
    main()
