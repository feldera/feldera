#!/usr/bin/env python3
"""Build and run isolated generated catalog circuits for frozen Python negatives."""

import argparse
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command, prefix, environment=None, timeout=120):
    """Retain outputs; terminate the process group if the command times out."""
    prefix.with_suffix(".command.json").write_text(json.dumps(command, indent=2) + "\n")
    with prefix.with_suffix(".stdout").open("w") as stdout, prefix.with_suffix(".stderr").open("w") as stderr:
        process = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=environment, start_new_session=True)
        try:
            return process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # wait(timeout) has not reaped an exited leader on its timeout path.
            # Keeping it waitable reserves its process-group ID through this kill.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            prefix.with_suffix(".timeout").write_text("timed out\n")
            return 124


def assess(case, returncode, stdout):
    observation = None
    if returncode == 0:
        try:
            observation = json.loads(stdout)
        except json.JSONDecodeError:
            pass
    valid = (isinstance(observation, dict) and observation.get("id") == case["id"]
             and isinstance(observation.get("observation"), dict))
    detail = observation["observation"] if valid else {}
    error = detail.get("error", "")
    expected = case["expected"].strip().lower()
    found = bool(expected) and isinstance(error, str) and expected in error.strip().lower()
    matched = (valid and returncode == 0 and detail.get("stage") == "table_transaction"
               and found and observation.get("cleanup_failed") is False)
    return {"id": case["id"], "exit_code": returncode, "expected_text_found": found,
            "local_runtime_contract_matched": matched, "observation": observation,
            "python_suite_executed": False, "platform_transport_executed": False}


def manifest(repository):
    template = (repository / "sql-to-dbsp-compiler/temp/Cargo.toml").read_text()
    if template.count('[dev-dependencies]') != 1 or template.count('name = "temp"') != 1:
        raise ValueError("generated test manifest template changed")
    template = template.split("[dev-dependencies]")[0]
    template = template.replace('name = "temp"', 'name = "p0-python-runtime-probe"')
    template = template.replace('"../../crates/', '"' + str(repository / "crates") + '/')
    template = template.replace('features = ["arbitrary_precision"]', 'features = ["arbitrary_precision", "raw_value"]')
    return template + '\n[workspace]\n\n[lib]\npath = "src/lib.rs"\n\n[[bin]]\nname = "p0-python-runtime-probe"\npath = "src/main.rs"\n'


def module_source(raw, index):
    """One library has one allocator; leave every generated circuit unchanged."""
    for marker in ["#[global_allocator]\n", '#[export_name = "malloc_conf"]\n']:
        if raw.count(marker) != 1:
            raise ValueError("generated allocator declaration changed")
        if index != 0:
            raw = raw.replace(marker, "", 1)
    return raw


def prepare(args, records, fixtures):
    output = args.output
    output.mkdir()
    for name in ["src", "cases", "logs"]:
        (output / name).mkdir()
    shutil.copyfile(args.inputs, output / "inputs.json")
    shutil.copyfile(args.repo / "Cargo.lock", output / "Cargo.lock")
    runner = args.repo / "crates/sql/scripts/python_runtime_runner.rs"
    shutil.copyfile(runner, output / "src/main.rs")
    (output / "src/udf.rs").write_text("")
    (output / "Cargo.toml").write_text(manifest(args.repo))
    constructors = []
    frozen_sources = None
    if args.generated_from:
        previous = json.loads((args.generated_from / "metadata.json").read_text())
        if previous["input_sha256"] != sha(args.inputs) or previous["selected_cases"] != [case["id"] for case in records]:
            raise ValueError("reused generated population differs from selected inputs")
        if previous["dependencies_sha256"] != sha(args.dependencies):
            raise ValueError("reused compiler dependencies differ")
        if previous["compiler_classes"] != {str(path.relative_to(args.classes)): sha(path) for path in sorted(args.classes.rglob("*")) if path.is_file()}:
            raise ValueError("reused compiler classes differ")
        frozen_sources = json.loads((args.generated_from / "generated-sources.json").read_text())
    for index, case in enumerate(records):
        if not re.fullmatch(r"[A-Za-z0-9_./:#-]+", case["id"]):
            raise ValueError("unsupported case identifier characters")
        module = f"case_{index:03d}"
        directory = output / "src" / module
        directory.mkdir()
        sql = output / "cases" / f"{index:03d}.sql"
        sql.write_text(case["sql"])
        frozen = dict(case, fixtures=[fixtures[key] for key in case["table_sequence"]])
        (output / "cases" / f"{index:03d}.json").write_text(json.dumps(frozen, indent=2, ensure_ascii=False) + "\n")
        command = ["java", "-Xmx1g", "-cp", str(args.classes) + ":" + str(args.dependencies),
                   "org.dbsp.sqlCompiler.CompilerMain", "-i", "-O", "2", "--alltables", "--ignoreOrder",
                   "--js", str(output / "cases" / f"{index:03d}.schema.json"),
                   "-o", str(directory / "mod.rs"), str(sql)]
        if args.generated_from:
            old = args.generated_from
            if (old / "cases" / f"{index:03d}.sql").read_text() != case["sql"] or json.loads((old / "cases" / f"{index:03d}.json").read_text()) != frozen:
                raise ValueError("reused SQL or fixture differs")
            for relative in [f"src/{module}/mod.raw.rs", f"src/{module}/stubs.rs", f"cases/{index:03d}.schema.json"]:
                if frozen_sources.get(relative) != sha(old / relative):
                    raise ValueError(f"reused generated source changed: {relative}")
                shutil.copyfile(old / relative, output / relative)
            raw = (directory / "mod.raw.rs").read_text()
        else:
            if run(command, output / "logs" / f"compile-{index:03d}") != 0:
                raise RuntimeError(f"compiler failed for {case['id']}; logs retained")
            raw = (directory / "mod.rs").read_text()
        (directory / "mod.raw.rs").write_text(raw)
        (directory / "mod.rs").write_text(module_source(raw, index))
        (directory / "udf.rs").write_text("")
        constructors.append((module, case["id"]))
        print(json.dumps({"generated": index + 1, "total": len(records), "id": case["id"]}), flush=True)
    library = "pub mod udf;\n" + "\n".join(f"mod {module};" for module, _ in constructors)
    library += '\npub fn circuit(id: &str, config: dbsp::circuit::CircuitConfig) -> Result<(dbsp::DBSPHandle, static_connectors::Catalog), dbsp::Error> {\n    match id {\n'
    library += "".join(f"        {json.dumps(identity)} => {module}::circuit(config),\n" for module, identity in constructors)
    library += '        _ => Err(dbsp::Error::Constructor(anyhow::anyhow!("unknown frozen case: {id}"))),\n    }\n}\n'
    (output / "src/lib.rs").write_text(library)
    generated = {str(path.relative_to(output)): sha(path) for path in sorted((output / "src").rglob("*.rs"))}
    generated.update({str(path.relative_to(output)): sha(path) for path in sorted((output / "cases").glob("*.schema.json"))})
    (output / "generated-sources.json").write_text(json.dumps(generated, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--classes", type=Path, required=True)
    parser.add_argument("--dependencies", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--case", action="append", default=[])
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--generated-from", type=Path, help="reuse hash-pinned raw generated modules from the same frozen inputs/compiler")
    args = parser.parse_args()
    if not 1 <= args.workers <= 64:
        raise ValueError("worker count must be between 1 and 64")
    report = json.loads(args.inputs.read_text())
    if not report["complete"] or report["runtime_executed"]:
        raise ValueError("expected frozen, unexecuted runtime inputs")
    records = [case for case in report["records"] if not args.case or case["id"] in args.case]
    if not records or (args.case and {case["id"] for case in records} != set(args.case)):
        raise ValueError("unknown or empty requested case selection")
    fixtures = {fixture["id"]: fixture for fixture in report["fixtures"]}
    prepare(args, records, fixtures)
    metadata = {"schema_version": 1, "input_sha256": sha(args.inputs),
                "compiler_classes": {str(path.relative_to(args.classes)): sha(path) for path in sorted(args.classes.rglob("*")) if path.is_file()},
                "dependencies_sha256": sha(args.dependencies), "runner_sha256": sha(args.output / "src/main.rs"),
                "script_sha256": sha(Path(__file__)), "selected_cases": [case["id"] for case in records],
                "compiler_flags": ["-i", "-O", "2", "--alltables", "--ignoreOrder"],
                "runtime_workers": args.workers, "registered_table_sequence_preserved": True,
                "generated_source_adjustment": "remove duplicate global_allocator and malloc_conf export attributes after first module; raw source retained",
                "generated_from": str(args.generated_from) if args.generated_from else None,
                "python_suite_executed": False, "platform_transport_executed": False}
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    environment = dict(os.environ, CARGO_BUILD_JOBS="2", CARGO_TARGET_DIR=str(args.target),
                       CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_DEV_INCREMENTAL="false")
    command = ["cargo", "build", "--offline", "--features", "catalog", "--manifest-path",
               str(args.output / "Cargo.toml"), "--bin", "p0-python-runtime-probe"]
    if run(command, args.output / "logs/build", environment, timeout=1800) != 0:
        raise RuntimeError("offline build failed; logs retained")
    binary = args.target / "debug/p0-python-runtime-probe"
    metadata["binary_sha256"] = sha(binary)
    metadata["cargo_lock_sha256"] = sha(args.output / "Cargo.lock")
    metadata["build_environment"] = {key: environment.get(key) for key in ["CARGO_HOME", "CARGO_BUILD_JOBS", "CARGO_TARGET_DIR", "CARGO_PROFILE_DEV_DEBUG", "CARGO_PROFILE_DEV_INCREMENTAL"]}
    results = []
    with (args.output / "results.jsonl").open("w") as stream:
        for index, case in enumerate(records):
            prefix = args.output / "logs" / f"run-{index:03d}"
            code = run([str(binary), str(args.output / "cases" / f"{index:03d}.json"), str(args.workers)], prefix, timeout=60)
            result = assess(case, code, prefix.with_suffix(".stdout").read_text())
            stream.write(json.dumps(result) + "\n")
            stream.flush()
            results.append(result)
            print(json.dumps({"executed": index + 1, "matched": result["local_runtime_contract_matched"], "id": case["id"]}), flush=True)
    metadata["matched"] = sum(result["local_runtime_contract_matched"] for result in results)
    metadata["executed"] = len(results)
    metadata["complete"] = True
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    if metadata["matched"] != len(results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
