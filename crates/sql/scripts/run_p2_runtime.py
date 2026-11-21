#!/usr/bin/env python3
"""Execute the frozen classic runtime assertions through the P2 worker in batches."""

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


RUNNER = "org.dbsp.sqlCompiler.compiler.sql.tools.CorpusRuntimeReplay"
TEMPLATE = Path(__file__).resolve().parents[3] / "sql-to-dbsp-compiler/temp"


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def classpath_hash(path):
    if path.is_file():
        return file_hash(path)
    if not path.is_dir():
        raise ValueError(f"Missing Java classpath entry: {path}")
    digest = hashlib.sha256()
    for file in sorted(p for p in path.rglob("*") if p.is_file()):
        digest.update(file.relative_to(path).as_posix().encode() + b"\0")
        digest.update(bytes.fromhex(file_hash(file)))
    return digest.hexdigest()


def runtime_environment(classpath):
    """A resumed batch must use the same compiler, runtime test crate and toolchain."""
    def version(command):
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()
    return {"classpath_sha256": [classpath_hash(Path(entry)) for entry in classpath.split(os.pathsep)],
            "test_manifest_sha256": file_hash(TEMPLATE / "Cargo.toml"),
            "test_lock_sha256": file_hash(TEMPLATE / "Cargo.lock"),
            "rustc": version(["rustc", "-Vv"]), "cargo": version(["cargo", "-V"]),
            "java": version(["java", "-version"])}


def create_test_project(project, package="temp"):
    (project / "src").mkdir()
    # Anchor the SQL test crate's local dependencies when building outside the workspace.
    manifest = re.sub(r'(\{ path = ")(\.\./[^"]+)', lambda m: m[1] + str((TEMPLATE / m[2]).resolve()),
                      (TEMPLATE / "Cargo.toml").read_text())
    def rename_root(text):
        original = 'name = "temp"'
        if text.count(original) != 1:
            raise ValueError("Expected exactly one temp package in the test crate")
        return text.replace(original, f'name = "{package}"')
    (project / "Cargo.toml").write_text(rename_root(manifest) + "\n[workspace]\n")
    (project / "Cargo.lock").write_text(rename_root((TEMPLATE / "Cargo.lock").read_text()))


def inventory(population, corpus):
    groups = defaultdict(list)
    seen = set()
    compile_only = []
    with population.open() as records:
        for line in records:
            source = json.loads(line)
            key = source["id"]
            if key in seen:
                raise ValueError(f"Duplicate frozen case {key}")
            seen.add(key)
            method, index = key.rsplit("/", 1)
            directory = hashlib.sha256(method.encode()).hexdigest()
            path = corpus / directory / f"case-{int(index):04}.json"
            case = json.loads(path.read_bytes())
            if (case["status"] != "exported" or case["engine_mode"] != "classic"
                    or (case["source_test"], case["case_index"]) != (method, int(index))
                    or any(case[k] != source[k] for k in ("program", "submissions"))):
                raise ValueError(f"Runtime case differs from frozen SQL input {key}")
            if not case["commands"] and case["expected_runtime_error"] is None:
                compile_only.append(key)
                continue
            options = case["options"]
            catalog = not options["ioOptions"]["emitHandles"]
            groups[catalog, json.dumps(options, sort_keys=True)].append((key, path))
    if not groups:
        raise ValueError("Corpus contains no runtime cases")
    return groups, compile_only


def case_name(index, case_id):
    java_name = "#".join(case_id.rsplit("/", 1))
    return f"test{index}_{hashlib.sha256(java_name.encode()).hexdigest()}"


def run_batch(cases, catalog, args, project, log, lane):
    source = project / "src/lib.rs"
    source.unlink(missing_ok=True)
    jvm = ["java", "-ea", "-Xmx2g"]
    if args.rust_optimizer_passes:
        jvm += ["-Dfeldera.runtime.rustOptimizerPasses=true", "-Xss8m",
                f"-Djava.library.path={args.worker.parent}"]
    with log.open("xb") as output:
        subprocess.run([*jvm, "-Dfeldera.runtime.caseNames=true", f"-Dfeldera.runtime.s3Worker={args.worker}",
                        "-cp", args.classpath, RUNNER, source, *(p for _, p in cases)],
                       check=True, stdout=output, stderr=subprocess.STDOUT)
        feature = ["--features", "catalog"] if catalog else []
        command = ["cargo", "test", "--locked", "--offline", "--manifest-path",
                   project / "Cargo.toml", "--lib", *feature]
        # Match Utilities.compileAndTest, including for deeply nested generated Rust.
        env = {**os.environ, "RUST_MIN_STACK": "8388608"}
        if args.target_pool:
            env["CARGO_TARGET_DIR"] = str(args.target_pool / str(lane))
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
        output.write(result.stdout)
        result.check_returncode()
    passed = re.findall(rb"^test (test[0-9]+_[0-9a-f]{64})(?: - should panic)? \.\.\. ok$", result.stdout, re.M)
    expected = {case_name(i, key).encode() for i, (key, _) in enumerate(cases)}
    if len(passed) != len(cases) or set(passed) != expected:
        raise ValueError(f"Missing or extra Rust runtime tests in {log}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("population", "corpus", "output"):
        parser.add_argument(name, type=Path)
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--rust-optimizer-passes", action="store_true",
                        help="run the installed native optimizer passes before the S3 worker")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--target-pool", type=Path, help="use one numbered Cargo target directory per job")
    parser.add_argument("--resume", action="store_true", help="reuse verified batches from this run")
    args = parser.parse_args()
    if min(args.batch_size, args.jobs) < 1:
        parser.error("batch size and jobs must be positive")
    args.worker = args.worker.resolve(strict=True)
    if args.target_pool:
        args.target_pool = args.target_pool.resolve()
    groups, compile_only = inventory(args.population, args.corpus)
    batches = [(catalog, group[offset:offset + args.batch_size])
               for (catalog, _), group in groups.items()
               for offset in range(0, len(group), args.batch_size)]
    args.output.mkdir(parents=True, exist_ok=args.resume)
    manifest = {"population_sha256": hashlib.sha256(args.population.read_bytes()).hexdigest(),
                "worker_sha256": hashlib.sha256(args.worker.read_bytes()).hexdigest(),
                "batch_size": args.batch_size, "rust_min_stack": 8388608, "case_named_tests": True}
    if args.rust_optimizer_passes:
        extension = "dylib" if sys.platform == "darwin" else "so"
        manifest["optimizer_library_sha256"] = file_hash(args.worker.with_name(f"libfeldera_sql_opt_jni.{extension}"))
    identity = args.output / "run.json"
    if args.resume and json.loads(identity.read_bytes()) != manifest:
        raise ValueError("Frozen corpus, worker or test settings changed")
    environment_file = args.output / "environment.json"
    environment = runtime_environment(args.classpath)
    if args.resume:
        if not environment_file.exists() or json.loads(environment_file.read_bytes()) != environment:
            raise ValueError("Java classpath, test crate or toolchain differs from the original run")
        if json.loads((args.output / "compile-only.json").read_bytes()) != compile_only:
            raise ValueError("Compile-only case inventory differs from the original run")
    else:
        environment_file.write_text(json.dumps(environment, indent=2) + "\n")
        identity.write_text(json.dumps(manifest, indent=2) + "\n")
        (args.output / "compile-only.json").write_text(json.dumps(compile_only) + "\n")
    with tempfile.TemporaryDirectory() as scratch:
        def execute(index, lane):
            catalog, cases = batches[index]
            stem = f"batch-{index:04}"
            receipt = args.output / f"{stem}.json"
            ids = [key for key, _ in cases]
            digest = hashlib.sha256()
            for _, path in cases:
                digest.update(hashlib.sha256(path.read_bytes()).digest())
            if receipt.exists():
                saved = json.loads(receipt.read_bytes())
                log = args.output / saved["log"]
                if (ids != saved["ids"] or saved["cases_sha256"] != digest.hexdigest()
                        or saved["log_sha256"] != hashlib.sha256(log.read_bytes()).hexdigest()):
                    raise ValueError(f"Case inputs or previous results changed in {receipt}")
                return
            # Cargo identifies path packages with the same name and version by a common
            # artifact filename. Give each frozen batch a different package and executable.
            package = "p2_c7_" + digest.hexdigest()
            project = Path(scratch) / package
            project.mkdir()
            create_test_project(project, package)
            attempt = len(list(args.output.glob(f"{stem}.*.log")))
            log = args.output / f"{stem}.{attempt}.log"
            print(f"{log.name} {ids[0]} ({len(ids)} cases)", flush=True)
            run_batch(cases, catalog, args, project, log, lane)
            record = {"ids": ids, "cases_sha256": digest.hexdigest(), "log": log.name,
                      "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest()}
            temp = receipt.with_suffix(".tmp")
            temp.write_text(json.dumps(record) + "\n")
            temp.replace(receipt)

        def run_lane(lane):
            for index in range(lane, len(batches), args.jobs):
                execute(index, lane)

        with ThreadPoolExecutor(max_workers=args.jobs) as pool:
            for _ in pool.map(run_lane, range(args.jobs)):
                pass
    summary = {**manifest, "executed": sum(len(cases) for _, cases in batches),
               "compile_only": len(compile_only), "batches": len(batches)}
    (args.output / "complete.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
