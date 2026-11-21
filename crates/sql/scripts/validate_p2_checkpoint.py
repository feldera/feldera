#!/usr/bin/env python3
"""Restore the grouped COUNT from validate_p2_hybrid across separate Java and hybrid binaries."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

from run_p2_runtime import create_test_project, file_hash


def build(family, artifacts, directory):
    project = directory / family
    project.mkdir()
    package = f"p2_{family}_{file_hash(artifacts / 'circuit.rs')[:24]}"
    create_test_project(project, package)
    manifest = project / "Cargo.toml"
    # The harness imports the generated library as temp. Cargo gives each binary its own package.
    manifest.write_text(manifest.read_text().replace("[lib]\n", '[lib]\nname = "temp"\n'))
    source = project / "src"
    shutil.copyfile(artifacts / "circuit.rs", source / "lib.rs")
    shutil.copyfile(artifacts / "stubs.rs", source / "stubs.rs")
    (source / "udf.rs").touch()
    shutil.copyfile(Path(__file__).with_name("p2_checkpoint_runner.rs"), source / "main.rs")
    result = subprocess.run(["cargo", "build", "--locked", "--offline", "--features", "catalog",
                             "--manifest-path", project / "Cargo.toml", "--message-format=json-render-diagnostics"],
                            stdout=subprocess.PIPE, check=True)
    binaries = [record["executable"] for line in result.stdout.splitlines() if
                (record := json.loads(line)).get("executable") and record["target"]["kind"] == ["bin"]]
    if len(binaries) != 1 or Path(binaries[0]).name != package:
        raise ValueError(f"Expected one executable for {family}, found {binaries}")
    binary = directory / f"{family}-checkpoint"
    shutil.copy2(binaries[0], binary)
    return binary, package


def verify(acceptance, lateness=False):
    with tempfile.TemporaryDirectory() as scratch:
        directory = Path(scratch)
        origins = {"java": acceptance / "classic-java", "hybrid": acceptance / "classic/result"}
        binaries = {family: build(family, path, directory) for family, path in origins.items()}
        pairs = []
        for writer, reader in [("java", "hybrid"), ("hybrid", "java")]:
            state = directory / f"{writer}-to-{reader}"
            (state / "dbsp").mkdir(parents=True)
            observations = []
            for family, operation in [(writer, "seed"), (reader, "restore-watermark" if lateness else "restore")]:
                binary, package = binaries[family]
                observed = json.loads(subprocess.check_output([binary, operation, state], timeout=120))
                if observed["package"] != package or observed["operation"] != operation or observed["bootstrap"]:
                    raise ValueError(f"Unexpected checkpoint executable or operation: {observed}")
                observations.append(observed)
            seed, restored = observations
            if seed["checkpoint_id"] != restored["checkpoint_id"] or restored["count_deltas"] != [[1, 1], [2, -1]]:
                raise ValueError(f"Restore differs from saved checkpoint: {observations}")
            pairs.append({"writer": writer, "reader": reader, "bootstrap": restored["bootstrap"],
                          "checkpoint_id": restored["checkpoint_id"], "restored_count_deltas": restored["count_deltas"]})
            if lateness:
                pairs[-1]["late_input_dropped_after_restore"] = True
        return {"program_sha256": hashlib.sha256((acceptance / "program.sql").read_bytes()).hexdigest(),
                "binary_sha256": {name: file_hash(binary) for name, (binary, _) in binaries.items()},
                "generated_rust_sha256": {name: hashlib.sha256((p / "circuit.rs").read_bytes()).hexdigest()
                                          for name, p in origins.items()}, "restores": pairs}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("acceptance", type=Path)
    parser.add_argument("report", type=Path)
    parser.add_argument("--lateness", action="store_true", help="verify a late input after restoring a watermark")
    args = parser.parse_args()
    report = verify(args.acceptance, args.lateness)
    with args.report.open("x") as stream:
        print(json.dumps(report, indent=2), file=stream)
