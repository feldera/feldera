#!/usr/bin/env python3
"""Compare native inline BIGINT operations with the actual sqllib functions."""

import argparse
import itertools
import json
import random
import re
import threading
from pathlib import Path

from run_determinism import OutputBudget, file_digest, run_process

ROOT = Path(__file__).resolve().parents[3]


def vectors():
    values = [
        None,
        -(2**63),
        -(2**63) + 1,
        -(2**32),
        -3_037_000_500,
        -2,
        -1,
        0,
        1,
        2,
        3_037_000_499,
        3_037_000_500,
        2**63 - 2,
        2**63 - 1,
    ]
    randomizer = random.Random(0x5EED)
    pairs = list(itertools.product(values, repeat=2))
    # Both full-width and small integers: successful products must be measured
    # alongside overflow, which dominates uniformly distributed 64-bit inputs.
    for bound in (2**63, 100_000):
        for _ in range(1024):
            pairs.append(
                tuple(
                    None
                    if randomizer.randrange(8) == 0
                    else randomizer.randrange(-bound, bound)
                    for _ in range(2)
                )
            )
    for operation in ("add", "subtract", "multiply"):
        for left, right in pairs:
            yield (operation, left, right)


def observations(path, count):
    lines = path.read_text().splitlines()
    if len(lines) != count:
        raise ValueError(f"{path}: expected {count} records, got {len(lines)}")
    values = []
    for index, line in enumerate(lines):
        fields = line.split("\t")
        if len(fields) != 2 or fields[0] != str(index):
            raise ValueError(f"{path}: missing, duplicated or reordered record {index}")
        value = fields[1]
        if value.startswith("OVERFLOW:"):
            match = re.fullmatch(
                r"OVERFLOW:'(0|-?[1-9][0-9]*) ([+*\-]) (0|-?[1-9][0-9]*)' causes overflow for type BIGINT",
                value,
            )
            if match is None or any(
                not -(2**63) <= int(match[group]) < 2**63 for group in (1, 3)
            ):
                raise ValueError(f"{path}: invalid overflow diagnostic {value!r}")
        elif value != "NULL" and (
            not re.fullmatch(r"0|-?[1-9][0-9]*", value)
            or not -(2**63) <= int(value) < 2**63
        ):
            raise ValueError(f"{path}: invalid BIGINT observation {value!r}")
        values.append(value)
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--runtime-reference", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir()
    binaries = {
        "native": args.probe.resolve(strict=True),
        "runtime": args.runtime_reference.resolve(strict=True),
    }
    sources = [
        *sorted((ROOT / "crates/sql/native-probe/src").rglob("*.rs")),
        *sorted((ROOT / "crates/sql/native-probe/examples").rglob("*.rs")),
        *sorted((ROOT / "crates/sql/native-probe/tests").rglob("*.rs")),
        ROOT / "crates/sql/native-probe/Cargo.toml",
        ROOT / "crates/sql/native-probe/Cargo.lock",
        ROOT / "crates/sqllib/examples/native_integer_reference.rs",
        ROOT / "crates/sqllib/src/operators.rs",
        ROOT / "Cargo.lock",
        Path(__file__).resolve(),
    ]
    provenance = {
        "sources": {str(p.relative_to(ROOT)): file_digest(p) for p in sources},
        "binaries": {
            name: {"path": str(p), "sha256": file_digest(p)}
            for name, p in binaries.items()
        },
    }
    (output / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    cases = list(vectors())
    input_path = output / "input.tsv"
    input_path.write_text(
        "".join(
            "\t".join("NULL" if value is None else str(value) for value in case) + "\n"
            for case in cases
        )
    )
    budget = OutputBudget(output, 16 * 1024**2, [output], 1024**3)
    outcomes = {}
    for name, binary in binaries.items():
        path = output / f"{name}.tsv"
        with path.open("x") as log, (output / f"{name}.stderr").open("x") as errors:
            run_process(
                [str(binary), str(input_path)],
                output,
                log,
                60,
                threading.Event(),
                budget,
                stderr=errors,
            )
        outcomes[name] = observations(path, len(cases))
        if file_digest(binary) != provenance["binaries"][name]["sha256"]:
            raise ValueError(f"{name} binary changed during measurement")
    if any(
        file_digest(p) != provenance["sources"][str(p.relative_to(ROOT))]
        for p in sources
    ):
        raise ValueError("source changed during measurement")
    differences = [
        {"index": index, "input": case, "native": native, "runtime": runtime}
        for index, (case, native, runtime) in enumerate(
            zip(cases, outcomes["native"], outcomes["runtime"])
        )
        if native != runtime
    ]
    result = {
        "schema_version": 2,
        "cases": len(cases),
        "differences": differences,
        "equal": not differences,
        "runtime_nulls": outcomes["runtime"].count("NULL"),
        "runtime_overflows": sum(
            value.startswith("OVERFLOW:") for value in outcomes["runtime"]
        ),
        "provenance": provenance,
        "artifacts": {
            p.name: file_digest(p) for p in sorted(output.iterdir()) if p.is_file()
        },
        "raw_directory": str(output),
        "error_messages_compared": True,
        "full_c12": False,
        "n1_ten_program_c7": False,
    }
    (output / "complete.json").write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: result[key]
                for key in ("cases", "equal", "runtime_nulls", "runtime_overflows")
            }
        )
    )
    if differences:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
