#!/usr/bin/env python3
"""Adjudicate all ten reviewed S1 shards with exactly two input replacements.

The original failures and old-production comparisons remain evidence. Selection
of corrected shard results does not change the comparator or its tolerances.
"""

import argparse
import hashlib
import itertools
import json
from pathlib import Path

from determinism import compare, open_cases
from normalize_evidence import portable_paths


IDENTITY = "org.dbsp.sqlCompiler.compiler.sql.simple.RegressionTests#issue3326/0"
REPLACEMENTS = {("classic", 5), ("gen2", 1)}
SHARDS = [(mode, index) for mode, count in (("classic", 6), ("gen2", 4)) for index in range(count)]


def read(path):
    return json.loads(path.read_text())


def sha(path):
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def verify_replacement(original, replacement, output, provenance):
    replacement_lines = replacement.read_bytes().splitlines(keepends=True)
    if len(replacement_lines) != 1 or json.loads(replacement_lines[0])["id"] != IDENTITY:
        raise ValueError("replacement must contain exactly the reviewed case")
    count = changed = 0
    with original.open("rb") as left, output.open("rb") as right:
        for old, new in itertools.zip_longest(left, right):
            if old is None or new is None:
                raise ValueError("replacement changes the ordered case population")
            count += 1
            old_id, new_id = json.loads(old)["id"], json.loads(new)["id"]
            if old_id != new_id:
                raise ValueError("replacement changes case identity or order")
            if old != new:
                if old_id != IDENTITY or new != replacement_lines[0]:
                    raise ValueError("unreviewed program replacement")
                changed += 1
    expected = {"original_sha256": sha(original), "replacement_sha256": sha(replacement),
                "output_sha256": sha(output), "programs": count, "changed_programs": changed,
                "changed_identity": IDENTITY}
    if changed != 1 or any(provenance.get(k) != v for k, v in expected.items()):
        raise ValueError("replacement provenance disagrees with actual input bytes")
    return expected


def program_inventory(programs, inputs):
    count = statements = 0
    identities = set()
    with programs.open() as left, inputs.open() as right:
        for program, source in itertools.zip_longest(left, right):
            if program is None or source is None:
                raise ValueError("transport and SQL input populations differ")
            program, source = json.loads(program), json.loads(source)
            if program["id"] != source["id"] or program["id"] in identities:
                raise ValueError("transport identity, order, or uniqueness differs")
            identities.add(program["id"])
            count += 1
            statements += len(program["statements"])
    return count, statements


def differences_by_field(expected, actual):
    """Retain exact changed fingerprint fields, including raw-byte metrics."""
    rows = []
    with open_cases(expected, read(expected / "complete.json")) as left, open_cases(actual, read(actual / "complete.json")) as right:
        for case_index, (a, b) in enumerate(itertools.zip_longest(left, right)):
            if a is None or b is None:
                raise ValueError("truncated production capture")
            a, b = json.loads(a), json.loads(b)
            if a["id"] != b["id"]:
                raise ValueError("production identity changed")
            changed = {}
            for key in sorted(a.keys() | b.keys()):
                if a.get(key) == b.get(key):
                    continue
                if isinstance(a.get(key), dict) and isinstance(b.get(key), dict):
                    changed[key] = {field: {"old": a[key].get(field), "reviewed": b[key].get(field)}
                                    for field in sorted(a[key].keys() | b[key].keys())
                                    if a[key].get(field) != b[key].get(field)}
                else:
                    changed[key] = {"old": a.get(key), "reviewed": b.get(key)}
            if changed:
                rows.append({"id": a["id"], "case_index": case_index, "changed": changed})
    return rows


def adjudicate(root):
    parent = json.loads(portable_paths((root / "parent-certificates/s1-prefix.json").read_text()))
    parent_artifacts = parent["artifacts"]
    if (sha(root / "reviewed/sql-difftest") != parent_artifacts["$SCRATCH/p0-s1-prefix-corpus-v1/sql-difftest"]
            or sha(root / "dependencies/dependencies.jar") != parent["classpaths"]["$SCRATCH/p0-seam-tools-v6/dependencies.jar"]
            or sha(root / "scripts/determinism.py") != parent_artifacts["$SCRATCH/p0-s1-prefix-corpus-v1/certificate-tools/determinism.py"]):
        raise ValueError("frozen comparator or dependency identity differs from the source-pinned parent")
    completed = read(root / "reviewed/complete.json")
    if len(completed["shards"]) != len(SHARDS):
        raise ValueError("all ten original shards must finish before adjudication")
    recorded = {(r["mode"], r["shard"]): r for r in completed["shards"]}
    if set(recorded) != set(SHARDS) or completed["complete"] is not False:
        raise ValueError("unexpected original completion population or failure disposition")
    if read(root / "corrected-shards/complete.json")["complete"] is not True:
        raise ValueError("both corrected shards must finish")
    rows = []
    populations = {mode: dict(cases=0, statements=0, observations=0) for mode in ("classic", "gen2")}
    for mode, index in SHARDS:
        shard = f"shard-{index:03}"
        directory = root / "reviewed" / mode / shard
        baseline = directory / "baseline"
        source = root / "inputs" / mode / (shard + ".jsonl")
        programs = root / "transport" / mode / shard / "cases.jsonl"
        old = root / "prior" / mode / shard / "baseline"
        original_input = (f"$SCRATCH/p0-s2-full-v7/{shard}/input.jsonl" if mode == "classic"
                          else f"$SCRATCH/p0-gen2-full-determinism-v4/input-{index:03}.jsonl")
        bound_inputs = [(source, original_input),
                        (programs, f"$SCRATCH/p0-statement-program-v9/{mode}/{shard}/rust/cases.jsonl")]
        bound_inputs += [(old / name, f"$SCRATCH/p0-s1-prefix-corpus-v1/{mode}/{shard}/baseline/{name}")
                         for name in ("complete.json", "cases.jsonl.gz")]
        if any(parent_artifacts.get(name) != sha(path) for path, name in bound_inputs):
            raise ValueError("original input or production capture differs from parent certificate")
        original = compare(baseline, directory / "import")
        production = compare(old, baseline)
        summary = recorded[(mode, index)]
        if (summary.get("complete") != original["equal"]
                or any(summary.get(key) != original[key] for key in ("cases", "observations", "differences"))):
            raise ValueError("original root summary disagrees with shard results")
        if original != read(directory / "comparison.json") or production != read(directory / "production-comparison.json"):
            raise ValueError("recorded comparison differs from independent replay")
        if read(baseline / "complete.json")["input_sha256"] != sha(source):
            raise ValueError("SQL input hash differs from completion record")
        if any(original["failed_cases"].values()) or any(production["failed_cases"].values()):
            raise ValueError("a failed compilation cannot establish replay equivalence")
        final = original
        replacement = None
        if (mode, index) in REPLACEMENTS:
            if original["equal"] or [r["id"] for r in original["differences"]] != [IDENTITY]:
                raise ValueError("unexpected original failure population")
            corrected = root / "corrected-shards" / mode
            replacement = verify_replacement(programs, root / "corrected-case" / mode / "rust/cases.jsonl",
                                             corrected / "programs.jsonl", read(corrected / "provenance.json"))
            case = root / "corrected-case" / mode
            java_wire = json.dumps(read(case / "java/cases.jsonl"), sort_keys=True, ensure_ascii=False)
            rust_wire = json.dumps(read(case / "rust/cases.jsonl"), sort_keys=True, ensure_ascii=False)
            if java_wire != rust_wire:
                raise ValueError("corrected Java/Rust transport JSON differs")
            case_comparison = compare(case / "baseline", case / "import")
            if not case_comparison["equal"] or case_comparison != read(case / "comparison.json"):
                raise ValueError("corrected case does not independently replay")
            final = compare(baseline, corrected / "import")
            if final != read(corrected / "comparison.json"):
                raise ValueError("corrected comparison differs from independent replay")
            programs = corrected / "programs.jsonl"
        elif not original["equal"]:
            raise ValueError("an unapproved shard requires replacement")
        if (not final["equal"] or any(final["failed_cases"].values())
                or final["exported_schema_differences"] or final["exported_diagnostic_differences"]):
            raise ValueError("selected replay has an unresolved contract difference")
        if final["observation_contract"] != "full-prefix-v1" or final["artifact_contract"] != "seam-v1":
            raise ValueError("comparison contract was weakened")
        cases, statements = program_inventory(programs, source)
        if cases != final["cases"]:
            raise ValueError("transport population differs from replay")
        population = populations[mode]
        population["cases"] += cases
        population["statements"] += statements
        population["observations"] += final["observations"]
        rows.append({"mode": mode, "shard": index, "original_comparison": original,
                     "selected_comparison": final, "replacement": replacement,
                     "old_production_comparison": production,
                     "old_production_field_differences": differences_by_field(old, baseline)})
    return {"schema_version": 1, "complete": True, "scope": "Frozen reviewed S1 full-prefix replay, with two source-reviewed ROW-selector input replacements",
            "populations": populations, "shards": rows, "replaced_shards": 2,
            "selected_shards": len(rows),
            "selected_replay_artifact_byte_differences": [
                {"mode": row["mode"], "shard": row["shard"], "id": identity}
                for row in rows for identity in row["selected_comparison"]["artifact_byte_differences"]],
            "old_production_equal": all(row["old_production_comparison"]["equal"] for row in rows),
            "old_production_canonical_changed_case_records": sum(
                len(row["old_production_comparison"]["differences"]) for row in rows),
            "old_production_raw_byte_changed_case_records": sum(
                len(row["old_production_comparison"]["artifact_byte_differences"]) for row in rows),
            "changed_program_identities": [IDENTITY], "changed_program_records": 2,
            "original_failures_retained": True, "comparator_tolerances_changed": False,
            "gen2_raw_bytes_are_reported_metric_under_existing_seam_v1": True,
            "runtime_executed": False, "current_working_tree_covered": False, "P0_complete": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    result = adjudicate(args.root)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps({"complete": result["complete"], "populations": result["populations"]}))


if __name__ == "__main__":
    main()
