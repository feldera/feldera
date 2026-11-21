#!/usr/bin/env python3
"""Derive reproducible raw-parser mutations from every distinct SQL submission."""

import argparse
import hashlib
import json
from pathlib import Path

from compare_parsers import digest


def mutations(sql):
    """Edit Unicode code points, including quoted/comment text; no SQL lexer assumed."""
    for operation in ("delete", "replace_closer", "insert_quote", "append_clause"):
        seed = hashlib.sha256((operation + "\0" + sql).encode()).digest()
        offset = int.from_bytes(seed[:8], "big") % len(sql)
        if operation == "delete":
            removed, inserted = sql[offset], ""
        elif operation == "replace_closer":
            removed, inserted = sql[offset], ")" if sql[offset] != ")" else "("
        elif operation == "insert_quote":
            removed, inserted = "", "'"
        else:
            offset, removed, inserted = (
                len(sql.rstrip().rstrip(";")),
                "",
                " QUALIFY TRUE",
            )
        changed = sql[:offset] + inserted + sql[offset + len(removed) :]
        assert changed != sql
        yield (
            operation,
            changed,
            {
                "offset_codepoints": offset,
                "removed": removed,
                "inserted": inserted,
            },
        )


def prepare(path):
    before = digest(path)
    identities, originals = set(), {}
    empty, submissions = 0, 0
    with path.open() as stream:
        for line in stream:
            case = json.loads(line)
            identity = case.get("id")
            if not isinstance(identity, str) or not identity or identity in identities:
                raise ValueError("Missing or duplicate corpus identity")
            identities.add(identity)
            if not isinstance(case.get("submissions"), list) or not case["submissions"]:
                raise ValueError("Expected nonempty SQL submissions")
            for index, submission in enumerate(case["submissions"]):
                sql, many, batch = (
                    submission.get("sql"),
                    submission.get("many"),
                    submission.get("batch"),
                )
                if (
                    not isinstance(sql, str)
                    or type(many) is not bool
                    or type(batch) is not int
                    or batch < 0
                ):
                    raise ValueError("Malformed corpus SQL submission")
                submissions += 1
                if not sql.strip():
                    empty += 1
                    continue
                originals.setdefault((sql, many), []).append(
                    {"case": identity, "submission": index, "batch": batch}
                )
    if not originals or digest(path) != before:
        raise ValueError("Corpus is empty or changed during preparation")
    cases, provenance = [], []
    for (sql, many), origins in sorted(originals.items()):
        fingerprint = hashlib.sha256(json.dumps([sql, many]).encode()).hexdigest()
        base = f"mutation/{fingerprint}"
        # The unchanged control distinguishes a mutation disagreement from a
        # pre-existing grammar gap. Raw parsing needs no prior catalog state.
        variants = [("control", sql, None), *mutations(sql)]
        for operation, changed, edit in variants:
            cases.append(
                {
                    "id": f"{base}/{operation}",
                    "submissions": [{"sql": changed, "many": many, "batch": 0}],
                }
            )
            provenance.append(
                {"id": f"{base}/{operation}", "edit": edit, "original": base}
            )
        origins.sort(key=lambda origin: (origin["case"], origin["submission"]))
        provenance.append({"id": base, "sql": sql, "many": many, "origins": origins})
    return cases, {
        "schema_version": 1,
        "scope": "isolated raw submissions with deterministic Unicode edits and an appended QUALIFY clause",
        "input_sha256": before,
        "generator_sha256": digest(Path(__file__)),
        "source_cases": len(identities),
        "source_submissions": submissions,
        "empty_submissions_skipped": empty,
        "distinct_submissions": len(originals),
        "controls": len(originals),
        "mutations": 4 * len(originals),
        "cases": len(cases),
        "source_options_applied": False,
        "semantic_equivalence_checked": False,
        "provenance": provenance,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    cases, report = prepare(args.input)
    args.output.mkdir()
    inputs = args.output / "input.jsonl"
    with inputs.open("x") as output:
        for case in cases:
            output.write(json.dumps(case, ensure_ascii=False) + "\n")
    report["generated_input_sha256"] = digest(inputs)
    (args.output / "preparation.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
