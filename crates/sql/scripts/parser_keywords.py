#!/usr/bin/env python3
"""Generate quoted/unquoted identifier probes from pinned parser inventories."""

import argparse
import hashlib
import importlib.util
import json
import re
import subprocess
from pathlib import Path


def documented_reserved(path):
    text = path.read_text()
    section = text.split("## Reserved keywords\n", 1)[1]
    table = [line for line in section.splitlines() if line.startswith("| ")]
    words = {word for line in table for word in re.findall(r"`([A-Z_0-9-]+)`", line)}
    if not words:
        raise ValueError("the documented reserved-word table is empty")
    return words


def statements(word):
    """Declaration and reference contexts must be measured independently."""
    return {
        "table": f"CREATE TABLE {word} (i INTEGER)",
        "column": f"CREATE TABLE t ({word} INTEGER)",
        "view": f"CREATE VIEW {word} AS SELECT 1 AS i",
        "qualified_reference": f"SELECT t.{word} FROM t",
        "alias": f"SELECT 1 AS {word}",
    }


def verify_reserved(actual, expected):
    if (
        not isinstance(actual, list)
        or not actual
        or any(not isinstance(word, str) for word in actual)
        or len(set(actual)) != len(actual)
    ):
        raise ValueError("Malformed prototype reserved-word inventory")
    missing = sorted(expected - set(actual))
    extra = sorted(set(actual) - expected)
    if missing or extra:
        raise ValueError(
            f"Prototype reserved words differ from pinned grammar: missing={missing}, extra={extra}"
        )


def generate(repo, calcite, executable, output, manifest_path):
    helper = repo / "sql-to-dbsp-compiler/check-keyword-drift.py"
    config = repo / "sql-to-dbsp-compiler/SQL-compiler/src/main/codegen/config.fmpp"
    template = calcite / "core/src/main/codegen/templates/Parser.jj"
    grammar = repo / "docs.feldera.com/docs/sql/grammar.md"
    extension = config.parent / "includes/parserImpls.ftl"
    sources = [helper, config, template, grammar, extension]
    for source in sources:
        if not source.is_file():
            raise ValueError(f"required keyword source is absent: {source}")
    # Reuse the repository's parser for its FMPP lists and Calcite tokens.
    spec = importlib.util.spec_from_file_location("keyword_drift", helper)
    drift = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(drift)
    declared = drift.declared_keywords(template) | drift.words_in_list(
        config, "keywords", False
    )
    # Feldera's DATEADD family is declared directly in its parser extension,
    # outside the template's KEYWORDS block and the FMPP keyword lists.
    extension_words = set(
        re.findall(r'<\s*[A-Z_0-9]+\s*:\s*"([A-Z_0-9-]+)"\s*>', extension.read_text())
    )
    if not extension_words:
        raise ValueError(
            "parser extension token declarations changed; update the inventory"
        )
    declared |= extension_words
    non_reserved = drift.words_in_list(
        config, "nonReservedKeywords", False
    ) | drift.words_in_list(config, "nonReservedKeywordsToAdd", False)
    reserved = declared - non_reserved
    documented = documented_reserved(grammar)
    upstream = json.loads(
        subprocess.run(
            [str(executable), "--keywords"], check=True, capture_output=True, text=True
        ).stdout
    )
    if set(upstream) != {"0.62.0", "0.64.0"}:
        raise ValueError("expected both pinned Rust keyword tables")
    prototype_reserved = json.loads(
        subprocess.run(
            [str(executable), "--reserved-keywords"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    verify_reserved(prototype_reserved, reserved)
    words = (
        declared
        | documented
        | {word for values in upstream.values() for word in values}
    )
    if any(not re.fullmatch(r"[A-Z_0-9-]+", word) for word in words):
        raise ValueError("unexpected keyword syntax; review the probe quoting")
    count = 0
    with output.open("x") as stream:
        for word in sorted(words):
            for quoted in (False, True):
                identifier = f'"{word}"' if quoted else word
                for context, sql in statements(identifier).items():
                    row = {
                        "id": f"keyword/{word}/{'quoted' if quoted else 'bare'}/{context}",
                        "submissions": [{"sql": sql, "many": True, "batch": 0}],
                    }
                    stream.write(json.dumps(row) + "\n")
                    count += 1
    manifest = {
        "schema_version": 1,
        "cases": count,
        "keywords": len(words),
        "contexts": list(statements("word")),
        "calcite_declared": len(declared),
        "feldera_reserved": sorted(reserved),
        "prototype_reserved": prototype_reserved,
        "prototype_reserved_matches_grammar": True,
        "documented_reserved": sorted(documented),
        "reserved_only_in_code": sorted(reserved - documented),
        "reserved_only_in_docs": sorted(documented - reserved),
        "upstream_counts": {
            version: len(values) for version, values in upstream.items()
        },
        "input_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        "source_sha256": {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources
        },
    }
    with manifest_path.open("x") as stream:
        json.dump(manifest, stream, indent=2)
        stream.write("\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("calcite", type=Path)
    parser.add_argument("executable", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("manifest", type=Path)
    parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parents[3]
    )
    args = parser.parse_args()
    report = generate(
        args.repo, args.calcite, args.executable, args.output, args.manifest
    )
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "cases",
                    "keywords",
                    "reserved_only_in_code",
                    "reserved_only_in_docs",
                )
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
