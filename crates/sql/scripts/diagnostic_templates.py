#!/usr/bin/env python3
"""Cluster diagnostic candidates by parsed source literals, preserving ambiguity.

This is a review aid, not a message-catalog acceptance or a threshold decision.
Dynamic source expressions are never evaluated. Constructor candidates may have
suffixes supplied by an unresolved overload, so prefix matches stay distinct.
"""

import argparse
import collections
import hashlib
import json
import re
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def normalized_parts(parts):
    result = []
    for part in parts:
        if set(part) == {"literal"} and isinstance(part["literal"], str):
            if not part["literal"]:
                continue
            if result and "literal" in result[-1]:
                result[-1]["literal"] += part["literal"]
            else:
                result.append(dict(part))
        elif set(part) == {"dynamic"} and isinstance(part["dynamic"], str):
            if not result or "parameter" not in result[-1]:
                result.append({"parameter": True})
        else:
            raise ValueError("unknown source template part")
    return result


def cluster(extraction, census, source_root):
    if extraction["schema_version"] != 1 or census["schema_version"] != 1:
        raise ValueError("unsupported template or census version")
    templates, unresolved, structural, sources = {}, [], [], {}
    for site in extraction["sites"]:
        source = (source_root / site["source"]).resolve()
        source.relative_to(source_root.resolve())
        if not source.is_file():
            raise ValueError(f"missing emission source: {source}")
        sources[site["source"]] = digest(source)
        parts = normalized_parts(site["parts"])
        dynamic = any("parameter" in part for part in parts)
        if dynamic and not any(part.get("literal", "").strip() for part in parts):
            unresolved.append(site)
            continue
        if dynamic and not any(any(char.isalnum() for char in part.get("literal", "")) for part in parts):
            structural.append(site)
            continue
        family = site["callee"] if site["kind"] == "constructor" else "report"
        key = json.dumps([family, site.get("error_type"), parts], sort_keys=True, ensure_ascii=False)
        identity = hashlib.sha256(key.encode()).hexdigest()
        template = templates.setdefault(identity, {
            "id": identity, "family": family, "error_type": site.get("error_type"), "parts": parts, "sites": [],
            "constructor_overload_resolved": False if site["kind"] == "constructor" else None,
        })
        template["sites"].append(site)
    patterns = {
        identity: re.compile("".join(re.escape(part["literal"]) if "literal" in part else ".*?"
                                     for part in template["parts"]), re.DOTALL)
        for identity, template in templates.items()
    }
    java = {}
    for row in census["java"]:
        for message in row["messages"]:
            key = (message["error_type"], message["message"])
            group = java.setdefault(key, {**message, "occurrences": 0, "call_sites": set()})
            if group["calcite_candidates"] != message["calcite_candidates"]:
                raise ValueError("inconsistent Calcite candidates for one message")
            group["occurrences"] += 1
            group["call_sites"].add(row["call_site"])
    matched = collections.Counter()
    for group in java.values():
        group["call_sites"] = sorted(group["call_sites"])
        candidates = []
        for identity, pattern in patterns.items():
            error_type = templates[identity]["error_type"]
            if error_type is not None and error_type != group["error_type"]:
                continue
            if pattern.fullmatch(group["message"]):
                kind = "full_expression"
            elif templates[identity]["family"] in {"UnsupportedException", "UnimplementedException", "InternalCompilerError"} and pattern.match(group["message"]):
                kind = "constructor_prefix"
            else:
                continue
            candidates.append({"id": identity, "match": kind})
            matched[identity] += group["occurrences"]
        group["source_candidates"] = candidates
    python = []
    for row in census["python"]:
        fragment = row["expected"].strip().lower()
        candidates = []
        for identity, template in templates.items():
            pattern = patterns[identity]
            if re.fullmatch(pattern.pattern, fragment, re.DOTALL | re.IGNORECASE) or any(
                fragment in part.get("literal", "").lower() for part in template["parts"]
            ):
                candidates.append(identity)
        python.append({**row, "source_candidates": candidates, "executed": False})
    messages = sorted(java.values(), key=lambda row: (row["error_type"], row["message"]))
    return {
        "schema_version": 1,
        "complete": True,
        "scope": "source-expression candidates for the recorded Java diagnostics and Python expectations",
        "counts": {
            "source_files_parsed": extraction["source_files"],
            "source_emission_sites": len(extraction["sites"]),
            "literal_expression_templates": len(templates),
            "dynamic_only_sites": len(unresolved),
            "punctuation_only_sites": len(structural),
            "distinct_java_diagnostics": len(messages),
            "java_diagnostics_with_source_candidates": sum(bool(row["source_candidates"]) for row in messages),
            "java_diagnostics_without_any_candidate": sum(not row["source_candidates"] and not row["calcite_candidates"] for row in messages),
            "java_diagnostics_with_ambiguous_source_candidates": sum(len(row["source_candidates"]) > 1 for row in messages),
            "source_templates_matching_java": len(matched),
            "python_expectations": len(python),
            "python_expectations_with_source_candidates": sum(bool(row["source_candidates"]) for row in python),
        },
        "templates": [dict(template, java_matches=matched[identity]) for identity, template in sorted(templates.items())],
        "dynamic_only_sites": unresolved,
        "punctuation_only_sites": structural,
        "java": messages,
        "python": python,
        "platform": census["platform"],
        "source_hashes": sources,
        "catalog_reviewed": False,
        "required_template_count": None,
        "threshold_decision": "pending candidate resolution and runtime/compiler classification",
        "P0_complete": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("extraction", type=Path)
    parser.add_argument("census", type=Path)
    parser.add_argument("source_root", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    report = cluster(json.loads(args.extraction.read_text()), json.loads(args.census.read_text()), args.source_root)
    report["inputs"] = {str(path): digest(path) for path in [args.extraction, args.census]}
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2, ensure_ascii=False)
        stream.write("\n")
    print(json.dumps(report["counts"]))


if __name__ == "__main__":
    main()
