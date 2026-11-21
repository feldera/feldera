#!/usr/bin/env python3
"""Locate asserted text within diagnostics without counting incidental messages.

Matches identify candidate dependencies, not the emitting source or an approved
template count. Java regular expressions and rendered-message boundaries remain
unresolved; Python compiler acceptance never certifies a runtime assertion.
"""

import argparse
import collections
import hashlib
import json
import re
from pathlib import Path

from diagnostic_census import message_pattern
from python_diagnostics import summarize

ASSERTION_FIELDS = ("assertion_kind", "rendered", "visible_diagnostics", "field_matches")

def field_matches(expected, diagnostics, *, ignore_case=False):
    """Keep body, class, and source-snippet matches separate, including overlaps."""
    if not isinstance(expected, str) or not expected:
        raise ValueError("a nonempty expectation is required")
    fragment = expected.strip().lower() if ignore_case else expected
    if not fragment:
        raise ValueError("an empty normalized expectation cannot identify a template")
    hits = []
    for index, diagnostic in enumerate(diagnostics):
        fields = []
        for field in ("message", "error_type", "snippet"):
            value = diagnostic.get(field)
            if not isinstance(value, str):
                raise ValueError(f"diagnostic lacks string {field}")
            if fragment in (value.lower() if ignore_case else value):
                fields.append(field)
        if fields:
            hits.append({"diagnostic_index": index, "fields": fields})
    return hits


def source_candidates(message, templates):
    """Retain every source-expression match, including uncertain constructor tails."""
    candidates = []
    for template in templates:
        if template["error_type"] not in (None, message["error_type"]):
            continue
        expression = "".join(re.escape(part["literal"]) if "literal" in part else ".*?"
                             for part in template["parts"])
        pattern = re.compile(expression, re.DOTALL)
        if pattern.fullmatch(message["message"]):
            match = "full_expression"
        elif template["family"] in {"UnsupportedException", "UnimplementedException", "InternalCompilerError"} and pattern.match(message["message"]):
            match = "constructor_prefix"
        else:
            continue
        candidates.append({"id": template["id"], "match": match})
    return candidates


def java_field_matches(raw):
    """Prefer matches captured by the actual Java assertion implementation.

    Historical records lack the rendered string and field matches. Their one
    Java regex remains unresolved; Python must not reinterpret its syntax.
    """
    fields_present = {key for key in ASSERTION_FIELDS if key in raw}
    if not fields_present:
        return None if raw["is_regex"] else field_matches(raw["expected"], raw["diagnostics"])
    if fields_present != set(ASSERTION_FIELDS):
        raise ValueError("incomplete Java field attribution")
    if raw.get("assertion_kind") not in {"compilation_error", "warnings", "messages"} or not isinstance(raw.get("rendered"), str):
        raise ValueError("Java field attribution lacks its rendered assertion")
    visible = raw["visible_diagnostics"]
    if (not isinstance(visible, list)
            or any(type(index) is not int or not 0 <= index < len(raw["diagnostics"]) for index in visible)
            or visible != sorted(set(visible))):
        raise ValueError("invalid visible Java diagnostic indices")
    if raw["assertion_kind"] == "warnings" and any(raw["diagnostics"][index].get("warning") is not True for index in visible):
        raise ValueError("warning rendering includes an error")
    hits, seen = raw["field_matches"], set()
    if not isinstance(hits, list):
        raise ValueError("Java field matches must be an array")
    for hit in hits:
        if not isinstance(hit, dict) or set(hit) != {"diagnostic_index", "fields"}:
            raise ValueError("malformed Java field match")
        index, fields = hit["diagnostic_index"], hit["fields"]
        if (type(index) is not int or not 0 <= index < len(raw["diagnostics"])
                or index not in visible or index in seen or not isinstance(fields, list) or not fields
                or any(field not in ("message", "error_type", "snippet") for field in fields)
                or len(set(fields)) != len(fields)):
            raise ValueError("invalid Java field match reference")
        if raw["assertion_kind"] == "warnings" and raw["diagnostics"][index].get("warning") is not True:
            raise ValueError("warning assertion attributed to an error")
        seen.add(index)
    if not raw["is_regex"]:
        if raw["expected"] not in raw["rendered"]:
            raise ValueError("literal assertion does not match its rendered text")
        expected_hits = [hit for hit in field_matches(raw["expected"], raw["diagnostics"])
                         if hit["diagnostic_index"] in visible]
        if hits != expected_hits:
            raise ValueError("captured literal field matches differ from diagnostic fields")
    return hits


def attribute(raw_java, census, templates, python_inputs, python_results, resources):
    if census["schema_version"] != 1 or templates["schema_version"] != 1:
        raise ValueError("unsupported diagnostic input version")
    if len(raw_java) != len(census["java"]) or not raw_java:
        raise ValueError("raw Java records disagree with census")
    python_report = summarize(python_inputs, python_results)
    patterns = {}
    for key, template in resources.items():
        try:
            patterns[key] = message_pattern(template)[0]
        except ValueError:
            pass  # The census retains unsupported resource formats separately.
    candidates = {(row["error_type"], row["message"]): row for row in templates["java"]}
    java_rows, python_rows = [], []

    def annotate(message, prior=None):
        return {
            "error_type": message["error_type"], "message": message["message"],
            "source_candidates": prior["source_candidates"] if prior is not None else source_candidates(message, templates["templates"]),
            "calcite_candidates": prior["calcite_candidates"] if prior is not None else [key for key, pattern in patterns.items() if pattern.fullmatch(message["message"])],
        }

    for index, (raw, recorded) in enumerate(zip(raw_java, census["java"])):
        for key in ("source_test", "call_site", "expected", "is_regex"):
            if raw[key] != recorded[key]:
                raise ValueError(f"Java census identity differs at record {index}")
        for key in ASSERTION_FIELDS:
            if (key in raw) != (key in recorded) or raw.get(key) != recorded.get(key):
                raise ValueError(f"Java census assertion metadata differs at record {index}")
        messages = raw["diagnostics"]
        if [(m["error_type"], m["message"]) for m in messages] != [(m["error_type"], m["message"]) for m in recorded["messages"]]:
            raise ValueError(f"Java census messages differ at record {index}")
        captured_matches = java_field_matches(raw)
        hits = captured_matches or []
        bodies = [hit["diagnostic_index"] for hit in hits if "message" in hit["fields"]]
        disposition = "regex_requires_java_rendered_attribution" if captured_matches is None else (
            "message_body_candidates" if bodies else "non_body_fields_only" if hits else "rendered_boundary_or_other_field_unresolved")
        java_rows.append({
            "record_index": index, **{key: raw[key] for key in ("source_test", "call_site", "expected", "is_regex")},
            "disposition": disposition, "field_matches": hits,
            "message_candidates": [dict(annotate(messages[i], candidates[(messages[i]["error_type"], messages[i]["message"])]), diagnostic_index=i) for i in bodies],
            "diagnostic_count": len(messages),
            "assertion_kind": raw.get("assertion_kind", "historical_compilation_error"),
            "field_matches_captured_by_java": "field_matches" in raw,
        })
    for source, result, classified in zip(python_inputs["records"], python_results, python_report["cases"]):
        row = {"id": source["id"], "expected": source["expected"], "compiler_disposition": classified["disposition"], "message_candidates": [], "field_matches": []}
        if not source["active"]:
            row["disposition"] = "not_registered"
        elif not result["compiler_rejected"]:
            row["disposition"] = "runtime_evaluation_pending"
        else:
            hits = field_matches(source["expected"], result["diagnostics"], ignore_case=True)
            bodies = [hit["diagnostic_index"] for hit in hits if "message" in hit["fields"]]
            row["field_matches"] = hits
            row["message_candidates"] = [dict(annotate(result["diagnostics"][i]), diagnostic_index=i) for i in bodies]
            row["disposition"] = "message_body_candidates" if bodies else "non_body_fields_only" if hits else "rendered_boundary_or_other_field_unresolved"
        python_rows.append(row)
    selected = [message for row in java_rows + python_rows for message in row["message_candidates"]]
    return {
        "schema_version": 1, "complete": True,
        "scope": "candidate dependencies of assertions, with rendered fields and runtime outcomes kept separate",
        "counts": {
            "java_assertions": len(java_rows), "java_dispositions": dict(collections.Counter(row["disposition"] for row in java_rows)),
            "java_field_attributions_captured": sum(row["field_matches_captured_by_java"] for row in java_rows),
            "python_assignments": len(python_rows), "python_dispositions": dict(collections.Counter(row["disposition"] for row in python_rows)),
            "platform_exact_assertions": len(census["platform"]),
            "distinct_candidate_bodies": len({(row["error_type"], row["message"]) for row in selected}),
            "source_candidates_for_asserted_bodies": len({candidate["id"] for row in selected for candidate in row["source_candidates"]}),
            "calcite_candidates_for_asserted_bodies": len({key for row in selected for key in row["calcite_candidates"]}),
            "distinct_candidate_bodies_without_template": len({(row["error_type"], row["message"]) for row in selected if not row["source_candidates"] and not row["calcite_candidates"]}),
        },
        "java": java_rows, "python": python_rows,
        "platform": [{**row, "disposition": "exact_composite_assertion_requires_renderer_and_message_attribution"} for row in census["platform"]],
        "required_template_count": None, "threshold_decision": "pending",
        "catalog_reviewed": False, "runtime_executed": False, "P0_complete": False,
        "limitations": [
            "A body substring match does not identify the emitting source; all compatible templates remain candidates.",
            "A snippet or error-class assertion does not establish dependence on the message body.",
            "Java regular expressions are not evaluated using Python regex semantics.",
            "Historical Java records omit rendered text and Java field matches; new records preserve both. Matches across rendering boundaries remain unresolved.",
            "Only the supplied assertion capture is counted; uncaptured helpers remain outside the report's population.",
            "Accepted Python programs have no observed runtime error in this evidence.",
            "The platform assertion covers SDK formatting and source snippets as well as compiler message text.",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("raw_java", "census", "templates", "python_inputs", "python_results", "resources", "output"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    paths = [getattr(args, name) for name in ("raw_java", "census", "templates", "python_inputs", "python_results", "resources")]
    loaded = [[json.loads(line) for line in path.read_text().splitlines()] if index in (0, 4) else json.loads(path.read_text()) for index, path in enumerate(paths)]
    report = attribute(*loaded)
    report["inputs"] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2, ensure_ascii=False)
        stream.write("\n")
    print(json.dumps(report["counts"]))


if __name__ == "__main__":
    main()
