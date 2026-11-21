#!/usr/bin/env python3
"""Resolve captured assertions that cross diagnostic rendering boundaries.

Literal occurrences must have an unambiguous assignment. Repeated identical
bodies can be assigned in diagnostic order only when their occurrence count
equals their visible diagnostic count. Regexes require an exact reviewed
disposition; Python never interprets Java regex syntax. Body attribution does
not identify an emitting template or establish the final catalog count.
"""

import argparse
import hashlib
import json
from pathlib import Path

from diagnostic_expectation_attribution import java_field_matches


def occurrences(text, fragment):
    if not fragment:
        return []
    starts, position = [], 0
    while (position := text.find(fragment, position)) >= 0:
        starts.append(position)
        position += 1
    return starts


def resolve(raw, regex_reviews):
    """Return byte-independent Python string offsets in the retained rendering."""
    java_field_matches(raw)
    rendered = raw.get("rendered")
    if not isinstance(rendered, str):
        return {"disposition": "historical_rendering_unavailable", "body_matches": []}
    if raw["is_regex"]:
        reviews = [review for review in regex_reviews
                   if review["source_test"] == raw["source_test"] and review["expected"] == raw["expected"]]
        if len(reviews) != 1:
            return {"disposition": "regex_review_required", "body_matches": []}
        fragments = reviews[0]["asserted_literal_fragments"]
    else:
        fragments = [raw["expected"]]
    spans = []
    for fragment in fragments:
        starts = occurrences(rendered, fragment)
        if len(starts) != 1:
            return {"disposition": "assertion_occurrence_ambiguous", "body_matches": []}
        spans.append((starts[0], starts[0] + len(fragment)))
    if spans != sorted(spans):
        raise ValueError("reviewed assertion fragments occur in a different order")
    body_indices = {}
    for index in raw["visible_diagnostics"]:
        body_indices.setdefault(raw["diagnostics"][index]["message"], []).append(index)
    matches, ambiguous = [], []
    for index in raw["visible_diagnostics"]:
        message = raw["diagnostics"][index]
        starts = occurrences(rendered, message["message"])
        if not message["message"]:
            continue
        # Both CompilerMessages.format and the warning-list renderer preserve
        # visible message order and print each body once. Extra occurrences in
        # context, snippets, headers, or another body make this assignment fail.
        identical = body_indices[message["message"]]
        if len(starts) != len(identical):
            ambiguous.append(index)
            continue
        start = starts[identical.index(index)]
        end = start + len(message["message"])
        overlaps = [(max(start, left), min(end, right)) for left, right in spans]
        overlaps = [(left, right) for left, right in overlaps
                    if left < right and any(char.isalnum() for char in rendered[left:right])]
        if overlaps:
            matches.append({
                "diagnostic_index": index, "error_type": message["error_type"],
                "message": message["message"], "body_span": [start, end],
                "asserted_spans": [list(span) for span in overlaps],
                "asserted_body_fragments": [rendered[left:right] for left, right in overlaps],
            })
    if ambiguous:
        return {"disposition": "body_occurrence_ambiguous", "body_matches": [],
                "ambiguous_diagnostics": ambiguous}
    return {"disposition": "body_fields_resolved" if matches else "no_body_text_asserted",
            "body_matches": matches, "assertion_spans": [list(span) for span in spans],
            "reviewed_java_regex": raw["is_regex"]}


def report(raw_rows, attribution, reviews):
    if len(raw_rows) != len(attribution["java"]):
        raise ValueError("Java attribution population differs")
    result = []
    for index, (raw, prior) in enumerate(zip(raw_rows, attribution["java"])):
        if prior["record_index"] != index or any(raw[key] != prior[key] for key in ("source_test", "call_site", "expected", "is_regex")):
            raise ValueError("Java attribution identity differs")
        if prior["disposition"] != "rendered_boundary_or_other_field_unresolved":
            continue
        result.append({"record_index": index, "source_test": raw["source_test"],
                       "call_site": raw["call_site"], "expected": raw["expected"],
                       **resolve(raw, reviews)})
    return {"schema_version": 1, "rows": result,
            "counts": {"reviewed_cross_field_assertions": len(result),
                       "resolved_assertions": sum(row["disposition"] == "body_fields_resolved" for row in result),
                       "selected_body_occurrences": sum(len(row["body_matches"]) for row in result)},
            "scope": "rendered field attribution only; template identities require separate source review",
            "required_template_count": None, "catalog_reviewed": False, "P0_complete": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("raw", "attribution", "dispositions", "output"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    raw = [json.loads(line) for line in args.raw.read_text().splitlines()]
    attribution = json.loads(args.attribution.read_text())
    dispositions = json.loads(args.dispositions.read_text())
    result = report(raw, attribution, dispositions["reviewed_regex_assertions"])
    result["inputs"] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                        for path in (args.raw, args.attribution, args.dispositions)}
    with args.output.open("x") as output:
        json.dump(result, output, indent=2, ensure_ascii=False)
        output.write("\n")
    print(json.dumps(result["counts"]))


if __name__ == "__main__":
    main()
