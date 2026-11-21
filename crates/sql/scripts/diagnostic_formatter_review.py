#!/usr/bin/env python3
"""Reconstruct reviewed diagnostic formatter chains without wildcard messages.

This validates the recorded examples, not every possible Java/Jackson input.
Enum spellings come from pinned declarations. External error envelopes and
Boolean policy strings are checked against the archived dependency bytecode.
"""

import argparse
import hashlib
import json
from pathlib import Path


DURATION_UNITS = 'expected one of "ns", "us", "ms", "s", "m", "h", or "d"'
SIZE_UNITS = ('expected one of "k", "M", "G", "T", "P", "E", "Ki", "Mi", '
              '"Gi", "Ti", "Pi", or "Ei", each with or without a trailing "B"')
SUGGEST = ('You can suggest support for this feature by filing an issue at '
           'https://github.com/feldera/feldera/issues')


def quoted(value):
    # The reviewed names and enum values use the common ASCII escaping subset.
    # Reject broader inputs rather than imply this reimplements Utilities.escape.
    if any(ord(char) < 32 or ord(char) > 126 for char in value):
        raise ValueError("quoted input is outside the reviewed ASCII subset")
    return json.dumps(value)


def part_of(part, text, kind):
    result = "'" + part + "'"
    return result if part == text else result + " in " + kind + " '" + text + "'"


def render(identity, arguments, enums):
    """Render only the reviewed source branches; unknown identities fail closed."""
    a = arguments
    if identity == "config.unknown_field":
        return "unknown field " + quoted(a["field"])
    if identity == "config.enum_invalid":
        prefix = "field " + quoted(a["field"]) + ": " if a["field"] else ""
        values = ", ".join(quoted(value) for value in enums[a["enum"]]["values"])
        return prefix + "invalid value " + quoted(a["value"]) + "; valid values are: " + values
    if identity in ("config.boolean_primitive", "config.boolean_boxed"):
        primitive = identity == "config.boolean_primitive"
        target = "boolean" if primitive else "java.lang.Boolean"
        policy = ('only "true"/"True"/"TRUE" or "false"/"False"/"FALSE" recognized'
                  if primitive else 'only "true" or "false" recognized')
        prefix = "field " + quoted(a["field"]) + ": " if a["field"] else ""
        return (prefix + "Cannot deserialize value of type `" + target
                + "` from String " + quoted(a["value"]) + ": " + policy)
    if identity.startswith("quantity."):
        kind = a["kind"]
        if kind not in ("size", "duration"):
            raise ValueError("unknown quantity kind")
        units = SIZE_UNITS if kind == "size" else DURATION_UNITS
        if identity == "quantity.unknown_unit":
            detail = "unknown unit " + part_of(a["unit"], a["text"], kind) + "; " + units
        elif identity == "quantity.missing_unit" and kind == "duration":
            detail = part_of(a["number"], a["text"], kind) + " has no unit; " + units
        elif identity == "quantity.negative" and kind == "duration":
            detail = "duration '" + a["text"] + "' cannot be negative"
        elif identity == "quantity.empty" and kind == "duration":
            detail = 'duration is empty; write "0", or a number and a unit such as "500ms"'
        else:
            raise ValueError("quantity branch was not reviewed")
        return quoted(a["path"].replace("/", ".")) + ": " + detail
    if identity == "geometry.literal_conversion":
        return ("cannot convert " + a["sql_type"] + " literal to class " + a["target_class"]
                + "\n" + a["node_display"] + "\n" + SUGGEST)
    raise ValueError("unreviewed formatter identity: " + identity)


def validate(review):
    identities = {entry["id"] for entry in review["identities"]}
    seen = set()
    for body in review["bodies"]:
        identity = body["identity"]
        if identity not in identities:
            raise ValueError("body has an undeclared identity")
        message = render(identity, body["arguments"], review["enums"])
        if message != body["message"]:
            raise ValueError("formatter reconstruction differs: " + identity)
        digest = hashlib.sha256(message.encode()).hexdigest()
        if digest != body["sha256"] or digest in seen:
            raise ValueError("invalid or duplicate body digest")
        if not body["observations"]:
            raise ValueError("body lacks capture provenance")
        seen.add(digest)
    for ambiguity in review["ambiguous_bodies"]:
        candidates = set(ambiguity["candidate_ids"])
        selected = {row["candidate_id"] for row in ambiguity["selected_sites"]}
        rejected = {row["candidate_id"] for row in ambiguity["rejected_candidates"]}
        if not selected or selected & rejected or selected | rejected != candidates:
            raise ValueError("ambiguous candidates are not fully accounted for")
        if not all(row["observations"] for row in ambiguity["selected_sites"]):
            raise ValueError("selected emitter lacks capture provenance")
    return {"reconstructed_bodies": len(seen), "formatter_identities": len(identities),
            "reviewed_ambiguous_bodies": len(review["ambiguous_bodies"]),
            "required_template_count": None, "catalog_reviewed": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("review", type=Path)
    args = parser.parse_args()
    print(json.dumps(validate(json.loads(args.review.read_text())), indent=2))


if __name__ == "__main__":
    main()
