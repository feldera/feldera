#!/usr/bin/env python3
"""Replace local paths in P0 evidence and document access to private archives."""

import argparse
import json
import re
from pathlib import Path

# Original capture roots can occur in keys, command lines, and JVM classpaths.
REPO = re.compile(r"/(?:Users/[^/\s]+/Dev/feldera-new-compiler|workspaces/feldera)\b")
MAVEN = re.compile(r"/home/[^/\s]+/\.m2/repository\b")
CARGO = re.compile(r"/home/[^/\s]+/\.cargo\b")
SCRATCH = re.compile(r"(?<![\w./$-])/(?:private/)?tmp(?=/|\b)")
AVAILABILITY = "available from the plan owner on request"


def portable_paths(text):
    text = REPO.sub("$REPO", text).replace("$REPO/.p0-artifacts/", ".p0-artifacts/")
    text = MAVEN.sub("$M2", text)
    text = CARGO.sub("$CARGO_HOME", text)
    return SCRATCH.sub("$SCRATCH", text)


def normalize(text, is_json):
    text = portable_paths(text)
    if is_json:
        record = json.loads(text)
        if ".p0-artifacts/" in text and "archive_availability" not in record:
            if not isinstance(record, dict):
                raise ValueError(
                    "Cannot add archive availability to this record. Supply a JSON object."
                )
            # Keep formatting, numeric literals, and recorded digests verbatim.
            start = text.index("{") + 1
            text = (
                text[:start]
                + f'\n  "archive_availability": "{AVAILABILITY}",'
                + text[start:]
            )
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", type=Path, nargs="+")
    parser.add_argument(
        "--check", action="store_true", help="report files that need changes"
    )
    args = parser.parse_args()
    changed = 0
    for path in args.files:
        if path.suffix not in (".md", ".json"):
            parser.error(
                f"{path} has an unsupported format. Supply Markdown or JSON evidence."
            )
        original = path.read_bytes().decode()
        try:
            result = normalize(original, path.suffix == ".json")
        except ValueError as error:
            parser.error(f"Cannot normalize {path}: {error}. Check the JSON record.")
        if original != result:
            changed += 1
            print(path)
            if not args.check:
                path.write_bytes(result.encode())
    if changed and args.check:
        parser.exit(
            1, "Run the same command without --check to normalize the listed files.\n"
        )


if __name__ == "__main__":
    main()
