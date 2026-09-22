#!/usr/bin/env python3
"""Extract the SQL program stored in a Feldera support bundle.

A pipeline may also carry user-defined functions: Rust in `udf_rust`, and the
Cargo dependencies they need in `udf_toml`, a fragment that belongs under
`[dependencies]`.  These can be extracted using -o

    extract_bundle_sql.py BUNDLE.zip > program.sql
    extract_bundle_sql.py -o program.sql BUNDLE.zip
"""

import argparse
import gzip
import json
import os
import posixpath
import sys
import zipfile
from typing import NamedTuple

CONFIG_NAME = "pipeline_config.json"
# A pipeline older than the config version stores it gzipped in the bundle.
GZIPPED_CONFIG_NAME = CONFIG_NAME + ".gz"


class Program(NamedTuple):
    """SQL and the user-defined functions"""

    sql: str
    udf_rust: str
    udf_toml: str


def read_program(bundle: str) -> Program:
    """Return the program in `bundle`, or raise LookupError."""
    with zipfile.ZipFile(bundle) as archive:
        configs = sorted(
            name
            for name in archive.namelist()
            if posixpath.basename(name).endswith((CONFIG_NAME, GZIPPED_CONFIG_NAME))
        )
        if not configs:
            raise LookupError(
                f"no {CONFIG_NAME} inside; is this a Feldera support bundle?"
            )
        raw = archive.read(configs[0])
        if configs[0].endswith(".gz"):
            raw = gzip.decompress(raw)
        config = json.loads(raw)

    sql = config.get("program_code")
    if not isinstance(sql, str) or not sql.strip():
        raise LookupError(f"{configs[0]} has no program_code")

    def text(field: str) -> str:
        value = config.get(field)
        return value if isinstance(value, str) and value.strip() else ""

    return Program(sql=sql, udf_rust=text("udf_rust"), udf_toml=text("udf_toml"))


def ends_with_newline(text: str) -> str:
    return text if text.endswith("\n") else text + "\n"


def write(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(ends_with_newline(text))
    print(f"{path}: {len(text)} bytes")


def write_program(output: str, program: Program) -> None:
    """Write the SQL to `output`, and any UDFs to siblings named after it."""
    write(output, program.sql)

    base = os.path.splitext(output)[0]
    for text, extension, what in (
        (program.udf_rust, ".rs", "Rust"),
        (program.udf_toml, ".toml", "Cargo dependencies"),
    ):
        if not text:
            continue
        path = base + extension
        # A base name that already carries the extension would overwrite the SQL.
        if os.path.abspath(path) == os.path.abspath(output):
            path = f"{base}_udf{extension}"
        write(path, text)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("bundle", help="support bundle .zip")
    parser.add_argument(
        "-o",
        "--output",
        metavar="FILE",
        help="write the SQL to FILE, and any UDFs to FILE's name with .rs and .toml",
    )
    args = parser.parse_args()

    try:
        program = read_program(args.bundle)
    except (OSError, zipfile.BadZipFile, ValueError, LookupError) as error:
        print(f"{parser.prog}: {args.bundle}: {error}", file=sys.stderr)
        return 1

    if args.output:
        try:
            write_program(args.output, program)
        except OSError as error:
            print(f"{parser.prog}: {error}", file=sys.stderr)
            return 1
        return 0

    ignored = [
        f"{len(text)} bytes of {what}"
        for text, what in (
            (program.udf_rust, "Rust"),
            (program.udf_toml, "Cargo dependencies"),
        )
        if text
    ]
    if ignored:
        print(
            f"{parser.prog}: {args.bundle}: ignoring {' and '.join(ignored)}"
            "; use -o to write them",
            file=sys.stderr,
        )
    sys.stdout.write(ends_with_newline(program.sql))
    return 0


if __name__ == "__main__":
    sys.exit(main())
