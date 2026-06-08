#!/usr/bin/env python3
"""Example: translate ALL of felderize's built-in Spark SQL examples via the API.

Iterates over the bundled example programs (the same ones behind
`felderize spark example <name>`), translates each through the Python API, and
prints a per-example status plus a summary.

    .venv/bin/python examples/translate_all_examples.py
"""

from pathlib import Path

import felderize
from felderize import Config, Status, translate_spark_to_feldera

# split_combined_sql is a helper (not part of the minimal public API) used here
# only to split the "combined" example files into schema + query.
from felderize.translator import split_combined_sql

EXAMPLES_DIR = Path(felderize.__file__).resolve().parent / "spark_sql"


def load_examples() -> dict[str, tuple[str, str]]:
    """Return {name: (schema_sql, query_sql)} for every bundled example."""
    examples: dict[str, tuple[str, str]] = {}
    for schema_file in sorted(EXAMPLES_DIR.glob("*_schema.sql")):
        name = schema_file.name.replace("_schema.sql", "")
        query_file = EXAMPLES_DIR / f"{name}_query.sql"
        if query_file.is_file():
            examples[name] = (schema_file.read_text(), query_file.read_text())
    for combined_file in sorted(EXAMPLES_DIR.glob("*_combined.sql")):
        name = combined_file.name.replace("_combined.sql", "")
        examples[name] = split_combined_sql(combined_file.read_text())
    return dict(sorted(examples.items()))


def main() -> None:
    cfg = Config.from_env()
    examples = load_examples()
    print(f"Translating {len(examples)} built-in examples (validate=False)...\n")

    counts: dict[str, int] = {}
    for name, (schema_sql, query_sql) in examples.items():
        # validate=False keeps this runnable with just an API key (no compiler);
        # set validate=True to also compile + repair each result.
        result = translate_spark_to_feldera(schema_sql, query_sql, cfg, validate=False)
        counts[result.status.value] = counts.get(result.status.value, 0) + 1
        note = (
            ""
            if result.status is Status.SUCCESS
            else f"  ({len(result.unsupported)} unsupported)"
        )
        print(f"  {name:16} {result.status.value}{note}")

    print("\nsummary: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))


if __name__ == "__main__":
    main()
