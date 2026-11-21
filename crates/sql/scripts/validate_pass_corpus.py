"""Verify population coverage and count matching Java and Rust optimizer invocations."""
import argparse
import json
from pathlib import Path

from validate_corpus_tail import records, verify


def summarize(population, results, pass_name, expect_absent=False):
    summary = verify(population, results)
    invocations = changed = absent = 0
    for path in results:
        for row in records(path):
            details = row["details"]
            if details["pass"] != pass_name:
                raise ValueError(f"Wrong pass for {row['id']}")
            count = details["invocations"]
            edits = details["changed"]
            if type(count) is not int or type(edits) is not int or not 0 <= edits <= count:
                raise ValueError(f"Invalid pass counts for {row['id']}")
            invocations += count
            changed += edits
            absent += count == 0
    if expect_absent and invocations:
        raise ValueError(f"The compiler unexpectedly invoked {pass_name} {invocations} times")
    if not expect_absent and not invocations:
        raise ValueError(f"The compiler never invoked {pass_name}")
    return {**summary, "pass": pass_name, "invocations": invocations, "changed": changed,
            "cases_without_placement": absent}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expect-absent", action="store_true", help="verify an engine disables this pass in every case")
    parser.add_argument("pass_name")
    parser.add_argument("population", type=Path)
    parser.add_argument("results", type=Path, nargs="+")
    args = parser.parse_args()
    print(json.dumps(summarize(args.population, args.results, args.pass_name, args.expect_absent), indent=2))
