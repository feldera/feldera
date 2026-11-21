#!/usr/bin/env python3
"""Audit a completed Gen-2 JUnit run without excluding failures or skips."""

import argparse
import hashlib
import json
import re
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

COUNT_FIELDS = ("tests", "failures", "errors", "skipped")


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_reports(directory):
    counts = Counter({field: 0 for field in COUNT_FIELDS})
    tests = {}
    artifacts = {}
    reports = sorted(directory.glob("TEST-*.xml"))
    if not reports:
        raise ValueError("No JUnit reports")
    for report in reports:
        suite = ET.parse(report).getroot()
        if suite.tag != "testsuite":
            raise ValueError(f"Expected a JUnit testsuite: {report.name}")
        declared = {field: int(suite.attrib[field]) for field in COUNT_FIELDS}
        actual = Counter({field: 0 for field in COUNT_FIELDS})
        for test in suite.findall("testcase"):
            identity = (test.attrib["classname"], test.attrib["name"])
            if not all(identity) or identity in tests:
                raise ValueError(f"Missing or duplicate test identity: {identity}")
            outcomes = [
                child for child in test if child.tag in ("failure", "error", "skipped")
            ]
            if len(outcomes) > 1:
                raise ValueError(f"Conflicting test outcomes: {identity}")
            actual["tests"] += 1
            status = outcomes[0].tag if outcomes else "pass"
            if outcomes:
                actual[
                    {"failure": "failures", "error": "errors", "skipped": "skipped"}[
                        status
                    ]
                ] += 1
            message = outcomes[0].attrib.get("message", "") if outcomes else ""
            tests[identity] = {
                "class": identity[0],
                "test": identity[1],
                "status": status,
                "exception": outcomes[0].attrib.get("type", "") if outcomes else "",
                "message": message.splitlines()[0] if message else "",
                "report": report.name,
            }
        if dict(actual) != declared:
            raise ValueError(
                f"JUnit case counts disagree with suite totals: {report.name}"
            )
        counts.update(actual)
        artifacts[report.name] = digest(report)
    if not counts["tests"]:
        raise ValueError("Empty JUnit run")
    return dict(counts), tests, artifacts


def failure_group(test):
    # Remove only generated operator identifiers, not SQL names or types.
    message = re.sub(r"(?<=node ')s\d+(?=')", "<id>", test["message"])
    message = re.sub(r"(?<=operator `)s\d+(?=`)", "<id>", message)
    if (
        "is not yet supported by the loader" in message
        or "unknown class `DBSP" in message
    ):
        category = "unsupported_engine_operator"
    elif "unsupported column type `" in message:
        category = "unsupported_engine_type"
    elif "Gen-2 output differs" in message:
        category = "runtime_value_mismatch"
    elif "the Gen-2 pipeline did not fail" in message:
        category = "missing_expected_runtime_error"
    elif "Gen-2 pipeline did not load" in message:
        category = "engine_initialization_error"
    elif "Gen-2 step " in message and " failed:" in message:
        category = "engine_step_error"
    elif "InternalCompilerError" in test["exception"]:
        category = "compiler_internal_error"
    else:
        category = "requires_individual_review"
    return category, message


def read_completed_run(run):
    completion = json.loads((run / "process-result.json").read_text())
    if type(completion.get("returncode")) is not int or not completion.get(
        "finished_utc"
    ):
        raise ValueError("Missing process completion evidence")
    counts, tests, reports = read_reports(run / "reports")
    log = (run / "maven.log").read_text()
    totals = re.findall(
        r"Tests run: (\d+), Failures: (\d+), Errors: (\d+), Skipped: (\d+)", log
    )
    if not totals or dict(zip(COUNT_FIELDS, map(int, totals[-1]))) != counts:
        raise ValueError("Maven final totals disagree with the saved reports")
    if (completion["returncode"] == 0) != (counts["failures"] + counts["errors"] == 0):
        raise ValueError("Process exit status disagrees with test outcomes")
    return counts, tests, reports, completion


def summarize(run, baseline=None):
    counts, tests, reports, completion = read_completed_run(run)
    passed = counts["tests"] - sum(counts[field] for field in COUNT_FIELDS[1:])
    executed = counts["tests"] - counts["skipped"]
    failures = [
        test for test in tests.values() if test["status"] in ("failure", "error")
    ]
    groups = Counter(failure_group(test) for test in failures)
    result = {
        "schema_version": 1,
        "counts": counts,
        "passes": passed,
        "pass_share_all_tests": passed / counts["tests"],
        "pass_share_excluding_skips": passed / executed if executed else None,
        "threshold": "80%",
        "threshold_met_all_tests": passed * 5 >= counts["tests"] * 4,
        "threshold_met_excluding_skips": executed > 0 and passed * 5 >= executed * 4,
        "failure_groups": [
            {"category": category, "message": message, "count": count}
            for (category, message), count in groups.most_common()
        ],
        "failures": failures,
        "skips": [test for test in tests.values() if test["status"] == "skipped"],
        "provenance": json.loads((run / "provenance.json").read_text()),
        "completion": completion,
        "reports_sha256": reports,
        "log_sha256": digest(run / "maven.log"),
        "classifier_sha256": digest(Path(__file__)),
        "classification_is_disposition": False,
        "approved_dispositions": [],
        "independent_review": False,
    }
    if baseline is not None:
        baseline_counts, old, old_reports = read_reports(baseline)
        result["baseline_comparison"] = {
            "counts": baseline_counts,
            "reports_sha256": old_reports,
            "added_tests": [
                test for identity, test in tests.items() if identity not in old
            ],
            "removed_tests": [
                test for identity, test in old.items() if identity not in tests
            ],
            "changed_outcomes": [
                {
                    "class": identity[0],
                    "test": identity[1],
                    "before": old[identity]["status"],
                    "after": test["status"],
                }
                for identity, test in tests.items()
                if identity in old and test["status"] != old[identity]["status"]
            ],
        }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--baseline-reports", type=Path)
    args = parser.parse_args()
    result = summarize(args.run, args.baseline_reports)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "counts",
                    "passes",
                    "pass_share_all_tests",
                    "pass_share_excluding_skips",
                    "threshold_met_all_tests",
                    "threshold_met_excluding_skips",
                )
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
