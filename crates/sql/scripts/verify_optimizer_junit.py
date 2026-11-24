#!/usr/bin/env python3
"""Verify that Surefire reported every optimizer suite and selected method."""
import argparse
from pathlib import Path
import xml.etree.ElementTree as ET


def verify(directory, selectors, suffix):
    selected = {}
    for selector in selectors:
        suite, separator, method = selector.partition("#")
        methods = selected.setdefault(suite, set())
        if separator:
            methods.add(method)

    total = 0
    for suite, methods in selected.items():
        filename = f"TEST-*.{suite}{'-' + suffix if suffix else ''}.xml"
        reports = list(directory.glob(filename))
        if len(reports) != 1:
            raise ValueError(f"Expected one report for {suite}, found {len(reports)} in {directory}")
        report = ET.parse(reports[0]).getroot()
        cases = report.findall("testcase")
        names = {case.attrib["name"] for case in cases}
        if not cases or len(cases) != int(report.attrib["tests"]):
            raise ValueError(f"Incomplete test results in {reports[0]}")
        if any(int(report.attrib[key]) for key in ("errors", "failures", "skipped")):
            raise ValueError(f"Failed or skipped optimizer tests in {reports[0]}")
        if methods and methods != names:
            raise ValueError(f"{suite} ran {sorted(names)}, expected {sorted(methods)}")
        total += len(cases)
    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("selectors", nargs="+", help="Surefire Class or Class#method selectors")
    parser.add_argument("--suffix", default="", help="reportNameSuffix from this Maven invocation")
    args = parser.parse_args()
    print(f"Verified {verify(args.directory, args.selectors, args.suffix)} optimizer tests")
