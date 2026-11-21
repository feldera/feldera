import json
import tempfile
import unittest
from pathlib import Path

from summarize_gen2 import read_reports, summarize


class Gen2SummaryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.reports = self.root / "reports"
        self.reports.mkdir()
        self.report = self.reports / "TEST-example.xml"
        self.report.write_text("""<testsuite tests="3" failures="1" errors="0" skipped="1">
          <testcase classname="C" name="pass"/>
          <testcase classname="C" name="fail"><failure message="different value"/></testcase>
          <testcase classname="C" name="skip"><skipped message="not supported"/></testcase>
        </testsuite>""")
        (self.root / "process-result.json").write_text(
            json.dumps({"returncode": 1, "finished_utc": "time"})
        )
        (self.root / "provenance.json").write_text("{}")
        (self.root / "maven.log").write_text(
            "Tests run: 3, Failures: 1, Errors: 0, Skipped: 1\n"
        )

    def test_denominators_keep_failures_and_make_skip_policy_explicit(self):
        result = summarize(self.root)
        self.assertEqual(result["passes"], 1)
        self.assertEqual(result["pass_share_all_tests"], 1 / 3)
        self.assertEqual(result["pass_share_excluding_skips"], 1 / 2)
        self.assertFalse(result["threshold_met_all_tests"])
        self.assertFalse(result["threshold_met_excluding_skips"])
        self.assertEqual(len(result["failures"]), 1)
        self.assertEqual(len(result["skips"]), 1)
        self.assertEqual(result["approved_dispositions"], [])

    def test_incomplete_or_duplicated_reports_cannot_pass(self):
        original = self.report.read_text()
        self.report.write_text(original.replace('tests="3"', 'tests="4"'))
        with self.assertRaisesRegex(ValueError, "totals"):
            read_reports(self.reports)
        self.report.write_text(original.replace('name="fail"', 'name="pass"'))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            read_reports(self.reports)

    def test_process_failure_or_missing_completion_cannot_be_hidden(self):
        (self.root / "process-result.json").write_text(
            '{"returncode":0,"finished_utc":"time"}'
        )
        with self.assertRaisesRegex(ValueError, "exit status"):
            summarize(self.root)
        (self.root / "process-result.json").write_text('{"returncode":1}')
        with self.assertRaisesRegex(ValueError, "completion"):
            summarize(self.root)

    def test_baseline_distinguishes_added_failures_from_regressions(self):
        baseline = self.root / "baseline"
        baseline.mkdir()
        (
            baseline / "TEST-example.xml"
        ).write_text("""<testsuite tests="2" failures="0" errors="0" skipped="0">
          <testcase classname="C" name="fail"/><testcase classname="C" name="removed"/>
        </testsuite>""")
        result = summarize(self.root, baseline)["baseline_comparison"]
        self.assertEqual(
            result["changed_outcomes"],
            [{"class": "C", "test": "fail", "before": "pass", "after": "failure"}],
        )
        self.assertEqual(
            {test["test"] for test in result["added_tests"]}, {"pass", "skip"}
        )
        self.assertEqual(
            [test["test"] for test in result["removed_tests"]], ["removed"]
        )
