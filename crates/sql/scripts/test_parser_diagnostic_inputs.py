import json
import tempfile
import unittest
from pathlib import Path

from parser_diagnostic_inputs import prepare


class DiagnosticInputTests(unittest.TestCase):
    def capture(self, directory, submissions):
        rows = [
            {
                "schema_version": 1,
                "source_test": "T#negative",
                "call_site": "T:20",
                "submissions": value,
            }
            for value in submissions
        ]
        (directory / "java-diagnostics.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows)
        )
        (directory / "complete.json").write_text(
            json.dumps(
                {"schema_version": 1, "records": len(rows), "junit_successful": True}
            )
        )

    def test_original_submission_boundaries_and_missing_sources_are_retained(self):
        with tempfile.TemporaryDirectory() as name:
            directory = Path(name)
            submissions = [
                {"sql": "SELECT ';'", "many": False, "batch": 0},
                {"sql": "SELECT (", "many": True, "batch": 1},
            ]
            self.capture(directory, [submissions, [], submissions])
            cases, report = prepare(directory)
            self.assertEqual(submissions, cases[0]["submissions"])
            self.assertNotEqual(cases[0]["id"], cases[1]["id"])
            self.assertEqual(3, report["records"])
            self.assertEqual(1, len(report["skipped"]))
            self.assertFalse(report["original_compiler_diagnostics_compared"])

    def test_bad_completion_or_submission_cannot_become_parser_evidence(self):
        with tempfile.TemporaryDirectory() as name:
            directory = Path(name)
            self.capture(
                directory, [[{"sql": "SELECT (", "many": True, "batch": False}]]
            )
            with self.assertRaisesRegex(ValueError, "Malformed diagnostic SQL"):
                prepare(directory)
            (directory / "complete.json").write_text(
                json.dumps(
                    {"schema_version": 1, "records": 2, "junit_successful": True}
                )
            )
            with self.assertRaisesRegex(ValueError, "record count"):
                prepare(directory)
