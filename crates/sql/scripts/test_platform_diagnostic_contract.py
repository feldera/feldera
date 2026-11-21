import tempfile
import unittest
from pathlib import Path

from diagnostic_census import platform_expectation
from platform_diagnostic_contract import load_method, verify


ROOT = Path(__file__).resolve().parents[3]


def fixture():
    sql = platform_expectation(ROOT)["sql"].strip()
    diagnostic = {"start_line_number": 5, "start_column": 32,
                  "end_line_number": 5, "end_column": 35, "warning": False,
                  "error_type": "Compilation error", "message": "Object 'blah' not found",
                  "snippet": "    4|);\n    5|CREATE VIEW s AS SELECT * FROM blah;\n                                     ^^^^\n"}
    return {"records": [{"sql": sql}]}, {"compiler_rejected": True, "circuit_produced": False,
                                             "diagnostics": [diagnostic], "options": {}}


class PlatformContractTests(unittest.TestCase):
    def test_actual_sdk_format_and_response_protocol(self):
        report = verify(ROOT, *fixture())
        self.assertTrue(report["matched"])
        self.assertEqual([r["selector"] for r in report["sdk_replay"]["response_requests"]], ["STATUS", "ALL"])
        self.assertFalse(report["full_platform_test_executed"])
        self.assertEqual(report["additional_body_roots"], 0)

    def test_changed_body_class_or_carets_fail_exact_contract(self):
        for field in ("message", "error_type", "snippet"):
            inputs, result = fixture()
            result["diagnostics"][0][field] += "changed"
            with self.assertRaisesRegex(ValueError, "exact platform"):
                verify(ROOT, inputs, result)

    def test_sql_normalization_must_match_compiler_input(self):
        inputs, result = fixture()
        inputs["records"][0]["sql"] = "\n" + inputs["records"][0]["sql"]
        with self.assertRaisesRegex(ValueError, "SDK normalization"):
            verify(ROOT, inputs, result)

    def test_missing_sdk_method_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sdk.py"
            path.write_text("class Pipeline:\n    pass\n")
            with self.assertRaisesRegex(ValueError, "missing or ambiguous"):
                load_method(path, "Pipeline", "__init__", {})


if __name__ == "__main__":
    unittest.main()
