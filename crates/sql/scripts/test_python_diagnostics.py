import copy
import unittest

from python_diagnostics import summarize


class PythonDiagnosticResultsTests(unittest.TestCase):
    def setUp(self):
        self.inputs = {"schema_version": 1, "complete": True, "records": [
            {"id": "compile", "active": True, "expected": "not found"},
            {"id": "runtime", "active": True, "expected": "overflow"},
            {"id": "disabled", "active": False, "expected": "unused"},
        ]}
        self.results = [
            {"id": "compile", "compiler_rejected": True, "circuit_produced": False,
             "rendered": "Table NOT FOUND", "expected_text_found": True, "diagnostics": [], "runtime_executed": False},
            {"id": "runtime", "compiler_rejected": False, "circuit_produced": True,
             "rendered": "", "expected_text_found": False, "diagnostics": [], "runtime_executed": False},
            {"id": "disabled", "disposition": "not_registered", "runtime_executed": False},
        ]

    def test_accepted_program_is_pending_runtime_not_a_passing_negative_test(self):
        report = summarize(self.inputs, self.results)
        self.assertEqual(report["counts"]["compiler_rejected_with_expected_text"], 1)
        self.assertEqual(report["cases"][1]["disposition"], "requires_runtime_evaluation")
        self.assertFalse(report["python_tests_executed"])
        self.assertFalse(report["P0_complete"])

    def test_missing_results_or_false_match_flags_fail(self):
        with self.assertRaisesRegex(ValueError, "omit, reorder, or add"):
            summarize(self.inputs, self.results[:-1])
        altered = copy.deepcopy(self.results)
        altered[0]["expected_text_found"] = False
        with self.assertRaisesRegex(ValueError, "match flag"):
            summarize(self.inputs, altered)

    def test_unexpected_internal_failure_stays_a_failed_measurement(self):
        altered = copy.deepcopy(self.results)
        altered[0].update(rendered="unexpected exception", expected_text_found=False,
                          diagnostics=[{"error_type": "This is a bug in the compiler (please report it to the developers)"}])
        report = summarize(self.inputs, altered)
        self.assertEqual(report["compiler_expectation_mismatches"], ["compile"])
        self.assertEqual(report["internal_compiler_failures"], ["compile"])


if __name__ == "__main__":
    unittest.main()
