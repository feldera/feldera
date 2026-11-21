import copy
import unittest

from diagnostic_direct_assertions import classify, summarize


class DirectDiagnosticAssertionsTests(unittest.TestCase):
    def row(self, target="message_body", comparison="contains", expected="invalid", actual="invalid value"):
        return dict(schema_version=1, source_test="Test#method", call_site="Test:1",
                    target=target, comparison=comparison, expected=expected, actual=actual)

    def test_body_class_and_rendering_have_distinct_requirements(self):
        self.assertEqual(classify(self.row()), "message_body_requirement")
        self.assertEqual(classify(self.row(target="error_class")), "non_body_contract")
        self.assertEqual(classify(self.row(target="rendered")), "rendered_text_requires_attribution")
        self.assertEqual(classify(self.row(target="config_exception")), "formatter_fragment_requirement")

    def test_order_and_absence_are_not_positive_template_dependencies(self):
        row = self.row("rendered_warning_lines", "ordered_list_equals", [], [])
        self.assertEqual(classify(row), "absence_contract")
        self.assertEqual(classify(self.row("rendered", "equals", "", "")), "absence_contract")
        row.update(expected=["a", "b"], actual=["b", "a"])
        with self.assertRaisesRegex(ValueError, "ordered warning assertion"):
            classify(row)
        row = self.row(comparison="not_contains", expected="warning", actual="no errors")
        self.assertEqual(classify(row), "absence_contract")
        row["actual"] = "warning"
        with self.assertRaisesRegex(ValueError, "predicate failed"):
            classify(row)

    def test_snapshot_stability_preserves_json_scalar_types(self):
        row = self.row("snapshot", "equals", [{"warning": True}], [{"warning": True}])
        self.assertEqual(classify(row), "snapshot_stability")
        row["actual"][0]["warning"] = 1
        with self.assertRaisesRegex(ValueError, "snapshot assertion"):
            classify(row)

    def test_java_regex_is_not_reinterpreted_and_unknown_contracts_fail(self):
        row = self.row("rendered", "no_regex_find", r"\Qnot present\E", "SQL error")
        self.assertEqual(classify(row), "absence_contract")
        for field, value in [("target", "unknown"), ("target", []), ("schema_version", True),
                             ("comparison", "regex"), ("extra", 1), ("actual", None)]:
            malformed = copy.deepcopy(row)
            malformed[field] = value
            with self.assertRaises(ValueError):
                classify(malformed)

    def test_failed_capture_and_changed_population_are_rejected(self):
        manifest = dict(direct_assertion_contract="typed-targets-v1", junit_successful=True, direct_assertions=1)
        result = summarize(manifest, [self.row()])
        self.assertEqual(result["counts"]["assertions"], 1)
        self.assertIsNone(result["required_template_count"])
        for field, value in [("junit_successful", False), ("direct_assertions", 0), ("direct_assertions", True)]:
            with self.assertRaises(ValueError):
                summarize({**manifest, field: value}, [self.row()])
