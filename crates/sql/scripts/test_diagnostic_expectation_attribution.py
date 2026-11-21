import copy
import unittest

from diagnostic_expectation_attribution import attribute, field_matches, java_field_matches


class DiagnosticExpectationAttributionTests(unittest.TestCase):
    def fixture(self):
        messages = [
            {"error_type": "Incidental warning", "message": "This warning is not asserted", "snippet": "    1|SELECT bad;\n             ^^^"},
            {"error_type": "Compilation error", "message": "Object 'bad' not found", "snippet": "    1|SELECT bad;\n             ^^^"},
        ]
        raw = [{"source_test": "Test#negative", "call_site": "Test:1", "expected": "Object 'bad' not found", "is_regex": False, "diagnostics": messages}]
        captured = [{key: row[key] for key in ("source_test", "call_site", "expected", "is_regex")} | {
            "messages": [{"error_type": message["error_type"], "message": message["message"], "calcite_candidates": []} for message in messages],
        } for row in raw]
        census = {"schema_version": 1, "java": captured, "platform": [{"comparison": "exact_after_strip", "executed": False}]}
        templates = {
            "schema_version": 1,
            "java": [{**message, "calcite_candidates": [], "source_candidates": [{"id": str(i), "match": "full_expression"}]} for i, message in enumerate(messages)],
            "templates": [{"id": "1", "family": "report", "error_type": "Compilation error", "parts": [{"literal": "Object '"}, {"parameter": True}, {"literal": "' not found"}]}],
        }
        inputs = {"schema_version": 1, "complete": True, "records": [
            {"id": "compiler", "expected": "OBJECT 'bad'", "active": True},
            {"id": "runtime", "expected": "divide by zero", "active": True},
            {"id": "disabled", "expected": "unused", "active": False},
        ]}
        results = [
            {"id": "compiler", "compiler_rejected": True, "circuit_produced": False, "runtime_executed": False, "rendered": "Object 'bad' not found", "expected_text_found": True, "diagnostics": [messages[1]]},
            {"id": "runtime", "compiler_rejected": False, "circuit_produced": True, "runtime_executed": False, "rendered": "", "expected_text_found": False, "diagnostics": []},
            {"id": "disabled", "disposition": "not_registered", "runtime_executed": False},
        ]
        return raw, census, templates, inputs, results, {"objectNotFound": "Object ''{0}'' not found"}

    def test_incidental_messages_and_runtime_pending_cases_do_not_enter_count(self):
        report = attribute(*self.fixture())
        self.assertEqual(report["java"][0]["message_candidates"][0]["diagnostic_index"], 1)
        self.assertEqual(report["counts"]["source_candidates_for_asserted_bodies"], 1)
        self.assertEqual(report["counts"]["distinct_candidate_bodies"], 1)
        self.assertEqual([row["disposition"] for row in report["python"]], ["message_body_candidates", "runtime_evaluation_pending", "not_registered"])
        self.assertIsNone(report["required_template_count"])
        self.assertEqual(report["platform"][0]["disposition"], "exact_composite_assertion_requires_renderer_and_message_attribution")

    def test_snippets_and_java_regex_are_not_message_template_dependencies(self):
        for expected, regex, disposition in [
            ("    1|SELECT bad;\n             ^^^", False, "non_body_fields_only"),
            ("Object.*not found", True, "regex_requires_java_rendered_attribution"),
        ]:
            raw, census, *rest = self.fixture()
            for row in (raw[0], census["java"][0]):
                row["expected"], row["is_regex"] = expected, regex
            report = attribute(raw, census, *rest)
            self.assertEqual(report["java"][0]["disposition"], disposition)
            self.assertEqual(report["java"][0]["message_candidates"], [])

    def test_source_population_drift_and_blank_fragments_are_rejected(self):
        fixture = list(self.fixture())
        fixture[1] = copy.deepcopy(fixture[1])
        fixture[1]["java"][0]["messages"][0]["message"] = "changed"
        with self.assertRaisesRegex(ValueError, "messages differ"):
            attribute(*fixture)
        with self.assertRaisesRegex(ValueError, "empty normalized expectation"):
            field_matches("  ", [], ignore_case=True)

    def test_java_regex_matches_require_captured_rendering_and_valid_field_references(self):
        raw, census, *rest = self.fixture()
        for row in (raw[0], census["java"][0]):
            row["expected"], row["is_regex"] = r"\QObject 'bad' not found\E", True
        for row in (raw[0], census["java"][0]):
            row.update(assertion_kind="compilation_error", rendered="Object 'bad' not found",
                       visible_diagnostics=[0, 1],
                       field_matches=[{"diagnostic_index": 1, "fields": ["message"]}])
        report = attribute(raw, census, *rest)
        self.assertEqual(report["java"][0]["disposition"], "message_body_candidates")
        self.assertEqual(report["counts"]["java_field_attributions_captured"], 1)
        for changes in [
            {"assertion_kind": "unknown"},
            {"rendered": None},
            {"field_matches": [{"diagnostic_index": 999, "fields": ["message"]}]},
            {"field_matches": [{"diagnostic_index": 1, "fields": ["unknown"]}]},
            {"assertion_kind": "warnings"},
            {"visible_diagnostics": [0]},
            {"visible_diagnostics": [True]},
            {"visible_diagnostics": [1, 1]},
        ]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                java_field_matches(raw[0] | changes)

    def test_captured_literals_are_recomputed_and_partial_metadata_cannot_downgrade(self):
        raw, census, *rest = self.fixture()
        row = raw[0] | {"assertion_kind": "compilation_error", "rendered": "Object 'bad' not found",
                        "visible_diagnostics": [0, 1],
                        "field_matches": [{"diagnostic_index": 1, "fields": ["message"]}]}
        self.assertEqual(java_field_matches(row), row["field_matches"])
        for field in ("assertion_kind", "rendered", "visible_diagnostics", "field_matches"):
            partial = row.copy()
            del partial[field]
            with self.assertRaisesRegex(ValueError, "incomplete"):
                java_field_matches(partial)
        for change in [
            {"rendered": "unrelated rendering"},
            {"field_matches": [{"diagnostic_index": 0, "fields": ["message"]}]},
            {"field_matches": []},
        ]:
            with self.subTest(change=change), self.assertRaises(ValueError):
                java_field_matches(row | change)
        raw[0] = row
        with self.assertRaisesRegex(ValueError, "metadata differs"):
            attribute(raw, census, *rest)

    def test_suppressed_warning_is_not_a_candidate_even_if_its_body_matches(self):
        raw = self.fixture()[0][0]
        raw["diagnostics"][0].update(message=raw["expected"], warning=True)
        raw.update(assertion_kind="compilation_error", rendered=raw["expected"], visible_diagnostics=[1],
                   field_matches=[{"diagnostic_index": 1, "fields": ["message"]}])
        self.assertEqual(java_field_matches(raw), raw["field_matches"])


if __name__ == "__main__":
    unittest.main()
