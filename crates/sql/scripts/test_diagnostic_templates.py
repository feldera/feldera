import tempfile
import unittest
from pathlib import Path

from diagnostic_templates import cluster, normalized_parts


class DiagnosticTemplateTests(unittest.TestCase):
    def test_normalization_preserves_literal_punctuation_and_parameter_order(self):
        self.assertEqual(normalized_parts([
            {"literal": "Column "}, {"literal": "'"}, {"dynamic": "name"},
            {"dynamic": "suffix"}, {"literal": "' missing."},
        ]), [{"literal": "Column '"}, {"parameter": True}, {"literal": "' missing."}])
        with self.assertRaises(ValueError):
            normalized_parts([{"regex": ".*"}])

    def test_overloads_ambiguity_and_unmatched_messages_remain_visible(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "Compiler.java").write_text("source fixture")
            site = {"source": "Compiler.java", "line": 1, "offset": 0,
                    "kind": "report", "callee": "reportError", "expression": "fixture",
                    "parts": [{"literal": "Missing '"}, {"dynamic": "name"}, {"literal": "'"}]}
            constructor = dict(site, kind="constructor", callee="UnimplementedException")
            dynamic = dict(site, parts=[{"dynamic": "exception.getMessage()"}])
            extraction = {"schema_version": 1, "source_files": 1, "sites": [site, constructor, dynamic]}
            messages = [{"error_type": "Missing", "message": message, "calcite_candidates": []}
                        for message in ["Missing 'x'", "Missing 'x'\nissue 123", "Unrelated error"]]
            census = {"schema_version": 1, "java": [{"call_site": "Test:1", "messages": messages}],
                      "python": [{"expected": "missing 'x'", "calcite_candidates": []}], "platform": []}
            report = cluster(extraction, census, root)
            self.assertEqual(report["counts"]["dynamic_only_sites"], 1)
            self.assertEqual(report["counts"]["java_diagnostics_with_ambiguous_source_candidates"], 1)
            self.assertEqual(report["counts"]["java_diagnostics_without_any_candidate"], 1)
            extended = next(row for row in report["java"] if "issue" in row["message"])
            self.assertEqual([row["match"] for row in extended["source_candidates"]], ["constructor_prefix"])
            self.assertIsNone(report["required_template_count"])
            self.assertFalse(report["catalog_reviewed"])
            self.assertFalse(report["python"][0]["executed"])

    def test_known_classes_and_empty_literals_are_not_wildcards(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "Compiler.java").write_text("source fixture")
            site = {"source": "Compiler.java", "kind": "report", "callee": "reportError",
                    "error_type": "Previous definition", "parts": [{"literal": ""}]}
            weak = dict(site, parts=[{"dynamic": "message"}, {"literal": ".\n"}, {"dynamic": "citation"}])
            extraction = {"schema_version": 1, "source_files": 1, "sites": [site, weak]}
            census = {"schema_version": 1, "java": [{"call_site": "Test:1", "messages": [
                {"error_type": title, "message": "", "calcite_candidates": []}
                for title in ["Previous definition", "Wrong class"]]}], "python": [], "platform": []}
            report = cluster(extraction, census, root)
            self.assertEqual(report["counts"]["punctuation_only_sites"], 1)
            self.assertEqual(report["counts"]["java_diagnostics_with_source_candidates"], 1)
            self.assertTrue(report["java"][0]["source_candidates"])
            self.assertFalse(report["java"][1]["source_candidates"])


if __name__ == "__main__":
    unittest.main()
