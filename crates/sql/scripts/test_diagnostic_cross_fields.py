import unittest

from diagnostic_cross_fields import resolve


class DiagnosticCrossFieldTests(unittest.TestCase):
    def row(self, expected, rendered, bodies, regex=False):
        return {"source_test": "Test#method", "call_site": "Test:1", "expected": expected,
                "is_regex": regex, "assertion_kind": "compilation_error", "rendered": rendered,
                "visible_diagnostics": list(range(len(bodies))), "field_matches": [],
                "diagnostics": [{"message": message, "error_type": "Error", "snippet": "", "warning": False} for message in bodies]}

    def test_class_and_body_boundary_selects_only_asserted_body(self):
        row = self.row("Error: Missing", "Error: Incidental\nError: Missing table\n", ["Incidental", "Missing table"])
        result = resolve(row, [])
        self.assertEqual(result["disposition"], "body_fields_resolved")
        self.assertEqual([m["diagnostic_index"] for m in result["body_matches"]], [1])
        self.assertEqual(result["body_matches"][0]["asserted_body_fragments"], ["Missing"])

    def test_duplicate_body_or_assertion_occurrences_remain_ambiguous(self):
        row = self.row("Error: Same", "context Same\nError: Same", ["Same"])
        self.assertEqual(resolve(row, [])["disposition"], "body_occurrence_ambiguous")
        row = self.row("Error: Same", "Error: Same\nError: Same", ["Same"])
        self.assertEqual(resolve(row, [])["disposition"], "assertion_occurrence_ambiguous")

    def test_source_context_match_does_not_assert_diagnostic_body(self):
        row = self.row("context SQL", "context SQL\nError: Bad type", ["Bad type"])
        self.assertEqual(resolve(row, [])["disposition"], "no_body_text_asserted")

    def test_repeated_identical_bodies_use_visible_order_when_counts_agree(self):
        row = self.row("2: Error: Same", "1: Error: Same\n2: Error: Same", ["Same", "Same"])
        result = resolve(row, [])
        self.assertEqual(result["disposition"], "body_fields_resolved")
        self.assertEqual([m["diagnostic_index"] for m in result["body_matches"]], [1])

    def test_java_regex_needs_exact_review_and_ordered_fragments(self):
        row = self.row("first.*second", "Error: first\nError: second", ["first", "second"], True)
        self.assertEqual(resolve(row, [])["disposition"], "regex_review_required")
        review = {"source_test": row["source_test"], "expected": row["expected"],
                  "asserted_literal_fragments": ["first", "second"]}
        self.assertEqual(len(resolve(row, [review])["body_matches"]), 2)
        review["asserted_literal_fragments"].reverse()
        with self.assertRaisesRegex(ValueError, "different order"):
            resolve(row, [review])


if __name__ == "__main__":
    unittest.main()
