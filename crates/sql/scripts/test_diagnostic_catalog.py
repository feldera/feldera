import unittest

from diagnostic_catalog import Catalog, build, digest, only_body_occurrences, source_signature


def fixture():
    template = {"id": "test-root", "family": "report", "error_type": "Invalid",
                "parts": [{"literal": "Invalid value "}, {"parameter": True}],
                "sites": [{"source": "Example.java", "parts": [{"literal": "Invalid value "}, {"dynamic": "value"}]}]}
    message = {"error_type": "Invalid", "message": "Invalid value bad", "snippet": ""}
    candidate = {**message, "source_candidates": [{"id": "test-root"}], "calcite_candidates": []}
    raw = [{"source_test": "Example#test", "call_site": "Example:1", "expected": "Invalid value bad",
            "is_regex": False, "rendered": "Invalid value bad", "visible_diagnostics": [0],
            "diagnostics": [message]}]
    attribution = {"java": [{"record_index": 0, **{k: raw[0][k] for k in ("source_test", "call_site", "expected", "is_regex")},
                             "message_candidates": [{**candidate, "diagnostic_index": 0}]}]}
    review = {"scope": "test", "counting_rule": "test", "approved_source_ids": ["test-root"],
              "approved_calcite_keys": ["Shared"], "source_signatures": {"test-root": source_signature(template)},
              "resource_digests": {"Shared": digest("Shared {0}")}, "additional_roots": [], "uncounted_populations": []}
    return [raw, attribution, {"rows": []}, {"templates": [template], "java": [candidate]},
            {"Shared": "Shared {0}"}, review, {"reviewed_bodies": [], "proposed_templates": []},
            {"bodies": [], "ambiguous_bodies": []}]


class CatalogTests(unittest.TestCase):
    def test_root_has_assertion_witness(self):
        report = build(*fixture())
        self.assertEqual(report["counts"]["covered_body_roots"], 1)
        self.assertEqual(report["counts"]["roots_with_mandatory_assertion_witness"], 1)
        self.assertEqual(report["roots"][0]["mandatory_witnesses"], [0])

    def test_context_alternative_is_not_a_body_witness(self):
        args = fixture()
        args[0][0]["rendered"] += "\nSQL: Invalid value bad"
        report = build(*args)
        self.assertEqual(report["counts"]["roots_with_mandatory_assertion_witness"], 0)

    def test_regex_needs_exact_review(self):
        raw = fixture()[0][0]
        raw["is_regex"] = True
        self.assertFalse(only_body_occurrences(raw))
        self.assertTrue(only_body_occurrences(raw, "Invalid value"))
        raw["rendered"] += "\nSQL: Invalid value"
        self.assertFalse(only_body_occurrences(raw, "Invalid value"))

    def test_source_and_resource_changes_fail_closed(self):
        args = fixture()
        args[3]["templates"][0]["sites"][0]["source"] = "Changed.java"
        with self.assertRaisesRegex(ValueError, "changed after review"):
            build(*args)
        args = fixture()
        catalog = Catalog(args[3], {"Shared": "changed"}, args[5], args[6], args[7])
        with self.assertRaisesRegex(ValueError, "changed after review"):
            catalog.calcite_root("Shared")

    def test_unreviewed_root_fails_closed(self):
        args = fixture()
        args[5]["approved_source_ids"] = []
        with self.assertRaisesRegex(ValueError, "lacks recorded review"):
            build(*args)

    def test_resource_used_as_dependency_is_one_node(self):
        args = fixture()
        catalog = Catalog(args[3], args[4], args[5], args[6], args[7])
        identity = catalog.calcite_root("Shared")
        wrapper = catalog.reviewed_root("wrapper", {"format": "rewritten"}, ["calcite:Shared"])
        self.assertEqual(len(catalog.roots), 2)
        self.assertEqual(catalog.roots[wrapper]["dependencies"], [identity])
        self.assertNotIn("formatter:calcite:Shared", catalog.dependencies)

    def test_wrong_capture_or_cross_field_provenance_fails(self):
        args = fixture()
        args[1]["java"][0]["message_candidates"][0]["message"] = "different"
        with self.assertRaisesRegex(ValueError, "differs from captured"):
            build(*args)
        args = fixture()
        args[2]["rows"] = [{"record_index": 1}]
        with self.assertRaisesRegex(ValueError, "indices"):
            build(*args)


if __name__ == "__main__":
    unittest.main()
