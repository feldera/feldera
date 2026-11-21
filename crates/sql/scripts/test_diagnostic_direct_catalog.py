import unittest

from diagnostic_direct_catalog import integrate, record_digest


def fixture(target="message_body", actual="bad value", expected="bad value"):
    record = {"schema_version": 1, "source_test": "Example#test", "call_site": "Example:1",
              "target": target, "comparison": "equals", "actual": actual, "expected": expected}
    root = {"id": "source:existing", "kind": "source_expression", "dependencies": [],
            "mandatory_witnesses": [0]}
    shared = {"roots": [root], "dependencies": [], "assertions": [{}], "uncounted_populations": []}
    binding = {"record_index": 0, "source_test": record["source_test"], "call_site": record["call_site"],
               "record_sha256": record_digest(record), "contract": "message_body_requirement",
               "body_roots": [root["id"]], "fragment_instances": [],
               "body_evidence": [{"message": actual, "root": root["id"]}]}
    review = {"bindings": [binding], "additional_body_roots": [], "additional_dependencies": [],
              "fragment_templates": [], "counting_rule": "test"}
    return shared, [record], review


class DirectCatalogTests(unittest.TestCase):
    def test_direct_witness_reuses_existing_root(self):
        report = integrate(*fixture())
        self.assertEqual(report["counts"]["covered_required_body_roots"], 1)
        self.assertEqual(report["counts"]["additional_direct_body_roots"], 0)
        self.assertEqual(report["roots"][0]["direct_mandatory_witnesses"], [0])

    def test_changed_capture_fails(self):
        shared, records, review = fixture()
        records[0]["source_test"] = "Other#test"
        with self.assertRaisesRegex(ValueError, "provenance"):
            integrate(shared, records, review)

    def test_snapshot_does_not_add_positive_root(self):
        shared, records, review = fixture("snapshot", [{"message": "variable"}], [{"message": "variable"}])
        binding = review["bindings"][0]
        binding.update(contract="snapshot_stability", body_roots=[], body_evidence=[])
        report = integrate(shared, records, review)
        self.assertEqual(report["counts"]["direct_body_assertions"], 0)
        binding["body_roots"] = ["source:existing"]
        with self.assertRaisesRegex(ValueError, "non-body contract"):
            integrate(shared, records, review)

    def test_fragment_reconstructs_captured_target(self):
        shared, records, review = fixture("config_exception")
        binding = review["bindings"][0]
        binding.update(contract="formatter_fragment_requirement", body_roots=[], body_evidence=[],
                       fragment_instances=[{"template": "fragment:test", "arguments": {"value": "value"},
                                            "message": "bad value"}])
        review["fragment_templates"] = [{"id": "fragment:test", "format": "bad {value}"}]
        report = integrate(shared, records, review)
        self.assertEqual(report["counts"]["direct_fragment_templates"], 1)
        binding["fragment_instances"][0]["arguments"]["value"] = "changed"
        with self.assertRaisesRegex(ValueError, "reconstruction"):
            integrate(shared, records, review)

    def test_missing_or_uncaptured_evidence_fails(self):
        shared, records, review = fixture()
        review["bindings"][0]["body_evidence"][0]["message"] = "not present"
        with self.assertRaisesRegex(ValueError, "does not occur"):
            integrate(shared, records, review)
        shared, records, review = fixture()
        review["additional_body_roots"] = [{"id": "source:unwitnessed", "kind": "source_expression",
                                             "dependencies": [], "mandatory_witnesses": []}]
        with self.assertRaisesRegex(ValueError, "lacks an assertion witness"):
            integrate(shared, records, review)

    def test_shared_dependency_conflict_fails(self):
        shared, records, review = fixture()
        shared["dependencies"] = [{"id": "formatter:shared", "description": "original"}]
        review["additional_dependencies"] = [{"id": "formatter:shared", "description": "changed"}]
        with self.assertRaisesRegex(ValueError, "conflicts"):
            integrate(shared, records, review)


if __name__ == "__main__":
    unittest.main()
