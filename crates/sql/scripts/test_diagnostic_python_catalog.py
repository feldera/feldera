import unittest

from diagnostic_python_catalog import integrate, record_digest


def fixture():
    source = {"id": "test:negative", "active": True, "expected": "bad value"}
    diagnostic = {"message": "Bad value 1", "error_type": "Compilation error", "snippet": "SELECT 1"}
    outcome = {"id": source["id"], "runtime_executed": False, "compiler_rejected": True,
               "circuit_produced": False, "expected_text_found": True,
               "rendered": "Bad value 1", "diagnostics": [diagnostic]}
    root = {"id": "source:new", "kind": "source_expression"}
    base = {"roots": [], "dependencies": [], "counts": {"covered_required_body_roots": 0},
            "uncounted_populations": []}
    binding = {"id": source["id"], "input_sha256": record_digest(source),
               "result_sha256": record_digest(outcome), "body_roots": [root["id"]],
               "disposition": "message_body_candidates", "body_evidence": [
                   dict(diagnostic_index=0, message=diagnostic["message"],
                        error_type=diagnostic["error_type"], roots=[root["id"]]) ]}
    review = {"bindings": [binding], "additional_body_roots": [root],
              "additional_dependencies": [], "ambiguity_resolutions": []}
    return base, {"schema_version": 1, "complete": True, "records": [source]}, [outcome], review


class PythonCatalogTests(unittest.TestCase):
    def test_case_insensitive_body_updates_generic_count(self):
        report = integrate(*fixture())
        self.assertEqual(report["counts"]["covered_required_body_roots"], 1)
        self.assertEqual(report["counts"]["java_required_body_roots"], 0)
        self.assertEqual(report["roots"][0]["python_compiler_witnesses"], ["test:negative"])

    def test_changed_input_or_result_is_rejected(self):
        for target in (1, 2):
            args = list(fixture())
            if target == 1:
                args[1]["records"][0]["sql"] = "changed"
            else:
                args[2][0]["options"] = {"changed": True}
            with self.assertRaisesRegex(ValueError, "provenance"):
                integrate(*args)

    def test_body_snippet_alternative_is_not_counted(self):
        base, inputs, results, review = fixture()
        results[0]["diagnostics"][0]["snippet"] = "bad value"
        review["bindings"][0]["result_sha256"] = record_digest(results[0])
        with self.assertRaisesRegex(ValueError, "non-body alternative"):
            integrate(base, inputs, results, review)

    def test_invented_or_missing_body_evidence_is_rejected(self):
        for indices in ([], [1]):
            base, inputs, results, review = fixture()
            if indices:
                review["bindings"][0]["body_evidence"][0]["message"] = "incidental"
            else:
                review["bindings"][0]["body_evidence"] = []
            with self.assertRaisesRegex(ValueError, "attribution"):
                integrate(base, inputs, results, review)

    def test_runtime_is_separate_and_exact_population_required(self):
        base, inputs, results, review = fixture()
        results[0].update(compiler_rejected=False, circuit_produced=True,
                          expected_text_found=False, rendered="", diagnostics=[])
        binding = review["bindings"][0]
        binding.update(result_sha256=record_digest(results[0]), body_roots=[], body_evidence=[],
                       disposition="runtime_evaluation_pending")
        review["additional_body_roots"] = []
        runtime = {"complete": True,
                   "cases": [{"id": "test:negative", "root": "overflow", "expected": "bad value"}],
                   "error_body_roots": {"overflow": {}}, "counts": {"body_roots": 1}}
        report = integrate(base, inputs, results, review, runtime)
        self.assertEqual(report["counts"]["covered_required_body_roots"], 0)
        self.assertEqual(report["python_expectations"][0]["disposition"], "runtime_contract_accounted_separately")
        runtime["cases"] = []
        with self.assertRaisesRegex(ValueError, "runtime attribution population"):
            integrate(base, inputs, results, review, runtime)


if __name__ == "__main__":
    unittest.main()
