import copy
import json
import unittest

from run_python_runtime_diagnostics import assess, module_source


class RuntimeDiagnosticAssessmentTests(unittest.TestCase):
    def test_allocator_packaging_changes_only_the_duplicate_attribute(self):
        raw = '#[global_allocator]\nstatic ALLOC: Alloc = Alloc;\n#[export_name = "malloc_conf"]\npub static malloc_conf: &[u8] = b"config";\nfn circuit() {}\n'
        self.assertEqual(module_source(raw, 0), raw)
        self.assertEqual(module_source(raw, 1), raw.replace('#[global_allocator]\n', '').replace('#[export_name = "malloc_conf"]\n', ''))
        with self.assertRaises(ValueError):
            module_source('fn circuit() {}', 1)

    def setUp(self):
        self.case = {"id": "case", "expected": " Overflow "}
        self.observation = {"id": "case", "observation": {"stage": "table_transaction", "error": "worker: OVERFLOW"},
                            "cleanup_failed": False}

    def test_matches_only_observed_circuit_runtime_error(self):
        result = assess(self.case, 0, json.dumps(self.observation))
        self.assertTrue(result["local_runtime_contract_matched"])
        self.assertFalse(result["python_suite_executed"])
        self.assertFalse(result["platform_transport_executed"])

    def test_same_error_at_wrong_stage_or_failed_cleanup_is_not_a_match(self):
        for stage in ["construction", "record_decode", "input_flush", "initial_transaction", "completed_without_exception"]:
            changed = copy.deepcopy(self.observation)
            changed["observation"]["stage"] = stage
            self.assertFalse(assess(self.case, 0, json.dumps(changed))["local_runtime_contract_matched"])
        self.observation["cleanup_failed"] = True
        self.assertFalse(assess(self.case, 0, json.dumps(self.observation))["local_runtime_contract_matched"])

    def test_failed_process_wrong_identity_malformed_output_and_echoed_expectation_fail(self):
        self.assertFalse(assess(self.case, 1, json.dumps(self.observation))["local_runtime_contract_matched"])
        for output in ["", "worker OVERFLOW", "[]", json.dumps(dict(self.observation, id="other")),
                       json.dumps({"id": "case", "expected": "Overflow"})]:
            self.assertFalse(assess(self.case, 0, output)["local_runtime_contract_matched"])


if __name__ == "__main__":
    unittest.main()
