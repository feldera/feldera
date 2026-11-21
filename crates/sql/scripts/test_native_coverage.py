"""Claim boundaries and identity failures in the native implementation ledger."""
import copy
import unittest
from native_coverage import ledger


class NativeCoverageTests(unittest.TestCase):
    def inputs(self):
        selection = {"programs": [{"id": str(i), "sha256": str(i) * 64, "features": []} for i in range(10)]}
        inventory = {
            "programs": [{"id": str(i), "step_semantics": "delta", "steps": [{"inputs": [{"row_type": "row"}], "outputs": [{"row_type": "row"}]}], "operators": [{"id": i, "class": "DBSPMapOperator", "path": "/allOperators/0", "output_type": "row"}], "function_path_segments": []} for i in range(10)],
            "types": [{"id": "row", "shape": {"class": "DBSPTypeTuple"}, "uses": [{"case": str(i), "role": "index_key"} for i in range(10)]}],
            "connector": {},
            "native_C7_executed": True,
        }
        return inventory, selection, {"selected_types": [{"type_id": "row"}]}

    def test_component_success_never_grants_program_or_normalized_key_credit(self):
        result = ledger(*self.inputs())
        self.assertEqual(result["native_C7_programs"], 0)
        self.assertFalse(result["N1_complete"])
        self.assertFalse(result["N2_complete"])
        for program in result["programs"]:
            self.assertEqual(program["archive_component_verified_type_ids"], ["row"])
            self.assertEqual(program["native_C7"], "not_executed")
            self.assertEqual(program["operators"][0]["native_ir_builder"], "missing")
            self.assertEqual(program["normalized_index_keys"][0]["sort_merge_measurement"], "missing_for_this_key_type")

    def test_cannot_shrink_ten_program_denominator(self):
        inventory, selection, evidence = self.inputs()
        inventory["programs"].pop()
        with self.assertRaisesRegex(ValueError, "complete frozen"):
            ledger(inventory, selection, evidence)

    def test_key_semantics_never_grant_performance_or_program_credit(self):
        inventory, selection, evidence = self.inputs()
        evidence["normalized_keys"] = [
            {"type_id": "row", "semantic_checks": "four direction/NULL policies"}
        ]
        result = ledger(inventory, selection, evidence)
        key = result["programs"][0]["normalized_index_keys"][0]
        self.assertEqual(key["semantic_checks"], "four direction/NULL policies")
        self.assertEqual(key["sort_merge_measurement"], "missing_for_this_key_type")
        self.assertEqual(result["native_C7_programs"], 0)
        evidence["normalized_keys"][0]["type_id"] = "unknown"
        with self.assertRaisesRegex(ValueError, "Component evidence"):
            ledger(inventory, selection, evidence)

    def test_unknown_operator_is_not_silently_excluded(self):
        inventory, selection, evidence = self.inputs()
        inventory["programs"][0]["operators"][0]["class"] = "DBSPUnknownOperator"
        with self.assertRaisesRegex(ValueError, "Unclassified"):
            ledger(inventory, selection, evidence)

    def test_complete_execution_only_credits_its_frozen_case(self):
        record = {"id": "0", "case_sha256": "0" * 64, "verified_steps": 1,
                  "compiled_and_loaded": True, "all_outputs_equal": True,
                  "runtime": "dbsp", "evidence": "range-case.json"}
        certificate = {"schema_version": 1, "programs": [record]}
        result = ledger(*self.inputs(), certificate)
        self.assertEqual(result["native_C7_programs"], 1)
        self.assertEqual(result["programs"][0]["native_C7"], "verified_on_dbsp")
        self.assertEqual(result["programs"][1]["native_C7"], "not_executed")
        self.assertFalse(result["N1_complete"])
        for changed in ({"case_sha256": "1" * 64}, {"all_outputs_equal": False},
                        {"verified_steps": True}, {"verified_steps": 0}):
            with self.assertRaisesRegex(ValueError, "Incomplete or mismatched"):
                ledger(*self.inputs(), {**certificate, "programs": [{**record, **changed}]})

    def test_missing_or_duplicate_type_is_rejected(self):
        inventory, selection, evidence = self.inputs()
        inventory["programs"][0]["steps"][0]["inputs"][0]["row_type"] = "missing"
        with self.assertRaisesRegex(ValueError, "missing type"):
            ledger(inventory, selection, evidence)
        inventory, selection, evidence = self.inputs()
        inventory["types"].append(copy.deepcopy(inventory["types"][0]))
        with self.assertRaisesRegex(ValueError, "Duplicate type"):
            ledger(inventory, selection, evidence)

    def test_connector_requires_the_frozen_input_and_equal_output_bytes(self):
        inventory, selection, evidence = self.inputs()
        fixture = {
            "case_id": "0", "step": 0, "input": 0, "records": 1,
            "payload_sha256": "a" * 64, "format": {"name": "json"},
        }
        inventory["connector"] = {**fixture, "connector_executed": True}
        execution = {
            "id": "0", "case_sha256": "0" * 64, "verified_steps": 1,
            "compiled_and_loaded": True, "all_outputs_equal": True,
            "runtime": "dbsp", "evidence": "case.json",
        }
        executed = {"schema_version": 1, "programs": [execution]}
        connector = {
            **fixture, "schema_version": 1, "archive_index": "connector-archive.json",
            "production_parser_and_encoder": True, "decoded_inputs_equal": True,
            "weighted_outputs_equal": True, "errors_empty": True,
            "classic_output_sha256": "b" * 64, "native_output_sha256": "b" * 64,
        }
        self.assertFalse(ledger(inventory, selection, evidence, executed)
                         ["connector"]["connector_executed"])
        result = ledger(inventory, selection, evidence, executed, connector)
        self.assertTrue(result["connector"]["connector_executed"])
        self.assertEqual(result["native_C7_steps"], 1)
        self.assertFalse(result["N1_complete"])
        for changed, error in [
            ({"payload_sha256": "c" * 64}, "frozen fixture"),
            ({"step": False}, "frozen fixture"),
            ({"native_output_sha256": "c" * 64}, "output bytes differ"),
            ({"production_parser_and_encoder": False}, "Incomplete connector"),
        ]:
            with self.assertRaisesRegex(ValueError, error):
                ledger(inventory, selection, evidence, executed, {**connector, **changed})
        with self.assertRaisesRegex(ValueError, "Incomplete connector"):
            ledger(inventory, selection, evidence, None, connector)


if __name__ == "__main__":
    unittest.main()
