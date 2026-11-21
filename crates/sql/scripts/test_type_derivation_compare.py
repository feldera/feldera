import copy
import json
from pathlib import Path
import tempfile
import unittest

from type_derivation_compare import compare, java_result, run, sql_type, unique_object


def golden():
    return {"schema_version": 1, "accepted": True, "compiler_bug": False, "diagnostics": [],
            "probe": {"schema_version": 1, "id": 0, "operation": "ABS", "arguments": []},
            "schema": {"outputs": [{"name": "v", "fields": [{"columntype":
                {"type": "DECIMAL", "nullable": True, "precision": 38, "scale": 10}}]}]}}


class TypeComparisonTest(unittest.TestCase):
    def test_parameters_and_nullability_are_compared(self):
        java = golden()
        rust = {"status": "bound", "result": java_result(java), "operands": []}
        self.assertEqual(compare(java, rust), "equal")
        for field, value in [("precision", 37), ("scale", 9)]:
            mutation = copy.deepcopy(rust)
            mutation["result"]["kind"][field] = value
            self.assertEqual(compare(java, mutation), "result_type_difference")
        rust["result"]["nullable"] = False
        self.assertEqual(compare(java, rust), "result_type_difference")
        rust["result"]["nullable"] = 1
        self.assertEqual(compare(java, rust), "result_type_difference")

    def test_fixed_character_and_nested_array_attributes_survive(self):
        fixed = {"type": "CHAR", "precision": 5, "nullable": False}
        variable = dict(fixed, type="VARCHAR")
        self.assertNotEqual(sql_type(fixed), sql_type(variable))
        array = {"type": "ARRAY", "component": fixed, "nullable": True}
        self.assertEqual(sql_type(array), {"component": sql_type(fixed), "nullable": True})

    def test_unknown_metadata_and_json_duplicates_fail_closed(self):
        with self.assertRaises(ValueError):
            sql_type({"type": "INTEGER", "nullable": False, "collation": "unexpected"})
        with self.assertRaises(ValueError):
            sql_type({"type": "TIME", "nullable": False})
        with self.assertRaises(ValueError):
            unique_object([("id", 0), ("id", 1)])

    def test_unsupported_is_never_a_matching_sql_rejection(self):
        java = golden()
        java["accepted"] = False
        self.assertEqual(compare(java, {"status": "unsupported"}), "unsupported")
        self.assertEqual(compare(java, {"status": "rejected"}), "equal")
        self.assertEqual(compare(java, {"status": "bound", "result": {}}), "acceptance_difference")

    def test_compiler_bugs_cannot_be_rejection_goldens(self):
        java = golden()
        java.update(accepted=False, compiler_bug=True)
        with self.assertRaises(ValueError):
            java_result(java)

    def test_complete_stream_is_counted_and_differences_are_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            java = golden()
            rust = {"schema_version": 1, "id": 0, "runtime_verified": False, "translated": {"status": "rejected"},
                    "rewritten": {"status": "rejected"}, "additional": {"status": "unsupported"}}
            (root / "java").write_text(json.dumps(java) + "\n")
            (root / "rust").write_text(json.dumps(rust) + "\n")
            report = run(root / "java", root / "rust", root / "report", 1)
            self.assertEqual(report["counts"]["translated"]["acceptance_difference"], 1)
            self.assertFalse(report["full_c9_verified"])
            self.assertEqual(len((root / "report/differences.jsonl").read_text().splitlines()), 2)
            with self.assertRaises(ValueError):
                run(root / "java", root / "rust", root / "truncated", 2)
            self.assertFalse((root / "truncated/complete.json").exists())

    def test_missing_and_duplicate_ids_do_not_create_completion_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            java = golden()
            rust = {"schema_version": 1, "id": 1, "runtime_verified": False}
            (root / "java").write_text(json.dumps(java) + "\n")
            (root / "rust").write_text(json.dumps(rust) + "\n")
            with self.assertRaises(ValueError):
                run(root / "java", root / "rust", root / "reordered", 1)
            self.assertFalse((root / "reordered/complete.json").exists())
            rust.update(id=0, translated={"status": "rejected"},
                        rewritten={"status": "rejected"}, additional={"status": "unsupported"})
            (root / "java").write_text((json.dumps(java) + "\n") * 2)
            (root / "rust").write_text((json.dumps(rust) + "\n") * 2)
            with self.assertRaises(ValueError):
                run(root / "java", root / "rust", root / "duplicated", 2)
            self.assertFalse((root / "duplicated/complete.json").exists())
            with self.assertRaises(ValueError):
                run(root / "java", root / "rust", root / "empty", 0)


if __name__ == "__main__":
    unittest.main()
