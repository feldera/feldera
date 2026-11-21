import copy
import json
import unittest

from connector_cases import cases, sql_cases
from run_connector_probe import PROBE, compare, runtime_fields


class ConnectorProbeTests(unittest.TestCase):
    def test_matrix_is_unique_and_every_input_has_all_sql_policies(self):
        selected = cases(json.loads((PROBE / "java-inventory.json").read_text()))
        self.assertEqual(len({x["id"] for x in selected}), len(selected))
        sql = list(sql_cases(selected))
        self.assertEqual(len(sql), 4 * len(selected))
        self.assertEqual(len({x["id"] for x in sql}), len(sql))
        self.assertIn("suppressed_and_promoted", {x["source"]["policy"] for x in sql})

    def test_comparator_rejects_missing_records_and_internal_failures(self):
        requests = [{"id": "a"}]
        records = [{"id": "a", "compiler_bug": False, "warnings": []}]
        self.assertTrue(compare(requests, records, records)["equal"])
        for bad in [
            [],
            [{"id": "a", "compiler_bug": True, "warnings": []}],
            [{"id": "a", "compiler_bug": 0, "warnings": []}],
            [{"id": "b", "compiler_bug": False, "warnings": []}],
        ]:
            with self.assertRaises(ValueError):
                compare(requests, records, bad)

    def test_message_text_is_a_metric_but_locations_and_snippets_are_hard(self):
        requests = [{"id": "a", "source": {}}]
        records = [
            {
                "id": "a",
                "compiler_bug": False,
                "accepted": True,
                "warnings": [
                    {
                        "warning": True,
                        "continuation": False,
                        "error_type": "Invalid configuration",
                        "message": "bad",
                        "snippet": "x",
                        "position": {
                            "start_line_number": 1,
                            "start_column": 2,
                            "end_line_number": 1,
                            "end_column": 2,
                        },
                    }
                ],
            }
        ]
        altered = copy.deepcopy(records)
        altered[0]["warnings"][0]["message"] = "different"
        self.assertTrue(compare(requests, records, altered)["equal"])
        altered[0]["warnings"][0]["snippet"] = ""
        self.assertFalse(compare(requests, records, altered)["equal"])
        altered[0]["warnings"][0]["warning"] = 1
        with self.assertRaises(ValueError):
            compare(requests, records, altered)

    def test_schema_traversal_follows_flattening_but_not_nested_field_types(self):
        schemas = {
            "base": {"properties": {"x": {"$ref": "missing"}}},
            "outer": {
                "allOf": [
                    {"$ref": "#/components/schemas/base"},
                    {"properties": {"y": {}}},
                ]
            },
        }
        self.assertEqual(runtime_fields(schemas, "outer"), {"x", "y"})
        with self.assertRaises(ValueError):
            runtime_fields({"a": {"$ref": "#/components/schemas/a"}}, "a")


if __name__ == "__main__":
    unittest.main()
