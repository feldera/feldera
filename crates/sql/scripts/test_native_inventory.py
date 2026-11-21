import copy
import json
import unittest

from native_inventory import Graph, Types, connector_fixture


def fixture():
    string = {"class": "DBSPTypeString", "id": 1, "fixed": False,
              "precision": -1, "code": "STRING", "mayBeNull": True}
    row = {"class": "DBSPTypeTuple", "id": 2, "tupFields": [string], "mayBeNull": False}
    zset = {"class": "DBSPZSetExpression", "elementType": row,
            "data": [{"class": "DBSPTupleExpression", "type": {"node": 2},
                      "fields": [{"class": "DBSPStringLiteral", "type": {"node": 1},
                                  "value": "🐍x", "charset": "UTF-8"}]}], "weights": [-2]}
    case = {"source_test": "Test#unicode", "case_index": 0, "step_semantics": "delta",
            "schema": {"inputs": [{"name": "t", "case_sensitive": False,
                                   "fields": [{"name": "s", "columntype": {
                                       "type": "VARCHAR", "precision": -1, "nullable": True}}]}]},
            "commands": [{"kind": "step", "inputs": [{"name": {"text": "t", "quoted": False},
                                                       "zset_ir": zset}]}]}
    specification = {"case_id": "Test#unicode/0", "step": 0, "input": 0,
                     "format": {"name": "json", "config": {"update_format": "insert_delete", "array": False}}}
    return case, specification, zset


class NativeInventoryTests(unittest.TestCase):
    def test_type_identity_ignores_node_numbers_but_retains_sql_parameters(self):
        types = Types()
        a = {"class": "DBSPTypeDecimal", "id": 1, "precision": 38, "scale": 10, "mayBeNull": False}
        b = dict(a, id=9, position={"line": 3})
        first = types.add(Graph(a), a, "case", "/a", "row")
        self.assertEqual(first, types.add(Graph(b), b, "case", "/b", "row"))
        for changed in [dict(a, scale=2), dict(a, precision=37), dict(a, mayBeNull=True)]:
            self.assertNotEqual(first, types.add(Graph(changed), changed, "case", "/c", "row"))

    def test_graph_references_fail_closed(self):
        graph = Graph({"class": "DBSPTypeTuple", "id": 1, "field": {"node": 1}})
        with self.assertRaisesRegex(ValueError, "cyclic"):
            graph.shape({"node": 1})
        with self.assertRaisesRegex(ValueError, "missing"):
            graph.shape({"node": 2})
        with self.assertRaisesRegex(ValueError, "conflicting"):
            Graph([{"class": "DBSPTypeTuple", "id": 1}, {"class": "DBSPTypeString", "id": 1}])

    def test_connector_preserves_signed_multiplicity_unicode_and_null(self):
        case, specification, zset = fixture()
        result = connector_fixture(case, specification)
        self.assertEqual([json.loads(line) for line in result["payload"].splitlines()],
                         [{"delete": {"s": "🐍x"}}, {"delete": {"s": "🐍x"}}])
        zset["weights"] = [1]
        zset["data"][0]["fields"][0]["value"] = None
        self.assertEqual(json.loads(connector_fixture(case, specification)["payload"]), {"insert": {"s": None}})
        zset["weights"] = [0]
        self.assertEqual(connector_fixture(case, specification)["payload"], "")

    def test_connector_rejects_sql_type_and_identifier_mismatches(self):
        for mutation in [
            lambda c, s, z: c["schema"]["inputs"][0]["fields"][0]["columntype"].update(nullable=False),
            lambda c, s, z: c["schema"]["inputs"][0].update(case_sensitive=True),
            lambda c, s, z: z["data"][0].update(type={"node": 1}),
            lambda c, s, z: z.update(weights=[True]),
            lambda c, s, z: z.update(weights=[10001]),
            lambda c, s, z: s.update(case_id="wrong/0"),
            lambda c, s, z: s["format"]["config"].update(array=True),
            lambda c, s, z: s.update(step=-1),
        ]:
            case, specification, zset = fixture()
            mutation(case, specification, zset)
            with self.assertRaises(ValueError):
                connector_fixture(case, specification)

    def test_connector_only_allows_nullability_widening_cast(self):
        case, specification, zset = fixture()
        source = zset["data"][0]["fields"][0]
        source["type"] = dict(zset["elementType"]["tupFields"][0], id=3, mayBeNull=False)
        zset["data"][0]["fields"] = [{"class": "DBSPCastExpression", "type": {"node": 1}, "source": source}]
        self.assertEqual(connector_fixture(case, specification)["records"], 2)
        source["type"]["precision"] = 1
        with self.assertRaisesRegex(ValueError, "nullability widening"):
            connector_fixture(case, specification)

    def test_whole_relation_requires_empty_state_and_rejects_later_step(self):
        case, specification, _ = fixture()
        case["step_semantics"] = "whole_relation"
        self.assertTrue(connector_fixture(case, specification)["initial_empty_state_required"])
        case["commands"].append(copy.deepcopy(case["commands"][0]))
        specification["step"] = 1
        with self.assertRaisesRegex(ValueError, "state differencing"):
            connector_fixture(case, specification)


if __name__ == "__main__":
    unittest.main()
