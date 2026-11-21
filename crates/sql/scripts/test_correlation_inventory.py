import copy
import unittest

from correlation_inventory import STAGES, case_inventory, inventory


def trace():
    shape = dict(join_type="LEFT", required_columns="{0}", left="LogicalTableScan",
                 right="LogicalAggregate", row_type="RecordType(INTEGER x)")
    return [dict(schema_version=1, step=stage, optimization_level=2, completed=True,
                 before=dict(correlates=[shape], node_counts={"LogicalCorrelate": 1}),
                 after=dict(correlates=[], node_counts={})) for stage in STAGES]


class CorrelationInventoryTest(unittest.TestCase):
    def test_each_stage_has_a_separate_denominator(self):
        records = trace()
        records.append(copy.deepcopy(records[1]))
        result = inventory(records)
        self.assertEqual([1, 2, 1, 1], [stage["before"]["occurrences"]
                                      for stage in result["stages"]])
        self.assertIsNone(result["C7_coverage"])
        self.assertIsNone(result["case_weighted_coverage"])

    def test_exact_shapes_preserve_required_columns_and_types(self):
        records = trace()
        other = copy.deepcopy(records[0])
        other["before"]["correlates"][0]["required_columns"] = "{1}"
        records.append(other)
        stage = inventory(records)["stages"][0]["before"]
        self.assertEqual(2, len(stage["shapes"]))
        self.assertEqual(1, len(stage["coarse_clusters"]))
        self.assertEqual([1, 5], sorted(s["first_journal_line"] for s in stage["shapes"]))

    def test_preserves_incomplete_and_retained_shapes(self):
        records = trace()
        records[-1]["completed"] = False
        records[-1]["after"] = copy.deepcopy(records[-1]["before"])
        result = inventory(records)["stages"][-1]
        self.assertEqual(1, result["incomplete"])
        self.assertEqual(1, result["after"]["occurrences"])

    def test_missing_stage_and_malformed_counts_fail(self):
        with self.assertRaises(ValueError):
            inventory(trace()[:-1])
        records = trace()
        records[0]["before"]["node_counts"]["LogicalCorrelate"] = 2
        with self.assertRaises(ValueError):
            inventory(records)

    def test_changed_shape_contract_fails(self):
        records = trace()
        records[0]["before"]["correlates"][0]["new_field"] = "value"
        with self.assertRaises(ValueError):
            inventory(records)


def case():
    graph = dict(root=0, correlates=1, subqueries=0, relations=[
        dict(id=0, kind="LogicalCorrelate", inputs=[1, 2], subqueries=[],
             correlate=dict(join_type="LEFT")),
        dict(id=1, kind="LogicalTableScan", inputs=[], subqueries=[]),
        dict(id=2, kind="LogicalAggregate", inputs=[3], subqueries=[]),
        dict(id=3, kind="LogicalTableScan", inputs=[], subqueries=[]),
    ])
    return dict(schema_version=1, id="case-a", stages_observed=4,
                stages=[dict(sequence=1, step=STAGES[1], completed=True,
                             before=graph, after=copy.deepcopy(graph))])


class CaseInventoryTest(unittest.TestCase):
    def test_records_case_stage_and_node_witness(self):
        result = case_inventory([case()])
        self.assertEqual(1, result["cases"])
        self.assertEqual(4, result["observed_stages"])
        cluster = result["stages"][1]["before"]["clusters"][0]
        self.assertEqual(dict(case="case-a", sequence=1, relation=0), cluster["witness"])
        self.assertEqual("LogicalAggregate", cluster["shape"]["right"])

    def test_duplicate_cases_are_not_extra_coverage(self):
        with self.assertRaises(ValueError):
            case_inventory([case(), case()])

    def test_reordered_and_outside_stage_indices_fail(self):
        record = case()
        record["stages"].append(copy.deepcopy(record["stages"][0]))
        with self.assertRaises(ValueError):
            case_inventory([record])
        record = case()
        record["stages"][0]["sequence"] = 4
        with self.assertRaises(ValueError):
            case_inventory([record])

    def test_nested_subquery_edges_are_checked(self):
        record = case()
        graph = record["stages"][0]["before"]
        graph["relations"][3]["subqueries"] = [dict(relation=99)]
        graph["subqueries"] = 1
        with self.assertRaises(ValueError):
            case_inventory([record])

    def test_boolean_counts_are_not_integer_counts(self):
        record = case()
        record["stages"][0]["before"]["correlates"] = True
        with self.assertRaises(ValueError):
            case_inventory([record])

    def test_no_correlation_cases_remain_in_denominator(self):
        record = case()
        record["stages"] = []
        result = case_inventory([record])
        self.assertEqual(1, result["cases"])
        self.assertEqual(0, result["cases_with_retained_stages"])
        self.assertIsNone(result["C7_coverage"])


if __name__ == "__main__":
    unittest.main()
