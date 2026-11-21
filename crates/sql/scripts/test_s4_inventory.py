import unittest

from s4_inventory import constructed_coverage, evaluate


class InventoryCoverageTests(unittest.TestCase):
    def test_placement_and_condition_remain_distinct(self):
        row = {
            "sources": [],
            "selectors": [{"container": "Composite"}],
            "required_points": [
                {
                    "container": "Composite",
                    "pass": "Graph",
                    "index": 0,
                    "activation": "always",
                },
                {
                    "container": "Composite",
                    "pass": "Graph",
                    "index": 2,
                    "activation": "has dependency",
                },
            ],
        }
        point = {
            "path": [{"container": "Composite", "pass": "Graph", "index": 0}],
            "observations": 7,
            "cases": 3,
            "examples": ["case/0"],
        }
        report = evaluate(row, [point])
        self.assertTrue(report["required_points"][0]["observed"])
        self.assertFalse(report["required_points"][1]["observed"])
        self.assertEqual("has dependency", report["required_points"][1]["activation"])
        self.assertFalse(report["state_contract_verified"])
        self.assertEqual(3, report["matching_path_case_pairs"])

    def test_constructed_but_unexecuted_conditional_paths_remain_visible(self):
        outer = {
            "path": [{"container": "Optimizer", "pass": "ConditionalPass", "index": 0}],
            "implementation": "Conditional",
        }
        inner = {
            "path": [
                *outer["path"],
                {"container": "Conditional", "pass": "ConditionalPass", "index": 0},
            ],
            "implementation": "Inner",
        }
        constructed = {
            "configurations": [{"points": [outer, inner]}, {"points": [outer]}]
        }
        report = constructed_coverage(constructed, [outer])
        self.assertEqual(2, report["unique_constructed_paths"])
        self.assertEqual(1, report["observed_constructed_paths"])
        self.assertEqual(
            inner["path"], report["unobserved_constructed_paths"][0]["path"]
        )
        self.assertEqual(
            [0], report["unobserved_constructed_paths"][0]["configurations"]
        )
        self.assertFalse(report["all_possible_configurations_enumerated"])

    def test_known_nonreturning_path_is_retained_with_an_explicit_disposition(self):
        point = {
            "path": [{"container": "Conditional", "pass": "Fail", "index": 0}],
            "implementation": "Fail",
        }
        construction = {"configurations": [{"points": [point]}]}
        report = constructed_coverage(construction, [], [{"implementation": "Fail"}])
        self.assertEqual(1, len(report["unobserved_constructed_paths"]))
        self.assertTrue(report["unobserved_constructed_paths"][0]["always_throws"])
        self.assertEqual(0, report["unobserved_paths_requiring_activation"])
        point["implementation"] = "Unknown"
        report = constructed_coverage(construction, [], [{"implementation": "Fail"}])
        self.assertEqual(1, report["unobserved_paths_requiring_activation"])

    def test_nested_selector_matches_once_per_path(self):
        row = {"selectors": [{"container": "Repeat"}], "required_points": []}
        point = {
            "path": [
                {"container": "Repeat", "pass": "Outer", "index": 0},
                {"container": "Repeat", "pass": "Inner", "index": 1},
            ],
            "observations": 5,
            "cases": 1,
            "examples": ["case/0"],
        }
        result = evaluate(row, [point])
        self.assertEqual(1, result["matching_paths"])
        self.assertEqual(5, result["observations"])


if __name__ == "__main__":
    unittest.main()
