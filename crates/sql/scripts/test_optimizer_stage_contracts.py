import copy
import unittest

from optimizer_stage_contracts import classify


class OptimizerStageContractTests(unittest.TestCase):
    def setUp(self):
        contract = {"index": 0, "step": "lower", "kind": "hep", "abort_on_error": True,
                    "family": "lowering", "contract": "preserve identity", "shared_instance_with": None}
        self.contracts = {"schema_version": 1, "scope": "test", "steps": [contract]}
        self.inventory = {"schema_version": 1, "steps": [
            {"index": 0, "step": "lower", "kind": "hep", "abort_on_error": True,
             "optimization_level": 0, "rules": [{"description": "Rule", "implementation": "example.Rule"}]}]}
        self.census = {"schema_version": 1, "configured_rules": 1, "productions": {},
                       "steps": [{"configured_rules": ["Rule"], "productions": {}, "attempts": {}}]}

    def test_zero_production_rule_retains_stage_role_and_scope(self):
        report = classify(self.contracts, self.inventory, self.census)
        self.assertEqual(report["rules"][0]["stage_families"], ["lowering"])
        self.assertTrue(report["rules"][0]["needs_targeted_production_test"])
        self.assertFalse(report["maintainer_review_complete"])

    def test_new_stage_or_changed_failure_policy_requires_review(self):
        altered = copy.deepcopy(self.inventory)
        altered["steps"][0]["abort_on_error"] = False
        with self.assertRaisesRegex(ValueError, "changed abort_on_error"):
            classify(self.contracts, altered, self.census)
        altered["steps"][0]["index"] = 1
        with self.assertRaisesRegex(ValueError, "no contract"):
            classify(self.contracts, altered, self.census)

    def test_shared_instance_contract_must_be_reciprocal(self):
        self.contracts["steps"][0]["shared_instance_with"] = 0
        with self.assertRaisesRegex(ValueError, "shared step-instance"):
            classify(self.contracts, self.inventory, self.census)


if __name__ == "__main__":
    unittest.main()
