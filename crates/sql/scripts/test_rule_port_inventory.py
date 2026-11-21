import unittest

from rule_port_inventory import reconcile


class RuleInventoryTests(unittest.TestCase):
    def setUp(self):
        self.step = {
            "kind": "hep",
            "index": 0,
            "step": "simplify",
            "optimization_level": 2,
            "rules": [{"description": "rule", "implementation": "example.Rule"}],
        }
        self.inventory = {"schema_version": 1, "steps": [self.step]}
        self.census = {
            "schema_version": 1,
            "configured_rules": 1,
            "productions": {},
            "steps": [
                {"configured_rules": ["rule"], "productions": {}, "attempts": {}}
            ],
        }

    def test_unobserved_rule_stays_in_scope(self):
        rules, _ = reconcile(self.inventory, self.census)
        self.assertTrue(rules[0]["needs_targeted_production_test"])
        self.assertEqual(
            "retain_behavior_in_initial_parity_scope", rules[0]["proposed_disposition"]
        )

    def test_missing_conditional_rule_is_not_silently_omitted(self):
        self.step["rules"] = []
        with self.assertRaisesRegex(ValueError, "missing implementations"):
            reconcile(self.inventory, self.census)

    def test_same_description_cannot_hide_different_implementations(self):
        self.step["rules"].append(
            {"description": "rule", "implementation": "example.OtherRule"}
        )
        with self.assertRaisesRegex(ValueError, "ambiguous implementation"):
            reconcile(self.inventory, self.census)

    def test_inconsistent_production_totals_fail(self):
        self.census["productions"] = {"rule": 3}
        with self.assertRaisesRegex(ValueError, "productions disagree"):
            reconcile(self.inventory, self.census)


if __name__ == "__main__":
    unittest.main()
