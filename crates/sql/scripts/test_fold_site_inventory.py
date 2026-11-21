import tempfile
import unittest
from pathlib import Path

from fold_site_inventory import inventory, unclassified_calls


class FoldSiteInventoryTests(unittest.TestCase):
    def test_new_simplification_call_requires_a_classification(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "NewConsumer.java").write_text("result = new Simplify(compiler).apply(value);\n")
            findings = unclassified_calls(root, [])
            self.assertEqual(findings[0]["file"], "NewConsumer.java")
            self.assertEqual(findings[0]["line"], 1)
            sites = [("NewConsumer.java", "new Simplify(compiler)")]
            self.assertEqual(unclassified_calls(root, sites), [])
            (root / "NewConsumer.java").write_text("result = other.reduce(compiler);\n")
            self.assertTrue(unclassified_calls(root, sites))

    def test_inventory_does_not_turn_fixture_presence_into_branch_coverage(self):
        result = inventory(Path(__file__).resolve().parents[3])
        self.assertTrue(result["local_explicit_frontend_calls_classified"], result)
        self.assertFalse(result["all_constant_site_coverage_complete"])
        self.assertTrue(any(site["fixture_ids"] for site in result["sites"]))
        self.assertTrue(any(not site["fixture_ids"] for site in result["sites"]))
        self.assertTrue(all(not site["branch_reachability_proven"] for site in result["sites"]))


if __name__ == "__main__":
    unittest.main()
