import copy
import unittest

from seam_inventory import summarize


class SeamInventoryTests(unittest.TestCase):
    def setUp(self):
        self.requests = [{"id": name} for name in ("b", "a", "c")]
        self.observations = [
            {
                "id": "b",
                "stage": "S2",
                "accepted": True,
                "compiler_bug": False,
                "operators": {"X": 2, "Y": 1},
                "declarations": {},
                "decoder_owners": {"X": "X", "Y": "Y"},
            },
            {
                "id": "a",
                "stage": "S2",
                "accepted": True,
                "compiler_bug": False,
                "operators": {"X": 1, "Z": 1},
                "declarations": {},
                "decoder_owners": {"X": "X", "Z": "Z"},
            },
            {
                "id": "c",
                "stage": "S2",
                "accepted": True,
                "compiler_bug": False,
                "operators": {"X": 1},
                "declarations": {},
                "decoder_owners": {"X": "X"},
            },
        ]

    def test_selection_retains_original_order_and_counts_instances_separately(self):
        result = summarize(self.requests, self.observations, "S2")
        self.assertEqual(result["cover_case_ids"], ["b", "a"])
        self.assertEqual(result["operator_instances"]["X"], 4)
        self.assertEqual(result["operator_case_counts"]["X"], 3)
        self.assertFalse(result["replay_verified"])

    def test_failures_are_retained(self):
        self.observations[2]["compiler_bug"] = True
        result = summarize(self.requests, self.observations, "S2")
        self.assertEqual(result["failures"], ["c"])
        self.assertEqual(result["cases"], 3)

    def test_missing_or_inconsistent_evidence_fails(self):
        with self.assertRaises(ValueError):
            summarize(self.requests, self.observations[:2], "S2")
        for change in (
            {"stage": "S3"},
            {"id": "a"},
            {"operators": {"X": True}},
            {"decoder_owners": {}},
        ):
            observations = copy.deepcopy(self.observations)
            observations[0].update(change)
            with self.assertRaises(ValueError):
                summarize(self.requests, observations, "S2")


if __name__ == "__main__":
    unittest.main()
