import unittest

from run_multiset_probe import compare, programs


class MultisetProbeTests(unittest.TestCase):
    def test_population_is_declared_before_compilation(self):
        selected = programs()
        self.assertEqual(len(selected), 50)
        self.assertIn("safe_narrow", selected)
        self.assertIn("lateness", selected)
        self.assertIn("constant_duplicate", selected)

    def test_comparison_detects_missing_ports_and_mixed_up_cases(self):
        expected = [
            {
                "id": "x",
                "multisets": [[False, True], [True]],
                "removed_operators": 2,
                "dead_code_equal": True,
            }
        ]
        self.assertTrue(compare(expected, expected)["equal"])
        self.assertFalse(
            compare(expected, [{"id": "x", "multisets": [[False], [True]]}])["equal"]
        )
        with self.assertRaises(ValueError):
            compare(expected, [])
        with self.assertRaises(ValueError):
            compare(expected, [{"id": "y", "multisets": [[False, True], [True]]}])
        with self.assertRaises(ValueError):
            compare(expected, [{"id": "x", "multisets": [[0, 1], [1]]}])
        with self.assertRaises(ValueError):
            compare(expected * 2, expected * 2)
        self.assertFalse(
            compare(expected, [{**expected[0], "dead_code_equal": False}])["equal"]
        )


if __name__ == "__main__":
    unittest.main()
