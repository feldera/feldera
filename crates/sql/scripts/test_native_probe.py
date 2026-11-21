import tempfile
import unittest
from pathlib import Path

from run_native_probe import observations, vectors


class NativeProbeTests(unittest.TestCase):
    def test_comparison_refuses_missing_reordered_or_invalid_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "values.tsv"
            for content in (
                "",
                "0\tNULL\n0\tNULL\n",
                "1\tNULL\n0\tNULL\n",
                "0\t0\n1\t9223372036854775808\n",
                "0\t0\n1\t-0\n",
                "0\t0\n1\tpanic\n",
                "0\t0\n1\t01\n",
                "0\t0\n1\tOVERFLOW\n",
                "0\t0\n1\tOVERFLOW:'1 / 0' causes overflow for type BIGINT\n",
            ):
                path.write_text(content)
                with self.assertRaises(ValueError):
                    observations(path, 2)
            overflow = (
                "OVERFLOW:'9223372036854775807 + 1' causes overflow for type BIGINT"
            )
            path.write_text(f"0\tNULL\n1\t{overflow}\n2\t-9223372036854775808\n")
            self.assertEqual(
                observations(path, 3), ["NULL", overflow, "-9223372036854775808"]
            )

    def test_vectors_cover_each_operation_and_nullable_boundaries(self):
        cases = list(vectors())
        self.assertEqual(len(cases), 6732)
        self.assertEqual(cases, list(vectors()))
        for operation in ("add", "subtract", "multiply"):
            self.assertIn((operation, None, -(2**63)), cases)
            self.assertIn((operation, 2**63 - 1, 2**63 - 1), cases)
