import itertools
import unittest
from unittest.mock import patch

from benchmark_keys import summarize


def samples(columns_per_workload=(1, 4, 12)):
    return [
        {
            "rows": rows,
            "columns": columns,
            "distribution": distribution,
            "iteration": iteration,
            "seed": 0x9E3779B97F4A7C15 + iteration,
            "warmup": iteration < 2,
            "direct_first": iteration % 2 == 0,
            "key_bytes": rows * columns * 9,
            "direct_sort_ns": 100 if iteration >= 2 else 100000,
            "direct_merge_ns": 20,
            "encode_ns": 200,
            "normalized_sort_ns": 80,
            "normalized_merge_ns": 10,
        }
        for rows, columns, distribution, iteration in itertools.product(
            (4096, 65536),
            columns_per_workload,
            ("Uniform", "CommonPrefix", "Duplicates"),
            range(7),
        )
    ]


class KeyBenchmarkTests(unittest.TestCase):
    @patch("benchmark_keys.bootstrap_ratio", return_value=[0, 0])
    def test_encoding_can_reverse_the_sorting_result(self, _bootstrap):
        result = summarize(samples(), 5)
        self.assertEqual(len(result), 18)
        for workload in result:
            comparisons = workload["comparisons"]
            self.assertEqual(
                comparisons["sort_reused_keys"]["normalized_over_direct"], 0.8
            )
            self.assertEqual(
                comparisons["sort_including_encoding"]["normalized_over_direct"], 2.8
            )
            self.assertEqual(
                comparisons["encode_sort_merge"]["normalized_over_direct"], 290 / 120
            )

    def test_incomplete_or_changed_inputs_cannot_produce_a_summary(self):
        missing_workload = [row for row in samples() if row["rows"] != 4096]
        duplicated = samples()
        duplicated.append(duplicated[0])
        wrong_seed = samples()
        wrong_seed[0]["seed"] += 1
        for rows in (missing_workload, duplicated, wrong_seed):
            with self.subTest(rows=len(rows)), self.assertRaises(ValueError):
                summarize(rows, 5)

    @patch("benchmark_keys.bootstrap_ratio", return_value=[0, 0])
    def test_mixed_payload_size_can_vary_with_values(self, _bootstrap):
        rows = samples((3,))
        for row in rows:
            row["key_bytes"] += row["iteration"]
        measured = summarize(rows, 5, mixed=True)
        self.assertEqual(len(measured), 6)
        self.assertEqual(measured[0]["key_bytes_min"], 4096 * 3 * 9 + 2)
        self.assertEqual(measured[0]["key_bytes_max"], 4096 * 3 * 9 + 6)
        rows[0]["key_bytes"] = 16 * 1024 * 1024 + 1
        with self.assertRaises(ValueError):
            summarize(rows, 5, mixed=True)


if __name__ == "__main__":
    unittest.main()
