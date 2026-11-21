import unittest

from benchmark_seams import bootstrap_ratio, summarize


class SeamTimingTests(unittest.TestCase):
    def test_paired_bootstrap_preserves_constant_ratio(self):
        self.assertEqual(bootstrap_ratio([1, 3, 5], [2, 6, 10]), [2, 2])

    def test_summary_uses_total_costs_and_excludes_warmup(self):
        rows = []
        for warmup, direct, head, tail in [
            (True, 100, 1, 1),
            (False, 1, 4, 5),
            (False, 9, 6, 3),
        ]:
            rows.append(
                {
                    "case": "example",
                    "warmup": warmup,
                    "seam_bytes": 10,
                    "direct": {"seconds": direct, "max_rss_kib": 100},
                    "head": {"seconds": head, "max_rss_kib": 80},
                    "tail": {"seconds": tail, "max_rss_kib": 90},
                }
            )
        result = summarize(rows)["example"]
        self.assertEqual(result["samples"], 2)
        self.assertEqual(result["direct"]["empirical_p95_seconds"], 9)
        self.assertEqual(result["ratio_of_mean_times"], 1.8)
        self.assertEqual(result["hybrid_max_child_rss_kib"], 90)


if __name__ == "__main__":
    unittest.main()
