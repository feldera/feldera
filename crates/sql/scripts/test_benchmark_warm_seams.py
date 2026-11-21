import copy
import unittest

from benchmark_warm_seams import summarize


class WarmSeamBenchmarkTests(unittest.TestCase):
    def setUp(self):
        self.schedule = []
        self.results = []
        for iteration, times in [(-1, [100, 1, 1]), (0, [1, 4, 5]), (1, [9, 6, 3])]:
            for part, seconds in zip(["direct", "head", "tail"], times):
                self.schedule.append(dict(case="sample", iteration=iteration, warmup=iteration < 0, part=part))
                self.results.append(dict(index=len(self.results), elapsed_ns=seconds * 1_000_000_000, exit_code=0))

    def test_costs_exclude_warmup_and_do_not_invent_per_path_memory(self):
        report = summarize(self.schedule, self.results)["sample"]
        self.assertEqual(report["samples"], 2)
        self.assertEqual(report["split_over_direct"], 1.8)
        self.assertEqual(report["direct"]["empirical_p95_seconds"], 9)
        self.assertNotIn("max_rss_kib", report["direct"])

    def test_missing_reordered_or_failed_observations_cannot_be_summarized(self):
        with self.assertRaisesRegex(ValueError, "incomplete"):
            summarize(self.schedule, self.results[:-1])
        altered = copy.deepcopy(self.results)
        altered[0]["index"] = 1
        with self.assertRaisesRegex(ValueError, "invalid or failed"):
            summarize(self.schedule, altered)
        altered[0]["index"] = 0
        altered[0]["exit_code"] = 1
        with self.assertRaisesRegex(ValueError, "invalid or failed"):
            summarize(self.schedule, altered)


if __name__ == "__main__":
    unittest.main()
