from datetime import date, timedelta
import unittest

from shadow_sample import EVENT, summarize


class FieldSample(unittest.TestCase):
    today = date(2026, 11, 20)

    def event(self, ago, matched, differences=0, incomplete=0, build="candidate"):
        return {"fields": {"message": EVENT, "compiler_build": build,
                "date": (self.today - timedelta(days=ago)).isoformat(),
                "matched_distinct": matched, "differences": differences, "incomplete": incomplete}}

    def test_restarts_missing_samples_and_the_open_day(self):
        report = summarize([self.event(1, 100), self.event(1, 120), self.event(0, 200),
                            self.event(2, 200, build="another")], "candidate", self.today)
        self.assertEqual(report["days"][0]["matched_distinct_lower_bound"], 120)
        self.assertEqual(len(report["days"]), 1)
        self.assertFalse(report["field_sample_satisfied"])

    def test_difference_restarts_the_window_and_cannot_be_overwritten(self):
        rows = [self.event(n, 200) for n in range(1, 33)]
        rows.extend([self.event(30, 200, differences=1), self.event(30, 200)])
        report = summarize(rows, "candidate", self.today)
        self.assertEqual(report["qualifying_days"], 29)
        self.assertFalse(report["field_sample_satisfied"])
        clean = summarize([self.event(n, 200) for n in range(1, 31)], "candidate", self.today)
        self.assertTrue(clean["field_sample_satisfied"])
        self.assertEqual(summarize([], "candidate", self.today)["qualifying_days"], 0)

    def test_difference_on_the_open_day_invalidates_a_completed_window(self):
        rows = [self.event(n, 200) for n in range(1, 31)]
        rows.append(self.event(0, 15, differences=1))
        report = summarize(rows, "candidate", self.today)
        self.assertFalse(report["field_sample_satisfied"])
        self.assertEqual(report["qualifying_days"], 0)
        self.assertTrue(report["days"][-1]["observed_difference"])
