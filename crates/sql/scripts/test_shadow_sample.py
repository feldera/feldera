from datetime import date, timedelta
import unittest

from shadow_sample import EVENT, summarize


class FieldSample(unittest.TestCase):
    package = "0" * 64
    today = date(2026, 11, 20)

    def event(self, ago, matched, differences=0, incomplete=0, build="candidate"):
        return {"fields": {"message": EVENT, "compiler_build": build,
                "package_sha256": self.package, "date": (self.today - timedelta(days=ago)).isoformat(),
                "matched_distinct": matched, "differences": differences, "incomplete": incomplete}}

    def test_restarts_missing_samples_and_the_open_day(self):
        report = summarize([self.event(1, 100), self.event(1, 120), self.event(0, 200),
                            self.event(2, 200, build="another")], "candidate", self.package, self.today)
        self.assertEqual(report["days"][0]["matched_distinct_lower_bound"], 120)
        self.assertEqual(len(report["days"]), 1)
        self.assertFalse(report["field_sample_satisfied"])

    def test_difference_restarts_the_window_and_cannot_be_overwritten(self):
        rows = [self.event(n, 200) for n in range(1, 33)]
        rows.extend([self.event(30, 200, differences=1), self.event(30, 200)])
        report = summarize(rows, "candidate", self.package, self.today)
        self.assertEqual(report["qualifying_days"], 29)
        self.assertFalse(report["field_sample_satisfied"])
        clean = summarize([self.event(n, 200) for n in range(1, 31)], "candidate", self.package, self.today)
        self.assertTrue(clean["field_sample_satisfied"])
        self.assertEqual(summarize([], "candidate", self.package, self.today)["qualifying_days"], 0)

    def test_difference_on_the_open_day_invalidates_a_completed_window(self):
        rows = [self.event(n, 200) for n in range(1, 31)]
        rows.append(self.event(0, 15, differences=1))
        report = summarize(rows, "candidate", self.package, self.today)
        self.assertFalse(report["field_sample_satisfied"])
        self.assertEqual(report["qualifying_days"], 0)
        self.assertTrue(report["days"][-1]["observed_difference"])

    def test_replaced_packages_and_unidentified_workers(self):
        rows = [self.event(n, 200) for n in range(1, 31)]
        rows[0]["fields"]["package_sha256"] = "a" * 64
        del rows[1]["fields"]["package_sha256"]
        report = summarize(rows, "candidate", self.package, self.today)
        self.assertEqual(report["qualifying_days"], 28)
        rows[1]["fields"]["differences"] = 1
        report = summarize(rows, "candidate", self.package, self.today)
        self.assertEqual(report["qualifying_days"], 0)
        self.assertEqual(report["days"][-1]["matched_distinct_lower_bound"], 0)

    def test_third_reset_requests_review_even_after_worker_restarts(self):
        rows = []
        for second in range(3):
            event = self.event(0, 1, differences=1)
            event["timestamp"] = f"{self.today}T12:00:0{second}.000000Z"
            event["fields"]["difference_this_attempt"] = 1
            rows.append(event)
        # A cumulative counter on a later successful attempt still reads one.
        same_count = self.event(0, 2, differences=1)
        same_count["fields"]["difference_this_attempt"] = 0
        two = [rows[0], rows[1], rows[0], same_count]
        self.assertFalse(summarize(two, "candidate", self.package, self.today)["reset_review_required"])
        report = summarize(two + rows, "candidate", self.package, self.today)
        self.assertTrue(report["reset_review_required"])
        self.assertEqual(report["resets_lower_bound"], 3)

    def test_cumulative_only_exports_retain_resets_across_days(self):
        old = self.event(2, 200, differences=2)
        unknown = self.event(1, 0, differences=1)
        del unknown["fields"]["package_sha256"]
        report = summarize([old, old, unknown], "candidate", self.package, self.today)
        self.assertEqual(report["resets_lower_bound"], 3)
        self.assertTrue(report["reset_review_required"])
