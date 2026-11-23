#!/usr/bin/env python3
"""Count completed UTC days in an exported field shadow log for one optimizer build."""
import argparse
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import json
from pathlib import Path
import re

EVENT = "Rust optimizer daily shadow sample"
MIN_PROGRAMS = 200
MIN_DAYS = 30


@dataclass
class DailySample:
    matched: int = 0
    incomplete: bool = False
    differences: int = 0
    mismatch_times: set[str] = field(default_factory=set)

    def resets(self):
        # The cumulative count survives a partial export. Distinct mismatch events
        # also capture restarts and concurrent workers. Both count the same attempts.
        return max(self.differences, len(self.mismatch_times))


def summarize(events, build, package, today):
    if not re.fullmatch(r"[0-9a-f]{64}", package):
        raise ValueError("Expected a lowercase SHA-256 compiler package digest")
    days = {}
    for event in events:
        fields = event.get("fields", event)
        if fields.get("message") != EVENT or fields.get("compiler_build") != build:
            continue
        observed = fields.get("package_sha256")
        identified = isinstance(observed, str) and re.fullmatch(r"[0-9a-f]{64}", observed)
        if identified and observed != package:
            continue
        day = date.fromisoformat(fields["date"])
        if day > today:
            raise ValueError(f"Future shadow sample on {day}")
        counts = [fields[key] for key in ("matched_distinct", "differences", "incomplete")]
        if any(type(value) is not int or value < 0 for value in counts) or counts[0] > MIN_PROGRAMS:
            raise ValueError(f"Invalid daily shadow counts on {day}")
        delta = fields.get("difference_this_attempt", 0)
        if type(delta) is not int or delta not in (0, 1) or delta > counts[1]:
            raise ValueError(f"Invalid shadow mismatch increment on {day}")
        if not identified:
            # An older worker or a package replaced during compilation cannot
            # contribute a match. Its difference may belong to this candidate.
            if not counts[1]:
                continue
            counts[0] = 0
        # These are cumulative process counters. Two processes can see identical programs.
        # A single process reaching the threshold proves the lower bound for the day.
        sample = days.setdefault(day, DailySample())
        sample.matched = max(sample.matched, counts[0])
        sample.differences = max(sample.differences, counts[1])
        sample.incomplete |= counts[2] > 0
        if delta and event.get("timestamp"):
            # Re-exporting a log shard must not count a mismatch twice.
            sample.mismatch_times.add(event["timestamp"])

    qualified = []
    report = []
    resets = 0
    for day, sample in sorted(days.items()):
        daily_resets = sample.resets()
        resets += daily_resets
        if daily_resets:
            qualified.clear()
        elif day < today and sample.matched >= MIN_PROGRAMS:
            qualified.append(day.isoformat())
        if day < today or daily_resets:
            report.append({"date": day.isoformat(), "matched_distinct_lower_bound": sample.matched,
                           "observed_difference": bool(daily_resets), "observed_incomplete": sample.incomplete,
                           "resets_lower_bound": daily_resets})
    return {"compiler_build": build, "package_sha256": package, "days": report, "qualifying_dates_since_last_difference": qualified,
            "qualifying_days": len(qualified), "field_sample_satisfied": len(qualified) >= MIN_DAYS,
            "resets_lower_bound": resets, "reset_review_required": resets > 2}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", required=True, help="compiler_build from the field manager")
    parser.add_argument("--package", required=True, help="package_sha256 from the field manager")
    parser.add_argument("logs", type=Path, nargs="+", help="complete exported JSON-lines logs for this field window")
    args = parser.parse_args()

    def events():
        for path in args.logs:
            with path.open() as stream:
                for line in stream:
                    if line.strip():
                        yield json.loads(line)

    print(json.dumps(summarize(events(), args.build, args.package, datetime.now(timezone.utc).date()), indent=2))


if __name__ == "__main__":
    main()
