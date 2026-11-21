#!/usr/bin/env python3
"""Count completed UTC days in an exported field shadow log for one optimizer build."""
import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path

EVENT = "Rust optimizer daily shadow sample"
MIN_PROGRAMS = 200
MIN_DAYS = 30


def summarize(events, build, today):
    days = {}
    for event in events:
        fields = event.get("fields", event)
        if fields.get("message") != EVENT or fields.get("compiler_build") != build:
            continue
        day = date.fromisoformat(fields["date"])
        if day > today:
            raise ValueError(f"Future shadow sample on {day}")
        counts = [fields[key] for key in ("matched_distinct", "differences", "incomplete")]
        if any(type(value) is not int or value < 0 for value in counts) or counts[0] > MIN_PROGRAMS:
            raise ValueError(f"Invalid daily shadow counts on {day}")
        # These are cumulative process counters. Two processes can see identical programs.
        # A single process reaching the threshold proves the lower bound for the day.
        distinct, differences, incomplete = days.get(day, (0, False, False))
        days[day] = (max(distinct, counts[0]), differences or counts[1] > 0,
                     incomplete or counts[2] > 0)

    qualified = []
    report = []
    for day, (distinct, different, incomplete) in sorted(days.items()):
        if different:
            qualified.clear()
        elif day < today and distinct >= MIN_PROGRAMS:
            qualified.append(day.isoformat())
        if day < today or different:
            report.append({"date": day.isoformat(), "matched_distinct_lower_bound": distinct,
                           "observed_difference": different, "observed_incomplete": incomplete})
    return {"compiler_build": build, "days": report, "qualifying_dates_since_last_difference": qualified,
            "qualifying_days": len(qualified), "field_sample_satisfied": len(qualified) >= MIN_DAYS}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", required=True, help="compiler_build from the field manager")
    parser.add_argument("logs", type=Path, nargs="+", help="complete exported JSON-lines logs for this field window")
    args = parser.parse_args()

    def events():
        for path in args.logs:
            with path.open() as stream:
                for line in stream:
                    if line.strip():
                        yield json.loads(line)

    print(json.dumps(summarize(events(), args.build, datetime.now(timezone.utc).date()), indent=2))


if __name__ == "__main__":
    main()
