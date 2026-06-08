-- rule: greatest_least_nulls
-- spark: greatest(a, b, ...) / least(a, b, ...) — return maximum/minimum value; Spark skips NULL values
-- feldera: GREATEST_IGNORE_NULLS(a, b, ...) / LEAST_IGNORE_NULLS(a, b, ...) — use the IGNORE_NULLS variant to match Spark's null-skipping semantics
CREATE TABLE timestamp_events (event_id INT, ts1 TIMESTAMP, ts2 TIMESTAMP, ts3 TIMESTAMP);
