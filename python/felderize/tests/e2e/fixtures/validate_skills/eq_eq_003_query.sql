-- rule: eq_eq
-- spark: a == b — double-equals equality operator (Spark allows this)
-- feldera: a = b — use single = in Feldera
CREATE OR REPLACE TEMP VIEW events_summary_v3 AS SELECT event_id, event_name, region, count FROM events_t3 WHERE year == 2024 AND region == 'North America';
