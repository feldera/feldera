-- rule: eq_eq
-- spark: a == b — double-equals equality operator (Spark allows this)
-- feldera: a = b — use single = in Feldera
CREATE TABLE events_t3 (event_id INT, event_name STRING, region STRING, year INT, count INT);
