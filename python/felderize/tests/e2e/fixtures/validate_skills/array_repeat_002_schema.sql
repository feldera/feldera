-- rule: array_repeat
-- spark: array_repeat(val, n) — create array with val repeated n times
-- feldera: ARRAY_REPEAT(val, n)
CREATE TABLE events_002 (event_id INT, event_type STRING, frequency INT);
