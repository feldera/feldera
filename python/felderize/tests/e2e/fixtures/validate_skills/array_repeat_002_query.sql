-- rule: array_repeat
-- spark: array_repeat(val, n) — create array with val repeated n times
-- feldera: ARRAY_REPEAT(val, n)
CREATE OR REPLACE TEMP VIEW event_sequence_v2 AS SELECT event_id, event_type, array_repeat(event_type, frequency) AS event_list FROM events_002 WHERE frequency > 0;
