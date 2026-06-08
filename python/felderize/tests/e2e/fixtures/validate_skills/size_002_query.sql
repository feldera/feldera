-- rule: size
-- spark: size(arr) — number of elements in array; returns -1 for NULL input
-- feldera: COALESCE(CARDINALITY(arr), -1) — CARDINALITY returns NULL for NULL input; COALESCE matches Spark's -1 for NULL
CREATE OR REPLACE TEMP VIEW event_size_v2 AS SELECT event_id, size(attendee_ids) AS num_attendees FROM event_attendees;
