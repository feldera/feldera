-- rule: sequence_integer
-- spark: sequence(start, stop) — generate an array of integers from start to stop (inclusive)
-- feldera: SEQUENCE(start, stop) — same semantics, supported directly in Feldera; returns INTEGER ARRAY
CREATE OR REPLACE TEMP VIEW seq_view_003 AS SELECT seq_id, sequence(begin_int, finish_int) AS int_array FROM interval_data ORDER BY seq_id;
