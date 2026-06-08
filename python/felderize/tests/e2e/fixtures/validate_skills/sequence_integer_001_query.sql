-- rule: sequence_integer
-- spark: sequence(start, stop) — generate an array of integers from start to stop (inclusive)
-- feldera: SEQUENCE(start, stop) — same semantics, supported directly in Feldera; returns INTEGER ARRAY
CREATE OR REPLACE TEMP VIEW seq_view_001 AS SELECT id, sequence(start_val, end_val) AS num_seq FROM ranges_001;
