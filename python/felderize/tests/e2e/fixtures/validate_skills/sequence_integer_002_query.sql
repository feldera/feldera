-- rule: sequence_integer
-- spark: sequence(start, stop) — generate an array of integers from start to stop (inclusive)
-- feldera: SEQUENCE(start, stop) — same semantics, supported directly in Feldera; returns INTEGER ARRAY
CREATE OR REPLACE TEMP VIEW seq_view_002 AS SELECT row_id, description, sequence(min_num, max_num) AS generated_sequence FROM number_ranges WHERE min_num <= max_num;
