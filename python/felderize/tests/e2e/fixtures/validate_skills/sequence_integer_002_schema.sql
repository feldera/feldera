-- rule: sequence_integer
-- spark: sequence(start, stop) — generate an array of integers from start to stop (inclusive)
-- feldera: SEQUENCE(start, stop) — same semantics, supported directly in Feldera; returns INTEGER ARRAY
CREATE TABLE number_ranges (row_id INT, min_num INT, max_num INT, description STRING);
