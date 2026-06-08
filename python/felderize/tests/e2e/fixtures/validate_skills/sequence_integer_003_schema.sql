-- rule: sequence_integer
-- spark: sequence(start, stop) — generate an array of integers from start to stop (inclusive)
-- feldera: SEQUENCE(start, stop) — same semantics, supported directly in Feldera; returns INTEGER ARRAY
CREATE TABLE interval_data (seq_id INT, begin_int INT, finish_int INT);
