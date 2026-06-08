-- rule: bit_or_agg
-- spark: bit_or(col) — bitwise OR aggregate over all rows in group
-- feldera: BIT_OR(col)
CREATE OR REPLACE TEMP VIEW bit_or_result_v1 AS SELECT category, BIT_OR(flag_value) AS combined_flags FROM flags_log_v1 GROUP BY category;
