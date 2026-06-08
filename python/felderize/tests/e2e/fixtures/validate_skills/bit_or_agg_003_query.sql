-- rule: bit_or_agg
-- spark: bit_or(col) — bitwise OR aggregate over all rows in group
-- feldera: BIT_OR(col)
CREATE OR REPLACE TEMP VIEW status_union_v3 AS SELECT region, event_date, BIT_OR(status_flag) AS event_flags FROM status_bits_v3 GROUP BY region, event_date;
