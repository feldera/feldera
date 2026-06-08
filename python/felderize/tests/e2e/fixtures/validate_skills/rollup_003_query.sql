-- rule: rollup
-- spark: ROLLUP(a, b) — hierarchical grouping
-- feldera: ROLLUP(a, b) — same
CREATE OR REPLACE TEMP VIEW quarterly_rollup_v3 AS SELECT year, quarter, SUM(transaction_amount) as total_amount, SUM(transaction_count) as total_count FROM transaction_log_v3 GROUP BY ROLLUP(year, quarter) ORDER BY year, quarter;
