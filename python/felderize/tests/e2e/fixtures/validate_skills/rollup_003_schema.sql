-- rule: rollup
-- spark: ROLLUP(a, b) — hierarchical grouping
-- feldera: ROLLUP(a, b) — same
CREATE TABLE transaction_log_v3 (year INT, quarter INT, transaction_amount DECIMAL(15,2), transaction_count INT);
