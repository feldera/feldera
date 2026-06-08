-- rule: range_between_window
-- spark: SUM(col) OVER (PARTITION BY ... ORDER BY ... RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) — running sum with RANGE frame
-- feldera: Same — RANGE BETWEEN is fully supported in Feldera
CREATE OR REPLACE TEMP VIEW invoice_cumulative_v3 AS SELECT store_code, transaction_date, invoice_amount, SUM(invoice_amount) OVER (PARTITION BY store_code ORDER BY transaction_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS store_running_sum FROM invoice_items;
