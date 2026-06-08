-- rule: range_between_window
-- spark: SUM(col) OVER (PARTITION BY ... ORDER BY ... RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) — running sum with RANGE frame
-- feldera: Same — RANGE BETWEEN is fully supported in Feldera
CREATE TABLE invoice_items (store_code STRING, transaction_date DATE, invoice_amount DECIMAL(12,2));
CREATE TABLE expected_invoices (store_code STRING, transaction_date DATE, invoice_amount DECIMAL(12,2), store_running_sum DECIMAL(12,2));
