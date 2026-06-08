-- rule: trunc_date
-- spark: trunc(d, 'YYYY'/'MM'/'QUARTER'/'WEEK') — truncate date using string unit; ONLY these formats work in Spark 4.x trunc() — do NOT use 'Q', 'DD', or 'DAY' as they return NULL
-- feldera: DATE_TRUNC(d, YEAR/MONTH/QUARTER/WEEK) — string unit becomes SQL keyword
CREATE TABLE invoice_records (
  invoice_id INT,
  invoice_date DATE,
  invoice_amount DECIMAL(10,2)
);
